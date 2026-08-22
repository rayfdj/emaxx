use super::*;
use crate::lisp::eval::TreeSitterParserState;
use crate::lisp::primitives::{self, print, regexp};
use std::rc::Rc;
use tree_sitter::{QueryPredicateArg, StreamingIterator};

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
        Some(buffer @ Value::Buffer(_)) => interp.resolve_buffer_id(buffer)?,
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

#[derive(Clone)]
struct FlatNode {
    id: usize,
    kind: String,
    named: bool,
    parent: Option<usize>,
    children: Vec<usize>,
}

struct FlatTree {
    nodes: Vec<FlatNode>,
    source: usize,
}

fn append_flat_node(
    node: tree_sitter::Node<'_>,
    parent: Option<usize>,
    nodes: &mut Vec<FlatNode>,
) -> usize {
    let index = nodes.len();
    nodes.push(FlatNode {
        id: node.id(),
        kind: node.kind().into(),
        named: node.is_named(),
        parent,
        children: Vec::new(),
    });
    let children = (0..node.child_count())
        .filter_map(|child| {
            u32::try_from(child)
                .ok()
                .and_then(|child| node.child(child))
        })
        .map(|child| append_flat_node(child, Some(index), nodes))
        .collect();
    nodes[index].children = children;
    index
}

fn flat_tree(interp: &Interpreter, source: &Value) -> Result<FlatTree, LispError> {
    interp.with_treesit_node(source, |node, parser| {
        let root = parser
            .tree
            .as_ref()
            .expect("a live Tree-sitter node belongs to a parsed tree")
            .root_node();
        let mut nodes = Vec::new();
        append_flat_node(root, None, &mut nodes);
        let source = nodes
            .iter()
            .position(|candidate| candidate.id == node.id())
            .expect("the live node is in its parser tree");
        FlatTree { nodes, source }
    })
}

fn related_flat_node(
    interp: &mut Interpreter,
    source: &Value,
    tree: &FlatTree,
    index: Option<usize>,
) -> Result<Value, LispError> {
    interp.related_treesit_node(source, index.map(|index| tree.nodes[index].id))
}

fn query_string(text: &str) -> String {
    let mut expanded = String::with_capacity(text.len() + 2);
    expanded.push('"');
    for ch in text.chars() {
        match ch {
            '\0' => expanded.push_str("\\0"),
            '\n' => expanded.push_str("\\n"),
            '\r' => expanded.push_str("\\r"),
            '\t' => expanded.push_str("\\t"),
            '"' => expanded.push_str("\\\""),
            '\\' => expanded.push_str("\\\\"),
            other => expanded.push(other),
        }
    }
    expanded.push('"');
    expanded
}

fn pattern_expand(
    interp: &mut Interpreter,
    pattern: &Value,
    env: &Env,
) -> Result<String, LispError> {
    if let Ok(symbol) = pattern.as_symbol()
        && let Some(expanded) = match symbol {
            ":anchor" => Some("."),
            ":?" => Some("?"),
            ":*" => Some("*"),
            ":+" => Some("+"),
            ":equal" => Some("#equal"),
            ":match" => Some("#match"),
            ":pred" => Some("#pred"),
            _ => None,
        }
    {
        return Ok(expanded.into());
    }
    if let Some(string) = primitives::string_like(pattern) {
        return Ok(query_string(&string.text));
    }
    if is_vector_value(pattern) {
        let mut items = pattern.to_vec()?;
        debug_assert_eq!(
            items.first().and_then(|value| value.as_symbol().ok()),
            Some("vector-literal")
        );
        items.remove(0);
        return Ok(format!(
            "[{}]",
            expand_patterns(interp, &items, env)?.join(" ")
        ));
    }
    if pattern.is_cons() {
        return Ok(format!(
            "({})",
            expand_patterns(interp, &pattern.to_vec()?, env)?.join(" ")
        ));
    }
    print::render_prin1_ephemeral(interp, pattern, env)
}

fn expand_patterns(
    interp: &mut Interpreter,
    patterns: &[Value],
    env: &Env,
) -> Result<Vec<String>, LispError> {
    patterns
        .iter()
        .map(|pattern| pattern_expand(interp, pattern, env))
        .collect()
}

fn query_expand(interp: &mut Interpreter, query: &Value, env: &Env) -> Result<String, LispError> {
    Ok(expand_patterns(interp, &query.to_vec()?, env)?.join(" "))
}

fn query_error(error: tree_sitter::QueryError, source: String) -> LispError {
    use tree_sitter::QueryErrorKind;
    let description = match error.kind {
        QueryErrorKind::Syntax | QueryErrorKind::Predicate | QueryErrorKind::Language => {
            "Syntax error at"
        }
        QueryErrorKind::NodeType => "Node type error at",
        QueryErrorKind::Field => "Field error at",
        QueryErrorKind::Capture => "Capture error at",
        QueryErrorKind::Structure => "Structure error at",
    };
    LispError::SignalValue(Value::list([
        Value::symbol("treesit-query-error"),
        Value::String(description.into()),
        Value::Integer(error.offset.saturating_add(1) as i64),
        Value::String(source.into()),
        Value::String("Debug the query with `treesit-query-validate'".into()),
    ]))
}

fn source_string(interp: &mut Interpreter, source: &Value, env: &Env) -> Result<String, LispError> {
    if source.is_cons() {
        query_expand(interp, source, env)
    } else {
        primitives::string_like(source)
            .map(|string| string.text)
            .ok_or_else(|| LispError::TypeError("treesit-query-p".into(), source.type_name()))
    }
}

fn normalize_gnu_predicates(source: &str) -> String {
    // Tree-sitter 0.26 requires predicate names to end in `?' or `!',
    // while GNU exposes #equal, #match and #pred.  Same-width private
    // spellings preserve GNU's byte offsets in any later query error.
    let mut normalized = source.as_bytes().to_vec();
    let mut index = 0;
    let mut in_string = false;
    while index < normalized.len() {
        let byte = normalized[index];
        index += 1;
        if in_string {
            if byte == b'\\' && index < normalized.len() {
                index += 1;
            } else if byte == b'"' {
                in_string = false;
            }
            continue;
        }
        if byte == b'"' {
            in_string = true;
            continue;
        }
        if byte != b'#' {
            continue;
        }
        for (operator, private) in [
            (b"equal".as_slice(), b"equa!".as_slice()),
            (b"match", b"matc!"),
            (b"pred", b"pre!"),
        ] {
            let end = index + operator.len();
            if normalized.get(index..end) == Some(operator)
                && normalized
                    .get(end)
                    .is_none_or(|next| next.is_ascii_whitespace() || *next == b')')
            {
                normalized[index..end].copy_from_slice(private);
                index = end;
                break;
            }
        }
    }
    String::from_utf8(normalized).expect("same-width query normalization preserves UTF-8")
}

fn compile_query(
    interp: &mut Interpreter,
    language: &tree_sitter::Language,
    source: &Value,
    env: &Env,
) -> Result<Rc<tree_sitter::Query>, LispError> {
    let source = source_string(interp, source, env)?;
    let normalized = normalize_gnu_predicates(&source);
    tree_sitter::Query::new(language, &normalized)
        .map(Rc::new)
        .map_err(|error| query_error(error, source))
}

fn ensure_compiled_query(
    interp: &mut Interpreter,
    value: &Value,
    env: &Env,
) -> Result<Rc<tree_sitter::Query>, LispError> {
    let (language, source, cached) = {
        let state = interp.treesit_query_state(value).ok_or_else(|| {
            LispError::TypeError("treesit-compiled-query-p".into(), value.type_name())
        })?;
        (
            state.language.clone(),
            state.source.clone(),
            state.query.clone(),
        )
    };
    if let Some(query) = cached {
        return Ok(query);
    }
    let grammar = interp.require_treesit_language(language.as_symbol()?)?;
    let query = compile_query(interp, &grammar, &source, env)?;
    interp.cache_treesit_query(value, query.clone());
    Ok(query)
}

fn resolve_query_node(interp: &mut Interpreter, value: &Value) -> Result<Value, LispError> {
    if interp.treesit_node_state(value).is_some() {
        interp.with_treesit_node(value, |_, _| ())?;
        return Ok(value.clone());
    }
    if interp.treesit_parser_state(value).is_some() {
        return interp.treesit_root_node(value);
    }
    if value.is_symbol() {
        let (buffer_id, list_buffer_id) = current_or_named_buffer(interp, None)?;
        let tag = Value::Nil;
        let parser = interp
            .reusable_treesit_parser(value, list_buffer_id, &tag)
            .map(Ok)
            .unwrap_or_else(|| {
                interp.create_treesit_parser(value.clone(), buffer_id, list_buffer_id, tag.clone())
            })?;
        return interp.treesit_root_node(&parser);
    }
    Err(LispError::SignalValue(Value::list([
        Value::symbol("wrong-type-argument"),
        Value::list([
            Value::symbol("or"),
            Value::symbol("treesit-node-p"),
            Value::symbol("treesit-parser-p"),
            Value::symbol("symbolp"),
        ]),
        value.clone(),
    ])))
}

fn predicate_signal(kind: &str, message: &str, predicate: &Value) -> LispError {
    LispError::SignalValue(Value::list([
        Value::symbol(kind),
        Value::String(message.into()),
        predicate.clone(),
    ]))
}

fn setting_predicate(
    interp: &Interpreter,
    language: &str,
    thing: &str,
    env: &Env,
) -> Result<Option<Value>, LispError> {
    let settings = interp
        .lookup_var("treesit-thing-settings", env)
        .unwrap_or(Value::Nil);
    for language_entry in settings.to_vec()? {
        let values = language_entry.to_vec()?;
        if values.first().and_then(|value| value.as_symbol().ok()) != Some(language) {
            continue;
        }
        for entry in &values[1..] {
            let definition = entry.to_vec()?;
            if definition.first().and_then(|value| value.as_symbol().ok()) == Some(thing) {
                return Ok(definition.get(1).cloned());
            }
        }
    }
    Ok(None)
}

fn functionp(interp: &mut Interpreter, value: &Value, env: &Env) -> bool {
    primitives::call(
        interp,
        "functionp",
        std::slice::from_ref(value),
        &mut env.clone(),
    )
    .is_ok_and(|value| value.is_truthy())
}

fn validate_predicate(
    interp: &mut Interpreter,
    predicate: &Value,
    language: &str,
    env: &Env,
    depth: usize,
) -> Result<bool, LispError> {
    if depth > 99 {
        return Err(predicate_signal(
            "treesit-invalid-predicate",
            "Predicate recursion level exceeded: it must not exceed 100 levels",
            predicate,
        ));
    }
    if predicate.is_string() || functionp(interp, predicate, env) {
        return Ok(true);
    }
    if let Ok(thing) = predicate.as_symbol() {
        let Some(definition) = setting_predicate(interp, language, thing, env)? else {
            return Ok(false);
        };
        return validate_predicate(interp, &definition, language, env, depth + 1);
    }
    if predicate.is_cons() {
        let car = predicate.car()?;
        let cdr = predicate.cdr()?;
        if car.as_symbol().ok() == Some("not") {
            let values = cdr.to_vec()?;
            if values.len() != 1 {
                return Err(predicate_signal(
                    "treesit-invalid-predicate",
                    "`not' can only have one argument",
                    predicate,
                ));
            }
            return validate_predicate(interp, &values[0], language, env, depth + 1);
        }
        if car.as_symbol().ok() == Some("or") {
            let values = cdr.to_vec()?;
            if values.is_empty() {
                return Err(predicate_signal(
                    "treesit-invalid-predicate",
                    "`or' must have a list of patterns as arguments",
                    predicate,
                ));
            }
            for value in values {
                if !validate_predicate(interp, &value, language, env, depth + 1)? {
                    return Ok(false);
                }
            }
            return Ok(true);
        }
        if car.is_string() && functionp(interp, &cdr, env) {
            return Ok(true);
        }
    }
    Err(predicate_signal(
        "treesit-invalid-predicate",
        "Invalid predicate, see `treesit-thing-settings' for valid forms of predicate",
        predicate,
    ))
}

fn regexp_matches(
    interp: &Interpreter,
    pattern: &Value,
    text: &str,
    env: &Env,
) -> Result<bool, LispError> {
    let pattern = primitives::string_like(pattern)
        .ok_or_else(|| LispError::WrongTypeArgument("stringp".into(), pattern.clone()))?;
    let mut case_sensitive = env.clone();
    case_sensitive.push(vec![("case-fold-search".into(), Value::Nil)].into());
    regexp::compile_elisp_regex(interp, &pattern, &case_sensitive, "", true)?
        .is_match(text)
        .map_err(|error| LispError::Signal(error.to_string()))
}

fn node_matches(
    interp: &mut Interpreter,
    source: &Value,
    tree: &FlatTree,
    index: usize,
    predicate: &Value,
    language: &str,
    env: &Env,
) -> Result<bool, LispError> {
    if predicate.is_string() {
        return regexp_matches(interp, predicate, &tree.nodes[index].kind, env);
    }
    if functionp(interp, predicate, env) {
        let node = related_flat_node(interp, source, tree, Some(index))?;
        return interp
            .call_function_value(predicate.clone(), None, &[node], &mut env.clone())
            .map(|value| value.is_truthy());
    }
    if let Ok(thing) = predicate.as_symbol() {
        let definition = setting_predicate(interp, language, thing, env)?
            .expect("validated named predicate remains defined");
        return node_matches(interp, source, tree, index, &definition, language, env);
    }
    let car = predicate.car()?;
    let cdr = predicate.cdr()?;
    if car.as_symbol().ok() == Some("not") {
        return Ok(!node_matches(
            interp,
            source,
            tree,
            index,
            &cdr.car()?,
            language,
            env,
        )?);
    }
    if car.as_symbol().ok() == Some("or") {
        for option in cdr.to_vec()? {
            if node_matches(interp, source, tree, index, &option, language, env)? {
                return Ok(true);
            }
        }
        return Ok(false);
    }
    if !regexp_matches(interp, &car, &tree.nodes[index].kind, env)? {
        return Ok(false);
    }
    let node = related_flat_node(interp, source, tree, Some(index))?;
    interp
        .call_function_value(cdr, None, &[node], &mut env.clone())
        .map(|value| value.is_truthy())
}

fn validate_for_traversal(
    interp: &mut Interpreter,
    source: &Value,
    predicate: &Value,
    env: &Env,
) -> Result<Option<(FlatTree, String)>, LispError> {
    let tree = flat_tree(interp, source)?;
    let language = interp.with_treesit_node(source, |_, parser| {
        parser.language.as_symbol().map(str::to_string)
    })??;
    if validate_predicate(interp, predicate, &language, env, 0)? {
        Ok(Some((tree, language)))
    } else {
        Ok(None)
    }
}

struct Traversal<'a> {
    source: &'a Value,
    tree: &'a FlatTree,
    predicate: &'a Value,
    language: &'a str,
    env: &'a Env,
}

fn search_subtree(
    interp: &mut Interpreter,
    traversal: &Traversal<'_>,
    index: usize,
    forward: bool,
    named: bool,
    depth: i64,
) -> Result<Option<usize>, LispError> {
    if (!named || traversal.tree.nodes[index].named)
        && node_matches(
            interp,
            traversal.source,
            traversal.tree,
            index,
            traversal.predicate,
            traversal.language,
            traversal.env,
        )?
    {
        return Ok(Some(index));
    }
    if depth == 0 {
        return Ok(None);
    }
    let mut children: Vec<_> = traversal.tree.nodes[index]
        .children
        .iter()
        .copied()
        .filter(|child| !named || traversal.tree.nodes[*child].named)
        .collect();
    if !forward {
        children.reverse();
    }
    for child in children {
        if let Some(found) = search_subtree(interp, traversal, child, forward, named, depth - 1)? {
            return Ok(Some(found));
        }
    }
    Ok(None)
}

fn sibling(tree: &FlatTree, index: usize, forward: bool, named: bool) -> Option<usize> {
    let parent = tree.nodes[index].parent?;
    let siblings = &tree.nodes[parent].children;
    let position = siblings.iter().position(|sibling| *sibling == index)?;
    if forward {
        siblings[position + 1..]
            .iter()
            .copied()
            .find(|candidate| !named || tree.nodes[*candidate].named)
    } else {
        siblings[..position]
            .iter()
            .rev()
            .copied()
            .find(|candidate| !named || tree.nodes[*candidate].named)
    }
}

fn deepest(tree: &FlatTree, mut index: usize, forward: bool) -> usize {
    while let Some(child) = if forward {
        tree.nodes[index].children.first()
    } else {
        tree.nodes[index].children.last()
    } {
        index = *child;
    }
    index
}

fn search_forward(
    interp: &mut Interpreter,
    traversal: &Traversal<'_>,
    forward: bool,
    named: bool,
) -> Result<Option<usize>, LispError> {
    let mut index = traversal.tree.source;
    loop {
        if let Some(next) = sibling(traversal.tree, index, forward, named) {
            index = deepest(traversal.tree, next, forward);
        } else if let Some(parent) = traversal.tree.nodes[index].parent {
            index = parent;
        } else {
            return Ok(None);
        }
        if (!named || traversal.tree.nodes[index].named)
            && node_matches(
                interp,
                traversal.source,
                traversal.tree,
                index,
                traversal.predicate,
                traversal.language,
                traversal.env,
            )?
        {
            return Ok(Some(index));
        }
    }
}

fn subtree_stat(tree: &FlatTree, root: usize) -> (usize, usize, usize) {
    let mut max_depth = 1;
    let mut max_width = 0;
    let mut count = 0;
    let mut pending = vec![(root, 0)];
    while let Some((index, depth)) = pending.pop() {
        count += 1;
        max_depth = max_depth.max(depth);
        max_width = max_width.max(tree.nodes[index].children.len());
        pending.extend(
            tree.nodes[index]
                .children
                .iter()
                .rev()
                .map(|child| (*child, depth + 1)),
        );
    }
    (max_depth, max_width, count)
}

#[derive(Clone)]
struct OwnedCapture {
    index: u32,
    name: String,
    node_id: usize,
    text: String,
}

struct OwnedMatch {
    pattern_index: usize,
    captures: Vec<OwnedCapture>,
}

fn collect_query_matches(
    interp: &Interpreter,
    node: &Value,
    query: &tree_sitter::Query,
    range: Option<std::ops::Range<usize>>,
) -> Result<Vec<OwnedMatch>, LispError> {
    interp.with_treesit_node(node, |node, parser| {
        let text = interp
            .get_buffer_by_id(parser.buffer_id)
            .expect("node buffer liveness was checked")
            .buffer_string();
        let mut cursor = tree_sitter::QueryCursor::new();
        if let Some(range) = range {
            cursor.set_byte_range(range);
        }
        let mut matches = cursor.matches(query, node, text.as_bytes());
        let mut result = Vec::new();
        while let Some(found) = matches.next() {
            result.push(OwnedMatch {
                pattern_index: found.pattern_index,
                captures: found
                    .captures
                    .iter()
                    .map(|capture| OwnedCapture {
                        index: capture.index,
                        name: query.capture_names()[capture.index as usize].into(),
                        node_id: capture.node.id(),
                        text: text
                            .get(capture.node.byte_range())
                            .unwrap_or_default()
                            .into(),
                    })
                    .collect(),
            });
        }
        result
    })
}

fn query_predicate_error(message: impl Into<String>) -> LispError {
    let message: String = message.into();
    LispError::SignalValue(Value::list([
        Value::symbol("treesit-query-error"),
        Value::String(message.into()),
    ]))
}

fn capture_arg<'a>(
    captures: &'a [OwnedCapture],
    arg: &QueryPredicateArg,
) -> Result<&'a OwnedCapture, LispError> {
    let QueryPredicateArg::Capture(index) = arg else {
        return Err(query_predicate_error("Expected a capture name"));
    };
    captures
        .iter()
        .find(|capture| capture.index == *index)
        .ok_or_else(|| {
            query_predicate_error(
                "Cannot find captured node; a predicate can only refer to captures in its pattern",
            )
        })
}

fn predicate_arg_text(
    captures: &[OwnedCapture],
    arg: &QueryPredicateArg,
) -> Result<String, LispError> {
    match arg {
        QueryPredicateArg::Capture(_) => Ok(capture_arg(captures, arg)?.text.clone()),
        QueryPredicateArg::String(text) => Ok(text.to_string()),
    }
}

fn query_match_passes(
    interp: &mut Interpreter,
    source: &Value,
    query: &tree_sitter::Query,
    found: &OwnedMatch,
    env: &Env,
) -> Result<bool, LispError> {
    for predicate in query.general_predicates(found.pattern_index) {
        match predicate.operator.as_ref() {
            "equa!" => {
                if predicate.args.len() != 2 {
                    return Err(query_predicate_error(format!(
                        "Predicate `equal' requires two arguments but got {}",
                        predicate.args.len()
                    )));
                }
                if predicate_arg_text(&found.captures, &predicate.args[0])?
                    != predicate_arg_text(&found.captures, &predicate.args[1])?
                {
                    return Ok(false);
                }
            }
            "matc!" => {
                if predicate.args.len() != 2 {
                    return Err(query_predicate_error(format!(
                        "Predicate `match' requires two arguments but got {}",
                        predicate.args.len()
                    )));
                }
                let QueryPredicateArg::String(pattern) = &predicate.args[0] else {
                    return Err(query_predicate_error(
                        "The first argument to `match' should be a regexp string",
                    ));
                };
                let capture = capture_arg(&found.captures, &predicate.args[1])?;
                if !regexp_matches(
                    interp,
                    &Value::String(pattern.to_string().into()),
                    &capture.text,
                    env,
                )? {
                    return Ok(false);
                }
            }
            "pre!" => {
                if predicate.args.len() < 2 {
                    return Err(query_predicate_error(format!(
                        "Predicate `pred' requires at least two arguments, but only got {}",
                        predicate.args.len()
                    )));
                }
                let QueryPredicateArg::String(function) = &predicate.args[0] else {
                    return Err(query_predicate_error(
                        "The first argument to `pred' should be a function name",
                    ));
                };
                let args = predicate.args[1..]
                    .iter()
                    .map(|arg| {
                        capture_arg(&found.captures, arg).and_then(|capture| {
                            interp.related_treesit_node(source, Some(capture.node_id))
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                if !interp
                    .call_function_value(
                        Value::symbol(function),
                        Some(function),
                        &args,
                        &mut env.clone(),
                    )?
                    .is_truthy()
                {
                    return Ok(false);
                }
            }
            operator => {
                return Err(query_predicate_error(format!(
                    "Invalid predicate `{operator}'"
                )));
            }
        }
    }
    Ok(true)
}

fn query_capture(
    interp: &mut Interpreter,
    source: &Value,
    query_value: &Value,
    range: Option<std::ops::Range<usize>>,
    node_only: bool,
    env: &Env,
) -> Result<Value, LispError> {
    let query = if interp.treesit_query_state(query_value).is_some() {
        ensure_compiled_query(interp, query_value, env)?
    } else {
        let language = interp.with_treesit_node(source, |_, parser| parser.language.clone())?;
        let grammar = interp.require_treesit_language(language.as_symbol()?)?;
        compile_query(interp, &grammar, query_value, env)?
    };
    let matches = collect_query_matches(interp, source, &query, range)?;
    let mut result = Vec::new();
    for found in matches {
        if !query_match_passes(interp, source, &query, &found, env)? {
            continue;
        }
        for capture in found.captures {
            let node = interp.related_treesit_node(source, Some(capture.node_id))?;
            result.push(if node_only {
                node
            } else {
                Value::cons(Value::symbol(&capture.name), node)
            });
        }
    }
    Ok(Value::list(result))
}

fn sparse_nodes(
    interp: &mut Interpreter,
    traversal: &Traversal<'_>,
    index: usize,
    process: Option<&Value>,
    depth: i64,
) -> Result<Vec<Value>, LispError> {
    let matched = node_matches(
        interp,
        traversal.source,
        traversal.tree,
        index,
        traversal.predicate,
        traversal.language,
        traversal.env,
    )?;
    let children = if depth > 0 {
        traversal.tree.nodes[index].children.clone()
    } else {
        Vec::new()
    };
    let mut descendants = Vec::new();
    for child in children {
        descendants.extend(sparse_nodes(interp, traversal, child, process, depth - 1)?);
    }
    if !matched {
        return Ok(descendants);
    }
    let node = related_flat_node(interp, traversal.source, traversal.tree, Some(index))?;
    let value = if let Some(function) = process {
        interp.call_function_value(function.clone(), None, &[node], &mut traversal.env.clone())?
    } else {
        node
    };
    Ok(vec![Value::cons(value, Value::list(descendants))])
}

define_dispatch!(
    pub(super) fn call(
        interp: &mut Interpreter,
        name: &str,
        args: &[Value],
        env: &Env,
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
                    && let Some(parser) =
                        interp.reusable_treesit_parser(&args[0], list_buffer_id, &tag)
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
                    return Err(LispError::WrongTypeArgument("consp".into(), args[1].clone()));
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
                    .ok_or_else(|| {
                        LispError::TypeError("treesit-node-p".into(), args[0].type_name())
                    })
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
                node_or_nil(interp, &args[0], |node, _| {
                    Value::String(node.to_sexp().into())
                })
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
            "treesit-node-first-child-for-pos" => {
                need_arg_range(name, args, 2, 3)?;
                if args[0].is_nil() {
                    return Ok(Value::Nil);
                }
                let position = args[1].as_integer()?;
                let byte = interp.treesit_node_relative_byte(&args[0], position)?;
                let named = args.get(2).is_some_and(Value::is_truthy);
                let child_id = interp.with_treesit_node(&args[0], |node, _| {
                    (0..node.child_count())
                        .filter_map(|index| {
                            u32::try_from(index)
                                .ok()
                                .and_then(|index| node.child(index))
                        })
                        .find(|child| child.end_byte() > byte && (!named || child.is_named()))
                        .map(|child| child.id())
                })?;
                interp.related_treesit_node(&args[0], child_id)
            }
            "treesit-node-descendant-for-range" => {
                need_arg_range(name, args, 3, 4)?;
                if args[0].is_nil() {
                    return Ok(Value::Nil);
                }
                let start = interp.treesit_node_relative_byte(&args[0], args[1].as_integer()?)?;
                let end = interp.treesit_node_relative_byte(&args[0], args[2].as_integer()?)?;
                let named = args.get(3).is_some_and(Value::is_truthy);
                let descendant = interp.with_treesit_node(&args[0], |node, _| {
                    if named {
                        node.named_descendant_for_byte_range(start, end)
                    } else {
                        node.descendant_for_byte_range(start, end)
                    }
                    .map(|node| node.id())
                })?;
                interp.related_treesit_node(&args[0], descendant)
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
                    .ok_or_else(|| LispError::WrongTypeArgument("stringp".into(), args[1].clone()))?;
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
            "treesit-pattern-expand" => {
                need_args(name, args, 1)?;
                Ok(Value::String(pattern_expand(interp, &args[0], env)?.into()))
            }
            "treesit-query-expand" => {
                need_args(name, args, 1)?;
                Ok(Value::String(query_expand(interp, &args[0], env)?.into()))
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
                        || matches!(args[0], Value::Cons(_)) && !is_vector_value(&args[0])
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
                    return Err(LispError::WrongTypeArgument("symbolp".into(), args[0].clone()));
                }
                if interp.treesit_query_state(&args[1]).is_some() {
                    return Ok(args[1].clone());
                }
                if !(args[1].is_string()
                    || matches!(args[1], Value::Cons(_)) && !is_vector_value(&args[1]))
                {
                    return Err(LispError::TypeError(
                        "treesit-query-p".into(),
                        args[1].type_name(),
                    ));
                }
                let query = interp.create_treesit_query(args[0].clone(), args[1].clone());
                if args.get(2).is_some_and(Value::is_truthy) {
                    ensure_compiled_query(interp, &query, env)?;
                }
                Ok(query)
            }
            "treesit-query-capture" => {
                need_arg_range(name, args, 2, 5)?;
                if !(interp.treesit_query_state(&args[1]).is_some()
                    || args[1].is_string()
                    || matches!(args[1], Value::Cons(_)) && !is_vector_value(&args[1]))
                {
                    return Err(LispError::TypeError(
                        "treesit-query-p".into(),
                        args[1].type_name(),
                    ));
                }
                let node = resolve_query_node(interp, &args[0])?;
                let start = args
                    .get(2)
                    .filter(|value| !value.is_nil())
                    .map(|value| interp.treesit_node_relative_byte(&node, value.as_integer()?))
                    .transpose()?;
                let end = args
                    .get(3)
                    .filter(|value| !value.is_nil())
                    .map(|value| interp.treesit_node_relative_byte(&node, value.as_integer()?))
                    .transpose()?;
                query_capture(
                    interp,
                    &node,
                    &args[1],
                    start.zip(end).map(|(start, end)| start..end),
                    args.get(4).is_some_and(Value::is_truthy),
                    env,
                )
            }
            "treesit-search-subtree" => {
                need_arg_range(name, args, 2, 5)?;
                args.get(2).unwrap_or(&Value::Nil).as_symbol()?;
                args.get(3).unwrap_or(&Value::Nil).as_symbol()?;
                let depth = args
                    .get(4)
                    .filter(|value| !value.is_nil())
                    .map(Value::as_integer)
                    .transpose()?
                    .unwrap_or(1000);
                let Some((tree, language)) =
                    validate_for_traversal(interp, &args[0], &args[1], env)?
                else {
                    return Ok(Value::Nil);
                };
                let traversal = Traversal {
                    source: &args[0],
                    tree: &tree,
                    predicate: &args[1],
                    language: &language,
                    env,
                };
                let found = search_subtree(
                    interp,
                    &traversal,
                    tree.source,
                    !args.get(2).is_some_and(Value::is_truthy),
                    !args.get(3).is_some_and(Value::is_truthy),
                    depth,
                )?;
                related_flat_node(interp, &args[0], &tree, found)
            }
            "treesit-search-forward" => {
                need_arg_range(name, args, 2, 4)?;
                args.get(2).unwrap_or(&Value::Nil).as_symbol()?;
                args.get(3).unwrap_or(&Value::Nil).as_symbol()?;
                let Some((tree, language)) =
                    validate_for_traversal(interp, &args[0], &args[1], env)?
                else {
                    return Ok(Value::Nil);
                };
                let traversal = Traversal {
                    source: &args[0],
                    tree: &tree,
                    predicate: &args[1],
                    language: &language,
                    env,
                };
                let found = search_forward(
                    interp,
                    &traversal,
                    !args.get(2).is_some_and(Value::is_truthy),
                    !args.get(3).is_some_and(Value::is_truthy),
                )?;
                related_flat_node(interp, &args[0], &tree, found)
            }
            "treesit-induce-sparse-tree" => {
                need_arg_range(name, args, 2, 4)?;
                let process = args.get(2).filter(|function| !function.is_nil());
                if process.is_some_and(|function| !functionp(interp, function, env)) {
                    return Err(LispError::TypeError(
                        "functionp".into(),
                        args[2].type_name(),
                    ));
                }
                let depth = args
                    .get(3)
                    .filter(|value| !value.is_nil())
                    .map(Value::as_integer)
                    .transpose()?
                    .unwrap_or(1000);
                let Some((tree, language)) =
                    validate_for_traversal(interp, &args[0], &args[1], env)?
                else {
                    return Ok(Value::Nil);
                };
                let traversal = Traversal {
                    source: &args[0],
                    tree: &tree,
                    predicate: &args[1],
                    language: &language,
                    env,
                };
                let sparse = sparse_nodes(interp, &traversal, tree.source, process, depth)?;
                Ok(if sparse.is_empty() {
                    Value::Nil
                } else {
                    Value::cons(Value::Nil, Value::list(sparse))
                })
            }
            "treesit-node-match-p" => {
                need_arg_range(name, args, 2, 3)?;
                if args[0].is_nil() {
                    return Ok(Value::Nil);
                }
                let (tree, language) = match validate_for_traversal(
                    interp, &args[0], &args[1], env,
                )? {
                    Some(validated) => validated,
                    None if args.get(2).is_some_and(Value::is_truthy) => {
                        return Ok(Value::Nil);
                    }
                    None => {
                        return Err(predicate_signal(
                            "treesit-predicate-not-found",
                            "Cannot find the definition of the predicate in `treesit-thing-settings'",
                            &args[1],
                        ));
                    }
                };
                Ok(
                    if node_matches(
                        interp,
                        &args[0],
                        &tree,
                        tree.source,
                        &args[1],
                        &language,
                        env,
                    )? {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
            "treesit-subtree-stat" => {
                need_args(name, args, 1)?;
                let tree = flat_tree(interp, &args[0])?;
                let (depth, width, count) = subtree_stat(&tree, tree.source);
                Ok(Value::list([
                    Value::Integer(depth as i64),
                    Value::Integer(width as i64),
                    Value::Integer(count as i64),
                ]))
            }
        }
    }
);
