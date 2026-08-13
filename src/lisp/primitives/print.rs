use super::*;

pub(crate) fn render_prin1_string(interp: &Interpreter, text: &str, env: &Env) -> String {
    let escape_multibyte = interp
        .lookup_var("print-escape-multibyte", env)
        .is_some_and(|value| value.is_truthy());
    let escape_newlines = interp
        .lookup_var("print-escape-newlines", env)
        .is_some_and(|value| value.is_truthy());

    // GNU prin1 escapes only `"' and `\' by default: newlines, tabs and
    // other control characters print raw unless print-escape-newlines is
    // non-nil (Rust's {:?} formatting escapes them all, which is wrong).
    let mut rendered = String::with_capacity(text.len() + 2);
    rendered.push('"');
    for ch in text.chars() {
        match ch {
            '"' => rendered.push_str("\\\""),
            '\\' => rendered.push_str("\\\\"),
            '\n' if escape_newlines => rendered.push_str("\\n"),
            '\r' if escape_newlines => rendered.push_str("\\r"),
            '\t' if escape_newlines => rendered.push_str("\\t"),
            '\u{0008}' if escape_newlines => rendered.push_str("\\b"),
            '\u{000C}' if escape_newlines => rendered.push_str("\\f"),
            ch if escape_multibyte && !ch.is_ascii() => {
                rendered.push_str(&format!("\\x{:04x}", ch as u32));
            }
            ch => rendered.push(ch),
        }
    }
    rendered.push('"');
    rendered
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum PrintRefKey {
    Cons(usize),
    Record(u64),
    StringObject(usize),
    Symbol(String),
}

#[derive(Clone, Copy, Default)]
pub(crate) struct PrintOptions {
    circle: bool,
    continuous_numbering: bool,
    dialect: PrintDialect,
    gensym: bool,
    integers_as_characters: bool,
    length: Option<usize>,
    level: Option<usize>,
    quoted: bool,
    string_length: Option<usize>,
}

#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum PrintDialect {
    #[default]
    Emacs,
    Cl,
}

#[derive(Clone)]
pub(crate) struct PrintContext {
    options: PrintOptions,
    counts: HashMap<PrintRefKey, usize>,
    labels: HashMap<PrintRefKey, usize>,
    next_label: usize,
    active: HashSet<PrintRefKey>,
    first_ellipsis_expansion: Option<String>,
}

impl PrintContext {
    fn new(
        interp: &Interpreter,
        value: &Value,
        env: &Env,
        options: PrintOptions,
    ) -> Result<Self, LispError> {
        let (labels, next_label) = if options.circle && options.continuous_numbering {
            parse_print_number_table(interp.lookup_var("print-number-table", env).as_ref())
        } else {
            (HashMap::new(), 1)
        };
        let mut counts = HashMap::new();
        if options.circle {
            collect_print_counts(interp, value, options, &mut counts, &mut HashSet::new())?;
        }
        Ok(Self {
            options,
            counts,
            labels,
            next_label,
            active: HashSet::new(),
            first_ellipsis_expansion: None,
        })
    }

    fn record_ellipsis_expansion(&mut self, expansion: String) {
        if self.first_ellipsis_expansion.is_none() {
            self.first_ellipsis_expansion = Some(expansion);
        }
    }
}

pub(crate) fn print_options(
    interp: &Interpreter,
    env: &Env,
    dialect: PrintDialect,
) -> PrintOptions {
    PrintOptions {
        circle: interp
            .lookup_var("print-circle", env)
            .is_some_and(|value| value.is_truthy()),
        continuous_numbering: dialect != PrintDialect::Cl
            && interp
                .lookup_var("print-continuous-numbering", env)
                .is_some_and(|value| value.is_truthy()),
        dialect,
        gensym: interp
            .lookup_var("print-gensym", env)
            .is_some_and(|value| value.is_truthy()),
        integers_as_characters: interp
            .lookup_var("print-integers-as-characters", env)
            .is_some_and(|value| value.is_truthy()),
        length: interp
            .lookup_var("print-length", env)
            .and_then(|value| value.as_integer().ok())
            .and_then(|value| usize::try_from(value).ok()),
        level: interp
            .lookup_var("print-level", env)
            .and_then(|value| value.as_integer().ok())
            .and_then(|value| usize::try_from(value).ok()),
        quoted: interp
            .lookup_var("print-quoted", env)
            .is_some_and(|value| value.is_truthy()),
        string_length: interp
            .lookup_var("cl-print-string-length", env)
            .and_then(|value| value.as_integer().ok())
            .and_then(|value| usize::try_from(value).ok()),
    }
}

pub(crate) fn record_prin1_fields(
    interp: &Interpreter,
    id: u64,
    dialect: PrintDialect,
) -> Option<Vec<Value>> {
    let record = interp.find_record(id)?;
    match record.type_name.as_str() {
        "thread" | "mutex" | "condition-variable" | "hash-table" | "process" | "obarray" => None,
        "literal-record" => Some(record.slots.clone()),
        _ => {
            if dialect == PrintDialect::Cl
                && let Some(slot_names) = interp
                    .get_symbol_property(&record.type_name, "emaxx-struct-slots")
                    .and_then(|value| value.to_vec().ok())
                && slot_names.len() == record.slots.len()
            {
                let mut fields = Vec::with_capacity(1 + record.slots.len() * 2);
                fields.push(Value::Symbol(record.type_name.clone().into()));
                for (slot_name, slot_value) in slot_names.iter().zip(record.slots.iter()) {
                    let Ok(slot_name) = slot_name.as_symbol() else {
                        return None;
                    };
                    fields.push(Value::Symbol(format!(":{slot_name}").into()));
                    fields.push(slot_value.clone());
                }
                return Some(fields);
            }
            Some(
                std::iter::once(Value::Symbol(record.type_name.clone().into()))
                    .chain(record.slots.iter().cloned())
                    .collect(),
            )
        }
    }
}

pub(crate) fn print_ref_key(
    interp: &Interpreter,
    value: &Value,
    options: PrintOptions,
) -> Option<PrintRefKey> {
    match value {
        Value::Cons(cell) => Some(PrintRefKey::Cons(crate::lisp::types::ConsCell::identity(
            cell,
        ))),
        Value::StringObject(state) => Some(PrintRefKey::StringObject(Rc::as_ptr(state) as usize)),
        Value::Symbol(symbol)
            if options.gensym && crate::lisp::types::is_uninterned_symbol(symbol) =>
        {
            Some(PrintRefKey::Symbol(symbol.to_string()))
        }
        Value::Record(id) if record_prin1_fields(interp, *id, PrintDialect::Emacs).is_some() => {
            Some(PrintRefKey::Record(*id))
        }
        _ => None,
    }
}

pub(crate) fn print_ref_placeholder(key: PrintRefKey) -> String {
    match key {
        PrintRefKey::Cons(_) => "#0".into(),
        PrintRefKey::Record(id) => format!("#{id}"),
        PrintRefKey::StringObject(_) => "#<string>".into(),
        PrintRefKey::Symbol(_) => "#<symbol>".into(),
    }
}

pub(crate) fn print_number_table_entry(key: &PrintRefKey, label: usize) -> Value {
    let label = Value::Integer(label as i64);
    match key {
        PrintRefKey::Cons(id) => Value::list([
            Value::Symbol("cons".into()),
            Value::String(id.to_string().into()),
            label,
        ]),
        PrintRefKey::Record(id) => Value::list([
            Value::Symbol("record".into()),
            Value::String(id.to_string().into()),
            label,
        ]),
        PrintRefKey::StringObject(id) => Value::list([
            Value::Symbol("string-object".into()),
            Value::String(id.to_string().into()),
            label,
        ]),
        PrintRefKey::Symbol(symbol) => Value::list([
            Value::Symbol("symbol".into()),
            Value::String(symbol.clone().into()),
            label,
        ]),
    }
}

pub(crate) fn print_number_table_value(
    labels: &HashMap<PrintRefKey, usize>,
    next_label: usize,
) -> Value {
    let mut entries = labels.iter().collect::<Vec<_>>();
    entries.sort_by_key(|(_, label)| **label);

    let mut items = Vec::with_capacity(entries.len() + 2);
    items.push(Value::Symbol("emaxx-print-number-table".into()));
    items.push(Value::Integer(next_label as i64));
    for (key, label) in entries {
        items.push(print_number_table_entry(key, *label));
    }
    Value::list(items)
}

pub(crate) fn parse_print_number_table(
    value: Option<&Value>,
) -> (HashMap<PrintRefKey, usize>, usize) {
    let Some(value) = value else {
        return (HashMap::new(), 1);
    };
    let Ok(items) = value.to_vec() else {
        return (HashMap::new(), 1);
    };
    let [Value::Symbol(tag), next_label, rest @ ..] = items.as_slice() else {
        return (HashMap::new(), 1);
    };
    if tag != "emaxx-print-number-table" {
        return (HashMap::new(), 1);
    }

    let next_label = next_label
        .as_integer()
        .ok()
        .and_then(|value| usize::try_from(value).ok())
        .filter(|value| *value > 0)
        .unwrap_or(1);

    let mut labels = HashMap::new();
    for entry in rest {
        let Ok(parts) = entry.to_vec() else {
            continue;
        };
        let [Value::Symbol(kind), data, label] = parts.as_slice() else {
            continue;
        };
        let Some(label) = label
            .as_integer()
            .ok()
            .and_then(|value| usize::try_from(value).ok())
        else {
            continue;
        };
        let Ok(data) = data.as_string() else {
            continue;
        };
        let key = match kind.as_str() {
            "cons" => data.parse().ok().map(PrintRefKey::Cons),
            "record" => data.parse().ok().map(PrintRefKey::Record),
            "string-object" => data.parse().ok().map(PrintRefKey::StringObject),
            "symbol" => Some(PrintRefKey::Symbol(data.to_string())),
            _ => None,
        };
        if let Some(key) = key {
            labels.insert(key, label);
        }
    }

    let max_label = labels.values().copied().max().unwrap_or(0);
    (labels, next_label.max(max_label + 1))
}

pub(crate) fn set_env_binding(env: &mut Env, name: &str, value: Value) {
    for frame in env.iter_mut().rev() {
        for (key, slot) in frame.iter_mut().rev() {
            if key == name {
                *slot = value;
                return;
            }
        }
    }
    env.push(vec![(name.into(), value)].into());
}

pub(crate) fn sync_print_number_table(
    target_env: &mut Env,
    overrides: Option<&Value>,
    source_env: &Env,
) {
    if !matches!(overrides, None | Some(Value::Nil)) {
        return;
    }
    let Some(value) = source_env
        .iter()
        .rev()
        .flat_map(|frame| frame.iter().rev())
        .find_map(|(name, value)| (name == "print-number-table").then(|| value.clone()))
    else {
        return;
    };
    set_env_binding(target_env, "print-number-table", value);
}

pub(crate) fn collect_print_counts(
    interp: &Interpreter,
    value: &Value,
    options: PrintOptions,
    counts: &mut HashMap<PrintRefKey, usize>,
    expanded: &mut HashSet<PrintRefKey>,
) -> Result<(), LispError> {
    if let Some(key) = print_ref_key(interp, value, options) {
        *counts.entry(key.clone()).or_insert(0) += 1;
        if !expanded.insert(key) {
            return Ok(());
        }
    }

    match value {
        Value::Cons(_) if is_vector_value(value) => {
            for item in vector_items(value)? {
                collect_print_counts(interp, &item, options, counts, expanded)?;
            }
        }
        Value::Cons(_) => {
            let Some((car, cdr)) = value.cons_values() else {
                return Ok(());
            };
            collect_print_counts(interp, &car, options, counts, expanded)?;
            collect_print_counts(interp, &cdr, options, counts, expanded)?;
        }
        Value::StringObject(state) => {
            let props = state.borrow().props.clone();
            for span in props {
                for (_, prop_value) in span.props {
                    collect_print_counts(interp, &prop_value, options, counts, expanded)?;
                }
            }
        }
        Value::Record(id) => {
            if let Some(fields) = record_prin1_fields(interp, *id, PrintDialect::Emacs) {
                for field in fields {
                    collect_print_counts(interp, &field, options, counts, expanded)?;
                }
            }
        }
        _ => {}
    }

    Ok(())
}

pub(crate) fn collect_print_table_objects(
    interp: &Interpreter,
    value: &Value,
    options: PrintOptions,
    counts: &mut HashMap<PrintRefKey, (usize, Value)>,
    expanded: &mut HashSet<PrintRefKey>,
) -> Result<(), LispError> {
    if let Some(key) = print_ref_key(interp, value, options) {
        let entry = counts.entry(key.clone()).or_insert((0, value.clone()));
        entry.0 += 1;
        if !expanded.insert(key) {
            return Ok(());
        }
    }

    match value {
        Value::Cons(_) if is_vector_value(value) => {
            for item in vector_items(value)? {
                collect_print_table_objects(interp, &item, options, counts, expanded)?;
            }
        }
        Value::Cons(_) => {
            let Some((car, cdr)) = value.cons_values() else {
                return Ok(());
            };
            collect_print_table_objects(interp, &car, options, counts, expanded)?;
            collect_print_table_objects(interp, &cdr, options, counts, expanded)?;
        }
        Value::StringObject(state) => {
            let props = state.borrow().props.clone();
            for span in props {
                for (_, prop_value) in span.props {
                    collect_print_table_objects(interp, &prop_value, options, counts, expanded)?;
                }
            }
        }
        Value::Record(id) => {
            if let Some(fields) = record_prin1_fields(interp, *id, PrintDialect::Emacs) {
                for field in fields {
                    collect_print_table_objects(interp, &field, options, counts, expanded)?;
                }
            }
        }
        _ => {}
    }

    Ok(())
}

pub(crate) fn print_preprocess(
    interp: &mut Interpreter,
    value: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    if !interp
        .lookup_var("print-circle", env)
        .is_some_and(|value| value.is_truthy())
    {
        return Ok(Value::Nil);
    }

    let table = match interp.lookup_var("print-number-table", env) {
        Some(existing) if json::is_hash_table(interp, &existing) => existing,
        _ => json::make_hash_table(interp, "eql", Vec::new()),
    };
    set_env_binding(env, "print-number-table", table.clone());

    let mut counts = HashMap::new();
    collect_print_table_objects(
        interp,
        value,
        print_options(interp, env, PrintDialect::Emacs),
        &mut counts,
        &mut HashSet::new(),
    )?;

    let entries = counts
        .into_values()
        .filter(|(count, _)| *count > 1)
        .map(|(_, object)| (object, Value::T))
        .collect::<Vec<_>>();
    set_hash_table_entries(interp, &table, entries)?;
    Ok(Value::Nil)
}

fn render_cl_ellipsis_object_expansion(
    interp: &mut Interpreter,
    value: &Value,
    env: &mut Env,
    context: &PrintContext,
) -> Result<String, LispError> {
    let mut expansion_context = context.clone();
    expansion_context.first_ellipsis_expansion = None;
    if let Some(key) = print_ref_key(interp, value, context.options) {
        expansion_context.active.remove(&key);
    }
    render_prin1_with_context(interp, value, env, &mut expansion_context, 0)
}

fn render_cl_list_tail_expansion(
    interp: &mut Interpreter,
    tail: &Value,
    env: &mut Env,
    context: &PrintContext,
) -> Result<String, LispError> {
    if let Some(key) = print_ref_key(interp, tail, context.options) {
        if context.options.circle
            && let Some(label) = context.labels.get(&key)
        {
            return Ok(format!("#{label}#"));
        }
        if !context.options.circle && context.active.contains(&key) {
            return Ok(print_ref_placeholder(key));
        }
    }

    let mut expansion_context = context.clone();
    expansion_context.first_ellipsis_expansion = None;
    let mut rendered = Vec::new();
    let mut current = tail.clone();
    loop {
        match current {
            Value::Nil => return Ok(rendered.join(" ")),
            Value::Cons(_) if is_vector_value(&current) => {
                let tail_rendered =
                    render_prin1_with_context(interp, &current, env, &mut expansion_context, 0)?;
                return Ok(if rendered.is_empty() {
                    tail_rendered
                } else {
                    format!("{} . {}", rendered.join(" "), tail_rendered)
                });
            }
            Value::Cons(_) => {
                if expansion_context
                    .options
                    .length
                    .is_some_and(|limit| rendered.len() >= limit)
                {
                    rendered.push("...".into());
                    return Ok(rendered.join(" "));
                }
                let Some((car, cdr)) = current.cons_values() else {
                    return Ok(rendered.join(" "));
                };
                rendered.push(render_prin1_with_context(
                    interp,
                    &car,
                    env,
                    &mut expansion_context,
                    0,
                )?);
                current = cdr;
            }
            other => {
                let tail_rendered =
                    render_prin1_with_context(interp, &other, env, &mut expansion_context, 0)?;
                return Ok(if rendered.is_empty() {
                    tail_rendered
                } else {
                    format!("{} . {}", rendered.join(" "), tail_rendered)
                });
            }
        }
    }
}

fn render_cl_vector_tail_expansion(
    interp: &mut Interpreter,
    items: &[Value],
    start: usize,
    env: &mut Env,
    context: &PrintContext,
) -> Result<String, LispError> {
    let mut expansion_context = context.clone();
    expansion_context.first_ellipsis_expansion = None;
    let mut rendered = Vec::new();
    for item in items.iter().skip(start) {
        if expansion_context
            .options
            .length
            .is_some_and(|limit| rendered.len() >= limit)
        {
            rendered.push("...".into());
            break;
        }
        rendered.push(render_prin1_with_context(
            interp,
            item,
            env,
            &mut expansion_context,
            0,
        )?);
    }
    Ok(rendered.join(" "))
}

fn render_cl_string_property_tail_expansion(
    interp: &mut Interpreter,
    fields: &[Value],
    start: usize,
    env: &mut Env,
    context: &PrintContext,
) -> Result<String, LispError> {
    let mut expansion_context = context.clone();
    expansion_context.first_ellipsis_expansion = None;
    let interval_limit = context
        .options
        .length
        .map(|limit| (limit / 3).max(1))
        .unwrap_or(usize::MAX);
    let mut rendered = Vec::new();
    let mut intervals = 0usize;
    let mut index = start;
    while index < fields.len() {
        if intervals >= interval_limit {
            rendered.push("...".into());
            break;
        }
        for field in fields.iter().skip(index).take(3) {
            rendered.push(render_prin1_with_context(
                interp,
                field,
                env,
                &mut expansion_context,
                0,
            )?);
        }
        intervals += 1;
        index += 3;
    }
    Ok(rendered.join(" "))
}

fn render_cl_record_tail_expansion(
    interp: &mut Interpreter,
    fields: &[Value],
    start: usize,
    env: &mut Env,
    context: &PrintContext,
) -> Result<String, LispError> {
    let mut expansion_context = context.clone();
    expansion_context.first_ellipsis_expansion = None;
    let slot_limit = context.options.length.unwrap_or(usize::MAX);
    let mut rendered = Vec::new();
    let mut slots = 0usize;
    let mut index = start;
    while index < fields.len() {
        if slots >= slot_limit {
            rendered.push("...".into());
            break;
        }
        rendered.push(render_prin1_with_context(
            interp,
            &fields[index],
            env,
            &mut expansion_context,
            0,
        )?);
        if let Some(value) = fields.get(index + 1) {
            rendered.push(render_prin1_with_context(
                interp,
                value,
                env,
                &mut expansion_context,
                0,
            )?);
        }
        slots += 1;
        index += 2;
    }
    Ok(rendered.join(" "))
}

fn render_cl_string_literal(
    interp: &Interpreter,
    text: &str,
    env: &Env,
    context: &mut PrintContext,
) -> String {
    if context.options.dialect == PrintDialect::Cl
        && let Some(limit) = context.options.string_length
    {
        let len = text.chars().count();
        if len > limit {
            let prefix = text.chars().take(limit).collect::<String>();
            let tail = text.chars().skip(limit).collect::<String>();
            let expansion = if tail.chars().count() > limit {
                format!("{}...", tail.chars().take(limit).collect::<String>())
            } else {
                tail
            };
            context.record_ellipsis_expansion(expansion);
            return render_prin1_string(interp, &format!("{prefix}..."), env);
        }
    }
    render_prin1_string(interp, text, env)
}

pub(crate) fn render_prin1_list(
    interp: &mut Interpreter,
    value: &Value,
    env: &mut crate::lisp::types::Env,
    context: &mut PrintContext,
    depth: usize,
) -> Result<String, LispError> {
    let Some((car, cdr)) = value.cons_values() else {
        return Ok(value.to_string());
    };
    if context.options.level.is_some_and(|limit| depth >= limit) {
        return Ok("...".into());
    }
    if context.options.length == Some(0) {
        return Ok("(...)".into());
    }

    let mut rendered = vec![render_prin1_with_context(
        interp,
        &car,
        env,
        context,
        depth + 1,
    )?];
    let mut tail_positions = HashMap::new();
    if !context.options.circle
        && let Some(key) = print_ref_key(interp, value, context.options)
    {
        tail_positions.insert(key, 0usize);
    }
    let mut tail = cdr;
    loop {
        if is_vector_value(&tail) {
            let tail_rendered = render_prin1_with_context(interp, &tail, env, context, depth + 1)?;
            return Ok(format!("({} . {})", rendered.join(" "), tail_rendered));
        }
        match tail {
            Value::Nil => return Ok(format!("({})", rendered.join(" "))),
            Value::Cons(_) => {
                if context
                    .options
                    .length
                    .is_some_and(|limit| rendered.len() >= limit)
                {
                    if context.options.dialect == PrintDialect::Cl {
                        let expansion = render_cl_list_tail_expansion(interp, &tail, env, context)?;
                        context.record_ellipsis_expansion(expansion);
                    }
                    rendered.push("...".into());
                    return Ok(format!("({})", rendered.join(" ")));
                }
                if let Some(key) = print_ref_key(interp, &tail, context.options) {
                    if should_label_value(&tail, &key, context) {
                        let tail_rendered =
                            render_prin1_with_context(interp, &tail, env, context, depth + 1)?;
                        return Ok(format!("({} . {})", rendered.join(" "), tail_rendered));
                    }
                    if !context.options.circle
                        && let Some(loopback_index) = tail_positions.get(&key)
                    {
                        return Ok(format!(
                            "({} . {})",
                            rendered.join(" "),
                            format_args!("#{loopback_index}")
                        ));
                    }
                    if !context.options.circle {
                        tail_positions.insert(key.clone(), rendered.len());
                    }
                }
                let Some((next_car, next_cdr)) = tail.cons_values() else {
                    return Ok(value.to_string());
                };
                rendered.push(render_prin1_with_context(
                    interp,
                    &next_car,
                    env,
                    context,
                    depth + 1,
                )?);
                tail = next_cdr;
            }
            other => {
                let tail_rendered =
                    render_prin1_with_context(interp, &other, env, context, depth + 1)?;
                return Ok(format!("({} . {})", rendered.join(" "), tail_rendered));
            }
        }
    }
}

pub(crate) fn should_label_value(value: &Value, key: &PrintRefKey, context: &PrintContext) -> bool {
    if !context.options.circle {
        return false;
    }
    if context.labels.contains_key(key) {
        return true;
    }
    if context.counts.get(key).copied().unwrap_or(0) > 1 {
        return true;
    }
    context.options.continuous_numbering
        && context.options.gensym
        && matches!(value, Value::Symbol(symbol) if crate::lisp::types::is_uninterned_symbol(symbol))
}

pub(crate) fn render_prin1_with_context(
    interp: &mut Interpreter,
    value: &Value,
    env: &mut crate::lisp::types::Env,
    context: &mut PrintContext,
    depth: usize,
) -> Result<String, LispError> {
    if context.options.circle
        && let Some(rendered) = print_number_table_substitution(interp, value, env)?
    {
        return Ok(rendered);
    }
    if let Some(key) = print_ref_key(interp, value, context.options) {
        if should_label_value(value, &key, context) {
            if let Some(label) = context.labels.get(&key) {
                return Ok(format!("#{label}#"));
            }
            let label = context.next_label;
            context.next_label += 1;
            context.labels.insert(key.clone(), label);
            context.active.insert(key.clone());
            let rendered = render_prin1_body(interp, value, env, context, depth);
            context.active.remove(&key);
            return rendered.map(|body| format!("#{label}={body}"));
        }
        if !context.active.insert(key.clone()) {
            return Ok(print_ref_placeholder(key));
        }
        let rendered = render_prin1_body(interp, value, env, context, depth);
        context.active.remove(&key);
        return rendered;
    }

    render_prin1_body(interp, value, env, context, depth)
}

pub(crate) fn print_number_table_substitution(
    interp: &mut Interpreter,
    value: &Value,
    env: &Env,
) -> Result<Option<String>, LispError> {
    let Some(table) = interp.lookup_var("print-number-table", env) else {
        return Ok(None);
    };
    let Some((_, entries)) = json::hash_table_entries(interp, &table) else {
        return Ok(None);
    };
    for (key, replacement) in entries {
        if key == *value
            && let Some(text) = string_like(&replacement)
        {
            return Ok(Some(text.text));
        }
    }
    Ok(None)
}

pub(crate) fn symbol_name_looks_like_number(name: &str) -> bool {
    let bytes = name.as_bytes();
    let signed = matches!(bytes.first(), Some(b'+' | b'-'));
    let Some(first) = bytes.get(signed as usize).copied() else {
        return false;
    };
    if !first.is_ascii_digit() && first != b'.' {
        return false;
    }
    decimal_number_prefix(name).is_some_and(|prefix| prefix.len() == name.len())
        || crate::lisp::reader::parse_special_float_token(name).is_some()
}

pub(crate) fn render_prin1_integer_as_character(value: &Value) -> Option<String> {
    let code = match value {
        Value::Integer(value) => *value,
        Value::BigInteger(value) => value.to_i64()?,
        _ => return None,
    };
    let codepoint = u32::try_from(code).ok()?;
    let ch = char::from_u32(codepoint)?;
    let body = match ch {
        '?' => "?".into(),
        ' ' => "\\s".into(),
        '\n' => "\\n".into(),
        '\r' => "\\r".into(),
        '\t' => "\\t".into(),
        '\u{0008}' => "\\b".into(),
        '\u{000C}' => "\\f".into(),
        '\'' => "\\'".into(),
        '"' | '\\' | ';' | '(' | ')' | '{' | '}' | '[' | ']' => format!("\\{ch}"),
        _ => {
            if matches!(code, 7 | 11 | 27 | 127) {
                return None;
            }
            match get_general_category(ch).abbreviation() {
                "Cc" | "Cf" | "Cn" | "Co" | "Cs" | "Mc" | "Me" | "Mn" | "Zl" | "Zp" | "Zs" => {
                    return None;
                }
                _ => ch.to_string(),
            }
        }
    };
    Some(format!("?{body}"))
}

pub(crate) fn render_prin1_symbol(symbol: &str, options: PrintOptions) -> String {
    let visible = crate::lisp::types::visible_symbol_name(symbol);
    if visible.is_empty() {
        if options.gensym && crate::lisp::types::is_uninterned_symbol(symbol) {
            return "#:##".into();
        }
        return "##".into();
    }

    let first = visible.chars().next();
    let mut confusing =
        symbol_name_looks_like_number(visible) || first == Some('?') || first == Some('.');

    let mut rendered = String::new();
    if options.gensym && crate::lisp::types::is_uninterned_symbol(symbol) {
        rendered.push_str("#:");
    }
    for ch in visible.chars() {
        if matches!(
            ch,
            '"' | '\\' | '\'' | ';' | '#' | '(' | ')' | ',' | '`' | '[' | ']'
        ) || ch <= ' '
            || ch == '\u{00A0}'
            || confusing
        {
            rendered.push('\\');
            confusing = false;
        }
        rendered.push(ch);
    }
    rendered
}

pub(crate) fn should_render_charset_text_property(
    interp: &Interpreter,
    env: &Env,
    dialect: PrintDialect,
    text: &str,
    span: &StringPropertySpan,
    value: &Value,
) -> bool {
    if dialect == PrintDialect::Cl {
        return true;
    }
    let setting = interp
        .lookup_var("print-charset-text-property", env)
        .unwrap_or(Value::Nil);
    if setting.is_nil() {
        return false;
    }
    if !matches!(&setting, Value::Symbol(symbol) if symbol == "default") {
        return true;
    }

    let Ok(charset) = value.as_symbol() else {
        return true;
    };
    let expected = interp
        .charset_canonical_name(charset)
        .unwrap_or_else(|| charset.to_string());

    text.chars()
        .skip(span.start)
        .take(span.end.saturating_sub(span.start))
        .any(|ch| !ch.is_ascii() && charset_for_char(ch as u32) != expected)
}

pub(crate) fn render_hash_table_prin1(
    interp: &mut Interpreter,
    value: &Value,
    env: &mut Env,
    context: &mut PrintContext,
    depth: usize,
) -> Result<String, LispError> {
    let size = hash_table_metadata_slot(interp, value, 2, Value::Integer(65))?;
    let test = hash_table_metadata_slot(interp, value, 0, Value::Symbol("eql".into()))?;
    let rehash_size = hash_table_metadata_slot(interp, value, 3, Value::Float(1.5))?;
    let rehash_threshold = hash_table_metadata_slot(interp, value, 4, Value::Float(0.8125))?;
    let weakness = hash_table_metadata_slot(interp, value, 5, Value::Nil)?;
    let entries = json::hash_table_entries(interp, value)
        .map(|(_, entries)| entries)
        .unwrap_or_default();
    let mut data_parts = Vec::new();
    for (index, (key, entry_value)) in entries.iter().enumerate() {
        if context.options.length.is_some_and(|limit| index >= limit) {
            data_parts.push("...".into());
            break;
        }
        data_parts.push(render_prin1_with_context(
            interp,
            key,
            env,
            context,
            depth + 1,
        )?);
        data_parts.push(render_prin1_with_context(
            interp,
            entry_value,
            env,
            context,
            depth + 1,
        )?);
    }
    let data_rendered = format!("({})", data_parts.join(" "));

    let mut fields = vec![
        Value::Symbol("hash-table".into()),
        Value::Symbol("size".into()),
        size,
        Value::Symbol("test".into()),
        test,
        Value::Symbol("rehash-size".into()),
        rehash_size,
        Value::Symbol("rehash-threshold".into()),
        rehash_threshold,
    ];
    if !weakness.is_nil() {
        fields.push(Value::Symbol("weakness".into()));
        fields.push(weakness);
    }
    let mut rendered_fields = fields
        .iter()
        .map(|field| render_prin1_with_context(interp, field, env, context, depth + 1))
        .collect::<Result<Vec<_>, _>>()?;
    rendered_fields.push("data".into());
    rendered_fields.push(data_rendered);

    Ok(format!("#s({})", rendered_fields.join(" ")))
}

pub(crate) fn render_prin1_body(
    interp: &mut Interpreter,
    value: &Value,
    env: &mut crate::lisp::types::Env,
    context: &mut PrintContext,
    depth: usize,
) -> Result<String, LispError> {
    let unreadable_override = |interp: &mut Interpreter,
                               value: &Value,
                               env: &mut crate::lisp::types::Env|
     -> Result<Option<String>, LispError> {
        let Some(function) = interp.lookup_var("print-unreadable-function", env) else {
            return Ok(None);
        };
        if !function.is_truthy() {
            return Ok(None);
        }
        let rendered = call_function_value(interp, &function, &[value.clone(), Value::T], env)?;
        if matches!(rendered, Value::T) {
            return Ok(Some(String::new()));
        }
        if rendered.is_nil() {
            return Ok(None);
        }
        Ok(Some(string_text(&rendered)?))
    };

    if context.options.dialect == PrintDialect::Cl
        && context.options.level.is_some_and(|limit| depth >= limit)
    {
        match value {
            Value::StringObject(state) if !state.borrow().props.is_empty() => {
                let expansion = render_cl_ellipsis_object_expansion(interp, value, env, context)?;
                context.record_ellipsis_expansion(expansion);
                return Ok("...".into());
            }
            Value::Cons(_) if is_vector_value(value) => {
                let expansion = render_cl_ellipsis_object_expansion(interp, value, env, context)?;
                context.record_ellipsis_expansion(expansion);
                return Ok("...".into());
            }
            Value::Record(id)
                if record_prin1_fields(interp, *id, context.options.dialect).is_some() =>
            {
                let expansion = render_cl_ellipsis_object_expansion(interp, value, env, context)?;
                context.record_ellipsis_expansion(expansion);
                return Ok("...".into());
            }
            Value::Cons(_) => {
                let expansion = render_cl_ellipsis_object_expansion(interp, value, env, context)?;
                context.record_ellipsis_expansion(expansion);
                return Ok("...".into());
            }
            _ => {}
        }
    }

    if context.options.quoted
        && let Ok(items) = value.to_vec()
    {
        let quoted = match items.as_slice() {
            [Value::Symbol(symbol), inner] if symbol == "quote" => Some(("'", inner)),
            [Value::Symbol(symbol), inner]
                if symbol == "function" || symbol == "function-quote" =>
            {
                Some(("#'", inner))
            }
            [Value::Symbol(symbol), inner] if symbol == "backquote" || symbol == "`" => {
                Some(("`", inner))
            }
            [Value::Symbol(symbol), inner] if symbol == "comma" || symbol == "," => {
                Some((",", inner))
            }
            [Value::Symbol(symbol), inner] if symbol == "comma-at" || symbol == ",@" => {
                Some((",@", inner))
            }
            _ => None,
        };
        if let Some((prefix, inner)) = quoted {
            // GNU's print-quoted syntax replaces the (quote INNER) wrapper;
            // it does not charge that elided cons level against print-level.
            // Passing depth + 1 here truncates one level too early (for
            // example, print-level 1 would render '(a) as '...).
            return Ok(format!(
                "{prefix}{}",
                render_prin1_with_context(interp, inner, env, context, depth)?
            ));
        }
    }

    match value {
        Value::Integer(_) | Value::BigInteger(_) if context.options.integers_as_characters => {
            if let Some(rendered) = render_prin1_integer_as_character(value) {
                return Ok(rendered);
            }
            Ok(value.to_string())
        }
        Value::String(text) => Ok(render_cl_string_literal(interp, text, env, context)),
        Value::StringObject(state) => {
            let (text, props) = {
                let state = state.borrow();
                (state.text.clone(), state.props.clone())
            };
            if props.is_empty() {
                return Ok(render_cl_string_literal(interp, &text, env, context));
            }
            let mut rendered = vec![render_cl_string_literal(interp, &text, env, context)];
            let mut field_values = Vec::new();
            for span in props {
                let filtered_props = span
                    .props
                    .iter()
                    .filter(|(name, value)| {
                        name != "charset"
                            || should_render_charset_text_property(
                                interp,
                                env,
                                context.options.dialect,
                                &text,
                                &span,
                                value,
                            )
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                if filtered_props.is_empty() {
                    continue;
                }
                field_values.push(Value::Integer(span.start as i64));
                field_values.push(Value::Integer(span.end as i64));
                field_values.push(plist_value(&filtered_props));
            }
            for (index, field) in field_values.iter().enumerate() {
                if context
                    .options
                    .length
                    .is_some_and(|limit| rendered.len() >= limit)
                {
                    if context.options.dialect == PrintDialect::Cl {
                        let expansion = render_cl_string_property_tail_expansion(
                            interp,
                            &field_values,
                            index,
                            env,
                            context,
                        )?;
                        context.record_ellipsis_expansion(expansion);
                    }
                    rendered.push("...".into());
                    break;
                }
                rendered.push(render_prin1_with_context(
                    interp,
                    field,
                    env,
                    context,
                    depth + 1,
                )?);
            }
            if rendered.len() == 1 {
                return Ok(render_cl_string_literal(interp, &text, env, context));
            }
            Ok(format!("#({})", rendered.join(" ")))
        }
        Value::Symbol(symbol) if symbol == "backquote" || symbol == "`" => Ok("\\`".into()),
        Value::Symbol(symbol) if symbol == "comma" || symbol == "," => Ok("\\,".into()),
        Value::Symbol(symbol) if symbol == "comma-at" || symbol == ",@" => Ok("\\,@".into()),
        Value::Symbol(symbol) => Ok(render_prin1_symbol(symbol, context.options)),
        Value::Cons(_) if is_vector_value(value) => {
            let items = vector_items(value)?;
            let mut rendered_items = Vec::new();
            for (index, item) in items.iter().enumerate() {
                if context.options.length.is_some_and(|limit| index >= limit) {
                    if context.options.dialect == PrintDialect::Cl {
                        let expansion =
                            render_cl_vector_tail_expansion(interp, &items, index, env, context)?;
                        context.record_ellipsis_expansion(expansion);
                    }
                    rendered_items.push("...".into());
                    break;
                }
                rendered_items.push(render_prin1_with_context(
                    interp,
                    item,
                    env,
                    context,
                    depth + 1,
                )?);
            }
            Ok(format!("[{}]", rendered_items.join(" ")))
        }
        Value::Cons(_) => render_prin1_list(interp, value, env, context, depth),
        Value::Lambda(lambda_value) => {
            let params = &lambda_value.params;
            let body = &lambda_value.body;
            let closure_env = &lambda_value.env;
            if let Some(rendered) = unreadable_override(interp, value, env)? {
                return Ok(rendered);
            }
            // cl-prin1 dispatches oclosures through the `cl-print-object'
            // generic (nadvice prints advice objects as "#f(advice ...)").
            if context.options.dialect == PrintDialect::Cl
                && crate::lisp::primitives::dispatch::oclosure_type_of(value).is_some()
                && interp.has_lisp_function("cl-print-object")
            {
                let form = Value::list([
                    Value::Symbol("with-output-to-string".into()),
                    Value::list([
                        Value::Symbol("cl-print-object".into()),
                        Value::list([Value::Symbol("quote".into()), value.clone()]),
                        Value::Symbol("standard-output".into()),
                    ]),
                ]);
                if let Ok(rendered) = interp.eval(&form, env) {
                    return string_text(&rendered);
                }
            }
            let captured = closure_env.borrow().clone();
            let prints_closure_env = body.len() > 1
                && matches!(
                    body.first(),
                    Some(Value::Symbol(marker)) if marker == ":closure-dont-trim-context"
                );
            if captured.is_empty() || !prints_closure_env {
                return Ok(value.to_string());
            }
            let params_value = Value::list(
                params
                    .iter()
                    .cloned()
                    .map(|value| Value::Symbol(value.into())),
            );
            let env_value = closure_env_print_value(&captured);
            Ok(format!(
                "#<closure {} {}>",
                render_prin1_with_context(interp, &params_value, env, context, depth + 1)?,
                render_prin1_with_context(interp, &env_value, env, context, depth + 1)?
            ))
        }
        Value::BuiltinFunc(_)
        | Value::Buffer(_)
        | Value::Marker(_)
        | Value::Overlay(_)
        | Value::CharTable(_) => {
            if let Some(rendered) = unreadable_override(interp, value, env)? {
                return Ok(rendered);
            }
            match value {
                Value::Marker(id) => {
                    if let Some(marker) = interp.find_marker(*id) {
                        return Ok(match marker.buffer_id {
                            Some(buffer_id) => {
                                let buffer_name = interp
                                    .get_buffer_by_id(buffer_id)
                                    .map(|buffer| buffer.name.clone())
                                    .unwrap_or_else(|| format!("buffer<{buffer_id}>"));
                                match marker.position {
                                    Some(position) => {
                                        format!("#<marker at {position} in {buffer_name}>")
                                    }
                                    None => format!("#<marker in {buffer_name}>"),
                                }
                            }
                            None => "#<marker in no buffer>".into(),
                        });
                    }
                    Ok(value.to_string())
                }
                _ => Ok(value.to_string()),
            }
        }
        Value::Frame(id) => {
            let name = interp
                .frame_state(*id)
                .map(|frame| string_text(&frame.name).unwrap_or_else(|_| format!("F{id}")))
                .unwrap_or_else(|| format!("F{id}"));
            Ok(format!("#<frame {name} 0x{id:x}>"))
        }
        Value::Terminal(id) => Ok(format!("#<terminal {id} on initial_terminal>")),
        Value::Record(id) => {
            if let Some(record) = interp.find_record(*id) {
                let rendered = match record.type_name.as_str() {
                    "thread" => interp
                        .thread_name(*id)
                        .map(|name| format!("#<thread {name}>"))
                        .unwrap_or_else(|| format!("#<thread id:{id}>")),
                    "mutex" => interp
                        .mutex_name(*id)
                        .map(|name| format!("#<mutex {name}>"))
                        .unwrap_or_else(|| "#<mutex>".into()),
                    "condition-variable" => interp
                        .condition_variable_name(*id)
                        .map(|name| format!("#<condvar {name}>"))
                        .unwrap_or_else(|| "#<condvar>".into()),
                    "hash-table" => render_hash_table_prin1(interp, value, env, context, depth)?,
                    "process" | "obarray" => value.to_string(),
                    _ => {
                        let Some(mut fields) =
                            record_prin1_fields(interp, *id, context.options.dialect)
                        else {
                            return Ok(value.to_string());
                        };
                        // Records tagged with their class OBJECT (GNU eieio
                        // objects created with `eieio-backward-compatibility'
                        // nil, and every class's default-object cache) print
                        // the class expanded in place of the type symbol;
                        // the cache inside the class then hits the active-set
                        // guard and prints as a circular `#N' marker.
                        if context.options.dialect == PrintDialect::Emacs
                            && interp.is_class_object_tagged_record(*id)
                            && let Some(class_record) = interp
                                .find_record(*id)
                                .map(|record| record.type_name.clone())
                                .and_then(|type_name| interp.class_value(&type_name))
                        {
                            fields[0] = class_record;
                        }
                        let mut rendered_fields = Vec::new();
                        if context.options.dialect == PrintDialect::Cl
                            && fields.len() > 1
                            && fields.len() % 2 == 1
                        {
                            rendered_fields.push(render_prin1_with_context(
                                interp,
                                &fields[0],
                                env,
                                context,
                                depth + 1,
                            )?);
                            let mut slot_index = 0usize;
                            let mut field_index = 1usize;
                            while field_index < fields.len() {
                                if context
                                    .options
                                    .length
                                    .is_some_and(|limit| slot_index >= limit)
                                {
                                    let expansion = render_cl_record_tail_expansion(
                                        interp,
                                        &fields,
                                        field_index,
                                        env,
                                        context,
                                    )?;
                                    context.record_ellipsis_expansion(expansion);
                                    rendered_fields.push("...".into());
                                    break;
                                }
                                rendered_fields.push(render_prin1_with_context(
                                    interp,
                                    &fields[field_index],
                                    env,
                                    context,
                                    depth + 1,
                                )?);
                                if let Some(field_value) = fields.get(field_index + 1) {
                                    rendered_fields.push(render_prin1_with_context(
                                        interp,
                                        field_value,
                                        env,
                                        context,
                                        depth + 1,
                                    )?);
                                }
                                slot_index += 1;
                                field_index += 2;
                            }
                        } else {
                            rendered_fields = fields
                                .iter()
                                .map(|field| {
                                    render_prin1_with_context(
                                        interp,
                                        field,
                                        env,
                                        context,
                                        depth + 1,
                                    )
                                })
                                .collect::<Result<Vec<_>, _>>()?;
                        }
                        format!("#s({})", rendered_fields.join(" "))
                    }
                };
                return Ok(rendered);
            }
            Ok(value.to_string())
        }
        _ => Ok(value.to_string()),
    }
}

fn closure_env_print_value(env: &Env) -> Value {
    Value::list(env.iter().map(|frame| {
        Value::list(
            frame.iter().map(|(name, value)| {
                Value::cons(Value::Symbol(name.clone().into()), value.clone())
            }),
        )
    }))
}

pub(crate) fn finish_print_number_table(env: &mut Env, context: &PrintContext) {
    if !context.options.circle || !context.options.continuous_numbering {
        return;
    }
    set_env_binding(
        env,
        "print-number-table",
        print_number_table_value(&context.labels, context.next_label),
    );
}

pub(crate) fn render_prin1(
    interp: &mut Interpreter,
    value: &Value,
    env: &mut crate::lisp::types::Env,
) -> Result<String, LispError> {
    let mut context = PrintContext::new(
        interp,
        value,
        env,
        print_options(interp, env, PrintDialect::Emacs),
    )?;
    let rendered = render_prin1_with_context(interp, value, env, &mut context, 0)?;
    finish_print_number_table(env, &context);
    Ok(rendered)
}

pub(crate) fn render_cl_prin1(
    interp: &mut Interpreter,
    value: &Value,
    env: &mut crate::lisp::types::Env,
) -> Result<String, LispError> {
    let mut context = PrintContext::new(
        interp,
        value,
        env,
        print_options(interp, env, PrintDialect::Cl),
    )?;
    let rendered = render_prin1_with_context(interp, value, env, &mut context, 0)?;
    finish_print_number_table(env, &context);
    Ok(rendered)
}

pub(crate) fn render_cl_prin1_value(
    interp: &mut Interpreter,
    value: &Value,
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    let mut context = PrintContext::new(
        interp,
        value,
        env,
        print_options(interp, env, PrintDialect::Cl),
    )?;
    let rendered = render_prin1_with_context(interp, value, env, &mut context, 0)?;
    finish_print_number_table(env, &context);
    let Some(expansion) = context.first_ellipsis_expansion else {
        return Ok(Value::String(rendered.into()));
    };
    let Some(start) = rendered
        .find("...")
        .map(|byte| rendered[..byte].chars().count())
    else {
        return Ok(Value::String(rendered.into()));
    };
    Ok(string_like_value(
        rendered,
        vec![TextPropertySpan {
            start,
            end: start + 3,
            props: vec![(
                "cl-print-ellipsis".into(),
                Value::list([
                    Value::Symbol("emaxx-cl-print-ellipsis".into()),
                    Value::String(expansion.into()),
                ]),
            )],
        }],
    ))
}

pub(crate) fn render_prin1_ephemeral(
    interp: &mut Interpreter,
    value: &Value,
    env: &crate::lisp::types::Env,
) -> Result<String, LispError> {
    let mut env = env.clone();
    render_prin1(interp, value, &mut env)
}

pub(crate) fn read_one_form_in_env(
    interp: &mut Interpreter,
    text: &str,
    env: &mut Env,
) -> Result<(Value, usize), LispError> {
    let symbol_shorthands = read_symbol_shorthands_in_env(interp, env)?;
    let mut reader = crate::lisp::reader::Reader::with_symbol_shorthands(text, symbol_shorthands);
    let value = match reader.read()? {
        Some(value) => crate::lisp::reader::resolve_circular_read_syntax(value)?,
        None => return Err(LispError::EndOfInput),
    };
    interp.set_variable(
        "lread--unescaped-character-literals",
        Value::list(reader.unescaped_character_literals().map(Value::Integer)),
        env,
    );
    let consumed = text[..reader.position()].chars().count();
    Ok((value, consumed))
}

pub(crate) fn read_positioning_symbols_from_lisp_source(
    interp: &mut Interpreter,
    source: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    match source {
        Value::Buffer(_) => {
            let buffer_id = interp.resolve_buffer_id(source)?;
            let (start, end, text) = {
                let buffer = interp
                    .get_buffer_by_id(buffer_id)
                    .ok_or_else(|| LispError::Signal(format!("No buffer with id {buffer_id}")))?;
                let start = buffer.point();
                let end = buffer.point_max();
                (
                    start,
                    end,
                    buffer
                        .buffer_substring(start, end)
                        .map_err(|error| LispError::Signal(error.to_string()))?,
                )
            };
            let (value, consumed) = read_one_positioned_form(interp, env, &text, start as i64)?;
            if let Some(buffer) = interp.get_buffer_by_id_mut(buffer_id) {
                buffer.goto_char((start + consumed).min(end));
            }
            Ok(value)
        }
        Value::Marker(id) => {
            let (buffer_id, start) = {
                let marker = interp.find_marker(*id).ok_or_else(|| {
                    LispError::TypeError("marker".into(), format!("marker<{id}>"))
                })?;
                let buffer_id = marker
                    .buffer_id
                    .ok_or_else(|| LispError::Signal("Marker does not point anywhere".into()))?;
                let start = marker
                    .position
                    .ok_or_else(|| LispError::Signal("Marker does not point anywhere".into()))?;
                (buffer_id, start)
            };
            let end = interp
                .get_buffer_by_id(buffer_id)
                .ok_or_else(|| LispError::Signal(format!("No buffer with id {buffer_id}")))?
                .point_max();
            let text = interp
                .get_buffer_by_id(buffer_id)
                .ok_or_else(|| LispError::Signal(format!("No buffer with id {buffer_id}")))?
                .buffer_substring(start, end)
                .map_err(|error| LispError::Signal(error.to_string()))?;
            let (value, consumed) = read_one_positioned_form(interp, env, &text, start as i64)?;
            interp.set_marker(*id, Some((start + consumed).min(end)), Some(buffer_id))?;
            Ok(value)
        }
        Value::BuiltinFunc(_) | Value::Lambda(_) => {
            let value = read_from_callable_source(interp, source, env)?;
            Ok(position_symbols_in_value(
                interp,
                value,
                &mut VecDeque::new(),
            ))
        }
        Value::Symbol(symbol) if interp.lookup_function(symbol, env).is_ok() => {
            let value = read_from_callable_source(interp, source, env)?;
            Ok(position_symbols_in_value(
                interp,
                value,
                &mut VecDeque::new(),
            ))
        }
        _ => {
            let text = string_text(source)?;
            read_one_positioned_form(interp, env, &text, 0).map(|(value, _)| value)
        }
    }
}

fn read_one_positioned_form(
    interp: &mut Interpreter,
    env: &mut Env,
    text: &str,
    base_position: i64,
) -> Result<(Value, usize), LispError> {
    let (value, consumed) = read_one_form_in_env(interp, text, env)?;
    let mut tokens = symbol_tokens_with_positions(text, base_position);
    Ok((
        position_symbols_in_value(interp, value, &mut tokens),
        consumed,
    ))
}

fn position_symbols_in_value(
    interp: &mut Interpreter,
    value: Value,
    tokens: &mut VecDeque<(String, i64)>,
) -> Value {
    match value {
        Value::Symbol(symbol) => {
            if matches!(tokens.front(), Some((token, _)) if token == &symbol)
                && let Some((_, position)) = tokens.pop_front()
            {
                interp.create_record(
                    "symbol-with-pos",
                    vec![Value::Symbol(symbol), Value::Integer(position)],
                )
            } else {
                Value::Symbol(symbol)
            }
        }
        Value::Cons(cons_cell) => {
            let car = &cons_cell.car;
            let cdr = &cons_cell.cdr;
            let positioned_car = position_symbols_in_value(interp, car.borrow().clone(), tokens);
            let positioned_cdr = position_symbols_in_value(interp, cdr.borrow().clone(), tokens);
            Value::cons(positioned_car, positioned_cdr)
        }
        other => other,
    }
}

fn symbol_tokens_with_positions(text: &str, base_position: i64) -> VecDeque<(String, i64)> {
    let chars: Vec<(usize, char)> = text.char_indices().collect();
    let mut tokens = VecDeque::new();
    let mut idx = 0usize;
    while idx < chars.len() {
        let (_, ch) = chars[idx];
        if ch.is_whitespace() {
            idx += 1;
            continue;
        }
        if ch == ';' {
            idx += 1;
            while idx < chars.len() && chars[idx].1 != '\n' {
                idx += 1;
            }
            continue;
        }
        if ch == '"' {
            idx += 1;
            let mut escaped = false;
            while idx < chars.len() {
                let current = chars[idx].1;
                idx += 1;
                if escaped {
                    escaped = false;
                } else if current == '\\' {
                    escaped = true;
                } else if current == '"' {
                    break;
                }
            }
            continue;
        }
        if is_reader_delimiter(ch) {
            idx += 1;
            continue;
        }

        let start = idx;
        let start_char_pos = text[..chars[start].0].chars().count() as i64;
        let mut token = String::new();
        while idx < chars.len() {
            let current = chars[idx].1;
            if current.is_whitespace() || is_reader_delimiter(current) || current == ';' {
                break;
            }
            if current == '\\' && idx + 1 < chars.len() {
                idx += 1;
                token.push(chars[idx].1);
                idx += 1;
                continue;
            }
            token.push(current);
            idx += 1;
        }
        if !token.is_empty() && token != "." {
            tokens.push_back((token, base_position + start_char_pos));
        }
    }
    tokens
}

fn is_reader_delimiter(ch: char) -> bool {
    matches!(ch, '(' | ')' | '[' | ']' | '\'' | '`' | ',' | '"' | '#')
}

pub(crate) fn record_literal_items(value: &Value) -> Option<Vec<Value>> {
    // Runtime vectors and record literals are both represented by tagged
    // conses for now.  Reject every other tag before materializing the
    // proper list: callers use this as a type predicate on hot paths such as
    // `aset', where walking a vector for every element would turn a fill
    // loop into quadratic work.
    let (car, _) = value.cons_cells()?;
    if !matches!(
        &*car.borrow(),
        Value::Symbol(name) if name == crate::lisp::reader::RECORD_LITERAL_SYMBOL
    ) {
        return None;
    }
    let items = value.to_vec().ok()?;
    Some(items)
}

pub(crate) fn record_literal_slot_data(value: &Value) -> Value {
    if let Ok(items) = value.to_vec()
        && let [Value::Symbol(symbol), inner] = items.as_slice()
        && symbol == "quote"
    {
        return inner.clone();
    }
    value.clone()
}

pub(crate) fn record_literal_aref(
    object: &Value,
    items: &[Value],
    idx: usize,
    idx_value: &Value,
) -> Result<Value, LispError> {
    let slot = items.get(idx + 1).cloned().ok_or_else(|| {
        LispError::SignalValue(Value::list([
            Value::Symbol("args-out-of-range".into()),
            object.clone(),
            idx_value.clone(),
        ]))
    })?;
    Ok(record_literal_slot_data(&slot))
}

pub(crate) fn read_from_callable_source(
    interp: &mut Interpreter,
    source: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    let callable = resolve_callable(interp, source, env)?;
    let original_name = source.as_symbol().ok();
    let mut text = String::new();
    loop {
        let next = interp.call_function_value(callable.clone(), original_name, &[], env)?;
        let Some(code) = (match next {
            Value::Integer(code) => Some(code),
            Value::Nil => None,
            other => {
                return Err(LispError::TypeError("integer".into(), other.type_name()));
            }
        }) else {
            break;
        };
        if code < 0 {
            break;
        }
        let Some(ch) = char::from_u32(code as u32) else {
            return Err(LispError::Signal("Invalid character".into()));
        };
        text.push(ch);
    }
    read_one_form_in_env(interp, &text, env).map(|(value, _)| value)
}

pub(crate) fn read_from_lisp_source(
    interp: &mut Interpreter,
    source: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    let value = read_from_lisp_source_raw(interp, source, env)?;
    interp.intern_symbols_in_value(&value);
    let value = interp.materialize_read_record_literals(&value)?;
    let value = materialize_read_hash_table_literals(interp, &value)?;
    materialize_read_char_table_literals(interp, &value)
}

// GNU's reader constructs real hash tables for `#s(hash-table ...)' input;
// emaxx's reader leaves a quoted literal form behind, which is fine for
// loaded code but wrong for `read' consumers that treat the result as data
// (e.g. `eieio-persistent-read').  Convert those literals into hash-table
// records after reading.
pub(crate) fn materialize_read_hash_table_literals(
    interp: &mut Interpreter,
    value: &Value,
) -> Result<Value, LispError> {
    let mut seen = HashSet::new();
    materialize_hash_table_literals_inner(interp, value, &mut seen)
}

const CHAR_TABLE_STANDARD_SLOTS: usize = 68;
const MAX_CHAR: u32 = 0x3f_ffff;

// GNU's reader turns `#^[...]' and nested `#^^[...]' syntax directly into
// character-table objects.  Emaxx's parser is deliberately independent of
// the interpreter, so it leaves private marker forms and materializes them at
// the read/evaluation boundary.  The serialized trie is flattened into the
// runtime's non-overlapping range representation once, at construction time.
pub(crate) fn materialize_read_char_table_literals(
    interp: &mut Interpreter,
    value: &Value,
) -> Result<Value, LispError> {
    let mut seen = HashSet::new();
    materialize_char_table_literals_inner(interp, value, &mut seen)
}

fn char_table_literal_fields(value: &Value) -> Option<Vec<Value>> {
    let items = value.to_vec().ok()?;
    match items.split_first() {
        Some((Value::Symbol(symbol), fields))
            if symbol == crate::lisp::reader::CHAR_TABLE_LITERAL_SYMBOL =>
        {
            Some(fields.to_vec())
        }
        _ => None,
    }
}

fn sub_char_table_literal_fields(value: &Value) -> Option<Vec<Value>> {
    let items = value.to_vec().ok()?;
    match items.split_first() {
        Some((Value::Symbol(symbol), fields))
            if symbol == crate::lisp::reader::SUB_CHAR_TABLE_LITERAL_SYMBOL =>
        {
            Some(fields.to_vec())
        }
        _ => None,
    }
}

fn materialize_char_table_literals_inner(
    interp: &mut Interpreter,
    value: &Value,
    seen: &mut HashSet<usize>,
) -> Result<Value, LispError> {
    let Some((car_cell, cdr_cell)) = (value).cons_cells() else {
        return Ok(value.clone());
    };
    if let Some(fields) = char_table_literal_fields(value) {
        return char_table_from_literal_fields(interp, &fields, seen);
    }
    let ptr = car_cell.cell_id();
    if !seen.insert(ptr) {
        return Ok(value.clone());
    }
    let car = car_cell.borrow().clone();
    *car_cell.borrow_mut() = materialize_char_table_literals_inner(interp, &car, seen)?;
    let cdr = cdr_cell.borrow().clone();
    *cdr_cell.borrow_mut() = materialize_char_table_literals_inner(interp, &cdr, seen)?;
    Ok(value.clone())
}

fn invalid_char_table_literal(message: &str) -> LispError {
    LispError::ReadError(message.into())
}

fn char_table_from_literal_fields(
    interp: &mut Interpreter,
    fields: &[Value],
    seen: &mut HashSet<usize>,
) -> Result<Value, LispError> {
    if fields.len() < CHAR_TABLE_STANDARD_SLOTS {
        return Err(invalid_char_table_literal("invalid size char-table"));
    }

    let default = materialize_literal_value(interp, &fields[0], seen)?;
    let parent = materialize_literal_value(interp, &fields[1], seen)?;
    let subtype = match &fields[2] {
        Value::Nil => None,
        Value::T => Some("t".into()),
        Value::Symbol(symbol) => Some(symbol.to_string()),
        _ => None,
    };
    let uncompress_property_values = subtype.as_deref() == Some("char-code-property-table");
    let decomposition_words = fields
        .get(CHAR_TABLE_STANDARD_SLOTS)
        .and_then(|value| value.as_symbol().ok())
        .filter(|property| *property == "decomposition")
        .and_then(|_| fields.get(CHAR_TABLE_STANDARD_SLOTS + 4))
        .and_then(literal_vector_values);
    let table = interp.make_char_table(subtype, default);
    let Value::CharTable(id) = table else {
        unreachable!("make_char_table always returns a character table")
    };

    let mut entries = Vec::new();
    {
        let mut flatten_context = CharTableFlattenContext {
            entries: &mut entries,
            seen,
            uncompress_property_values,
            decomposition_words: decomposition_words.as_deref(),
        };
        for (index, value) in fields[4..CHAR_TABLE_STANDARD_SLOTS].iter().enumerate() {
            let start = (index as u32) << 16;
            let end = start + 0xffff;
            // GNU consults the dedicated ASCII slot for 0..127 and never
            // falls through to root slot zero.
            flatten_char_table_value(interp, value, start.max(128), end, &mut flatten_context)?;
        }
        flatten_char_table_value(interp, &fields[3], 0, 127, &mut flatten_context)?;
    }

    let extra_slots = fields[CHAR_TABLE_STANDARD_SLOTS..]
        .iter()
        .map(|value| materialize_literal_value(interp, value, seen))
        .collect::<Result<Vec<_>, _>>()?;
    let state = interp
        .find_char_table_mut(id)
        .expect("new character table must exist");
    state.parent = match parent {
        Value::CharTable(parent_id) => Some(parent_id),
        _ => None,
    };
    state.replace_entries(entries);
    state.extra_slots = extra_slots;
    Ok(table)
}

struct CharTableFlattenContext<'a> {
    entries: &'a mut Vec<crate::lisp::eval::CharTableEntry>,
    seen: &'a mut HashSet<usize>,
    uncompress_property_values: bool,
    decomposition_words: Option<&'a [Value]>,
}

fn flatten_char_table_value(
    interp: &mut Interpreter,
    value: &Value,
    allowed_start: u32,
    allowed_end: u32,
    context: &mut CharTableFlattenContext<'_>,
) -> Result<(), LispError> {
    if allowed_start > allowed_end || allowed_start > MAX_CHAR {
        return Ok(());
    }
    if let Some(fields) = sub_char_table_literal_fields(value) {
        let [
            Value::Integer(depth @ 1..=3),
            Value::Integer(min_char),
            contents @ ..,
        ] = fields.as_slice()
        else {
            return Err(invalid_char_table_literal("invalid sub-char-table header"));
        };
        let (expected, width) = match depth {
            1 => (16, 4096),
            2 => (32, 128),
            3 => (128, 1),
            _ => unreachable!("validated sub-char-table depth"),
        };
        if contents.len() != expected || !(0..=i64::from(MAX_CHAR)).contains(min_char) {
            return Err(invalid_char_table_literal(
                "invalid size or minimum in sub-char-table",
            ));
        }
        let min_char = *min_char as u32;
        for (index, content) in contents.iter().enumerate() {
            let start = min_char.saturating_add(index as u32 * width);
            let end = start.saturating_add(width - 1).min(MAX_CHAR);
            flatten_char_table_value(
                interp,
                content,
                start.max(allowed_start),
                end.min(allowed_end),
                context,
            )?;
        }
        return Ok(());
    }

    if context.uncompress_property_values
        && let Some(values) = uncompress_char_property_values(value, context.decomposition_words)?
    {
        for (offset, value) in values.into_iter().enumerate() {
            if let Some(value) = value {
                let character = allowed_start.saturating_add(offset as u32);
                if character <= allowed_end && character <= MAX_CHAR {
                    append_char_table_range(context.entries, character, character, value);
                }
            }
        }
        return Ok(());
    }

    let value = materialize_literal_value(interp, value, context.seen)?;
    if value.is_nil() {
        return Ok(());
    }
    append_char_table_range(
        context.entries,
        allowed_start,
        allowed_end.min(MAX_CHAR),
        value,
    );
    Ok(())
}

fn literal_vector_values(value: &Value) -> Option<Vec<Value>> {
    let items = value.to_vec().ok()?;
    match items.split_first() {
        Some((Value::Symbol(marker), values)) if marker == "vector-literal" => {
            Some(values.to_vec())
        }
        _ => Some(items),
    }
}

fn uncompress_char_property_values(
    value: &Value,
    decomposition_words: Option<&[Value]>,
) -> Result<Option<Vec<Option<Value>>>, LispError> {
    let text = match value {
        Value::String(_) | Value::StringObject(_) => string_text(value)?,
        _ => return Ok(None),
    };
    let mut chars = text.chars().map(u32::from).peekable();
    let Some(format) = chars.next() else {
        return Ok(None);
    };
    if format == 0
        && let Some(words) = decomposition_words
    {
        return Ok(Some(uncompress_decomposition_values(chars, words)?));
    }
    if !matches!(format, 1 | 2) {
        return Ok(None);
    }
    let mut values = Vec::with_capacity(128);
    if format == 1 {
        let start = chars
            .next()
            .ok_or_else(|| invalid_char_table_literal("truncated simple Unicode property table"))?
            as usize;
        values.resize(start.min(128), None);
        for value in chars.take(128usize.saturating_sub(values.len())) {
            values.push((value != 0).then_some(Value::Integer(value as i64)));
        }
    } else {
        while values.len() < 128 {
            let Some(value) = chars.next() else {
                break;
            };
            let count = match chars.peek().copied() {
                Some(encoded) if encoded >= 128 => {
                    chars.next();
                    (encoded - 128) as usize
                }
                _ => 1,
            };
            values.extend(
                std::iter::repeat_n(Some(Value::Integer(value as i64)), count)
                    .take(128 - values.len()),
            );
        }
    }
    values.resize(128, None);
    Ok(Some(values))
}

// Generated decomposition tables use the word-list delta format implemented
// by GNU unidata-get-decomposition.  Decode a whole 128-character leaf once
// while reading the table, so normal property lookup remains an O(log n)
// char-table operation and never needs the generated byte-code decoder.
fn uncompress_decomposition_values(
    chars: impl Iterator<Item = u32>,
    words: &[Value],
) -> Result<Vec<Option<Value>>, LispError> {
    let encoded = chars.collect::<Vec<_>>();
    let mut values = vec![None; 128];
    let mut index = 0usize;
    let mut position = 0usize;
    let mut difference_head = 0usize;
    let mut previous = Vec::<Value>::new();
    let mut head = Vec::<Value>::new();
    let mut tail = Vec::<Value>::new();

    while position < encoded.len() && index < values.len() {
        let code = encoded[position];
        position += 1;
        if code < 3 {
            if !head.is_empty() || !tail.is_empty() {
                head.append(&mut tail);
                previous.clone_from(&head);
                values[index] = Some(Value::list(head.drain(..)));
            }
            index += 1;
            if code == 0 {
                continue;
            }
            if code == 1 {
                difference_head = usize::try_from(*encoded.get(position).ok_or_else(|| {
                    invalid_char_table_literal("truncated decomposition property table")
                })?)
                .map_err(|_| invalid_char_table_literal("invalid decomposition delta"))?;
                position += 1;
            }
            let head_len = difference_head / 16;
            let tail_start = difference_head % 16;
            head.extend(previous.iter().take(head_len).cloned());
            tail.extend(previous.iter().skip(tail_start).cloned());
            continue;
        }

        let word_index = usize::try_from(code - 3)
            .map_err(|_| invalid_char_table_literal("invalid decomposition word index"))?;
        head.push(
            words
                .get(word_index)
                .cloned()
                .unwrap_or(Value::Integer(i64::from(code))),
        );
    }
    if index < values.len() && (!head.is_empty() || !tail.is_empty()) {
        head.extend(tail);
        values[index] = Some(Value::list(head));
    }
    Ok(values)
}

fn materialize_literal_value(
    interp: &mut Interpreter,
    value: &Value,
    seen: &mut HashSet<usize>,
) -> Result<Value, LispError> {
    // Hash-table and character-table traversals maintain independent cycle
    // sets: sharing one would make the second traversal mistake already
    // visited ordinary conses for cycles.
    let value = materialize_read_hash_table_literals(interp, value)?;
    materialize_char_table_literals_inner(interp, &value, seen)
}

fn append_char_table_range(
    entries: &mut Vec<crate::lisp::eval::CharTableEntry>,
    start: u32,
    end: u32,
    value: Value,
) {
    if let Some(previous) = entries.last_mut()
        && previous.end.checked_add(1) == Some(start)
        && char_table_values_share_identity(&previous.value, &value)
    {
        previous.end = end;
        return;
    }
    entries.push(crate::lisp::eval::CharTableEntry { start, end, value });
}

fn char_table_values_share_identity(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::T, Value::T) | (Value::Nil, Value::Nil) | (Value::Unbound, Value::Unbound) => true,
        (Value::Integer(left), Value::Integer(right)) => left == right,
        (Value::Symbol(left), Value::Symbol(right)) => left == right,
        (Value::BuiltinFunc(left), Value::BuiltinFunc(right)) => left == right,
        (Value::Buffer(left), Value::Buffer(right)) => left.id == right.id,
        (Value::Marker(left), Value::Marker(right))
        | (Value::Overlay(left), Value::Overlay(right))
        | (Value::CharTable(left), Value::CharTable(right))
        | (Value::Record(left), Value::Record(right))
        | (Value::Finalizer(left), Value::Finalizer(right)) => left == right,
        _ => false,
    }
}

fn quoted_hash_table_literal_fields(value: &Value) -> Option<Vec<Value>> {
    let items = value.to_vec().ok()?;
    let [Value::Symbol(head), literal] = items.as_slice() else {
        return None;
    };
    if head != "quote" {
        return None;
    }
    let literal_items = literal.to_vec().ok()?;
    match literal_items.split_first() {
        Some((Value::Symbol(symbol), fields))
            if symbol == crate::lisp::json::HASH_TABLE_LITERAL_SYMBOL =>
        {
            Some(fields.to_vec())
        }
        _ => None,
    }
}

fn bare_hash_table_literal_fields(value: &Value) -> Option<Vec<Value>> {
    let items = value.to_vec().ok()?;
    match items.split_first() {
        Some((Value::Symbol(symbol), fields))
            if symbol == crate::lisp::json::HASH_TABLE_LITERAL_SYMBOL =>
        {
            Some(fields.to_vec())
        }
        _ => None,
    }
}

fn materialize_hash_table_literals_inner(
    interp: &mut Interpreter,
    value: &Value,
    seen: &mut HashSet<usize>,
) -> Result<Value, LispError> {
    let Some((car_cell, cdr_cell)) = (value).cons_cells() else {
        return Ok(value.clone());
    };
    if let Some(fields) = quoted_hash_table_literal_fields(value) {
        return hash_table_from_literal_fields(interp, &fields, seen);
    }
    if let Some(fields) = bare_hash_table_literal_fields(value) {
        return hash_table_from_literal_fields(interp, &fields, seen);
    }
    let ptr = car_cell.cell_id();
    if !seen.insert(ptr) {
        return Ok(value.clone());
    }
    let car = car_cell.borrow().clone();
    let new_car = materialize_hash_table_literals_inner(interp, &car, seen)?;
    *car_cell.borrow_mut() = new_car;
    let cdr = cdr_cell.borrow().clone();
    let new_cdr = materialize_hash_table_literals_inner(interp, &cdr, seen)?;
    *cdr_cell.borrow_mut() = new_cdr;
    Ok(value.clone())
}

fn hash_table_from_literal_fields(
    interp: &mut Interpreter,
    fields: &[Value],
    seen: &mut HashSet<usize>,
) -> Result<Value, LispError> {
    let mut test = "eql".to_string();
    let mut size = Value::Integer(65);
    let mut rehash_size = Value::Float(1.5);
    let mut rehash_threshold = Value::Float(0.8125);
    let mut weakness = Value::Nil;
    let mut entries = Vec::new();
    let mut index = 0usize;
    while index + 1 < fields.len() {
        let Ok(key) = fields[index].as_symbol() else {
            index += 2;
            continue;
        };
        let key = key.to_string();
        let field_value = fields[index + 1].clone();
        match key.as_str() {
            "test" => test = field_value.as_symbol()?.to_string(),
            "size" => size = field_value,
            "rehash-size" => rehash_size = field_value,
            "rehash-threshold" => rehash_threshold = field_value,
            "weakness" => weakness = field_value,
            "data" => {
                let items = field_value.to_vec()?;
                let mut cursor = 0usize;
                while cursor + 1 < items.len() {
                    let entry_key =
                        materialize_hash_table_literals_inner(interp, &items[cursor], seen)?;
                    let entry_value =
                        materialize_hash_table_literals_inner(interp, &items[cursor + 1], seen)?;
                    entries.push((entry_key, entry_value));
                    cursor += 2;
                }
            }
            _ => {}
        }
        index += 2;
    }
    let table = crate::lisp::json::make_hash_table(interp, &test, entries);
    if let Value::Record(id) = &table
        && let Some(record) = interp.find_record_mut(*id)
    {
        if record.slots.len() < 6 {
            record.slots.resize(6, Value::Nil);
        }
        record.slots[2] = size;
        record.slots[3] = rehash_size;
        record.slots[4] = rehash_threshold;
        record.slots[5] = weakness;
    }
    Ok(table)
}

fn read_from_lisp_source_raw(
    interp: &mut Interpreter,
    source: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    match source {
        Value::Buffer(_) => {
            let buffer_id = interp.resolve_buffer_id(source)?;
            let (start, end, text) = {
                let buffer = interp
                    .get_buffer_by_id(buffer_id)
                    .ok_or_else(|| LispError::Signal(format!("No buffer with id {buffer_id}")))?;
                let start = buffer.point();
                let end = buffer.point_max();
                (
                    start,
                    end,
                    buffer
                        .buffer_substring(start, end)
                        .map_err(|error| LispError::Signal(error.to_string()))?,
                )
            };
            let (value, consumed) = read_one_form_in_env(interp, &text, env)?;
            if let Some(buffer) = interp.get_buffer_by_id_mut(buffer_id) {
                buffer.goto_char((start + consumed).min(end));
            }
            Ok(value)
        }
        Value::Marker(id) => {
            let (buffer_id, start) = {
                let marker = interp.find_marker(*id).ok_or_else(|| {
                    LispError::TypeError("marker".into(), format!("marker<{id}>"))
                })?;
                let buffer_id = marker
                    .buffer_id
                    .ok_or_else(|| LispError::Signal("Marker does not point anywhere".into()))?;
                let start = marker
                    .position
                    .ok_or_else(|| LispError::Signal("Marker does not point anywhere".into()))?;
                (buffer_id, start)
            };
            let end = interp
                .get_buffer_by_id(buffer_id)
                .ok_or_else(|| LispError::Signal(format!("No buffer with id {buffer_id}")))?
                .point_max();
            let text = interp
                .get_buffer_by_id(buffer_id)
                .ok_or_else(|| LispError::Signal(format!("No buffer with id {buffer_id}")))?
                .buffer_substring(start, end)
                .map_err(|error| LispError::Signal(error.to_string()))?;
            let (value, consumed) = read_one_form_in_env(interp, &text, env)?;
            interp.set_marker(*id, Some((start + consumed).min(end)), Some(buffer_id))?;
            Ok(value)
        }
        Value::BuiltinFunc(_) | Value::Lambda(_) => read_from_callable_source(interp, source, env),
        Value::Symbol(symbol) if interp.lookup_function(symbol, env).is_ok() => {
            read_from_callable_source(interp, source, env)
        }
        _ => {
            let s = string_text(source)?;
            read_one_form_in_env(interp, &s, env).map(|(value, _)| value)
        }
    }
}

pub(crate) fn md5_source_text(
    interp: &mut Interpreter,
    source: &Value,
    start: Option<&Value>,
    end: Option<&Value>,
) -> Result<String, LispError> {
    match source {
        Value::Buffer(_) => {
            let buffer_id = interp.resolve_buffer_id(source)?;
            let buffer = interp
                .get_buffer_by_id(buffer_id)
                .ok_or_else(|| LispError::Signal(format!("No buffer with id {buffer_id}")))?;
            let start = start
                .filter(|value| !value.is_nil())
                .map(|value| position_from_value(interp, value))
                .transpose()?
                .unwrap_or_else(|| buffer.point_min());
            let end = end
                .filter(|value| !value.is_nil())
                .map(|value| position_from_value(interp, value))
                .transpose()?
                .unwrap_or_else(|| buffer.point_max());
            buffer
                .buffer_substring(start, end)
                .map_err(|error| LispError::Signal(error.to_string()))
        }
        _ => {
            let text = string_text(source)?;
            let chars: Vec<char> = text.chars().collect();
            let start = normalize_string_index(start, 0, chars.len() as i64)? as usize;
            let end = normalize_string_index(end, chars.len() as i64, chars.len() as i64)? as usize;
            Ok(chars[start..end].iter().collect())
        }
    }
}
