use super::*;

pub(crate) fn render_prin1_string(interp: &Interpreter, text: &str, env: &Env) -> String {
    let escape_multibyte = interp
        .lookup_var("print-escape-multibyte", env)
        .is_some_and(|value| value.is_truthy());
    let escape_newlines = interp
        .lookup_var("print-escape-newlines", env)
        .is_some_and(|value| value.is_truthy());

    if !escape_multibyte && !escape_newlines {
        return format!("{:?}", text);
    }

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
}

#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub(crate) enum PrintDialect {
    #[default]
    Emacs,
    Cl,
}

pub(crate) struct PrintContext {
    options: PrintOptions,
    counts: HashMap<PrintRefKey, usize>,
    labels: HashMap<PrintRefKey, usize>,
    next_label: usize,
    active: HashSet<PrintRefKey>,
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
        })
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
                fields.push(Value::Symbol(record.type_name.clone()));
                for (slot_name, slot_value) in slot_names.iter().zip(record.slots.iter()) {
                    let Ok(slot_name) = slot_name.as_symbol() else {
                        return None;
                    };
                    fields.push(Value::Symbol(format!(":{slot_name}")));
                    fields.push(slot_value.clone());
                }
                return Some(fields);
            }
            Some(
                std::iter::once(Value::Symbol(record.type_name.clone()))
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
        Value::Cons(car, _) => Some(PrintRefKey::Cons(Rc::as_ptr(car) as usize)),
        Value::StringObject(state) => Some(PrintRefKey::StringObject(Rc::as_ptr(state) as usize)),
        Value::Symbol(symbol)
            if options.gensym && crate::lisp::types::is_uninterned_symbol(symbol) =>
        {
            Some(PrintRefKey::Symbol(symbol.clone()))
        }
        Value::Record(id) if record_prin1_fields(interp, *id, PrintDialect::Emacs).is_some() => {
            Some(PrintRefKey::Record(*id))
        }
        _ => None,
    }
}

pub(crate) fn print_ref_placeholder(key: PrintRefKey) -> String {
    match key {
        PrintRefKey::Cons(id) => format!("#{id}"),
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
            Value::String(id.to_string()),
            label,
        ]),
        PrintRefKey::Record(id) => Value::list([
            Value::Symbol("record".into()),
            Value::String(id.to_string()),
            label,
        ]),
        PrintRefKey::StringObject(id) => Value::list([
            Value::Symbol("string-object".into()),
            Value::String(id.to_string()),
            label,
        ]),
        PrintRefKey::Symbol(symbol) => Value::list([
            Value::Symbol("symbol".into()),
            Value::String(symbol.clone()),
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
    env.push(vec![(name.into(), value)]);
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
        Value::Cons(_, _) if is_vector_value(value) => {
            for item in vector_items(value)? {
                collect_print_counts(interp, &item, options, counts, expanded)?;
            }
        }
        Value::Cons(_, _) => {
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
        Value::Cons(_, _) if is_vector_value(value) => {
            for item in vector_items(value)? {
                collect_print_table_objects(interp, &item, options, counts, expanded)?;
            }
        }
        Value::Cons(_, _) => {
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
            Value::Cons(_, _) => {
                if context
                    .options
                    .length
                    .is_some_and(|limit| rendered.len() >= limit)
                {
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
        || name
            .split_once(['e', 'E'])
            .is_some_and(|(mantissa, suffix)| {
                mantissa.parse::<f64>().is_ok()
                    && matches!(
                        suffix.to_ascii_uppercase().as_str(),
                        "NAN" | "+NAN" | "-NAN" | "INF" | "+INF" | "-INF"
                    )
            })
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
    let second = visible.chars().nth(1);
    let mut confusing = symbol_name_looks_like_number(visible)
        || first == Some('?')
        || matches!(first, Some('.')) && !second.is_some_and(|ch| ch.is_ascii_alphabetic());

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
                return Ok("...".into());
            }
            Value::Cons(_, _) if is_vector_value(value) => return Ok("...".into()),
            Value::Record(id)
                if record_prin1_fields(interp, *id, context.options.dialect).is_some() =>
            {
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
            [Value::Symbol(symbol), inner] if symbol == "backquote" => Some(("`", inner)),
            [Value::Symbol(symbol), inner] if symbol == "comma" => Some((",", inner)),
            [Value::Symbol(symbol), inner] if symbol == "comma-at" => Some((",@", inner)),
            _ => None,
        };
        if let Some((prefix, inner)) = quoted {
            return Ok(format!(
                "{prefix}{}",
                render_prin1_with_context(interp, inner, env, context, depth + 1)?
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
        Value::String(text) => Ok(render_prin1_string(interp, text, env)),
        Value::StringObject(state) => {
            let (text, props) = {
                let state = state.borrow();
                (state.text.clone(), state.props.clone())
            };
            if props.is_empty() {
                return Ok(render_prin1_string(interp, &text, env));
            }
            let mut rendered = vec![render_prin1_string(interp, &text, env)];
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
                rendered.push(span.start.to_string());
                rendered.push(span.end.to_string());
                rendered.push(render_prin1_with_context(
                    interp,
                    &plist_value(&filtered_props),
                    env,
                    context,
                    depth + 1,
                )?);
            }
            if rendered.len() == 1 {
                return Ok(render_prin1_string(interp, &text, env));
            }
            Ok(format!("#({})", rendered.join(" ")))
        }
        Value::Symbol(symbol) if symbol == "backquote" => Ok("\\`".into()),
        Value::Symbol(symbol) if symbol == "comma" => Ok("\\,".into()),
        Value::Symbol(symbol) if symbol == "comma-at" => Ok("\\,@".into()),
        Value::Symbol(symbol) => Ok(render_prin1_symbol(symbol, context.options)),
        Value::Cons(_, _) if is_vector_value(value) => {
            let items = vector_items(value)?;
            let mut rendered_items = Vec::new();
            for (index, item) in items.iter().enumerate() {
                if context.options.length.is_some_and(|limit| index >= limit) {
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
        Value::Cons(_, _) => render_prin1_list(interp, value, env, context, depth),
        Value::BuiltinFunc(_)
        | Value::Lambda(_, _, _)
        | Value::Buffer(_, _)
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
                        let Some(fields) =
                            record_prin1_fields(interp, *id, context.options.dialect)
                        else {
                            return Ok(value.to_string());
                        };
                        format!(
                            "#s({})",
                            fields
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
                                .collect::<Result<Vec<_>, _>>()?
                                .join(" ")
                        )
                    }
                };
                return Ok(rendered);
            }
            Ok(value.to_string())
        }
        _ => Ok(value.to_string()),
    }
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

pub(crate) fn render_prin1_ephemeral(
    interp: &mut Interpreter,
    value: &Value,
    env: &crate::lisp::types::Env,
) -> Result<String, LispError> {
    let mut env = env.clone();
    render_prin1(interp, value, &mut env)
}

pub(crate) fn read_one_form(text: &str) -> Result<(Value, usize), LispError> {
    let mut reader = crate::lisp::reader::Reader::new(text);
    let value = match reader.read()? {
        Some(value) => crate::lisp::reader::resolve_circular_read_syntax(value)?,
        None => return Err(LispError::EndOfInput),
    };
    let consumed = text[..reader.position()].chars().count();
    Ok((value, consumed))
}

pub(crate) fn record_literal_items(value: &Value) -> Option<Vec<Value>> {
    let items = value.to_vec().ok()?;
    matches!(
        items.first(),
        Some(Value::Symbol(name)) if name == crate::lisp::reader::RECORD_LITERAL_SYMBOL
    )
    .then_some(items)
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
    read_one_form(&text).map(|(value, _)| value)
}

pub(crate) fn read_from_lisp_source(
    interp: &mut Interpreter,
    source: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    match source {
        Value::Buffer(_, _) => {
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
            let (value, consumed) = read_one_form(&text)?;
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
            let (value, consumed) = read_one_form(&text)?;
            interp.set_marker(*id, Some((start + consumed).min(end)), Some(buffer_id))?;
            Ok(value)
        }
        Value::BuiltinFunc(_) | Value::Lambda(_, _, _) => {
            read_from_callable_source(interp, source, env)
        }
        Value::Symbol(symbol) if interp.lookup_function(symbol, env).is_ok() => {
            read_from_callable_source(interp, source, env)
        }
        _ => {
            let s = string_text(source)?;
            read_one_form(&s).map(|(value, _)| value)
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
        Value::Buffer(_, _) => {
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
