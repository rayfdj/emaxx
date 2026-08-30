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
            // GNU print.c octal-escapes raw 8-bit bytes (`\300') whenever
            // they cannot be emitted as characters: always in multibyte
            // strings, and under `print-escape-nonascii' (auto-bound by
            // string output in multibyte contexts) in unibyte strings.
            // Emaxx represents such bytes as placeholder scalars, which
            // must never leak into printed syntax.
            ch if case::is_raw_byte_regex_char(ch) => {
                let byte = case::raw_byte_from_regex_char(ch)
                    .expect("raw byte placeholder maps back to its byte");
                rendered.push_str(&format!("\\{byte:03o}"));
            }
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
    /// print.c passes `escapeflag' to every `print_object' call: `prin1'
    /// sets it, `princ' clears it, and it reaches nested elements too, so
    /// `(princ (list "a"))' prints `(a)'.
    escape: bool,
    circle: bool,
    continuous_numbering: bool,
    gensym: bool,
    integers_as_characters: bool,
    symbols_bare: bool,
    length: Option<usize>,
    level: Option<usize>,
    quoted: bool,
}

#[derive(Clone)]
pub(crate) struct PrintContext {
    options: PrintOptions,
    counts: HashMap<PrintRefKey, usize>,
    labels: HashMap<PrintRefKey, PrintLabel>,
    next_label: usize,
    active: HashMap<PrintRefKey, usize>,
    number_table: Option<Value>,
}

#[derive(Clone)]
struct PrintLabel {
    number: usize,
    printed: bool,
    object: Value,
}

impl PrintContext {
    fn new(
        interp: &Interpreter,
        value: &Value,
        env: &Env,
        options: PrintOptions,
    ) -> Result<Self, LispError> {
        let number_table = interp
            .lookup_var("print-number-table", env)
            .filter(|value| json::is_hash_table(interp, value));
        let (labels, next_label) = if options.circle && options.continuous_numbering {
            parse_print_number_table(interp, number_table.as_ref(), options)
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
            active: HashMap::new(),
            number_table,
        })
    }
}

pub(crate) fn print_options(interp: &Interpreter, env: &Env) -> PrintOptions {
    PrintOptions {
        escape: true,
        circle: interp
            .lookup_var("print-circle", env)
            .is_some_and(|value| value.is_truthy()),
        continuous_numbering: interp
            .lookup_var("print-continuous-numbering", env)
            .is_some_and(|value| value.is_truthy()),
        gensym: interp
            .lookup_var("print-gensym", env)
            .is_some_and(|value| value.is_truthy()),
        integers_as_characters: interp
            .lookup_var("print-integers-as-characters", env)
            .is_some_and(|value| value.is_truthy()),
        symbols_bare: interp
            .lookup_var("print-symbols-bare", env)
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

pub(crate) fn record_prin1_fields(interp: &Interpreter, id: u64) -> Option<Vec<Value>> {
    let record = interp.find_record(id)?;
    // GNU print.c handles PVEC_SYMBOL_WITH_POS directly rather than as a
    // record or print-circle candidate.  Its dedicated rendering branch is
    // in `render_prin1_body' below.
    if record.kind == crate::lisp::eval::RecordKind::SymbolWithPos {
        return None;
    }
    match record.kind {
        crate::lisp::eval::RecordKind::Thread
        | crate::lisp::eval::RecordKind::Mutex
        | crate::lisp::eval::RecordKind::ConditionVariable
        | crate::lisp::eval::RecordKind::HashTable
        | crate::lisp::eval::RecordKind::Process
        | crate::lisp::eval::RecordKind::Obarray => None,
        _ => Some(
            std::iter::once(record.type_tag.clone())
                .chain(record.slots.iter().cloned())
                .collect(),
        ),
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
        // print.c:1299 `PRINT_CIRCLE_CANDIDATE_P' counts hash tables, so a
        // table that contains itself is labelled (or truncated) rather than
        // printed forever.
        Value::Record(id)
            if record_prin1_fields(interp, *id).is_some() || json::is_hash_table(interp, value) =>
        {
            Some(PrintRefKey::Record(*id))
        }
        _ => None,
    }
}

/// print.c:2253: without `print-circle', an object already being printed is
/// rendered as `#N', where N is the print depth the outer occurrence sits
/// at -- not an object identity of any kind.
/// print.c:63 `PRINT_CIRCLE'.
const PRINT_CIRCLE_DEPTH_LIMIT: usize = 200;

pub(crate) fn print_ref_placeholder(depth: usize) -> String {
    format!("#{depth}")
}

fn parse_print_number_table(
    interp: &Interpreter,
    value: Option<&Value>,
    options: PrintOptions,
) -> (HashMap<PrintRefKey, PrintLabel>, usize) {
    let Some(value) = value else {
        return (HashMap::new(), 1);
    };
    let Some((_, entries)) = json::hash_table_entries(interp, value) else {
        return (HashMap::new(), 1);
    };

    let mut labels = HashMap::new();
    for (object, state) in entries {
        let Ok(state) = state.as_integer() else {
            continue;
        };
        let Some(number) = state
            .checked_abs()
            .and_then(|number| usize::try_from(number).ok())
            .filter(|number| *number > 0)
        else {
            continue;
        };
        let Some(key) = print_ref_key(interp, &object, options) else {
            continue;
        };
        labels.insert(
            key,
            PrintLabel {
                number,
                printed: state > 0,
                object,
            },
        );
    }

    let next_label = labels.values().map(|label| label.number).max().unwrap_or(0) + 1;
    (labels, next_label)
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
    walk_print_graph(interp, value, options, |key, _| {
        *counts.entry(key.clone()).or_insert(0) += 1;
        expanded.insert(key)
    })
}

/// Walk the exact object graph considered by GNU print.c's
/// PRINT_CIRCLE_CANDIDATE_P, visiting children in print order.  VISIT returns
/// whether a candidate's children should be traversed.  The explicit work
/// stack matches GNU's ppstack and remains safe for deep and cyclic objects.
fn walk_print_graph(
    interp: &Interpreter,
    value: &Value,
    options: PrintOptions,
    mut visit: impl FnMut(PrintRefKey, &Value) -> bool,
) -> Result<(), LispError> {
    let mut pending = vec![value.clone()];
    while let Some(value) = pending.pop() {
        if let Some(key) = print_ref_key(interp, &value, options)
            && !visit(key, &value)
        {
            continue;
        }

        match &value {
            Value::Cons(_) if is_vector_value(&value) => {
                let items = vector_items(&value)?;
                pending.extend(items.into_iter().rev());
            }
            Value::Cons(_) => {
                let Some((car, cdr)) = value.cons_values() else {
                    continue;
                };
                pending.push(cdr);
                pending.push(car);
            }
            Value::StringObject(state) => {
                let props = state.borrow().props.clone();
                for span in props.into_iter().rev() {
                    pending.extend(
                        span.props
                            .into_iter()
                            .rev()
                            .map(|(_, prop_value)| prop_value),
                    );
                }
            }
            Value::Record(id) => {
                if let Some(fields) = record_prin1_fields(interp, *id) {
                    pending.extend(fields.into_iter().rev());
                } else if let Some((_, entries)) = json::hash_table_entries(interp, &value) {
                    for (key, entry_value) in entries.into_iter().rev() {
                        pending.push(entry_value);
                        pending.push(key);
                    }
                }
            }
            _ => {}
        }
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
        _ => json::make_hash_table(interp, "eq", Vec::new()),
    };
    // GNU's Vprint_number_table is a real special variable.  Update its
    // active dynamic binding, not merely a lexical frame passed to the
    // primitive.
    interp.set_variable("print-number-table", table.clone(), env);

    let options = print_options(interp, env);
    let mut entries = json::hash_table_entries(interp, &table)
        .map(|(_, entries)| entries)
        .unwrap_or_default();
    let mut positions = entries
        .iter()
        .enumerate()
        .filter_map(|(index, (object, _))| {
            print_ref_key(interp, object, options).map(|key| (key, index))
        })
        .collect::<HashMap<_, _>>();
    let mut number_index = 0i64;
    walk_print_graph(interp, value, options, |key, object| {
        let continuous_gensym = options.continuous_numbering
            && matches!(
                object,
                Value::Symbol(symbol) if crate::lisp::types::is_uninterned_symbol(symbol)
            );
        if let Some(index) = positions.get(&key).copied() {
            let state = &entries[index].1;
            if state.is_truthy() || continuous_gensym {
                if matches!(state, Value::Nil | Value::T | Value::Symbol(_)) {
                    number_index = number_index.saturating_add(1);
                    entries[index].1 = Value::Integer(-number_index);
                }
                return false;
            }
            entries[index].1 = Value::T;
            return true;
        }

        let state = if continuous_gensym {
            number_index = number_index.saturating_add(1);
            Value::Integer(-number_index)
        } else {
            Value::T
        };
        let descend = !continuous_gensym;
        positions.insert(key, entries.len());
        entries.push((object.clone(), state));
        descend
    })?;

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
    // print.c:2541 seeds Brent's cycle detection with the cons whose car was
    // just printed; the tortoise teleports on a doubling period, so a
    // circular list prints its elements until the hare laps it and then
    // closes with `. #TORTOISE-INDEX'.
    let mut tortoise = value.clone();
    let mut tortoise_countdown: i64 = 2;
    let mut tortoise_period: i64 = 2;
    let mut tortoise_index: i64 = 0;
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
                    rendered.push("...".into());
                    return Ok(format!("({})", rendered.join(" ")));
                }
                if let Some(key) = print_ref_key(interp, &tail, context.options)
                    && should_label_value(&tail, &key, context)
                {
                    let tail_rendered =
                        render_prin1_with_context(interp, &tail, env, context, depth + 1)?;
                    return Ok(format!("({} . {})", rendered.join(" "), tail_rendered));
                }
                if !context.options.circle {
                    tortoise_countdown -= 1;
                    if tortoise_countdown == 0 {
                        tortoise_index += tortoise_period;
                        tortoise_period <<= 1;
                        tortoise_countdown = tortoise_period;
                        tortoise = tail.clone();
                    } else if same_cons_cell(&tail, &tortoise) {
                        return Ok(format!("({} . #{})", rendered.join(" "), tortoise_index));
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

fn same_cons_cell(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Cons(left), Value::Cons(right)) => {
            crate::lisp::types::ConsCell::identity(left)
                == crate::lisp::types::ConsCell::identity(right)
        }
        _ => false,
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
            if let Some(label) = context.labels.get_mut(&key) {
                let number = label.number;
                if label.printed {
                    return Ok(format!("#{number}#"));
                }
                label.printed = true;
                context.active.insert(key.clone(), depth);
                let rendered = render_prin1_body(interp, value, env, context, depth);
                context.active.remove(&key);
                return rendered.map(|body| format!("#{number}={body}"));
            }
            let number = context.next_label;
            context.next_label += 1;
            context.labels.insert(
                key.clone(),
                PrintLabel {
                    number,
                    printed: true,
                    object: value.clone(),
                },
            );
            context.active.insert(key.clone(), depth);
            let rendered = render_prin1_body(interp, value, env, context, depth);
            context.active.remove(&key);
            return rendered.map(|body| format!("#{number}={body}"));
        }
        if let Some(outer_depth) = context.active.get(&key).copied() {
            return Ok(print_ref_placeholder(outer_depth));
        }
        // print.c:2249: printing without `print-circle' gives up past
        // PRINT_CIRCLE levels rather than exhausting the C stack.
        if depth >= PRINT_CIRCLE_DEPTH_LIMIT {
            return Err(LispError::Signal(
                "Apparently circular structure being printed".into(),
            ));
        }
        context.active.insert(key.clone(), depth);
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

fn render_integer_as_character(value: &Value, escape: bool) -> Option<String> {
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
        '\'' | '"' | '\\' | ';' | '(' | ')' | '{' | '}' | '[' | ']' if escape => {
            format!("\\{ch}")
        }
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

pub(crate) fn render_prin1_integer_as_character(value: &Value) -> Option<String> {
    render_integer_as_character(value, true)
}

pub(crate) fn render_princ_integer_as_character(value: &Value) -> Option<String> {
    render_integer_as_character(value, false)
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
    // print.c quotes a symbol's confusing characters only under
    // `escapeflag'; `princ' writes the name as it stands.
    if !options.escape {
        rendered.push_str(visible);
        return rendered;
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
    text: &str,
    span: &StringPropertySpan,
    value: &Value,
) -> bool {
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
    // print.c:2588 prints only what the reader needs: the test when it is
    // not `eql', the weakness when the table is weak, `purecopy t' when
    // set, and the data when the table is non-empty.
    let test = hash_table_metadata_slot(interp, value, 0, Value::Symbol("eql".into()))?;
    let weakness = hash_table_metadata_slot(interp, value, 5, Value::Nil)?;
    let purecopy = hash_table_metadata_slot(interp, value, 6, Value::Nil)?;
    let entries = json::hash_table_entries(interp, value)
        .map(|(_, entries)| entries)
        .unwrap_or_default();

    let mut rendered = String::from("#s(hash-table");
    if !matches!(&test, Value::Symbol(name) if name == "eql") {
        rendered.push_str(" test ");
        rendered.push_str(&render_prin1_with_context(
            interp,
            &test,
            env,
            context,
            depth + 1,
        )?);
    }
    if weakness.is_truthy() {
        rendered.push_str(" weakness ");
        rendered.push_str(&render_prin1_with_context(
            interp,
            &weakness,
            env,
            context,
            depth + 1,
        )?);
    }
    if purecopy.is_truthy() {
        rendered.push_str(" purecopy t");
    }

    if !entries.is_empty() {
        let count = entries.len();
        let printed = context.options.length.unwrap_or(count).min(count);
        let mut data_parts = Vec::new();
        for (key, entry_value) in entries.iter().take(printed) {
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
        if printed < count {
            data_parts.push("...".into());
        }
        rendered.push_str(" data (");
        rendered.push_str(&data_parts.join(" "));
        rendered.push(')');
    }
    rendered.push(')');
    Ok(rendered)
}

/// `print_bool_vector' (print.c): the bits are packed eight to a byte,
/// low-order bit first, and each byte is written with the escape rules
/// `octalout' applies.
fn render_bool_vector_prin1(
    interp: &Interpreter,
    env: &Env,
    bits: &[bool],
    options: PrintOptions,
) -> String {
    let escape_newlines = interp
        .lookup_var("print-escape-newlines", env)
        .is_some_and(|value| value.is_truthy());
    let escape_control = interp
        .lookup_var("print-escape-control-characters", env)
        .is_some_and(|value| value.is_truthy());

    let size = bits.len();
    let real_size_in_bytes = size.div_ceil(8);
    let mut data = vec![0u8; real_size_in_bytes];
    for (index, bit) in bits.iter().enumerate() {
        if *bit {
            data[index / 8] |= 1 << (index % 8);
        }
    }
    let size_in_bytes = options
        .length
        .map_or(real_size_in_bytes, |limit| limit.min(real_size_in_bytes));

    let mut rendered = format!("#&{size}\"");
    for index in 0..size_in_bytes {
        let byte = data[index];
        if byte == b'\n' && escape_newlines {
            rendered.push_str("\\n");
        } else if byte == 0x0c && escape_newlines {
            rendered.push_str("\\f");
        } else if byte > 0o177 || (escape_control && byte.is_ascii_control()) {
            let digits = if byte > 0o77
                || data
                    .get(index + 1)
                    .is_some_and(|next| index + 1 < size_in_bytes && (b'0'..=b'7').contains(next))
            {
                3
            } else if byte > 0o7 {
                2
            } else {
                1
            };
            rendered.push('\\');
            for shift in (0..digits).rev() {
                rendered.push(char::from(b'0' + ((byte >> (3 * shift)) & 7)));
            }
        } else {
            if byte == b'"' || byte == b'\\' {
                rendered.push('\\');
            }
            rendered.push(char::from(byte));
        }
    }
    if size_in_bytes < real_size_in_bytes {
        rendered.push_str(" ...");
    }
    rendered.push('"');
    rendered
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
            let rendered = if context.options.escape {
                render_prin1_integer_as_character(value)
            } else {
                render_princ_integer_as_character(value)
            };
            Ok(rendered.unwrap_or_else(|| value.to_string()))
        }
        Value::String(text) if !context.options.escape => Ok(text.to_string()),
        Value::String(text) => Ok(render_prin1_string(interp, text, env)),
        Value::StringObject(state) if !context.options.escape => Ok(state.borrow().text.clone()),
        Value::StringObject(state) => {
            let (text, props) = {
                let state = state.borrow();
                (state.text.clone(), state.props.clone())
            };
            if props.is_empty() {
                return Ok(render_prin1_string(interp, &text, env));
            }
            let mut rendered = vec![render_prin1_string(interp, &text, env)];
            let mut field_values = Vec::new();
            for span in props {
                let filtered_props = span
                    .props
                    .iter()
                    .filter(|(name, value)| {
                        name != "charset"
                            || should_render_charset_text_property(interp, env, &text, &span, value)
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
            for field in &field_values {
                if context
                    .options
                    .length
                    .is_some_and(|limit| rendered.len() >= limit)
                {
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
                return Ok(render_prin1_string(interp, &text, env));
            }
            Ok(format!("#({})", rendered.join(" ")))
        }
        Value::Symbol(symbol)
            if context.options.escape && (symbol == "backquote" || symbol == "`") =>
        {
            Ok("\\`".into())
        }
        Value::Symbol(symbol) if context.options.escape && (symbol == "comma" || symbol == ",") => {
            Ok("\\,".into())
        }
        Value::Symbol(symbol)
            if context.options.escape && (symbol == "comma-at" || symbol == ",@") =>
        {
            Ok("\\,@".into())
        }
        Value::Symbol(symbol) => Ok(render_prin1_symbol(symbol, context.options)),
        Value::Cons(_) if is_vector_value(value) => {
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
        Value::Cons(_) => render_prin1_list(interp, value, env, context, depth),
        Value::Lambda(lambda_value) => {
            let params = &lambda_value.params;
            let body = &lambda_value.body;
            let closure_env = &lambda_value.env;
            if let Some(rendered) = unreadable_override(interp, value, env)? {
                return Ok(rendered);
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
                Value::BuiltinFunc(name) => Ok(format!("#<subr {name}>")),
                Value::Buffer(buffer) if !context.options.escape => Ok(buffer.name.to_string()),
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
                if record.kind == crate::lisp::eval::RecordKind::SymbolWithPos {
                    let Some((symbol, position)) = symbol_with_pos_parts(interp, value) else {
                        return Ok("#<symbol NOT A SYMBOL!! NOT A POSITION!!>".into());
                    };
                    if context.options.symbols_bare {
                        return render_prin1_with_context(interp, &symbol, env, context, depth);
                    }
                    let rendered_symbol =
                        render_prin1_with_context(interp, &symbol, env, context, depth + 1)?;
                    return Ok(format!("#<symbol {rendered_symbol} at {position}>"));
                }
                let rendered = match record.kind {
                    crate::lisp::eval::RecordKind::Closure => {
                        // GNU print.c writes PVEC_CLOSURE with its dedicated
                        // readable `#[...]' syntax.  `#s(...)' would read back
                        // as an ordinary record and make a freshly emitted
                        // .elc's byte-code functions non-callable.
                        let slots = record.slots.clone();
                        let mut rendered_slots = Vec::new();
                        for (index, slot) in slots.iter().enumerate() {
                            if context.options.length.is_some_and(|limit| index >= limit) {
                                rendered_slots.push("...".into());
                                break;
                            }
                            rendered_slots.push(render_prin1_with_context(
                                interp,
                                slot,
                                env,
                                context,
                                depth + 1,
                            )?);
                        }
                        format!("#[{}]", rendered_slots.join(" "))
                    }
                    // print.c:1930 prints a thread, mutex or condition
                    // variable by name, falling back to the object's
                    // address.  Emaxx has no addresses to quote, so it
                    // prints its own object identity in the same syntax.
                    crate::lisp::eval::RecordKind::Thread => interp
                        .thread_name(*id)
                        .map(|name| format!("#<thread {name}>"))
                        .unwrap_or_else(|| format!("#<thread 0x{id:x}>")),
                    crate::lisp::eval::RecordKind::Mutex => interp
                        .mutex_name(*id)
                        .map(|name| format!("#<mutex {name}>"))
                        .unwrap_or_else(|| format!("#<mutex 0x{id:x}>")),
                    crate::lisp::eval::RecordKind::ConditionVariable => interp
                        .condition_variable_name(*id)
                        .map(|name| format!("#<condvar {name}>"))
                        .unwrap_or_else(|| format!("#<condvar 0x{id:x}>")),
                    crate::lisp::eval::RecordKind::HashTable => {
                        render_hash_table_prin1(interp, value, env, context, depth)?
                    }
                    // print.c `print_bool_vector': `#&SIZE"BYTES"', the
                    // bits packed low-order-first and the bytes written
                    // with string escaping rules.
                    crate::lisp::eval::RecordKind::BoolVector => {
                        let bits = bool_vector_bits(interp, value)?;
                        render_bool_vector_prin1(interp, env, &bits, context.options)
                    }
                    // print.c:1782: a process prints as `#<process NAME>',
                    // or as its bare name when `princ' clears escapeflag.
                    crate::lisp::eval::RecordKind::Process => {
                        let name = interp
                            .process_name(*id)
                            .unwrap_or_else(|| format!("0x{id:x}"));
                        if context.options.escape {
                            format!("#<process {name}>")
                        } else {
                            name
                        }
                    }
                    // print.c has no keymap case at all: a GNU keymap IS
                    // the list, so it prints as one.  Print the public list
                    // view -- the same value `car', `cdr' and `equal'
                    // already expose -- so `prin1' stops contradicting
                    // `type-of' about what a keymap is.
                    crate::lisp::eval::RecordKind::Keymap => {
                        let view = crate::lisp::primitives::values::runtime_keymap_public_view(
                            interp, value,
                        )
                        .unwrap_or(Value::Nil);
                        render_prin1_with_context(interp, &view, env, context, depth)?
                    }
                    // print.c:2087.
                    crate::lisp::eval::RecordKind::Obarray => {
                        let count =
                            crate::lisp::primitives::completion::obarray_symbols(interp, value)
                                .map(|symbols| symbols.len())
                                .unwrap_or(0);
                        format!("#<obarray n={count}>")
                    }
                    _ => {
                        let Some(fields) = record_prin1_fields(interp, *id) else {
                            return Ok(value.to_string());
                        };
                        let rendered_fields = fields
                            .iter()
                            .map(|field| {
                                render_prin1_with_context(interp, field, env, context, depth + 1)
                            })
                            .collect::<Result<Vec<_>, _>>()?;
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

pub(crate) fn finish_print_number_table(
    interp: &mut Interpreter,
    env: &mut Env,
    context: &PrintContext,
) -> Result<(), LispError> {
    if !context.options.circle || !context.options.continuous_numbering {
        return Ok(());
    }
    let table = context
        .number_table
        .clone()
        .unwrap_or_else(|| json::make_hash_table(interp, "eq", Vec::new()));
    let mut entries = json::hash_table_entries(interp, &table)
        .map(|(_, entries)| entries)
        .unwrap_or_default();
    entries.retain(|(_, state)| !matches!(state, Value::Integer(_)));
    let mut labels = context.labels.values().collect::<Vec<_>>();
    labels.sort_by_key(|label| label.number);
    entries.extend(labels.into_iter().map(|label| {
        let number = i64::try_from(label.number).unwrap_or(i64::MAX);
        let state = if label.printed { number } else { -number };
        (label.object.clone(), Value::Integer(state))
    }));
    set_hash_table_entries(interp, &table, entries)?;
    set_env_binding(env, "print-number-table", table);
    Ok(())
}

pub(crate) fn render_prin1(
    interp: &mut Interpreter,
    value: &Value,
    env: &mut crate::lisp::types::Env,
) -> Result<String, LispError> {
    let mut context = PrintContext::new(interp, value, env, print_options(interp, env))?;
    let rendered = render_prin1_with_context(interp, value, env, &mut context, 0)?;
    finish_print_number_table(interp, env, &context)?;
    Ok(rendered)
}

/// `princ': print.c's `print_object' with `escapeflag' cleared.  This is the
/// same traversal `prin1' uses, so the flag reaches nested elements.
pub(crate) fn render_princ_object(
    interp: &mut Interpreter,
    value: &Value,
    env: &mut crate::lisp::types::Env,
) -> Result<String, LispError> {
    let mut options = print_options(interp, env);
    options.escape = false;
    let mut context = PrintContext::new(interp, value, env, options)?;
    let rendered = render_prin1_with_context(interp, value, env, &mut context, 0)?;
    finish_print_number_table(interp, env, &context)?;
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
            let result = read_one_positioned_form(interp, env, &text, start as i64);
            let consumed = match &result {
                Ok((_, consumed)) => *consumed,
                Err(LispError::EndOfInput) => text.chars().count(),
                Err(_) => 0,
            };
            if let Some(buffer) = interp.get_buffer_by_id_mut(buffer_id) {
                buffer.goto_char((start + consumed).min(end));
            }
            result.map(|(value, _)| value)
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
            let result = read_one_positioned_form(interp, env, &text, start as i64);
            let consumed = match &result {
                Ok((_, consumed)) => *consumed,
                Err(LispError::EndOfInput) => text.chars().count(),
                Err(_) => 0,
            };
            interp.set_marker(*id, Some((start + consumed).min(end)), Some(buffer_id))?;
            result.map(|(value, _)| value)
        }
        Value::BuiltinFunc(_) | Value::Lambda(_) => {
            let value = read_from_callable_source(interp, source, env)?;
            // GNU's `read0' interns a symbol before wrapping it in the
            // position-bearing pseudovector.  Emaxx parses independently of
            // its obarray, so preserve that C-owned reader side effect
            // explicitly before replacing ordinary symbols with records.
            interp.intern_symbols_in_value(&value);
            Ok(position_symbols_in_value(
                interp,
                value,
                &mut VecDeque::new(),
            ))
        }
        Value::Symbol(symbol) if interp.lookup_function(symbol, env).is_ok() => {
            let value = read_from_callable_source(interp, source, env)?;
            interp.intern_symbols_in_value(&value);
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
    // GNU 30.2 lread.c:read0 interns every ordinary symbol even when
    // LOCATE_SYMS asks it to return a `symbol-with-pos' wrapper.
    interp.intern_symbols_in_value(&value);
    let symbol_shorthands = read_symbol_shorthands_in_env(interp, env)?;
    let mut tokens = symbol_tokens_with_positions(text, base_position, &symbol_shorthands);
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
                interp.create_pseudovector(
                    crate::lisp::eval::RecordKind::SymbolWithPos,
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

fn symbol_tokens_with_positions(
    text: &str,
    base_position: i64,
    symbol_shorthands: &[(String, String)],
) -> VecDeque<(String, i64)> {
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
        let skip_shorthand = ch == '#' && chars.get(idx + 1).is_some_and(|(_, next)| *next == '_');
        if skip_shorthand {
            idx += 2;
        } else if is_reader_delimiter(ch) {
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
            let token = if skip_shorthand {
                token
            } else {
                crate::lisp::reader::apply_symbol_shorthands_to_token(token, symbol_shorthands)
            };
            tokens.push_back((token, base_position + start_char_pos));
        }
    }
    tokens
}

fn is_reader_delimiter(ch: char) -> bool {
    matches!(ch, '(' | ')' | '[' | ']' | '\'' | '`' | ',' | '"' | '#')
}

pub(crate) fn record_literal_items(value: &Value) -> Option<Vec<Value>> {
    let Value::ReaderForm(form) = value else {
        return None;
    };
    let crate::lisp::types::ReaderForm::Record { slots } = form.as_ref() else {
        return None;
    };
    Some(
        std::iter::once(Value::Nil)
            .chain(slots.iter().cloned())
            .collect(),
    )
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
                return Err(LispError::WrongTypeArgument(
                    "integerp".into(),
                    other.clone(),
                ));
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
    match value {
        Value::ReaderForm(form) => match form.as_ref() {
            crate::lisp::types::ReaderForm::CharTable { fields } => Some(fields.clone()),
            _ => None,
        },
        _ => None,
    }
}

fn sub_char_table_literal_fields(value: &Value) -> Option<Vec<Value>> {
    match value {
        Value::ReaderForm(form) => match form.as_ref() {
            crate::lisp::types::ReaderForm::SubCharTable { fields } => Some(fields.clone()),
            _ => None,
        },
        _ => None,
    }
}

fn materialize_char_table_literals_inner(
    interp: &mut Interpreter,
    value: &Value,
    seen: &mut HashSet<usize>,
) -> Result<Value, LispError> {
    if let Some(fields) = char_table_literal_fields(value) {
        return char_table_from_literal_fields(interp, &fields, seen);
    }
    let Some((car_cell, cdr_cell)) = (value).cons_cells() else {
        return Ok(value.clone());
    };
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
    // GNU's reader constructs every identity-bearing object recursively.  A
    // character-table slot can itself contain a `#[...]' decoder (the Unicode
    // name table does), so materialize record/closure literals before the
    // hash- and character-table passes.  Each object kind keeps an independent
    // cycle/identity context: sharing `seen` would make a later pass mistake an
    // ordinary cons already visited by an earlier pass for a cycle.
    let value = interp.materialize_read_record_literals(value)?;
    let value = materialize_read_hash_table_literals(interp, &value)?;
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
    match literal {
        Value::ReaderForm(form) => match form.as_ref() {
            crate::lisp::types::ReaderForm::HashTable { fields } => Some(fields.clone()),
            _ => None,
        },
        _ => None,
    }
}

fn bare_hash_table_literal_fields(value: &Value) -> Option<Vec<Value>> {
    match value {
        Value::ReaderForm(form) => match form.as_ref() {
            crate::lisp::types::ReaderForm::HashTable { fields } => Some(fields.clone()),
            _ => None,
        },
        _ => None,
    }
}

fn materialize_hash_table_literals_inner(
    interp: &mut Interpreter,
    value: &Value,
    seen: &mut HashSet<usize>,
) -> Result<Value, LispError> {
    if let Some(fields) = quoted_hash_table_literal_fields(value) {
        return hash_table_from_literal_fields(interp, &fields, seen);
    }
    if let Some(fields) = bare_hash_table_literal_fields(value) {
        return hash_table_from_literal_fields(interp, &fields, seen);
    }
    let Some((car_cell, cdr_cell)) = (value).cons_cells() else {
        return Ok(value.clone());
    };
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
            let result = read_one_form_in_env(interp, &text, env);
            let consumed = match &result {
                Ok((_, consumed)) => *consumed,
                Err(LispError::EndOfInput) => text.chars().count(),
                Err(_) => 0,
            };
            if let Some(buffer) = interp.get_buffer_by_id_mut(buffer_id) {
                buffer.goto_char((start + consumed).min(end));
            }
            result.map(|(value, _)| value)
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
            let result = read_one_form_in_env(interp, &text, env);
            let consumed = match &result {
                Ok((_, consumed)) => *consumed,
                Err(LispError::EndOfInput) => text.chars().count(),
                Err(_) => 0,
            };
            interp.set_marker(*id, Some((start + consumed).min(end)), Some(buffer_id))?;
            result.map(|(value, _)| value)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn preprocess_env(interp: &mut Interpreter, table: Value) -> Env {
        interp.set_symbol_value_cell("print-circle", Value::T);
        interp.set_symbol_value_cell("print-continuous-numbering", Value::Nil);
        interp.set_symbol_value_cell("print-gensym", Value::Nil);
        interp.set_symbol_value_cell("print-number-table", table);
        Env::new()
    }

    fn table_state(interp: &Interpreter, table: &Value, object: &Value) -> Option<Value> {
        let key = print_ref_key(
            interp,
            object,
            PrintOptions {
                circle: true,
                ..PrintOptions::default()
            },
        )?;
        json::hash_table_entries(interp, table)?
            .1
            .into_iter()
            .find_map(|(candidate, state)| {
                (print_ref_key(
                    interp,
                    &candidate,
                    PrintOptions {
                        circle: true,
                        ..PrintOptions::default()
                    },
                ) == Some(key.clone()))
                .then_some(state)
            })
    }

    #[test]
    fn print_preprocess_numbers_repeated_objects_in_second_encounter_order() {
        let mut interp = Interpreter::new();
        let a = Value::list([Value::symbol("a")]);
        let b = Value::list([Value::symbol("b")]);
        let object = Value::list([a.clone(), b.clone(), a.clone(), b.clone()]);
        let mut env = preprocess_env(&mut interp, Value::Nil);

        print_preprocess(&mut interp, &object, &mut env).expect("preprocess shared list");

        let table = interp
            .lookup_var("print-number-table", &env)
            .expect("public number table");
        assert_eq!(table_state(&interp, &table, &a), Some(Value::Integer(-1)));
        assert_eq!(table_state(&interp, &table, &b), Some(Value::Integer(-2)));
    }

    #[test]
    fn print_preprocess_terminates_and_numbers_a_materialized_cycle() {
        let mut interp = Interpreter::new();
        let object = Value::cons(Value::Integer(1), Value::Nil);
        let (_, cdr) = object.cons_cells().expect("cons");
        *cdr.borrow_mut() = object.clone();
        let mut env = preprocess_env(&mut interp, Value::Nil);

        print_preprocess(&mut interp, &object, &mut env).expect("preprocess cyclic list");

        let table = interp
            .lookup_var("print-number-table", &env)
            .expect("public number table");
        assert_eq!(
            table_state(&interp, &table, &object),
            Some(Value::Integer(-1))
        );
    }

    #[test]
    fn print_preprocess_respects_existing_states_and_resets_new_numbering() {
        let mut interp = Interpreter::new();
        let existing = Value::list([Value::symbol("existing")]);
        let repeated = Value::list([Value::symbol("repeated")]);
        let table = json::make_hash_table(
            &mut interp,
            "eq",
            vec![(existing.clone(), Value::Integer(-7))],
        );
        let object = Value::list([existing.clone(), repeated.clone(), repeated.clone()]);
        let mut env = preprocess_env(&mut interp, table.clone());

        print_preprocess(&mut interp, &object, &mut env).expect("preprocess existing table");

        assert_eq!(
            table_state(&interp, &table, &existing),
            Some(Value::Integer(-7))
        );
        assert_eq!(
            table_state(&interp, &table, &repeated),
            Some(Value::Integer(-1))
        );
    }
}
