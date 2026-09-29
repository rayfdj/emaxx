use super::*;
use crate::lisp::types::Kind;
use crate::lisp::types::LispErrorKind;

pub(crate) fn render_prin1_string(
    interp: &Interpreter,
    text: &str,
    multibyte: bool,
    env: &Env,
) -> String {
    let escape_multibyte = interp
        .lookup_var("print-escape-multibyte", env)
        .is_some_and(|value| value.is_truthy());
    let escape_newlines = interp
        .lookup_var("print-escape-newlines", env)
        .is_some_and(|value| value.is_truthy());

    // GNU prin1 escapes only `"' and `\' by default: newlines, tabs and
    // other control characters print raw unless print-escape-newlines is
    // non-nil (Rust's {:?} formatting escapes them all, which is wrong).
    // print.c:1635 escapes exactly `\n'/`\f' under print-escape-newlines,
    // and octal-escapes control characters under
    // print-escape-control-characters via octalout, whose width obeys the
    // following-octal-digit guard.
    let escape_control = interp
        .lookup_var("print-escape-control-characters", env)
        .is_some_and(|value| value.is_truthy());
    let escape_nonascii = interp
        .lookup_var("print-escape-nonascii", env)
        .is_some_and(|value| value.is_truthy());
    let mut rendered = String::with_capacity(text.len() + 2);
    rendered.push('"');
    let mut chars = text.chars().peekable();
    let mut need_nonhex = false;
    while let Some(ch) = chars.next() {
        if need_nonhex && ch.is_ascii_hexdigit() {
            rendered.push_str("\\ ");
        }
        need_nonhex = false;
        match ch {
            '"' => rendered.push_str("\\\""),
            '\\' => rendered.push_str("\\\\"),
            '\n' if escape_newlines => rendered.push_str("\\n"),
            '\u{000C}' if escape_newlines => rendered.push_str("\\f"),
            ch if escape_control && ((ch as u32) < 0x20 || ch as u32 == 0x7f) => {
                let code = ch as u32;
                let next_is_octal_digit =
                    matches!(chars.peek(), Some(next) if ('0'..='7').contains(next));
                let digits = if code > 0o77 || next_is_octal_digit {
                    3
                } else if code > 0o7 {
                    2
                } else {
                    1
                };
                rendered.push('\\');
                for shift in (0..digits).rev() {
                    let digit = (code >> (3 * shift)) & 7;
                    rendered.push(char::from(b'0' + digit as u8));
                }
            }
            // print.c:print_object distinguishes raw bytes in multibyte
            // strings from unibyte bytes. The latter remain BYTE8 chars
            // unless the actual destination requests octal escapes.
            ch if case::is_raw_byte_regex_char(ch) && (multibyte || escape_nonascii) => {
                let byte = case::raw_byte_from_regex_char(ch)
                    .expect("raw byte placeholder maps back to its byte");
                rendered.push_str(&format!("\\{byte:03o}"));
            }
            ch if multibyte && escape_multibyte && !ch.is_ascii() => {
                rendered.push_str(&format!("\\x{:04x}", ch as u32));
                need_nonhex = true;
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
    Vector(usize),
    Lambda(usize),
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
    /// print_string sends unibyte character codes to function streams,
    /// while buffer/stdout output preserves their byte8 representation.
    output_is_function: bool,
}

fn render_princ_string(
    interp: &Interpreter,
    text: &str,
    multibyte: bool,
    env: &Env,
    output_is_function: bool,
) -> String {
    let escape_nonascii = !output_is_function
        && interp
            .lookup_var("print-escape-nonascii", env)
            .is_some_and(|value| value.is_truthy());
    let mut rendered = String::with_capacity(text.len());
    for ch in text.chars() {
        if let Some(byte) = raw_byte_from_regex_char(ch) {
            if escape_nonascii {
                rendered.push_str(&format!("\\{byte:03o}"));
            } else if output_is_function && !multibyte {
                rendered.push(char::from(byte));
            } else {
                rendered.push(ch);
            }
        } else {
            rendered.push(ch);
        }
    }
    rendered
}

#[derive(Clone)]
pub(crate) struct PrintContext {
    options: PrintOptions,
    counts: HashMap<PrintRefKey, usize>,
    labels: HashMap<PrintRefKey, PrintLabel>,
    next_label: usize,
    active: HashMap<PrintRefKey, usize>,
    number_table: Option<Value>,
    /// print.c's `new_backquote_output': the backquote nesting the comma
    /// shorthand `,X' is printed within; outside one, `(\, X)' is a list.
    backquote_output: usize,
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
        let mut labels = if options.circle && options.continuous_numbering {
            parse_print_number_table(interp, number_table.as_ref(), options)
        } else {
            HashMap::new()
        };
        // print.c's `print_number_index' is independent of the public hash
        // table.  In particular, `print--preprocess' can advance the counter
        // in a temporary dynamic binding before a nested printer reuses it.
        let mut next_label = if options.continuous_numbering {
            interp.print_number_index.saturating_add(1)
        } else {
            1
        };
        let mut counts = HashMap::new();
        if options.circle {
            if options.continuous_numbering {
                collect_print_counts(interp, value, options, &mut counts)?;
            } else {
                collect_print_sharing(
                    interp,
                    value,
                    options,
                    &mut counts,
                    &mut labels,
                    &mut next_label,
                )?;
            }
        }
        Ok(Self {
            options,
            counts,
            labels,
            next_label,
            active: HashMap::new(),
            number_table,
            backquote_output: 0,
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
        output_is_function: false,
    }
}

pub(crate) fn record_prin1_fields(interp: &Interpreter, id: u64) -> Option<Vec<Value>> {
    let record = interp.find_record(id)?;
    match record.kind {
        crate::lisp::eval::RecordKind::Thread
        | crate::lisp::eval::RecordKind::Mutex
        | crate::lisp::eval::RecordKind::ConditionVariable
        | crate::lisp::eval::RecordKind::Process
        | crate::lisp::eval::RecordKind::Obarray => None,
        _ => Some(
            std::iter::once(record.type_tag)
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
    match value.kind() {
        Kind::Cons(cell) => Some(PrintRefKey::Cons(crate::lisp::types::ConsCell::identity(
            &cell,
        ))),
        Kind::Vector(vector) => Some(PrintRefKey::Vector(vector.identity())),
        Kind::LispRecord(record) => Some(PrintRefKey::Vector(record.identity())),
        Kind::CharTable(table) => Some(PrintRefKey::Vector(table.identity())),
        Kind::HashTable(table) => Some(PrintRefKey::Vector(table.identity())),
        Kind::SubCharTable(table) => Some(PrintRefKey::Vector(table.identity())),
        // print.c:PRINT_CIRCLE_CANDIDATE_P includes every string.  Immutable
        // strings still have Lisp identity: cloning SharedText preserves its
        // Rc allocation, so repeated occurrences must receive one #N label.
        Kind::String(text) => Some(PrintRefKey::StringObject(text.identity_ptr())),
        Kind::Lambda(lambda) => Some(PrintRefKey::Lambda(lambda.identity())),
        Kind::StringObject(state) => Some(PrintRefKey::StringObject(state.identity())),
        Kind::Symbol(symbol)
            if options.gensym && crate::lisp::types::is_uninterned_symbol(&symbol) =>
        {
            Some(PrintRefKey::Symbol(symbol.to_string()))
        }
        // print.c:1299 `PRINT_CIRCLE_CANDIDATE_P' counts hash tables, so a
        // table that contains itself is labelled (or truncated) rather than
        // printed forever.
        Kind::Record(id)
            if record_prin1_fields(interp, id.id).is_some()
                || json::is_hash_table(interp, value) =>
        {
            Some(PrintRefKey::Record(id.id))
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
) -> HashMap<PrintRefKey, PrintLabel> {
    let Some(value) = value else {
        return HashMap::new();
    };
    let Some((_, entries)) = json::hash_table_entries(interp, value) else {
        return HashMap::new();
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

    labels
}

/// A binding of NAME in a private printing environment: assigned in
/// place when bound there, consed onto the environment otherwise.
pub(crate) fn set_env_binding(env: &mut Env, name: &str, value: Value) {
    if let Some(environment) = crate::lisp::types::current_environment(env)
        && let Ok(Some(binding)) = crate::lisp::types::assq_binding_named(environment, name)
    {
        binding.cdr.set(value);
        return;
    }
    Interpreter::push_bindings(env, vec![(name.into(), value)]);
}

pub(crate) fn sync_print_number_table(
    target_env: &mut Env,
    overrides: Option<&Value>,
    source_env: &Env,
) {
    if !matches!(overrides.map(|v| v.kind()), None | Some(Kind::Nil)) {
        return;
    }
    let Some(value) = crate::lisp::types::current_environment(source_env)
        .and_then(|environment| {
            crate::lisp::types::assq_binding_named(environment, "print-number-table")
                .ok()
                .flatten()
        })
        .map(|binding| binding.cdr.get())
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
) -> Result<(), LispError> {
    let mut expanded = HashSet::new();
    walk_print_graph(interp, value, options, |key, _| {
        *counts.entry(key.clone()).or_insert(0) += 1;
        expanded.insert(key)
    })
}

/// Match print.c:print_preprocess's numbering rule: a shared object's label
/// is allocated when the traversal encounters that object for the second
/// time, not when the printer later reaches its first occurrence.  These
/// orders differ for nested vectors and are observable in serialized .eln
/// constants.
fn collect_print_sharing(
    interp: &Interpreter,
    value: &Value,
    options: PrintOptions,
    counts: &mut HashMap<PrintRefKey, usize>,
    labels: &mut HashMap<PrintRefKey, PrintLabel>,
    next_label: &mut usize,
) -> Result<(), LispError> {
    walk_print_graph(interp, value, options, |key, object| {
        let count = counts.entry(key.clone()).or_insert(0);
        *count += 1;
        if *count == 2 {
            labels.insert(
                key,
                PrintLabel {
                    number: *next_label,
                    printed: false,
                    object: *object,
                },
            );
            *next_label += 1;
        }
        *count == 1
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
    let mut pending = vec![*value];
    while let Some(value) = pending.pop() {
        if let Some(key) = print_ref_key(interp, &value, options)
            && !visit(key, &value)
        {
            continue;
        }

        match value.kind() {
            Kind::Vector(_) | Kind::Cons(_) if is_vector_value(&value) => {
                let items = vector_items(&value)?;
                pending.extend(items.into_iter().rev());
            }
            Kind::Cons(_) => {
                let Some((car, cdr)) = value.cons_values() else {
                    continue;
                };
                pending.push(cdr);
                pending.push(car);
            }
            Kind::StringObject(state) => {
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
            Kind::HashTable(table) => {
                for slot in (0..table.capacity()).rev() {
                    if let Some((key, value)) = table.entry(slot) {
                        pending.push(value);
                        pending.push(key);
                    }
                }
            }
            Kind::Record(id) => {
                if let Some(fields) = record_prin1_fields(interp, id.id) {
                    pending.extend(fields.into_iter().rev());
                }
            }
            Kind::LispRecord(record) => pending.extend(record.slots().rev()),
            Kind::CharTable(table) => pending.extend(table.slots().rev()),
            Kind::SubCharTable(table) => pending.extend(table.slots().rev()),
            Kind::Lambda(lambda) => {
                pending.extend(interp.interpreted_closure_slots(&lambda).into_iter().rev());
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

    // Fprint_preprocess resets the process-local counter on every call,
    // independently of `print-number-table'.
    interp.print_number_index = 0;

    let table = match interp.lookup_var("print-number-table", env) {
        Some(existing) if json::is_hash_table(interp, &existing) => existing,
        _ => json::make_hash_table(interp, "eq", Vec::new()),
    };
    // GNU's Vprint_number_table is a real special variable.  Update its
    // active dynamic binding, not merely a lexical frame passed to the
    // primitive.
    interp.set_variable("print-number-table", table, env);

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
                object.kind(),
                Kind::Symbol(symbol) if crate::lisp::types::is_uninterned_symbol(&symbol)
            );
        if let Some(index) = positions.get(&key).copied() {
            let state = &entries[index].1;
            if state.is_truthy() || continuous_gensym {
                if matches!(state.kind(), Kind::Nil | Kind::T | Kind::Symbol(_)) {
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
        entries.push((*object, state));
        descend
    })?;

    set_hash_table_entries(interp, &table, entries)?;
    interp.print_number_index = usize::try_from(number_index).unwrap_or(usize::MAX);
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
    let mut tortoise = *value;
    let mut tortoise_countdown: i64 = 2;
    let mut tortoise_period: i64 = 2;
    let mut tortoise_index: i64 = 0;
    let mut tail = cdr;
    loop {
        if is_vector_value(&tail) {
            let tail_rendered = render_prin1_with_context(interp, &tail, env, context, depth + 1)?;
            return Ok(format!("({} . {})", rendered.join(" "), tail_rendered));
        }
        match tail.kind() {
            Kind::Nil => return Ok(format!("({})", rendered.join(" "))),
            Kind::Cons(_) => {
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
                        tortoise = tail;
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
                    render_prin1_with_context(interp, &other.value(), env, context, depth + 1)?;
                return Ok(format!("({} . {})", rendered.join(" "), tail_rendered));
            }
        }
    }
}

fn same_cons_cell(left: &Value, right: &Value) -> bool {
    match (left.kind(), right.kind()) {
        (Kind::Cons(left), Kind::Cons(right)) => {
            crate::lisp::types::ConsCell::identity(&left)
                == crate::lisp::types::ConsCell::identity(&right)
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
        && matches!(value.kind(), Kind::Symbol(symbol) if crate::lisp::types::is_uninterned_symbol(&symbol))
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
                    object: *value,
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
        // PRINT_CIRCLE levels rather than exhausting the C stack; with
        // `print-circle' non-nil GNU prints ANY depth (its object walk
        // is explicitly iterative), so the guard must not fire there.
        if !context.options.circle && depth >= PRINT_CIRCLE_DEPTH_LIMIT {
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
    let code = match value.kind() {
        Kind::Integer(value) => value,
        Kind::BigInteger(value) => value.to_i64()?,
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

/// print.c print_prune_string_charset: a string's `charset' properties
/// print only when `print-charset-text-property' is t, or when it is
/// `default' and some property is "unsafe" -- a non-ASCII character in
/// its span belongs (by CHAR_CHARSET) to a different charset.  The
/// decision is made once for the whole string.
pub(crate) fn charset_text_properties_print(
    interp: &Interpreter,
    env: &Env,
    text: &str,
    multibyte: bool,
    props: &[StringPropertySpan],
) -> bool {
    let setting = interp
        .lookup_var("print-charset-text-property", env)
        .unwrap_or(Value::Nil);
    if setting.is_nil() {
        return false;
    }
    if matches!(setting.kind(), Kind::T) {
        return true;
    }
    let ordered = interp.charset_priority_list();
    let head = interp.charset_non_preferred_head();
    let chars: Vec<char> = text.chars().collect();
    props.iter().any(|span| {
        span.props.iter().any(|(name, value)| {
            if name != "charset" {
                return false;
            }
            let Ok(charset) = value.as_symbol() else {
                return true;
            };
            let expected = interp
                .charset_canonical_name(charset)
                .unwrap_or_else(|| charset.to_string());
            chars
                .iter()
                .skip(span.start)
                .take(span.end.saturating_sub(span.start))
                .any(|ch| {
                    if ch.is_ascii() {
                        return false;
                    }
                    // fetch_string_char_advance: a unibyte string yields
                    // its bytes as characters (0xE9 is U+00E9 there), a
                    // multibyte one its characters, raw bytes included.
                    let code = match raw_byte_from_regex_char(*ch) {
                        Some(byte) if !multibyte => u32::from(byte),
                        Some(byte) => RAW_BYTE_REGEX_BASE + u32::from(byte),
                        None => *ch as u32,
                    };
                    char_charset_ordered(interp, &ordered, head.as_deref(), code).0 != expected
                })
        })
    })
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
    let table = check_hash_table(value)?;
    let test = table.test_name();
    let weakness = hash_table_weakness_value(table);
    let purecopy = table.purecopy();
    let entries = json::hash_table_entries(interp, value)
        .map(|(_, entries)| entries)
        .unwrap_or_default();

    let mut rendered = String::from("#s(hash-table");
    if !matches!(test.kind(), Kind::Symbol(name) if name == "eql") {
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
    if purecopy {
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
        let rendered = call_function_value(interp, &function, &[*value, Value::T], env)?;
        if matches!(rendered.kind(), Kind::T) {
            return Ok(Some(String::new()));
        }
        if rendered.is_nil() {
            return Ok(None);
        }
        Ok(Some(string_text(&rendered)?))
    };

    // print.c's print_object for a two-element list headed by Qquote,
    // Qfunction, Qbackquote (the symbol named "`"), Qcomma (",") or
    // Qcomma_at (",@") under `print-quoted'.  The symbols named
    // `backquote', `comma' and `comma-at' are ordinary.  The comma
    // shorthand appears only inside a printed backquote
    // (`new_backquote_output'), and consumes one level of it.
    if context.options.quoted
        && let Some((head, rest)) = value.cons_values()
        && let Kind::Symbol(symbol) = head.kind()
        && let Some((inner, tail)) = rest.cons_values()
        && tail.is_nil()
    {
        let quoted = match symbol.as_str() {
            "quote" => Some(("'", 0)),
            "function" | "function-quote" => Some(("#'", 0)),
            "`" => Some(("`", 1)),
            "," if context.backquote_output > 0 => Some((",", -1)),
            ",@" if context.backquote_output > 0 => Some((",@", -1)),
            _ => None,
        };
        if let Some((prefix, nesting)) = quoted {
            // GNU's print-quoted syntax replaces the (quote INNER) wrapper;
            // it does not charge that elided cons level against print-level.
            // Passing depth + 1 here truncates one level too early (for
            // example, print-level 1 would render '(a) as '...).
            context.backquote_output = context.backquote_output.wrapping_add_signed(nesting);
            let rendered = render_prin1_with_context(interp, &inner, env, context, depth);
            context.backquote_output = context.backquote_output.wrapping_add_signed(-nesting);
            return Ok(format!("{prefix}{}", rendered?));
        }
    }

    match value.kind() {
        Kind::Integer(_) | Kind::BigInteger(_) if context.options.integers_as_characters => {
            let rendered = if context.options.escape {
                render_prin1_integer_as_character(value)
            } else {
                render_princ_integer_as_character(value)
            };
            Ok(rendered.unwrap_or_else(|| value.to_string()))
        }
        Kind::String(text) if !context.options.escape => Ok(render_princ_string(
            interp,
            &text,
            string_argument_multibyte(value),
            env,
            context.options.output_is_function,
        )),
        Kind::String(text) => Ok(render_prin1_string(
            interp,
            &text,
            string_argument_multibyte(value),
            env,
        )),
        Kind::StringObject(state) if !context.options.escape => {
            let state = state.borrow();
            Ok(render_princ_string(
                interp,
                &state.text,
                state.multibyte,
                env,
                context.options.output_is_function,
            ))
        }
        Kind::StringObject(state) => {
            let (text, props, multibyte) = {
                let state = state.borrow();
                (state.text.clone(), state.props.clone(), state.multibyte)
            };
            if props.is_empty() {
                return Ok(render_prin1_string(interp, &text, multibyte, env));
            }
            let mut rendered = vec![render_prin1_string(interp, &text, multibyte, env)];
            let mut field_values = Vec::new();
            let keep_charset = charset_text_properties_print(interp, env, &text, multibyte, &props);
            for span in props {
                let filtered_props = span
                    .props
                    .iter()
                    .filter(|(name, _)| name != "charset" || keep_charset)
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
                return Ok(render_prin1_string(interp, &text, multibyte, env));
            }
            Ok(format!("#({})", rendered.join(" ")))
        }
        Kind::Symbol(symbol) if context.options.escape && symbol == "`" => Ok("\\`".into()),
        Kind::Symbol(symbol) if context.options.escape && symbol == "," => Ok("\\,".into()),
        Kind::Symbol(symbol) if context.options.escape && symbol == ",@" => Ok("\\,@".into()),
        Kind::Symbol(symbol) => Ok(render_prin1_symbol(&symbol, context.options)),
        Kind::Vector(_) | Kind::Cons(_) if is_vector_value(value) => {
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
        Kind::Cons(_) => render_prin1_list(interp, value, env, context, depth),
        Kind::Lambda(lambda_value) => {
            if let Some(rendered) = unreadable_override(interp, value, env)? {
                return Ok(rendered);
            }
            // GNU print.c prints every stored closure slot. Capture filtering
            // belongs to cconv.el at construction, never to the printer.
            let slots = interp.interpreted_closure_slots(&lambda_value);
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
            Ok(format!("#[{}]", rendered_slots.join(" ")))
        }
        Kind::CharTable(table) => {
            let mut fields = Vec::new();
            for (index, field) in table.slots().enumerate() {
                if context.options.length.is_some_and(|limit| index >= limit) {
                    fields.push("...".into());
                    break;
                }
                fields.push(render_prin1_with_context(
                    interp,
                    &field,
                    env,
                    context,
                    depth + 1,
                )?);
            }
            Ok(format!("#^[{}]", fields.join(" ")))
        }
        Kind::SubCharTable(table) => {
            let mut fields = vec![table.depth().to_string(), table.min_char().to_string()];
            for field in table.slots() {
                if context
                    .options
                    .length
                    .is_some_and(|limit| fields.len() >= limit)
                {
                    fields.push("...".into());
                    break;
                }
                fields.push(render_prin1_with_context(
                    interp,
                    &field,
                    env,
                    context,
                    depth + 1,
                )?);
            }
            Ok(format!("#^^[{}]", fields.join(" ")))
        }
        Kind::BuiltinFunc(_) | Kind::Buffer(_) | Kind::Marker(_) | Kind::Overlay(_) => {
            if let Some(rendered) = unreadable_override(interp, value, env)? {
                return Ok(rendered);
            }
            match value.kind() {
                Kind::BuiltinFunc(name) => Ok(format!("#<subr {name}>")),
                Kind::Buffer(buffer) => Ok(match interp.get_buffer_by_id(buffer.id) {
                    Some(live) if context.options.escape => format!("#<buffer {}>", live.name),
                    Some(live) => live.name.clone(),
                    None => "#<killed buffer>".into(),
                }),
                Kind::Marker(marker) => Ok(match marker.buffer() {
                    Some(buffer) => {
                        let buffer_name = &buffer.borrow().name;
                        let position = marker.last_position();
                        let advances = if marker.insertion_type() {
                            " (moves after insertion)"
                        } else {
                            ""
                        };
                        format!("#<marker{advances} at {position} in {buffer_name}>")
                    }
                    None => "#<marker in no buffer>".into(),
                }),
                _ => Ok(value.to_string()),
            }
        }
        Kind::Frame(frame) => {
            let id = frame.identity();
            let name = string_text(&frame.name.get())
                .unwrap_or_else(|_| format!("F{}", frame.borrow().id));
            Ok(format!("#<frame {name} 0x{id:x}>"))
        }
        Kind::Terminal(terminal) => {
            let state = terminal.borrow();
            Ok(if state.live {
                format!("#<terminal {} on {}>", terminal.id, state.name)
            } else {
                format!("#<terminal {}>", terminal.id)
            })
        }
        Kind::SymbolWithPos(object) => {
            let symbol = object.symbol();
            let position = object.position();
            if context.options.symbols_bare {
                return render_prin1_with_context(interp, &symbol, env, context, depth);
            }
            let rendered_symbol =
                render_prin1_with_context(interp, &symbol, env, context, depth + 1)?;
            Ok(format!("#<symbol {rendered_symbol} at {position}>"))
        }
        Kind::LispRecord(record) => {
            let mut fields = Vec::new();
            for (index, field) in record.slots().enumerate() {
                if context.options.length.is_some_and(|limit| index >= limit) {
                    fields.push("...".into());
                    break;
                }
                fields.push(render_prin1_with_context(
                    interp,
                    &field,
                    env,
                    context,
                    depth + 1,
                )?);
            }
            Ok(format!("#s({})", fields.join(" ")))
        }
        Kind::HashTable(_) => render_hash_table_prin1(interp, value, env, context, depth),
        Kind::Record(id) => {
            if let Some(record) = interp.find_record(id) {
                let rendered = match record.kind {
                    crate::lisp::eval::RecordKind::ModuleFunction => {
                        crate::lisp::modules::print_function(interp, id.id)
                    }
                    crate::lisp::eval::RecordKind::UserPointer => {
                        crate::lisp::modules::print_user_pointer(interp, id.id)
                    }
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
                        .thread_name(id.id)
                        .map(|name| format!("#<thread {name}>"))
                        .unwrap_or_else(|| format!("#<thread 0x{:x}>", id.identity())),
                    crate::lisp::eval::RecordKind::Mutex => interp
                        .mutex_name(id.id)
                        .map(|name| format!("#<mutex {name}>"))
                        .unwrap_or_else(|| format!("#<mutex 0x{:x}>", id.identity())),
                    crate::lisp::eval::RecordKind::ConditionVariable => interp
                        .condition_variable_name(id.id)
                        .map(|name| format!("#<condvar {name}>"))
                        .unwrap_or_else(|| format!("#<condvar 0x{:x}>", id.identity())),
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
                            .process_name(id.id)
                            .unwrap_or_else(|| format!("0x{:x}", id.identity()));
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
                        let Some(fields) = record_prin1_fields(interp, id.id) else {
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

pub(crate) fn finish_print_number_table(
    interp: &mut Interpreter,
    env: &mut Env,
    context: &PrintContext,
) -> Result<(), LispError> {
    if !context.options.circle || !context.options.continuous_numbering {
        return Ok(());
    }
    interp.print_number_index = context.next_label.saturating_sub(1);
    if context.number_table.is_none() && context.counts.is_empty() {
        return Ok(());
    }
    let table = context
        .number_table
        .unwrap_or_else(|| json::make_hash_table(interp, "eq", Vec::new()));
    let mut entries = json::hash_table_entries(interp, &table)
        .map(|(_, entries)| entries)
        .unwrap_or_default();
    entries.retain(|(_, state)| !matches!(state.kind(), Kind::Integer(_)));
    let mut labels = context.labels.values().collect::<Vec<_>>();
    labels.sort_by_key(|label| label.number);
    entries.extend(labels.into_iter().map(|label| {
        let number = i64::try_from(label.number).unwrap_or(i64::MAX);
        let state = if label.printed { number } else { -number };
        (label.object, Value::Integer(state))
    }));
    set_hash_table_entries(interp, &table, entries)?;
    // This is a native special variable, so update its active dynamic value
    // cell.  Writing only the evaluator frame makes the table disappear at a
    // function boundary (and leaks a fake lexical binding to the caller).
    interp.set_variable("print-number-table", table, env);
    Ok(())
}

fn prepare_print_numbering(interp: &mut Interpreter, env: &mut Env, options: PrintOptions) {
    // print.c resets both pieces of state unless continuous numbering has a
    // live public table.  A nil table therefore starts numbering over even
    // when `print-continuous-numbering' itself is non-nil.
    if !options.continuous_numbering
        || interp
            .lookup_var("print-number-table", env)
            .is_none_or(|value| value.is_nil())
    {
        interp.print_number_index = 0;
        interp.set_variable("print-number-table", Value::Nil, env);
    }
}

pub(crate) fn render_prin1(
    interp: &mut Interpreter,
    value: &Value,
    env: &mut crate::lisp::types::Env,
) -> Result<String, LispError> {
    with_printer_buffer_escape(interp, env, Some(true), |interp, env| {
        render_printer_object(interp, value, env, true, false)
    })
}

/// print.c:print_prepare binds the escape flag required by a buffer's
/// encoding. Function and stdout streams do not impose either flag.
pub(crate) fn with_printer_buffer_escape<T>(
    interp: &mut Interpreter,
    env: &mut Env,
    multibyte: Option<bool>,
    body: impl FnOnce(&mut Interpreter, &mut Env) -> Result<T, LispError>,
) -> Result<T, LispError> {
    let Some(multibyte) = multibyte else {
        return body(interp, env);
    };
    let name = if multibyte {
        "print-escape-nonascii"
    } else {
        "print-escape-multibyte"
    };
    if interp
        .lookup_var(name, env)
        .is_some_and(|value| value.is_truthy())
    {
        return body(interp, env);
    }
    let restore = interp.bind_special_dynamic(name, Value::T, env)?;
    let result = body(interp, env);
    let restored = interp.restore_special_dynamic(restore, env);
    restored.and(result)
}

pub(crate) fn render_printer_object(
    interp: &mut Interpreter,
    value: &Value,
    env: &mut Env,
    escape: bool,
    output_is_function: bool,
) -> Result<String, LispError> {
    let mut options = print_options(interp, env);
    options.escape = escape;
    options.output_is_function = output_is_function;
    prepare_print_numbering(interp, env, options);
    let mut context = PrintContext::new(interp, value, env, options)?;
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
    render_printer_object(interp, value, env, false, false)
}

pub(crate) fn render_prin1_ephemeral(
    interp: &mut Interpreter,
    value: &Value,
    env: &crate::lisp::types::Env,
) -> Result<String, LispError> {
    let mut env = env.clone();
    render_prin1(interp, value, &mut env)
}

/// lread.c:end_of_file_error: `(end-of-file FILE)' while a file is being
/// loaded (`load-true-file-name' a string), else `(end-of-file)'.
fn end_of_file_error(interp: &Interpreter, env: &Env) -> LispError {
    match interp
        .lookup_var("load-true-file-name", env)
        .map(|v| v.kind())
    {
        Some(file @ Kind::String(_)) => LispError::SignalValue(Value::list([
            Value::Symbol("end-of-file".into()),
            file.value(),
        ])),
        _ => LispError::EndOfInput(),
    }
}

/// lread.c:readchar returns bytes 0..255 from an unibyte string, rather
/// than the byte8 character codes it returns from an unibyte buffer.
/// The reader must therefore see literal Latin-1 characters in this source;
/// multibyte raw-byte characters and explicit escapes keep their meaning.
pub(crate) fn reader_string_source_text(source: &Value) -> Result<String, LispError> {
    let text = string_text(source)?;
    if string_argument_multibyte(source) || text.is_ascii() {
        return Ok(text);
    }
    Ok(text
        .chars()
        .map(|character| raw_byte_from_regex_char(character).map_or(character, char::from))
        .collect())
}

pub(crate) fn read_one_form_in_env(
    interp: &mut Interpreter,
    text: &str,
    env: &mut Env,
) -> Result<(Value, usize), LispError> {
    let symbol_shorthands = read_symbol_shorthands_in_env(interp, env)?;
    let mut reader = crate::lisp::reader::Reader::with_symbol_shorthands(text, symbol_shorthands);
    let value = match reader.read()? {
        Some(value) => value,
        None => return Err(end_of_file_error(interp, env)),
    };
    let value = interp.intern_read_symbols_in_value(value, env)?;
    // Allocate identity-bearing objects before resolving references into
    // them. The parser-only resolver could not construct cyclic closures
    // or records and performed a second, competing graph reconstruction.
    let value = if reader.emitted_reader_forms() {
        interp.materialize_read_object_literals(value, env)?
    } else {
        value
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
    match source.kind() {
        Kind::Buffer(_) => {
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
            let consumed = match result.as_ref().map_err(LispError::kind) {
                Ok((_, consumed)) => *consumed,
                Err(LispErrorKind::EndOfInput) => text.chars().count(),
                Err(_) => 0,
            };
            if let Some(mut buffer) = interp.get_buffer_by_id_mut(buffer_id) {
                buffer.goto_char((start + consumed).min(end));
            }
            result.map(|(value, _)| value)
        }
        Kind::Marker(id) => {
            let (buffer_id, start) = {
                let marker = id;
                let buffer_id = marker
                    .buffer()
                    .map(|buffer| buffer.id)
                    .ok_or_else(|| LispError::Signal("Marker does not point anywhere".into()))?;
                let start = marker
                    .position()
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
            // lread.c:read_internal_start uses an absolute base only for
            // BUFFERP. Marker streams count from zero for each read.
            let result = read_one_positioned_form(interp, env, &text, 0);
            let consumed = match result.as_ref().map_err(LispError::kind) {
                Ok((_, consumed)) => *consumed,
                Err(LispErrorKind::EndOfInput) => text.chars().count(),
                Err(_) => 0,
            };
            interp.set_marker(id, Some((start + consumed).min(end)), Some(buffer_id))?;
            result.map(|(value, _)| value)
        }
        Kind::String(_) | Kind::StringObject(_) => {
            let text = reader_string_source_text(source)?;
            read_one_positioned_form(interp, env, &text, 0).map(|(value, _)| value)
        }
        // lread.c:readchar calls the remaining stream objects, including
        // bytecode closures, through the ordinary function dispatcher.
        _ => read_callable_source(interp, source, env, true),
    }
}

fn read_one_positioned_form(
    interp: &mut Interpreter,
    env: &mut Env,
    text: &str,
    base_position: i64,
) -> Result<(Value, usize), LispError> {
    // read0 with LOCATE_SYMS: the reader itself wraps each symbol
    // occurrence with its character position (the retired token-stream
    // zip desynced on any non-symbol token — a number, `t' — and then
    // silently dropped every later position; bytecomp warnings inherited
    // the enclosing defun's position instead of the offending form's).
    let symbol_shorthands = read_symbol_shorthands_in_env(interp, env)?;
    let obarray = interp.lookup_var("obarray", env).unwrap_or(Value::Nil);
    let (value, consumed, unescaped) = {
        let mut resolve_symbol = |name: &str| intern_in_obarray(interp, &obarray, name);
        let mut reader = crate::lisp::reader::Reader::with_positioned_symbols(
            text,
            symbol_shorthands,
            base_position,
        )
        .with_symbol_resolver(&mut resolve_symbol);
        let value = reader.read()?;
        let consumed = text[..reader.position()].chars().count();
        let unescaped = reader
            .unescaped_character_literals()
            .map(Value::Integer)
            .collect::<Vec<_>>();
        (value, consumed, unescaped)
    };
    let value = value.ok_or_else(|| end_of_file_error(interp, env))?;
    interp.set_variable(
        "lread--unescaped-character-literals",
        Value::list(unescaped),
        env,
    );
    // GNU's reader constructs `#s(...)', `#^[...]', and bool-vector objects
    // before read-positioning-symbols returns.  In particular, the byte
    // compiler must receive an actual hash table constant rather than
    // Emaxx's parser-private ReaderForm marker.
    let value = interp.materialize_read_object_literals(value, env)?;
    Ok((value, consumed))
}

pub(crate) fn record_literal_items(value: &Value) -> Option<Vec<Value>> {
    let Kind::ReaderForm(form) = value.kind() else {
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
        && let [Kind::Symbol(symbol), inner] = items
            .as_slice()
            .iter()
            .map(|v| v.kind())
            .collect::<Vec<_>>()
            .as_slice()
        && symbol == "quote"
    {
        return inner.value();
    }
    *value
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
            *object,
            *idx_value,
        ]))
    })?;
    Ok(record_literal_slot_data(&slot))
}

pub(crate) fn read_from_callable_source(
    interp: &mut Interpreter,
    source: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    read_callable_source(interp, source, env, false)
}

struct CallableReader<'a> {
    interp: &'a mut Interpreter,
    source: Value,
    env: &'a mut Env,
}

impl CallableReader<'_> {
    fn call(&mut self, args: &[Value]) -> Result<Value, LispError> {
        // GNU call0/call1 resolve a symbol stream on every invocation. The
        // callback can redefine that function, including during unread.
        let callable = resolve_callable(self.interp, &self.source, self.env)?;
        self.interp
            .call_function_value(callable, self.source.as_symbol().ok(), args, self.env)
    }
}

impl crate::lisp::reader::ReaderStream for CallableReader<'_> {
    fn read_character(&mut self) -> Result<Option<i64>, LispError> {
        match self.call(&[])?.kind() {
            Kind::Nil => Ok(None),
            Kind::Integer(code) if code < 0 => Ok(None),
            Kind::Integer(code) => Ok(Some(code)),
            other => Err(LispError::WrongTypeArgument(
                "integerp".into(),
                other.value(),
            )),
        }
    }

    fn unread_character(&mut self, character: i64) -> Result<(), LispError> {
        self.call(&[Value::Integer(character)]).map(|_| ())
    }

    fn intern_symbol(&mut self, name: &str, shorthand: bool) -> Result<Value, LispError> {
        let name = if shorthand {
            let shorthands = read_symbol_shorthands_in_env(self.interp, self.env)?;
            crate::lisp::reader::apply_symbol_shorthands_to_token(name.to_owned(), &shorthands)
        } else {
            name.to_owned()
        };
        let obarray = self
            .interp
            .lookup_var("obarray", self.env)
            .unwrap_or(Value::Nil);
        intern_in_obarray(self.interp, &obarray, &name)
    }
}

fn read_callable_source(
    interp: &mut Interpreter,
    source: &Value,
    env: &mut Env,
    locate_symbols: bool,
) -> Result<Value, LispError> {
    let (value, unescaped, needs_materialization) = {
        let mut stream = CallableReader {
            interp,
            source: *source,
            env,
        };
        let mut reader = crate::lisp::reader::Reader::from_stream(&mut stream, locate_symbols);
        let value = reader.read()?;
        reader.finish_stream()?;
        let unescaped = reader
            .unescaped_character_literals()
            .map(Value::Integer)
            .collect::<Vec<_>>();
        (value, unescaped, reader.emitted_reader_forms())
    };
    let value = value.ok_or_else(|| end_of_file_error(interp, env))?;
    interp.set_variable(
        "lread--unescaped-character-literals",
        Value::list(unescaped),
        env,
    );
    if needs_materialization {
        interp.materialize_read_object_literals(value, env)
    } else {
        Ok(value)
    }
}

pub(crate) fn read_from_lisp_source(
    interp: &mut Interpreter,
    source: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    read_from_lisp_source_raw(interp, source, env)
}

// GNU's reader constructs real hash tables for `#s(hash-table ...)' input;
// emaxx's reader leaves a quoted literal form behind, which is fine for
// loaded code but wrong for `read' consumers that treat the result as data
// (e.g. `eieio-persistent-read').  Convert those literals into hash-table
// records after reading.
pub(crate) fn materialize_read_hash_table_literals(
    interp: &mut Interpreter,
    value: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    let mut seen = HashSet::new();
    materialize_hash_table_literals_inner(interp, value, env, &mut seen)
}

pub(crate) fn materialize_read_hash_table_literal_fields(
    interp: &mut Interpreter,
    fields: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    hash_table_from_literal_fields(interp, fields, env, &mut HashSet::new())
}

const CHAR_TABLE_STANDARD_SLOTS: usize = 68;

// Reader forms become the actual root/subtable graph. Each object is
// published before its fields so repeated references keep their identity.
pub(crate) fn materialize_read_char_table_literals(
    interp: &mut Interpreter,
    value: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    materialize_char_table_literals_inner(interp, value, env, &mut HashMap::new())
}

fn materialize_char_table_literals_inner(
    interp: &mut Interpreter,
    value: &Value,
    env: &mut Env,
    seen: &mut HashMap<usize, Value>,
) -> Result<Value, LispError> {
    if let Some(copy) = seen.get(&value.word()) {
        return Ok(*copy);
    }
    if let Kind::ReaderForm(form) = value.kind() {
        let (copy, fields, skip) = match form.as_ref() {
            crate::lisp::types::ReaderForm::CharTable { fields } => {
                if !(CHAR_TABLE_STANDARD_SLOTS
                    ..=crate::lisp::alloc::vectors::char_tables::CHAR_TABLE_MAX_SLOTS)
                    .contains(&fields.len())
                {
                    return Err(LispError::ReadError("invalid size char-table".into()));
                }
                (
                    Value::CharTable(crate::lisp::types::CharTableRef::new(
                        Value::Nil,
                        Value::Nil,
                        fields.len() - CHAR_TABLE_STANDARD_SLOTS,
                    )),
                    fields,
                    0,
                )
            }
            crate::lisp::types::ReaderForm::SubCharTable { fields } => {
                let depth = fields[0].as_integer()? as usize;
                let minimum = fields[1].as_integer()? as u32;
                (
                    Value::SubCharTable(crate::lisp::types::SubCharTableRef::new(
                        depth,
                        minimum,
                        Value::Nil,
                    )),
                    fields,
                    2,
                )
            }
            _ => return Ok(*value),
        };
        seen.insert(value.word(), copy);
        for (index, field) in fields.iter().skip(skip).enumerate() {
            let field = interp.materialize_read_record_literals(field, env)?;
            let field = materialize_read_hash_table_literals(interp, &field, env)?;
            let field = materialize_char_table_literals_inner(interp, &field, env, seen)?;
            match copy.kind() {
                Kind::CharTable(table) => table.set_slot(index, field),
                Kind::SubCharTable(table) => table.set_slot(index, field),
                _ => unreachable!(),
            }
        }
        return Ok(copy);
    }
    seen.insert(value.word(), *value);
    match value.kind() {
        Kind::Vector(vector) => {
            for (index, field) in vector.slots().enumerate() {
                vector.set(
                    index,
                    materialize_char_table_literals_inner(interp, &field, env, seen)?,
                );
            }
        }
        Kind::Cons(cell) => {
            cell.car.set(materialize_char_table_literals_inner(
                interp,
                &cell.car.get(),
                env,
                seen,
            )?);
            cell.cdr.set(materialize_char_table_literals_inner(
                interp,
                &cell.cdr.get(),
                env,
                seen,
            )?);
        }
        Kind::CharTable(table) => {
            for (index, field) in table.slots().enumerate() {
                table.set_slot(
                    index,
                    materialize_char_table_literals_inner(interp, &field, env, seen)?,
                );
            }
        }
        Kind::SubCharTable(table) => {
            for (index, field) in table.slots().enumerate() {
                table.set_slot(
                    index,
                    materialize_char_table_literals_inner(interp, &field, env, seen)?,
                );
            }
        }
        _ => {}
    }
    Ok(*value)
}

fn quoted_hash_table_literal_fields(value: &Value) -> Option<Vec<Value>> {
    let items = value.to_vec().ok()?;
    let kinds = items.iter().map(|v| v.kind()).collect::<Vec<_>>();
    let [Kind::Symbol(head), literal] = kinds.as_slice() else {
        return None;
    };
    if head != "quote" {
        return None;
    }
    match *literal {
        Kind::ReaderForm(form) => match form.as_ref() {
            crate::lisp::types::ReaderForm::HashTable { fields } => Some(fields.clone()),
            _ => None,
        },
        _ => None,
    }
}

fn bare_hash_table_literal_fields(value: &Value) -> Option<Vec<Value>> {
    match value.kind() {
        Kind::ReaderForm(form) => match form.as_ref() {
            crate::lisp::types::ReaderForm::HashTable { fields } => Some(fields.clone()),
            _ => None,
        },
        _ => None,
    }
}

fn materialize_hash_table_literals_inner(
    interp: &mut Interpreter,
    value: &Value,
    env: &mut Env,
    seen: &mut HashSet<usize>,
) -> Result<Value, LispError> {
    if let Some(fields) = quoted_hash_table_literal_fields(value) {
        return hash_table_from_literal_fields(interp, &fields, env, seen);
    }
    if let Some(fields) = bare_hash_table_literal_fields(value) {
        return hash_table_from_literal_fields(interp, &fields, env, seen);
    }
    if let Kind::Vector(vector) = value.kind() {
        if !seen.insert(vector.identity()) {
            return Ok(*value);
        }
        let slots = vector.slots().collect::<Vec<_>>();
        for (index, slot) in slots.iter().enumerate() {
            vector.set(
                index,
                materialize_hash_table_literals_inner(interp, slot, env, seen)?,
            );
        }
        return Ok(*value);
    }
    let Some((car_cell, cdr_cell)) = (value).cons_cells() else {
        return Ok(*value);
    };
    let ptr = car_cell.cell_id();
    if !seen.insert(ptr) {
        return Ok(*value);
    }
    let car = car_cell.get();
    let new_car = materialize_hash_table_literals_inner(interp, &car, env, seen)?;
    car_cell.set(new_car);
    let cdr = cdr_cell.get();
    let new_cdr = materialize_hash_table_literals_inner(interp, &cdr, env, seen)?;
    cdr_cell.set(new_cdr);
    Ok(*value)
}

fn hash_table_from_literal_fields(
    interp: &mut Interpreter,
    fields: &[Value],
    env: &mut Env,
    _seen: &mut HashSet<usize>,
) -> Result<Value, LispError> {
    let mut test = "eql".to_string();
    let mut weakness = Value::Nil;
    let mut purecopy = Value::Nil;
    let mut entries = Vec::new();
    let mut index = 0usize;
    while index + 1 < fields.len() {
        let Ok(key) = fields[index].as_symbol() else {
            index += 2;
            continue;
        };
        let key = key.to_string();
        let field_value = fields[index + 1];
        match key.as_str() {
            "test" => test = field_value.as_symbol()?.to_string(),
            "weakness" => weakness = field_value,
            "purecopy" => purecopy = field_value,
            "data" => {
                let items = field_value.to_vec()?;
                let mut cursor = 0usize;
                while cursor + 1 < items.len() {
                    let entry_key = interp.materialize_read_object_literals(items[cursor], env)?;
                    let entry_value =
                        interp.materialize_read_object_literals(items[cursor + 1], env)?;
                    entries.push((entry_key, entry_value));
                    cursor += 2;
                }
            }
            _ => {}
        }
        index += 2;
    }
    // lread.c:hash_table_from_plist ignores serialized size/rehash metadata,
    // creates the table with exactly one slot per DATA pair, then calls
    // Fputhash for every pair.  In particular, a user-defined test runs its
    // hash/comparison functions here, while the object is read; delaying that
    // work until the first lookup changes both side effects and bucket state.
    let capacity = entries.len();
    let table = make_hash_table_value(
        interp,
        Value::symbol(&test),
        capacity,
        weakness,
        purecopy,
        env,
    )?;
    let object = check_hash_table(&table)?;
    for (key, value) in entries {
        hash_table_put(interp, object, key, value, env)?;
    }
    Ok(table)
}

fn read_from_lisp_source_raw(
    interp: &mut Interpreter,
    source: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    match source.kind() {
        Kind::Buffer(_) => {
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
            let consumed = match result.as_ref().map_err(LispError::kind) {
                Ok((_, consumed)) => *consumed,
                Err(LispErrorKind::EndOfInput) => text.chars().count(),
                Err(_) => 0,
            };
            if let Some(mut buffer) = interp.get_buffer_by_id_mut(buffer_id) {
                buffer.goto_char((start + consumed).min(end));
            }
            result.map(|(value, _)| value)
        }
        Kind::Marker(id) => {
            let (buffer_id, start) = {
                let marker = id;
                let buffer_id = marker
                    .buffer()
                    .map(|buffer| buffer.id)
                    .ok_or_else(|| LispError::Signal("Marker does not point anywhere".into()))?;
                let start = marker
                    .position()
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
            let consumed = match result.as_ref().map_err(LispError::kind) {
                Ok((_, consumed)) => *consumed,
                Err(LispErrorKind::EndOfInput) => text.chars().count(),
                Err(_) => 0,
            };
            interp.set_marker(id, Some((start + consumed).min(end)), Some(buffer_id))?;
            result.map(|(value, _)| value)
        }
        Kind::String(_) | Kind::StringObject(_) => {
            let s = reader_string_source_text(source)?;
            read_one_form_in_env(interp, &s, env).map(|(value, _)| value)
        }
        _ => read_from_callable_source(interp, source, env),
    }
}

/// fns.c:extract_data_from_object's MD5 coding policy. String endpoints
/// address the encoded string; buffer endpoints address the narrowed buffer.
pub(crate) fn md5_source_bytes(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Vec<u8>, LispError> {
    let source = &args[0];
    let start = args.get(1).unwrap_or(&Value::Nil);
    let end = args.get(2).unwrap_or(&Value::Nil);
    let mut coding = args.get(3).cloned().unwrap_or(Value::Nil);
    let noerror = args.get(4).is_some_and(Value::is_truthy);
    let validate = |interp: &Interpreter, coding: Value| -> Result<Value, LispError> {
        if coding.is_nil()
            || coding
                .as_symbol()
                .ok()
                .is_some_and(|name| interp.has_coding_system(name))
        {
            Ok(coding)
        } else if noerror {
            Ok(Value::symbol("raw-text"))
        } else {
            Err(LispError::SignalValue(Value::list([
                Value::symbol("coding-system-error"),
                coding,
            ])))
        }
    };
    let buffer_source = matches!(source.kind(), Kind::Buffer(_));
    let object = if buffer_source {
        let id = interp.resolve_buffer_id(source)?;
        let saved = interp.current_buffer_id();
        interp.set_current_buffer_id(id)?;
        let result = (|| {
            let buffer = interp.get_buffer_by_id(id).expect("resolved live buffer");
            let mut b = if start.is_nil() {
                buffer.point_min()
            } else {
                position_from_value(interp, start)?
            };
            let mut e = if end.is_nil() {
                buffer.point_max()
            } else {
                position_from_value(interp, end)?
            };
            if b > e {
                std::mem::swap(&mut b, &mut e);
            }
            if b < buffer.point_min() || e > buffer.point_max() {
                return Err(LispError::SignalValue(Value::list([
                    Value::symbol("args-out-of-range"),
                    *start,
                    *end,
                ])));
            }
            let multibyte = buffer.is_multibyte();
            drop(buffer);
            let b = Value::Integer(b as i64);
            let e = Value::Integer(e as i64);
            if coding.is_nil() {
                coding = interp
                    .lookup_var("coding-system-for-write", env)
                    .unwrap_or(Value::Nil);
                if coding.is_nil() {
                    let default = interp
                        .lookup_var("buffer-file-coding-system", env)
                        .unwrap_or(Value::Nil);
                    coding = default;
                    let local = call(
                        interp,
                        "local-variable-p",
                        &[Value::symbol("buffer-file-coding-system")],
                        env,
                    )?
                    .is_truthy();
                    let force_raw = (coding.is_nil() || !local) && !multibyte;
                    if !local {
                        coding = Value::Nil;
                    }
                    let filename = call(
                        interp,
                        "buffer-file-name",
                        std::slice::from_ref(source),
                        env,
                    )?;
                    if coding.is_nil() && filename.is_truthy() {
                        let selected = interp.call_function_value(
                            Value::symbol("find-operation-coding-system"),
                            Some("find-operation-coding-system"),
                            &[Value::symbol("write-region"), b, e, filename],
                            env,
                        )?;
                        if let Some((_, value)) = selected.cons_values()
                            && value.is_truthy()
                        {
                            coding = value;
                        }
                    }
                    if coding.is_nil() {
                        coding = default;
                    }
                    let selector = interp
                        .lookup_var("select-safe-coding-system-function", env)
                        .unwrap_or(Value::Nil);
                    if !force_raw
                        && call(interp, "fboundp", std::slice::from_ref(&selector), env)?
                            .is_truthy()
                    {
                        coding = interp.call_function_value(
                            selector,
                            None,
                            &[b, e, coding, Value::Nil],
                            env,
                        )?;
                    }
                    if force_raw {
                        coding = Value::symbol("raw-text");
                    }
                }
                coding = validate(interp, coding)?;
            }
            call(interp, "buffer-substring-no-properties", &[b, e], env)
        })();
        if interp.has_buffer_id(saved) {
            interp.set_current_buffer_id(saved)?;
        }
        result?
    } else if let Some(string) = string_like(source) {
        if coding.is_nil() {
            coding = if string.multibyte {
                interp
                    .coding_system_priority_list()
                    .first()
                    .map(|name| Value::symbol(name))
                    .unwrap_or(Value::Nil)
            } else {
                Value::symbol("raw-text")
            };
        }
        coding = validate(interp, coding)?;
        *source
    } else if matches!(source.kind(), Kind::Symbol(name) if name == "iv-auto") {
        if !matches!(start.kind(), Kind::Integer(number) if number >= 0) {
            return Err(LispError::Signal(
                "Without a length, `iv-auto' can't be used; see ELisp manual".into(),
            ));
        }
        return crate::lisp::primitives::text::secure_hash_source_bytes(
            interp,
            source,
            Some(start),
            Some(end),
        );
    } else {
        return Err(LispError::SignalValue(Value::list([
            Value::symbol("error"),
            Value::string("Invalid object argument"),
            if source.is_nil() {
                Value::string("nil")
            } else {
                *source
            },
        ])));
    };
    let object = if string_like(&object).expect("extracted string").multibyte {
        // Explicit buffer coding is checked by code_convert_string only
        // when the extracted string is multibyte. NOERROR does not alter
        // that check, and the requested alias is what gets recorded.
        let coding_name = if coding.is_nil() {
            None
        } else {
            let name = coding.as_symbol()?;
            if !interp.has_coding_system(name) {
                return Err(LispError::SignalValue(Value::list([
                    Value::symbol("coding-system-error"),
                    coding,
                ])));
            }
            Some(name)
        };
        encode_coding_value_recording(interp, &object, coding_name, false, buffer_source, env)?
    } else {
        object
    };
    let bytes = crate::lisp::primitives::text::internal_string_bytes(
        &string_like(&object).expect("encoded string"),
    )?;
    if buffer_source {
        return Ok(bytes);
    }
    let endpoint = |value: &Value, default: i64| -> Result<i64, LispError> {
        match value.kind() {
            Kind::Nil => Ok(default),
            Kind::Integer(index) => Ok(if index < 0 {
                index + bytes.len() as i64
            } else {
                index
            }),
            _ => Err(LispError::WrongTypeArgument("integerp".into(), *value)),
        }
    };
    let b = endpoint(start, 0)?;
    let e = endpoint(end, bytes.len() as i64)?;
    if b < 0 || b > e || e > bytes.len() as i64 {
        return Err(LispError::SignalValue(Value::list([
            Value::symbol("args-out-of-range"),
            object,
            *start,
            *end,
        ])));
    }
    Ok(bytes[b as usize..e as usize].to_vec())
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
        let object = Value::list([a, b, a, b]);
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
        cdr.set(object);
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
        let table = json::make_hash_table(&mut interp, "eq", vec![(existing, Value::Integer(-7))]);
        let object = Value::list([existing, repeated, repeated]);
        let mut env = preprocess_env(&mut interp, table);

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
