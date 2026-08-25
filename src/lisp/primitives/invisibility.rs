use crate::buffer::Buffer;
use crate::lisp::eval::Interpreter;
use crate::lisp::types::Value;

/// The resolved `buffer-invisibility-spec', xdisp.c's
/// TEXT_PROP_MEANS_INVISIBLE: t means every non-nil `invisible' value
/// hides text without an ellipsis; a list matches values (and members
/// of list-valued properties) against its atoms, where an `(ATOM . t)'
/// entry hides with the ellipsis.  `active' records whether the buffer
/// carries any `invisible' source at all, so ordinary buffers skip the
/// per-character walk entirely.
#[derive(Clone, Default)]
pub(crate) struct InvisibilitySpec {
    pub(crate) all: bool,
    pub(crate) entries: Vec<(Value, bool)>,
    pub(crate) active: bool,
}

pub(crate) fn resolve_buffer_invisibility(
    interpreter: &Interpreter,
    buffer: &Buffer,
    buffer_id: u64,
) -> InvisibilitySpec {
    let has_source = buffer.has_text_property_named("invisible")
        || buffer.overlays.iter().any(|overlay| {
            !overlay.is_dead()
                && overlay
                    .get_prop(&Value::Symbol("invisible".into()))
                    .is_some_and(|value| !value.is_nil())
        });
    if !has_source {
        return InvisibilitySpec::default();
    }
    let spec = interpreter
        .buffer_local_value(buffer_id, "buffer-invisibility-spec")
        .or_else(|| interpreter.lookup_var("buffer-invisibility-spec", &Vec::new()))
        .unwrap_or(Value::T);
    match &spec {
        Value::Nil => InvisibilitySpec::default(),
        Value::Symbol(name) if name == "t" => InvisibilitySpec {
            all: true,
            entries: Vec::new(),
            active: true,
        },
        _ => {
            let entries = spec
                .to_vec()
                .unwrap_or_default()
                .into_iter()
                .map(|entry| match (&entry, entry.car(), entry.cdr()) {
                    (Value::Cons(_), Ok(atom), Ok(flag)) => (atom, flag.is_truthy()),
                    _ => (entry, false),
                })
                .collect();
            InvisibilitySpec {
                all: false,
                entries,
                active: true,
            }
        }
    }
}

/// EQ for the atoms an invisibility spec realistically holds (symbols,
/// integers); other object identities answer false, as a pointer EQ
/// against a fresh value would.
pub(crate) fn invisibility_atom_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Symbol(a), Value::Symbol(b)) => a == b,
        (Value::Integer(a), Value::Integer(b)) => a == b,
        (Value::Nil, Value::Nil) => true,
        _ => false,
    }
}

/// xdisp.c invisible_prop: 0 visible, 1 invisible, 2 invisible with the
/// ellipsis.
pub(crate) fn invisible_value_class(spec: &InvisibilitySpec, value: &Value) -> u8 {
    if value.is_nil() {
        return 0;
    }
    if spec.all {
        return 1;
    }
    let match_atom = |atom: &Value| -> u8 {
        for (entry, ellipsis) in &spec.entries {
            if invisibility_atom_eq(atom, entry) {
                return if *ellipsis { 2 } else { 1 };
            }
        }
        0
    };
    let direct = match_atom(value);
    if direct != 0 {
        return direct;
    }
    if matches!(value, Value::Cons(_)) {
        let mut tail = value.clone();
        while let Value::Cons(_) = tail {
            if let Ok(member) = tail.car() {
                let class = match_atom(&member);
                if class != 0 {
                    return class;
                }
            }
            tail = tail.cdr().unwrap_or(Value::Nil);
        }
    }
    0
}

/// The `invisible' value governing POS, get-char-property's answer: the
/// covering overlay with the highest priority wins over the text
/// property.
pub(crate) fn invisible_class_at(buffer: &Buffer, spec: &InvisibilitySpec, pos: usize) -> u8 {
    let mut best: Option<(i64, u8)> = None;
    for overlay in &buffer.overlays {
        if overlay.is_dead() || pos < overlay.beg || pos >= overlay.end {
            continue;
        }
        let Some(value) = overlay.get_prop(&Value::Symbol("invisible".into())) else {
            continue;
        };
        let class = invisible_value_class(spec, value);
        if class == 0 {
            continue;
        }
        let priority = overlay
            .get_prop(&Value::Symbol("priority".into()))
            .and_then(|priority| priority.as_integer().ok())
            .unwrap_or(0);
        if best.is_none_or(|(existing, _)| priority >= existing) {
            best = Some((priority, class));
        }
    }
    if let Some((_, class)) = best {
        return class;
    }
    buffer
        .text_property_at(pos, "invisible")
        .map(|value| invisible_value_class(spec, &value))
        .unwrap_or(0)
}

/// The invisible run starting at POS: its exclusive end and whether the
/// ellipsis shows (handle_invisible_prop extends across consecutive
/// invisible stretches; the last stretch's flag decides the ellipsis).
pub(crate) fn invisible_run_at(
    buffer: &Buffer,
    spec: &InvisibilitySpec,
    pos: usize,
) -> Option<(usize, bool)> {
    let mut class = invisible_class_at(buffer, spec, pos);
    if class == 0 {
        return None;
    }
    let limit = buffer.point_max();
    let mut end = pos + 1;
    while end < limit {
        let next = invisible_class_at(buffer, spec, end);
        if next == 0 {
            break;
        }
        class = next;
        end += 1;
    }
    Some((end, class == 2))
}

/// The first raw line of the display line containing LINE: while the
/// newline ending the previous raw line is inside an invisible run, the
/// display line began earlier.
pub(crate) fn visual_line_first_line(
    buffer: &Buffer,
    spec: &InvisibilitySpec,
    mut line: usize,
) -> usize {
    if !spec.active {
        return line;
    }
    while line > 1 {
        let newline_pos = buffer.line_start_of(line).saturating_sub(1);
        if newline_pos == 0 || invisible_class_at(buffer, spec, newline_pos) == 0 {
            break;
        }
        line = buffer.line_number_at_pos(newline_pos);
    }
    line
}
