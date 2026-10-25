// ── Mode-line formatting (xdisp.c's display_mode_element) ──────────────
//
// One engine with GNU's three output targets: MODE_LINE_NOPROP builds
// plain text (`format-mode-line' with an integer FACE), MODE_LINE_STRING
// collects propertized pieces (`format-mode-line' otherwise), and
// MODE_LINE_DISPLAY lays a terminal row out as display_string does (a
// window's mode, header and tab lines).  decode_mode_spec expands the
// %-constructs; display_mode_line drives the row.

use super::*;
use crate::lisp::alloc::RootedVec;

/// dispextern.h's DISP_INFINITY: display_string's unbounded field width.
const DISP_INFINITY: i64 = 10_000_000;

/// The terminal's measures of a mode-line row, which display_string
/// takes from the glyphs the frame produces.
pub(crate) trait ModeLineCells {
    /// The cells each character of TEXT produces in the row (the display
    /// table, escape translations, `char-width-table'); TEXT holds no TAB.
    fn widths(&self, interp: &Interpreter, text: &[char]) -> Vec<usize>;
    /// produce_stretch_glyph's width of a `(space . PLIST)' spec at X.
    fn stretch(&self, interp: &Interpreter, plist: &[Value], x: usize) -> usize;
    /// calc_pixel_width_or_height of SPEC as a width, in cells.
    fn width_of(&self, interp: &Interpreter, spec: &Value) -> Option<f64>;
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Target {
    NoProp,
    String,
    Display,
}

/// The glyph row display_string fills (MODE_LINE_DISPLAY): its characters
/// as the frontend paints them, their face runs in character offsets, and
/// the iterator's position.
struct Row<'a> {
    cells: &'a dyn ModeLineCells,
    text: String,
    length: usize,
    faces: RootedVec<(usize, usize, Value)>,
    /// it->current_x and it->hpos (a `min-width' stretch advances only x).
    x: usize,
    hpos: i64,
    last_visible_x: usize,
    tab_width: usize,
    /// it->min_width_property and it->min_width_start.
    min_width: Value,
    min_width_start: usize,
    invisibility: crate::lisp::primitives::invisibility::InvisibilitySpec,
}

impl Row<'_> {
    fn push(&mut self, character: char, cells: usize, face: Value, counted: bool) {
        self.text.push(character);
        if !face.is_nil() {
            match self.faces.last_mut() {
                Some((_, end, last)) if *end == self.length && last.word() == face.word() => {
                    *end += 1
                }
                _ => self.faces.push((self.length, self.length + 1, face)),
            }
        }
        self.length += 1;
        self.x += cells;
        if counted {
            self.hpos += cells as i64;
        }
    }

    /// produce_stretch_glyph on a terminal: WIDTH blank glyphs.
    fn stretch(&mut self, width: usize, face: Value, counted: bool) {
        for _ in 0..width {
            self.push(' ', 1, face, counted);
        }
    }
}

struct ModeLine<'a> {
    target: Target,
    window_id: u64,
    /// MODE_LINE_NOPROP's mode_line_noprop_buf.
    noprop: String,
    /// MODE_LINE_STRING's mode_line_string_list, in order.
    strings: RootedVec<Value>,
    string_face: Value,
    string_face_prop: Value,
    row: Option<Row<'a>>,
}

fn primitive(
    interp: &mut Interpreter,
    env: &mut Env,
    name: &str,
    args: &[Value],
) -> Result<Value, LispError> {
    super::super::call(interp, name, args, env)
}

fn eq(a: Value, b: Value) -> bool {
    a.word() == b.word()
}

fn string_chars(value: &Value) -> Vec<char> {
    string_like(value)
        .map(|string| string.text.chars().collect())
        .unwrap_or_default()
}

fn schars(value: &Value) -> usize {
    match value.kind() {
        Kind::StringObject(state) => state.borrow().len(),
        _ => 0,
    }
}

/// Fget_text_property for strings, through textget.
fn string_property(interp: &Interpreter, string: &Value, pos: usize, prop: &str) -> Value {
    crate::lisp::primitives::string_property_at_with_category(interp, string, pos, prop)
        .unwrap_or(Value::Nil)
}

/// xdisp.c find_display_property: PROP's value in the display spec
/// DISP, a single spec, a list or a vector of them.
fn find_display_property(disp: Value, prop: &str) -> Value {
    let is_prop = |spec: &Value| match spec.kind() {
        Kind::Cons(cell) => matches!(cell.car.get().kind(), Kind::Symbol(name) if name == prop)
            .then(|| cell.cdr.get().car().unwrap_or(Value::Nil)),
        _ => None,
    };
    match disp.kind() {
        Kind::Cons(cell) if matches!(cell.car.get().kind(), Kind::Cons(_)) => {
            let mut tail = disp;
            let mut seen = crate::lisp::types::CycleGuard::new();
            while let Kind::Cons(cell) = tail.kind() {
                if let Some(value) = is_prop(&cell.car.get()) {
                    return value;
                }
                if seen.step(crate::lisp::types::ConsCell::identity(&cell)) {
                    break;
                }
                tail = cell.cdr.get();
            }
            Value::Nil
        }
        Kind::Vector(vector) => (0..vector.len())
            .filter_map(|index| vector.get(index))
            .find_map(|spec| is_prop(&spec))
            .unwrap_or(Value::Nil),
        _ => is_prop(&disp).unwrap_or(Value::Nil),
    }
}

/// xdisp.c get_display_property.
fn get_display_property(interp: &Interpreter, pos: usize, prop: &str, object: &Value) -> Value {
    find_display_property(string_property(interp, object, pos, "display"), prop)
}

/// lisp.h's plist_get: PROP's value in PLIST, by `eq'.
fn plist_get(plist: Value, prop: Value) -> Value {
    let mut tail = plist;
    let mut seen = crate::lisp::types::CycleGuard::new();
    while let Kind::Cons(cell) = tail.kind() {
        let Kind::Cons(rest) = cell.cdr.get().kind() else {
            break;
        };
        if eq(cell.car.get(), prop) {
            return rest.car.get();
        }
        if seen.step(crate::lisp::types::ConsCell::identity(&cell)) {
            break;
        }
        tail = rest.cdr.get();
    }
    Value::Nil
}

/// xdisp.c:dsafe_eval: `(eval SEXPR t)' with quitting inhibited, an
/// error logged ("Error during redisplay") and answered with nil.
fn dsafe_eval(interp: &mut Interpreter, env: &mut Env, sexpr: Value) -> Result<Value, LispError> {
    let restore = interp.bind_special_dynamic("inhibit-quit", Value::T, env)?;
    let result = redisplay_safe_call(interp, Value::symbol("eval"), &[sexpr, Value::T], env);
    interp.restore_special_dynamic(restore, env)?;
    result
}

/// A buffer-local C variable as redisplay reads it (BVAR, or the
/// variable's value in the current buffer).
fn bvar(interp: &Interpreter, name: &str) -> Value {
    interp
        .lookup_var(name, &crate::lisp::types::Env::new())
        .unwrap_or(Value::Nil)
}

impl<'a> ModeLine<'a> {
    fn new(target: Target, window_id: u64) -> Self {
        Self {
            target,
            window_id,
            noprop: String::new(),
            strings: RootedVec::new(),
            string_face: Value::Nil,
            string_face_prop: Value::Nil,
            row: None,
        }
    }

    /// xdisp.c display_mode_element: ELT's contribution, field width
    /// FIELD_WIDTH and precision PRECISION in columns, PROPS a property
    /// list for the strings met and RISKY their properties removed (and
    /// :eval and :propertize ignored).  The number of columns produced.
    #[allow(clippy::too_many_arguments)]
    fn display_mode_element(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        mut depth: i32,
        mut field_width: i64,
        mut precision: i64,
        mut elt: Value,
        props: Value,
        mut risky: bool,
    ) -> Result<i64, LispError> {
        let mut n: i64 = 0;
        let mut literal = false;
        'tail_recurse: loop {
            if depth > 100 {
                elt = Value::String("*too-deep*".into());
            }
            depth += 1;
            match elt.kind() {
                Kind::StringObject(_) => {
                    n += self.mode_string(
                        interp,
                        env,
                        depth,
                        field_width,
                        precision,
                        elt,
                        props,
                        risky,
                        literal,
                        n,
                    )?;
                    break 'tail_recurse;
                }
                Kind::Cons(cell) => {
                    let car = cell.car.get();
                    let cdr = cell.cdr.get();
                    if matches!(car.kind(), Kind::Symbol(name) if name == ":eval") {
                        // (:eval FORM): FORM's value as a mode line element.
                        if risky {
                            break 'tail_recurse;
                        }
                        if let Kind::Cons(form) = cdr.kind() {
                            let spec = dsafe_eval(interp, env, form.car.get())?;
                            n += self.display_mode_element(
                                interp,
                                env,
                                depth,
                                field_width - n,
                                precision - n,
                                spec,
                                props,
                                risky,
                            )?;
                        }
                        break 'tail_recurse;
                    }
                    if matches!(car.kind(), Kind::Symbol(name) if name == ":propertize") {
                        // (:propertize ELT PROPS...): ELT with PROPS.
                        if risky {
                            break 'tail_recurse;
                        }
                        if let Kind::Cons(rest) = cdr.kind() {
                            n += self.display_mode_element(
                                interp,
                                env,
                                depth,
                                field_width - n,
                                precision - n,
                                rest.car.get(),
                                rest.cdr.get(),
                                risky,
                            )?;
                        }
                        break 'tail_recurse;
                    }
                    if car.is_nil() || matches!(car.kind(), Kind::T | Kind::Symbol(_)) {
                        // (SYMBOL THEN [ELSE]).
                        let value = symbol_value(interp, env, car)?;
                        let Kind::Cons(branches) = cdr.kind() else {
                            elt = Value::String("*invalid*".into());
                            continue 'tail_recurse;
                        };
                        if value.is_some_and(|value| !value.is_nil()) {
                            elt = branches.car.get();
                            continue 'tail_recurse;
                        }
                        let rest = branches.cdr.get();
                        match rest.kind() {
                            Kind::Nil => break 'tail_recurse,
                            Kind::Cons(else_branch) => {
                                elt = else_branch.car.get();
                                continue 'tail_recurse;
                            }
                            _ => {
                                elt = Value::String("*invalid*".into());
                                continue 'tail_recurse;
                            }
                        }
                    }
                    if let Kind::Integer(lim) = car.kind() {
                        // (WIDTH REST...): pad or truncate REST.
                        elt = cdr;
                        if lim < 0 {
                            precision = if precision <= 0 {
                                -lim
                            } else {
                                precision.min(-lim)
                            };
                        } else if lim > 0 {
                            let lim = if precision > 0 {
                                precision.min(lim)
                            } else {
                                lim
                            };
                            field_width = field_width.max(lim);
                        }
                        continue 'tail_recurse;
                    }
                    if car.is_string() || matches!(car.kind(), Kind::Cons(_)) {
                        // A list of elements, padded after only the last.
                        let mut tail = elt;
                        let mut seen = crate::lisp::types::CycleGuard::new();
                        while let Kind::Cons(cell) = tail.kind() {
                            if 0 < precision && precision <= n {
                                break;
                            }
                            let rest = cell.cdr.get();
                            let last = !matches!(rest.kind(), Kind::Cons(_));
                            n += self.display_mode_element(
                                interp,
                                env,
                                depth,
                                if last { field_width - n } else { 0 },
                                precision - n,
                                cell.car.get(),
                                props,
                                risky,
                            )?;
                            if seen.step(crate::lisp::types::ConsCell::identity(&cell)) {
                                break;
                            }
                            tail = rest;
                        }
                    }
                    break 'tail_recurse;
                }
                Kind::Nil | Kind::T | Kind::Symbol(_) => {
                    // A symbol: its value, a string shown literally.
                    let symbol =
                        crate::lisp::primitives::checked_symbol_identity(interp, &elt, env)?;
                    let risky_name = Value::symbol("risky-local-variable")
                        .symbol_cell()
                        .expect("an interned symbol");
                    let risky_property = interp
                        .get_symbol_property_of(&symbol, &risky_name)
                        .unwrap_or(Value::Nil);
                    if risky_property.is_nil() {
                        risky = true;
                    }
                    if let Ok(value) = interp.symbol_value_cell_symbol(&symbol) {
                        if value.is_string() {
                            literal = true;
                        }
                        if !eq(value, elt) {
                            elt = value;
                            continue 'tail_recurse;
                        }
                    }
                    break 'tail_recurse;
                }
                _ => {
                    elt = Value::String("*invalid*".into());
                    continue 'tail_recurse;
                }
            }
        }
        // Pad to FIELD_WIDTH.
        if field_width > 0 && n < field_width {
            n += match self.target {
                Target::NoProp => self.store_noprop(interp, env, &[], field_width - n, 0),
                Target::String => self.store_string(
                    interp,
                    env,
                    true,
                    Value::Nil,
                    false,
                    field_width - n,
                    0,
                    Value::Nil,
                )?,
                Target::Display => self.display_string(
                    interp,
                    env,
                    Some(Vec::new()),
                    None,
                    None,
                    0,
                    field_width - n,
                    0,
                )?,
            };
        }
        Ok(n)
    }

    /// display_mode_element's string case: the props translation, then
    /// the literal text runs and %-constructs.
    #[allow(clippy::too_many_arguments)]
    fn mode_string(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        depth: i32,
        field_width: i64,
        precision: i64,
        mut elt: Value,
        mut props: Value,
        risky: bool,
        literal: bool,
        n_before: i64,
    ) -> Result<i64, LispError> {
        let mut n = n_before;
        if schars(&elt) > 0 && (!props.is_nil() || risky) {
            let oprops = primitive(interp, env, "text-properties-at", &[Value::Integer(0), elt])?;
            let equal = primitive(interp, env, "equal", &[props, oprops])?;
            if equal.is_nil() || risky {
                // The starting string's properties merged under PROPS,
                // or (risky) PROPS alone, over the whole string.
                if !oprops.is_nil() && !risky {
                    let mut merged = primitive(interp, env, "copy-sequence", &[oprops])?;
                    let mut tail = props;
                    while let Kind::Cons(cell) = tail.kind() {
                        let Kind::Cons(rest) = cell.cdr.get().kind() else {
                            break;
                        };
                        merged = primitive(
                            interp,
                            env,
                            "plist-put",
                            &[merged, cell.car.get(), rest.car.get()],
                        )?;
                        tail = rest.cdr.get();
                    }
                    props = merged;
                }
                // mode_line_proptrans_alist only saves this consing: the
                // copy carries exactly PROPS, so a cached one is `equal'.
                elt = primitive(interp, env, "copy-sequence", &[elt])?;
                let length = Value::Integer(schars(&elt) as i64);
                let args = [Value::Integer(0), length, props, elt];
                if let Err(error) = primitive(interp, env, "set-text-properties", &args) {
                    log_redisplay_error(
                        interp,
                        env,
                        Value::symbol("set-text-properties"),
                        &args,
                        error,
                    )?;
                }
            }
        }

        let chars = string_chars(&elt);
        if literal {
            let prec = precision - n;
            match self.target {
                Target::NoProp => {
                    n += self.store_noprop(interp, env, &chars, -1, prec);
                }
                Target::String => {
                    n += self.store_string(interp, env, false, elt, true, 0, prec, Value::Nil)?;
                }
                Target::Display => {
                    n += self.display_string(interp, env, None, Some(elt), None, 0, 0, prec)?;
                }
            }
            return Ok(n - n_before);
        }

        // The C loop reads bytes up to a NUL; characters do the same here.
        let at = |offset: usize| chars.get(offset).copied().unwrap_or('\0');
        let mut offset = 0usize;
        while (precision <= 0 || n < precision)
            && at(offset) != '\0'
            && (self.target != Target::Display
                || self
                    .row
                    .as_ref()
                    .is_some_and(|row| row.x < row.last_visible_x))
        {
            let last_offset = offset;
            let mut c;
            loop {
                c = at(offset);
                offset += 1;
                if c == '\0' || c == '%' {
                    break;
                }
            }
            if offset - 1 != last_offset {
                // Output to the end of the string or up to the `%'; at
                // most what PRECISION allows.
                offset -= 1;
                let run = &chars[last_offset..offset];
                let (prec, nchars) = c_string_width(interp, env, run, precision - n);
                match self.target {
                    Target::NoProp => {
                        n += self.store_noprop(interp, env, &chars[last_offset..], 0, prec);
                    }
                    Target::String => {
                        let end = if precision <= 0 {
                            offset
                        } else {
                            last_offset + nchars
                        };
                        let piece = primitive(
                            interp,
                            env,
                            "substring",
                            &[
                                elt,
                                Value::Integer(last_offset as i64),
                                Value::Integer(end as i64),
                            ],
                        )?;
                        n +=
                            self.store_string(interp, env, false, piece, false, 0, 0, Value::Nil)?;
                    }
                    Target::Display => {
                        let nchars = if precision <= 0 {
                            offset - last_offset
                        } else {
                            nchars
                        };
                        n += self.display_string(
                            interp,
                            env,
                            None,
                            Some(elt),
                            None,
                            last_offset,
                            0,
                            nchars as i64,
                        )?;
                    }
                }
            } else {
                // c == '%'.
                let percent_position = offset;
                let mut field: i64 = 0;
                loop {
                    c = at(offset);
                    offset += 1;
                    let Some(digit) = c.to_digit(10) else {
                        break;
                    };
                    field = field.saturating_mul(10).saturating_add(i64::from(digit));
                }
                // Don't pad beyond the total padding allowed.
                if field_width - n > 0 && field > field_width - n {
                    field = field_width - n;
                }
                let prec = precision - n;
                if c == 'M' {
                    let global = bvar(interp, "global-mode-string");
                    n += self.display_mode_element(
                        interp, env, depth, field, prec, global, props, risky,
                    )?;
                } else if c != '\0' {
                    let charpos = percent_position;
                    let (spec, string) = self.decode_mode_spec(interp, env, c, field)?;
                    match self.target {
                        Target::NoProp => {
                            n += self.store_noprop(interp, env, &spec, field, prec);
                        }
                        Target::String => {
                            let tem = Value::String(spec.iter().collect::<String>().into());
                            props = primitive(
                                interp,
                                env,
                                "text-properties-at",
                                &[Value::Integer(charpos as i64), elt],
                            )?;
                            n += self
                                .store_string(interp, env, false, tem, false, field, prec, props)?;
                        }
                        Target::Display => {
                            n += self.display_string(
                                interp,
                                env,
                                Some(spec),
                                string,
                                Some((elt, charpos)),
                                0,
                                field,
                                prec,
                            )?;
                        }
                    }
                } else {
                    break;
                }
            }
        }
        Ok(n - n_before)
    }

    /// xdisp.c store_mode_line_noprop: at most PRECISION columns of TEXT
    /// (PRECISION <= 0: all of it), padded with spaces to FIELD_WIDTH.
    fn store_noprop(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        text: &[char],
        field_width: i64,
        precision: i64,
    ) -> i64 {
        // The C string ends at its first NUL.
        let text = &text[..text.iter().position(|&c| c == '\0').unwrap_or(text.len())];
        let (mut n, nchars) = c_string_width(interp, env, text, precision);
        let nchars = if precision > 0 { nchars } else { text.len() };
        self.noprop.extend(&text[..nchars]);
        while field_width > 0 && n < field_width {
            self.noprop.push(' ');
            n += 1;
        }
        n
    }

    /// xdisp.c store_mode_line_string: LISP_STRING (or, with EMPTY, the C
    /// string "") as a piece of the result, at most PRECISION characters
    /// with PROPS added (and `mode_line_string_face' merged in), padded
    /// with spaces to FIELD_WIDTH.
    #[allow(clippy::too_many_arguments)]
    fn store_string(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        empty: bool,
        mut lisp_string: Value,
        copy_string: bool,
        mut field_width: i64,
        precision: i64,
        mut props: Value,
    ) -> Result<i64, LispError> {
        let mut n = 0;
        let len;
        let face_symbol = Value::symbol("face");
        if empty {
            len = 0;
            lisp_string = Value::String(String::new().into());
            if props.is_nil() {
                props = self.string_face_prop;
            } else if !self.string_face.is_nil() {
                let face = plist_get(props, face_symbol);
                props = primitive(interp, env, "copy-sequence", &[props])?;
                let face = if face.is_nil() {
                    self.string_face
                } else {
                    Value::list([face, self.string_face])
                };
                props = primitive(interp, env, "plist-put", &[props, face_symbol, face])?;
            }
        } else {
            let mut length = schars(&lisp_string) as i64;
            if precision > 0 && length > precision {
                length = precision;
                lisp_string = primitive(
                    interp,
                    env,
                    "substring",
                    &[lisp_string, Value::Integer(0), Value::Integer(length)],
                )?;
            }
            len = length;
            if !self.string_face.is_nil() {
                if props.is_nil() {
                    props = primitive(
                        interp,
                        env,
                        "text-properties-at",
                        &[Value::Integer(0), lisp_string],
                    )?;
                }
                let face = plist_get(props, face_symbol);
                let face = if face.is_nil() {
                    self.string_face
                } else {
                    Value::list([face, self.string_face])
                };
                props = Value::list([face_symbol, face]);
                if copy_string {
                    lisp_string = primitive(interp, env, "copy-sequence", &[lisp_string])?;
                }
            }
            if !props.is_nil() {
                primitive(
                    interp,
                    env,
                    "add-text-properties",
                    &[Value::Integer(0), Value::Integer(len), props, lisp_string],
                )?;
            }
        }
        if len > 0 {
            self.strings.push(lisp_string);
            n += len;
        }
        if field_width > len {
            field_width -= len;
            let padding = primitive(
                interp,
                env,
                "make-string",
                &[Value::Integer(field_width), Value::Integer(' ' as i64)],
            )?;
            if !props.is_nil() {
                primitive(
                    interp,
                    env,
                    "add-text-properties",
                    &[
                        Value::Integer(0),
                        Value::Integer(field_width),
                        props,
                        padding,
                    ],
                )?;
            }
            self.strings.push(padding);
            n += field_width;
        }
        Ok(n)
    }

    /// xdisp.c display_string on the mode-line row: SPEC's characters (a
    /// C string; with LISP_STRING its characters, their properties
    /// ignored) or LISP_STRING's from START under its own properties,
    /// faced from FACE_STRING's position when given, at most PRECISION
    /// characters and padded with spaces to FIELD_WIDTH, never past the
    /// row's end.  The glyphs produced.
    #[allow(clippy::too_many_arguments)]
    fn display_string(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        spec: Option<Vec<char>>,
        lisp_string: Option<Value>,
        face_string: Option<(Value, usize)>,
        start: usize,
        field_width: i64,
        precision: i64,
    ) -> Result<i64, LispError> {
        let Some(row) = self.row.as_mut() else {
            return Ok(0);
        };
        let hpos_at_start = row.hpos;
        // reseat_to_string.
        let walk = spec.is_none();
        let chars = match (&spec, lisp_string) {
            (Some(spec), None) => spec.clone(),
            (_, Some(string)) => string_chars(&string),
            (None, None) => Vec::new(),
        };
        let mut string_nchars = chars.len();
        let mut end = string_nchars;
        let start = start.min(end);
        if precision > 0 && (end - start) as i64 > precision {
            end = start + precision as usize;
            string_nchars = end;
        }
        let field = if field_width < 0 {
            DISP_INFINITY
        } else {
            field_width
        };
        if field > (end - start) as i64 {
            end = start + field as usize;
        }
        let mut face = Value::Nil;
        if let Some((face_string, pos)) = face_string {
            face = string_property(interp, &face_string, pos, "face");
            if lisp_string.is_none() {
                // A display spec of the :propertize the %-construct
                // came from: its `min-width'.
                let display = string_property(interp, &face_string, 0, "display");
                if !display.is_nil() {
                    let min_width = plist_get(display, Value::symbol("min-width"));
                    if !min_width.is_nil() {
                        display_min_width(interp, row, &face_string, min_width, face);
                    }
                }
            }
        }
        let max_x = row.last_visible_x;
        let visible = &chars[start..string_nchars.min(chars.len()).max(start)];
        let measured: Vec<char> = visible
            .iter()
            .map(|&c| if c == '\t' { ' ' } else { c })
            .collect();
        let widths = row.cells.widths(interp, &measured);
        let source = lisp_string.filter(|_| walk);
        let props_of = |pos: usize| -> Option<usize> {
            let string = source?;
            let Kind::StringObject(state) = string.kind() else {
                return None;
            };
            state
                .borrow()
                .props
                .iter()
                .position(|span| span.start <= pos && pos < span.end)
        };
        let mut pos = start;
        let mut span: Option<Option<usize>> = None;
        while row.x < max_x {
            if pos >= end {
                break;
            }
            if pos >= string_nchars {
                // Pad with spaces.
                row.push(' ', 1, face, true);
                pos += 1;
                if row.x >= row.last_visible_x {
                    break;
                }
                continue;
            }
            if let Some(string) = source {
                let here = props_of(pos);
                if span != Some(here) {
                    span = Some(here);
                    // handle_stop: the face, invisibility and display
                    // properties at a change of the string's intervals.
                    face = string_property(interp, &string, pos, "face");
                    let invisible = string_property(interp, &string, pos, "invisible");
                    let class = crate::lisp::primitives::invisibility::invisible_value_class(
                        &row.invisibility,
                        &invisible,
                    );
                    if class != 0 {
                        let mut skip = pos;
                        while skip < string_nchars
                            && eq(
                                string_property(interp, &string, skip, "invisible"),
                                invisible,
                            )
                        {
                            skip += 1;
                        }
                        pos = skip;
                        span = None;
                        if class == 2 {
                            for _ in 0..3 {
                                let width = row.cells.widths(interp, &['.'])[0];
                                row.push('.', width, face, true);
                            }
                        }
                        continue;
                    }
                    let display = string_property(interp, &string, pos, "display");
                    if !row.min_width.is_nil()
                        && find_display_property(display, "min-width").is_nil()
                    {
                        display_min_width(interp, row, &string, Value::Nil, face);
                    }
                    if !display.is_nil() {
                        let replaced =
                            handle_display_spec(interp, env, row, &string, pos, display, face)?;
                        if replaced {
                            let mut skip = pos;
                            while skip < string_nchars
                                && eq(string_property(interp, &string, skip, "display"), display)
                            {
                                skip += 1;
                            }
                            pos = skip;
                            span = None;
                            if row.x >= row.last_visible_x {
                                break;
                            }
                            continue;
                        }
                    }
                }
            }
            let c = chars[pos];
            if c == '\t' {
                let tab = row.tab_width.max(1);
                let spaces = tab - row.x % tab;
                for _ in 0..spaces {
                    row.push(' ', 1, face, true);
                }
            } else {
                let width = widths.get(pos - start).copied().unwrap_or(1);
                row.push(c, width, face, true);
            }
            pos += 1;
            // A truncated row stops at its right edge.
            if row.x >= row.last_visible_x {
                break;
            }
        }
        Ok(row.hpos - hpos_at_start)
    }
}

/// xdisp.c handle_display_spec for a mode-line string: the replacing
/// specs (a string, a `(space ...)' stretch) produce their glyphs and
/// answer true; `min-width' starts its sequence; `(when FORM . SPEC)'
/// tests FORM first.
fn handle_display_spec(
    interp: &mut Interpreter,
    env: &mut Env,
    row: &mut Row<'_>,
    string: &Value,
    pos: usize,
    spec: Value,
    face: Value,
) -> Result<bool, LispError> {
    let specs: Vec<Value> = match spec.kind() {
        Kind::Cons(cell) if matches!(cell.car.get().kind(), Kind::Cons(_)) => {
            spec.to_vec().unwrap_or_default()
        }
        Kind::Vector(vector) => (0..vector.len())
            .filter_map(|index| vector.get(index))
            .collect(),
        _ => vec![spec],
    };
    let mut replaced = false;
    for spec in specs {
        let mut spec = spec;
        if let Kind::Cons(cell) = spec.kind()
            && matches!(cell.car.get().kind(), Kind::Symbol(name) if name == "when")
        {
            let Kind::Cons(rest) = cell.cdr.get().kind() else {
                continue;
            };
            let form = rest.car.get();
            spec = rest.cdr.get();
            if !form.is_nil() && !matches!(form.kind(), Kind::T) {
                let restore = [
                    interp.bind_special_dynamic("object", *string, env)?,
                    interp.bind_special_dynamic("position", Value::Integer(pos as i64), env)?,
                    interp.bind_special_dynamic("buffer-position", Value::Integer(0), env)?,
                ];
                let value = dsafe_eval(interp, env, form);
                for binding in restore.into_iter().rev() {
                    interp.restore_special_dynamic(binding, env)?;
                }
                if value?.is_nil() {
                    continue;
                }
            }
        }
        if replaced {
            continue;
        }
        match spec.kind() {
            Kind::Cons(cell) => {
                let head = cell.car.get();
                if matches!(head.kind(), Kind::Symbol(name) if name == "min-width") {
                    let width = cell.cdr.get().car().unwrap_or(Value::Nil);
                    display_min_width(interp, row, string, width, face);
                } else if matches!(head.kind(), Kind::Symbol(name) if name == "space") {
                    let plist = cell.cdr.get().to_vec().unwrap_or_default();
                    let width = row.cells.stretch(interp, &plist, row.x);
                    row.stretch(width, face, true);
                    replaced = true;
                }
            }
            Kind::StringObject(_) => {
                let replacement = spec;
                let chars = string_chars(&replacement);
                let widths = row.cells.widths(interp, &chars);
                for (index, character) in chars.iter().enumerate() {
                    if row.x >= row.last_visible_x {
                        break;
                    }
                    let own = string_property(interp, &replacement, index, "face");
                    let glyph_face = if own.is_nil() { face } else { own };
                    if *character == '\t' {
                        let tab = row.tab_width.max(1);
                        row.stretch(tab - row.x % tab, glyph_face, true);
                    } else {
                        row.push(*character, widths[index], glyph_face, true);
                    }
                }
                replaced = true;
            }
            _ => {}
        }
    }
    Ok(replaced)
}

/// xdisp.c display_min_width on the mode-line row: at the end of a
/// `min-width' sequence (the row moved on to a string without it) the
/// stretch that pads the sequence to its width, which counts toward x
/// but not hpos; at the start of one, its spec and start column.
fn display_min_width(
    interp: &Interpreter,
    row: &mut Row<'_>,
    object: &Value,
    width_spec: Value,
    face: Value,
) {
    if !row.min_width.is_nil()
        && !eq(width_spec, row.min_width)
        && !eq(
            row.min_width,
            get_display_property(interp, 0, "min-width", object),
        )
    {
        let target = row
            .min_width
            .car()
            .ok()
            .and_then(|spec| row.cells.width_of(interp, &spec))
            .unwrap_or(0.0);
        let width = target as i64 - (row.x - row.min_width_start) as i64;
        // produce_stretch_glyph: `(space :width W)' with a negative W
        // is one column wide.
        let width = if width < 0 { 1 } else { width as usize };
        row.stretch(width, face, false);
        row.min_width = Value::Nil;
    }
    if matches!(width_spec.kind(), Kind::Cons(_))
        && !eq(
            row.min_width,
            get_display_property(interp, 0, "min-width", object),
        )
    {
        row.min_width = width_spec;
        row.min_width_start = row.x;
    }
}

/// character.c c_string_width with the buffer's display table: TEXT's
/// columns, and with PRECISION > 0 those of the longest prefix that
/// fits, with the characters it holds.
fn c_string_width(interp: &Interpreter, env: &Env, text: &[char], precision: i64) -> (i64, usize) {
    let dp = crate::lisp::primitives::buffer_display_table(interp, env);
    let mut width: i64 = 0;
    for (index, character) in text.iter().enumerate() {
        let code = crate::lisp::primitives::string_character_code(true, *character) as u32;
        let this = crate::lisp::primitives::display_char_width(interp, code, dp, env) as i64;
        if 0 < precision && precision - width < this {
            return (width, index);
        }
        width += this;
    }
    (width, text.len())
}

/// The symbol's dynamic value, as Fboundp and Fsymbol_value read it
/// (None: void).
fn symbol_value(
    interp: &Interpreter,
    env: &Env,
    symbol: Value,
) -> Result<Option<Value>, LispError> {
    let name = crate::lisp::primitives::checked_symbol_identity(interp, &symbol, env)?;
    Ok(interp.symbol_value_cell_symbol(&name).ok())
}

/// dsafe_eval_handler's log of an error a redisplay call signaled.
fn log_redisplay_error(
    interp: &mut Interpreter,
    env: &mut Env,
    function: Value,
    args: &[Value],
    error: LispError,
) -> Result<(), LispError> {
    match error.into_kind() {
        error @ (LispErrorKind::Throw(_, _) | LispErrorKind::Terminate(_)) => {
            Err(LispError::from(error))
        }
        error => {
            let message = super::super::call(
                interp,
                "format-message",
                &[
                    Value::String("Error during redisplay: %S signaled %S".into()),
                    Value::list(std::iter::once(function).chain(args.iter().cloned())),
                    crate::lisp::eval::error_condition_value(&LispError::from(error)),
                ],
                env,
            )?;
            let text = string_text(&message)?;
            log_message_text(interp, &text, env);
            Ok(())
        }
    }
}

// ── decode_mode_spec ───────────────────────────────────────────────────

/// xdisp.c pint2str: D right-justified in WIDTH columns.
fn pint2str(width: i64, d: i64) -> String {
    let digits = if d <= 0 {
        "0".to_string()
    } else {
        d.to_string()
    };
    format!("{digits:>width$}", width = width.max(0) as usize)
}

/// xdisp.c pint2hrstr: D with a power-of-1000 suffix, right-justified
/// in WIDTH columns.
fn pint2hrstr(width: i64, d: i64) -> String {
    const POWER_LETTER: [&str; 11] = ["", "k", "M", "G", "T", "P", "E", "Z", "Y", "R", "Q"];
    let mut quotient = d.max(0);
    let mut tenths: i64 = -1;
    let mut exponent = 0usize;
    if quotient >= 1000 {
        let mut remainder;
        loop {
            remainder = quotient % 1000;
            quotient /= 1000;
            exponent += 1;
            if quotient < 1000 {
                break;
            }
        }
        if quotient <= 9 {
            tenths = remainder / 100;
            if remainder % 100 >= 50 {
                if tenths < 9 {
                    tenths += 1;
                } else {
                    quotient += 1;
                    tenths = if quotient == 10 { -1 } else { 0 };
                }
            }
        } else if remainder >= 500 {
            if quotient < 999 {
                quotient += 1;
            } else {
                quotient = 1;
                exponent += 1;
                tenths = 0;
            }
        }
    }
    let number = if tenths >= 0 {
        format!("{quotient}.{tenths}")
    } else {
        quotient.to_string()
    };
    let length = if tenths == -1 && quotient <= 99 {
        if quotient <= 9 { 1 } else { 2 }
    } else {
        3
    };
    let pad = (width.max(0) as usize)
        .max(length)
        .saturating_sub(number.len());
    format!(
        "{}{number}{}",
        " ".repeat(pad),
        POWER_LETTER[exponent.min(10)]
    )
}

/// xdisp.c percent99: N's percentage of D, rounding up, at most 99.
fn percent99(n: i64, d: i64) -> i64 {
    let d = d.max(1);
    (((d - 1) as f64 + 100.0 * n as f64) / d as f64)
        .floor()
        .min(99.0) as i64
}

impl ModeLine<'_> {
    /// xdisp.c decode_mode_spec: the text of the %-construct C with
    /// FIELD_WIDTH, and the Lisp string it came from (%b, %f, %m, %s).
    fn decode_mode_spec(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        c: char,
        field_width: i64,
    ) -> Result<(Vec<char>, Option<Value>), LispError> {
        let frame_cols = interp.frame_width().max(1);
        // FRAME_MESSAGE_BUF_SIZE bounds the numeric fields.
        let message_buf_size = frame_cols * 4;
        let width = field_width.min(message_buf_size);
        let text = |s: &str| (s.chars().collect::<Vec<char>>(), None);
        let modified = interp.buffer.borrow().is_modified();
        let read_only = bvar(interp, "buffer-read-only").is_truthy();
        let obj = match c {
            '*' => {
                return Ok(text(if read_only {
                    "%"
                } else if modified {
                    "*"
                } else {
                    "-"
                }));
            }
            '+' => {
                return Ok(text(if modified {
                    "*"
                } else if read_only {
                    "%"
                } else {
                    "-"
                }));
            }
            '&' => return Ok(text(if modified { "*" } else { "-" })),
            '%' => return Ok(text("%")),
            '[' | ']' => {
                let level = interp.command_loop_recursion_depth;
                if level > 5 {
                    return Ok(text(if c == '[' { "[[[... " } else { " ...]]]" }));
                }
                return Ok((std::iter::repeat_n(c, level).collect(), None));
            }
            '-' => {
                if matches!(self.target, Target::NoProp | Target::String) {
                    return Ok(text("--"));
                }
                // lots_of_dashes, else a buffer's worth.
                let count = if field_width <= 0 || field_width > 141 {
                    message_buf_size - 1
                } else {
                    140
                };
                return Ok((
                    std::iter::repeat_n('-', count.max(0) as usize).collect(),
                    None,
                ));
            }
            'b' => Value::String(interp.buffer.borrow().name.clone().into()),
            'c' | 'C' => {
                let column = primitive(interp, env, "current-column", &[])?
                    .as_integer()
                    .unwrap_or(0);
                let shown = if c == 'C' { column + 1 } else { column };
                return Ok(text(&pint2str(width, shown)));
            }
            'e' => {
                return Ok(text(if bvar(interp, "memory-full").is_nil() {
                    ""
                } else {
                    "!MEM FULL! "
                }));
            }
            'F' => {
                // The frame's title, else its name (a terminal frame's).
                let title = primitive(
                    interp,
                    env,
                    "frame-parameter",
                    &[Value::Nil, Value::symbol("title")],
                )?;
                let name = if title.is_string() {
                    title
                } else {
                    primitive(
                        interp,
                        env,
                        "frame-parameter",
                        &[Value::Nil, Value::symbol("name")],
                    )?
                };
                return Ok(text(&string_text(&name).unwrap_or_default()));
            }
            'f' => bvar(interp, "buffer-file-name"),
            'i' | 'I' => {
                let size = {
                    let buffer = interp.buffer.borrow();
                    (buffer.point_max() - buffer.point_min()) as i64
                };
                return Ok(text(&if c == 'i' {
                    pint2str(width, size)
                } else {
                    pint2hrstr(width, size)
                }));
            }
            'l' => return Ok(text(&self.line_number_spec(interp, width))),
            'm' => bvar(interp, "mode-name"),
            'n' => {
                let buffer = interp.buffer.borrow();
                if buffer.point_min() > 1 || buffer.point_max() < buffer.size_total() + 1 {
                    return Ok(text(" Narrow"));
                }
                Value::Nil
            }
            'o' | 'p' | 'P' | 'q' => return Ok(text(&self.window_percent_spec(interp, c))),
            's' => {
                let buffer = Value::Buffer(interp.buffer);
                let process = primitive(interp, env, "get-buffer-process", &[buffer])?;
                if process.is_nil() {
                    return Ok(text("no process"));
                }
                let status = primitive(interp, env, "process-status", &[process])?;
                primitive(interp, env, "symbol-name", &[status])?
            }
            '@' => {
                let directory = bvar(interp, "default-directory");
                let remote = if directory.is_string() {
                    redisplay_safe_call(interp, Value::symbol("file-remote-p"), &[directory], env)?
                } else {
                    Value::Nil
                };
                return Ok(text(if remote.is_nil() { "-" } else { "@" }));
            }
            'z' | 'Z' => {
                // A terminal frame names the keyboard's and the terminal's
                // coding systems first (no end-of-line for them).
                let mut buf = String::new();
                for source in ["keyboard-coding-system", "terminal-coding-system"] {
                    let coding = primitive(interp, env, source, &[]).unwrap_or(Value::Nil);
                    decode_mode_spec_coding(interp, env, &coding, &mut buf, false);
                }
                let coding = bvar(interp, "buffer-file-coding-system");
                decode_mode_spec_coding(interp, env, &coding, &mut buf, c == 'Z');
                return Ok(text(&buf));
            }
            _ => Value::Nil,
        };
        if obj.is_string() {
            return Ok((string_chars(&obj), Some(obj)));
        }
        Ok((Vec::new(), None))
    }

    /// decode_mode_spec's %l: the line number of point, counted from the
    /// window's base line (xdisp.c's w->base_line_number cache), or "??"
    /// when the buffer is too big or its lines too long.
    fn line_number_spec(&mut self, interp: &mut Interpreter, width: i64) -> String {
        let no_value = || {
            let mut text = " ".repeat((width - 2).max(0) as usize);
            text.push_str("??");
            text
        };
        let window_id = self.window_id;
        let window = interp.record_value(window_id);
        let window_buffer = window_buffer_id(interp, &window);
        let current = interp.current_buffer_id();
        let own_buffer = window_buffer == Some(current);
        let (begv, zv, pt, modiff) = {
            let buffer = interp.buffer.borrow();
            (
                buffer.point_min(),
                buffer.point_max(),
                buffer.point(),
                buffer.modified_tick() as u64,
            )
        };
        let mut startpos = if window_id == interp.selected_window_id() {
            current_window_start(interp)
        } else {
            window_slot_value(interp, window_id, WINDOW_START_SLOT)
                .as_integer()
                .unwrap_or(1)
                .max(1) as usize
        };
        let height = window_geometry(interp, window_id).1.max(1);
        if !own_buffer || !(begv <= startpos && startpos <= zv) {
            startpos = begv;
        }
        let mut base = BaseLine::read(interp, window_id);
        // set_window_buffer resets the cache; one an edit or narrowing
        // may have staled since it was made (BASE_LINE_NUMBER_VALID_P)
        // keeps its position but no line number.
        if base.buffer != window_buffer {
            base = BaseLine {
                buffer: window_buffer,
                ..BaseLine::default()
            };
        }
        if own_buffer && base.stamp != (modiff, begv, zv) {
            base.number = 0;
            base.stamp = (modiff, begv, zv);
        }
        let store = |interp: &mut Interpreter, base: &BaseLine| {
            if own_buffer {
                base.write(interp, window_id);
            }
        };
        if base.pos == -1 {
            return no_value();
        }
        if let Some(limit) = bvar(interp, "line-number-display-limit").integer_value()
            && (zv - begv) as i64 > limit
        {
            return no_value();
        }
        let selective = {
            let value = bvar(interp, "selective-display");
            !value.is_nil() && value.integer_value().is_none()
        };
        let (mut line, linepos) =
            if own_buffer && base.number > 0 && base.pos > 0 && base.pos as usize <= startpos {
                (base.number, base.pos as usize)
            } else {
                (1, begv)
            };
        let buffer = interp.buffer.borrow();
        let nlines = count_lines_forward(&buffer, linepos, startpos, selective);
        let topline = nlines + line;
        if !own_buffer {
            // The base line is for another buffer: don't touch it.
        } else if startpos == begv {
            base.number = topline;
            base.pos = begv as i64;
        } else if nlines < height + 25 || nlines > height * 3 + 50 || linepos == begv {
            let limit_width = bvar(interp, "line-number-display-limit-width")
                .integer_value()
                .unwrap_or(200);
            let distance = if limit_width < 0 {
                0
            } else {
                limit_width.saturating_mul(height * 2 + 30)
            };
            let limit = if (startpos as i64).saturating_sub(distance) > begv as i64 {
                (startpos as i64 - distance) as usize
            } else {
                begv
            };
            let (nlines, position) = count_lines_backward(
                &buffer,
                startpos,
                limit,
                (height * 2 + 30) as usize,
                selective,
            );
            if position == limit && (limit as i64) == (startpos as i64).saturating_sub(distance) {
                base.pos = -1;
                base.number = 0;
                drop(buffer);
                store(interp, &base);
                return no_value();
            }
            base.number = topline - nlines as i64;
            base.pos = position as i64;
        }
        let nlines = count_lines_forward(&buffer, startpos, pt, selective);
        drop(buffer);
        store(interp, &base);
        line = topline + nlines;
        pint2str(width, line)
    }

    /// decode_mode_spec's %o, %p, %P and %q from the window's start and
    /// the end its last redisplay recorded.
    fn window_percent_spec(&self, interp: &Interpreter, c: char) -> String {
        let (begv, zv, z) = {
            let buffer = interp.buffer.borrow();
            (
                buffer.point_min() as i64,
                buffer.point_max() as i64,
                (buffer.size_total() + 1) as i64,
            )
        };
        let window_id = self.window_id;
        let toppos = if window_id == interp.selected_window_id() {
            current_window_start(interp) as i64
        } else {
            window_slot_value(interp, window_id, WINDOW_START_SLOT)
                .as_integer()
                .unwrap_or(1)
        };
        let window_end_pos = window_end_pos(interp, window_id, z);
        let botpos = z - window_end_pos;
        match c {
            'o' => {
                if zv <= botpos {
                    if toppos <= begv { "All" } else { "Bottom" }.into()
                } else if toppos <= begv {
                    "Top".into()
                } else {
                    format!(
                        "{:2}%",
                        percent99(toppos - begv, (toppos - begv) + (zv - botpos))
                    )
                }
            }
            'p' => {
                if window_end_pos <= z - zv {
                    if toppos <= begv { "All" } else { "Bottom" }.into()
                } else if toppos <= begv {
                    "Top".into()
                } else {
                    format!("{:2}%", percent99(toppos - begv, zv - begv))
                }
            }
            'P' => {
                if zv <= botpos {
                    if toppos <= begv { "All" } else { "Bottom" }.into()
                } else {
                    let percent = percent99(botpos - begv, zv - begv);
                    if begv < toppos {
                        format!("{percent:2}%")
                    } else {
                        format!("Top{percent:2}%")
                    }
                }
            }
            _ => {
                if toppos <= begv && zv <= botpos {
                    return "All   ".into();
                }
                let top = if toppos <= begv {
                    0
                } else {
                    percent99(toppos - begv, zv - begv)
                };
                let bottom = if zv <= botpos {
                    100
                } else {
                    percent99(botpos - begv, zv - begv)
                };
                if top == bottom {
                    format!("{top}%")
                } else {
                    format!("{top}-{bottom}%")
                }
            }
        }
    }
}

/// display_count_lines forward: the newlines (and, under a non-integer
/// `selective-display', carriage returns) in [START, LIMIT).
fn count_lines_forward(
    buffer: &crate::buffer::Buffer,
    start: usize,
    limit: usize,
    selective: bool,
) -> i64 {
    if start >= limit {
        return 0;
    }
    if !selective {
        return (buffer.line_number_at_pos(limit) - buffer.line_number_at_pos(start)) as i64;
    }
    (start..limit)
        .filter(|&pos| matches!(buffer.char_at(pos), Some('\n' | '\r')))
        .count() as i64
}

/// display_count_lines backward: up to COUNT line ends before START, not
/// below LIMIT.  The lines found (the one the scan stops after not
/// counted) and the position after the last line end, LIMIT when there
/// were fewer.
fn count_lines_backward(
    buffer: &crate::buffer::Buffer,
    start: usize,
    limit: usize,
    count: usize,
    selective: bool,
) -> (usize, usize) {
    if count == 0 || start <= limit {
        return (0, limit);
    }
    if !selective {
        // Newline #K (from the buffer's start) ends line K; the one the
        // scan stops at is the COUNTth before START.
        let before_start = buffer.line_number_at_pos(start) - 1;
        let between = before_start - (buffer.line_number_at_pos(limit) - 1);
        if between < count {
            return (between, limit);
        }
        let newline = before_start - count + 1;
        return (count - 1, buffer.line_start_of(newline + 1));
    }
    let mut found = 0;
    let mut pos = start;
    while pos > limit {
        pos -= 1;
        if matches!(buffer.char_at(pos), Some('\n' | '\r')) {
            found += 1;
            if found == count {
                return (count - 1, pos + 1);
            }
        }
    }
    (found, limit)
}

/// xdisp.c decode_mode_spec_coding: CODING's mnemonic (`-' or ` ' when
/// not yet decided), with EOL_FLAG its end-of-line mnemonic.
fn decode_mode_spec_coding(
    interp: &mut Interpreter,
    env: &mut Env,
    coding: &Value,
    buf: &mut String,
    eol_flag: bool,
) {
    let multibyte = interp.buffer.borrow().is_multibyte();
    let defined = !coding.is_nil()
        && primitive(interp, env, "coding-system-p", std::slice::from_ref(coding))
            .is_ok_and(|value| value.is_truthy());
    let mut eoltype = Value::Nil;
    if !defined {
        buf.push(if multibyte { '-' } else { ' ' });
        if eol_flag {
            eoltype = bvar(interp, "eol-mnemonic-undecided");
        }
    } else {
        if multibyte {
            let plist = primitive(
                interp,
                env,
                "coding-system-plist",
                std::slice::from_ref(coding),
            )
            .unwrap_or(Value::Nil);
            let mnemonic = plist_get(plist, Value::symbol(":mnemonic"));
            buf.push(
                mnemonic
                    .integer_value()
                    .and_then(|code| char::from_u32(code as u32))
                    .unwrap_or('-'),
            );
        } else {
            buf.push(' ');
        }
        if eol_flag {
            let eol = primitive(
                interp,
                env,
                "coding-system-eol-type",
                std::slice::from_ref(coding),
            )
            .unwrap_or(Value::Nil);
            eoltype = bvar(
                interp,
                match eol.kind() {
                    Kind::Integer(0) => "eol-mnemonic-unix",
                    Kind::Integer(1) => "eol-mnemonic-dos",
                    Kind::Integer(2) => "eol-mnemonic-mac",
                    _ => "eol-mnemonic-undecided",
                },
            );
        }
    }
    if eol_flag {
        if let Some(text) = string_like(&eoltype) {
            buf.push_str(&text.text);
        } else if let Some(c) = eoltype
            .integer_value()
            .and_then(|code| char::from_u32(code as u32))
        {
            buf.push(c);
        } else {
            buf.push_str("(*invalid*)");
        }
    }
}

// ── Window state the mode line keeps ───────────────────────────────────

/// window.h's base_line_number and base_line_pos, with the buffer and
/// the (modiff, begv, zv) they were computed under.
#[derive(Default)]
struct BaseLine {
    number: i64,
    pos: i64,
    buffer: Option<u64>,
    stamp: (u64, usize, usize),
}

impl BaseLine {
    fn read(interp: &Interpreter, window_id: u64) -> Self {
        let slot = window_slot_value(interp, window_id, WINDOW_BASE_LINE_SLOT);
        let items = slot.to_vec().unwrap_or_default();
        let int = |index: usize| {
            items
                .get(index)
                .and_then(|v| v.integer_value())
                .unwrap_or(0)
        };
        if items.len() != 6 {
            return Self::default();
        }
        Self {
            number: int(0),
            pos: int(1),
            buffer: Some(int(2) as u64),
            stamp: (int(3) as u64, int(4) as usize, int(5) as usize),
        }
    }

    fn write(&self, interp: &mut Interpreter, window_id: u64) {
        let value = Value::list([
            Value::Integer(self.number),
            Value::Integer(self.pos),
            Value::Integer(self.buffer.unwrap_or(0) as i64),
            Value::Integer(self.stamp.0 as i64),
            Value::Integer(self.stamp.1 as i64),
            Value::Integer(self.stamp.2 as i64),
        ]);
        let _ = set_window_slot_value(interp, window_id, WINDOW_BASE_LINE_SLOT, value);
    }
}

/// w->window_end_pos: Z minus the end of the window's last redisplay;
/// the live metrics answer while the window's lines are being drawn, 0
/// (the end of the buffer) before any.
fn window_end_pos(interp: &Interpreter, window_id: u64, z: i64) -> i64 {
    if let Some(pos) = window_slot_value(interp, window_id, WINDOW_END_POS_SLOT).integer_value() {
        return pos;
    }
    match interactive_window_metrics() {
        Some(metrics) if window_id == interp.selected_window_id() => z - metrics.window_end as i64,
        _ => 0,
    }
}

/// Record the end of WINDOW_ID's displayed text for its mode line's
/// %-constructs, as redisplay sets w->window_end_pos.
pub(super) fn record_window_end(interp: &mut Interpreter, window_id: u64, end: usize) {
    let z = {
        let buffer = interp.buffer.borrow();
        buffer.size_total() + 1
    };
    let _ = set_window_slot_value(
        interp,
        window_id,
        WINDOW_END_POS_SLOT,
        Value::Integer(z as i64 - end as i64),
    );
}

// ── Entry points ───────────────────────────────────────────────────────

/// xdisp.c Fformat_mode_line.
pub(super) fn format_mode_line(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    need_arg_range("format-mode-line", args, 1, 4)?;
    let arg = |index: usize| args.get(index).copied().unwrap_or(Value::Nil);
    let format = arg(0);
    let mut face = arg(1);
    // decode_any_window, then WINDOW's buffer (CHECK_BUFFER), before the
    // noninteractive no-op.
    let window = if arg(2).is_nil() {
        interp.selected_window_value()
    } else {
        arg(2)
    };
    let window_id = window_record_id_from_value(interp, &window)
        .ok_or_else(|| LispError::WrongTypeArgument("windowp".into(), window))?;
    let buffer = if arg(3).is_nil() {
        match window_buffer_id(interp, &window) {
            Some(id) => interp.buffer_value(id).unwrap_or(Value::Nil),
            None => {
                let contents = window_slot_value(interp, window_id, WINDOW_FIRST_CHILD_SLOT);
                return Err(LispError::WrongTypeArgument("bufferp".into(), contents));
            }
        }
    } else {
        arg(3)
    };
    let Kind::Buffer(buffer_ref) = buffer.kind() else {
        return Err(LispError::WrongTypeArgument("bufferp".into(), buffer));
    };
    let buffer_id = buffer_ref.id;
    if !interp
        .buffer_object(buffer_id)
        .is_some_and(|live| live.ptr_eq(&buffer_ref))
    {
        return Err(LispError::Signal(
            "Attempt to format a mode line for a dead buffer".into(),
        ));
    }
    // Formatting is a no-op when noninteractive (the frame is only half
    // made); a frontend that publishes window geometry is interactive.
    if format.is_nil() || interactive_window_metrics().is_none() {
        return Ok(Value::String(String::new().into()));
    }
    let no_props = face.integer_value().is_some();
    if no_props {
        face = Value::Nil;
    }
    // unwind_format_mode_line: the selected window (before the buffer),
    // the point of the window's buffer it left selected, then the buffer.
    let old_window = interp.selected_window_value();
    let old_buffer = interp.current_buffer_id();
    let old_window_buffer = interp.selected_window_buffer_id();
    let old_window_point = interp
        .get_buffer_by_id(old_window_buffer)
        .map(|buffer| buffer.point())
        .unwrap_or_else(|| interp.buffer.borrow().point());
    select_window_value(interp, &window, true)?;
    let switched = interp.set_current_buffer_id(buffer_id);
    let result = switched.and_then(|_| {
        let mut ml = ModeLine::new(
            if no_props {
                Target::NoProp
            } else {
                Target::String
            },
            window_id,
        );
        ml.string_face = face;
        ml.string_face_prop = if face.is_nil() {
            Value::Nil
        } else {
            Value::list([Value::symbol("face"), face])
        };
        ml.display_mode_element(interp, env, 0, 0, 0, format, Value::Nil, false)?;
        ml.result(interp, env)
    });
    let restored = select_window_value(interp, &old_window, true).map(|_| ());
    if let Some(mut buffer) = interp.get_buffer_by_id_mut(old_window_buffer) {
        buffer.goto_char(old_window_point);
    }
    let reset = interp.set_current_buffer_id(old_buffer).map(|_| ());
    let value = result?;
    restored?;
    reset?;
    Ok(value)
}

impl ModeLine<'_> {
    /// The formatted string: the NOPROP text, or the STRING pieces
    /// concatenated.
    fn result(&mut self, interp: &mut Interpreter, env: &mut Env) -> Result<Value, LispError> {
        match self.target {
            Target::NoProp => Ok(Value::String(std::mem::take(&mut self.noprop).into())),
            _ => {
                let pieces: Vec<Value> = self.strings.iter().copied().collect();
                if pieces.is_empty() {
                    return Ok(Value::String(String::new().into()));
                }
                primitive(interp, env, "concat", &pieces)
            }
        }
    }
}

/// xdisp.c display_mode_line: FORMAT laid out on a terminal row of WIDTH
/// cells for the window, with the iterator's face runs.  A mode line
/// under `mode-line-compact' is `format-mode-line''s string with runs of
/// spaces squeezed.
pub(super) fn display_mode_line(
    interp: &mut Interpreter,
    env: &mut Env,
    window_id: u64,
    format: Value,
    width: usize,
    compact: bool,
    cells: &dyn ModeLineCells,
) -> Result<(String, FaceSpans), LispError> {
    let tab_width = match bvar(interp, "tab-width").kind() {
        Kind::Integer(width) if (1..=1000).contains(&width) => width as usize,
        _ => 8,
    };
    let invisibility = crate::lisp::primitives::invisibility::invisibility_spec_of(bvar(
        interp,
        "buffer-invisibility-spec",
    ));
    let mut ml = ModeLine::new(Target::Display, window_id);
    ml.row = Some(Row {
        cells,
        text: String::new(),
        length: 0,
        faces: RootedVec::new(),
        x: 0,
        hpos: 0,
        last_visible_x: width,
        tab_width,
        min_width: Value::Nil,
        min_width_start: 0,
        invisibility,
    });
    // record_unwind_save_match_data.
    let match_data = primitive(interp, env, "match-data", &[])?;
    let result = (|| -> Result<(), LispError> {
        let compact_value = bvar(interp, "mode-line-compact");
        if !compact || compact_value.is_nil() {
            ml.display_mode_element(interp, env, 0, 0, 0, format, Value::Nil, false)?;
            return Ok(());
        }
        let mut string_ml = ModeLine::new(Target::String, window_id);
        string_ml.display_mode_element(interp, env, 0, 0, 0, format, Value::Nil, false)?;
        let mode_string = string_ml.result(interp, env)?;
        let chars = string_chars(&mode_string);
        let substring = |interp: &mut Interpreter, env: &mut Env, from: usize, to: usize| {
            primitive(
                interp,
                env,
                "substring",
                &[
                    mode_string,
                    Value::Integer(from as i64),
                    Value::Integer(to as i64),
                ],
            )
        };
        if matches!(compact_value.kind(), Kind::Symbol(name) if name == "long")
            && window_total_columns(interp, window_id).max(0) as usize >= chars.len()
        {
            ml.display_string(interp, env, None, Some(mode_string), None, 0, 0, 0)?;
            return Ok(());
        }
        // Compress the mode line: a run of spaces shows as one.
        let (mut i, mut start, mut prev) = (0usize, 0usize, '\0');
        while i < chars.len() {
            let mut c = chars[i];
            i += 1;
            if c == ' ' && prev == ' ' {
                let piece = substring(interp, env, start, i - 1)?;
                ml.display_string(interp, env, None, Some(piece), None, 0, 0, 0)?;
                while c == ' ' && i < chars.len() {
                    c = chars[i];
                    i += 1;
                }
                start = i - 1;
            }
            prev = c;
        }
        if start < i {
            let piece = substring(interp, env, start, i)?;
            ml.display_string(interp, env, None, Some(piece), None, 0, 0, 0)?;
        }
        Ok(())
    })();
    primitive(interp, env, "set-match-data", &[match_data, Value::T])?;
    result?;
    // Fill up with spaces.
    ml.display_string(interp, env, Some(vec![' ']), None, None, 0, 10_000, -1)?;
    let row = ml.row.take().expect("display row");
    Ok((row.text, row.faces.iter().copied().collect()))
}
