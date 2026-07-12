use super::*;
use std::collections::HashSet;

/// Hidden binding stamped into lexical binding frames to give them an
/// identity: frames are plain vectors, and without a marker two unrelated
/// frames binding the same variable names are indistinguishable to the
/// closure-environment alignment logic.
pub(crate) const FRAME_IDENTITY_MARKER: &str = "--emaxx-frame-id--";

impl Interpreter {
    fn dolist_items(value: &Value) -> Result<Vec<Value>, LispError> {
        let mut items = Vec::new();
        let mut current = value.clone();
        let mut seen = HashSet::new();
        loop {
            match current {
                Value::Nil => return Ok(items),
                Value::Cons(car, cdr) => {
                    let cell_id = std::rc::Rc::as_ptr(&car) as usize;
                    if !seen.insert(cell_id) {
                        return Err(LispError::SignalValue(Value::list([
                            Value::Symbol("circular-list".into()),
                            Value::String("Circular list".into()),
                        ])));
                    }
                    let item = {
                        let mut car_value = car.borrow_mut();
                        if matches!(&*car_value, Value::String(_)) {
                            *car_value = Self::stored_value(car_value.clone());
                        }
                        car_value.clone()
                    };
                    items.push(item);
                    current = cdr.borrow().clone();
                }
                other => return Err(LispError::TypeError("list".into(), other.type_name())),
            }
        }
    }

    pub(super) fn sf_insert_function(
        &mut self,
        items: &[Value],
        env: &mut Env,
        inherit: bool,
        before_markers: bool,
    ) -> Result<Value, LispError> {
        let mut evaluated = Vec::with_capacity(items.len().saturating_sub(1));
        for item in &items[1..] {
            evaluated.push(self.eval(item, env)?);
        }
        crate::lisp::primitives::insert_impl(self, &evaluated, env, inherit, before_markers)
    }

    pub(super) fn sf_insert_char_function(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let mut evaluated = Vec::with_capacity(items.len().saturating_sub(1));
        for item in &items[1..] {
            evaluated.push(self.eval(item, env)?);
        }
        crate::lisp::primitives::insert_char_impl(self, &evaluated, env)
    }

    pub(super) fn sf_call_interactively(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Err(LispError::WrongNumberOfArgs("call-interactively".into(), 0));
        }
        let mut evaluated = Vec::with_capacity(items.len().saturating_sub(1));
        for item in &items[1..] {
            evaluated.push(self.eval(item, env)?);
        }
        crate::lisp::primitives::call_interactively_impl(self, &evaluated, env)
    }

    pub(super) fn parse_params(&self, spec: &Value) -> Result<Vec<String>, LispError> {
        match spec {
            Value::Nil => Ok(Vec::new()),
            Value::Cons(_, _) => {
                let items = spec.to_vec()?;
                validate_lambda_list(spec, &items)?;
                items
                    .into_iter()
                    .map(|v| match v {
                        Value::Symbol(s) => Ok(s),
                        _ => Err(invalid_function(spec.clone())),
                    })
                    .collect()
            }
            _ => Err(invalid_function(spec.clone())),
        }
    }

    pub(super) fn sf_while(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        loop {
            let cond = self.eval(&items[1], env)?;
            if cond.is_nil() {
                break;
            }
            self.sf_progn(&items[2..], env)?;
        }
        Ok(Value::Nil)
    }

    pub(super) fn sf_dolist(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        let spec = items[1].to_vec()?;
        let var_name = spec[0].as_symbol()?.to_string();
        let list_val = self.eval(&spec[1], env)?;
        let list_items = Self::dolist_items(&list_val)?;

        // Upstream expands to a fresh `let' per iteration, which binds
        // special names dynamically.
        let dynamic =
            self.is_dynamic_binding_name(&var_name) || self.local_special_active(&var_name, env);
        let frame_index = env.len();
        if !dynamic {
            Self::push_marked_frame(env, vec![(var_name.clone(), Value::Nil)]);
        }
        let mut outcome = Ok(());
        for item in list_items {
            if dynamic {
                let restore = self.bind_special_variable(&var_name, item, env)?;
                let body_result = self.sf_progn(&items[2..], env);
                let restore_result = self.restore_special_binding(restore, env);
                if let Err(error) = body_result.and(restore_result) {
                    outcome = Err(error);
                    break;
                }
            } else {
                let frame = env
                    .get_mut(frame_index)
                    .expect("dolist binding frame remains active during loop");
                Self::upsert_frame_binding(frame, var_name.clone(), item);
                if let Err(error) = self.sf_progn(&items[2..], env) {
                    outcome = Err(error);
                    break;
                }
            }
        }
        let result = outcome.and_then(|()| {
            if spec.len() > 2 {
                // GNU's lexical-binding expansion leaves VAR unbound around
                // the RESULT form (each iteration used a fresh `let').
                self.eval(&spec[2], env)
            } else {
                Ok(Value::Nil)
            }
        });
        // Truncate rather than pop: the binding frame must come off even if
        // an error unwound out of the body mid-iteration.
        env.truncate(frame_index);
        result
    }

    pub(super) fn sf_dolist_with_progress_reporter(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Err(LispError::WrongNumberOfArgs(
                "dolist-with-progress-reporter".into(),
                items.len().saturating_sub(1),
            ));
        }
        let _ = self.eval(&items[2], env)?;
        let mut dolist_items = Vec::with_capacity(items.len() - 1);
        dolist_items.push(Value::Symbol("dolist".into()));
        dolist_items.push(items[1].clone());
        dolist_items.extend_from_slice(&items[3..]);
        self.sf_dolist(&dolist_items, env)
    }

    pub(super) fn sf_pcase_dolist(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let spec = items[1].to_vec()?;
        if spec.len() < 2 {
            return Err(LispError::WrongNumberOfArgs(
                "pcase-dolist".into(),
                items.len().saturating_sub(1),
            ));
        }
        let pattern = &spec[0];
        let list_val = self.eval(&spec[1], env)?;
        let list_items = list_val.to_vec()?;

        for item in list_items {
            let mut bindings = Vec::new();
            if !pcase_pattern_bindings_lenient_list(self, env, pattern, &item, &mut bindings)? {
                return Err(LispError::Signal("pcase-dolist: no matching clause".into()));
            }
            let frame_index = env.len();
            Self::push_marked_frame(env, bindings);
            let result = self.sf_progn(&items[2..], env);
            env.truncate(frame_index);
            result?;
        }

        if spec.len() > 2 {
            self.eval(&spec[2], env)
        } else {
            Ok(Value::Nil)
        }
    }

    pub(super) fn sf_dotimes(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let spec = items[1].to_vec()?;
        let var_name = spec[0].as_symbol()?.to_string();
        let count = self.eval(&spec[1], env)?.as_integer()?;

        // Upstream expands to a fresh `let' per iteration, which binds
        // special names dynamically.
        let dynamic =
            self.is_dynamic_binding_name(&var_name) || self.local_special_active(&var_name, env);
        let frame_index = env.len();
        if !dynamic {
            Self::push_marked_frame(env, vec![(var_name.clone(), Value::Integer(0))]);
        }
        let mut outcome = Ok(());
        for i in 0..count {
            if dynamic {
                let restore = self.bind_special_variable(&var_name, Value::Integer(i), env)?;
                let body_result = self.sf_progn(&items[2..], env);
                let restore_result = self.restore_special_binding(restore, env);
                if let Err(error) = body_result.and(restore_result) {
                    outcome = Err(error);
                    break;
                }
            } else {
                let frame = env
                    .get_mut(frame_index)
                    .expect("dotimes binding frame remains active during loop");
                frame[0] = (var_name.clone(), Value::Integer(i));
                if let Err(error) = self.sf_progn(&items[2..], env) {
                    outcome = Err(error);
                    break;
                }
            }
        }
        let result = outcome.and_then(|()| {
            if spec.len() > 2 {
                self.eval(&spec[2], env)
            } else {
                Ok(Value::Nil)
            }
        });
        env.truncate(frame_index);
        result
    }

    pub(super) fn sf_cl_loop(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        enum LoopSpec {
            Range {
                name: String,
                values: Vec<Value>,
            },
            List {
                pattern: Value,
                values: Vec<Value>,
            },
            ListExpr {
                pattern: Value,
                expr: Value,
            },
            From {
                name: String,
                start: i64,
            },
            Assign {
                name: String,
                expr: Value,
            },
            AssignThen {
                name: String,
                init: Value,
                step: Value,
            },
            Repeat {
                count: usize,
            },
        }

        fn is_cl_loop_clause_keyword(value: Option<&Value>) -> bool {
            matches!(
                value,
                Some(Value::Symbol(symbol))
                    if matches!(
                        symbol.as_str(),
                        "with"
                            | "for"
                            | "while"
                            | "until"
                            | "repeat"
                            | "initially"
                            | "when"
                            | "do"
                            | "collect"
                            | "append"
                            | "vconcat"
                            | "thereis"
                            | "always"
                            | "sum"
                            | "return"
                            | "unless"
                            | "if"
                    )
            )
        }

        enum LoopAction {
            Do(Vec<Value>),
            DoCollect {
                body: Vec<Value>,
                collect: Value,
            },
            Collect(Value),
            CollectDo {
                expr: Value,
                body: Vec<Value>,
            },
            Append(Value),
            Vconcat(Value),
            VconcatIntoAppendInto {
                vconcat_expr: Value,
                vconcat_name: String,
                append_expr: Value,
                append_name: String,
            },
            Thereis {
                expr: Value,
                until: Option<Value>,
            },
            Always(Value),
            Sum(Value),
            Return(Value),
            WhenReturn {
                condition: Value,
                expr: Value,
            },
            WhenCollect {
                condition: Value,
                expr: Value,
            },
            WhenDo {
                condition: Value,
                body: Vec<Value>,
            },
            WhenAppend {
                condition: Value,
                expr: Value,
            },
            WhenCollectInto {
                condition: Value,
                expr: Value,
                name: String,
            },
            NestedWhenCollectInto(Box<NestedWhenCollectIntoAction>),
            WhenCollectIntoWhenDoWhenDo {
                collect_condition: Value,
                collect_expr: Value,
                collect_name: String,
                first_do_condition: Value,
                first_do_body: Vec<Value>,
                second_do_condition: Value,
                second_do_body: Vec<Value>,
            },
            UnlessCollect {
                condition: Value,
                expr: Value,
            },
            UnlessCountElseCount {
                condition: Value,
                unless_name: String,
                else_name: String,
            },
            CollectInto {
                expr: Value,
                name: String,
            },
            IfDoAppend {
                condition: Value,
                body: Value,
                append: Value,
            },
            IfDoElseDo {
                condition: Value,
                then_body: Vec<Value>,
                else_body: Vec<Value>,
            },
            IfCollectElseCollect {
                condition: Value,
                collect: Value,
                else_collect: Value,
            },
            IfCollectIntoElseCollectInto {
                condition: Value,
                collect: Value,
                collect_name: String,
                else_collect: Value,
                else_collect_name: String,
            },
            IfCollectElseAppend {
                condition: Value,
                collect: Value,
                append: Value,
            },
            UnlessDo {
                condition: Value,
                body: Vec<Value>,
                after_body: Vec<Value>,
            },
        }

        struct NestedWhenCollectIntoAction {
            condition: Value,
            then_condition: Value,
            then_expr: Value,
            then_name: String,
            then_else_expr: Value,
            then_else_name: String,
            else_condition: Value,
            else_expr: Value,
            else_name: String,
            else_else_expr: Value,
            else_else_name: String,
        }

        if items.len() < 3 {
            return Err(LispError::WrongNumberOfArgs(
                "cl-loop".into(),
                items.len().saturating_sub(1),
            ));
        }
        let mut specs = Vec::new();
        let mut with_binding_groups = Vec::new();
        let mut while_expr = None;
        let mut until_expr = None;
        // GNU cl-loop evaluates clauses in written order, so assignment
        // clauses that appear before a while/until guard must update before
        // the guard is tested each iteration.
        let mut guard_assign_position: Option<usize> = None;
        let mut initially_body = Vec::new();
        let mut index = 1usize;
        let named_loop_tag = if matches!(items.get(index), Some(Value::Symbol(symbol)) if symbol == "named")
        {
            let name = items
                .get(index + 1)
                .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                .as_symbol()?;
            index += 2;
            Some(Value::Symbol(format!("--cl-block-{name}--")))
        } else {
            None
        };
        while index < items.len() {
            match items.get(index) {
                Some(Value::Symbol(symbol)) if symbol == "with" => {
                    let mut group = Vec::new();
                    loop {
                        let pattern = items
                            .get(index + 1)
                            .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                            .clone();
                        let (expr, next_index) = if matches!(items.get(index + 2), Some(Value::Symbol(eq)) if eq == "=")
                        {
                            (
                                Some(
                                    items
                                        .get(index + 3)
                                        .ok_or_else(|| {
                                            LispError::Signal("Unsupported cl-loop syntax".into())
                                        })?
                                        .clone(),
                                ),
                                index + 4,
                            )
                        } else {
                            (None, index + 2)
                        };
                        group.push((pattern, expr));
                        if !matches!(items.get(next_index), Some(Value::Symbol(and)) if and == "and")
                        {
                            index = next_index;
                            break;
                        }
                        index = next_index;
                    }
                    with_binding_groups.push(group);
                }
                Some(Value::Symbol(symbol)) if symbol == "for" => {
                    let pattern = items
                        .get(index + 1)
                        .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                        .clone();
                    match items.get(index + 2) {
                        Some(Value::Symbol(kind)) if kind == "from" => {
                            let name = pattern.as_symbol()?.to_string();
                            let start = self
                                .eval(
                                    items.get(index + 3).ok_or_else(|| {
                                        LispError::Signal("Unsupported cl-loop syntax".into())
                                    })?,
                                    env,
                                )?
                                .as_integer()?;
                            match items
                                .get(index + 4)
                                .and_then(|value| value.as_symbol().ok())
                            {
                                Some("to") | Some("upto") | Some("below") => {
                                    let bound_kind =
                                        items[index + 4].as_symbol().expect("checked symbol");
                                    let end = self
                                        .eval(
                                            items.get(index + 5).ok_or_else(|| {
                                                LispError::Signal(
                                                    "Unsupported cl-loop syntax".into(),
                                                )
                                            })?,
                                            env,
                                        )?
                                        .as_integer()?;
                                    let mut step = 1usize;
                                    let mut next_index = 6usize;
                                    if matches!(items.get(index + 6), Some(Value::Symbol(by)) if by == "by")
                                    {
                                        step = self
                                            .eval(
                                                items.get(index + 7).ok_or_else(|| {
                                                    LispError::Signal(
                                                        "Unsupported cl-loop syntax".into(),
                                                    )
                                                })?,
                                                env,
                                            )?
                                            .as_integer()?
                                            .max(1)
                                            as usize;
                                        next_index = 8;
                                    }
                                    let values = match bound_kind {
                                        "to" | "upto" if start <= end => (start..=end)
                                            .step_by(step)
                                            .map(Value::Integer)
                                            .collect(),
                                        "below" if start < end => {
                                            (start..end).step_by(step).map(Value::Integer).collect()
                                        }
                                        "to" | "upto" | "below" => Vec::new(),
                                        _ => unreachable!(),
                                    };
                                    specs.push(LoopSpec::Range { name, values });
                                    index += next_index;
                                }
                                _ => {
                                    specs.push(LoopSpec::From { name, start });
                                    index += 4;
                                }
                            }
                        }
                        Some(Value::Symbol(kind))
                            if matches!(kind.as_str(), "to" | "upto" | "below") =>
                        {
                            let name = pattern.as_symbol()?.to_string();
                            let end = self
                                .eval(
                                    items.get(index + 3).ok_or_else(|| {
                                        LispError::Signal("Unsupported cl-loop syntax".into())
                                    })?,
                                    env,
                                )?
                                .as_integer()?;
                            let mut step = 1usize;
                            let mut next_index = 4usize;
                            if matches!(items.get(index + 4), Some(Value::Symbol(by)) if by == "by")
                            {
                                step = self
                                    .eval(
                                        items.get(index + 5).ok_or_else(|| {
                                            LispError::Signal("Unsupported cl-loop syntax".into())
                                        })?,
                                        env,
                                    )?
                                    .as_integer()?
                                    .max(1) as usize;
                                next_index = 6;
                            }
                            let values = match kind.as_str() {
                                "to" | "upto" if end >= 0 => {
                                    (0..=end).step_by(step).map(Value::Integer).collect()
                                }
                                "below" if end > 0 => {
                                    (0..end).step_by(step).map(Value::Integer).collect()
                                }
                                "to" | "upto" | "below" => Vec::new(),
                                _ => unreachable!(),
                            };
                            specs.push(LoopSpec::Range { name, values });
                            index += next_index;
                        }
                        Some(Value::Symbol(kind)) if kind == "in" => {
                            let expr = items
                                .get(index + 3)
                                .ok_or_else(|| {
                                    LispError::Signal("Unsupported cl-loop syntax".into())
                                })?
                                .clone();
                            specs.push(LoopSpec::ListExpr { pattern, expr });
                            index += 4;
                        }
                        Some(Value::Symbol(kind)) if kind == "across" => {
                            let name = pattern.as_symbol()?.to_string();
                            let sequence = self.eval(
                                items.get(index + 3).ok_or_else(|| {
                                    LispError::Signal("Unsupported cl-loop syntax".into())
                                })?,
                                env,
                            )?;
                            let values = if sequence.is_string() {
                                crate::lisp::primitives::string_text(&sequence)?
                                    .chars()
                                    .map(|ch| Value::Integer(ch as i64))
                                    .collect()
                            } else {
                                crate::lisp::primitives::vector_items(&sequence)?
                            };
                            specs.push(LoopSpec::List {
                                pattern: Value::Symbol(name),
                                values,
                            });
                            index += 4;
                        }
                        Some(Value::Symbol(kind)) if kind == "=" => {
                            let name = pattern.as_symbol()?.to_string();
                            let expr = items
                                .get(index + 3)
                                .ok_or_else(|| {
                                    LispError::Signal("Unsupported cl-loop syntax".into())
                                })?
                                .clone();
                            if matches!(items.get(index + 4), Some(Value::Symbol(kind)) if kind == "then")
                            {
                                let step = items
                                    .get(index + 5)
                                    .ok_or_else(|| {
                                        LispError::Signal("Unsupported cl-loop syntax".into())
                                    })?
                                    .clone();
                                specs.push(LoopSpec::AssignThen {
                                    name,
                                    init: expr,
                                    step,
                                });
                                index += 6;
                            } else {
                                specs.push(LoopSpec::Assign { name, expr });
                                index += 4;
                            }
                        }
                        _ => return Err(LispError::Signal("Unsupported cl-loop syntax".into())),
                    }
                }
                Some(Value::Symbol(symbol)) if symbol == "while" => {
                    if guard_assign_position.is_none() {
                        guard_assign_position = Some(
                            specs
                                .iter()
                                .filter(|spec| {
                                    matches!(
                                        spec,
                                        LoopSpec::Assign { .. } | LoopSpec::AssignThen { .. }
                                    )
                                })
                                .count(),
                        );
                    }
                    while_expr = Some(
                        items
                            .get(index + 1)
                            .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                            .clone(),
                    );
                    index += 2;
                }
                Some(Value::Symbol(symbol)) if symbol == "until" => {
                    if guard_assign_position.is_none() {
                        guard_assign_position = Some(
                            specs
                                .iter()
                                .filter(|spec| {
                                    matches!(
                                        spec,
                                        LoopSpec::Assign { .. } | LoopSpec::AssignThen { .. }
                                    )
                                })
                                .count(),
                        );
                    }
                    until_expr = Some(
                        items
                            .get(index + 1)
                            .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                            .clone(),
                    );
                    index += 2;
                }
                Some(Value::Symbol(symbol)) if symbol == "initially" => {
                    index += 1;
                    while index < items.len() && !is_cl_loop_clause_keyword(items.get(index)) {
                        initially_body.push(items[index].clone());
                        index += 1;
                    }
                    if initially_body.is_empty() || index >= items.len() {
                        return Err(LispError::Signal("Unsupported cl-loop syntax".into()));
                    }
                }
                Some(Value::Symbol(symbol)) if symbol == "repeat" => {
                    let count = self
                        .eval(
                            items.get(index + 1).ok_or_else(|| {
                                LispError::Signal("Unsupported cl-loop syntax".into())
                            })?,
                            env,
                        )?
                        .as_integer()?
                        .max(0) as usize;
                    specs.push(LoopSpec::Repeat { count });
                    index += 2;
                }
                _ => break,
            }
        }

        if (specs.is_empty()
            && with_binding_groups.is_empty()
            && while_expr.is_none()
            && until_expr.is_none())
            || index >= items.len()
        {
            return Err(LispError::Signal("Unsupported cl-loop syntax".into()));
        }

        let mut bindings = specs
            .iter()
            .filter_map(|spec| match spec {
                LoopSpec::Range { name, .. }
                | LoopSpec::From { name, .. }
                | LoopSpec::Assign { name, .. }
                | LoopSpec::AssignThen { name, .. } => Some((name.clone(), Value::Nil)),
                LoopSpec::List { .. } | LoopSpec::ListExpr { .. } | LoopSpec::Repeat { .. } => None,
            })
            .collect::<Vec<_>>();
        for spec in &specs {
            if let LoopSpec::List { pattern, .. } | LoopSpec::ListExpr { pattern, .. } = spec {
                self.collect_cl_pattern_names(pattern, &mut bindings)?;
            }
        }
        Self::push_marked_frame(env, bindings);
        for group in &with_binding_groups {
            let values = group
                .iter()
                .map(|(_, expr)| {
                    expr.as_ref()
                        .map_or(Ok(Value::Nil), |expr| self.eval(expr, env))
                })
                .collect::<Result<Vec<_>, _>>()?;
            let frame = env.last_mut().expect("env frame just pushed");
            for ((pattern, _), value) in group.iter().zip(values) {
                self.bind_cl_pattern(pattern, value, frame)?;
            }
        }

        let specs = specs
            .into_iter()
            .map(|spec| match spec {
                LoopSpec::ListExpr { pattern, expr } => {
                    let values = self.eval(&expr, env)?.to_vec()?;
                    Ok(LoopSpec::List { pattern, values })
                }
                other => Ok(other),
            })
            .collect::<Result<Vec<_>, LispError>>()?;

        let iterations = specs
            .iter()
            .filter_map(|spec| match spec {
                LoopSpec::Range { values, .. } | LoopSpec::List { values, .. } => {
                    Some(values.len())
                }
                LoopSpec::Repeat { count } => Some(*count),
                LoopSpec::From { .. } | LoopSpec::Assign { .. } | LoopSpec::AssignThen { .. } => {
                    None
                }
                LoopSpec::ListExpr { .. } => unreachable!("list expressions are resolved"),
            })
            .min()
            .unwrap_or(if while_expr.is_some() || until_expr.is_some() {
                usize::MAX
            } else {
                1
            });

        let mut final_return = None;
        if matches!(items.get(index), Some(Value::Symbol(symbol)) if symbol == "initially") {
            index += 1;
            while index < items.len() && !is_cl_loop_clause_keyword(items.get(index)) {
                initially_body.push(items[index].clone());
                index += 1;
            }
            if initially_body.is_empty() || index >= items.len() {
                return Err(LispError::Signal("Unsupported cl-loop syntax".into()));
            }
        }
        let action = match items.get(index) {
            Some(Value::Symbol(symbol)) if symbol == "when" => match items.get(index + 2) {
                Some(Value::Symbol(kind)) if kind == "when" => {
                    if !matches!(items.get(index + 4), Some(Value::Symbol(kind)) if kind == "collect")
                        || !matches!(items.get(index + 6), Some(Value::Symbol(kind)) if kind == "into")
                        || !matches!(items.get(index + 8), Some(Value::Symbol(kind)) if kind == "else")
                        || !matches!(items.get(index + 9), Some(Value::Symbol(kind)) if kind == "collect")
                        || !matches!(items.get(index + 11), Some(Value::Symbol(kind)) if kind == "into")
                        || !matches!(items.get(index + 13), Some(Value::Symbol(kind)) if kind == "else")
                        || !matches!(items.get(index + 14), Some(Value::Symbol(kind)) if kind == "when")
                        || !matches!(items.get(index + 16), Some(Value::Symbol(kind)) if kind == "collect")
                        || !matches!(items.get(index + 18), Some(Value::Symbol(kind)) if kind == "into")
                        || !matches!(items.get(index + 20), Some(Value::Symbol(kind)) if kind == "else")
                        || !matches!(items.get(index + 21), Some(Value::Symbol(kind)) if kind == "collect")
                        || !matches!(items.get(index + 23), Some(Value::Symbol(kind)) if kind == "into")
                        || !matches!(items.get(index + 25), Some(Value::Symbol(kind)) if kind == "finally")
                        || !matches!(items.get(index + 26), Some(Value::Symbol(kind)) if kind == "return")
                    {
                        return Err(LispError::Signal("Unsupported cl-loop syntax".into()));
                    }
                    final_return = Some(
                        items
                            .get(index + 27)
                            .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                            .clone(),
                    );
                    LoopAction::NestedWhenCollectInto(Box::new(NestedWhenCollectIntoAction {
                        condition: items
                            .get(index + 1)
                            .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                            .clone(),
                        then_condition: items
                            .get(index + 3)
                            .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                            .clone(),
                        then_expr: items
                            .get(index + 5)
                            .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                            .clone(),
                        then_name: items
                            .get(index + 7)
                            .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                            .as_symbol()?
                            .to_string(),
                        then_else_expr: items
                            .get(index + 10)
                            .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                            .clone(),
                        then_else_name: items
                            .get(index + 12)
                            .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                            .as_symbol()?
                            .to_string(),
                        else_condition: items
                            .get(index + 15)
                            .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                            .clone(),
                        else_expr: items
                            .get(index + 17)
                            .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                            .clone(),
                        else_name: items
                            .get(index + 19)
                            .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                            .as_symbol()?
                            .to_string(),
                        else_else_expr: items
                            .get(index + 22)
                            .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                            .clone(),
                        else_else_name: items
                            .get(index + 24)
                            .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                            .as_symbol()?
                            .to_string(),
                    }))
                }
                Some(Value::Symbol(kind)) if kind == "return" => {
                    if items.get(index + 4).is_some() {
                        if !matches!(items.get(index + 4), Some(Value::Symbol(kind)) if kind == "finally")
                            || !matches!(items.get(index + 5), Some(Value::Symbol(kind)) if kind == "return")
                        {
                            return Err(LispError::Signal("Unsupported cl-loop syntax".into()));
                        }
                        final_return = Some(
                            items
                                .get(index + 6)
                                .ok_or_else(|| {
                                    LispError::Signal("Unsupported cl-loop syntax".into())
                                })?
                                .clone(),
                        );
                    }
                    LoopAction::WhenReturn {
                        condition: items
                            .get(index + 1)
                            .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                            .clone(),
                        expr: items
                            .get(index + 3)
                            .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                            .clone(),
                    }
                }
                Some(Value::Symbol(kind)) if kind == "collect" => {
                    if matches!(items.get(index + 4), Some(Value::Symbol(kind)) if kind == "into")
                        && matches!(items.get(index + 6), Some(Value::Symbol(kind)) if kind == "when")
                    {
                        if !matches!(items.get(index + 8), Some(Value::Symbol(kind)) if kind == "do")
                        {
                            return Err(LispError::Signal("Unsupported cl-loop syntax".into()));
                        }
                        let first_do_start = index + 9;
                        let second_when_index = items[first_do_start..]
                            .iter()
                            .position(
                                |item| matches!(item, Value::Symbol(symbol) if symbol == "when"),
                            )
                            .map(|offset| first_do_start + offset)
                            .ok_or_else(|| {
                                LispError::Signal("Unsupported cl-loop syntax".into())
                            })?;
                        if !matches!(items.get(second_when_index + 2), Some(Value::Symbol(kind)) if kind == "do")
                        {
                            return Err(LispError::Signal("Unsupported cl-loop syntax".into()));
                        }
                        LoopAction::WhenCollectIntoWhenDoWhenDo {
                            collect_condition: items
                                .get(index + 1)
                                .ok_or_else(|| {
                                    LispError::Signal("Unsupported cl-loop syntax".into())
                                })?
                                .clone(),
                            collect_expr: items
                                .get(index + 3)
                                .ok_or_else(|| {
                                    LispError::Signal("Unsupported cl-loop syntax".into())
                                })?
                                .clone(),
                            collect_name: items
                                .get(index + 5)
                                .ok_or_else(|| {
                                    LispError::Signal("Unsupported cl-loop syntax".into())
                                })?
                                .as_symbol()?
                                .to_string(),
                            first_do_condition: items
                                .get(index + 7)
                                .ok_or_else(|| {
                                    LispError::Signal("Unsupported cl-loop syntax".into())
                                })?
                                .clone(),
                            first_do_body: items[first_do_start..second_when_index].to_vec(),
                            second_do_condition: items
                                .get(second_when_index + 1)
                                .ok_or_else(|| {
                                    LispError::Signal("Unsupported cl-loop syntax".into())
                                })?
                                .clone(),
                            second_do_body: items[second_when_index + 3..].to_vec(),
                        }
                    } else if !matches!(items.get(index + 4), Some(Value::Symbol(kind)) if kind == "into")
                    {
                        LoopAction::WhenCollect {
                            condition: items
                                .get(index + 1)
                                .ok_or_else(|| {
                                    LispError::Signal("Unsupported cl-loop syntax".into())
                                })?
                                .clone(),
                            expr: items
                                .get(index + 3)
                                .ok_or_else(|| {
                                    LispError::Signal("Unsupported cl-loop syntax".into())
                                })?
                                .clone(),
                        }
                    } else {
                        if !matches!(items.get(index + 6), Some(Value::Symbol(kind)) if kind == "finally")
                            || !matches!(items.get(index + 7), Some(Value::Symbol(kind)) if kind == "return")
                        {
                            return Err(LispError::Signal("Unsupported cl-loop syntax".into()));
                        }
                        let name = items
                            .get(index + 5)
                            .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                            .as_symbol()?
                            .to_string();
                        final_return = Some(
                            items
                                .get(index + 8)
                                .ok_or_else(|| {
                                    LispError::Signal("Unsupported cl-loop syntax".into())
                                })?
                                .clone(),
                        );
                        LoopAction::WhenCollectInto {
                            condition: items
                                .get(index + 1)
                                .ok_or_else(|| {
                                    LispError::Signal("Unsupported cl-loop syntax".into())
                                })?
                                .clone(),
                            expr: items
                                .get(index + 3)
                                .ok_or_else(|| {
                                    LispError::Signal("Unsupported cl-loop syntax".into())
                                })?
                                .clone(),
                            name,
                        }
                    }
                }
                Some(Value::Symbol(kind)) if kind == "append" => LoopAction::WhenAppend {
                    condition: items
                        .get(index + 1)
                        .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                        .clone(),
                    expr: items
                        .get(index + 3)
                        .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                        .clone(),
                },
                Some(Value::Symbol(kind)) if kind == "do" => {
                    let body_start = index + 3;
                    let finally_index = items[body_start..]
                        .iter()
                        .position(
                            |item| matches!(item, Value::Symbol(symbol) if symbol == "finally"),
                        )
                        .map(|offset| body_start + offset);
                    if let Some(finally_index) = finally_index {
                        if !matches!(items.get(finally_index + 1), Some(Value::Symbol(kind)) if kind == "return")
                        {
                            return Err(LispError::Signal("Unsupported cl-loop syntax".into()));
                        }
                        final_return = Some(
                            items
                                .get(finally_index + 2)
                                .ok_or_else(|| {
                                    LispError::Signal("Unsupported cl-loop syntax".into())
                                })?
                                .clone(),
                        );
                        LoopAction::WhenDo {
                            condition: items
                                .get(index + 1)
                                .ok_or_else(|| {
                                    LispError::Signal("Unsupported cl-loop syntax".into())
                                })?
                                .clone(),
                            body: items[body_start..finally_index].to_vec(),
                        }
                    } else {
                        LoopAction::WhenDo {
                            condition: items
                                .get(index + 1)
                                .ok_or_else(|| {
                                    LispError::Signal("Unsupported cl-loop syntax".into())
                                })?
                                .clone(),
                            body: items[body_start..].to_vec(),
                        }
                    }
                }
                _ => return Err(LispError::Signal("Unsupported cl-loop syntax".into())),
            },
            Some(Value::Symbol(symbol)) if symbol == "do" => {
                let body_start = index + 1;
                let finally_index = items[body_start..]
                    .iter()
                    .position(|item| matches!(item, Value::Symbol(symbol) if symbol == "finally"))
                    .map(|offset| body_start + offset);
                if let Some(finally_index) = finally_index {
                    let final_expr_index = if matches!(
                        items.get(finally_index + 1),
                        Some(Value::Symbol(symbol)) if symbol == "return"
                    ) {
                        finally_index + 2
                    } else {
                        finally_index + 1
                    };
                    final_return = Some(
                        items
                            .get(final_expr_index)
                            .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                            .clone(),
                    );
                    LoopAction::Do(items[body_start..finally_index].to_vec())
                } else {
                    let collect_index = items[body_start..]
                        .iter()
                        .position(
                            |item| matches!(item, Value::Symbol(symbol) if symbol == "collect"),
                        )
                        .map(|offset| body_start + offset);
                    if let Some(collect_index) = collect_index {
                        if collect_index <= body_start || collect_index + 2 != items.len() {
                            return Err(LispError::Signal("Unsupported cl-loop syntax".into()));
                        }
                        LoopAction::DoCollect {
                            body: items[body_start..collect_index].to_vec(),
                            collect: items
                                .get(collect_index + 1)
                                .ok_or_else(|| {
                                    LispError::Signal("Unsupported cl-loop syntax".into())
                                })?
                                .clone(),
                        }
                    } else {
                        LoopAction::Do(items[body_start..].to_vec())
                    }
                }
            }
            Some(Value::Symbol(symbol)) if symbol == "collect" => {
                if !matches!(items.get(index + 2), Some(Value::Symbol(kind)) if kind == "into") {
                    if matches!(items.get(index + 2), Some(Value::Symbol(kind)) if kind == "do") {
                        LoopAction::CollectDo {
                            expr: items
                                .get(index + 1)
                                .ok_or_else(|| {
                                    LispError::Signal("Unsupported cl-loop syntax".into())
                                })?
                                .clone(),
                            body: items[index + 3..].to_vec(),
                        }
                    } else {
                        LoopAction::Collect(
                            items
                                .get(index + 1)
                                .ok_or_else(|| {
                                    LispError::Signal("Unsupported cl-loop syntax".into())
                                })?
                                .clone(),
                        )
                    }
                } else {
                    if !matches!(items.get(index + 4), Some(Value::Symbol(kind)) if kind == "finally")
                        || !matches!(items.get(index + 5), Some(Value::Symbol(kind)) if kind == "return")
                    {
                        return Err(LispError::Signal("Unsupported cl-loop syntax".into()));
                    }
                    let name = items
                        .get(index + 3)
                        .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                        .as_symbol()?
                        .to_string();
                    final_return = Some(
                        items
                            .get(index + 6)
                            .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                            .clone(),
                    );
                    LoopAction::CollectInto {
                        expr: items
                            .get(index + 1)
                            .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                            .clone(),
                        name,
                    }
                }
            }
            Some(Value::Symbol(symbol)) if symbol == "append" => LoopAction::Append(
                items
                    .get(index + 1)
                    .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                    .clone(),
            ),
            Some(Value::Symbol(symbol)) if symbol == "vconcat" => {
                if matches!(items.get(index + 2), Some(Value::Symbol(kind)) if kind == "into")
                    && matches!(items.get(index + 4), Some(Value::Symbol(kind)) if kind == "append")
                    && matches!(items.get(index + 6), Some(Value::Symbol(kind)) if kind == "into")
                    && matches!(items.get(index + 8), Some(Value::Symbol(kind)) if kind == "finally")
                    && matches!(items.get(index + 9), Some(Value::Symbol(kind)) if kind == "return")
                {
                    final_return = Some(
                        items
                            .get(index + 10)
                            .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                            .clone(),
                    );
                    LoopAction::VconcatIntoAppendInto {
                        vconcat_expr: items
                            .get(index + 1)
                            .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                            .clone(),
                        vconcat_name: items
                            .get(index + 3)
                            .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                            .as_symbol()?
                            .to_string(),
                        append_expr: items
                            .get(index + 5)
                            .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                            .clone(),
                        append_name: items
                            .get(index + 7)
                            .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                            .as_symbol()?
                            .to_string(),
                    }
                } else {
                    LoopAction::Vconcat(
                        items
                            .get(index + 1)
                            .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                            .clone(),
                    )
                }
            }
            Some(Value::Symbol(symbol)) if symbol == "thereis" => LoopAction::Thereis {
                expr: items
                    .get(index + 1)
                    .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                    .clone(),
                until: if matches!(items.get(index + 2), Some(Value::Symbol(kind)) if kind == "until")
                {
                    Some(
                        items
                            .get(index + 3)
                            .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                            .clone(),
                    )
                } else {
                    None
                },
            },
            Some(Value::Symbol(symbol)) if symbol == "always" => LoopAction::Always(
                items
                    .get(index + 1)
                    .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                    .clone(),
            ),
            Some(Value::Symbol(symbol)) if symbol == "sum" => LoopAction::Sum(
                items
                    .get(index + 1)
                    .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                    .clone(),
            ),
            Some(Value::Symbol(symbol)) if symbol == "return" => LoopAction::Return(
                items
                    .get(index + 1)
                    .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                    .clone(),
            ),
            Some(Value::Symbol(symbol)) if symbol == "unless" => match items.get(index + 2) {
                Some(Value::Symbol(kind)) if kind == "count" => {
                    if !matches!(items.get(index + 4), Some(Value::Symbol(kind)) if kind == "into")
                        || !matches!(items.get(index + 6), Some(Value::Symbol(kind)) if kind == "else")
                        || !matches!(items.get(index + 7), Some(Value::Symbol(kind)) if kind == "count")
                        || !matches!(items.get(index + 9), Some(Value::Symbol(kind)) if kind == "into")
                        || !matches!(items.get(index + 11), Some(Value::Symbol(kind)) if kind == "finally")
                    {
                        return Err(LispError::Signal("Unsupported cl-loop syntax".into()));
                    }
                    final_return = Some(
                        items
                            .get(index + 12)
                            .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                            .clone(),
                    );
                    LoopAction::UnlessCountElseCount {
                        condition: items
                            .get(index + 1)
                            .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                            .clone(),
                        unless_name: items
                            .get(index + 5)
                            .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                            .as_symbol()?
                            .to_string(),
                        else_name: items
                            .get(index + 10)
                            .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                            .as_symbol()?
                            .to_string(),
                    }
                }
                Some(Value::Symbol(kind)) if kind == "collect" => LoopAction::UnlessCollect {
                    condition: items
                        .get(index + 1)
                        .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                        .clone(),
                    expr: items
                        .get(index + 3)
                        .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                        .clone(),
                },
                Some(Value::Symbol(kind)) if kind == "do" => {
                    let condition = items
                        .get(index + 1)
                        .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                        .clone();
                    let body = {
                        let body_start = index + 3;
                        let finally_index = items[body_start..]
                            .iter()
                            .position(
                                |item| matches!(item, Value::Symbol(symbol) if symbol == "finally"),
                            )
                            .map(|offset| body_start + offset);
                        if let Some(finally_index) = finally_index {
                            final_return = if matches!(
                                items.get(finally_index + 1),
                                Some(Value::Symbol(kind)) if kind == "return"
                            ) {
                                Some(
                                    items
                                        .get(finally_index + 2)
                                        .ok_or_else(|| {
                                            LispError::Signal("Unsupported cl-loop syntax".into())
                                        })?
                                        .clone(),
                                )
                            } else if items.get(finally_index + 1).is_some() {
                                Some(Value::list(
                                    std::iter::once(Value::symbol("progn"))
                                        .chain(items[finally_index + 1..].iter().cloned())
                                        .collect::<Vec<_>>(),
                                ))
                            } else {
                                return Err(LispError::Signal("Unsupported cl-loop syntax".into()));
                            };
                            items[body_start..finally_index].to_vec()
                        } else {
                            items[body_start..].to_vec()
                        }
                    };
                    let split_index = body
                        .iter()
                        .position(|item| matches!(item, Value::Symbol(symbol) if symbol == "do"));
                    let (body, after_body) = if let Some(split_index) = split_index {
                        (
                            body[..split_index].to_vec(),
                            body[split_index + 1..].to_vec(),
                        )
                    } else {
                        (body, Vec::new())
                    };
                    LoopAction::UnlessDo {
                        condition,
                        body,
                        after_body,
                    }
                }
                _ => return Err(LispError::Signal("Unsupported cl-loop syntax".into())),
            },
            Some(Value::Symbol(symbol)) if symbol == "if" => match items.get(index + 2) {
                Some(Value::Symbol(kind)) if kind == "collect" => {
                    if matches!(items.get(index + 4), Some(Value::Symbol(kind)) if kind == "into") {
                        if !matches!(items.get(index + 6), Some(Value::Symbol(kind)) if kind == "else")
                            || !matches!(items.get(index + 7), Some(Value::Symbol(kind)) if kind == "collect")
                            || !matches!(items.get(index + 9), Some(Value::Symbol(kind)) if kind == "into")
                            || !matches!(items.get(index + 11), Some(Value::Symbol(kind)) if kind == "finally")
                            || !matches!(items.get(index + 12), Some(Value::Symbol(kind)) if kind == "return")
                        {
                            return Err(LispError::Signal("Unsupported cl-loop syntax".into()));
                        }
                        final_return = Some(
                            items
                                .get(index + 13)
                                .ok_or_else(|| {
                                    LispError::Signal("Unsupported cl-loop syntax".into())
                                })?
                                .clone(),
                        );
                        LoopAction::IfCollectIntoElseCollectInto {
                            condition: items
                                .get(index + 1)
                                .ok_or_else(|| {
                                    LispError::Signal("Unsupported cl-loop syntax".into())
                                })?
                                .clone(),
                            collect: items
                                .get(index + 3)
                                .ok_or_else(|| {
                                    LispError::Signal("Unsupported cl-loop syntax".into())
                                })?
                                .clone(),
                            collect_name: items
                                .get(index + 5)
                                .ok_or_else(|| {
                                    LispError::Signal("Unsupported cl-loop syntax".into())
                                })?
                                .as_symbol()?
                                .to_string(),
                            else_collect: items
                                .get(index + 8)
                                .ok_or_else(|| {
                                    LispError::Signal("Unsupported cl-loop syntax".into())
                                })?
                                .clone(),
                            else_collect_name: items
                                .get(index + 10)
                                .ok_or_else(|| {
                                    LispError::Signal("Unsupported cl-loop syntax".into())
                                })?
                                .as_symbol()?
                                .to_string(),
                        }
                    } else if !matches!(items.get(index + 4), Some(Value::Symbol(kind)) if kind == "else")
                    {
                        return Err(LispError::Signal("Unsupported cl-loop syntax".into()));
                    } else {
                        match items.get(index + 5) {
                            Some(Value::Symbol(kind)) if kind == "append" => {
                                LoopAction::IfCollectElseAppend {
                                    condition: items
                                        .get(index + 1)
                                        .ok_or_else(|| {
                                            LispError::Signal("Unsupported cl-loop syntax".into())
                                        })?
                                        .clone(),
                                    collect: items
                                        .get(index + 3)
                                        .ok_or_else(|| {
                                            LispError::Signal("Unsupported cl-loop syntax".into())
                                        })?
                                        .clone(),
                                    append: items
                                        .get(index + 6)
                                        .ok_or_else(|| {
                                            LispError::Signal("Unsupported cl-loop syntax".into())
                                        })?
                                        .clone(),
                                }
                            }
                            Some(Value::Symbol(kind)) if kind == "collect" => {
                                LoopAction::IfCollectElseCollect {
                                    condition: items
                                        .get(index + 1)
                                        .ok_or_else(|| {
                                            LispError::Signal("Unsupported cl-loop syntax".into())
                                        })?
                                        .clone(),
                                    collect: items
                                        .get(index + 3)
                                        .ok_or_else(|| {
                                            LispError::Signal("Unsupported cl-loop syntax".into())
                                        })?
                                        .clone(),
                                    else_collect: items
                                        .get(index + 6)
                                        .ok_or_else(|| {
                                            LispError::Signal("Unsupported cl-loop syntax".into())
                                        })?
                                        .clone(),
                                }
                            }
                            _ => {
                                return Err(LispError::Signal("Unsupported cl-loop syntax".into()));
                            }
                        }
                    }
                }
                Some(Value::Symbol(kind)) if kind == "do" => {
                    if matches!(items.get(index + 4), Some(Value::Symbol(kind)) if kind == "append")
                    {
                        LoopAction::IfDoAppend {
                            condition: items
                                .get(index + 1)
                                .ok_or_else(|| {
                                    LispError::Signal("Unsupported cl-loop syntax".into())
                                })?
                                .clone(),
                            body: items
                                .get(index + 3)
                                .ok_or_else(|| {
                                    LispError::Signal("Unsupported cl-loop syntax".into())
                                })?
                                .clone(),
                            append: items
                                .get(index + 5)
                                .ok_or_else(|| {
                                    LispError::Signal("Unsupported cl-loop syntax".into())
                                })?
                                .clone(),
                        }
                    } else {
                        let then_start = index + 3;
                        let else_index = items[then_start..]
                            .iter()
                            .position(
                                |item| matches!(item, Value::Symbol(symbol) if symbol == "else"),
                            )
                            .map(|offset| then_start + offset)
                            .ok_or_else(|| {
                                LispError::Signal("Unsupported cl-loop syntax".into())
                            })?;
                        if !matches!(items.get(else_index + 1), Some(Value::Symbol(kind)) if kind == "do")
                        {
                            return Err(LispError::Signal("Unsupported cl-loop syntax".into()));
                        }
                        let else_start = else_index + 2;
                        let finally_index = items[else_start..]
                            .iter()
                            .position(
                                |item| matches!(item, Value::Symbol(symbol) if symbol == "finally"),
                            )
                            .map(|offset| else_start + offset);
                        let else_end = if let Some(finally_index) = finally_index {
                            if !matches!(items.get(finally_index + 1), Some(Value::Symbol(kind)) if kind == "return")
                            {
                                return Err(LispError::Signal("Unsupported cl-loop syntax".into()));
                            }
                            final_return = Some(
                                items
                                    .get(finally_index + 2)
                                    .ok_or_else(|| {
                                        LispError::Signal("Unsupported cl-loop syntax".into())
                                    })?
                                    .clone(),
                            );
                            finally_index
                        } else {
                            items.len()
                        };
                        LoopAction::IfDoElseDo {
                            condition: items
                                .get(index + 1)
                                .ok_or_else(|| {
                                    LispError::Signal("Unsupported cl-loop syntax".into())
                                })?
                                .clone(),
                            then_body: items[then_start..else_index].to_vec(),
                            else_body: items[else_start..else_end].to_vec(),
                        }
                    }
                }
                _ => return Err(LispError::Signal("Unsupported cl-loop syntax".into())),
            },
            _ => return Err(LispError::Signal("Unsupported cl-loop syntax".into())),
        };
        {
            let frame = env.last_mut().expect("env frame just pushed");
            match &action {
                LoopAction::WhenCollectInto { name, .. } | LoopAction::CollectInto { name, .. } => {
                    Self::upsert_frame_binding(frame, name.clone(), Value::Nil);
                }
                LoopAction::UnlessCountElseCount {
                    unless_name,
                    else_name,
                    ..
                } => {
                    Self::upsert_frame_binding(frame, unless_name.clone(), Value::Integer(0));
                    Self::upsert_frame_binding(frame, else_name.clone(), Value::Integer(0));
                }
                LoopAction::NestedWhenCollectInto(action) => {
                    for name in [
                        &action.then_name,
                        &action.then_else_name,
                        &action.else_name,
                        &action.else_else_name,
                    ] {
                        Self::upsert_frame_binding(frame, name.clone(), Value::Nil);
                    }
                }
                LoopAction::WhenCollectIntoWhenDoWhenDo { collect_name, .. } => {
                    Self::upsert_frame_binding(frame, collect_name.clone(), Value::Nil);
                }
                LoopAction::IfCollectIntoElseCollectInto {
                    collect_name,
                    else_collect_name,
                    ..
                } => {
                    Self::upsert_frame_binding(frame, collect_name.clone(), Value::Nil);
                    Self::upsert_frame_binding(frame, else_collect_name.clone(), Value::Nil);
                }
                LoopAction::VconcatIntoAppendInto {
                    vconcat_name,
                    append_name,
                    ..
                } => {
                    Self::upsert_frame_binding(
                        frame,
                        vconcat_name.clone(),
                        Value::list([Value::Symbol("vector-literal".into())]),
                    );
                    Self::upsert_frame_binding(frame, append_name.clone(), Value::Nil);
                }
                _ => {}
            }
        }

        let mut result = Value::Nil;
        let mut returned_early = false;
        let mut collected = Vec::new();
        let mut vconcat_into_collected = Vec::new();
        let mut append_into_collected = Vec::new();
        let mut sum = 0i64;
        if !initially_body.is_empty() {
            match self.eval_cl_loop_do_body(&initially_body, env) {
                Ok(value) => result = value,
                Err(LispError::Throw(tag, value))
                    if tag == Value::Symbol("--cl-block-nil--".into()) =>
                {
                    result = value;
                    returned_early = true;
                }
                Err(error) => return Err(error),
            }
        }
        for iteration in 0..iterations {
            if returned_early {
                break;
            }
            {
                let frame = env.last_mut().expect("env frame just pushed");
                for spec in &specs {
                    match spec {
                        LoopSpec::Range { name, values } => {
                            Self::upsert_frame_binding(
                                frame,
                                name.clone(),
                                Self::stored_value(values[iteration].clone()),
                            );
                        }
                        LoopSpec::List { pattern, values } => {
                            self.bind_cl_pattern(pattern, values[iteration].clone(), frame)?;
                        }
                        LoopSpec::From { name, start } => {
                            Self::upsert_frame_binding(
                                frame,
                                name.clone(),
                                Self::stored_value(Value::Integer(*start + iteration as i64)),
                            );
                        }
                        LoopSpec::Assign { .. } | LoopSpec::AssignThen { .. } => {}
                        LoopSpec::Repeat { .. } => {}
                        LoopSpec::ListExpr { .. } => unreachable!("list expressions are resolved"),
                    }
                }
            }

            let guard_split = guard_assign_position.unwrap_or(0);
            let mut assign_position = 0usize;
            for spec in &specs {
                match spec {
                    LoopSpec::Assign { name, expr } => {
                        if assign_position < guard_split {
                            let value = Self::stored_value(self.eval(expr, env)?);
                            let frame = env.last_mut().expect("env frame just pushed");
                            Self::upsert_frame_binding(frame, name.clone(), value);
                        }
                        assign_position += 1;
                    }
                    LoopSpec::AssignThen { name, init, step } => {
                        if assign_position < guard_split {
                            let form = if iteration == 0 { init } else { step };
                            let value = Self::stored_value(self.eval(form, env)?);
                            let frame = env.last_mut().expect("env frame just pushed");
                            Self::upsert_frame_binding(frame, name.clone(), value);
                        }
                        assign_position += 1;
                    }
                    _ => {}
                }
            }

            if let Some(expr) = while_expr.as_ref()
                && !self.eval(expr, env)?.is_truthy()
            {
                break;
            }
            if let Some(expr) = until_expr.as_ref()
                && self.eval(expr, env)?.is_truthy()
            {
                break;
            }

            assign_position = 0;
            for spec in &specs {
                match spec {
                    LoopSpec::Assign { name, expr } => {
                        if assign_position >= guard_split {
                            let value = Self::stored_value(self.eval(expr, env)?);
                            let frame = env.last_mut().expect("env frame just pushed");
                            Self::upsert_frame_binding(frame, name.clone(), value);
                        }
                        assign_position += 1;
                    }
                    LoopSpec::AssignThen { name, init, step } => {
                        if assign_position >= guard_split {
                            let form = if iteration == 0 { init } else { step };
                            let value = Self::stored_value(self.eval(form, env)?);
                            let frame = env.last_mut().expect("env frame just pushed");
                            Self::upsert_frame_binding(frame, name.clone(), value);
                        }
                        assign_position += 1;
                    }
                    _ => {}
                }
            }

            match &action {
                LoopAction::Do(body) => result = self.eval_cl_loop_do_body(body, env)?,
                LoopAction::DoCollect { body, collect } => {
                    result = self.eval_cl_loop_do_body(body, env)?;
                    collected.push(self.eval(collect, env)?);
                }
                LoopAction::Collect(expr) => collected.push(self.eval(expr, env)?),
                LoopAction::CollectDo { expr, body } => {
                    collected.push(self.eval(expr, env)?);
                    result = self.eval_cl_loop_do_body(body, env)?;
                }
                LoopAction::Append(expr) => {
                    let values = self.eval(expr, env)?.to_vec()?;
                    collected.extend(values);
                }
                LoopAction::Vconcat(expr) => {
                    collected.extend(crate::lisp::primitives::vector_items(
                        &self.eval(expr, env)?,
                    )?);
                }
                LoopAction::VconcatIntoAppendInto {
                    vconcat_expr,
                    vconcat_name,
                    append_expr,
                    append_name,
                } => {
                    vconcat_into_collected.extend(crate::lisp::primitives::vector_items(
                        &self.eval(vconcat_expr, env)?,
                    )?);
                    append_into_collected.extend(self.eval(append_expr, env)?.to_vec()?);
                    let frame = env.last_mut().expect("env frame just pushed");
                    Self::upsert_frame_binding(
                        frame,
                        vconcat_name.clone(),
                        Value::list(
                            std::iter::once(Value::Symbol("vector-literal".into()))
                                .chain(vconcat_into_collected.iter().cloned()),
                        ),
                    );
                    Self::upsert_frame_binding(
                        frame,
                        append_name.clone(),
                        Value::list(append_into_collected.clone()),
                    );
                }
                LoopAction::Thereis { expr, until } => {
                    if let Some(until_expr) = until
                        && self.eval(until_expr, env)?.is_truthy()
                    {
                        result = Value::Nil;
                        break;
                    }
                    let value = self.eval(expr, env)?;
                    if value.is_truthy() {
                        result = value;
                        break;
                    }
                }
                LoopAction::Always(expr) => {
                    if !self.eval(expr, env)?.is_truthy() {
                        result = Value::Nil;
                        env.pop();
                        return Ok(result);
                    }
                    result = Value::T;
                }
                LoopAction::Sum(expr) => {
                    sum += self.eval(expr, env)?.as_integer()?;
                    result = Value::Integer(sum);
                }
                LoopAction::Return(expr) => {
                    result = self.eval(expr, env)?;
                    returned_early = true;
                    break;
                }
                LoopAction::WhenReturn { condition, expr } => {
                    let condition_value = self.eval(condition, env)?;
                    if condition_value.is_truthy() {
                        let frame = env.last_mut().expect("env frame just pushed");
                        Self::upsert_frame_binding(
                            frame,
                            "it".into(),
                            Self::stored_value(condition_value),
                        );
                        result = self.eval(expr, env)?;
                        returned_early = true;
                        break;
                    }
                }
                LoopAction::WhenCollect { condition, expr } => {
                    let condition_value = self.eval(condition, env)?;
                    if condition_value.is_truthy() {
                        let frame = env.last_mut().expect("env frame just pushed");
                        Self::upsert_frame_binding(
                            frame,
                            "it".into(),
                            Self::stored_value(condition_value),
                        );
                        collected.push(self.eval(expr, env)?);
                    }
                }
                LoopAction::WhenAppend { condition, expr } => {
                    let condition_value = self.eval(condition, env)?;
                    if condition_value.is_truthy() {
                        let frame = env.last_mut().expect("env frame just pushed");
                        Self::upsert_frame_binding(
                            frame,
                            "it".into(),
                            Self::stored_value(condition_value),
                        );
                        collected.extend(self.eval(expr, env)?.to_vec()?);
                    }
                }
                LoopAction::WhenDo { condition, body } => {
                    let condition_value = self.eval(condition, env)?;
                    if condition_value.is_truthy() {
                        let frame = env.last_mut().expect("env frame just pushed");
                        Self::upsert_frame_binding(
                            frame,
                            "it".into(),
                            Self::stored_value(condition_value),
                        );
                        match self.eval_cl_loop_do_body(body, env) {
                            Ok(value) => result = value,
                            Err(LispError::Throw(tag, value))
                                if named_loop_tag
                                    .as_ref()
                                    .is_some_and(|expected| *expected == tag) =>
                            {
                                result = value;
                                returned_early = true;
                                break;
                            }
                            Err(error) => return Err(error),
                        }
                    }
                }
                LoopAction::CollectInto { expr, name } => {
                    collected.push(self.eval(expr, env)?);
                    let frame = env.last_mut().expect("env frame just pushed");
                    Self::upsert_frame_binding(frame, name.clone(), Value::list(collected.clone()));
                }
                LoopAction::WhenCollectInto {
                    condition,
                    expr,
                    name,
                } => {
                    let condition_value = self.eval(condition, env)?;
                    if condition_value.is_truthy() {
                        let frame = env.last_mut().expect("env frame just pushed");
                        Self::upsert_frame_binding(
                            frame,
                            "it".into(),
                            Self::stored_value(condition_value),
                        );
                        collected.push(self.eval(expr, env)?);
                        let frame = env.last_mut().expect("env frame just pushed");
                        Self::upsert_frame_binding(
                            frame,
                            name.clone(),
                            Value::list(collected.clone()),
                        );
                    }
                }
                LoopAction::NestedWhenCollectInto(action) => {
                    let (target_expr, target_name) = {
                        let condition_value = self.eval(&action.condition, env)?;
                        if condition_value.is_truthy() {
                            let frame = env.last_mut().expect("env frame just pushed");
                            Self::upsert_frame_binding(
                                frame,
                                "it".into(),
                                Self::stored_value(condition_value),
                            );
                            let nested_condition = self.eval(&action.then_condition, env)?;
                            if nested_condition.is_truthy() {
                                let frame = env.last_mut().expect("env frame just pushed");
                                Self::upsert_frame_binding(
                                    frame,
                                    "it".into(),
                                    Self::stored_value(nested_condition),
                                );
                                (&action.then_expr, &action.then_name)
                            } else {
                                (&action.then_else_expr, &action.then_else_name)
                            }
                        } else {
                            let nested_condition = self.eval(&action.else_condition, env)?;
                            if nested_condition.is_truthy() {
                                let frame = env.last_mut().expect("env frame just pushed");
                                Self::upsert_frame_binding(
                                    frame,
                                    "it".into(),
                                    Self::stored_value(nested_condition),
                                );
                                (&action.else_expr, &action.else_name)
                            } else {
                                (&action.else_else_expr, &action.else_else_name)
                            }
                        }
                    };
                    let value = self.eval(target_expr, env)?;
                    let current = self.lookup(target_name, env).unwrap_or(Value::Nil);
                    let mut values = current.to_vec().unwrap_or_default();
                    values.push(value);
                    let frame = env.last_mut().expect("env frame just pushed");
                    Self::upsert_frame_binding(frame, target_name.clone(), Value::list(values));
                }
                LoopAction::WhenCollectIntoWhenDoWhenDo {
                    collect_condition,
                    collect_expr,
                    collect_name,
                    first_do_condition,
                    first_do_body,
                    second_do_condition,
                    second_do_body,
                } => {
                    if self.eval(collect_condition, env)?.is_truthy() {
                        let value = self.eval(collect_expr, env)?;
                        let current = self.lookup(collect_name, env).unwrap_or(Value::Nil);
                        let mut values = current.to_vec().unwrap_or_default();
                        values.push(value);
                        let frame = env.last_mut().expect("env frame just pushed");
                        Self::upsert_frame_binding(
                            frame,
                            collect_name.clone(),
                            Value::list(values),
                        );
                    }
                    if self.eval(first_do_condition, env)?.is_truthy() {
                        match self.eval_cl_loop_do_body(first_do_body, env) {
                            Ok(value) => result = value,
                            Err(LispError::Throw(tag, value))
                                if tag == Value::Symbol("--cl-block-nil--".into()) =>
                            {
                                result = value;
                                returned_early = true;
                                break;
                            }
                            Err(error) => return Err(error),
                        }
                    }
                    if self.eval(second_do_condition, env)?.is_truthy() {
                        match self.eval_cl_loop_do_body(second_do_body, env) {
                            Ok(value) => result = value,
                            Err(LispError::Throw(tag, value))
                                if tag == Value::Symbol("--cl-block-nil--".into()) =>
                            {
                                result = value;
                                returned_early = true;
                                break;
                            }
                            Err(error) => return Err(error),
                        }
                    }
                }
                LoopAction::UnlessCollect { condition, expr } => {
                    if !self.eval(condition, env)?.is_truthy() {
                        collected.push(self.eval(expr, env)?);
                    }
                }
                LoopAction::UnlessCountElseCount {
                    condition,
                    unless_name,
                    else_name,
                } => {
                    let target = if self.eval(condition, env)?.is_truthy() {
                        else_name
                    } else {
                        unless_name
                    };
                    let current = self
                        .lookup(target, env)
                        .unwrap_or(Value::Integer(0))
                        .as_integer()?;
                    let frame = env.last_mut().expect("env frame just pushed");
                    Self::upsert_frame_binding(frame, target.clone(), Value::Integer(current + 1));
                }
                LoopAction::UnlessDo {
                    condition,
                    body,
                    after_body,
                } => {
                    if !self.eval(condition, env)?.is_truthy() {
                        result = self.eval_cl_loop_do_body(body, env)?;
                    }
                    if !after_body.is_empty() {
                        result = self.eval_cl_loop_do_body(after_body, env)?;
                    }
                }
                LoopAction::IfDoAppend {
                    condition,
                    body,
                    append,
                } => {
                    if self.eval(condition, env)?.is_truthy() {
                        result = self.eval(body, env)?;
                        collected.extend(self.eval(append, env)?.to_vec()?);
                    }
                }
                LoopAction::IfDoElseDo {
                    condition,
                    then_body,
                    else_body,
                } => {
                    result = if self.eval(condition, env)?.is_truthy() {
                        self.eval_cl_loop_do_body(then_body, env)?
                    } else {
                        self.eval_cl_loop_do_body(else_body, env)?
                    };
                }
                LoopAction::IfCollectElseCollect {
                    condition,
                    collect,
                    else_collect,
                } => {
                    let expr = if self.eval(condition, env)?.is_truthy() {
                        collect
                    } else {
                        else_collect
                    };
                    collected.push(self.eval(expr, env)?);
                }
                LoopAction::IfCollectIntoElseCollectInto {
                    condition,
                    collect,
                    collect_name,
                    else_collect,
                    else_collect_name,
                } => {
                    let (target_expr, target_name) = if self.eval(condition, env)?.is_truthy() {
                        (collect, collect_name)
                    } else {
                        (else_collect, else_collect_name)
                    };
                    let value = self.eval(target_expr, env)?;
                    let current = self.lookup(target_name, env).unwrap_or(Value::Nil);
                    let mut values = current.to_vec().unwrap_or_default();
                    values.push(value);
                    let frame = env.last_mut().expect("env frame just pushed");
                    Self::upsert_frame_binding(frame, target_name.clone(), Value::list(values));
                }
                LoopAction::IfCollectElseAppend {
                    condition,
                    collect,
                    append,
                } => {
                    if self.eval(condition, env)?.is_truthy() {
                        collected.push(self.eval(collect, env)?);
                    } else {
                        collected.extend(self.eval(append, env)?.to_vec()?);
                    }
                }
            }
        }

        if let Some(expr) = final_return.as_ref()
            && !returned_early
        {
            result = self.eval_cl_loop_final_return(expr, env)?;
        }

        env.pop();
        if final_return.is_some() {
            return Ok(result);
        }
        Ok(match action {
            LoopAction::Collect(_) | LoopAction::Append(_) | LoopAction::DoCollect { .. } => {
                Value::list(collected)
            }
            LoopAction::CollectDo { .. } => Value::list(collected),
            LoopAction::Vconcat(_) => Value::list(
                std::iter::once(Value::symbol("vector-literal"))
                    .chain(collected)
                    .collect::<Vec<_>>(),
            ),
            LoopAction::WhenCollect { .. } | LoopAction::WhenAppend { .. } => {
                Value::list(collected)
            }
            LoopAction::IfDoAppend { .. }
            | LoopAction::IfCollectElseCollect { .. }
            | LoopAction::IfCollectIntoElseCollectInto { .. }
            | LoopAction::IfCollectElseAppend { .. } => Value::list(collected),
            LoopAction::Always(_) if result.is_nil() => Value::Nil,
            LoopAction::Always(_) => Value::T,
            LoopAction::Sum(_) => Value::Integer(sum),
            LoopAction::UnlessCollect { .. } => Value::list(collected),
            _ => result,
        })
    }

    fn eval_cl_loop_final_return(
        &mut self,
        expr: &Value,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if let Ok(items) = expr.to_vec()
            && matches!(items.first(), Some(Value::Symbol(symbol)) if symbol == "cl-return")
        {
            return match items.as_slice() {
                [_, value] => self.eval(value, env),
                [_] => Ok(Value::Nil),
                _ => Err(LispError::Signal("Unsupported cl-loop syntax".into())),
            };
        }
        match self.eval(expr, env) {
            Err(LispError::Throw(tag, value))
                if tag == Value::Symbol("--cl-block-nil--".into()) =>
            {
                Ok(value)
            }
            other => other,
        }
    }

    pub(super) fn eval_resolved_setf_place_current_value(
        &mut self,
        place: &Value,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        match &place {
            Value::Symbol(name) => self.lookup(name, env),
            Value::Cons(_, _) => {
                let items = place.to_vec()?;
                if matches!(items.first(), Some(Value::Symbol(name)) if name == "--emaxx-setf-car-place" || name == "--emaxx-setf-cdr-place")
                {
                    let Some(target) = items.get(1) else {
                        return Err(LispError::Signal("Unsupported setf place".into()));
                    };
                    if matches!(items.first(), Some(Value::Symbol(name)) if name == "--emaxx-setf-car-place")
                    {
                        target.car()
                    } else {
                        target.cdr()
                    }
                } else if matches!(
                    items.first(),
                    Some(Value::Symbol(name)) if name == "--emaxx-setf-decoded-time-place"
                ) {
                    let Some(accessor) = items.get(1).and_then(|value| value.as_symbol().ok())
                    else {
                        return Err(LispError::Signal("Unsupported setf place".into()));
                    };
                    let Some(index) = decoded_time_accessor_index(accessor) else {
                        return Err(LispError::Signal("Unsupported setf place".into()));
                    };
                    let Some(target) = items.get(2) else {
                        return Err(LispError::Signal("Unsupported setf place".into()));
                    };
                    decoded_time_accessor_value(index, target)
                } else if matches!(
                    items.first(),
                    Some(Value::Symbol(name)) if name == "--emaxx-setf-gv-synthetic-place"
                ) {
                    items
                        .get(1)
                        .cloned()
                        .ok_or_else(|| LispError::Signal("Unsupported setf place".into()))
                } else {
                    self.eval(place, env)
                }
            }
            _ => self.eval(place, env),
        }
    }

    pub(super) fn resolve_setf_place(
        &mut self,
        place: &Value,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let Value::Cons(_, _) = place else {
            return Ok(place.clone());
        };
        let items = place.to_vec()?;
        if let Some(Value::Symbol(name)) = items.first()
            && let Some(expanded) = self.try_macroexpand(name, &items[1..], env)?
            && expanded != *place
        {
            return self.resolve_setf_place(&expanded, env);
        }
        match items.first() {
            Some(Value::Symbol(name)) if name == "edebug-after" && items.len() == 4 => {
                // Edebug's gv-expander notifies the stepper through the
                // instrumented getter, then operates on the raw place.
                self.eval(place, env)?;
                self.resolve_setf_place(&items[3], env)
            }
            Some(Value::Symbol(name)) if name == "cond" => {
                for clause in &items[1..] {
                    let forms = clause.to_vec()?;
                    if forms.is_empty() {
                        continue;
                    }
                    if self.eval(&forms[0], env)?.is_truthy() {
                        if forms.len() > 2 {
                            self.sf_progn(&forms[1..forms.len() - 1], env)?;
                        }
                        return self.resolve_setf_place(forms.last().unwrap_or(&Value::Nil), env);
                    }
                }
                Ok(Value::Nil)
            }
            Some(Value::Symbol(name)) if name == "if" => {
                let Some(condition) = items.get(1) else {
                    return Ok(Value::Nil);
                };
                if self.eval(condition, env)?.is_truthy() {
                    self.resolve_setf_place(items.get(2).unwrap_or(&Value::Nil), env)
                } else {
                    self.resolve_setf_place(items.get(3).unwrap_or(&Value::Nil), env)
                }
            }
            Some(Value::Symbol(name)) if name == "progn" => {
                if items.len() > 2 {
                    self.sf_progn(&items[1..items.len() - 1], env)?;
                }
                self.resolve_setf_place(items.last().unwrap_or(&Value::Nil), env)
            }
            Some(Value::Symbol(name)) if name == "gv-synthetic-place" => {
                let Some(getter_expr) = items.get(1) else {
                    return Ok(place.clone());
                };
                let Some(setter_expr) = items.get(2) else {
                    return Ok(place.clone());
                };
                let getter = self.eval(getter_expr, env)?;
                let setter = self.eval(setter_expr, env)?;
                Ok(Value::list([
                    Value::Symbol("--emaxx-setf-gv-synthetic-place".into()),
                    getter,
                    setter,
                ]))
            }
            Some(Value::Symbol(name)) if name == "symbol-value" => {
                let Some(symbol_form) = items.get(1) else {
                    return Ok(place.clone());
                };
                let symbol = self.eval(symbol_form, env)?;
                Ok(Value::list([
                    Value::Symbol("symbol-value".into()),
                    quoted_literal(&symbol),
                ]))
            }
            Some(Value::Symbol(name))
                if matches!(name.as_str(), "car" | "cdr")
                    || decoded_time_accessor_index(name).is_some()
                    || self
                        .get_symbol_property(name, "emaxx-struct-slot")
                        .is_some()
                    || self.get_symbol_property(name, "emaxx-gv-setter").is_some() =>
            {
                let Some(target_expr) = items.get(1) else {
                    return Ok(place.clone());
                };
                let target = self.eval(target_expr, env)?;
                if matches!(name.as_str(), "car" | "cdr") {
                    return Ok(Value::list([
                        Value::Symbol(format!("--emaxx-setf-{name}-place")),
                        target,
                    ]));
                }
                if decoded_time_accessor_index(name).is_some() {
                    return Ok(Value::list([
                        Value::Symbol("--emaxx-setf-decoded-time-place".into()),
                        Value::Symbol(name.clone()),
                        target,
                    ]));
                }
                if self.get_symbol_property(name, "emaxx-gv-setter").is_some() {
                    let mut resolved = Vec::with_capacity(items.len());
                    resolved.push(Value::Symbol(name.clone()));
                    resolved.push(quoted_literal(&target));
                    for arg in &items[2..] {
                        let evaluated = self.eval(arg, env)?;
                        resolved.push(quoted_literal(&evaluated));
                    }
                    return Ok(Value::list(resolved));
                }
                Ok(Value::list([
                    Value::Symbol(name.clone()),
                    quoted_literal(&target),
                ]))
            }
            Some(Value::Symbol(name)) if name == "overlay-get" => {
                let Some(overlay_expr) = items.get(1) else {
                    return Ok(place.clone());
                };
                let Some(prop_expr) = items.get(2) else {
                    return Ok(place.clone());
                };
                let overlay = self.eval(overlay_expr, env)?;
                let property = self.eval(prop_expr, env)?;
                Ok(Value::list([
                    Value::Symbol("overlay-get".into()),
                    quoted_literal(&overlay),
                    quoted_literal(&property),
                ]))
            }
            Some(Value::Symbol(name)) if name == "get" => {
                let Some(symbol_expr) = items.get(1) else {
                    return Ok(place.clone());
                };
                let Some(prop_expr) = items.get(2) else {
                    return Ok(place.clone());
                };
                let symbol = self.eval(symbol_expr, env)?;
                let property = self.eval(prop_expr, env)?;
                Ok(Value::list([
                    Value::Symbol("get".into()),
                    quoted_literal(&symbol),
                    quoted_literal(&property),
                ]))
            }
            Some(Value::Symbol(name)) if name == "terminal-parameter" => {
                let Some(terminal_expr) = items.get(1) else {
                    return Ok(place.clone());
                };
                let Some(parameter_expr) = items.get(2) else {
                    return Ok(place.clone());
                };
                let terminal = self.eval(terminal_expr, env)?;
                let parameter = self.eval(parameter_expr, env)?;
                Ok(Value::list([
                    Value::Symbol("terminal-parameter".into()),
                    quoted_literal(&terminal),
                    quoted_literal(&parameter),
                ]))
            }
            Some(Value::Symbol(name)) if name == "alist-get" => {
                let Some(key_expr) = items.get(1) else {
                    return Ok(place.clone());
                };
                let Some(alist_place) = items.get(2) else {
                    return Ok(place.clone());
                };
                let key = self.eval(key_expr, env)?;
                let mut resolved = vec![
                    Value::Symbol("alist-get".into()),
                    quoted_literal(&key),
                    self.resolve_setf_place(alist_place, env)?,
                ];
                if let Some(default_expr) = items.get(3) {
                    let default = self.eval(default_expr, env)?;
                    resolved.push(quoted_literal(&default));
                }
                if let Some(remove_expr) = items.get(4) {
                    let remove = self.eval(remove_expr, env)?;
                    resolved.push(quoted_literal(&remove));
                }
                if let Some(testfn_expr) = items.get(5) {
                    let testfn = self.eval(testfn_expr, env)?;
                    resolved.push(quoted_literal(&testfn));
                }
                Ok(Value::list(resolved))
            }
            Some(Value::Symbol(name)) if matches!(name.as_str(), "plist-get" | "cl-getf") => {
                let Some(plist_place) = items.get(1) else {
                    return Ok(place.clone());
                };
                let Some(key_expr) = items.get(2) else {
                    return Ok(place.clone());
                };
                let key = self.eval(key_expr, env)?;
                let mut resolved = vec![
                    Value::Symbol(name.clone()),
                    self.resolve_setf_place(plist_place, env)?,
                    quoted_literal(&key),
                ];
                if let Some(extra_expr) = items.get(3) {
                    let extra = self.eval(extra_expr, env)?;
                    resolved.push(quoted_literal(&extra));
                }
                Ok(Value::list(resolved))
            }
            Some(Value::Symbol(name)) if name == "aref" || name == "elt" => {
                let Some(sequence_place) = items.get(1) else {
                    return Ok(place.clone());
                };
                let Some(index_expr) = items.get(2) else {
                    return Ok(place.clone());
                };
                let index = self.eval(index_expr, env)?;
                Ok(Value::list([
                    Value::Symbol(name.clone()),
                    self.resolve_setf_place(sequence_place, env)?,
                    quoted_literal(&index),
                ]))
            }
            Some(Value::Symbol(name)) if name == "image-property" => {
                let Some(image_place) = items.get(1) else {
                    return Ok(place.clone());
                };
                let Some(property_expr) = items.get(2) else {
                    return Ok(place.clone());
                };
                let property = self.eval(property_expr, env)?;
                Ok(Value::list([
                    Value::Symbol("image-property".into()),
                    self.resolve_setf_place(image_place, env)?,
                    quoted_literal(&property),
                ]))
            }
            Some(Value::Symbol(name)) if name == "gethash" => {
                let Some(key_expr) = items.get(1) else {
                    return Ok(place.clone());
                };
                let Some(table_expr) = items.get(2) else {
                    return Ok(place.clone());
                };
                let key = self.eval(key_expr, env)?;
                let table = self.eval(table_expr, env)?;
                Ok(Value::list([
                    Value::Symbol("gethash".into()),
                    quoted_literal(&key),
                    quoted_literal(&table),
                ]))
            }
            // Last resort: a `gv-expander' property registered by gv.el's
            // `gv-define-setter' and friends.  Follow `gv-get's protocol:
            // the expander is called with a DO function plus the
            // unevaluated place arguments and returns an expression built
            // around DO's result.  Slot 1 carries the place's current
            // value (DO returns the getter form); slot 2 a function of the
            // new value returning the store FORM (DO hands the setter the
            // quoted value).  Native place handlers above take precedence.
            Some(Value::Symbol(name))
                if self.get_symbol_property(name, "gv-expander").is_some() =>
            {
                let expander = self
                    .get_symbol_property(name, "gv-expander")
                    .expect("checked above");
                let getter_do = Value::Lambda(
                    vec!["--emaxx-gv-getter--".into(), "--emaxx-gv-setter--".into()],
                    vec![Value::Symbol("--emaxx-gv-getter--".into())],
                    shared_env(env.clone()),
                );
                let mut getter_args = vec![getter_do];
                getter_args.extend(items[1..].iter().cloned());
                let getter_form =
                    self.call_function_value(expander.clone(), None, &getter_args, env)?;
                let current = self.eval(&getter_form, env)?;
                let setter_do = Value::list([
                    Value::Symbol("lambda".into()),
                    Value::list([
                        Value::Symbol("--emaxx-gv-getter--".into()),
                        Value::Symbol("--emaxx-gv-setter--".into()),
                    ]),
                    Value::list([
                        Value::Symbol("funcall".into()),
                        Value::Symbol("--emaxx-gv-setter--".into()),
                        Value::list([
                            Value::Symbol("list".into()),
                            Value::list([
                                Value::Symbol("quote".into()),
                                Value::Symbol("quote".into()),
                            ]),
                            Value::Symbol("--emaxx-gv-value--".into()),
                        ]),
                    ]),
                ]);
                let mut funcall = vec![
                    Value::Symbol("funcall".into()),
                    Value::list([Value::Symbol("quote".into()), expander]),
                    setter_do,
                ];
                for arg in &items[1..] {
                    funcall.push(Value::list([Value::Symbol("quote".into()), arg.clone()]));
                }
                let setter = Value::Lambda(
                    vec!["--emaxx-gv-value--".into()],
                    vec![Value::list(funcall)],
                    shared_env(env.clone()),
                );
                Ok(Value::list([
                    Value::Symbol("--emaxx-setf-gv-synthetic-place".into()),
                    current,
                    setter,
                ]))
            }
            _ => Ok(place.clone()),
        }
    }

    pub(super) fn apply_cl_sequence_key(
        &mut self,
        keyfn: Option<&Value>,
        value: &Value,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let Some(keyfn) = keyfn.filter(|value| !value.is_nil()) else {
            return Ok(value.clone());
        };
        let func = match keyfn {
            Value::Symbol(name) => self.lookup_function(name, env)?,
            other => other.clone(),
        };
        self.call_function_value(
            func,
            keyfn.as_symbol().ok(),
            std::slice::from_ref(value),
            env,
        )
    }

    pub(super) fn call_binary_predicate(
        &mut self,
        predicate: &Value,
        left: &Value,
        right: &Value,
        env: &mut Env,
    ) -> Result<bool, LispError> {
        let func = match predicate {
            Value::Symbol(name) => self.lookup_function(name, env)?,
            other => other.clone(),
        };
        Ok(self
            .call_function_value(
                func,
                predicate.as_symbol().ok(),
                &[left.clone(), right.clone()],
                env,
            )?
            .is_truthy())
    }

    pub(super) fn bind_cl_pattern(
        &mut self,
        pattern: &Value,
        value: Value,
        frame: &mut Vec<(String, Value)>,
    ) -> Result<(), LispError> {
        match pattern {
            Value::Symbol(name) if name == "nil" => Ok(()),
            Value::Symbol(name) => {
                Self::upsert_frame_binding(frame, name.clone(), Self::stored_value(value));
                Ok(())
            }
            Value::Cons(_, _) => {
                if let Ok(pattern_items) = pattern.to_vec() {
                    let values = value.to_vec()?;
                    let mut pi = 0usize;
                    let mut vi = 0usize;
                    let mut optional = false;
                    while pi < pattern_items.len() {
                        match &pattern_items[pi] {
                            Value::Symbol(symbol) if symbol == "&optional" => {
                                optional = true;
                                pi += 1;
                                continue;
                            }
                            Value::Symbol(symbol) if symbol == "&rest" => {
                                pi += 1;
                                if let Some(rest_pattern) = pattern_items.get(pi) {
                                    self.bind_cl_pattern(
                                        rest_pattern,
                                        Value::list(values[vi..].to_vec()),
                                        frame,
                                    )?;
                                }
                                break;
                            }
                            subpattern => {
                                let consumed = vi < values.len();
                                let bound_value = if consumed {
                                    values[vi].clone()
                                } else if optional {
                                    Value::Nil
                                } else {
                                    return Err(LispError::WrongNumberOfArgs(
                                        "cl-destructuring-bind".into(),
                                        values.len(),
                                    ));
                                };
                                self.bind_cl_pattern(subpattern, bound_value, frame)?;
                                if consumed {
                                    vi += 1;
                                }
                            }
                        }
                        pi += 1;
                    }
                    Ok(())
                } else {
                    let mut current_pattern = pattern.clone();
                    let mut current_value = value;
                    loop {
                        match current_pattern {
                            Value::Cons(car, cdr) => {
                                let Some((head, tail)) = current_value.cons_values() else {
                                    return Err(LispError::WrongNumberOfArgs(
                                        "cl-destructuring-bind".into(),
                                        0,
                                    ));
                                };
                                self.bind_cl_pattern(&car.borrow().clone(), head, frame)?;
                                current_pattern = cdr.borrow().clone();
                                current_value = tail;
                            }
                            Value::Nil => return Ok(()),
                            other => return self.bind_cl_pattern(&other, current_value, frame),
                        }
                    }
                }
            }
            other => Err(LispError::TypeError("list".into(), other.type_name())),
        }
    }

    pub(super) fn collect_cl_pattern_names(
        &self,
        pattern: &Value,
        bindings: &mut Vec<(String, Value)>,
    ) -> Result<(), LispError> {
        match pattern {
            Value::Symbol(name) if name == "nil" => Ok(()),
            Value::Symbol(name) => {
                if !bindings.iter().any(|(existing, _)| existing == name) {
                    bindings.push((name.clone(), Value::Nil));
                }
                Ok(())
            }
            Value::Cons(_, _) => {
                if let Ok(items) = pattern.to_vec() {
                    for item in items {
                        if matches!(&item, Value::Symbol(symbol) if symbol == "&optional" || symbol == "&rest")
                        {
                            continue;
                        }
                        self.collect_cl_pattern_names(&item, bindings)?;
                    }
                    Ok(())
                } else {
                    let mut current = pattern.clone();
                    loop {
                        match current {
                            Value::Cons(car, cdr) => {
                                self.collect_cl_pattern_names(&car.borrow().clone(), bindings)?;
                                current = cdr.borrow().clone();
                            }
                            Value::Nil => return Ok(()),
                            other => return self.collect_cl_pattern_names(&other, bindings),
                        }
                    }
                }
            }
            other => Err(LispError::TypeError("list".into(), other.type_name())),
        }
    }

    pub(super) fn upsert_frame_binding(
        frame: &mut Vec<(String, Value)>,
        name: String,
        value: Value,
    ) {
        if let Some(index) = frame.iter().rposition(|(existing, _)| existing == &name) {
            frame[index].1 = value;
        } else {
            frame.push((name, value));
        }
    }

    pub(super) fn same_frame_shape(left: &[(String, Value)], right: &[(String, Value)]) -> bool {
        // Frames stamped with an identity marker are the same frame exactly
        // when the markers agree; a name-shape match between two DIFFERENT
        // binding frames (e.g. two unrelated `let's binding the same
        // variable) must not alias them.
        match (Self::frame_identity(left), Self::frame_identity(right)) {
            (Some(left_id), Some(right_id)) => return left_id == right_id,
            (None, None) => {}
            _ => return false,
        }
        left.len() <= right.len()
            && left.iter().zip(right.iter()).all(
                |((left_name, left_value), (right_name, right_value))| {
                    left_name == right_name
                        && !(left_name == "sti"
                            && matches!(
                            (left_value, right_value),
                            (Value::Record(left_id), Value::Record(right_id)) if left_id != right_id
                            ))
                },
            )
    }

    pub(crate) fn frame_identity(frame: &[(String, Value)]) -> Option<i64> {
        frame.iter().rev().find_map(|(name, value)| {
            if name == FRAME_IDENTITY_MARKER
                && let Value::Integer(id) = value
            {
                Some(*id)
            } else {
                None
            }
        })
    }

    /// Push FRAME onto ENV with a fresh identity marker appended, so the
    /// closure-environment alignment logic can tell it apart from other
    /// frames that happen to bind the same names.
    pub(crate) fn push_marked_frame(env: &mut Env, mut frame: Vec<(String, Value)>) {
        frame.push(Self::fresh_frame_identity());
        env.push(frame);
    }

    // Give FRAME a brand-new identity stamp (replacing any existing one):
    // an oclosure copy is a DIFFERENT object, so its slot frame must never
    // unify with the original's under the frame-merge machinery.
    pub(crate) fn restamp_frame_identity(frame: &mut Vec<(String, Value)>) {
        frame.retain(|(name, _)| name != FRAME_IDENTITY_MARKER);
        frame.push(Self::fresh_frame_identity());
    }

    pub(crate) fn fresh_frame_identity() -> (String, Value) {
        use std::sync::atomic::{AtomicI64, Ordering};
        static NEXT_FRAME_IDENTITY: AtomicI64 = AtomicI64::new(1);
        (
            FRAME_IDENTITY_MARKER.to_string(),
            Value::Integer(NEXT_FRAME_IDENTITY.fetch_add(1, Ordering::Relaxed)),
        )
    }

    pub(super) fn align_captured_frames(captured: &Env, current: &Env) -> Vec<Option<usize>> {
        let mut mapping = vec![None; captured.len()];
        let mut search_start = 0;
        for captured_index in 0..captured.len() {
            for (current_index, current_frame) in current.iter().enumerate().skip(search_start) {
                if Self::same_frame_shape(&captured[captured_index], current_frame) {
                    mapping[captured_index] = Some(current_index);
                    search_start = current_index + 1;
                    break;
                }
            }
        }
        mapping
    }

    pub(super) fn merge_lexical_lambda_env(
        current: &Env,
        captured: &Env,
        mapping: &[Option<usize>],
    ) -> Env {
        let mut merged = captured.clone();
        for (captured_index, current_index) in mapping.iter().enumerate() {
            if let Some(current_index) = current_index
                && captured_index < merged.len()
                && *current_index < current.len()
            {
                merged[captured_index] = current[*current_index].clone();
            }
        }
        merged
    }

    pub(super) fn eval_cl_loop_do_body(
        &mut self,
        body: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let mut result = Value::Nil;
        let mut index = 0usize;
        while index < body.len() {
            match body.get(index) {
                Some(Value::Symbol(symbol)) if symbol == "do" => {
                    index += 1;
                }
                Some(Value::Symbol(symbol)) if symbol == "when" => {
                    let condition = body
                        .get(index + 1)
                        .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?;
                    if !matches!(body.get(index + 2), Some(Value::Symbol(kind)) if kind == "do") {
                        return Err(LispError::Signal("Unsupported cl-loop syntax".into()));
                    }
                    index += 3;
                    let clause_start = index;
                    while index < body.len()
                        && !matches!(body.get(index), Some(Value::Symbol(keyword)) if keyword == "when")
                    {
                        index += 1;
                    }
                    if self.eval(condition, env)?.is_truthy() {
                        result = self.sf_progn(&body[clause_start..index], env)?;
                    }
                }
                Some(form) => {
                    result = self.eval(form, env)?;
                    index += 1;
                }
                None => break,
            }
        }
        Ok(result)
    }
}
