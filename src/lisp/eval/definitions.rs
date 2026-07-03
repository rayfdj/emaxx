use super::*;

#[derive(Clone, Debug, PartialEq)]
enum ClDefmethodStoredSpecializer {
    Class(String),
    Eql(Value),
    Head(Value),
}

fn cl_defmethod_argument_key(name: &str) -> &str {
    name.trim_start_matches('_')
}

fn cl_defmethod_argument_precedence_index(variable: &str, precedence_order: &[String]) -> usize {
    let key = cl_defmethod_argument_key(variable);
    precedence_order
        .iter()
        .position(|candidate| candidate == variable || candidate == key)
        .unwrap_or(usize::MAX)
}

fn cl_defmethod_dispatch_specializers(
    method_specializers: &[ClDefmethodSpecializer],
    method_params: &[String],
    generic_params: &[String],
) -> Vec<ClDefmethodSpecializer> {
    let method_argument_variables = lambda_list_fixed_params(method_params);
    let generic_argument_variables = lambda_list_fixed_params(generic_params);
    method_specializers
        .iter()
        .map(|specializer| {
            let mut dispatch_specializer = specializer.clone();
            if !dispatch_specializer.is_context
                && let Some(position) = method_argument_variables
                    .iter()
                    .position(|variable| variable == &specializer.variable)
                && let Some(generic_variable) = generic_argument_variables.get(position)
            {
                dispatch_specializer.variable = generic_variable.clone();
            }
            dispatch_specializer
        })
        .collect()
}

fn cl_defmethod_runtime_variables(
    method_params: &[String],
    generic_params: &[String],
    method_specializers: &[ClDefmethodSpecializer],
) -> Vec<(String, String, Value)> {
    let method_fixed_params = lambda_list_fixed_params(method_params);
    let generic_fixed_params = lambda_list_fixed_params(generic_params);
    let method_rest_param = lambda_list_rest_param_from_params(method_params);
    let generic_rest_param = lambda_list_rest_param_from_params(generic_params);
    let mut runtime_variables = generic_fixed_params
        .iter()
        .map(|param| {
            (
                param.clone(),
                cl_defmethod_argument_key(param).to_string(),
                Value::Symbol(param.clone()),
            )
        })
        .collect::<Vec<_>>();
    if let Some(rest_param) = &generic_rest_param {
        runtime_variables.push((
            rest_param.clone(),
            cl_defmethod_argument_key(rest_param).to_string(),
            Value::Symbol(rest_param.clone()),
        ));
    }
    for (index, param) in method_fixed_params.iter().enumerate() {
        let source = if let Some(generic) = generic_fixed_params.get(index) {
            Value::Symbol(generic.clone())
        } else if let Some(generic_rest_param) = &generic_rest_param {
            Value::list([
                Value::Symbol("nth".into()),
                Value::Integer((index - generic_fixed_params.len()) as i64),
                Value::Symbol(generic_rest_param.clone()),
            ])
        } else {
            continue;
        };
        runtime_variables.push((
            param.clone(),
            cl_defmethod_argument_key(param).to_string(),
            source,
        ));
    }
    if let (Some(method_rest_param), Some(generic_rest_param)) =
        (&method_rest_param, &generic_rest_param)
    {
        let rest_offset = method_fixed_params
            .len()
            .saturating_sub(generic_fixed_params.len());
        let source = if rest_offset == 0 {
            Value::Symbol(generic_rest_param.clone())
        } else {
            Value::list([
                Value::Symbol("nthcdr".into()),
                Value::Integer(rest_offset as i64),
                Value::Symbol(generic_rest_param.clone()),
            ])
        };
        runtime_variables.push((
            method_rest_param.clone(),
            cl_defmethod_argument_key(method_rest_param).to_string(),
            source,
        ));
    }
    for specializer in method_specializers {
        if specializer.is_context {
            runtime_variables.push((
                specializer.variable.clone(),
                cl_defmethod_argument_key(&specializer.variable).to_string(),
                Value::Symbol(specializer.variable.clone()),
            ));
        }
    }
    runtime_variables
}

fn cl_defmethod_alias_method_body(
    method_fixed_params: &[String],
    method_rest_param: Option<&str>,
    generic_fixed_params: &[String],
    generic_rest_param: Option<&str>,
    method_body: Vec<Value>,
) -> Vec<Value> {
    let mut alias_bindings = Vec::new();
    for (index, param) in method_fixed_params.iter().enumerate() {
        let source = if let Some(generic) = generic_fixed_params.get(index) {
            Value::Symbol(generic.clone())
        } else if let Some(rest_param) = generic_rest_param {
            Value::list([
                Value::Symbol("nth".into()),
                Value::Integer((index - generic_fixed_params.len()) as i64),
                Value::Symbol(rest_param.to_string()),
            ])
        } else {
            continue;
        };
        if !matches!(&source, Value::Symbol(name) if name == param) {
            alias_bindings.push(Value::list([Value::Symbol(param.clone()), source]));
        }
    }
    if let (Some(method_rest_param), Some(generic_rest_param)) =
        (method_rest_param, generic_rest_param)
    {
        let rest_offset = method_fixed_params
            .len()
            .saturating_sub(generic_fixed_params.len());
        let source = if rest_offset == 0 {
            Value::Symbol(generic_rest_param.to_string())
        } else {
            Value::list([
                Value::Symbol("nthcdr".into()),
                Value::Integer(rest_offset as i64),
                Value::Symbol(generic_rest_param.to_string()),
            ])
        };
        if !matches!(&source, Value::Symbol(name) if name == method_rest_param) {
            alias_bindings.push(Value::list([
                Value::Symbol(method_rest_param.to_string()),
                source,
            ]));
        }
    }
    if alias_bindings.is_empty() {
        method_body
    } else {
        vec![Value::list([
            Value::Symbol("let".into()),
            Value::list(alias_bindings),
            Value::list(
                std::iter::once(Value::Symbol("progn".into()))
                    .chain(method_body)
                    .collect::<Vec<_>>(),
            ),
        ])]
    }
}

impl ClDefmethodStoredSpecializer {
    fn parse(value: &Value) -> Option<Self> {
        if let Ok(items) = value.to_vec() {
            match items.as_slice() {
                [Value::Symbol(kind), Value::Symbol(class_name)] if kind == "class" => {
                    return Some(Self::Class(class_name.clone()));
                }
                [Value::Symbol(kind), eql_value] if kind == "eql" => {
                    return Some(Self::Eql(eql_value.clone()));
                }
                [Value::Symbol(kind), head_value] if kind == "head" => {
                    return Some(Self::Head(head_value.clone()));
                }
                _ => {}
            }
        }
        value
            .as_symbol()
            .ok()
            .map(|name| Self::Class(name.to_string()))
    }

    fn key(&self) -> String {
        match self {
            Self::Class(class_name) => format!("class:{class_name}"),
            Self::Eql(value) => format!("eql:{value}"),
            Self::Head(value) => format!("head:{value}"),
        }
    }

    fn hidden_key(&self) -> String {
        self.key().replace([':', '\'', ' ', '(', ')'], "_")
    }

    fn metadata_value(&self) -> Value {
        match self {
            Self::Class(class_name) => Value::list([
                Value::Symbol("class".into()),
                Value::Symbol(class_name.clone()),
            ]),
            Self::Eql(value) => Value::list([Value::Symbol("eql".into()), value.clone()]),
            Self::Head(value) => Value::list([Value::Symbol("head".into()), value.clone()]),
        }
    }

    fn quoted_or_self_evaluating(value: &Value) -> Value {
        if matches!(value, Value::Symbol(_) | Value::Nil | Value::T) {
            Value::list([Value::Symbol("quote".into()), value.clone()])
        } else {
            value.clone()
        }
    }

    fn condition_value(&self, runtime_value: Value) -> Value {
        match self {
            Self::Class(class_name) if class_name == "t" => Value::T,
            Self::Class(class_name) => Value::list([
                Value::Symbol("cl-typep".into()),
                runtime_value,
                Value::list([
                    Value::Symbol("quote".into()),
                    Value::Symbol(class_name.clone()),
                ]),
            ]),
            Self::Eql(expected) => Value::list([
                Value::Symbol("eql".into()),
                runtime_value,
                Self::quoted_or_self_evaluating(expected),
            ]),
            Self::Head(expected) => Value::list([
                Value::Symbol("and".into()),
                Value::list([Value::Symbol("consp".into()), runtime_value.clone()]),
                Value::list([
                    Value::Symbol("eql".into()),
                    Value::list([Value::Symbol("car".into()), runtime_value]),
                    Self::quoted_or_self_evaluating(expected),
                ]),
            ]),
        }
    }

    fn is_more_specific_than(&self, other: &Self, interp: &Interpreter) -> bool {
        match (self, other) {
            (Self::Class(left), Self::Class(right)) => {
                left != right
                    && interp
                        .class_allparents(left)
                        .iter()
                        .any(|parent| matches!(parent, Value::Symbol(parent) if parent == right))
            }
            (Self::Eql(value), Self::Class(class_name)) => {
                let Ok(actual) = primitives::cl_type_name(interp, value) else {
                    return false;
                };
                class_name == "t"
                    || actual == class_name
                    || interp.class_allparents(actual).iter().any(
                        |parent| matches!(parent, Value::Symbol(parent) if parent == class_name),
                    )
            }
            (Self::Head(_), Self::Class(class_name)) => class_name == "t",
            _ => false,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
struct ClDefmethodStoredMethod {
    specializers: Vec<(String, ClDefmethodStoredSpecializer)>,
}

impl ClDefmethodStoredMethod {
    fn from_specializers(specializers: &[ClDefmethodSpecializer]) -> Self {
        let specializers = specializers
            .iter()
            .filter_map(|specializer| {
                ClDefmethodStoredSpecializer::parse(&specializer.metadata_value())
                    .map(|stored| (specializer.variable.clone(), stored))
            })
            .collect();
        Self { specializers }
    }

    fn parse(value: &Value) -> Option<Self> {
        if let Some(specializer) = ClDefmethodStoredSpecializer::parse(value) {
            return Some(Self {
                specializers: vec![("".into(), specializer)],
            });
        }
        let items = value.to_vec().ok()?;
        if !matches!(items.first(), Some(Value::Symbol(kind)) if kind == "method") {
            return None;
        }
        let mut specializers = Vec::new();
        for item in &items[1..] {
            let parts = item.to_vec().ok()?;
            let [Value::Symbol(variable), specializer] = parts.as_slice() else {
                return None;
            };
            specializers.push((
                variable.clone(),
                ClDefmethodStoredSpecializer::parse(specializer)?,
            ));
        }
        Some(Self { specializers })
    }

    fn metadata_value(&self) -> Value {
        let mut items = Vec::with_capacity(self.specializers.len() + 1);
        items.push(Value::Symbol("method".into()));
        items.extend(self.specializers.iter().map(|(variable, specializer)| {
            Value::list([
                Value::Symbol(variable.clone()),
                specializer.metadata_value(),
            ])
        }));
        Value::list(items)
    }

    fn hidden_key(&self) -> String {
        self.specializers
            .iter()
            .map(|(variable, specializer)| format!("{variable}_{}", specializer.hidden_key()))
            .collect::<Vec<_>>()
            .join("_")
            .replace([':', '\'', ' ', '(', ')'], "_")
    }

    fn condition(&self, runtime_variables: &[(String, String, Value)]) -> Value {
        let mut conditions = self
            .specializers
            .iter()
            .filter_map(|(variable, specializer)| {
                let key = cl_defmethod_argument_key(variable);
                let runtime_variable =
                    runtime_variables
                        .iter()
                        .find_map(|(runtime, runtime_key, value)| {
                            (runtime == variable || runtime_key == key).then_some(value.clone())
                        })?;
                Some(specializer.condition_value(runtime_variable))
            });
        let Some(first) = conditions.next() else {
            return Value::T;
        };
        conditions.fold(first, |condition, next| {
            Value::list([Value::Symbol("and".into()), condition, next])
        })
    }

    fn is_more_specific_than(
        &self,
        other: &Self,
        precedence_order: &[String],
        interp: &Interpreter,
    ) -> bool {
        let mut variables = self
            .specializers
            .iter()
            .map(|(variable, _)| variable.clone())
            .chain(
                other
                    .specializers
                    .iter()
                    .map(|(variable, _)| variable.clone()),
            )
            .collect::<Vec<_>>();
        variables.sort();
        variables.dedup();
        variables.sort_by_key(|variable| {
            cl_defmethod_argument_precedence_index(variable, precedence_order)
        });
        for variable in variables {
            let left = self.specializer_for(&variable);
            let right = other.specializer_for(&variable);
            match (left, right) {
                (Some(left), Some(right)) if left == right => {}
                (Some(left), Some(right)) if left.is_more_specific_than(right, interp) => {
                    return true;
                }
                (Some(_), None) => return true,
                (None, Some(_)) => return false,
                (Some(_), Some(_)) => return false,
                (None, None) => {}
            }
        }
        false
    }

    fn specializer_for(&self, variable: &str) -> Option<&ClDefmethodStoredSpecializer> {
        let key = cl_defmethod_argument_key(variable);
        self.specializers
            .iter()
            .find_map(|(candidate, specializer)| {
                (candidate == variable || cl_defmethod_argument_key(candidate) == key)
                    .then_some(specializer)
            })
    }
}

fn record_defun_attributes(interp: &mut Interpreter, name: &str, forms: &[Value]) {
    interp.remove_symbol_property(name, "interactive-form");
    for form in forms {
        if matches!(form, Value::String(_) | Value::StringObject(_)) {
            continue;
        }
        let Ok(items) = form.to_vec() else {
            break;
        };
        match items.first().and_then(|value| value.as_symbol().ok()) {
            Some("declare") => {
                for declaration in items.iter().skip(1) {
                    let Ok(parts) = declaration.to_vec() else {
                        continue;
                    };
                    match parts.first().and_then(|value| value.as_symbol().ok()) {
                        Some("pure") if parts.len() >= 2 => {
                            interp.put_symbol_property(name, "pure", parts[1].clone());
                        }
                        Some("indent") if parts.len() >= 2 => {
                            interp.put_symbol_property(
                                name,
                                "lisp-indent-function",
                                parts[1].clone(),
                            );
                        }
                        _ => {}
                    }
                }
            }
            Some("interactive") => {
                interp.put_symbol_property(name, "interactive-form", Value::list(items));
            }
            _ => break,
        }
    }
}

fn body_closure_dont_trim_context(body: &[Value]) -> bool {
    let mut start = 0usize;
    if body.len() > 1
        && matches!(
            body.first(),
            Some(Value::String(_) | Value::StringObject(_))
        )
    {
        start = 1;
    }
    matches!(
        body.get(start),
        Some(Value::Symbol(marker)) if marker == ":closure-dont-trim-context"
    ) && body.len().saturating_sub(start) > 1
}

impl Interpreter {
    fn normalize_function_body_documentation(
        &mut self,
        forms: &[Value],
        env: &mut Env,
    ) -> Result<(Option<Value>, Vec<Value>), LispError> {
        let Some(first) = forms.first() else {
            return Ok((None, Vec::new()));
        };
        let documentation = match first {
            Value::String(text) => Some(Value::String(text.clone())),
            Value::StringObject(state) => Some(Value::String(state.borrow().text.clone())),
            Value::Cons(_, _) => {
                let items = first.to_vec()?;
                match items.as_slice() {
                    [Value::Symbol(head), expression] if head == ":documentation" => {
                        let value = self.eval(expression, env)?;
                        Some(Value::String(value.as_string()?.to_string()))
                    }
                    _ => None,
                }
            }
            _ => None,
        };

        let Some(documentation) = documentation else {
            return Ok((None, forms.to_vec()));
        };
        let mut normalized = Vec::with_capacity(forms.len());
        normalized.push(documentation.clone());
        normalized.extend(forms[1..].iter().cloned());
        Ok((Some(documentation), normalized))
    }

    pub(super) fn sf_setq(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        self.sf_setq_internal(items, env, false)
    }

    pub(super) fn sf_setq_default(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let mut result = Value::Nil;
        let mut index = 1usize;
        while index + 1 < items.len() {
            let name = assignment_target_name(&items[index])?;
            let resolved = self.resolve_variable_name(&name)?;
            let evaluated = self.eval(&items[index + 1], env)?;
            let value = self.prepare_variable_assignment(&resolved, evaluated)?;
            result = value.clone();
            self.notify_variable_watchers(&resolved, value.clone(), "set", None, env)?;
            self.set_global_binding(&resolved, value);
            index += 2;
        }
        Ok(result)
    }

    pub(super) fn sf_setq_local(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        self.sf_setq_internal(items, env, true)
    }

    pub fn set_custom_option(
        &mut self,
        symbol: &str,
        value: Value,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let resolved = self.resolve_variable_name(symbol)?;
        if let Some(setter) = self.get_symbol_property(&resolved, "custom-set") {
            self.call_function_value(
                setter,
                None,
                &[Value::Symbol(resolved.clone()), value.clone()],
                env,
            )?;
        } else {
            self.call_function_value(
                Value::BuiltinFunc("set-default".into()),
                Some("set-default"),
                &[Value::Symbol(resolved), value.clone()],
                env,
            )?;
        }
        Ok(value)
    }

    pub(super) fn sf_setopt(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() == 1 {
            return Ok(Value::Nil);
        }
        if items.len().is_multiple_of(2) {
            return Err(LispError::WrongNumberOfArgs(
                "setopt".into(),
                items.len().saturating_sub(1),
            ));
        }

        let mut result = Value::Nil;
        let mut index = 1;
        while index + 1 < items.len() {
            let symbol = items[index].as_symbol()?.to_string();
            let value = self.eval(&items[index + 1], env)?;
            if let Some(custom_type) = self.get_symbol_property(&symbol, "custom-type") {
                let matches = if self.lookup_function("widget-convert", env).is_ok()
                    && self.lookup_function("widget-apply", env).is_ok()
                {
                    let widget = self.call_function_value(
                        Value::Symbol("widget-convert".into()),
                        Some("widget-convert"),
                        std::slice::from_ref(&custom_type),
                        env,
                    )?;
                    self.call_function_value(
                        Value::Symbol("widget-apply".into()),
                        Some("widget-apply"),
                        &[widget, Value::Symbol(":match".into()), value.clone()],
                        env,
                    )?
                    .is_truthy()
                } else {
                    custom_type_matches_value(&custom_type, &value)
                };
                if !matches {
                    self.call_function_value(
                        Value::BuiltinFunc("warn".into()),
                        Some("warn"),
                        &[
                            Value::String("Value `%S' does not match type %s".into()),
                            value.clone(),
                            custom_type,
                        ],
                        env,
                    )?;
                }
            }
            result = self.set_custom_option(&symbol, value, env)?;
            index += 2;
        }

        Ok(result)
    }

    pub(super) fn sf_setf(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 3 || items.len().is_multiple_of(2) {
            return Err(LispError::WrongNumberOfArgs(
                "setf".into(),
                items.len().saturating_sub(1),
            ));
        }
        if items.len() > 3 {
            let mut result = Value::Nil;
            let mut index = 1;
            while index + 1 < items.len() {
                let pair = [
                    Value::Symbol("setf".into()),
                    items[index].clone(),
                    items[index + 1].clone(),
                ];
                result = self.sf_setf(&pair, env)?;
                index += 2;
            }
            return Ok(result);
        }
        if matches!(items.get(1), Some(Value::Symbol(_) | Value::Nil | Value::T)) {
            let setq_items = [
                Value::Symbol("setq".into()),
                items[1].clone(),
                items[2].clone(),
            ];
            return self.sf_setq(&setq_items, env);
        }
        let place = items[1].to_vec()?;
        if let Some(Value::Symbol(name)) = place.first()
            && let Some(expanded) = self.try_macroexpand(name, &place[1..], env)?
            && expanded != items[1]
        {
            let expanded_items = [Value::Symbol("setf".into()), expanded, items[2].clone()];
            return self.sf_setf(&expanded_items, env);
        }
        match place.first() {
            Some(Value::Symbol(name)) if name == "symbol-function" => {
                let Some(target) = place.get(1) else {
                    return Err(LispError::Signal(format!(
                        "Unsupported setf place: {}",
                        items[1]
                    )));
                };
                let function_name = function_name_from_binding_form(target)?;
                let value = self.eval(&items[2], env)?;
                if value.is_nil() {
                    self.set_function_binding(&function_name, None);
                    Ok(Value::Nil)
                } else {
                    self.validate_function_binding(&function_name, &value)?;
                    self.set_function_binding(&function_name, Some(value.clone()));
                    Ok(value)
                }
            }
            Some(Value::Symbol(name)) if matches!(name.as_str(), "cond" | "if" | "progn") => {
                let resolved = self.resolve_setf_place(&items[1], env)?;
                let value = self.eval(&items[2], env)?;
                self.set_resolved_setf_place_value(&resolved, value.clone(), env)?;
                Ok(value)
            }
            Some(Value::Symbol(name)) if name == "gv-synthetic-place" => {
                let resolved = self.resolve_setf_place(&items[1], env)?;
                let value = self.eval(&items[2], env)?;
                self.set_resolved_setf_place_value(&resolved, value.clone(), env)?;
                Ok(value)
            }
            Some(Value::Symbol(name)) if name == "cl--class-parents" => {
                let Some(target) = place.get(1) else {
                    return Err(LispError::Signal(format!(
                        "Unsupported setf place: {}",
                        items[1]
                    )));
                };
                let class = self.eval(target, env)?;
                let value = self.eval(&items[2], env)?;
                self.set_class_parents_value(&class, value.clone())?;
                Ok(value)
            }
            Some(Value::Symbol(name)) if name == "cl--find-class" => {
                let Some(target) = place.get(1) else {
                    return Err(LispError::Signal(format!(
                        "Unsupported setf place: {}",
                        items[1]
                    )));
                };
                let class_name = self.eval(target, env)?.as_symbol()?.to_string();
                let value = self.eval(&items[2], env)?;
                self.set_class_record(&class_name, value.clone())?;
                Ok(value)
            }
            Some(Value::Symbol(name))
                if self
                    .get_symbol_property(name, "emaxx-struct-slot")
                    .is_some() =>
            {
                self.sf_setf_struct_accessor(name, &place, &items[2], env)
            }
            Some(Value::Symbol(name))
                if self.get_symbol_property(name, "emaxx-gv-setter").is_some() =>
            {
                self.sf_setf_gv_setter(name, &place, &items[2], env)
            }
            Some(Value::Symbol(name)) if name == "alist-get" => {
                self.sf_setf_alist_get(&place, &items[2], env)
            }
            Some(Value::Symbol(name)) if matches!(name.as_str(), "plist-get" | "cl-getf") => {
                self.sf_setf_plist_get(&place, &items[2], env)
            }
            Some(Value::Symbol(name)) if name == "gethash" => {
                self.sf_setf_gethash(&place, &items[2], env)
            }
            Some(Value::Symbol(name)) if name == "slot-value" => {
                let Some(object_expr) = place.get(1) else {
                    return Err(LispError::Signal(format!(
                        "Unsupported setf place: {}",
                        items[1]
                    )));
                };
                let Some(slot_expr) = place.get(2) else {
                    return Err(LispError::Signal(format!(
                        "Unsupported setf place: {}",
                        items[1]
                    )));
                };
                let object = self.eval(object_expr, env)?;
                let slot = self.eval(slot_expr, env)?;
                let value = self.eval(&items[2], env)?;
                self.call_function_value(
                    Value::Symbol("eieio-oset".into()),
                    Some("eieio-oset"),
                    &[object, slot, value.clone()],
                    env,
                )?;
                Ok(value)
            }
            Some(Value::Symbol(name)) if decoded_time_accessor_index(name).is_some() => {
                self.sf_setf_decoded_time_accessor(name, &place, &items[2], env)
            }
            Some(Value::Symbol(name)) if matches!(name.as_str(), "car" | "cdr") => {
                let resolved = self.resolve_setf_place(&items[1], env)?;
                let value = self.eval(&items[2], env)?;
                self.set_resolved_setf_place_value(&resolved, value.clone(), env)?;
                Ok(value)
            }
            Some(Value::Symbol(name)) if name == "nth" => self.sf_setf_nth(&place, &items[2], env),
            Some(Value::Symbol(name)) if name == "elt" => self.sf_setf_aref(&place, &items[2], env),
            Some(Value::Symbol(name)) if name == "nthcdr" => {
                let value = self.eval(&items[2], env)?;
                self.set_setf_place_value(&items[1], value.clone(), env)?;
                Ok(value)
            }
            Some(Value::Symbol(name)) if name == "aref" => {
                self.sf_setf_aref(&place, &items[2], env)
            }
            Some(Value::Symbol(name)) if name == "image-property" => {
                self.sf_setf_image_property(&place, &items[2], env)
            }
            Some(Value::Symbol(name)) if name == "terminal-parameter" => {
                let value = self.eval(&items[2], env)?;
                self.set_setf_place_value(&items[1], value.clone(), env)?;
                Ok(value)
            }
            Some(Value::Symbol(name)) => {
                // Last resort before giving up: a `gv-expander' property
                // registered by a `(declare (gv-expander ...))'.  `gv-get'
                // calls the expander with a DO continuation and the
                // unevaluated place arguments; DO's setter returns the store
                // expression, which we then evaluate.  Native place handlers
                // above take precedence.
                if let Some(expander) = self.get_symbol_property(name, "gv-expander") {
                    let do_form = Value::list([
                        Value::Symbol("lambda".into()),
                        Value::list([
                            Value::Symbol("getter".into()),
                            Value::Symbol("setter".into()),
                        ]),
                        Value::list([
                            Value::Symbol("funcall".into()),
                            Value::Symbol("setter".into()),
                            Value::list([Value::Symbol("quote".into()), items[2].clone()]),
                        ]),
                    ]);
                    let do_function = self.eval(&do_form, env)?;
                    let mut expander_args = vec![do_function];
                    expander_args.extend(place[1..].iter().cloned());
                    let store_expression =
                        self.call_function_value(expander, None, &expander_args, env)?;
                    return self.eval(&store_expression, env);
                }
                let setter_name = format!("(setf {name})");
                let Ok(setter) = self.lookup_function(&setter_name, env) else {
                    return Err(LispError::Signal(format!(
                        "Unsupported setf place: {}",
                        items[1]
                    )));
                };
                let mut place_args = Vec::with_capacity(place.len().saturating_sub(1));
                for arg in &place[1..] {
                    place_args.push(self.eval(arg, env)?);
                }
                let value = self.eval(&items[2], env)?;
                let mut args = Vec::with_capacity(place_args.len() + 1);
                args.push(value);
                args.extend(place_args);
                self.call_function_value(setter, Some(&setter_name), &args, env)
            }
            _ => Err(LispError::Signal(format!(
                "Unsupported setf place: {}",
                items[1]
            ))),
        }
    }

    pub(super) fn sf_setf_struct_accessor(
        &mut self,
        accessor: &str,
        place: &[Value],
        value_expr: &Value,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let Some(target) = place.get(1) else {
            return Err(LispError::Signal(format!(
                "Unsupported setf place: {}",
                Value::list(place.to_vec())
            )));
        };
        let expected_type = self
            .get_symbol_property(accessor, "emaxx-struct-type")
            .and_then(|value| value.as_symbol().ok().map(str::to_string))
            .ok_or_else(|| LispError::Signal(format!("Unknown struct accessor: {accessor}")))?;
        let slot_index = self
            .get_symbol_property(accessor, "emaxx-struct-slot")
            .and_then(|value| value.as_integer().ok())
            .map(|value| value.max(0) as usize)
            .ok_or_else(|| LispError::Signal(format!("Unknown struct accessor: {accessor}")))?;
        let object = self.eval(target, env)?;
        let value = self.eval(value_expr, env)?;
        let predicate = format!("{expected_type}-p");
        let Value::Record(id) = object.clone() else {
            return Err(wrong_type_argument(&predicate, object));
        };
        let type_matches = self
            .find_record(id)
            .is_some_and(|record| record.type_name == expected_type)
            || self.value_is_instance_of_class(&object, &expected_type);
        let record = self
            .find_record_mut(id)
            .ok_or_else(|| wrong_type_argument(&predicate, Value::Record(id)))?;
        if !type_matches {
            return Err(wrong_type_argument(&predicate, Value::Record(id)));
        }
        if slot_index >= record.slots.len() {
            return Err(LispError::Signal(format!(
                "Struct slot out of range: {slot_index}"
            )));
        }
        record.slots[slot_index] = value.clone();
        Ok(value)
    }

    pub(super) fn sf_setf_gv_setter(
        &mut self,
        accessor: &str,
        place: &[Value],
        value_expr: &Value,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let setter = self
            .get_symbol_property(accessor, "emaxx-gv-setter")
            .and_then(|value| value.as_symbol().ok().map(str::to_string))
            .ok_or_else(|| LispError::Signal(format!("Unknown gv setter: {accessor}")))?;
        let mut args = Vec::with_capacity(place.len());
        for arg in &place[1..] {
            args.push(self.eval(arg, env)?);
        }
        let value = self.eval(value_expr, env)?;
        args.push(value.clone());
        let setter_function = self.lookup_function(&setter, env)?;
        self.call_function_value(setter_function, Some(&setter), &args, env)?;
        Ok(value)
    }

    pub(super) fn sf_setf_alist_get(
        &mut self,
        place: &[Value],
        value_expr: &Value,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let Some(key_expr) = place.get(1) else {
            return Err(LispError::Signal("Unsupported setf place".into()));
        };
        let Some(alist_place) = place.get(2) else {
            return Err(LispError::Signal("Unsupported setf place".into()));
        };
        let key = self.eval(key_expr, env)?;
        let alist = self.eval(alist_place, env)?;
        let default = match place.get(3) {
            Some(expr) => self.eval(expr, env)?,
            None => Value::Nil,
        };
        let remove = match place.get(4) {
            Some(expr) => self.eval(expr, env)?,
            None => Value::Nil,
        };
        let testfn = match place.get(5) {
            Some(expr) => Some(self.eval(expr, env)?),
            None => None,
        };
        let value = self.eval(value_expr, env)?;
        let should_remove = remove.is_truthy() && value == default;
        let mut updated = false;
        let mut new_entries = Vec::new();

        for entry in alist.to_vec()? {
            let matches = if !updated {
                if let Some((car, _)) = entry.cons_values() {
                    primitives::value_matches_with_test(self, &key, &car, testfn.as_ref(), env)?
                } else {
                    false
                }
            } else {
                false
            };
            if matches {
                updated = true;
                if !should_remove {
                    new_entries.push(Value::cons(entry.car()?, value.clone()));
                }
            } else {
                new_entries.push(entry);
            }
        }

        if !updated && !should_remove {
            new_entries.insert(0, Value::cons(key.clone(), value.clone()));
        }

        self.set_setf_place_value(alist_place, Value::list(new_entries), env)?;
        Ok(value)
    }

    pub(super) fn sf_setf_plist_get(
        &mut self,
        place: &[Value],
        value_expr: &Value,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let Some(plist_place) = place.get(1) else {
            return Err(LispError::Signal("Unsupported setf place".into()));
        };
        let Some(key_expr) = place.get(2) else {
            return Err(LispError::Signal("Unsupported setf place".into()));
        };
        let plist = self.eval(plist_place, env)?;
        let key = self.eval(key_expr, env)?;
        let value = self.eval(value_expr, env)?;
        let head = place.first().and_then(|value| value.as_symbol().ok());
        let updated = if head == Some("cl-getf") {
            self.cl_set_getf(plist, key, value.clone(), env)?
        } else if head == Some("plist-get")
            && let Some(testfn_expr) = place.get(3)
        {
            let testfn = self.eval(testfn_expr, env)?;
            primitives::call(self, "plist-put", &[plist, key, value.clone(), testfn], env)?
        } else {
            primitives::call(self, "plist-put", &[plist, key, value.clone()], env)?
        };
        self.set_setf_place_value(plist_place, updated, env)?;
        Ok(value)
    }

    fn cl_set_getf(
        &mut self,
        plist: Value,
        key: Value,
        value: Value,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let mut current = plist.clone();
        let mut seen = std::collections::HashSet::new();
        loop {
            match current {
                Value::Nil => {
                    return Ok(Value::cons(key, Value::cons(value, plist)));
                }
                Value::Cons(car, cdr) => {
                    let cell_id = std::rc::Rc::as_ptr(&car) as usize;
                    if !seen.insert(cell_id) {
                        return Err(LispError::SignalValue(Value::list([
                            Value::Symbol("circular-list".into()),
                            Value::String("Circular list".into()),
                        ])));
                    }
                    let property = car.borrow().clone();
                    if primitives::value_matches_with_test(self, &property, &key, None, env)? {
                        return match cdr.borrow().clone() {
                            Value::Cons(value_cell, _) => {
                                *value_cell.borrow_mut() = value;
                                Ok(plist)
                            }
                            Value::Nil => Ok(Value::cons(key, Value::cons(value, plist))),
                            _ => Err(primitives::plist_type_error(&plist)),
                        };
                    }
                    match cdr.borrow().clone() {
                        Value::Cons(_, next_cdr) => current = next_cdr.borrow().clone(),
                        Value::Nil => return Ok(Value::cons(key, Value::cons(value, plist))),
                        _ => return Err(primitives::plist_type_error(&plist)),
                    }
                }
                _ => return Err(primitives::plist_type_error(&plist)),
            }
        }
    }

    pub(super) fn sf_setf_gethash(
        &mut self,
        place: &[Value],
        value_expr: &Value,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let Some(key_expr) = place.get(1) else {
            return Err(LispError::Signal("Unsupported setf place".into()));
        };
        let Some(table_expr) = place.get(2) else {
            return Err(LispError::Signal("Unsupported setf place".into()));
        };
        let key = self.eval(key_expr, env)?;
        let table = self.eval(table_expr, env)?;
        let value = self.eval(value_expr, env)?;
        primitives::call(self, "puthash", &[key, value.clone(), table], env)?;
        Ok(value)
    }

    pub(super) fn sf_setf_nth(
        &mut self,
        place: &[Value],
        value_expr: &Value,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if place.len() < 3 {
            return Err(LispError::Signal("Unsupported setf place".into()));
        }
        let index = self.eval(&place[1], env)?.as_integer()?.max(0) as usize;
        let mut cell = self.eval(&place[2], env)?;
        for _ in 0..index {
            cell = cell.cdr()?;
        }
        let value = self.eval(value_expr, env)?;
        cell.set_car(value.clone())?;
        Ok(value)
    }

    pub(super) fn sf_setf_decoded_time_accessor(
        &mut self,
        accessor: &str,
        place: &[Value],
        value_expr: &Value,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let Some(index) = decoded_time_accessor_index(accessor) else {
            return Err(LispError::Signal("Unsupported setf place".into()));
        };
        let Some(target_expr) = place.get(1) else {
            return Err(LispError::Signal("Unsupported setf place".into()));
        };

        let mut cell = self.eval(target_expr, env)?;
        let value = self.eval(value_expr, env)?;
        set_decoded_time_accessor_value(index, &mut cell, value.clone())?;
        Ok(value)
    }

    pub(super) fn sf_setf_aref(
        &mut self,
        place: &[Value],
        value_expr: &Value,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let Some(sequence_place) = place.get(1) else {
            return Err(LispError::Signal("Unsupported setf place".into()));
        };
        let Some(index_expr) = place.get(2) else {
            return Err(LispError::Signal("Unsupported setf place".into()));
        };
        let current = self.eval(sequence_place, env)?;
        let index_value = self.eval(index_expr, env)?;
        let value = self.eval(value_expr, env)?;

        if matches!(current, Value::CharTable(_))
            || matches!(
                &current,
                Value::Record(id)
                    if self
                        .find_record(*id)
                        .is_some_and(|record| record.type_name == "bool-vector")
            )
        {
            primitives::call(self, "aset", &[current, index_value, value.clone()], env)?;
            return Ok(value);
        }

        let index = index_value.as_integer()? as usize;
        let updated = if matches!(current, Value::String(_) | Value::StringObject(_)) {
            primitives::aset_string_value(&current, index, &value)?
        } else {
            let mut entries = current.to_vec()?;
            let tagged = matches!(
                entries.first(),
                Some(Value::Symbol(symbol)) if symbol == "vector-literal"
            );
            let slot = if tagged { index + 1 } else { index };
            if slot >= entries.len() {
                return Err(LispError::Signal("Args out of range".into()));
            }
            entries[slot] = value.clone();
            Value::list(entries)
        };

        self.set_setf_place_value(sequence_place, updated, env)?;
        Ok(value)
    }

    pub(super) fn sf_setf_image_property(
        &mut self,
        place: &[Value],
        value_expr: &Value,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let Some(image_place) = place.get(1) else {
            return Err(LispError::Signal("Unsupported setf place".into()));
        };
        let Some(property_expr) = place.get(2) else {
            return Err(LispError::Signal("Unsupported setf place".into()));
        };
        let image = self.eval(image_place, env)?;
        let property = self.eval(property_expr, env)?;
        let value = self.eval(value_expr, env)?;
        let mut descriptor = image.to_vec()?;
        if descriptor.is_empty() {
            return Err(LispError::Signal("Unsupported setf place".into()));
        }

        let mut property_index = None;
        let mut cursor = 1;
        while cursor + 1 < descriptor.len() {
            if descriptor[cursor] == property {
                property_index = Some(cursor);
                break;
            }
            cursor += 2;
        }

        match property_index {
            Some(index) if value.is_nil() => {
                descriptor.drain(index..=index + 1);
            }
            Some(index) => descriptor[index + 1] = value.clone(),
            None if !value.is_nil() => {
                descriptor.push(property);
                descriptor.push(value.clone());
            }
            None => {}
        }

        self.set_setf_place_value(image_place, Value::list(descriptor), env)?;
        Ok(value)
    }

    pub(super) fn set_setf_place_value(
        &mut self,
        place: &Value,
        value: Value,
        env: &mut Env,
    ) -> Result<(), LispError> {
        let place = self.resolve_setf_place(place, env)?;
        self.set_resolved_setf_place_value(&place, value, env)
    }

    pub(super) fn set_resolved_setf_place_value(
        &mut self,
        place: &Value,
        value: Value,
        env: &mut Env,
    ) -> Result<(), LispError> {
        match &place {
            Value::Symbol(name) => {
                self.set_variable(name, value, env);
                Ok(())
            }
            Value::Cons(_, _) => {
                let items = place.to_vec()?;
                if matches!(items.first(), Some(Value::Symbol(name)) if name == "--emaxx-setf-car-place" || name == "--emaxx-setf-cdr-place")
                {
                    let Some(target) = items.get(1) else {
                        return Err(LispError::Signal("Unsupported setf place".into()));
                    };
                    if matches!(items.first(), Some(Value::Symbol(name)) if name == "--emaxx-setf-car-place")
                    {
                        target.set_car(value)?;
                    } else {
                        target.set_cdr(value)?;
                    }
                    Ok(())
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
                    let mut cell = target.clone();
                    set_decoded_time_accessor_value(index, &mut cell, value)
                } else if matches!(
                    items.first(),
                    Some(Value::Symbol(name)) if name == "--emaxx-setf-gv-synthetic-place"
                ) {
                    let Some(setter) = items.get(2) else {
                        return Err(LispError::Signal("Unsupported setf place".into()));
                    };
                    let setter_form =
                        self.call_function_value(setter.clone(), None, &[value], env)?;
                    self.eval(&setter_form, env).map(|_| ())
                } else if matches!(items.first(), Some(Value::Symbol(name)) if name == "symbol-value")
                {
                    let Some(symbol_form) = items.get(1) else {
                        return Err(LispError::Signal("Unsupported setf place".into()));
                    };
                    let symbol = self.eval(symbol_form, env)?;
                    let symbol = symbol.as_symbol()?.to_string();
                    self.set_symbol_value_cell(&symbol, value);
                    Ok(())
                } else if matches!(
                    items.first(),
                    Some(Value::Symbol(name))
                        if self.get_symbol_property(name, "emaxx-struct-slot").is_some()
                ) {
                    let accessor = items[0].as_symbol().expect("checked symbol").to_string();
                    let value_expr = quoted_literal(&value);
                    self.sf_setf_struct_accessor(&accessor, &items, &value_expr, env)
                        .map(|_| ())
                } else if matches!(
                    items.first(),
                    Some(Value::Symbol(name))
                        if self.get_symbol_property(name, "emaxx-gv-setter").is_some()
                ) {
                    let accessor = items[0].as_symbol().expect("checked symbol").to_string();
                    let value_expr = quoted_literal(&value);
                    self.sf_setf_gv_setter(&accessor, &items, &value_expr, env)
                        .map(|_| ())
                } else if matches!(items.first(), Some(Value::Symbol(name)) if name == "car" || name == "cdr")
                {
                    let Some(target_expr) = items.get(1) else {
                        return Err(LispError::Signal("Unsupported setf place".into()));
                    };
                    let target = self.eval(target_expr, env)?;
                    if matches!(items.first(), Some(Value::Symbol(name)) if name == "car") {
                        target.set_car(value)?;
                    } else {
                        target.set_cdr(value)?;
                    }
                    Ok(())
                } else if matches!(items.first(), Some(Value::Symbol(name)) if name == "overlay-get")
                {
                    let Some(overlay_expr) = items.get(1) else {
                        return Err(LispError::Signal("Unsupported setf place".into()));
                    };
                    let Some(prop_expr) = items.get(2) else {
                        return Err(LispError::Signal("Unsupported setf place".into()));
                    };
                    let overlay = self.eval(overlay_expr, env)?;
                    let overlay_id = match overlay {
                        Value::Overlay(id) => id,
                        other => {
                            return Err(LispError::TypeError("overlay".into(), other.type_name()));
                        }
                    };
                    let prop = self.eval(prop_expr, env)?;
                    let prop_name = prop.as_symbol()?.to_string();
                    let Some(existing) = self.find_overlay_mut(overlay_id) else {
                        return Err(LispError::TypeError(
                            "overlay".into(),
                            format!("overlay<{overlay_id}>"),
                        ));
                    };
                    existing.put_prop(&prop_name, value);
                    Ok(())
                } else if matches!(items.first(), Some(Value::Symbol(name)) if name == "get") {
                    let Some(symbol_expr) = items.get(1) else {
                        return Err(LispError::Signal("Unsupported setf place".into()));
                    };
                    let Some(prop_expr) = items.get(2) else {
                        return Err(LispError::Signal("Unsupported setf place".into()));
                    };
                    let symbol = self.eval(symbol_expr, env)?.as_symbol()?.to_string();
                    let property = self.eval(prop_expr, env)?.as_symbol()?.to_string();
                    self.put_symbol_property(&symbol, &property, value);
                    Ok(())
                } else if matches!(
                    items.first(),
                    Some(Value::Symbol(name)) if name == "terminal-parameter"
                ) {
                    let Some(terminal_expr) = items.get(1) else {
                        return Err(LispError::Signal("Unsupported setf place".into()));
                    };
                    let Some(parameter_expr) = items.get(2) else {
                        return Err(LispError::Signal("Unsupported setf place".into()));
                    };
                    let terminal = self.eval(terminal_expr, env)?;
                    let parameter = self.eval(parameter_expr, env)?;
                    primitives::call(
                        self,
                        "set-terminal-parameter",
                        &[terminal, parameter, value],
                        env,
                    )?;
                    Ok(())
                } else if matches!(items.first(), Some(Value::Symbol(name)) if name == "alist-get")
                {
                    let value_expr = quoted_literal(&value);
                    self.sf_setf_alist_get(&items, &value_expr, env).map(|_| ())
                } else if matches!(
                    items.first(),
                    Some(Value::Symbol(name)) if matches!(name.as_str(), "plist-get" | "cl-getf")
                ) {
                    let value_expr = quoted_literal(&value);
                    self.sf_setf_plist_get(&items, &value_expr, env).map(|_| ())
                } else if matches!(
                    items.first(),
                    Some(Value::Symbol(name)) if decoded_time_accessor_index(name).is_some()
                ) {
                    let accessor = items[0].as_symbol().expect("checked symbol").to_string();
                    let value_expr = quoted_literal(&value);
                    self.sf_setf_decoded_time_accessor(&accessor, &items, &value_expr, env)
                        .map(|_| ())
                } else if matches!(items.first(), Some(Value::Symbol(name)) if name == "nth") {
                    let value_expr = quoted_literal(&value);
                    self.sf_setf_nth(&items, &value_expr, env).map(|_| ())
                } else if matches!(items.first(), Some(Value::Symbol(name)) if name == "nthcdr") {
                    let Some(index_expr) = items.get(1) else {
                        return Err(LispError::Signal("Unsupported setf place".into()));
                    };
                    let Some(list_expr) = items.get(2) else {
                        return Err(LispError::Signal("Unsupported setf place".into()));
                    };
                    let index = self.eval(index_expr, env)?.as_integer()?;
                    if index <= 0 {
                        return self.set_setf_place_value(list_expr, value, env);
                    }
                    let mut current = self.eval(list_expr, env)?;
                    for _ in 1..index {
                        current = current.cdr()?;
                    }
                    current.set_cdr(value)
                } else if matches!(items.first(), Some(Value::Symbol(name)) if name == "aref") {
                    let value_expr = quoted_literal(&value);
                    self.sf_setf_aref(&items, &value_expr, env).map(|_| ())
                } else if matches!(items.first(), Some(Value::Symbol(name)) if name == "image-property")
                {
                    let value_expr = quoted_literal(&value);
                    self.sf_setf_image_property(&items, &value_expr, env)
                        .map(|_| ())
                } else if matches!(items.first(), Some(Value::Symbol(name)) if name == "gethash") {
                    let Some(key_expr) = items.get(1) else {
                        return Err(LispError::Signal("Unsupported setf place".into()));
                    };
                    let Some(table_expr) = items.get(2) else {
                        return Err(LispError::Signal("Unsupported setf place".into()));
                    };
                    let key = self.eval(key_expr, env)?;
                    let table = self.eval(table_expr, env)?;
                    primitives::call(self, "puthash", &[key, value, table], env)?;
                    Ok(())
                } else {
                    Err(LispError::Signal("Unsupported setf place".into()))
                }
            }
            _ => Err(LispError::Signal("Unsupported setf place".into())),
        }
    }

    pub(super) fn sf_setq_internal(
        &mut self,
        items: &[Value],
        env: &mut Env,
        local_only: bool,
    ) -> Result<Value, LispError> {
        let mut result = Value::Nil;
        let mut i = 1;
        while i + 1 < items.len() {
            let name = assignment_target_name(&items[i])?;
            let resolved = self.resolve_variable_name(&name)?;
            let evaluated = self.eval(&items[i + 1], env)?;
            let val = self.prepare_variable_assignment(&resolved, evaluated)?;
            result = val.clone();
            if local_only {
                self.notify_variable_watchers(
                    &resolved,
                    val.clone(),
                    "set",
                    Some(self.current_buffer_id()),
                    env,
                )?;
                self.set_buffer_local_value(self.current_buffer_id(), &resolved, val);
            } else {
                let buffer_id = self.assignment_buffer_id(&resolved);
                self.notify_variable_watchers(&resolved, val.clone(), "set", buffer_id, env)?;
                self.set_variable(&resolved, val, env);
            }
            i += 2;
        }
        Ok(result)
    }

    pub(super) fn sf_defvar(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Ok(Value::Nil);
        }
        let name = items[1].as_symbol()?.to_string();
        let resolved = self.resolve_variable_name(&name)?;
        let is_defcustom =
            matches!(items.first(), Some(Value::Symbol(symbol)) if symbol == "defcustom");
        if is_defcustom {
            self.record_defcustom_properties(&resolved, items, env)?;
        }
        self.mark_special_variable(&resolved);
        if matches!(items.first(), Some(Value::Symbol(symbol)) if symbol == "defconst") {
            if items.len() > 2 {
                let val = self.eval(&items[2], env)?;
                self.set_default_toplevel_value(&resolved, val);
            }
            return Ok(Value::Nil);
        }
        // Bare `defvar` declarations mark a variable special without binding it.
        if self.default_toplevel_value(&resolved).is_none() && items.len() > 2 {
            let (val, used_stashed_theme_value) = if is_defcustom {
                let standard = self.eval(&items[2], env)?;
                if self
                    .get_symbol_property(&resolved, "standard-value")
                    .is_none()
                {
                    self.put_symbol_property(
                        &resolved,
                        "standard-value",
                        Value::list([quoted_literal(&standard)]),
                    );
                }
                if let Some(saved) = self
                    .get_symbol_property(&resolved, "saved-value")
                    .and_then(|value| value.to_vec().ok())
                    .and_then(|items| items.first().cloned())
                {
                    (self.eval(&saved, env)?, true)
                } else {
                    (standard, false)
                }
            } else {
                (self.eval(&items[2], env)?, false)
            };
            self.set_default_toplevel_value(&resolved, val);
            if self
                .get_symbol_property(&resolved, "standard-value")
                .is_none()
            {
                let stored = self.lookup_var(&resolved, env).unwrap_or(Value::Nil);
                self.put_symbol_property(
                    &resolved,
                    "standard-value",
                    Value::list([quoted_literal(&stored)]),
                );
            }
            if used_stashed_theme_value && self.has_non_user_theme_value(&resolved) {
                self.put_symbol_property(&resolved, "saved-value", Value::Nil);
            }
        }
        Ok(Value::Nil)
    }

    pub(super) fn record_defcustom_properties(
        &mut self,
        symbol: &str,
        items: &[Value],
        env: &mut Env,
    ) -> Result<(), LispError> {
        let mut index = if matches!(
            items.get(3),
            Some(Value::String(_) | Value::StringObject(_))
        ) {
            4
        } else {
            3
        };
        while index + 1 < items.len() {
            let Some(keyword) = items[index].as_symbol().ok() else {
                break;
            };
            if !keyword.starts_with(':') {
                break;
            }
            match keyword {
                ":set" => {
                    let setter = self.eval(&items[index + 1], env)?;
                    self.put_symbol_property(symbol, "custom-set", setter);
                }
                ":type" => {
                    let custom_type = self.eval(&items[index + 1], env)?;
                    self.put_symbol_property(symbol, "custom-type", custom_type);
                }
                ":version" => {
                    self.put_symbol_property(symbol, "custom-version", items[index + 1].clone());
                }
                ":package-version" => {
                    self.put_symbol_property(
                        symbol,
                        "custom-package-version",
                        items[index + 1].clone(),
                    );
                }
                ":group" => {
                    let group = self.eval(&items[index + 1], env)?;
                    if let Ok(group) = group.as_symbol() {
                        crate::lisp::primitives::custom_add_to_group(
                            self,
                            group,
                            Value::Symbol(symbol.to_string()),
                            Value::Symbol("custom-variable".into()),
                        );
                    }
                }
                ":local" => {
                    let value = self.eval(&items[index + 1], env)?;
                    let is_permanent =
                        matches!(&value, Value::Symbol(local) if local == "permanent");
                    if matches!(value, Value::T) || is_permanent {
                        self.mark_auto_buffer_local(symbol);
                    }
                    if is_permanent {
                        self.put_symbol_property(symbol, "permanent-local", Value::T);
                    }
                }
                _ => {}
            }
            index += 2;
        }
        Ok(())
    }

    fn has_non_user_theme_value(&self, symbol: &str) -> bool {
        let Some(theme_value) = self.get_symbol_property(symbol, "theme-value") else {
            return false;
        };
        let Ok(entries) = theme_value.to_vec() else {
            return false;
        };
        let Some(first) = entries.first() else {
            return false;
        };
        let Ok(items) = first.to_vec() else {
            return false;
        };
        !matches!(items.first(), Some(Value::Symbol(theme)) if theme == "user")
    }

    pub(super) fn sf_defvar_local(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() >= 2 {
            self.mark_auto_buffer_local(items[1].as_symbol()?);
        }
        self.sf_defvar(items, env)
    }

    pub(super) fn sf_defgroup(&mut self, items: &[Value]) -> Result<Value, LispError> {
        let Some(Value::Symbol(name)) = items.get(1) else {
            return Ok(Value::Nil);
        };

        if let Some(doc) = items.get(3)
            && let Some(text) = match doc {
                Value::String(text) => Some(text.clone()),
                Value::StringObject(shared) => Some(shared.borrow().text.clone()),
                _ => None,
            }
        {
            self.put_symbol_property(name, "group-documentation", Value::String(text));
        }

        if let Some(members) = items.get(2)
            && let Ok(entries) = members.to_vec()
        {
            for entry in entries {
                let Ok(parts) = entry.to_vec() else {
                    continue;
                };
                if parts.len() < 2 {
                    continue;
                }
                crate::lisp::primitives::custom_add_to_group(
                    self,
                    name,
                    parts[0].clone(),
                    parts[1].clone(),
                );
            }
        }

        let mut index = 4usize;
        while index + 1 < items.len() {
            if let Value::Symbol(keyword) = &items[index] {
                match keyword.as_str() {
                    ":prefix" => {
                        self.put_symbol_property(name, "custom-prefix", items[index + 1].clone());
                    }
                    ":version" => {
                        self.put_symbol_property(name, "custom-version", items[index + 1].clone());
                    }
                    ":package-version" => {
                        self.put_symbol_property(
                            name,
                            "custom-package-version",
                            items[index + 1].clone(),
                        );
                    }
                    ":group" => {
                        if let Some(group) = quoted_symbol_name(&items[index + 1]) {
                            crate::lisp::primitives::custom_add_to_group(
                                self,
                                &group,
                                Value::Symbol(name.clone()),
                                Value::Symbol("custom-group".into()),
                            );
                        }
                    }
                    _ => {}
                }
            }
            index += 2;
        }

        crate::lisp::primitives::custom_set_current_group(self, name);
        Ok(Value::Symbol(name.clone()))
    }

    pub(super) fn sf_defface(&mut self, items: &[Value]) -> Result<Value, LispError> {
        let Some(name) = items.get(1).and_then(|value| value.as_symbol().ok()) else {
            return Ok(Value::Nil);
        };
        if let Some(spec) = items.get(2) {
            self.put_symbol_property(name, "face-defface-spec", spec.clone());
            self.put_symbol_property(name, "face-modified", Value::Nil);
            if let Some(doc) = items.get(3)
                && matches!(doc, Value::String(_) | Value::StringObject(_))
            {
                self.put_symbol_property(name, "face-documentation", doc.clone());
            }
            self.record_defface_runtime_attributes(name, spec)?;
        }
        Ok(Value::Symbol(name.to_string()))
    }

    pub(super) fn record_defface_runtime_attributes(
        &mut self,
        face: &str,
        spec_form: &Value,
    ) -> Result<(), LispError> {
        let Some(spec) = defface_spec_literal(spec_form) else {
            return Ok(());
        };
        let Some(attributes) = defface_runtime_attributes(&spec) else {
            return Ok(());
        };

        for (attribute, value) in attributes {
            if attribute == ":inherit" {
                match &value {
                    Value::Nil => self.set_face_inherit_target(face, None)?,
                    Value::Symbol(symbol) => {
                        self.set_face_inherit_target(face, Some(symbol.clone()))?
                    }
                    _ => {}
                }
            }
            self.put_symbol_property(
                face,
                &crate::lisp::primitives::face_attribute_property_name(&attribute),
                value,
            );
        }
        Ok(())
    }

    pub(super) fn sf_defvar_keymap(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Ok(Value::Nil);
        }
        let name = items[1].as_symbol()?.to_string();
        let resolved = self.resolve_variable_name(&name)?;
        self.mark_special_variable(&resolved);
        if self.lookup(&resolved, env).is_ok() {
            return Ok(Value::Nil);
        }

        let keymap = crate::lisp::primitives::make_runtime_keymap(self, Some(&resolved));
        let mut index = 2;
        let mut seen_keys = HashSet::new();
        while index + 1 < items.len() {
            if matches!(&items[index], Value::Symbol(keyword) if keyword.starts_with(':')) {
                index += 2;
                continue;
            }

            let duplicate_key = items[index].to_string();
            if !seen_keys.insert(duplicate_key.clone()) {
                return Err(LispError::Signal(format!(
                    "Duplicate definition for key '{}' in keymap '{}'",
                    duplicate_key, resolved
                )));
            }

            let key = match self.eval(&items[index], env)? {
                Value::String(text) => text,
                Value::StringObject(state) => state.borrow().text.clone(),
                other => {
                    return Err(LispError::TypeError("string".into(), other.type_name()));
                }
            };
            let definition = self.eval(&items[index + 1], env)?;
            crate::lisp::primitives::keymap_define_binding(self, &keymap, &key, definition)?;
            index += 2;
        }

        if let Some(existing) = self
            .globals
            .iter_mut()
            .rposition(|(symbol, _)| symbol == &resolved)
        {
            self.globals[existing].1 = Self::stored_value(keymap);
        } else {
            self.globals.push((resolved, Self::stored_value(keymap)));
        }
        Ok(Value::Nil)
    }

    pub(super) fn sf_define_mode(&mut self, items: &[Value]) -> Result<Value, LispError> {
        let Some(name) = items.get(1).and_then(|value| value.as_symbol().ok()) else {
            return Ok(Value::Nil);
        };
        if let Some(Value::Symbol(kind)) = items.first() {
            if kind == "define-minor-mode" {
                let mut init_value = Value::Nil;
                let mut global = false;
                let mut variable_name = name.to_string();
                let mut index = if matches!(
                    items.get(2),
                    Some(Value::String(_) | Value::StringObject(_))
                ) {
                    3
                } else {
                    2
                };

                while index + 1 < items.len() {
                    let Some(keyword) = items[index].as_symbol().ok() else {
                        break;
                    };
                    if !keyword.starts_with(':') {
                        break;
                    }
                    match keyword {
                        ":init-value" => init_value = items[index + 1].clone(),
                        ":global" => global = items[index + 1].is_truthy(),
                        ":variable" => {
                            if let Some((variable, _setter)) = items[index + 1].cons_values()
                                && let Ok(variable) = variable.as_symbol()
                            {
                                variable_name = variable.to_string();
                            }
                        }
                        _ => {}
                    }
                    index += 2;
                }

                self.mark_special_variable(&variable_name);
                let init_value_truthy = init_value.is_truthy();
                if !global {
                    self.mark_auto_buffer_local(&variable_name);
                }
                if self.lookup_var(&variable_name, &Vec::new()).is_none() {
                    self.globals
                        .push((variable_name.clone(), Self::stored_value(init_value)));
                }
                if let Some(map) = self.lookup_var(&format!("{name}-map"), &Vec::new()) {
                    let entry = Value::cons(Value::Symbol(name.to_string()), map);
                    let mut entries = self
                        .lookup_var("minor-mode-map-alist", &Vec::new())
                        .unwrap_or(Value::Nil)
                        .to_vec()
                        .unwrap_or_default();
                    if let Some(index) = entries.iter().position(|existing| {
                        existing
                            .cons_values()
                            .is_some_and(|(mode, _)| mode == Value::Symbol(name.to_string()))
                    }) {
                        entries[index] = entry;
                    } else {
                        entries.push(entry);
                    }
                    self.set_global_binding("minor-mode-map-alist", Value::list(entries));
                }

                let setter_symbol = if global { "setq-default" } else { "setq" };
                let current_mode_form = if global {
                    Value::list([
                        Value::Symbol("default-value".into()),
                        Value::list([
                            Value::Symbol("quote".into()),
                            Value::Symbol(variable_name.clone()),
                        ]),
                    ])
                } else {
                    Value::Symbol(variable_name.clone())
                };
                let toggle_form = Value::list([
                    Value::Symbol("if".into()),
                    Value::list([
                        Value::Symbol("eq".into()),
                        Value::Symbol("arg".into()),
                        Value::list([
                            Value::Symbol("quote".into()),
                            Value::Symbol("toggle".into()),
                        ]),
                    ]),
                    Value::list([Value::Symbol("not".into()), current_mode_form.clone()]),
                    Value::list([
                        Value::Symbol("if".into()),
                        Value::list([Value::Symbol("not".into()), Value::Symbol("arg".into())]),
                        Value::T,
                        Value::list([
                            Value::Symbol("if".into()),
                            Value::list([
                                Value::Symbol("integerp".into()),
                                Value::Symbol("arg".into()),
                            ]),
                            Value::list([
                                Value::Symbol(">".into()),
                                Value::Symbol("arg".into()),
                                Value::Integer(0),
                            ]),
                            Value::T,
                        ]),
                    ]),
                ]);

                let mut body = vec![Value::list([
                    Value::Symbol(setter_symbol.into()),
                    Value::Symbol(variable_name.clone()),
                    toggle_form,
                ])];
                body.extend_from_slice(&items[index..]);
                body.push(Value::Symbol(variable_name));

                self.set_function_binding(
                    name,
                    Some(Value::Lambda(
                        vec!["&optional".into(), "arg".into()],
                        body,
                        shared_env(Vec::new()),
                    )),
                );
                if global && init_value_truthy && name == "electric-indent-mode" {
                    self.call_function_value(
                        Value::Symbol(name.to_string()),
                        Some(name),
                        &[Value::Integer(1)],
                        &mut Vec::new(),
                    )?;
                }
                return Ok(Value::Symbol(name.to_string()));
            }

            if kind == "define-globalized-minor-mode" {
                self.mark_special_variable(name);
                if self.lookup_var(name, &Vec::new()).is_none() {
                    self.globals
                        .push((name.to_string(), Self::stored_value(Value::Nil)));
                }
            }

            if kind == "define-derived-mode" {
                let parent = items.get(2).and_then(|value| match value {
                    Value::Symbol(symbol) => Some(symbol.as_str()),
                    Value::Nil => None,
                    _ => None,
                });
                let map_name = format!("{name}-map");
                if self.lookup_var(&map_name, &Vec::new()).is_none() {
                    self.globals.push((
                        map_name.clone(),
                        Self::stored_value(crate::lisp::primitives::keymap_placeholder(Some(
                            &map_name,
                        ))),
                    ));
                }
                let mut index = 4;
                if matches!(
                    items.get(index),
                    Some(Value::String(_) | Value::StringObject(_))
                ) {
                    index += 1;
                }
                let mut after_hook = None;
                while let Some(Value::Symbol(keyword)) = items.get(index)
                    && keyword.starts_with(':')
                {
                    if keyword == ":after-hook" {
                        after_hook = items.get(index + 1).cloned();
                    }
                    index += 2;
                }
                let mut delayed_body = Vec::new();
                if let Some(parent) = parent {
                    delayed_body.push(Value::list([Value::Symbol(parent.to_string())]));
                }
                delayed_body.push(Value::list([
                    Value::Symbol("use-local-map".into()),
                    Value::Symbol(map_name.clone()),
                ]));
                delayed_body.push(Value::list([
                    Value::Symbol("setq-local".into()),
                    Value::Symbol("major-mode".into()),
                    Value::list([
                        Value::Symbol("quote".into()),
                        Value::Symbol(name.to_string()),
                    ]),
                ]));
                if !matches!(items.get(3), None | Some(Value::Nil)) {
                    delayed_body.push(Value::list([
                        Value::Symbol("setq-local".into()),
                        Value::Symbol("mode-name".into()),
                        items[3].clone(),
                    ]));
                }
                delayed_body.extend_from_slice(&items[index..]);
                let mut body = vec![Value::list(
                    std::iter::once(Value::Symbol("delay-mode-hooks".into()))
                        .chain(delayed_body)
                        .collect::<Vec<_>>(),
                )];
                if let Some(after_hook) = after_hook {
                    body.push(Value::list([
                        Value::Symbol("push".into()),
                        Value::list([
                            Value::Symbol("lambda".into()),
                            Value::Nil,
                            Value::Symbol(":closure-isolated-current-env".into()),
                            after_hook,
                        ]),
                        Value::Symbol("delayed-after-hook-functions".into()),
                    ]));
                }
                body.push(Value::list([
                    Value::Symbol("run-mode-hooks".into()),
                    Value::list([
                        Value::Symbol("quote".into()),
                        Value::Symbol(format!("{name}-hook")),
                    ]),
                ]));
                body.push(Value::list([
                    Value::Symbol("quote".into()),
                    Value::Symbol(name.to_string()),
                ]));
                self.set_function_binding(
                    name,
                    Some(Value::Lambda(Vec::new(), body, shared_env(Vec::new()))),
                );
                crate::lisp::primitives::derived_mode_set_parent(self, name, parent);
                return Ok(Value::Symbol(name.to_string()));
            }
        }
        if self.lookup_function(name, &Vec::new()).is_err() {
            self.set_function_binding(name, Some(Value::BuiltinFunc("ignore".into())));
        }
        Ok(Value::Symbol(name.to_string()))
    }

    pub(super) fn sf_cl_defstruct(&mut self, items: &[Value]) -> Result<Value, LispError> {
        let Some(struct_spec) = items.get(1) else {
            return Ok(Value::Nil);
        };
        let (name, options) = match struct_spec {
            Value::Symbol(name) => (name.clone(), Vec::new()),
            Value::Cons(_, _) => {
                let Some(parts) = struct_spec.to_vec().ok() else {
                    return Ok(Value::Nil);
                };
                let Some(name) = parts
                    .first()
                    .and_then(|value| value.as_symbol().ok())
                    .map(str::to_string)
                else {
                    return Ok(Value::Nil);
                };
                (name, parts[1..].to_vec())
            }
            _ => return Ok(Value::Nil),
        };

        // `:include' prepends the parent's slots so accessor indexes stay
        // aligned across the inheritance chain.
        let mut slot_specs: Vec<(String, Value)> = Vec::new();
        for option in &options {
            if let Ok(parts) = option.to_vec()
                && matches!(parts.first(), Some(Value::Symbol(keyword)) if keyword == ":include")
                && let Some(Value::Symbol(parent)) = parts.get(1)
                && let Some(parent_slots) = self.get_symbol_property(parent, "emaxx-struct-slots")
                && let Ok(parent_names) = parent_slots.to_vec()
            {
                for slot in parent_names {
                    if let Value::Symbol(slot_name) = slot {
                        slot_specs.push((slot_name, Value::Nil));
                    }
                }
            }
        }
        slot_specs.extend(items[2..].iter().filter_map(|slot| match slot {
            Value::Symbol(name) => Some((name.clone(), Value::Nil)),
            Value::Cons(_, _) => slot.to_vec().ok().and_then(|parts| {
                let name = parts
                    .first()
                    .and_then(|value| value.as_symbol().ok().map(str::to_string))?;
                let default = parts.get(1).cloned().unwrap_or(Value::Nil);
                Some((name, default))
            }),
            _ => None,
        }));
        let slot_names = slot_specs
            .iter()
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>();
        let slot_defaults = slot_specs
            .iter()
            .map(|(_, default)| default.clone())
            .collect::<Vec<_>>();

        let mut constructors = Vec::new();
        let mut suppress_default_constructor = false;
        let mut conc_name = format!("{name}-");
        let mut predicate_name = format!("{name}-p");
        let mut parent_names = Vec::new();
        let mut list_backed = false;
        for option in options {
            let Some(parts) = option.to_vec().ok() else {
                continue;
            };
            match parts.first() {
                Some(Value::Symbol(keyword)) if keyword == ":include" => {
                    if let Some(Value::Symbol(parent_name)) = parts.get(1) {
                        parent_names.push(parent_name.clone());
                    }
                }
                Some(Value::Symbol(keyword)) if keyword == ":constructor" => match parts.get(1) {
                    Some(Value::Nil) => suppress_default_constructor = true,
                    Some(Value::Symbol(constructor_name)) => {
                        let (params, aux_bindings) = parts
                            .get(2)
                            .and_then(|value| value.to_vec().ok())
                            .map(parse_cl_defstruct_constructor_params)
                            .unwrap_or_else(|| {
                                (
                                    std::iter::once("&key".to_string())
                                        .chain(slot_names.iter().cloned())
                                        .collect::<Vec<_>>(),
                                    Vec::new(),
                                )
                            });
                        constructors.push((constructor_name.clone(), params, aux_bindings));
                    }
                    _ => {}
                },
                Some(Value::Symbol(keyword)) if keyword == ":predicate" => match parts.get(1) {
                    Some(Value::Nil) => predicate_name.clear(),
                    Some(Value::Symbol(predicate)) => predicate_name = predicate.clone(),
                    _ => {}
                },
                Some(Value::Symbol(keyword)) if keyword == ":type" => {
                    if matches!(parts.get(1), Some(Value::Symbol(kind)) if kind == "list") {
                        list_backed = true;
                    }
                }
                Some(Value::Symbol(keyword)) if keyword == ":conc-name" => match parts.get(1) {
                    Some(Value::Nil) => conc_name.clear(),
                    Some(Value::Symbol(prefix)) => conc_name = prefix.clone(),
                    Some(Value::String(prefix)) => conc_name = prefix.clone(),
                    Some(Value::StringObject(prefix)) => conc_name = prefix.borrow().text.clone(),
                    _ => {}
                },
                _ => {}
            }
        }
        let default_constructor_name = format!("make-{name}");
        if !suppress_default_constructor
            && !constructors
                .iter()
                .any(|(constructor_name, _, _)| constructor_name == &default_constructor_name)
        {
            constructors.push((
                default_constructor_name,
                std::iter::once("&key".to_string())
                    .chain(slot_names.iter().cloned())
                    .collect::<Vec<_>>(),
                Vec::new(),
            ));
        }

        let struct_name = Value::list([Value::Symbol("quote".into()), Value::Symbol(name.clone())]);
        let slot_names_list = Value::list(
            slot_names
                .iter()
                .cloned()
                .map(Value::Symbol)
                .collect::<Vec<_>>(),
        );
        let slot_names_value =
            Value::list([Value::Symbol("quote".into()), slot_names_list.clone()]);
        let slot_defaults_value =
            Value::list([Value::Symbol("quote".into()), Value::list(slot_defaults)]);

        self.put_symbol_property(&name, "emaxx-struct-slots", slot_names_list.clone());
        self.register_class(
            &name,
            parent_names,
            slot_names.iter().cloned().map(Value::Symbol).collect(),
            Vec::new(),
        );

        if !predicate_name.is_empty() {
            self.set_function_binding(
                &predicate_name,
                Some(Value::Lambda(
                    vec!["object".into()],
                    vec![Value::list([
                        Value::Symbol("emaxx-struct-p".into()),
                        struct_name.clone(),
                        Value::Symbol("object".into()),
                    ])],
                    shared_env(Vec::new()),
                )),
            );
        }

        self.set_function_binding(
            &format!("copy-{name}"),
            Some(Value::Lambda(
                vec!["object".into()],
                vec![Value::list([
                    Value::Symbol("copy-sequence".into()),
                    Value::Symbol("object".into()),
                ])],
                shared_env(Vec::new()),
            )),
        );

        for (index, slot_name) in slot_names.iter().enumerate() {
            let accessor_name = format!("{conc_name}{slot_name}");
            self.put_symbol_property(
                &accessor_name,
                "emaxx-struct-type",
                Value::Symbol(name.clone()),
            );
            self.put_symbol_property(
                &accessor_name,
                "emaxx-struct-slot",
                Value::Integer(index as i64),
            );
            self.set_function_binding(
                &accessor_name,
                Some(Value::Lambda(
                    vec!["object".into()],
                    vec![Value::list([
                        Value::Symbol("emaxx-struct-ref".into()),
                        struct_name.clone(),
                        Value::Integer(index as i64),
                        Value::Symbol("object".into()),
                        if list_backed { Value::T } else { Value::Nil },
                    ])],
                    shared_env(Vec::new()),
                )),
            );
        }

        for (constructor_name, params, aux_bindings) in constructors {
            self.put_symbol_property(
                &constructor_name,
                "emaxx-function-arglist",
                Value::list(params.iter().cloned().map(Value::Symbol)),
            );
            let params_for_make = if aux_bindings.is_empty() {
                params.clone()
            } else {
                params
                    .iter()
                    .cloned()
                    .chain(std::iter::once("&key".to_string()))
                    .chain(slot_names.iter().cloned())
                    .collect::<Vec<_>>()
            };
            let params_list = Value::list(params_for_make.into_iter().map(Value::Symbol));
            let params_value = Value::list([Value::Symbol("quote".into()), params_list]);
            let call_args = if aux_bindings.is_empty() {
                Value::Symbol("args".into())
            } else {
                let aux_keywords = aux_bindings
                    .iter()
                    .flat_map(|(name, _)| {
                        [
                            Value::Symbol(format!(":{name}")),
                            Value::Symbol(name.clone()),
                        ]
                    })
                    .collect::<Vec<_>>();
                Value::list([
                    Value::Symbol("append".into()),
                    Value::Symbol("args".into()),
                    Value::list(
                        std::iter::once(Value::Symbol("list".into()))
                            .chain(aux_keywords)
                            .collect::<Vec<_>>(),
                    ),
                ])
            };
            let make_form = Value::list([
                Value::Symbol("emaxx-struct-make".into()),
                struct_name.clone(),
                slot_names_value.clone(),
                slot_defaults_value.clone(),
                params_value,
                call_args,
            ]);
            let body = if aux_bindings.is_empty() {
                make_form
            } else {
                let let_bindings = Value::list(cl_defstruct_constructor_aux_let_bindings(
                    &params,
                    aux_bindings,
                ));
                Value::list([Value::Symbol("let*".into()), let_bindings, make_form])
            };
            self.set_function_binding(
                &constructor_name,
                Some(Value::Lambda(
                    vec!["&rest".into(), "args".into()],
                    vec![body],
                    shared_env(Vec::new()),
                )),
            );
        }

        Ok(Value::Symbol(name))
    }

    pub(super) fn sf_incf(
        &mut self,
        items: &[Value],
        env: &mut Env,
        sign: i64,
    ) -> Result<Value, LispError> {
        if items.len() < 2 || items.len() > 3 {
            return Err(LispError::WrongNumberOfArgs(
                if sign >= 0 {
                    "incf".into()
                } else {
                    "decf".into()
                },
                items.len().saturating_sub(1),
            ));
        }
        let delta = if let Some(amount) = items.get(2) {
            self.eval(amount, env)?
        } else {
            Value::Integer(1)
        };
        if let Ok(name) = items[1].as_symbol() {
            let name = name.to_string();
            let current = self.lookup(&name, env)?;
            let updated = primitives::call(
                self,
                if sign >= 0 { "+" } else { "-" },
                &[current, delta],
                env,
            )?;
            self.set_variable(&name, updated.clone(), env);
            return Ok(updated);
        }

        let place = self.resolve_setf_place(&items[1], env)?;
        let current = self.eval_resolved_setf_place_current_value(&place, env)?;
        let updated = primitives::call(
            self,
            if sign >= 0 { "+" } else { "-" },
            &[current, delta],
            env,
        )?;
        self.set_resolved_setf_place_value(&place, updated.clone(), env)?;
        Ok(updated)
    }

    pub(super) fn sf_cl_callf(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Err(LispError::WrongNumberOfArgs(
                "cl-callf".into(),
                items.len().saturating_sub(1),
            ));
        }
        let function = match &items[1] {
            Value::Symbol(name) => self.lookup_function(name, env)?,
            other => self.eval(other, env)?,
        };
        let place = self.resolve_setf_place(&items[2], env)?;
        let mut args = Vec::with_capacity(items.len() - 2);
        args.push(self.eval_resolved_setf_place_current_value(&place, env)?);
        for expr in &items[3..] {
            args.push(self.eval(expr, env)?);
        }
        let updated = self.call_function_value(function, items[1].as_symbol().ok(), &args, env)?;
        self.set_resolved_setf_place_value(&place, updated.clone(), env)?;
        Ok(updated)
    }

    pub(super) fn sf_add_to_list(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Err(LispError::WrongNumberOfArgs(
                "add-to-list".into(),
                items.len().saturating_sub(1),
            ));
        }

        let place = quoted_symbol_name(&items[1])
            .ok_or_else(|| LispError::TypeError("symbol".into(), unquote(&items[1]).type_name()))?;
        let value = self.eval(&items[2], env)?;
        let append = if items.len() > 3 {
            self.eval(&items[3], env)?.is_truthy()
        } else {
            false
        };
        if items.len() > 4 {
            // Emacs accepts an optional comparison function. We don't have distinct
            // eq/equal semantics yet, but evaluating it still preserves load-time
            // errors from invalid comparator expressions.
            let _ = self.eval(&items[4], env)?;
        }

        let current = self.lookup_var(&place, env).unwrap_or(Value::Nil);
        let mut values = current.to_vec()?;
        if values
            .iter()
            .any(|existing| crate::lisp::primitives::values_equal(self, existing, &value))
        {
            return Ok(current);
        }

        if append {
            values.push(value);
        } else {
            values.insert(0, value);
        }
        let updated = Value::list(values);
        self.set_variable(&place, updated.clone(), env);
        Ok(updated)
    }

    pub(super) fn sf_cl_pushnew(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Err(LispError::WrongNumberOfArgs(
                "cl-pushnew".into(),
                items.len().saturating_sub(1),
            ));
        }

        let new_value = self.eval(&items[1], env)?;
        let mut testfn = None;
        let mut test_not = None;
        let mut keyfn = None;
        let mut cursor = 3usize;
        while cursor < items.len() {
            let Some(keyword) = items[cursor].as_symbol().ok() else {
                return Err(LispError::Signal("Unsupported cl-pushnew syntax".into()));
            };
            let Some(value_expr) = items.get(cursor + 1) else {
                return Err(LispError::Signal("Unsupported cl-pushnew syntax".into()));
            };
            let value = self.eval(value_expr, env)?;
            match keyword {
                ":test" => testfn = Some(value),
                ":test-not" => test_not = Some(value),
                ":key" => keyfn = Some(value),
                _ => return Err(LispError::Signal("Unsupported cl-pushnew syntax".into())),
            }
            cursor += 2;
        }

        let place = self.resolve_setf_place(&items[2], env)?;
        let current = self.eval_resolved_setf_place_current_value(&place, env)?;
        let values = current.to_vec()?;
        let keyed_new = self.apply_cl_sequence_key(keyfn.as_ref(), &new_value, env)?;
        let mut already_present = false;
        for existing in &values {
            let keyed_existing = self.apply_cl_sequence_key(keyfn.as_ref(), existing, env)?;
            let matches = if let Some(predicate) = test_not.as_ref() {
                !self.call_binary_predicate(predicate, &keyed_new, &keyed_existing, env)?
            } else {
                primitives::value_matches_with_test(
                    self,
                    &keyed_new,
                    &keyed_existing,
                    testfn.as_ref(),
                    env,
                )?
            };
            if matches {
                already_present = true;
                break;
            }
        }

        if already_present {
            return Ok(current);
        }

        let updated = Value::cons(new_value, current);
        self.set_resolved_setf_place_value(&place, updated.clone(), env)?;
        Ok(updated)
    }

    pub(super) fn sf_defun(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Err(LispError::Signal("defun needs name, params, body".into()));
        }
        let name = items[1].as_symbol()?.to_string();
        let params = self.parse_params(&items[2])?;
        let (docstring, normalized_forms) =
            self.normalize_function_body_documentation(&items[3..], env)?;

        if let Some(docstring) = docstring.clone() {
            self.put_symbol_property(&name, "function-documentation", docstring);
        } else {
            self.remove_symbol_property(&name, "function-documentation");
        }

        record_defun_attributes(self, &name, &normalized_forms);

        // Skip docstring if present
        let body_start = if normalized_forms.len() > 1 {
            if matches!(
                normalized_forms.first(),
                Some(Value::String(_) | Value::StringObject(_))
            ) {
                1
            } else {
                0
            }
        } else {
            0
        };

        if body_start < normalized_forms.len()
            && let Some(setter) = function_declare_gv_setter(&normalized_forms[body_start])
        {
            self.put_symbol_property(&name, "emaxx-gv-setter", Value::Symbol(setter));
        }
        if body_start < normalized_forms.len()
            && let Some(handler) = function_declare_gv_expander(&normalized_forms[body_start])
            && let Ok(handler_items) = handler.to_vec()
            && handler_items.len() >= 3
            && matches!(handler_items.first(), Some(Value::Symbol(head)) if head == "lambda")
            && let Ok(handler_params) = handler_items[1].to_vec()
        {
            // `(declare (gv-expander (lambda (do) ...)))' becomes an expander
            // taking DO plus the function's own arguments, like
            // `gv--defun-declaration'.
            let mut expander_params = handler_params;
            for param in &params {
                expander_params.push(Value::Symbol(param.clone()));
            }
            let expander_form = Value::list(
                std::iter::once(Value::Symbol("lambda".into()))
                    .chain(std::iter::once(Value::list(expander_params)))
                    .chain(handler_items[2..].iter().cloned())
                    .collect::<Vec<_>>(),
            );
            let expander = self.eval(&expander_form, env)?;
            self.put_symbol_property(&name, "gv-expander", expander);
        }
        let body: Vec<Value> = normalized_forms[body_start..].to_vec();
        if crate::lisp::primitives::prefer_builtin_override(&name) {
            self.functions
                .push((name.clone(), Value::BuiltinFunc(name.clone())));
            return Ok(Value::Symbol(name));
        }
        // GNU eagerly macroexpands top-level forms when they are loaded or
        // eval-defun'ed, so an edebug-instrumented `cl-macrolet' runs its
        // (instrumented) local macros while the defun itself is evaluated
        // (Bug#29919). Replicate that for instrumented bodies.
        let body = if body
            .iter()
            .any(|form| value_tree_contains_symbol(form, "edebug-enter"))
            && body
                .iter()
                .any(|form| value_tree_contains_symbol(form, "cl-macrolet"))
        {
            body.iter()
                .map(|form| self.eagerly_expand_cl_macrolet(form, env))
                .collect::<Result<Vec<_>, _>>()?
        } else {
            body
        };
        let lambda = Value::Lambda(params, body, shared_env(env.clone()));
        self.functions.push((name.clone(), lambda));
        Ok(Value::Symbol(name))
    }

    fn eagerly_expand_cl_macrolet(
        &mut self,
        form: &Value,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let Ok(items) = form.to_vec() else {
            return Ok(form.clone());
        };
        if items.is_empty() {
            return Ok(form.clone());
        }
        match items.first() {
            Some(Value::Symbol(head)) if head == "quote" => return Ok(form.clone()),
            Some(Value::Symbol(head)) if head == "cl-macrolet" && items.len() >= 3 => {
                let local_macros = self.parse_cl_macrolet_bindings(&items[1])?;
                let local_start = self.macros.len();
                self.macros.extend(local_macros.iter().cloned());
                let local_count = self.macros.len() - local_start;
                let mut expanded_forms = Vec::with_capacity(items.len());
                expanded_forms.push(Value::Symbol("progn".into()));
                let mut failure = None;
                for body_form in &items[2..] {
                    match self.macroexpand_all_form_with_environment(body_form, None, env) {
                        Ok(expanded) => expanded_forms.push(expanded),
                        Err(error) => {
                            failure = Some(error);
                            break;
                        }
                    }
                }
                self.macros.drain(local_start..local_start + local_count);
                if let Some(error) = failure {
                    return Err(error);
                }
                return Ok(Value::list(expanded_forms));
            }
            _ => {}
        }
        let expanded = items
            .iter()
            .map(|item| self.eagerly_expand_cl_macrolet(item, env))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Value::list(expanded))
    }

    pub(super) fn sf_cl_deftype(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 4 {
            return Err(LispError::Signal(
                "cl-deftype needs name, params, body".into(),
            ));
        }
        let name = items[1].as_symbol()?.to_string();
        let params = self.parse_params(&items[2])?;
        let body = items[3..].to_vec();
        let lambda = Value::Lambda(params, body, shared_env(env.clone()));
        self.put_symbol_property(&name, "emaxx-cl-deftype-handler", lambda);
        Ok(Value::Symbol(name))
    }

    pub(super) fn sf_defclass(&mut self, items: &[Value]) -> Result<Value, LispError> {
        let Some(name) = items.get(1).and_then(|value| value.as_symbol().ok()) else {
            return Ok(Value::Nil);
        };
        let parents = items
            .get(2)
            .map(Value::to_vec)
            .transpose()?
            .unwrap_or_default()
            .into_iter()
            .filter_map(|value| value.as_symbol().ok().map(str::to_string))
            .collect::<Vec<_>>();
        let slot_specs = items
            .get(3)
            .map(Value::to_vec)
            .transpose()?
            .unwrap_or_default();
        let options = items.get(4..).unwrap_or(&[]).to_vec();
        self.register_class(name, parents, slot_specs, options);
        classes::install_eieio_slot_accessors(self, name)?;
        self.set_function_binding(
            name,
            Some(Value::Lambda(
                vec!["&rest".into(), "initargs".into()],
                vec![Value::list([
                    Value::Symbol("emaxx-class-make".into()),
                    Value::list([
                        Value::Symbol("quote".into()),
                        Value::Symbol(name.to_string()),
                    ]),
                    Value::Symbol("initargs".into()),
                ])],
                shared_env(Vec::new()),
            )),
        );
        self.set_function_binding(
            &format!("{name}-p"),
            Some(Value::Lambda(
                vec!["object".into()],
                vec![Value::list([
                    Value::Symbol("emaxx-class-p".into()),
                    Value::list([
                        Value::Symbol("quote".into()),
                        Value::Symbol(name.to_string()),
                    ]),
                    Value::Symbol("object".into()),
                ])],
                shared_env(Vec::new()),
            )),
        );
        Ok(Value::Symbol(name.to_string()))
    }

    pub(super) fn sf_cl_defun(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 4 {
            return Err(LispError::Signal(
                "cl-defun needs name, params, body".into(),
            ));
        }

        let name = items[1].as_symbol()?.to_string();
        let lowered_cl_defun = lower_cl_defun_lambda_list(&name, &items[2])?;

        let original_body = items[3..].to_vec();
        let body_prefix_len = original_body
            .iter()
            .take_while(|form| {
                matches!(form, Value::String(_) | Value::StringObject(_))
                    || is_function_declare_form(form)
                    || is_function_interactive_form(form)
            })
            .count();
        let mut lowered_body = original_body[..body_prefix_len].to_vec();
        let mut executable_body = original_body[body_prefix_len..].to_vec();
        if !lowered_cl_defun.keyword_bindings.is_empty() {
            let mut let_bindings = Vec::new();
            for binding in &lowered_cl_defun.keyword_bindings {
                let present_name =
                    format!("emaxx--cl-defun-{}-{}-present", name, binding.variable_name);
                let keyword_rest_param =
                    lowered_cl_defun.keyword_rest_param.clone().ok_or_else(|| {
                        LispError::Signal("cl-defun keyword lowering lost its rest source".into())
                    })?;
                let keyword_symbol = Value::Symbol(binding.keyword_name.clone());
                let keyword_source = Value::Symbol(keyword_rest_param);
                let_bindings.push(Value::list([
                    Value::Symbol(present_name.clone()),
                    Value::list([
                        Value::Symbol("plist-member".into()),
                        keyword_source.clone(),
                        keyword_symbol.clone(),
                    ]),
                ]));
                let_bindings.push(Value::list([
                    Value::Symbol(binding.variable_name.clone()),
                    Value::list([
                        Value::Symbol("if".into()),
                        Value::Symbol(present_name.clone()),
                        Value::list([
                            Value::Symbol("plist-get".into()),
                            keyword_source.clone(),
                            keyword_symbol,
                        ]),
                        binding.default_value.clone(),
                    ]),
                ]));
                if let Some(supplied_name) = &binding.supplied_name {
                    let_bindings.push(Value::list([
                        Value::Symbol(supplied_name.clone()),
                        Value::list([
                            Value::Symbol("if".into()),
                            Value::Symbol(present_name),
                            Value::T,
                            Value::Nil,
                        ]),
                    ]));
                }
            }
            let mut wrapped = vec![Value::Symbol("let*".into()), Value::list(let_bindings)];
            wrapped.append(&mut executable_body);
            executable_body = vec![Value::list(wrapped)];
        }
        for (pattern, temp_name) in lowered_cl_defun.destructuring_bindings.into_iter().rev() {
            let mut wrapped = vec![
                Value::Symbol("cl-destructuring-bind".into()),
                pattern,
                Value::Symbol(temp_name),
            ];
            wrapped.append(&mut executable_body);
            executable_body = vec![Value::list(wrapped)];
        }
        let mut block_body = vec![
            Value::Symbol("catch".into()),
            quoted_literal(&Value::Symbol(format!("--cl-block-{name}--"))),
        ];
        block_body.append(&mut executable_body);
        lowered_body.push(Value::list(block_body));

        let mut lowered = Vec::with_capacity(3 + lowered_body.len());
        lowered.push(Value::Symbol("defun".into()));
        lowered.push(Value::Symbol(name));
        lowered.push(Value::list(lowered_cl_defun.params));
        lowered.extend(lowered_body);
        self.sf_defun(&lowered, env)
    }

    pub(super) fn sf_cl_defmacro(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 4 {
            return Err(LispError::Signal(
                "cl-defmacro needs name, params, body".into(),
            ));
        }

        let name = items[1].as_symbol()?.to_string();
        if self.lookup_function("cl-copy-list", env).is_err() {
            self.load_target("cl-lib")?;
        }
        if self.lookup_function("cl--transform-lambda", env).is_err() {
            self.load_target("cl-macs")?;
        }
        let transformer = self.lookup_function("cl--transform-lambda", env)?;
        let lambda_form = Value::cons(items[2].clone(), Value::list(items[3..].to_vec()));
        let transformed = self.call_function_value(
            transformer,
            Some("cl--transform-lambda"),
            &[lambda_form, Value::Symbol(name.clone())],
            env,
        )?;
        let mut lowered = Vec::with_capacity(2);
        lowered.push(Value::Symbol("defmacro".into()));
        lowered.push(Value::Symbol(name));
        lowered.extend(transformed.to_vec()?);
        self.sf_defmacro(&lowered)
    }

    pub(super) fn sf_cl_defgeneric(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Err(LispError::Signal(
                "cl-defgeneric needs name and params".into(),
            ));
        }
        let name = function_name_from_binding_form(&items[1])?;
        self.put_symbol_property(&name, "emaxx-cl-defgeneric-lambda-list", items[2].clone());
        let mut body_start = 3;
        if matches!(
            items.get(3),
            Some(Value::String(_) | Value::StringObject(_))
        ) {
            let doc = match &items[3] {
                Value::String(text) => Value::String(text.clone()),
                Value::StringObject(state) => Value::String(state.borrow().text.clone()),
                _ => Value::Nil,
            };
            self.put_symbol_property(&name, "emaxx-cl-defgeneric-documentation", doc.clone());
            self.put_symbol_property(&name, "function-documentation", doc);
            body_start = 4;
        }
        while let Some(form) = items.get(body_start) {
            let Ok(parts) = form.to_vec() else {
                break;
            };
            let Some(Value::Symbol(head)) = parts.first() else {
                break;
            };
            match head.as_str() {
                "declare" => {
                    for declaration in &parts[1..] {
                        let Ok(declaration_parts) = declaration.to_vec() else {
                            continue;
                        };
                        if matches!(
                            declaration_parts.first(),
                            Some(Value::Symbol(kind)) if kind == "advertised-calling-convention"
                        ) && let Some(convention) = declaration_parts.get(1)
                        {
                            self.put_symbol_property(
                                &name,
                                "advertised-calling-convention",
                                convention.clone(),
                            );
                        }
                    }
                    body_start += 1;
                }
                ":documentation" => {
                    if let Some(expression) = parts.get(1) {
                        let value = self.eval(expression, env)?;
                        let doc = Value::String(value.as_string()?.to_string());
                        self.put_symbol_property(
                            &name,
                            "emaxx-cl-defgeneric-documentation",
                            doc.clone(),
                        );
                        self.put_symbol_property(&name, "function-documentation", doc);
                    }
                    body_start += 1;
                }
                ":argument-precedence-order" => {
                    let precedence = parts[1..]
                        .iter()
                        .filter_map(|value| value.as_symbol().ok())
                        .map(|name| Value::Symbol(name.to_string()))
                        .collect::<Vec<_>>();
                    self.put_symbol_property(
                        &name,
                        "emaxx-cl-defgeneric-argument-precedence-order",
                        Value::list(precedence),
                    );
                    body_start += 1;
                }
                ":method" => {
                    let mut lowered_method = Vec::with_capacity(parts.len() + 1);
                    lowered_method.push(Value::Symbol("cl-defmethod".into()));
                    lowered_method.push(Value::Symbol(name.clone()));
                    lowered_method.extend(parts[1..].iter().cloned());
                    self.sf_cl_defmethod(&lowered_method, env)?;
                    body_start += 1;
                }
                _ => break,
            }
        }
        if body_start < items.len() {
            let mut lowered = Vec::with_capacity(items.len() - body_start + 4);
            lowered.push(Value::Symbol("cl-defun".into()));
            lowered.push(Value::Symbol(name.clone()));
            lowered.push(items[2].clone());
            if matches!(
                items.get(3),
                Some(Value::String(_) | Value::StringObject(_))
            ) {
                lowered.push(items[3].clone());
            }
            lowered.extend(items[body_start..].iter().cloned());
            self.sf_cl_defun(&lowered, env)?;
        } else if self.lookup_function(&name, env).is_err() {
            self.set_function_binding(&name, Some(Value::BuiltinFunc("ignore".into())));
        }
        Ok(Value::Symbol(name))
    }

    pub(super) fn sf_def_edebug_elem_spec(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() != 3 {
            return Err(LispError::WrongNumberOfArgs(
                "def-edebug-elem-spec".into(),
                items.len().saturating_sub(1),
            ));
        }
        let name_value = self.eval(&items[1], env)?;
        let spec = self.eval(&items[2], env)?;
        let name = name_value.as_symbol()?.to_string();
        if name.starts_with('&') || name.starts_with(':') {
            return Err(LispError::Signal(
                "Edebug spec name cannot start with '&' or ':'".into(),
            ));
        }
        if !matches!(spec, Value::Cons(_, _)) {
            return Err(LispError::Signal(format!(
                "Edebug spec has to be a list: {spec}"
            )));
        }
        self.put_symbol_property(&name, "edebug-elem-spec", spec.clone());
        Ok(spec)
    }

    pub(super) fn sf_cl_generic_define_generalizer(
        &mut self,
        items: &[Value],
    ) -> Result<Value, LispError> {
        if items.len() < 5 {
            return Err(LispError::Signal(
                "cl-generic-define-generalizer needs name, priority, tagcode, and specializers"
                    .into(),
            ));
        }
        let name = items[1].as_symbol()?.to_string();
        let priority = items[2].as_integer()?;
        let tagcode_function = self.eval(&items[3], &mut Vec::new())?;
        let specializers_function = self.eval(&items[4], &mut Vec::new())?;
        self.register_generic_generalizer(&name, priority, tagcode_function, specializers_function);
        Ok(Value::Symbol(name))
    }

    pub(super) fn sf_cl_defmethod(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Err(LispError::Signal(
                "cl-defmethod needs name and params".into(),
            ));
        }
        let name = function_name_from_binding_form(&items[1])?;
        let lambda_list_index = items
            .iter()
            .enumerate()
            .skip(2)
            .find_map(|(index, value)| {
                matches!(value, Value::Cons(_, _) | Value::Nil).then_some(index)
            })
            .ok_or_else(|| LispError::Signal("cl-defmethod needs a lambda list".into()))?;
        let mut lowered = Vec::with_capacity(items.len());
        lowered.push(Value::Symbol("cl-defun".into()));
        lowered.push(Value::Symbol(name));
        let lowered_lambda_list = lower_cl_defmethod_lambda_list(&items[lambda_list_index])?;
        lowered.push(lowered_lambda_list.clone());
        let (method_doc, normalized_method_forms) =
            self.normalize_function_body_documentation(&items[lambda_list_index + 1..], env)?;
        let executable_method_forms = if normalized_method_forms.is_empty() {
            vec![Value::Nil]
        } else {
            normalized_method_forms
        };
        if let Some(doc) = method_doc.clone() {
            self.add_cl_defmethod_documentation(&function_name_from_binding_form(&items[1])?, doc);
        }
        lowered.extend(executable_method_forms.iter().cloned());
        let requested_method_name = function_name_from_binding_form(&items[1])?;
        let method_name = self.canonical_function_name(&requested_method_name, env);
        let precedence_order = self.cl_defgeneric_argument_precedence_order(&method_name);
        let method_specializers = cl_defmethod_specializers(&items[lambda_list_index])?;
        self.add_cl_defmethod_load_history(
            &method_name,
            &items[2..lambda_list_index],
            &items[lambda_list_index],
        );
        let is_before_method = items[2..lambda_list_index]
            .iter()
            .any(|value| matches!(value, Value::Symbol(name) if name == ":before"));
        let is_after_method = items[2..lambda_list_index]
            .iter()
            .any(|value| matches!(value, Value::Symbol(name) if name == ":after"));
        let mut ordered_method_specializers = method_specializers.clone();
        ordered_method_specializers.sort_by_key(|specializer| {
            cl_defmethod_argument_precedence_index(&specializer.variable, &precedence_order)
        });
        // Methods defined with no surrounding lexical bindings (top-level
        // definitions like edebug's spec-matching methods) capture nothing,
        // so their dispatch wrappers can run on the caller's environment
        // chain. That keeps lexical mutations made inside method bodies
        // visible to the calling scope instead of dying in cloned frames.
        // Methods that do close over locals keep the cloned-env model.
        let transparent_dispatch = env.iter().all(|frame| frame.is_empty());
        let mark_transparent = |mut body: Vec<Value>| {
            body.insert(0, Value::Symbol(":closure-transparent-env".into()));
            body
        };
        if (is_before_method || is_after_method)
            && let Ok(previous) =
                self.lookup_function(&function_name_from_binding_form(&items[1])?, env)
            && previous != Value::BuiltinFunc("ignore".into())
        {
            let params = self.parse_params(&lowered_lambda_list)?;
            let generic_lambda_list = self
                .cl_defgeneric_lambda_list(&method_name)
                .unwrap_or_else(|| lowered_lambda_list.clone());
            let generic_params = self.parse_params(&generic_lambda_list)?;
            let dispatch_method_specializers =
                cl_defmethod_dispatch_specializers(&method_specializers, &params, &generic_params);
            let current_stored_specializer =
                ClDefmethodStoredMethod::from_specializers(&dispatch_method_specializers);
            let generic_runtime_variables =
                cl_defmethod_runtime_variables(&params, &generic_params, &method_specializers);
            let condition = current_stored_specializer.condition(&generic_runtime_variables);
            let method_rest_param = lambda_list_rest_param_from_params(&params);
            let generic_rest_param = lambda_list_rest_param_from_params(&generic_params);
            let method_fixed_params = lambda_list_fixed_params(&params);
            let generic_fixed_params = lambda_list_fixed_params(&generic_params);
            let method_body = cl_defmethod_alias_method_body(
                &method_fixed_params,
                method_rest_param.as_deref(),
                &generic_fixed_params,
                generic_rest_param.as_deref(),
                executable_method_forms.clone(),
            );
            let fixed_call_args = generic_fixed_params
                .iter()
                .map(|param| Value::Symbol(param.clone()))
                .collect::<Vec<_>>();
            let forwarded_args = if let Some(rest_param) = &generic_rest_param {
                let mut list_args = Vec::with_capacity(fixed_call_args.len() + 1);
                list_args.push(Value::Symbol("list".into()));
                list_args.extend(fixed_call_args);
                Value::list([
                    Value::Symbol("append".into()),
                    Value::list(list_args),
                    Value::Symbol(rest_param.clone()),
                ])
            } else {
                let mut list_args = Vec::with_capacity(fixed_call_args.len() + 1);
                list_args.push(Value::Symbol("list".into()));
                list_args.extend(fixed_call_args);
                Value::list(list_args)
            };
            let previous_symbol = format!(
                "__emaxx_{}_method_{}",
                if is_before_method { "before" } else { "after" },
                method_name.replace('-', "_")
            );
            let result_symbol = "__emaxx-cl-defmethod-result".to_string();
            let call_previous = Value::list([
                Value::Symbol("apply".into()),
                Value::Symbol(previous_symbol.clone()),
                forwarded_args.clone(),
            ]);
            let method_effect_form = Value::list(
                std::iter::once(Value::Symbol("progn".into()))
                    .chain(method_body)
                    .collect::<Vec<_>>(),
            );
            let qualified_body = if is_before_method {
                Value::list([
                    Value::Symbol("progn".into()),
                    method_effect_form,
                    call_previous.clone(),
                ])
            } else {
                Value::list([
                    Value::Symbol("let".into()),
                    Value::list([Value::list([
                        Value::Symbol(result_symbol.clone()),
                        call_previous.clone(),
                    ])]),
                    method_effect_form,
                    Value::Symbol(result_symbol),
                ])
            };
            let wrapper_body = vec![Value::list([
                Value::Symbol("if".into()),
                condition,
                qualified_body,
                call_previous,
            ])];
            let wrapper_body = if transparent_dispatch {
                mark_transparent(wrapper_body)
            } else {
                wrapper_body
            };
            let splice = is_before_method
                .then(|| cl_defmethod_first_previous_binding(&previous))
                .flatten();
            let captured_previous = splice
                .as_ref()
                .map(|(_, _, value)| value.clone())
                .unwrap_or_else(|| previous.clone());
            let mut closure_env = Vec::with_capacity(env.len() + 1);
            closure_env.push(vec![(
                previous_symbol,
                Self::stored_value(captured_previous),
            )]);
            closure_env.extend(env.iter().cloned());
            let wrapper = Value::Lambda(generic_params, wrapper_body, shared_env(closure_env));
            if let Some((previous_env, previous_name, _)) = splice {
                let mut previous_env = previous_env.borrow_mut();
                for frame in previous_env.iter_mut() {
                    if let Some((_, value)) =
                        frame.iter_mut().find(|(name, _)| name == &previous_name)
                    {
                        *value = Self::stored_value(wrapper);
                        break;
                    }
                }
            } else {
                self.set_function_binding(&method_name, Some(wrapper));
            }
            return Ok(items[1].clone());
        }
        if let Some(specializer) = ordered_method_specializers.first().cloned()
            && let Ok(previous) =
                self.lookup_function(&function_name_from_binding_form(&items[1])?, env)
            && (previous != Value::BuiltinFunc("ignore".into())
                || method_specializers
                    .iter()
                    .any(|specializer| specializer.is_context))
        {
            let advice_original = cl_defmethod_advice_original_binding(&previous);
            let dispatch_root = advice_original
                .as_ref()
                .map(|(_, _, value)| value.clone())
                .unwrap_or_else(|| previous.clone());
            let is_around_method = items[2..lambda_list_index]
                .iter()
                .any(|value| matches!(value, Value::Symbol(name) if name == ":around"));
            let around_splice = if is_around_method {
                None
            } else {
                let target_class = specializer.class_name();
                let mut is_applicable = |around_class: &str| {
                    around_class == "t"
                        || target_class.is_some_and(|target_class| {
                            around_class == target_class
                                || self.class_allparents(target_class).iter().any(|parent| {
                                    matches!(parent, Value::Symbol(parent) if parent == around_class)
                                })
                        })
                };
                cl_defmethod_around_previous_binding(
                    &dispatch_root,
                    &method_name,
                    &mut is_applicable,
                )
            };
            let previous_specializers = self
                .get_symbol_property(&method_name, "emaxx-cl-defmethod-specializers")
                .and_then(|value| value.to_vec().ok())
                .unwrap_or_default();
            let previous_specializers = previous_specializers
                .iter()
                .filter_map(ClDefmethodStoredMethod::parse)
                .collect::<Vec<_>>();
            let params = self.parse_params(&lowered_lambda_list)?;
            let generic_lambda_list = self
                .cl_defgeneric_lambda_list(&method_name)
                .unwrap_or_else(|| lowered_lambda_list.clone());
            let generic_params = self.parse_params(&generic_lambda_list)?;
            let dispatch_method_specializers =
                cl_defmethod_dispatch_specializers(&method_specializers, &params, &generic_params);
            let current_stored_specializer =
                ClDefmethodStoredMethod::from_specializers(&dispatch_method_specializers);
            let current_method_key = current_stored_specializer.hidden_key();
            let more_specific_previous = if is_around_method {
                Vec::new()
            } else {
                previous_specializers
                    .iter()
                    .filter(|previous| {
                        previous.is_more_specific_than(
                            &current_stored_specializer,
                            &precedence_order,
                            self,
                        )
                    })
                    .cloned()
                    .collect::<Vec<_>>()
            };
            let immediate_more_specific_previous = more_specific_previous
                .iter()
                .filter(|previous| {
                    !more_specific_previous.iter().any(|candidate| {
                        *previous != candidate
                            && previous.is_more_specific_than(candidate, &precedence_order, self)
                            && candidate.is_more_specific_than(
                                &current_stored_specializer,
                                &precedence_order,
                                self,
                            )
                    })
                })
                .cloned()
                .collect::<Vec<_>>();
            let generic_runtime_variables =
                cl_defmethod_runtime_variables(&params, &generic_params, &method_specializers);
            let qualifier_key = cl_defmethod_qualifier_key(&items[2..lambda_list_index]);
            let previous_method_symbol = format!(
                "__emaxx_previous_method_{}_{}{}",
                function_name_from_binding_form(&items[1])?.replace('-', "_"),
                qualifier_key,
                current_method_key
            );
            let method_previous_symbol = format!("{previous_method_symbol}_method");
            let current_method_symbol = format!("{method_previous_symbol}_current");
            let specific_splices = if around_splice.is_some() {
                Vec::new()
            } else {
                immediate_more_specific_previous
                    .iter()
                    .filter_map(|previous_specializer| {
                        let previous_method_symbol = format!(
                            "__emaxx_previous_method_{}_{}{}_method",
                            method_name.replace('-', "_"),
                            qualifier_key,
                            previous_specializer.hidden_key()
                        );
                        cl_defmethod_previous_binding(&dispatch_root, &previous_method_symbol)
                    })
                    .collect::<Vec<_>>()
            };
            let dispatch_previous = around_splice
                .as_ref()
                .map(|(_, _, value)| value.clone())
                .or_else(|| specific_splices.first().map(|(_, _, value)| value.clone()))
                .unwrap_or_else(|| dispatch_root.clone());
            let method_rest_param = lambda_list_rest_param_from_params(&params);
            let generic_rest_param = lambda_list_rest_param_from_params(&generic_params);
            let method_fixed_params = lambda_list_fixed_params(&params);
            let generic_fixed_params = lambda_list_fixed_params(&generic_params);
            let fixed_call_args = generic_fixed_params
                .iter()
                .map(|param| Value::Symbol(param.clone()))
                .collect::<Vec<_>>();
            let mut next_default_args = Vec::with_capacity(fixed_call_args.len() + 1);
            next_default_args.push(Value::Symbol("list".into()));
            next_default_args.extend(fixed_call_args.iter().cloned());
            let next_default_form = if let Some(rest_param) = &generic_rest_param {
                Value::list([
                    Value::Symbol("append".into()),
                    Value::list(next_default_args),
                    Value::Symbol(rest_param.clone()),
                ])
            } else {
                Value::list(next_default_args)
            };
            let method_body = rewrite_cl_call_next_method_forms(
                &executable_method_forms,
                &method_previous_symbol,
                &next_default_form,
                if dispatch_previous == Value::BuiltinFunc("ignore".into()) {
                    Value::Nil
                } else {
                    Value::T
                },
            )?;
            let method_body = cl_defmethod_alias_method_body(
                &method_fixed_params,
                method_rest_param.as_deref(),
                &generic_fixed_params,
                generic_rest_param.as_deref(),
                method_body,
            );
            let method_body = if transparent_dispatch {
                mark_transparent(method_body)
            } else {
                method_body
            };
            let mut dispatch_stop_variables = dispatch_method_specializers
                .iter()
                .map(|specializer| specializer.variable.clone())
                .collect::<Vec<_>>();
            dispatch_stop_variables.extend(more_specific_previous.iter().flat_map(|method| {
                method
                    .specializers
                    .iter()
                    .map(|(variable, _)| variable.clone())
            }));
            let wrapper_rest_param = "__emaxx-cl-defmethod-rest".to_string();
            let wrapper_params = cl_defmethod_dispatch_wrapper_params(
                &generic_lambda_list,
                &cl_defmethod_dispatch_stop_variable(
                    &generic_lambda_list,
                    &dispatch_stop_variables,
                    generic_params
                        .last()
                        .map(String::as_str)
                        .unwrap_or(&specializer.variable),
                )?,
                &wrapper_rest_param,
            )?;
            let wrapper_forward_rest_param =
                lambda_list_rest_param_from_params(&wrapper_params).unwrap_or(wrapper_rest_param);
            let wrapper_fixed_args = wrapper_params
                .iter()
                .take_while(|param| param.as_str() != "&rest")
                .map(|param| Value::Symbol(param.clone()))
                .collect::<Vec<_>>();
            let mut wrapper_arg_list = Vec::with_capacity(wrapper_fixed_args.len() + 1);
            wrapper_arg_list.push(Value::Symbol("list".into()));
            wrapper_arg_list.extend(wrapper_fixed_args);
            let forwarded_args = if wrapper_params.iter().any(|param| param == "&rest") {
                Value::list([
                    Value::Symbol("append".into()),
                    Value::list(wrapper_arg_list),
                    Value::Symbol(wrapper_forward_rest_param),
                ])
            } else {
                Value::list(wrapper_arg_list)
            };
            let method_next = dispatch_previous.clone();
            let current_method_env = std::iter::once(vec![(
                method_previous_symbol.clone(),
                Self::stored_value(method_next),
            )])
            .chain(env.iter().cloned())
            .collect::<Vec<_>>();
            let current_method = Value::Lambda(
                generic_params.clone(),
                method_body,
                shared_env(current_method_env),
            );
            let next_condition = current_stored_specializer.condition(&generic_runtime_variables);
            let mut top_condition = next_condition.clone();
            for previous in &more_specific_previous {
                top_condition = Value::list([
                    Value::Symbol("and".into()),
                    top_condition,
                    Value::list([
                        Value::Symbol("not".into()),
                        previous.condition(&generic_runtime_variables),
                    ]),
                ]);
            }
            let dispatch_body = |condition: Value| {
                let body = vec![Value::list([
                    Value::Symbol("if".into()),
                    condition,
                    Value::list([
                        Value::Symbol("apply".into()),
                        Value::Symbol(current_method_symbol.clone()),
                        forwarded_args.clone(),
                    ]),
                    Value::list([
                        Value::Symbol("apply".into()),
                        Value::Symbol(previous_method_symbol.clone()),
                        forwarded_args.clone(),
                    ]),
                ])];
                if transparent_dispatch {
                    mark_transparent(body)
                } else {
                    body
                }
            };
            let wrapper_closure = |previous: Value| {
                shared_env(
                    std::iter::once(vec![
                        (previous_method_symbol.clone(), Self::stored_value(previous)),
                        (
                            current_method_symbol.clone(),
                            Self::stored_value(current_method.clone()),
                        ),
                    ])
                    .chain(env.iter().cloned())
                    .collect::<Vec<_>>(),
                )
            };
            let top_wrapper = Value::Lambda(
                wrapper_params.clone(),
                dispatch_body(top_condition),
                wrapper_closure(previous.clone()),
            );
            let next_wrapper = Value::Lambda(
                wrapper_params,
                dispatch_body(next_condition),
                wrapper_closure(dispatch_previous),
            );
            let wrapper = if specific_splices.is_empty() {
                top_wrapper.clone()
            } else {
                next_wrapper.clone()
            };
            if let Some((around_env, around_previous_name, _)) = around_splice {
                let mut around_env = around_env.borrow_mut();
                for frame in around_env.iter_mut() {
                    if let Some((_, value)) = frame
                        .iter_mut()
                        .find(|(name, _)| name == &around_previous_name)
                    {
                        *value = Self::stored_value(wrapper.clone());
                        break;
                    }
                }
            } else {
                for (specific_env, specific_previous_name, _) in specific_splices {
                    let mut specific_env = specific_env.borrow_mut();
                    for frame in specific_env.iter_mut() {
                        if let Some((_, value)) = frame
                            .iter_mut()
                            .find(|(name, _)| name == &specific_previous_name)
                        {
                            *value = Self::stored_value(wrapper.clone());
                            break;
                        }
                    }
                }
                if let Some((advice_env, advice_original_name, _)) = advice_original {
                    let mut advice_env = advice_env.borrow_mut();
                    for frame in advice_env.iter_mut() {
                        if let Some((_, value)) = frame
                            .iter_mut()
                            .find(|(name, _)| name == &advice_original_name)
                        {
                            *value = Self::stored_value(top_wrapper.clone());
                            break;
                        }
                    }
                    drop(advice_env);
                    self.replace_next_function_binding(&method_name, top_wrapper);
                } else {
                    self.set_function_binding(&method_name, Some(top_wrapper));
                }
            }
            self.add_cl_defmethod_specializer(
                &method_name,
                current_stored_specializer.metadata_value(),
            );
            return Ok(items[1].clone());
        }
        if method_specializers.is_empty()
            && let Ok(previous) =
                self.lookup_function(&function_name_from_binding_form(&items[1])?, env)
            && previous != Value::BuiltinFunc("ignore".into())
            && self
                .get_symbol_property(&method_name, "emaxx-cl-defmethod-specializers")
                .is_some()
        {
            let generic_lambda_list = self
                .cl_defgeneric_lambda_list(&method_name)
                .unwrap_or_else(|| lowered_lambda_list.clone());
            let generic_params = self.parse_params(&generic_lambda_list)?;
            let base_method = Value::Lambda(
                generic_params,
                executable_method_forms.clone(),
                shared_env(env.clone()),
            );
            if cl_defmethod_replace_ignore_previous_bindings(&previous, &base_method) {
                return Ok(items[1].clone());
            }
        }
        let result = if method_specializers.is_empty() {
            self.sf_cl_defun(&lowered, env)
        } else {
            let mut direct_lowered = lowered[..3].to_vec();
            direct_lowered.extend(rewrite_cl_next_method_p_forms(
                &executable_method_forms,
                Value::Nil,
            )?);
            self.sf_cl_defun(&direct_lowered, env)
        };
        if !method_specializers.is_empty() {
            let params = self.parse_params(&lowered_lambda_list)?;
            let generic_lambda_list = self
                .cl_defgeneric_lambda_list(&method_name)
                .unwrap_or_else(|| lowered_lambda_list.clone());
            let generic_params = self.parse_params(&generic_lambda_list)?;
            let dispatch_method_specializers =
                cl_defmethod_dispatch_specializers(&method_specializers, &params, &generic_params);
            let current_stored_specializer =
                ClDefmethodStoredMethod::from_specializers(&dispatch_method_specializers);
            self.add_cl_defmethod_specializer(
                &method_name,
                current_stored_specializer.metadata_value(),
            );
        }
        result
    }

    fn canonical_function_name(&self, name: &str, env: &Env) -> String {
        let mut current = name.to_string();
        let mut seen = HashSet::new();
        while seen.insert(current.clone()) {
            match self.raw_function_binding(&current, env) {
                Some(Value::Symbol(next)) => current = next,
                _ => return current,
            }
        }
        name.to_string()
    }

    fn cl_defgeneric_argument_precedence_order(&self, method_name: &str) -> Vec<String> {
        self.get_symbol_property(method_name, "emaxx-cl-defgeneric-argument-precedence-order")
            .and_then(|value| value.to_vec().ok())
            .unwrap_or_default()
            .into_iter()
            .filter_map(|value| value.as_symbol().ok().map(str::to_string))
            .collect()
    }

    fn cl_defgeneric_lambda_list(&self, method_name: &str) -> Option<Value> {
        self.get_symbol_property(method_name, "emaxx-cl-defgeneric-lambda-list")
    }

    fn add_cl_defmethod_specializer(&mut self, method_name: &str, specializer: Value) {
        let mut specializers = self
            .get_symbol_property(method_name, "emaxx-cl-defmethod-specializers")
            .and_then(|value| value.to_vec().ok())
            .unwrap_or_default();
        if !specializers.iter().any(|value| value == &specializer) {
            specializers.push(specializer);
            self.put_symbol_property(
                method_name,
                "emaxx-cl-defmethod-specializers",
                Value::list(specializers),
            );
        }
    }

    fn add_cl_defmethod_documentation(&mut self, method_name: &str, doc: Value) {
        let mut docs = self
            .get_symbol_property(method_name, "emaxx-cl-defmethod-documentation")
            .and_then(|value| value.to_vec().ok())
            .unwrap_or_default();
        if !docs.iter().any(|value| value == &doc) {
            docs.push(doc);
            self.put_symbol_property(
                method_name,
                "emaxx-cl-defmethod-documentation",
                Value::list(docs),
            );
        }
    }

    fn add_cl_defmethod_load_history(
        &mut self,
        method_name: &str,
        qualifiers: &[Value],
        spec: &Value,
    ) {
        let Some(mut current_load_list) = self.lookup_var("current-load-list", &Env::new()) else {
            return;
        };
        if current_load_list.is_nil() {
            return;
        }
        let mut method = Vec::with_capacity(2 + qualifiers.len());
        method.push(Value::Symbol(method_name.to_string()));
        method.push(Value::list(qualifiers.to_vec()));
        method.extend(cl_defmethod_load_history_specializers(spec));
        let entry = Value::cons(Value::Symbol("cl-defmethod".into()), Value::list(method));
        if current_load_list
            .to_vec()
            .is_ok_and(|items| items.iter().any(|item| item == &entry))
        {
            return;
        }
        let mut entries = current_load_list.to_vec().unwrap_or_default();
        entries.push(entry);
        current_load_list = Value::list(entries);
        self.set_global_binding("current-load-list", current_load_list);
    }

    pub(super) fn sf_oclosure_define(&mut self, items: &[Value]) -> Result<Value, LispError> {
        let Some(name_form) = items.get(1) else {
            return Err(LispError::Signal("oclosure-define needs a name".into()));
        };
        Ok(Value::Symbol(function_name_from_binding_form(name_form)?))
    }

    pub(super) fn sf_oclosure_lambda(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Err(LispError::Signal(
                "oclosure-lambda needs slots, args, and body".into(),
            ));
        }
        let mut lowered = Vec::with_capacity(items.len());
        lowered.push(Value::Symbol("lambda".into()));
        lowered.push(items[2].clone());
        lowered.extend(items[3..].iter().cloned());
        self.sf_lambda(&lowered, env)
    }

    pub(super) fn sf_define_inline(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 4 {
            return Err(LispError::Signal(
                "define-inline needs name, params, body".into(),
            ));
        }
        let mut lowered = Vec::with_capacity(items.len());
        lowered.push(Value::Symbol("defun".into()));
        lowered.push(items[1].clone());
        lowered.push(items[2].clone());
        lowered.extend(items[3..].iter().map(lower_define_inline_form));
        self.sf_defun(&lowered, env)
    }

    pub(super) fn sf_lambda(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Err(LispError::Signal("lambda needs params".into()));
        }
        let params = self.parse_params(&items[1])?;
        let (_, body) = self.normalize_function_body_documentation(&items[2..], env)?;
        let keep_full_context = body_closure_dont_trim_context(&body);
        let closure_env = if self.lambda_capture_override().unwrap_or(true) {
            if !keep_full_context && self.lambda_trim_override() {
                shared_env(trim_lambda_closure_env(env, &body))
            } else {
                self.capture_closure_env(env.clone())
            }
        } else {
            shared_env(Vec::new())
        };
        Ok(Value::Lambda(params, body, closure_env))
    }

    pub(super) fn sf_eval_function(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 2 || items.len() > 3 {
            return Err(LispError::WrongNumberOfArgs(
                "eval".into(),
                items.len().saturating_sub(1),
            ));
        }
        let mut evaluated = Vec::with_capacity(items.len().saturating_sub(1));
        for item in &items[1..] {
            evaluated.push(self.eval(item, env)?);
        }
        crate::lisp::primitives::eval_impl(self, &evaluated, env)
    }
}

fn trim_lambda_closure_env(env: &Env, body: &[Value]) -> Env {
    let mut referenced = HashSet::new();
    for form in body {
        collect_referenced_symbols(form, &mut referenced);
    }

    env.iter()
        .filter_map(|frame| {
            let last_used = frame
                .iter()
                .rposition(|(name, _)| referenced.contains(name.as_str()))?;
            Some(frame[..=last_used].to_vec())
        })
        .collect()
}

fn value_tree_contains_symbol(value: &Value, target: &str) -> bool {
    match value {
        Value::Symbol(symbol) => symbol == target,
        Value::Cons(car, cdr) => {
            value_tree_contains_symbol(&car.borrow(), target)
                || value_tree_contains_symbol(&cdr.borrow(), target)
        }
        _ => false,
    }
}

fn collect_referenced_symbols(value: &Value, referenced: &mut HashSet<String>) {
    match value {
        Value::Symbol(symbol) => {
            referenced.insert(symbol.clone());
        }
        Value::Cons(_, _) => {
            let Ok(items) = value.to_vec() else {
                collect_dotted_list_symbols(value, referenced);
                return;
            };
            if matches!(items.first(), Some(Value::Symbol(symbol)) if symbol == "quote") {
                return;
            }
            for item in items {
                collect_referenced_symbols(&item, referenced);
            }
        }
        _ => {}
    }
}

fn collect_dotted_list_symbols(value: &Value, referenced: &mut HashSet<String>) {
    let Some((car, cdr)) = value.cons_values() else {
        collect_referenced_symbols(value, referenced);
        return;
    };
    collect_referenced_symbols(&car, referenced);
    collect_referenced_symbols(&cdr, referenced);
}

fn custom_type_matches_value(custom_type: &Value, value: &Value) -> bool {
    match custom_type {
        Value::Symbol(symbol) => match symbol.as_str() {
            "boolean" => matches!(value, Value::Nil | Value::T),
            "integer" => value.is_integer(),
            "number" => value.is_integer() || matches!(value, Value::Float(_)),
            "string" => crate::lisp::primitives::string_like(value).is_some(),
            "symbol" => value.is_symbol(),
            _ => true,
        },
        _ => true,
    }
}
