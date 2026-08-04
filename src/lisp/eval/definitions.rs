use super::*;

#[derive(Clone, Debug, PartialEq)]
enum ClDefmethodStoredSpecializer {
    Class(String),
    Subclass(String),
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
            // Expression contexts (from context rewriters) evaluate their
            // form at dispatch; plain contexts read the variable.
            let source = specializer
                .context_expr
                .clone()
                .unwrap_or_else(|| Value::Symbol(specializer.variable.clone()));
            runtime_variables.push((
                specializer.variable.clone(),
                cl_defmethod_argument_key(&specializer.variable).to_string(),
                source,
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
                [Value::Symbol(kind), Value::Symbol(class_name)] if kind == "subclass" => {
                    return Some(Self::Subclass(class_name.clone()));
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
            Self::Subclass(class_name) => format!("subclass:{class_name}"),
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
            Self::Subclass(class_name) => Value::list([
                Value::Symbol("subclass".into()),
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
            Self::Subclass(class_name) => Value::list([
                Value::Symbol("cl-typep".into()),
                runtime_value,
                Value::list([
                    Value::Symbol("quote".into()),
                    Value::list([
                        Value::Symbol("subclass".into()),
                        Value::Symbol(class_name.clone()),
                    ]),
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

    fn is_default(&self) -> bool {
        matches!(self, Self::Class(class_name) if class_name == "t")
    }

    fn is_more_specific_than(&self, other: &Self, interp: &Interpreter) -> bool {
        let inherits = |child: &str, ancestor: &str| {
            interp
                .class_allparents(child)
                .iter()
                .any(|parent| matches!(parent, Value::Symbol(parent) if parent == ancestor))
        };
        // Oclosures add a semantic type on top of their callable storage.
        // An interpreted or compiled oclosure therefore matches both its
        // concrete type and the corresponding representation class, but GNU
        // generic dispatch always prefers the concrete oclosure method.
        let oclosure_precedes_representation = |child: &str, ancestor: &str| {
            interp.class_is_oclosure_type(child)
                && matches!(ancestor, "interpreted-function" | "byte-code-function")
        };
        match (self, other) {
            (Self::Class(left), Self::Class(right)) => {
                left != right
                    && (inherits(left, right)
                        || oclosure_precedes_representation(left, right)
                        || (!inherits(right, left)
                            && !oclosure_precedes_representation(right, left)
                            && interp.class_sibling_precedes(left, right)))
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
            (Self::Subclass(left), Self::Subclass(right)) => {
                left != right
                    && (inherits(left, right)
                        || oclosure_precedes_representation(left, right)
                        || (!inherits(right, left)
                            && !oclosure_precedes_representation(right, left)
                            && interp.class_sibling_precedes(left, right)))
            }
            (Self::Subclass(_), Self::Class(class_name)) => class_name == "t",
            (Self::Eql(_), Self::Subclass(_)) => true,
            _ => false,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
struct ClDefmethodStoredMethod {
    // (variable, specializer, context expression): the expression is
    // present for `&context (EXPR SPEC)' entries so another method's
    // guard can re-evaluate the context test.
    specializers: Vec<(String, ClDefmethodStoredSpecializer, Option<Value>)>,
}

impl ClDefmethodStoredMethod {
    fn from_specializers(specializers: &[ClDefmethodSpecializer]) -> Self {
        let specializers = specializers
            .iter()
            .filter_map(|specializer| {
                ClDefmethodStoredSpecializer::parse(&specializer.metadata_value()).map(|stored| {
                    (
                        specializer.variable.clone(),
                        stored,
                        specializer.context_expr.clone(),
                    )
                })
            })
            .collect();
        Self { specializers }
    }

    fn parse(value: &Value) -> Option<Self> {
        if let Some(specializer) = ClDefmethodStoredSpecializer::parse(value) {
            return Some(Self {
                specializers: vec![("".into(), specializer, None)],
            });
        }
        let items = value.to_vec().ok()?;
        if !matches!(items.first(), Some(Value::Symbol(kind)) if kind == "method") {
            return None;
        }
        let mut specializers = Vec::new();
        for item in &items[1..] {
            let parts = item.to_vec().ok()?;
            match parts.as_slice() {
                [Value::Symbol(variable), specializer] => specializers.push((
                    variable.clone(),
                    ClDefmethodStoredSpecializer::parse(specializer)?,
                    None,
                )),
                [Value::Symbol(variable), specializer, context_expr] => specializers.push((
                    variable.clone(),
                    ClDefmethodStoredSpecializer::parse(specializer)?,
                    Some(context_expr.clone()),
                )),
                _ => return None,
            }
        }
        Some(Self { specializers })
    }

    fn metadata_value(&self) -> Value {
        let mut items = Vec::with_capacity(self.specializers.len() + 1);
        items.push(Value::Symbol("method".into()));
        items.extend(
            self.specializers
                .iter()
                .map(|(variable, specializer, context_expr)| {
                    let mut parts = vec![
                        Value::Symbol(variable.clone()),
                        specializer.metadata_value(),
                    ];
                    if let Some(expr) = context_expr {
                        parts.push(expr.clone());
                    }
                    Value::list(parts)
                }),
        );
        Value::list(items)
    }

    fn hidden_key(&self) -> String {
        self.specializers
            .iter()
            .map(|(variable, specializer, _)| format!("{variable}_{}", specializer.hidden_key()))
            .collect::<Vec<_>>()
            .join("_")
            .replace([':', '\'', ' ', '(', ')'], "_")
    }

    fn condition(&self, runtime_variables: &[(String, String, Value)]) -> Value {
        let mut conditions =
            self.specializers
                .iter()
                .filter_map(|(variable, specializer, context_expr)| {
                    let key = cl_defmethod_argument_key(variable);
                    let runtime_variable = runtime_variables
                        .iter()
                        .find_map(|(runtime, runtime_key, value)| {
                            (runtime == variable || runtime_key == key).then_some(value.clone())
                        })
                        .or_else(|| context_expr.clone())?;
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
            .map(|(variable, _, _)| variable.clone())
            .chain(
                other
                    .specializers
                    .iter()
                    .map(|(variable, _, _)| variable.clone()),
            )
            .collect::<Vec<_>>();
        variables.sort();
        variables.dedup();
        variables.sort_by_key(|variable| {
            cl_defmethod_argument_precedence_index(variable, precedence_order)
        });
        for variable in variables {
            // A class-`t' specializer is the default: it ranks like an
            // unspecialized argument, so a method with a real specializer
            // on a later argument still beats it.
            let left = self
                .specializer_for(&variable)
                .filter(|specializer| !specializer.is_default());
            let right = other
                .specializer_for(&variable)
                .filter(|specializer| !specializer.is_default());
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
            .find_map(|(candidate, specializer, _)| {
                (candidate == variable || cl_defmethod_argument_key(candidate) == key)
                    .then_some(specializer)
            })
    }
}

// caar/cadr/.../cddddr accessors usable as setf places ("car"/"cdr"
// themselves take the resolve_setf_place path).
fn is_cxr_accessor_name(name: &str) -> bool {
    let letters = name
        .strip_prefix('c')
        .and_then(|rest| rest.strip_suffix('r'))
        .unwrap_or("");
    (2..=4).contains(&letters.len()) && letters.chars().all(|ch| ch == 'a' || ch == 'd')
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
                        // GNU defun-declarations-alist: (obsolete NEW WHEN)
                        // runs `make-obsolete', storing (NEW nil WHEN).
                        Some("obsolete") if parts.len() >= 2 => {
                            let when = parts.get(2).cloned().unwrap_or(Value::Nil);
                            interp.put_symbol_property(
                                name,
                                "byte-obsolete-info",
                                Value::list([parts[1].clone(), Value::Nil, when]),
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
        let mut explicit_documentation = false;
        let documentation = match first {
            Value::String(text) => Some(Value::String(text.clone())),
            Value::StringObject(state) => Some(Value::String(state.borrow().text.clone())),
            Value::Cons(_, _) => {
                let items = first.to_vec()?;
                match items.as_slice() {
                    [Value::Symbol(head), expression] if head == ":documentation" => {
                        explicit_documentation = true;
                        let value = self.eval(expression, env)?;
                        let text = crate::lisp::primitives::string_like(&value)
                            .map(|string| string.text)
                            .ok_or_else(|| {
                                LispError::TypeError("string".into(), value.type_name())
                            })?;
                        Some(Value::String(text))
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
        // Keep the original source object in the function body.  A
        // one-form body consisting of a string is executable (it is only a
        // docstring when another body form follows), and GNU returns the
        // same literal object on every call.  The separately normalized
        // documentation value is for metadata consumers only.  An explicit
        // `(:documentation EXPR)' is different: GNU evaluates it at
        // definition time and replaces the form with the resulting
        // docstring, so it must never remain executable body code.
        normalized.push(if explicit_documentation {
            documentation.clone()
        } else {
            first.clone()
        });
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
                // An autoload stub is not a usable widget runtime when this
                // interpreter has no matching Lisp load path.  The native
                // setopt fallback can validate the common Custom types
                // without turning a merely advertised wid-edit autoload into
                // an unconditional file dependency.
                let widget_runtime_loaded =
                    ["widget-convert", "widget-apply"].into_iter().all(|name| {
                        self.lookup_function(name, env).is_ok_and(|binding| {
                            crate::lisp::primitives::autoload_parts(&binding).is_none()
                        })
                    });
                let matches = if widget_runtime_loaded {
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
            Some(Value::Symbol(name))
                if self
                    .get_symbol_property(name, "emaxx-gv-setter-handler")
                    .is_some() =>
            {
                let handler = self
                    .get_symbol_property(name, "emaxx-gv-setter-handler")
                    .expect("checked above");
                let mut handler_args = Vec::with_capacity(place.len());
                handler_args.push(items[2].clone());
                handler_args.extend(place[1..].iter().cloned());
                let store_expression =
                    self.call_function_value(handler, None, &handler_args, env)?;
                self.eval(&store_expression, env)
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
            Some(Value::Symbol(name)) if name == "get" && place.len() >= 3 => {
                // (setf (get SYMBOL PROP) VAL) == (put SYMBOL PROP VAL)
                let symbol = self.eval(&place[1], env)?;
                let property = self.eval(&place[2], env)?;
                let value = self.eval(&items[2], env)?;
                primitives::call(self, "put", &[symbol, property, value.clone()], env)?;
                Ok(value)
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
            // (setf (cXY..r P) V) = (set{car,cdr} (cY..r P) V): the first
            // a/d picks the cell mutator, the rest re-derive the cell.
            Some(Value::Symbol(name)) if is_cxr_accessor_name(name) => {
                let Some(target_expr) = place.get(1) else {
                    return Err(LispError::Signal("Unsupported setf place".into()));
                };
                let letters = &name[1..name.len() - 1];
                let rest = &letters[1..];
                let inner_expr = if rest.is_empty() {
                    target_expr.clone()
                } else {
                    Value::list([Value::Symbol(format!("c{rest}r")), target_expr.clone()])
                };
                let cell = self.eval(&inner_expr, env)?;
                let value = self.eval(&items[2], env)?;
                if letters.starts_with('a') {
                    cell.set_car(value.clone())?;
                } else {
                    cell.set_cdr(value.clone())?;
                }
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
        // Unnamed `:type vector' structs live in plain vectors.
        if crate::lisp::primitives::is_vector_value(&object) {
            crate::lisp::primitives::aset_vector_value(&object, slot_index, value.clone())
                .map_err(|_| {
                    LispError::Signal(format!("Struct slot out of range: {slot_index}"))
                })?;
            return Ok(value);
        }
        // Unnamed `:type list' structs live in plain lists: replace the car of
        // the slot's cons cell in place.
        if matches!(object, Value::Cons(_, _))
            && self
                .get_symbol_property(&expected_type, "emaxx-struct-sequence-type")
                .and_then(|kind| kind.as_symbol().ok().map(str::to_string))
                .as_deref()
                == Some("list")
        {
            let mut current = object.clone();
            for _ in 0..slot_index {
                let Value::Cons(_, cdr) = current else {
                    return Err(LispError::Signal(format!(
                        "Struct slot out of range: {slot_index}"
                    )));
                };
                let next = cdr.borrow().clone();
                current = next;
            }
            let Value::Cons(car, _) = &current else {
                return Err(LispError::Signal(format!(
                    "Struct slot out of range: {slot_index}"
                )));
            };
            *car.borrow_mut() = value.clone();
            return Ok(value);
        }
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
        // GNU's gv expander mutates a found pair with `setcdr' (the list
        // itself stays `eq'; map-put! relies on that to detect in-place
        // updates) and only assigns the place when prepending or removing.
        let entries = alist.to_vec()?;
        let mut found = None;
        for (index, entry) in entries.iter().enumerate() {
            if let Some((car, _)) = entry.cons_values()
                && primitives::value_matches_with_test(self, &key, &car, testfn.as_ref(), env)?
            {
                found = Some(index);
                break;
            }
        }
        match found {
            Some(index) => {
                if should_remove {
                    if index == 0 {
                        // Removing the head entry reassigns the place to
                        // the tail; deeper entries splice out with
                        // `setcdr' so the list stays `eq' (GNU gv).
                        self.set_setf_place_value(alist_place, alist.cdr()?, env)?;
                    } else {
                        let mut spine = alist.clone();
                        for _ in 0..index - 1 {
                            spine = spine.cdr()?;
                        }
                        let removed = spine.cdr()?;
                        spine.set_cdr(removed.cdr()?)?;
                    }
                } else {
                    entries[index].set_cdr(value.clone())?;
                }
            }
            None => {
                if !should_remove {
                    self.set_setf_place_value(
                        alist_place,
                        Value::cons(Value::cons(key.clone(), value.clone()), alist),
                        env,
                    )?;
                }
            }
        }
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
        // GNU's gv expander for plist-get/cl-getf PREPENDS missing keys
        // ((setf (plist-get l :d) v) => (:d v . l)); plist-put would
        // append instead.  Only plist-get's third argument is a
        // predicate; cl-getf's is a DEFAULT that the setter ignores.
        let takes_testfn =
            matches!(place.first(), Some(Value::Symbol(name)) if name == "plist-get");
        let testfn = match place.get(3) {
            Some(extra_expr) => {
                let extra = self.eval(extra_expr, env)?;
                (takes_testfn && !extra.is_nil()).then_some(extra)
            }
            None => None,
        };
        let updated = self.cl_set_getf(plist, key, value.clone(), testfn, env)?;
        self.set_setf_place_value(plist_place, updated, env)?;
        Ok(value)
    }

    fn cl_set_getf(
        &mut self,
        plist: Value,
        key: Value,
        value: Value,
        testfn: Option<Value>,
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
                    if primitives::value_matches_with_test(
                        self,
                        &property,
                        &key,
                        testfn.as_ref(),
                        env,
                    )? {
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
        // `(setf (elt LIST i) v)` mutates the list structure itself (GNU
        // expands it to `setcar`), so aliases of the list see the change.
        if matches!(current, Value::Cons(_, _))
            && !matches!(
                current.car(),
                Ok(Value::Symbol(symbol)) if symbol == "vector-literal"
            )
        {
            let mut cell = current;
            for _ in 0..index {
                cell = cell.cdr()?;
            }
            if !matches!(cell, Value::Cons(_, _)) {
                return Err(LispError::Signal("Args out of range".into()));
            }
            cell.set_car(value.clone())?;
            return Ok(value);
        }
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
                } else if matches!(items.first(), Some(Value::Symbol(name)) if name == "aref" || name == "elt")
                {
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
        // GNU: a bare one-arg `defvar' NOT at top level only makes the
        // variable special within the enclosing lexical scope — the global
        // flag stays off (`special-variable-p' returns nil), so other
        // functions' same-named arguments and `let's remain lexical
        // (erc-send-input relies on this for its obsolete dynamic `str').
        // The local specialness is recorded as a frame marker scoped to the
        // current activation so `let's in the SAME scope bind dynamically.
        if items.len() > 2 || is_defcustom {
            self.mark_special_variable(&resolved);
        } else if env.is_empty() {
            // Top level of a lexical-binding file: GNU scopes the
            // declaration to the file (special-variable-p stays nil).
            self.mark_soft_special(&resolved);
        } else {
            self.push_local_special_marker(&resolved, env);
        }
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
            if is_defcustom
                && self
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
        // GNU custom-declare-variable records the unevaluated default under
        // `standard-value' for every defcustom; custom-variable-p keys off it
        // (erc--update-modules activates global module modes through it).
        self.put_symbol_property(
            symbol,
            "standard-value",
            Value::list([items.get(2).cloned().unwrap_or(Value::Nil)]),
        );
        let mut initialize = None;
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
                ":get" => {
                    let getter = self.eval(&items[index + 1], env)?;
                    self.put_symbol_property(symbol, "custom-get", getter);
                }
                ":initialize" => {
                    let function = self.eval(&items[index + 1], env)?;
                    // `custom-declare-variable' delegates the initial
                    // assignment to this function.  In particular,
                    // `custom-initialize-reset' must invoke a :set function
                    // even when an earlier `defvar' already bound the option;
                    // completion-pcm uses that side effect to compile its
                    // delimiter regexp.
                    initialize = Some(function);
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
        // GNU's custom-declare-variable records every keyword first and then
        // calls the :initialize function with (SYMBOL EXP), EXP being the
        // unevaluated default (erc-modules' initializer walks the
        // custom-type entries recorded above to stamp `erc--module').
        if let Some(function) = initialize {
            let default_expr = items.get(2).cloned().unwrap_or(Value::Nil);
            match self.call_function_value(
                function,
                None,
                &[Value::Symbol(symbol.to_string()), default_expr],
                env,
            ) {
                Ok(_) => {}
                // Bare interpreters (unit tests) may not have the
                // custom-initialize family loaded; the plain default
                // assignment already happened, so a void initializer is
                // tolerable.
                Err(LispError::VoidFunction(_)) => {}
                Err(LispError::SignalValue(condition))
                    if matches!(condition.car(), Ok(Value::Symbol(kind))
                        if kind == "void-function") => {}
                Err(error) => return Err(error),
            }
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
            self.ensure_lisp_face(name, false, false)?;
            self.ensure_lisp_face(name, true, false)?;
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

    pub(crate) fn record_defface_runtime_attributes(
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
            if let Some(index) = crate::lisp::primitives::face_attribute_index(&attribute) {
                self.set_lisp_face_attribute(face, index, value.clone(), false)?;
            }
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

        let full = items
            .windows(2)
            .find(|pair| matches!(&pair[0], Value::Symbol(keyword) if keyword == ":full"))
            .is_some_and(|pair| pair[1].is_truthy());
        let keymap = if full {
            crate::lisp::primitives::make_runtime_full_keymap(self, Some(&resolved))
        } else {
            crate::lisp::primitives::make_runtime_keymap(self, Some(&resolved))
        };
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

        let stored = Self::stored_value(keymap);
        self.globals_index.insert(resolved.clone(), stored.clone());
        if let Some(existing) = self
            .globals
            .iter_mut()
            .rposition(|(symbol, _)| symbol == &resolved)
        {
            self.globals[existing].1 = stored;
        } else {
            self.globals.push((resolved, stored));
        }
        Ok(Value::Nil)
    }

    pub(super) fn sf_define_mode(&mut self, items: &[Value]) -> Result<Value, LispError> {
        let Some(name) = items.get(1).and_then(|value| value.as_symbol().ok()) else {
            return Ok(Value::Nil);
        };
        if let Some(Value::Symbol(raw_kind)) = items.first() {
            // Eager source expansion lowers `define-derived-mode' to a
            // GNU-shaped search stub followed by this internal executor.
            // The executor must replace that stub with the real mode body;
            // treating its private name as a fourth mode kind leaves the
            // preceding `(defalias MODE #'ignore)' installed.
            let kind = if raw_kind == "emaxx--define-derived-mode" {
                "define-derived-mode"
            } else {
                raw_kind.as_str()
            };
            if kind == "define-minor-mode" {
                let mut init_value = Value::Nil;
                let mut global = false;
                let mut variable_name = name.to_string();
                let mut variable_setter = None;
                let mut lighter = Value::Nil;
                let mut keymap_spec = None;
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
                        ":lighter" => lighter = items[index + 1].clone(),
                        ":keymap" => keymap_spec = Some(items[index + 1].clone()),
                        ":variable" => match &items[index + 1] {
                            Value::Symbol(variable) => variable_name = variable.clone(),
                            variable => {
                                if let Some((variable, setter)) = variable.cons_values()
                                    && let Ok(variable) = variable.as_symbol()
                                {
                                    variable_name = variable.to_string();
                                    variable_setter = Some(setter);
                                }
                            }
                        },
                        _ => {}
                    }
                    index += 2;
                }

                self.mark_special_variable(&variable_name);
                let init_value_truthy = init_value.is_truthy();
                if global {
                    // GNU's :global define-minor-mode declares the mode
                    // variable with defcustom, so it carries a
                    // `standard-value' and satisfies custom-variable-p
                    // (erc--update-modules activates global modes through it).
                    self.put_symbol_property(
                        &variable_name,
                        "standard-value",
                        Value::list([init_value.clone()]),
                    );
                } else {
                    self.mark_auto_buffer_local(&variable_name);
                    // The generated body maintains the GNU C variable
                    // `local-minor-modes'; make sure it exists even in
                    // sessions without the preloaded lisp.
                    self.mark_special_variable("local-minor-modes");
                    self.mark_auto_buffer_local("local-minor-modes");
                    if self.lookup_var("local-minor-modes", &Vec::new()).is_none() {
                        self.globals_index
                            .insert("local-minor-modes".into(), Value::Nil);
                        self.globals.push(("local-minor-modes".into(), Value::Nil));
                    }
                }
                if self.lookup_var(&variable_name, &Vec::new()).is_none() {
                    let stored = Self::stored_value(init_value);
                    self.globals_index
                        .insert(variable_name.clone(), stored.clone());
                    self.globals.push((variable_name.clone(), stored));
                }
                let toggle = Value::Symbol(variable_name.clone());
                let mut minor_modes = self
                    .lookup_var("minor-mode-list", &Vec::new())
                    .unwrap_or(Value::Nil)
                    .to_vec()
                    .unwrap_or_default();
                if !minor_modes.iter().any(|mode| mode == &toggle) {
                    minor_modes.insert(0, toggle.clone());
                    self.set_global_binding("minor-mode-list", Value::list(minor_modes));
                }

                if !lighter.is_nil() {
                    let entry = Value::list([toggle.clone(), lighter]);
                    let mut entries = self
                        .lookup_var("minor-mode-alist", &Vec::new())
                        .unwrap_or(Value::Nil)
                        .to_vec()
                        .unwrap_or_default();
                    if let Some(index) = entries
                        .iter()
                        .position(|existing| existing.car().is_ok_and(|mode| mode == toggle))
                    {
                        entries[index] = entry;
                    } else {
                        entries.insert(0, entry);
                    }
                    self.set_global_binding("minor-mode-alist", Value::list(entries));
                }

                let map = match keymap_spec {
                    Some(Value::Symbol(map_name)) => self.lookup_var(&map_name, &Vec::new()),
                    Some(Value::Nil) => None,
                    Some(_) => self.lookup_var(&format!("{name}-map"), &Vec::new()),
                    None => self.lookup_var(&format!("{name}-map"), &Vec::new()),
                };
                if let Some(map) = map {
                    let entry_mode = toggle.clone();
                    let entry = Value::cons(toggle, map);
                    let mut entries = self
                        .lookup_var("minor-mode-map-alist", &Vec::new())
                        .unwrap_or(Value::Nil)
                        .to_vec()
                        .unwrap_or_default();
                    if let Some(index) = entries.iter().position(|existing| {
                        existing
                            .cons_values()
                            .is_some_and(|(mode, _)| mode == entry_mode)
                    }) {
                        entries[index] = entry;
                    } else {
                        entries.insert(0, entry);
                    }
                    self.set_global_binding("minor-mode-map-alist", Value::list(entries));
                }

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

                let set_mode_form = if let Some(setter) = variable_setter {
                    Value::list([Value::Symbol("funcall".into()), setter, toggle_form])
                } else {
                    Value::list([
                        Value::Symbol(if global { "setq-default" } else { "setq" }.into()),
                        Value::Symbol(variable_name.clone()),
                        toggle_form,
                    ])
                };
                let mut body = vec![set_mode_form];
                if !global {
                    // GNU's expansion tracks enabled buffer-local modes in
                    // the C variable `local-minor-modes'.
                    let mode_symbol = Value::list([
                        Value::Symbol("quote".into()),
                        Value::Symbol(name.to_string()),
                    ]);
                    body.push(Value::list([
                        Value::Symbol("if".into()),
                        Value::Symbol(variable_name.clone()),
                        Value::list([
                            Value::Symbol("unless".into()),
                            Value::list([
                                Value::Symbol("memq".into()),
                                mode_symbol.clone(),
                                Value::Symbol("local-minor-modes".into()),
                            ]),
                            Value::list([
                                Value::Symbol("push".into()),
                                mode_symbol.clone(),
                                Value::Symbol("local-minor-modes".into()),
                            ]),
                        ]),
                        Value::list([
                            Value::Symbol("setq".into()),
                            Value::Symbol("local-minor-modes".into()),
                            Value::list([
                                Value::Symbol("delq".into()),
                                mode_symbol,
                                Value::Symbol("local-minor-modes".into()),
                            ]),
                        ]),
                    ]));
                }
                body.extend_from_slice(&items[index..]);
                // GNU runs MODE-hook (and the on/off variant) on every
                // toggle, in both directions.
                let quoted_symbol = |symbol: String| {
                    Value::list([Value::Symbol("quote".into()), Value::Symbol(symbol)])
                };
                body.push(Value::list([
                    Value::Symbol("run-hooks".into()),
                    quoted_symbol(format!("{name}-hook")),
                    Value::list([
                        Value::Symbol("if".into()),
                        current_mode_form,
                        quoted_symbol(format!("{name}-on-hook")),
                        quoted_symbol(format!("{name}-off-hook")),
                    ]),
                ]));
                body.push(Value::Symbol(variable_name));

                self.set_function_binding(
                    name,
                    Some(Value::Lambda(
                        vec!["&optional".into(), "arg".into()],
                        body.into(),
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
                    self.globals_index.insert(name.to_string(), Value::Nil);
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
                let hook_name = format!("{name}-hook");
                let default_syntax_table_name = format!("{name}-syntax-table");
                let default_abbrev_table_name = format!("{name}-abbrev-table");
                if self.lookup_var(&map_name, &Vec::new()).is_none() {
                    let stored = Self::stored_value(crate::lisp::primitives::keymap_placeholder(
                        Some(&map_name),
                    ));
                    self.globals_index.insert(map_name.clone(), stored.clone());
                    self.globals.push((map_name.clone(), stored));
                }
                let mut index = 4;
                if matches!(
                    items.get(index),
                    Some(Value::String(_) | Value::StringObject(_))
                ) {
                    index += 1;
                }
                let mut after_hook = None;
                let mut interactive = true;
                let mut syntax_table = Value::Symbol(default_syntax_table_name.clone());
                let mut abbrev_table = Value::Symbol(default_abbrev_table_name.clone());
                let mut declare_syntax_table = true;
                let mut declare_abbrev_table = true;
                while let Some(Value::Symbol(keyword)) = items.get(index)
                    && keyword.starts_with(':')
                {
                    match keyword.as_str() {
                        ":after-hook" => after_hook = items.get(index + 1).cloned(),
                        ":interactive" => {
                            interactive = items.get(index + 1).is_some_and(Value::is_truthy);
                        }
                        ":syntax-table" => {
                            syntax_table = items.get(index + 1).cloned().unwrap_or(Value::Nil);
                            declare_syntax_table = false;
                        }
                        ":abbrev-table" => {
                            abbrev_table = items.get(index + 1).cloned().unwrap_or(Value::Nil);
                            declare_abbrev_table = false;
                        }
                        _ => {}
                    }
                    index += 2;
                }
                for variable in [
                    hook_name.as_str(),
                    map_name.as_str(),
                    default_syntax_table_name.as_str(),
                    default_abbrev_table_name.as_str(),
                ] {
                    self.mark_special_variable(variable);
                }
                if self.lookup_var(&hook_name, &Vec::new()).is_none() {
                    self.set_global_binding(&hook_name, Value::Nil);
                }
                if declare_syntax_table
                    && self
                        .lookup_var(&default_syntax_table_name, &Vec::new())
                        .is_none()
                {
                    let Value::CharTable(table_id) =
                        self.make_char_table(Some("syntax-table".into()), Value::Nil)
                    else {
                        unreachable!("make_char_table returns a char table");
                    };
                    self.set_char_table_parent(table_id, Some(self.standard_syntax_table_id()))?;
                    self.set_global_binding(&default_syntax_table_name, Value::CharTable(table_id));
                }
                if declare_abbrev_table
                    && !self
                        .lookup_var(&default_abbrev_table_name, &Vec::new())
                        .is_some_and(|value| {
                            crate::lisp::primitives::is_abbrev_table_value(self, &value)
                        })
                {
                    let table = crate::lisp::primitives::make_runtime_abbrev_table(
                        self,
                        Some(&default_abbrev_table_name),
                        Value::Nil,
                    );
                    self.set_global_binding(&default_abbrev_table_name, table);
                    crate::lisp::primitives::register_abbrev_table_symbol(
                        self,
                        &default_abbrev_table_name,
                    );
                }
                let mut delayed_body = Vec::new();
                // GNU expands to (delay-mode-hooks (,(or PARENT
                // 'kill-all-local-variables)) ...): the parent chain ends
                // in a mode that resets buffer locals.
                if let Some(parent) = parent {
                    delayed_body.push(Value::list([Value::Symbol(parent.to_string())]));
                } else {
                    delayed_body.push(Value::list([Value::Symbol(
                        "kill-all-local-variables".into(),
                    )]));
                }
                delayed_body.push(Value::list([
                    Value::Symbol("use-local-map".into()),
                    Value::Symbol(map_name.clone()),
                ]));
                if !syntax_table.is_nil() {
                    delayed_body.push(Value::list([
                        Value::Symbol("set-syntax-table".into()),
                        syntax_table,
                    ]));
                }
                if !abbrev_table.is_nil() {
                    delayed_body.push(Value::list([
                        Value::Symbol("setq-local".into()),
                        Value::Symbol("local-abbrev-table".into()),
                        abbrev_table,
                    ]));
                }
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
                let mut body = Vec::new();
                if interactive {
                    body.push(Value::list([Value::Symbol("interactive".into())]));
                }
                body.push(Value::list(
                    std::iter::once(Value::Symbol("delay-mode-hooks".into()))
                        .chain(delayed_body)
                        .collect::<Vec<_>>(),
                ));
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
                    Some(Value::Lambda(
                        Vec::new(),
                        body.into(),
                        shared_env(Vec::new()),
                    )),
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

        // GNU cl--struct-name-p: reject nil, keywords, and built-in type
        // names with the same wrong-type-argument signal as cl-defstruct.
        if name == "nil"
            || name.starts_with(':')
            || crate::lisp::primitives::is_builtin_class_name(&name)
        {
            return Err(LispError::SignalValue(Value::list([
                Value::Symbol("wrong-type-argument".into()),
                Value::Symbol("cl-struct-name-p".into()),
                Value::Symbol(name),
                Value::Symbol("name".into()),
            ])));
        }

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
                let parent_defaults = self
                    .get_symbol_property(parent, "emaxx-struct-defaults")
                    .and_then(|value| value.to_vec().ok())
                    .unwrap_or_default();
                for (index, slot) in parent_names.into_iter().enumerate() {
                    if let Value::Symbol(slot_name) = slot {
                        slot_specs.push((
                            slot_name,
                            parent_defaults.get(index).cloned().unwrap_or(Value::Nil),
                        ));
                    }
                }
                // GNU :include accepts replacement slot specs after the
                // parent name.  Preserve their initforms for each child
                // constructor; ERC uses dynamically evaluated send/insert
                // defaults here.
                for override_spec in parts.iter().skip(2) {
                    let Ok(override_parts) = override_spec.to_vec() else {
                        continue;
                    };
                    let Some(Value::Symbol(slot_name)) = override_parts.first() else {
                        continue;
                    };
                    if let Some((_, default)) = slot_specs
                        .iter_mut()
                        .find(|(existing, _)| existing == slot_name)
                    {
                        *default = override_parts.get(1).cloned().unwrap_or(Value::Nil);
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
        let mut vector_backed = false;
        // `:named' vector structs keep their tag slot and stay records;
        // only unnamed `:type vector' structs become plain vectors.
        let struct_named = options
            .iter()
            .any(|option| matches!(option, Value::Symbol(keyword) if keyword == ":named"));
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
                        let (params, aux_bindings, direct_lambda) = parts
                            .get(2)
                            .and_then(|value| value.to_vec().ok())
                            .map(parse_cl_defstruct_constructor_params)
                            .unwrap_or_else(|| {
                                (
                                    std::iter::once("&key".to_string())
                                        .chain(slot_names.iter().cloned())
                                        .collect::<Vec<_>>(),
                                    Vec::new(),
                                    false,
                                )
                            });
                        let docstring = parts.get(3).and_then(|value| match value {
                            Value::String(text) => Some(text.clone()),
                            Value::StringObject(state) => Some(state.borrow().text.clone()),
                            _ => None,
                        });
                        constructors.push((
                            constructor_name.clone(),
                            params,
                            aux_bindings,
                            docstring,
                            direct_lambda,
                        ));
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
                    // GNU stores unnamed vector structs as plain vectors
                    // (ewoc's node type walks them with raw `aref').
                    if matches!(parts.get(1), Some(Value::Symbol(kind)) if kind == "vector")
                        && !struct_named
                    {
                        vector_backed = true;
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
                .any(|(constructor_name, _, _, _, _)| constructor_name == &default_constructor_name)
        {
            constructors.push((
                default_constructor_name,
                std::iter::once("&key".to_string())
                    .chain(slot_names.iter().cloned())
                    .collect::<Vec<_>>(),
                Vec::new(),
                None,
                false,
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
        let slot_defaults_value = Value::list([
            Value::Symbol("quote".into()),
            Value::list(slot_defaults.clone()),
        ]);

        self.put_symbol_property(&name, "emaxx-struct-slots", slot_names_list.clone());
        self.put_symbol_property(
            &name,
            "emaxx-struct-defaults",
            Value::list(slot_defaults.clone()),
        );
        // GNU cl-struct-sequence-type: list / vector / nil (record).
        self.put_symbol_property(
            &name,
            "emaxx-struct-sequence-type",
            if list_backed {
                Value::Symbol("list".into())
            } else if vector_backed {
                Value::Symbol("vector".into())
            } else {
                Value::Nil
            },
        );
        // GNU cl-struct-slot-info shape: ((cl-tag-slot) DESC...) where each
        // DESC is the original (NAME DEFAULT OPTS...) slot spec; inherited
        // slots come first.  Consumed by the Lisp cl-struct-slot-* helpers.
        {
            let mut descs = vec![Value::list([Value::Symbol("cl-tag-slot".into())])];
            for parent in &parent_names {
                if let Some(parent_descs) =
                    self.get_symbol_property(parent, "emaxx-struct-slot-descs")
                    && let Ok(parent_items) = parent_descs.to_vec()
                {
                    descs.extend(parent_items.into_iter().skip(1));
                }
            }
            for slot in &items[2..] {
                match slot {
                    Value::Symbol(slot_name) => {
                        descs.push(Value::list([Value::Symbol(slot_name.clone())]));
                    }
                    Value::Cons(_, _) => descs.push(slot.clone()),
                    _ => {}
                }
            }
            self.put_symbol_property(&name, "emaxx-struct-slot-descs", Value::list(descs));
        }
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
                    ])]
                    .into(),
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
                ])]
                .into(),
                shared_env(Vec::new()),
            )),
        );

        let accessor_alist = Value::list(
            slot_names
                .iter()
                .map(|slot_name| {
                    Value::cons(
                        Value::Symbol(slot_name.clone()),
                        Value::Symbol(format!("{conc_name}{slot_name}")),
                    )
                })
                .collect::<Vec<_>>(),
        );
        self.put_symbol_property(&name, "emaxx-struct-accessors", accessor_alist);
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
                        if list_backed {
                            Value::T
                        } else if vector_backed {
                            Value::list([
                                Value::Symbol("quote".into()),
                                Value::Symbol("vector".into()),
                            ])
                        } else {
                            Value::Nil
                        },
                    ])]
                    .into(),
                    shared_env(Vec::new()),
                )),
            );
            // GNU's default inline cl-defstruct accessors publish a compiler
            // macro that lowers the logical accessor to its physical
            // sequence/record slot.  gv.el deliberately relies on that
            // metadata to implement `setf'.
            let (operator, physical_index) = if list_backed && !struct_named {
                ("nth", index)
            } else if vector_backed {
                ("aref", index)
            } else {
                // GNU records expose their type tag at aref index zero.
                ("aref", index + 1)
            };
            self.install_struct_accessor_compiler_macro(&accessor_name, operator, physical_index);
            // GNU cl-defstruct also emits `(defun (setf ACCESSOR) (val cl-x)
            // ...)' so gv's `(funcall #'(setf ACCESSOR) VAL X)' fallback
            // works for struct slots.
            self.set_function_binding(
                &format!("(setf {accessor_name})"),
                Some(Value::Lambda(
                    vec!["val".into(), "cl-x".into()],
                    vec![Value::list([
                        Value::Symbol("setf".into()),
                        Value::list([
                            Value::Symbol(accessor_name.clone()),
                            Value::Symbol("cl-x".into()),
                        ]),
                        Value::Symbol("val".into()),
                    ])]
                    .into(),
                    shared_env(Vec::new()),
                )),
            );
        }

        for (constructor_name, params, aux_bindings, docstring, direct_lambda) in constructors {
            self.put_symbol_property(
                &constructor_name,
                "emaxx-function-arglist",
                Value::list(params.iter().cloned().map(Value::Symbol)),
            );
            let params_for_make = if aux_bindings.is_empty() {
                params.clone()
            } else {
                std::iter::once("&key".to_string())
                    .chain(slot_names.iter().cloned())
                    .collect::<Vec<_>>()
            };
            let params_list = Value::list(params_for_make.into_iter().map(Value::Symbol));
            let params_value = Value::list([Value::Symbol("quote".into()), params_list]);
            let call_args = if aux_bindings.is_empty() && direct_lambda {
                Value::list(
                    std::iter::once(Value::Symbol("list".into())).chain(
                        params
                            .iter()
                            .filter(|param| !param.starts_with('&'))
                            .cloned()
                            .map(Value::Symbol),
                    ),
                )
            } else if aux_bindings.is_empty() {
                Value::Symbol("args".into())
            } else {
                // GNU fills slots only from constructor params and &aux
                // bindings whose names match slot names; the let* below
                // binds them all, so pass those as pure keywords (raw args
                // must not leak into slot positions).
                let mut binding_names = params
                    .iter()
                    .filter(|param| !param.starts_with('&'))
                    .cloned()
                    .collect::<Vec<_>>();
                for (aux_name, _) in &aux_bindings {
                    if !binding_names.contains(aux_name) {
                        binding_names.push(aux_name.clone());
                    }
                }
                let slot_keywords = binding_names
                    .into_iter()
                    .filter(|binding| slot_names.contains(binding))
                    .flat_map(|binding| {
                        [Value::Symbol(format!(":{binding}")), Value::Symbol(binding)]
                    })
                    .collect::<Vec<_>>();
                Value::list(
                    std::iter::once(Value::Symbol("list".into()))
                        .chain(slot_keywords)
                        .collect::<Vec<_>>(),
                )
            };
            let mut make_items = vec![
                Value::Symbol("emaxx-struct-make".into()),
                struct_name.clone(),
                slot_names_value.clone(),
                slot_defaults_value.clone(),
                params_value,
                call_args,
            ];
            if vector_backed {
                make_items.push(Value::list([
                    Value::Symbol("quote".into()),
                    Value::Symbol("vector".into()),
                ]));
            } else if list_backed && !struct_named {
                // GNU stores unnamed `:type list' structs as plain lists
                // (testcover walks edebug--form-data entries with `car').
                make_items.push(Value::list([
                    Value::Symbol("quote".into()),
                    Value::Symbol("list".into()),
                ]));
            }
            let make_form = Value::list(make_items);
            let body = if aux_bindings.is_empty() {
                make_form
            } else {
                let let_bindings = if direct_lambda {
                    Value::list(
                        aux_bindings
                            .into_iter()
                            .map(|(name, form)| Value::list([Value::Symbol(name), form])),
                    )
                } else {
                    Value::list(cl_defstruct_constructor_aux_let_bindings(
                        &params,
                        aux_bindings,
                    ))
                };
                Value::list([Value::Symbol("let*".into()), let_bindings, make_form])
            };
            let lambda_body = match docstring {
                Some(doc) => vec![Value::String(doc), body],
                None => vec![body],
            };
            self.set_function_binding(
                &constructor_name,
                Some(Value::Lambda(
                    if direct_lambda {
                        params
                    } else {
                        vec!["&rest".into(), "args".into()]
                    },
                    lambda_body.into(),
                    shared_env(Vec::new()),
                )),
            );
        }

        Ok(Value::Symbol(name))
    }

    pub(super) fn install_struct_accessor_compiler_macro(
        &mut self,
        accessor: &str,
        operator: &str,
        physical_index: usize,
    ) {
        let compiler_macro_name = format!("{accessor}--cmacro");
        let expanded_form = if operator == "nth" {
            Value::list([
                Value::Symbol("list".into()),
                quoted_literal(&Value::Symbol("nth".into())),
                Value::Integer(physical_index as i64),
                Value::Symbol("object".into()),
            ])
        } else {
            Value::list([
                Value::Symbol("list".into()),
                quoted_literal(&Value::Symbol("aref".into())),
                Value::Symbol("object".into()),
                Value::Integer(physical_index as i64),
            ])
        };
        self.set_function_binding(
            &compiler_macro_name,
            Some(Value::Lambda(
                vec!["_form".into(), "object".into()],
                vec![expanded_form].into(),
                shared_env(Vec::new()),
            )),
        );
        self.put_symbol_property(
            accessor,
            "compiler-macro",
            Value::Symbol(compiler_macro_name),
        );
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
            self.put_symbol_property(&name, "emaxx-gv-setter", Value::Symbol(setter.clone()));
            // GNU's defun-declarations-alist also registers the gv-expander
            // through gv.el, so GNU elisp places (gv-letplace in nadvice's
            // advice--add-function) find it instead of the void
            // `(setf NAME)' fallback.  Only when gv.el is loadable.
            if self.resolve_load_target("gv").is_some() {
                let form = format!("(gv-define-simple-setter {name} {setter})");
                if let Ok(forms) = crate::lisp::reader::Reader::new(&form).read_all() {
                    for form in forms {
                        let _ = self.eval(&form, env);
                    }
                }
            }
        }
        if body_start < normalized_forms.len()
            && let Some(handler) = function_declare_gv_setter_handler(&normalized_forms[body_start])
            && let Ok(handler_items) = handler.to_vec()
            && handler_items.len() >= 3
            && matches!(handler_items.first(), Some(Value::Symbol(head)) if head == "lambda")
            && let Ok(handler_params) = handler_items[1].to_vec()
        {
            // A `(gv-setter (lambda (VALUE) ...))' declaration is a macro
            // generator: VALUE and the accessor's arguments are unevaluated
            // forms, and its result is the store expression.
            let mut setter_params = handler_params;
            setter_params.extend(params.iter().cloned().map(Value::Symbol));
            let setter_form = Value::list(
                std::iter::once(Value::Symbol("lambda".into()))
                    .chain(std::iter::once(Value::list(setter_params)))
                    .chain(handler_items[2..].iter().cloned())
                    .collect::<Vec<_>>(),
            );
            let setter = self.eval(&setter_form, env)?;
            self.put_symbol_property(&name, "emaxx-gv-setter-handler", setter);

            // This is the public half of GNU's `gv-setter' declaration
            // contract.  `gv--defun-declaration' lowers the declaration to
            // `gv-define-setter', which installs a `gv-expander'.  Keeping
            // only the private native handler above makes an Emaxx-native
            // `setf' work but leaves GNU macros (notably ERT's `should') to
            // generate a nonexistent `(setf NAME)' function call.
            //
            // Install the same expander shape directly.  It is safe before
            // gv.el is loaded: the closure calls `gv--defsetter' only when
            // GNU's `gv-get' later invokes it, at which point gv.el is
            // necessarily present.
            let gv_expander_form = Value::list([
                Value::Symbol("lambda".into()),
                Value::list([
                    Value::Symbol("do".into()),
                    Value::Symbol("&rest".into()),
                    Value::Symbol("args".into()),
                ]),
                Value::list([
                    Value::Symbol("gv--defsetter".into()),
                    Value::list([Value::Symbol("quote".into()), Value::Symbol(name.clone())]),
                    setter_form,
                    Value::Symbol("do".into()),
                    Value::Symbol("args".into()),
                ]),
            ]);
            let gv_expander = self.eval(&gv_expander_form, env)?;
            self.put_symbol_property(&name, "gv-expander", gv_expander);
        }
        if body_start < normalized_forms.len()
            && let Some(handler) = function_declare_gv_expander(&normalized_forms[body_start])
        {
            let expander = match handler {
                // GNU's symbolic declaration form installs the named
                // function itself.  `gv-synthetic-place' uses `funcall', so
                // ignoring this case makes GNU setf fall back to the bogus
                // `(setf gv-synthetic-place)' function name.
                Value::Symbol(handler_name) => self.lookup_function(&handler_name, env)?,
                Value::Cons(_, _) => {
                    let handler_items = handler.to_vec()?;
                    if handler_items.len() < 3
                        || !matches!(handler_items.first(), Some(Value::Symbol(head)) if head == "lambda")
                    {
                        return Err(LispError::Signal("Invalid gv-expander declaration".into()));
                    }
                    let mut expander_params = handler_items[1].to_vec()?;
                    for param in &params {
                        expander_params.push(Value::Symbol(param.clone()));
                    }
                    let expander_form = Value::list(
                        std::iter::once(Value::Symbol("lambda".into()))
                            .chain(std::iter::once(Value::list(expander_params)))
                            .chain(handler_items[2..].iter().cloned())
                            .collect::<Vec<_>>(),
                    );
                    self.eval(&expander_form, env)?
                }
                _ => return Err(LispError::Signal("Invalid gv-expander declaration".into())),
            };
            self.put_symbol_property(&name, "gv-expander", expander);
        }
        let body: Vec<Value> = normalized_forms[body_start..].to_vec();
        if crate::lisp::primitives::prefer_builtin_override(&name) {
            self.push_function_binding(&name, Value::BuiltinFunc(name.clone()));
            self.record_definition_in_load_history("defun", &name);
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
        let closure_env = shared_env(env.clone());
        if self
            .lookup_var("lexical-binding", env)
            .is_some_and(|value| value.is_truthy())
        {
            self.mark_lexical_closure_env(&closure_env);
        }
        let lambda = Value::Lambda(params, body.into(), closure_env);
        // GNU defalias routes advised names through the symbol's
        // `defalias-fset-function' (nadvice's advice--defalias-fset), which
        // re-applies pending or existing advice around the new definition.
        if self.defalias_fset_function_handles(&name, &lambda, env) {
            self.record_definition_in_load_history("defun", &name);
            self.advice_note_new_definition(&name);
            return Ok(Value::Symbol(name));
        }
        // A defun over a macro name erases the macro (GNU function cell).
        self.shadow_macro_binding(&name);
        self.push_function_binding(&name, lambda);
        self.record_definition_in_load_history("defun", &name);
        self.advice_note_new_definition(&name);
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
                let local_macros = self.parse_cl_macrolet_bindings(&items[1], env)?;
                let (local_start, local_count) = self.push_local_macros(&local_macros);
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
                self.drain_local_macros(local_start, local_count);
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
        let lambda = Value::Lambda(params, body.into(), shared_env(env.clone()));
        self.put_symbol_property(&name, "cl-deftype-handler", lambda);
        Ok(Value::Symbol(name))
    }

    pub(super) fn sf_defclass(&mut self, items: &[Value]) -> Result<Value, LispError> {
        let Some(name) = items.get(1).and_then(|value| value.as_symbol().ok()) else {
            return Ok(Value::Nil);
        };
        let mut parents = items
            .get(2)
            .map(Value::to_vec)
            .transpose()?
            .unwrap_or_default()
            .into_iter()
            .filter_map(|value| value.as_symbol().ok().map(str::to_string))
            .collect::<Vec<_>>();
        // GNU adopts `eieio-default-superclass' as the implicit parent of
        // parentless classes, which is what makes eieio's default generic
        // methods (make-instance, initialize-instance, slot-missing, ...)
        // applicable to every class.
        if parents.is_empty()
            && name != "eieio-default-superclass"
            && self.class_value("eieio-default-superclass").is_some()
        {
            parents.push("eieio-default-superclass".to_string());
        }
        let slot_specs = items
            .get(3)
            .map(Value::to_vec)
            .transpose()?
            .unwrap_or_default();
        let options = items.get(4..).unwrap_or(&[]).to_vec();
        let abstract_class = options.windows(2).any(|pair| {
            matches!(&pair[0], Value::Symbol(option) if option == ":abstract")
                && pair[1].is_truthy()
        });
        let class_record = self.register_class(name, parents, slot_specs, options);
        // ClassState is also used by the compact cl-defstruct facade, so
        // retain the producer's semantic category explicitly.  This matters
        // before the full GNU EIEIO hierarchy has been bootstrapped and an
        // implicit eieio-default-superclass parent is available.
        self.put_symbol_property(name, "emaxx-eieio-class", Value::T);
        classes::install_eieio_slot_accessors(self, name)?;
        // GNU validates slot declarations at class-definition time: a
        // constant initform must match the slot :type, and a subclass may
        // not change an inherited slot's type or protection.
        primitives::eieio_validate_class_slots(self, name, &mut Env::new())?;
        // GNU eieio classes cache a default object whose record tag is the
        // class object itself, so raw-printing a class (or an object created
        // with `eieio-backward-compatibility' nil) emits a circular
        // reference marker.  Model that cache as an extra class-record slot
        // holding a default-initialized instance tagged with the class
        // object (GNU's `eieio-set-defaults' fills the cache at defclass
        // time, ignoring initform evaluation errors).
        let slots = primitives::eieio_slot_specs(self, name).unwrap_or_default();
        let mut cache_values = Vec::with_capacity(slots.len());
        for slot in &slots {
            let value = match (&slot.initform, slot.class_allocated) {
                (Some(initform), false) => {
                    self.eval(initform, &mut Env::new()).unwrap_or(Value::Nil)
                }
                _ => Value::Unbound,
            };
            cache_values.push(value);
            if slot.class_allocated {
                // Class-allocated slots evaluate their initform once, at
                // class-definition time, into per-class shared storage.
                let shared = match &slot.initform {
                    Some(initform) => self.eval(initform, &mut Env::new()).unwrap_or(Value::Nil),
                    None => Value::Unbound,
                };
                self.put_symbol_property(
                    name,
                    &primitives::eieio_class_allocation_property(&slot.name),
                    shared,
                );
            }
        }
        let cache = self.create_record(name, cache_values);
        if let Value::Record(cache_id) = &cache {
            self.mark_class_object_tagged_record(*cache_id);
        }
        if let Value::Record(class_record_id) = &class_record
            && let Some(record) = self.find_record_mut(*class_record_id)
        {
            record.slots.push(cache);
        }
        // Constructing through `make-instance' lets methods registered on
        // the generic (eieio's static constructor methods) participate;
        // without any methods the builtin constructs directly.  GNU's
        // `defclass' generates an erroring constructor for :abstract
        // classes instead.
        if abstract_class {
            self.set_function_binding(
                name,
                Some(Value::Lambda(
                    vec!["&rest".into(), "_ignore".into()],
                    vec![Value::list([
                        Value::Symbol("error".into()),
                        Value::String(format!("Class {name} is abstract")),
                    ])]
                    .into(),
                    shared_env(Vec::new()),
                )),
            );
        } else {
            self.set_function_binding(
                name,
                Some(Value::Lambda(
                    vec!["&rest".into(), "initargs".into()],
                    vec![Value::list([
                        Value::Symbol("apply".into()),
                        Value::list([
                            Value::Symbol("function".into()),
                            Value::Symbol("make-instance".into()),
                        ]),
                        Value::list([
                            Value::Symbol("quote".into()),
                            Value::Symbol(name.to_string()),
                        ]),
                        Value::Symbol("initargs".into()),
                    ])]
                    .into(),
                    shared_env(Vec::new()),
                )),
            );
        }
        // GNU's generated `NAME-p' matches the exact class
        // (`eieio-make-class-predicate'); `NAME--eieio-childp' accepts
        // subclasses (`eieio-make-child-predicate').
        self.set_function_binding(
            &format!("{name}-p"),
            Some(Value::Lambda(
                vec!["object".into()],
                vec![Value::list([
                    Value::Symbol("and".into()),
                    Value::list([
                        Value::Symbol("eieio-object-p".into()),
                        Value::Symbol("object".into()),
                    ]),
                    Value::list([
                        Value::Symbol("same-class-p".into()),
                        Value::Symbol("object".into()),
                        Value::list([
                            Value::Symbol("quote".into()),
                            Value::Symbol(name.to_string()),
                        ]),
                    ]),
                ])]
                .into(),
                shared_env(Vec::new()),
            )),
        );
        self.set_function_binding(
            &format!("{name}--eieio-childp"),
            Some(Value::Lambda(
                vec!["object".into()],
                vec![Value::list([
                    Value::Symbol("emaxx-class-p".into()),
                    Value::list([
                        Value::Symbol("quote".into()),
                        Value::Symbol(name.to_string()),
                    ]),
                    Value::Symbol("object".into()),
                ])]
                .into(),
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
        // GNU cl--transform-lambda: a leading `(:documentation EXPR)' form is
        // evaluated at definition time and becomes the docstring.
        if let Some(first) = executable_body.first().cloned()
            && let Ok(parts) = first.to_vec()
            && parts.len() == 2
            && matches!(parts.first(), Some(Value::Symbol(head)) if head == ":documentation")
        {
            let doc = self.eval(&parts[1], env)?;
            let doc_text = match doc {
                Value::String(text) => Some(text),
                Value::StringObject(state) => Some(state.borrow().text.clone()),
                _ => None,
            };
            if let Some(text) = doc_text {
                lowered_body.insert(0, Value::String(text));
            }
            executable_body.remove(0);
            if executable_body.is_empty() {
                executable_body.push(Value::Nil);
            }
        }
        // CL argument bindings are sequential: required destructuring makes
        // names visible to optional defaults, optionals consume the raw rest
        // list left-to-right, then rest/key and finally &aux are bound.
        // Build the wrappers inside-out so their runtime order stays explicit.
        for (pattern, init) in lowered_cl_defun.aux_bindings.into_iter().rev() {
            let mut wrapped = if matches!(pattern, Value::Symbol(_)) {
                vec![
                    Value::Symbol("--emaxx-lexical-let*".into()),
                    Value::list([Value::list([pattern, init])]),
                ]
            } else {
                vec![Value::Symbol("cl-destructuring-bind".into()), pattern, init]
            };
            wrapped.append(&mut executable_body);
            executable_body = vec![Value::list(wrapped)];
        }
        if !lowered_cl_defun.keyword_bindings.is_empty() {
            let mut let_bindings = Vec::new();
            for binding in &lowered_cl_defun.keyword_bindings {
                let present_name =
                    format!("emaxx--cl-defun-{}-{}-present", name, binding.variable_name);
                let remaining_args_name =
                    lowered_cl_defun
                        .remaining_args_name
                        .clone()
                        .ok_or_else(|| {
                            LispError::Signal(
                                "cl-defun keyword lowering lost its rest source".into(),
                            )
                        })?;
                // Explicit CL key names need not be keywords (ERC uses a
                // bare `--interactive-env--' key).  Quote every lookup key
                // so non-keyword symbols are compared as data, not read as
                // variables; quoting regular :keywords is equivalent.
                let keyword_symbol = Value::list([
                    Value::Symbol("quote".into()),
                    Value::Symbol(binding.keyword_name.clone()),
                ]);
                let keyword_source = Value::Symbol(remaining_args_name);
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
            // GNU cl--do-arglist gives dynamic-variable arguments lexical
            // aliases (bug#47552): function arguments are always statically
            // scoped, so the generated bindings must stay lexical even when
            // a key shares its name with a special variable.
            let mut wrapped = vec![
                Value::Symbol("--emaxx-lexical-let*".into()),
                Value::list(let_bindings),
            ];
            wrapped.append(&mut executable_body);
            executable_body = vec![Value::list(wrapped)];
        }

        if let Some(pattern) = lowered_cl_defun.rest_binding {
            let remaining_args_name =
                lowered_cl_defun
                    .remaining_args_name
                    .clone()
                    .ok_or_else(|| {
                        LispError::Signal("cl-defun rest lowering lost its rest source".into())
                    })?;
            let mut wrapped = if matches!(pattern, Value::Symbol(_)) {
                vec![
                    Value::Symbol("--emaxx-lexical-let*".into()),
                    Value::list([Value::list([pattern, Value::Symbol(remaining_args_name)])]),
                ]
            } else {
                vec![
                    Value::Symbol("cl-destructuring-bind".into()),
                    pattern,
                    Value::Symbol(remaining_args_name),
                ]
            };
            wrapped.append(&mut executable_body);
            executable_body = vec![Value::list(wrapped)];
        }

        if lowered_cl_defun.reject_remaining_args {
            let remaining_args_name =
                lowered_cl_defun
                    .remaining_args_name
                    .clone()
                    .ok_or_else(|| {
                        LispError::Signal("cl-defun optional lowering lost its rest source".into())
                    })?;
            let raw_rest_param = lowered_cl_defun.raw_rest_param.clone().ok_or_else(|| {
                LispError::Signal("cl-defun optional lowering lost its raw argument list".into())
            })?;
            executable_body.insert(
                0,
                Value::list([
                    Value::Symbol("if".into()),
                    Value::Symbol(remaining_args_name),
                    Value::list([
                        Value::Symbol("signal".into()),
                        quoted_literal(&Value::Symbol("wrong-number-of-arguments".into())),
                        Value::list([
                            Value::Symbol("list".into()),
                            quoted_literal(&Value::Symbol(name.clone())),
                            Value::list([
                                Value::Symbol("+".into()),
                                Value::Integer(lowered_cl_defun.required_count as i64),
                                Value::list([
                                    Value::Symbol("length".into()),
                                    Value::Symbol(raw_rest_param),
                                ]),
                            ]),
                        ]),
                    ]),
                    Value::Nil,
                ]),
            );
        }

        for (index, binding) in lowered_cl_defun
            .optional_bindings
            .into_iter()
            .enumerate()
            .rev()
        {
            let remaining_args_name =
                lowered_cl_defun
                    .remaining_args_name
                    .clone()
                    .ok_or_else(|| {
                        LispError::Signal("cl-defun optional lowering lost its rest source".into())
                    })?;
            let supplied_form = Value::list([
                Value::Symbol("and".into()),
                Value::Symbol(remaining_args_name.clone()),
                Value::T,
            ]);
            let value_form = Value::list([
                Value::Symbol("if".into()),
                Value::Symbol(remaining_args_name.clone()),
                Value::list([
                    Value::Symbol("pop".into()),
                    Value::Symbol(remaining_args_name),
                ]),
                binding.default_value,
            ]);
            let mut let_bindings = Vec::new();
            if let Some(supplied_name) = binding.supplied_name {
                let_bindings.push(Value::list([Value::Symbol(supplied_name), supplied_form]));
            }

            let mut wrapped = if matches!(binding.pattern, Value::Symbol(_)) {
                let_bindings.push(Value::list([binding.pattern, value_form]));
                vec![
                    Value::Symbol("--emaxx-lexical-let*".into()),
                    Value::list(let_bindings),
                ]
            } else {
                let temp_name = format!("emaxx--cl-defun-{name}-optional-{index}");
                let_bindings.push(Value::list([Value::Symbol(temp_name.clone()), value_form]));
                let mut destructured = vec![
                    Value::Symbol("cl-destructuring-bind".into()),
                    binding.pattern,
                    Value::Symbol(temp_name),
                ];
                destructured.append(&mut executable_body);
                executable_body = vec![Value::list(destructured)];
                vec![
                    Value::Symbol("--emaxx-lexical-let*".into()),
                    Value::list(let_bindings),
                ]
            };
            wrapped.append(&mut executable_body);
            executable_body = vec![Value::list(wrapped)];
        }

        if let (Some(raw_rest_param), Some(remaining_args_name)) = (
            lowered_cl_defun.raw_rest_param,
            lowered_cl_defun.remaining_args_name,
        ) {
            let mut wrapped = vec![
                Value::Symbol("--emaxx-lexical-let*".into()),
                Value::list([Value::list([
                    Value::Symbol(remaining_args_name),
                    Value::Symbol(raw_rest_param),
                ])]),
            ];
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
        self.sf_defmacro(&lowered, env)
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
                        // `(declare (gv-expander (lambda (do) ...)))' takes
                        // DO plus the generic's own lambda list, like
                        // `gv--defun-declaration' (map.el's map-elt).
                        if matches!(
                            declaration_parts.first(),
                            Some(Value::Symbol(kind)) if kind == "gv-expander"
                        ) && let Some(handler) = declaration_parts.get(1)
                            && let Ok(handler_items) = handler.to_vec()
                            && handler_items.len() >= 3
                            && matches!(
                                handler_items.first(),
                                Some(Value::Symbol(head)) if head == "lambda"
                            )
                            && let Ok(handler_params) = handler_items[1].to_vec()
                        {
                            let mut expander_params = handler_params;
                            if let Ok(generic_params) = items[2].to_vec() {
                                expander_params.extend(generic_params);
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
                    }
                    body_start += 1;
                }
                ":documentation" => {
                    if let Some(expression) = parts.get(1) {
                        let value = self.eval(expression, env)?;
                        let text = crate::lisp::primitives::string_like(&value)
                            .map(|string| string.text)
                            .ok_or_else(|| {
                                LispError::TypeError("string".into(), value.type_name())
                            })?;
                        let doc = Value::String(text);
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
            let existing_dispatch = self
                .get_symbol_property(&name, "emaxx-cl-defmethod-specializers")
                .and_then(|_| self.lookup_function(&name, env).ok());
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
            if let Some(existing_dispatch) = existing_dispatch
                && let Ok(new_default) = self.lookup_function(&name, env)
                && cl_defmethod_replace_terminal_previous_bindings(&existing_dispatch, &new_default)
            {
                self.set_function_binding(&name, Some(existing_dispatch));
            }
        } else if self.lookup_function(&name, env).is_err() {
            self.set_function_binding(&name, Some(Value::BuiltinFunc("ignore".into())));
        }
        if name == "loadhist-unload-element" {
            // cl-generic.el normally installs this method after defining its
            // record-backed generic engine.  Emaxx keeps generic dispatch in
            // Rust during bootstrap, so install the same Lisp-visible method
            // on that facade and let a tiny host primitive splice its native
            // method wrapper.  `unload-feature' itself remains GNU Lisp.
            self.sf_cl_defmethod(
                &[
                    Value::Symbol("cl-defmethod".into()),
                    Value::Symbol(name.clone()),
                    Value::list([Value::list([
                        Value::Symbol("element".into()),
                        Value::list([
                            Value::Symbol("head".into()),
                            Value::Symbol("cl-defmethod".into()),
                        ]),
                    ])]),
                    Value::list([
                        Value::Symbol("emaxx--cl-generic-remove-loadhist-method".into()),
                        Value::Symbol("element".into()),
                    ]),
                ],
                env,
            )?;
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

    // Expand registered `cl-generic-define-context-rewriter' heads inside a
    // cl-defmethod lambda list's &context section: (erc-obsolete-var VAR
    // SPEC) becomes the rewriter's ((EXPR) SPEC) output.
    fn expand_generic_context_rewriters(
        &mut self,
        lambda_list: &Value,
        env: &mut Env,
    ) -> Result<Option<Value>, LispError> {
        let Ok(entries) = lambda_list.to_vec() else {
            return Ok(None);
        };
        let mut in_context = false;
        let mut changed = false;
        let mut expanded = Vec::with_capacity(entries.len());
        for entry in entries {
            if let Value::Symbol(symbol) = &entry {
                if symbol == "&context" {
                    in_context = true;
                } else if is_lambda_list_keyword(symbol) {
                    in_context = false;
                }
                expanded.push(entry);
                continue;
            }
            if in_context && let Value::Cons(car, _) = &entry {
                let head = match &*car.borrow() {
                    Value::Symbol(head) => Some(head.clone()),
                    _ => None,
                };
                if let Some(head) = head {
                    let rewriter = format!("cl-generic--context-rewriter--{head}");
                    if self.has_macro_binding(&rewriter) {
                        let mut call = entry.to_vec()?;
                        call[0] = Value::Symbol(rewriter);
                        let call_form = Value::list(call);
                        expanded
                            .push(self.macroexpand_1_form_with_environment(&call_form, None, env)?);
                        changed = true;
                        continue;
                    }
                }
            }
            expanded.push(entry);
        }
        Ok(changed.then(|| Value::list(expanded)))
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
        // Rewrite context-rewriter entries before any downstream parsing.
        let items_storage;
        let items = match self.expand_generic_context_rewriters(&items[lambda_list_index], env)? {
            Some(expanded_lambda_list) => {
                let mut rewritten = items.to_vec();
                rewritten[lambda_list_index] = expanded_lambda_list;
                items_storage = rewritten;
                &items_storage
            }
            None => items,
        };
        // GNU permits repeated `_' ignored parameters; the machinery below
        // keys specializers by variable NAME, so uniquify them positionally
        // (erc-networks--id-equal-p specializes two `_' arguments).
        let items_storage_ignored;
        let items = match uniquify_ignored_lambda_list_params(&items[lambda_list_index]) {
            Some(renamed_lambda_list) => {
                let mut rewritten = items.to_vec();
                rewritten[lambda_list_index] = renamed_lambda_list;
                items_storage_ignored = rewritten;
                &items_storage_ignored
            }
            None => items,
        };
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
        // GNU's top-level cl-defmethod macro expands the method lambda body
        // when the definition is evaluated.  Deferring that work until the
        // first dispatch makes macro side effects depend on test/call order
        // (Edebug's pcase-let* matcher consumed gensyms from the first
        // caller's dynamic binding).  Store the already-expanded body, just
        // as sf_defun does for an ordinary loaded function.
        let executable_method_forms = executable_method_forms
            .iter()
            .map(|form| self.macroexpand_all_form_with_environment(form, None, env))
            .collect::<Result<Vec<_>, _>>()?;
        if let Some(doc) = method_doc.clone() {
            self.add_cl_defmethod_documentation(&function_name_from_binding_form(&items[1])?, doc);
        }
        lowered.extend(executable_method_forms.iter().cloned());
        let requested_method_name = function_name_from_binding_form(&items[1])?;
        let method_name = self.canonical_function_name(&requested_method_name, env);
        let generic_doc =
            self.get_symbol_property(&method_name, "emaxx-cl-defgeneric-documentation");
        // Without an explicit `cl-defgeneric', the first method's formals
        // become the generic's, as in GNU.  Later methods then rename their
        // parameters onto these, so stored specializer variables, dispatch
        // conditions, and wrapper parameters all agree even when each
        // `cl-defmethod' picks different argument names.
        if self.cl_defgeneric_lambda_list(&method_name).is_none() {
            self.put_symbol_property(
                &method_name,
                "emaxx-cl-defgeneric-lambda-list",
                lowered_lambda_list.clone(),
            );
        }
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
        {
            let params = self.parse_params(&lowered_lambda_list)?;
            let generic_lambda_list = self
                .cl_defgeneric_lambda_list(&method_name)
                .unwrap_or_else(|| lowered_lambda_list.clone());
            let generic_params = self.parse_params(&generic_lambda_list)?;
            let previous = if self.callable_is_ignore(&previous) {
                cl_generic_no_applicable_function(&method_name, &generic_params)
            } else {
                previous
            };
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
            // Include the specializer key: several :before/:after methods
            // on one generic each capture their own previous chain, and a
            // shared symbol would let a wrapper resolve to itself when the
            // frames stack in one environment.
            let previous_symbol = format!(
                "__emaxx_{}_method_{}_{}",
                if is_before_method { "before" } else { "after" },
                method_name.replace('-', "_"),
                current_stored_specializer.hidden_key()
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
            if let Some((existing_env, _, existing_previous)) =
                cl_defmethod_previous_binding(&previous, &previous_symbol)
            {
                let replacement = Value::Lambda(
                    generic_params.clone(),
                    wrapper_body.clone().into(),
                    shared_env(
                        std::iter::once(vec![
                            (
                                previous_symbol.clone(),
                                Self::stored_value(existing_previous),
                            ),
                            (
                                "__emaxx-qualifier-specializer".into(),
                                current_stored_specializer.metadata_value(),
                            ),
                        ])
                        .chain(env.iter().cloned())
                        .collect(),
                    ),
                );
                let target_id = existing_env.as_ptr() as usize;
                if matches!(&previous, Value::Lambda(_, _, root_env) if root_env.as_ptr() as usize == target_id)
                {
                    self.set_function_binding(&method_name, Some(replacement));
                } else {
                    cl_defmethod_replace_child_environment(&previous, target_id, &replacement);
                }
                self.add_cl_defmethod_specializer(
                    &method_name,
                    current_stored_specializer.metadata_value(),
                );
                return Ok(items[1].clone());
            }
            // Keep the :before/:after wrapper stack ordered with the most
            // specific method outermost: :before bodies then run
            // most-specific-first on the way in, and :after bodies
            // most-specific-last on the way out, like CLOS.
            let mut insertion_parent: Option<(SharedEnv, String)> = None;
            let mut captured_previous = previous.clone();
            while let Some((wrapper_env, previous_name, previous_value, specializer)) =
                cl_defmethod_qualifier_wrapper_parts(&captured_previous)
            {
                let more_specific = specializer
                    .and_then(|metadata| ClDefmethodStoredMethod::parse(&metadata))
                    .map(|stored| {
                        stored.is_more_specific_than(
                            &current_stored_specializer,
                            &precedence_order,
                            self,
                        )
                    })
                    .unwrap_or(false);
                if !more_specific {
                    break;
                }
                insertion_parent = Some((wrapper_env, previous_name));
                captured_previous = previous_value;
            }
            let mut closure_env = Vec::with_capacity(env.len() + 1);
            closure_env.push(vec![
                (previous_symbol, Self::stored_value(captured_previous)),
                (
                    "__emaxx-qualifier-specializer".into(),
                    current_stored_specializer.metadata_value(),
                ),
            ]);
            closure_env.extend(env.iter().cloned());
            let wrapper =
                Value::Lambda(generic_params, wrapper_body.into(), shared_env(closure_env));
            if let Some((previous_env, previous_name)) = insertion_parent {
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
            self.add_cl_defmethod_specializer(
                &method_name,
                current_stored_specializer.metadata_value(),
            );
            return Ok(items[1].clone());
        }
        if let Some(specializer) = ordered_method_specializers.first().cloned() {
            let previous = self
                .lookup_function(&function_name_from_binding_form(&items[1])?, env)
                .unwrap_or_else(|_| Value::BuiltinFunc("ignore".into()));
            let advice_original = cl_defmethod_advice_original_binding(&previous);
            let dispatch_root = advice_original
                .as_ref()
                .map(|(_, _, value)| value.clone())
                .unwrap_or_else(|| previous.clone());
            // Primary methods live below the :before/:after wrapper stack;
            // descend to the boundary so the new wrapper is inserted there
            // and the qualifier stack stays on top.
            let mut qualifier_boundary: Option<(SharedEnv, String)> = None;
            let mut dispatch_root = dispatch_root;
            while let Some((wrapper_env, previous_name, previous_value, _)) =
                cl_defmethod_qualifier_wrapper_parts(&dispatch_root)
            {
                qualifier_boundary = Some((wrapper_env, previous_name));
                dispatch_root = previous_value;
            }
            let previous = if qualifier_boundary.is_some() {
                dispatch_root.clone()
            } else {
                previous
            };
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
            // Use the canonical generic name: methods registered through an
            // alias (old EIEIO's `constructor' -> `make-instance') must get
            // symbols that later splices can reconstruct.
            let previous_method_symbol = format!(
                "__emaxx_previous_method_{}_{}{}",
                method_name.replace('-', "_"),
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
                &method_name,
                &method_previous_symbol,
                &next_default_form,
                if self.callable_is_ignore(&dispatch_previous) {
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
            // GNU replaces a re-registered method (same qualifiers and
            // specializers) in place: swap the stored method body and keep
            // the dispatch chain intact.  Splicing a duplicate wrapper
            // instead makes the two same-condition wrappers point at each
            // other and the chain loops forever once neither matches.
            if let Ok(existing_generic) = self.lookup_function(&method_name, env)
                && cl_defmethod_find_named_binding(&existing_generic, &current_method_symbol)
                    .is_some()
            {
                let old_current =
                    cl_defmethod_find_named_binding(&existing_generic, &current_method_symbol)
                        .unwrap_or(Value::Nil);
                let old_next = match &old_current {
                    Value::Lambda(_, _, old_env) => old_env
                        .borrow()
                        .first()
                        .and_then(|frame| {
                            frame
                                .iter()
                                .find(|(binding, _)| binding == &method_previous_symbol)
                                .map(|(_, value)| value.clone())
                        })
                        .unwrap_or(Value::BuiltinFunc("ignore".into())),
                    _ => Value::BuiltinFunc("ignore".into()),
                };
                let replacement_env = std::iter::once(vec![(
                    method_previous_symbol.clone(),
                    Self::stored_value(old_next),
                )])
                .chain(env.iter().cloned())
                .collect::<Vec<_>>();
                let replacement = Value::Lambda(
                    generic_params.clone(),
                    method_body.clone().into(),
                    shared_env(replacement_env),
                );
                if cl_defmethod_set_named_binding(
                    &existing_generic,
                    &current_method_symbol,
                    &replacement,
                ) {
                    return Ok(items[1].clone());
                }
            }
            let mut dispatch_stop_variables = dispatch_method_specializers
                .iter()
                .map(|specializer| specializer.variable.clone())
                .collect::<Vec<_>>();
            dispatch_stop_variables.extend(more_specific_previous.iter().flat_map(|method| {
                method
                    .specializers
                    .iter()
                    .map(|(variable, _, _)| variable.clone())
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
                method_body.into(),
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
                // The bottom of the chain holds the `ignore' sentinel; the
                // runtime helper applies a real next wrapper and routes the
                // sentinel through `cl-no-applicable-method' like GNU.
                let body = vec![Value::list([
                    Value::Symbol("if".into()),
                    condition,
                    Value::list([
                        Value::Symbol("apply".into()),
                        Value::Symbol(current_method_symbol.clone()),
                        forwarded_args.clone(),
                    ]),
                    Value::list([
                        Value::Symbol("emaxx--cl-generic-apply-next".into()),
                        Value::Symbol(previous_method_symbol.clone()),
                        Value::list([
                            Value::Symbol("quote".into()),
                            Value::Symbol(method_name.clone()),
                        ]),
                        Value::list([
                            Value::Symbol("quote".into()),
                            Value::Symbol("no-applicable".into()),
                        ]),
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
                dispatch_body(top_condition).into(),
                wrapper_closure(previous.clone()),
            );
            let next_wrapper = Value::Lambda(
                wrapper_params,
                dispatch_body(next_condition).into(),
                wrapper_closure(dispatch_previous),
            );
            let wrapper = if specific_splices.is_empty() {
                top_wrapper.clone()
            } else {
                next_wrapper.clone()
            };
            if let Some((around_env, around_previous_name, _)) = around_splice {
                // Splice below an existing :around method: the inserted
                // wrapper must fall through to the around's OLD next chain
                // (next_wrapper), not to the current top binding, which sits
                // above the around and would make the chain cyclic.
                let mut around_env = around_env.borrow_mut();
                for frame in around_env.iter_mut() {
                    if let Some((_, value)) = frame
                        .iter_mut()
                        .find(|(name, _)| name == &around_previous_name)
                    {
                        *value = Self::stored_value(next_wrapper.clone());
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
                } else if let Some((boundary_env, boundary_name)) = qualifier_boundary {
                    let mut boundary_env = boundary_env.borrow_mut();
                    for frame in boundary_env.iter_mut() {
                        if let Some((_, value)) =
                            frame.iter_mut().find(|(name, _)| name == &boundary_name)
                        {
                            *value = Self::stored_value(top_wrapper);
                            break;
                        }
                    }
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
            && !self.callable_is_ignore(&previous)
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
                executable_method_forms.clone().into(),
                shared_env(env.clone()),
            );
            if cl_defmethod_replace_ignore_previous_bindings(self, &previous, &base_method)
                || cl_defmethod_replace_terminal_previous_bindings(&previous, &base_method)
            {
                return Ok(items[1].clone());
            }
        }
        let is_around_qualifier = items[2..lambda_list_index]
            .iter()
            .any(|value| matches!(value, Value::Symbol(name) if name == ":around"));
        let result = if method_specializers.is_empty()
            && is_around_qualifier
            && let Ok(previous) = self.lookup_function(&method_name, env)
            && !self.callable_is_ignore(&previous)
        {
            // A specializer-less :around wraps whatever the generic
            // currently runs (often the cl-defgeneric default body), with
            // `cl-call-next-method' chaining to it (erc-stamp--current-time).
            let params = self.parse_params(&lowered_lambda_list)?;
            let fixed_params = lambda_list_fixed_params(&params);
            let rest_param = lambda_list_rest_param_from_params(&params);
            let mut default_args = vec![Value::Symbol("list".into())];
            default_args.extend(
                fixed_params
                    .iter()
                    .map(|param| Value::Symbol(param.clone())),
            );
            let default_form = if let Some(rest_param) = &rest_param {
                Value::list([
                    Value::Symbol("append".into()),
                    Value::list(default_args),
                    Value::Symbol(rest_param.clone()),
                ])
            } else {
                Value::list(default_args)
            };
            let previous_symbol = format!(
                "__emaxx_previous_method_{}_around_class_t_method",
                method_name.replace('-', "_")
            );
            let body = rewrite_cl_call_next_method_forms(
                &executable_method_forms,
                &method_name,
                &previous_symbol,
                &default_form,
                Value::T,
            )?;
            let mut closure_env = env.clone();
            closure_env.push(vec![(previous_symbol, Self::stored_value(previous))]);
            let wrapper = Value::Lambda(params, body.into(), shared_env(closure_env));
            self.set_function_binding(&method_name, Some(wrapper));
            Ok(items[1].clone())
        } else if method_specializers.is_empty() {
            // If a specializer-less :around wrapper is already installed,
            // this primary must become the around's next method instead of
            // clobbering the wrapper (e.g. erc-stamp--current-time).
            let previous_symbol = format!(
                "__emaxx_previous_method_{}_around_class_t_method",
                method_name.replace('-', "_")
            );
            let existing_around = self
                .lookup_function(&method_name, env)
                .ok()
                .filter(|previous| match previous {
                    Value::Lambda(_, _, closure) => closure
                        .borrow()
                        .iter()
                        .any(|frame| frame.iter().any(|(name, _)| name == &previous_symbol)),
                    _ => false,
                });
            let result = self.sf_cl_defun(&lowered, env);
            if let Some(around) = existing_around
                && result.is_ok()
                && let Ok(new_primary) = self.lookup_function(&method_name, env)
            {
                if let Value::Lambda(_, _, closure) = &around {
                    for frame in closure.borrow_mut().iter_mut() {
                        if let Some(slot) =
                            frame.iter_mut().find(|(name, _)| name == &previous_symbol)
                        {
                            slot.1 = Self::stored_value(new_primary.clone());
                        }
                    }
                }
                self.set_function_binding(&method_name, Some(around));
            }
            result
        } else {
            let mut direct_lowered = lowered[..3].to_vec();
            let direct_params = self.parse_params(&lowered_lambda_list)?;
            let mut direct_default_args = vec![Value::Symbol("list".into())];
            direct_default_args.extend(
                direct_params
                    .iter()
                    .take_while(|param| !param.starts_with('&'))
                    .map(|param| Value::Symbol(param.clone())),
            );
            let direct_rest_param = lambda_list_rest_param_from_params(&direct_params);
            let direct_default_form = if let Some(rest_param) = &direct_rest_param {
                Value::list([
                    Value::Symbol("append".into()),
                    Value::list(direct_default_args),
                    Value::Symbol(rest_param.clone()),
                ])
            } else {
                Value::list(direct_default_args)
            };
            let direct_body = rewrite_cl_next_method_p_forms(
                &executable_method_forms,
                &method_name,
                &direct_default_form,
                Value::Nil,
            )?;
            // GNU checks the specializers even when the generic has a
            // single method; a non-matching call goes through
            // `cl-no-applicable-method'.
            let direct_dispatch_specializers = cl_defmethod_dispatch_specializers(
                &method_specializers,
                &direct_params,
                &direct_params,
            );
            let direct_condition =
                ClDefmethodStoredMethod::from_specializers(&direct_dispatch_specializers)
                    .condition(&cl_defmethod_runtime_variables(
                        &direct_params,
                        &direct_params,
                        &method_specializers,
                    ));
            let mut guarded_body = Vec::with_capacity(direct_body.len() + 1);
            guarded_body.push(Value::Symbol("progn".into()));
            guarded_body.extend(direct_body);
            direct_lowered.push(Value::list([
                Value::Symbol("if".into()),
                direct_condition,
                Value::list(guarded_body),
                Value::list([
                    Value::Symbol("emaxx--cl-generic-apply-next".into()),
                    Value::Nil,
                    Value::list([
                        Value::Symbol("quote".into()),
                        Value::Symbol(method_name.clone()),
                    ]),
                    Value::list([
                        Value::Symbol("quote".into()),
                        Value::Symbol("no-applicable".into()),
                    ]),
                    direct_default_form.clone(),
                ]),
            ]));
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
        // `cl-defmethod' may replace the generic's callable dispatch wrapper
        // through `cl-defun', but it does not redefine the generic itself.
        // `sf_defun' correctly clears stale docs for ordinary redefinitions;
        // restore this separately owned generic metadata after the wrapper
        // transition.  Method docstrings remain on their method metadata.
        if result.is_ok()
            && let Some(generic_doc) = generic_doc
        {
            self.put_symbol_property(&method_name, "function-documentation", generic_doc);
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

    fn remove_native_cl_defmethod(
        &mut self,
        method_name: &str,
        qualifiers: &Value,
        specializers: &Value,
    ) {
        let Some(lambda_list) = self.cl_defgeneric_lambda_list(method_name) else {
            return;
        };
        let Ok(params) = self.parse_params(&lambda_list) else {
            return;
        };
        let qualifiers = qualifiers.to_vec().unwrap_or_default();
        let specializers = specializers.to_vec().unwrap_or_default();
        let method_specializers = params
            .iter()
            .filter(|param| !param.starts_with('&'))
            .zip(specializers.iter())
            .filter_map(|(variable, specializer)| {
                cl_defmethod_specializer_kind(Some(specializer)).map(|kind| {
                    ClDefmethodSpecializer {
                        variable: variable.clone(),
                        kind,
                        is_context: false,
                        context_expr: None,
                    }
                })
            })
            .collect::<Vec<_>>();
        let stored = ClDefmethodStoredMethod::from_specializers(&method_specializers);
        let qualifier_key = cl_defmethod_qualifier_key(&qualifiers);
        let method_key = stored.hidden_key();
        let qualifier = qualifiers
            .iter()
            .filter_map(|value| value.as_symbol().ok())
            .find(|name| matches!(*name, ":before" | ":after"));
        let previous_name = match qualifier {
            Some(qualifier) => format!(
                "__emaxx_{}_method_{}_{}",
                qualifier.trim_start_matches(':'),
                method_name.replace('-', "_"),
                method_key
            ),
            None => format!(
                "__emaxx_previous_method_{}_{}{}",
                method_name.replace('-', "_"),
                qualifier_key,
                method_key
            ),
        };
        let Ok(root) = self.lookup_function(method_name, &Env::new()) else {
            return;
        };
        let Some((target_env, _, previous)) = cl_defmethod_previous_binding(&root, &previous_name)
        else {
            return;
        };
        let previous_is_ignore = self.callable_is_ignore(&previous);
        let target_id = target_env.as_ptr() as usize;
        let mut replacement = match &root {
            Value::Lambda(_, _, root_env) if root_env.as_ptr() as usize == target_id => previous,
            _ => {
                cl_defmethod_replace_child_environment(&root, target_id, &previous);
                root
            }
        };

        let retain_metadata =
            cl_defmethod_contains_binding_fragment(&replacement, &format!("_{method_key}"));
        let metadata = self
            .get_symbol_property(method_name, "emaxx-cl-defmethod-specializers")
            .and_then(|value| value.to_vec().ok())
            .unwrap_or_default()
            .into_iter()
            .filter(|value| retain_metadata || value != &stored.metadata_value())
            .collect::<Vec<_>>();
        if metadata.is_empty() && previous_is_ignore {
            replacement = cl_generic_no_applicable_function(method_name, &params);
        }
        self.set_function_binding(method_name, Some(replacement));
        self.put_symbol_property(
            method_name,
            "emaxx-cl-defmethod-specializers",
            Value::list(metadata),
        );
    }

    pub(crate) fn remove_native_cl_defmethod_loadhist_entry(
        &mut self,
        entry: &Value,
    ) -> Result<Value, LispError> {
        let parts = entry.to_vec()?;
        if parts.len() < 3
            || !matches!(parts.first(), Some(Value::Symbol(kind)) if kind == "cl-defmethod")
        {
            return Ok(Value::Nil);
        }
        let method_name = parts[1].as_symbol()?;
        self.remove_native_cl_defmethod(method_name, &parts[2], &Value::list(parts[3..].to_vec()));
        Ok(Value::Nil)
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
        // GNU LOADHIST_ATTACH conses onto the front (the load-file name
        // stays LAST; `macroexp-file-name' depends on that).
        current_load_list = Value::cons(entry, current_load_list);
        self.set_global_binding("current-load-list", current_load_list);
    }

    // Oclosures are closures with named slots visible as lexical bindings
    // in the body (GNU oclosure.el).  emaxx represents one as a Lambda whose
    // captured env leads with a marker frame holding the type and slots.
    pub(super) fn sf_oclosure_define(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let Some(name_form) = items.get(1) else {
            return Err(LispError::Signal("oclosure-define needs a name".into()));
        };
        let (type_name, options) = match name_form {
            Value::Symbol(name) => (name.clone(), Vec::new()),
            other => {
                let parts = other.to_vec()?;
                let name = parts
                    .first()
                    .and_then(|value| value.as_symbol().ok())
                    .ok_or_else(|| LispError::Signal("oclosure-define needs a name".into()))?
                    .to_string();
                (name, parts[1..].to_vec())
            }
        };
        let mut parent: Option<String> = None;
        let mut predicate: Option<String> = None;
        let mut copiers: Vec<(String, Option<Vec<String>>)> = Vec::new();
        for option in &options {
            let Ok(parts) = option.to_vec() else { continue };
            match parts.first().and_then(|value| value.as_symbol().ok()) {
                Some(":predicate") => {
                    predicate = parts
                        .get(1)
                        .and_then(|v| v.as_symbol().ok())
                        .map(String::from);
                }
                Some(":parent") => {
                    parent = parts
                        .get(1)
                        .and_then(|v| v.as_symbol().ok())
                        .map(String::from);
                }
                Some(":copier") => {
                    let Some(copier_name) = parts
                        .get(1)
                        .and_then(|v| v.as_symbol().ok())
                        .map(String::from)
                    else {
                        continue;
                    };
                    // No arglist -> GNU generates a KEYWORD copier over all
                    // slots ((:copier NAME) -> (NAME obj &key fst snd ...)).
                    let arglist =
                        parts
                            .get(2)
                            .and_then(|value| value.to_vec().ok())
                            .map(|values| {
                                values
                                    .iter()
                                    .filter_map(|value| value.as_symbol().ok().map(String::from))
                                    .collect::<Vec<_>>()
                            });
                    copiers.push((copier_name, arglist));
                }
                _ => {}
            }
        }
        // Slot names follow (docstrings are skipped); inherit parent slots
        // and their mutability.
        let mut slots: Vec<String> = parent
            .as_ref()
            .and_then(|parent| self.get_symbol_property(parent, "emaxx-oclosure-slots"))
            .and_then(|value| value.to_vec().ok())
            .unwrap_or_default()
            .iter()
            .filter_map(|value| value.as_symbol().ok().map(String::from))
            .collect();
        let mut mutable_slots: Vec<String> = parent
            .as_ref()
            .and_then(|parent| self.get_symbol_property(parent, "emaxx-oclosure-mutable-slots"))
            .and_then(|value| value.to_vec().ok())
            .unwrap_or_default()
            .iter()
            .filter_map(|value| value.as_symbol().ok().map(String::from))
            .collect();
        for slot in &items[2..] {
            match slot {
                Value::Symbol(slot_name) => slots.push(slot_name.clone()),
                Value::Cons(_, _) => {
                    if let Ok(parts) = slot.to_vec()
                        && let Some(Value::Symbol(slot_name)) = parts.first()
                    {
                        slots.push(slot_name.clone());
                        // (SLOT :mutable t) marks the slot settable.
                        let mutable = parts.windows(2).any(|window| {
                            matches!(&window[0], Value::Symbol(key) if key == ":mutable")
                                && window[1].is_truthy()
                        });
                        if mutable {
                            mutable_slots.push(slot_name.clone());
                        }
                    }
                }
                _ => {}
            }
        }
        self.put_symbol_property(
            &type_name,
            "emaxx-oclosure-slots",
            Value::list(slots.iter().map(|slot| Value::Symbol(slot.clone()))),
        );
        self.put_symbol_property(
            &type_name,
            "emaxx-oclosure-mutable-slots",
            Value::list(mutable_slots.iter().map(|slot| Value::Symbol(slot.clone()))),
        );
        if let Some(parent) = &parent {
            self.put_symbol_property(
                &type_name,
                "emaxx-oclosure-parent",
                Value::Symbol(parent.clone()),
            );
        }
        // Generated helpers: predicate, per-slot accessors, copiers.
        let mut defs = String::new();
        if let Some(predicate) = &predicate {
            defs.push_str(&format!(
                "(defalias '{predicate} (lambda (obj) (emaxx--oclosure-type-p obj '{type_name})))\n"
            ));
        }
        for slot in &slots {
            defs.push_str(&format!(
                "(defalias '{type_name}--{slot} (lambda (obj) \"Access slot \\\"{slot}\\\" of OBJ of type `{type_name}'.\" (emaxx--oclosure-slot obj '{slot})))\n"
            ));
            // setf place for the accessor; emaxx--oclosure-set-slot enforces
            // :mutable (setting-constant otherwise), like GNU's oclosure--set.
            defs.push_str(&format!(
                "(defalias '{type_name}--{slot}--emaxx-set (lambda (obj value) (emaxx--oclosure-set-slot obj '{slot} value)))\n\
                 (put '{type_name}--{slot} 'emaxx-gv-setter '{type_name}--{slot}--emaxx-set)\n"
            ));
            // GNU's GV fallback calls the canonical `(setf ACCESSOR)'
            // function with VALUE before the accessor's ordinary arguments.
            self.set_function_binding(
                &format!("(setf {type_name}--{slot})"),
                Some(Value::Lambda(
                    vec!["value".into(), "obj".into()],
                    vec![Value::list([
                        Value::Symbol("emaxx--oclosure-set-slot".into()),
                        Value::Symbol("obj".into()),
                        quoted_literal(&Value::Symbol(slot.clone())),
                        Value::Symbol("value".into()),
                    ])]
                    .into(),
                    shared_env(Vec::new()),
                )),
            );
        }
        for (copier_name, copier_slots) in &copiers {
            match copier_slots {
                Some(copier_slots) => {
                    let params = copier_slots.join(" ");
                    let pairs = copier_slots
                        .iter()
                        .map(|slot| format!("(cons '{slot} {slot})"))
                        .collect::<Vec<_>>()
                        .join(" ");
                    defs.push_str(&format!(
                        "(defalias '{copier_name} (lambda (obj {params}) \"Copier for objects of type `{type_name}'.\" (emaxx--oclosure-copy obj (list {pairs}))))\n"
                    ));
                }
                None => {
                    // GNU's default copier takes keyword arguments naming
                    // the slots; only the PROVIDED keys are replaced.
                    defs.push_str(&format!(
                        "(defalias '{copier_name} \
                           (lambda (obj &rest emaxx--kv) \
                             \"Copier for objects of type `{type_name}'.\" \
                             (let (emaxx--repl) \
                               (while emaxx--kv \
                                 (let ((emaxx--key (pop emaxx--kv))) \
                                   (unless (keywordp emaxx--key) \
                                     (error \"Keyword argument %s not one of nil\" emaxx--key)) \
                                   (push (cons (intern (substring (symbol-name emaxx--key) 1)) \
                                               (pop emaxx--kv)) \
                                         emaxx--repl))) \
                               (emaxx--oclosure-copy obj emaxx--repl))))\n"
                    ));
                }
            }
        }
        for form in crate::lisp::reader::Reader::new(&defs).read_all()? {
            self.eval(&form, env)?;
        }
        Ok(Value::Symbol(type_name))
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
        let spec = items[1].to_vec()?;
        let type_name = spec
            .first()
            .and_then(|value| value.as_symbol().ok())
            .ok_or_else(|| LispError::Signal("oclosure-lambda needs a type".into()))?
            .to_string();
        let declared: Vec<String> = self
            .get_symbol_property(&type_name, "emaxx-oclosure-slots")
            .and_then(|value| value.to_vec().ok())
            .unwrap_or_default()
            .iter()
            .filter_map(|value| value.as_symbol().ok().map(String::from))
            .collect();
        let mut frame: Vec<(String, Value)> = vec![(
            crate::lisp::eval::OCLOSURE_TYPE_MARKER.to_string(),
            Value::Symbol(type_name),
        )];
        let mut initialized: Vec<(String, Value)> = Vec::new();
        for binding in &spec[1..] {
            let Ok(parts) = binding.to_vec() else {
                continue;
            };
            let Some(slot_name) = parts.first().and_then(|v| v.as_symbol().ok()) else {
                continue;
            };
            let value = match parts.get(1) {
                Some(expr) => self.eval(expr, env)?,
                None => Value::Nil,
            };
            initialized.push((slot_name.to_string(), value));
        }
        for slot in &declared {
            let value = initialized
                .iter()
                .find(|(name, _)| name == slot)
                .map(|(_, value)| value.clone())
                .unwrap_or(Value::Nil);
            frame.push((slot.clone(), value));
        }
        let mut lowered = Vec::with_capacity(items.len());
        lowered.push(Value::Symbol("lambda".into()));
        lowered.push(items[2].clone());
        // Inert identification marker: oclosure recognizers require it as
        // the first body form so closures that merely CAPTURED an
        // oclosure's frames are not mistaken for oclosures.
        lowered.push(Value::Symbol(":closure-oclosure".into()));
        lowered.extend(items[3..].iter().cloned());
        let lambda = self.sf_lambda(&lowered, env)?;
        let Value::Lambda(params, body, closure_env) = lambda else {
            return Err(LispError::Signal("oclosure-lambda lowering failed".into()));
        };
        let lexical_closure = self.closure_env_is_lexical(&closure_env);
        // Nested advice objects share slot names (car/cdr/...); the identity
        // stamp keeps the frame-merge machinery from unifying two DIFFERENT
        // objects' identically-shaped slot frames (which would self-recurse),
        // while callees invoked from the body still see the caller's lexical
        // frames (dynamic bindings and shared-cell mutation keep working).
        frame.push(Self::fresh_frame_identity());
        // Each oclosure owns its slot frame (copiers replace slot values
        // per object), so never share the captured env Rc.
        let mut contents = closure_env.borrow().clone();
        contents.push(frame);
        let closure_env = shared_env(contents);
        if lexical_closure {
            self.mark_lexical_closure_env(&closure_env);
        }
        Ok(Value::Lambda(params, body, closure_env))
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
        self.sf_lambda_with_source(items, None, env)
    }

    pub(super) fn sf_lambda_from_source(
        &mut self,
        source: &Value,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let source_anchor = source.cons_cells().map(|(car, _)| car);
        self.sf_lambda_with_source(items, source_anchor, env)
    }

    fn sf_lambda_with_source(
        &mut self,
        items: &[Value],
        source_anchor: Option<crate::lisp::types::ConsSlot>,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Err(LispError::Signal("lambda needs params".into()));
        }
        let params = self.parse_params(&items[1])?;
        let (_, body) = self.normalize_function_body_documentation(&items[2..], env)?;
        let keep_full_context = body_closure_dont_trim_context(&body);
        let capture_override = self.lambda_capture_override();
        let closure_env = if capture_override.unwrap_or(true) {
            let captured = if !keep_full_context && self.lambda_trim_override() {
                trim_lambda_closure_env(env, &body)
            } else {
                env.clone()
            };
            // A lexical lambda carries an explicit context marker even when
            // it has no free variables.  Besides forming the scope boundary,
            // invocation uses this marker to give delayed macro expansion
            // the lexical-binding context in which the lambda was created.
            let lexical_source = capture_override == Some(true)
                || self
                    .lookup_var("lexical-binding", env)
                    .is_some_and(|value| value.is_truthy());
            let closure_env = self.capture_closure_env(captured);
            if lexical_source {
                self.mark_lexical_closure_env(&closure_env);
            }
            closure_env
        } else {
            let closure_env = shared_env(Vec::new());
            self.mark_closure_eval_context(&closure_env, false);
            closure_env
        };
        let body = match source_anchor {
            Some(source_anchor) => {
                let source_id = Rc::as_ptr(&source_anchor) as usize;
                if let Some((cached_source, cached_body)) =
                    self.lambda_source_bodies.get(&source_id)
                    && cached_source
                        .upgrade()
                        .is_some_and(|cached| Rc::ptr_eq(&cached, &source_anchor))
                    && let Some(body) = cached_body.upgrade()
                {
                    body
                } else {
                    let body = Rc::new(body);
                    self.lambda_source_bodies.insert(
                        source_id,
                        (Rc::downgrade(&source_anchor), Rc::downgrade(&body)),
                    );
                    body
                }
            }
            None => Rc::new(body),
        };
        Ok(Value::Lambda(params, body, closure_env))
    }
}

fn trim_lambda_closure_env(env: &Env, body: &[Value]) -> Env {
    let mut referenced = HashSet::new();
    for form in body {
        collect_referenced_symbols(form, &mut referenced);
    }

    env.iter()
        .filter_map(|frame| {
            let last_used = frame.iter().rposition(|(name, _)| {
                referenced.contains(name.as_str())
                    || name
                        .strip_prefix("--emaxx-local-special--")
                        .is_some_and(|declared| referenced.contains(declared))
            })?;
            let mut trimmed = frame[..=last_used].to_vec();
            // The stamp is part of the lexical cell's identity, not a
            // user-visible binding.  Keep it even when all referenced values
            // precede it, otherwise two unrelated trimmed frames with the
            // same names can be merged when nested closures call each other.
            if let Some(identity) = frame
                .iter()
                .rev()
                .find(|(name, _)| name == super::loops::FRAME_IDENTITY_MARKER)
                .filter(|identity| !trimmed.contains(identity))
            {
                trimmed.push(identity.clone());
            }
            Some(trimmed)
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

/// Rename each `_' parameter of a `cl-defmethod' lambda list to a unique
/// positional name.  Returns None when nothing needed renaming.
fn uniquify_ignored_lambda_list_params(lambda_list: &Value) -> Option<Value> {
    let entries = lambda_list.to_vec().ok()?;
    let mut changed = false;
    let mut counter = 0usize;
    let mut fresh = || {
        let name = format!("_emaxx-ignored-{counter}");
        counter += 1;
        name
    };
    let rewritten = entries
        .iter()
        .map(|entry| match entry {
            Value::Symbol(name) if name == "_" => {
                changed = true;
                Value::Symbol(fresh())
            }
            Value::Cons(_, _) => match entry.cons_values() {
                Some((Value::Symbol(name), rest)) if name == "_" => {
                    changed = true;
                    Value::cons(Value::Symbol(fresh()), rest)
                }
                _ => entry.clone(),
            },
            _ => entry.clone(),
        })
        .collect::<Vec<_>>();
    changed.then(|| Value::list(rewritten))
}
