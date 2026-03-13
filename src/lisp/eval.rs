use super::primitives;
use super::types::{Env, LispError, Value};

/// The interpreter state: holds the global environment, the current buffer,
/// and ERT test results.
pub struct Interpreter {
    /// Global variable bindings (defvar, setq at top level).
    globals: Vec<(String, Value)>,
    /// The current buffer being operated on.
    pub buffer: crate::buffer::Buffer,
    /// Known buffer names (simple buffer list for tests).
    pub buffer_list: Vec<String>,
    /// Collected ERT test definitions: (name, body).
    pub ert_tests: Vec<(String, Value)>,
    /// Results from running tests: (name, passed, error_message).
    pub test_results: Vec<(String, bool, Option<String>)>,
}

impl Default for Interpreter {
    fn default() -> Self {
        Self::new()
    }
}

impl Interpreter {
    pub fn new() -> Self {
        Interpreter {
            globals: Vec::new(),
            buffer: crate::buffer::Buffer::new("*test*"),
            buffer_list: vec!["*test*".to_string()],
            ert_tests: Vec::new(),
            test_results: Vec::new(),
        }
    }

    /// Look up a variable, returning None if unbound (for use by primitives).
    pub fn lookup_var(&self, name: &str, env: &Env) -> Option<Value> {
        for frame in env.iter().rev() {
            for (k, v) in frame.iter().rev() {
                if k == name {
                    return Some(v.clone());
                }
            }
        }
        for (k, v) in self.globals.iter().rev() {
            if k == name {
                return Some(v.clone());
            }
        }
        None
    }

    /// Look up a variable in the given local env, then globals.
    fn lookup(&self, name: &str, env: &Env) -> Result<Value, LispError> {
        // Search local frames from innermost to outermost
        for frame in env.iter().rev() {
            for (k, v) in frame.iter().rev() {
                if k == name {
                    return Ok(v.clone());
                }
            }
        }
        // Search globals
        for (k, v) in self.globals.iter().rev() {
            if k == name {
                return Ok(v.clone());
            }
        }
        // Built-in constants and functions
        match name {
            "nil" => Ok(Value::Nil),
            "t" => Ok(Value::T),
            "most-positive-fixnum" => Ok(Value::Integer(i64::MAX)),
            "most-negative-fixnum" => Ok(Value::Integer(i64::MIN)),
            _ => {
                // Check if it's a known builtin function
                if primitives::is_builtin(name) {
                    Ok(Value::BuiltinFunc(name.to_string()))
                } else {
                    Err(LispError::Void(name.to_string()))
                }
            }
        }
    }

    /// Set a variable in the innermost local frame, or in globals.
    fn set_variable(&mut self, name: &str, value: Value, env: &mut Env) {
        // Try to find and update in local env
        for frame in env.iter_mut().rev() {
            for (k, v) in frame.iter_mut().rev() {
                if k == name {
                    *v = value;
                    return;
                }
            }
        }
        // Set in globals
        for (k, v) in self.globals.iter_mut().rev() {
            if k == name {
                *v = value;
                return;
            }
        }
        self.globals.push((name.to_string(), value));
    }

    /// Evaluate an expression.
    pub fn eval(&mut self, expr: &Value, env: &mut Env) -> Result<Value, LispError> {
        match expr {
            Value::Nil | Value::T | Value::Integer(_) | Value::Float(_) | Value::String(_) => {
                Ok(expr.clone())
            }

            Value::BuiltinFunc(_) | Value::Lambda(_, _, _) | Value::Buffer(_) => Ok(expr.clone()),

            Value::Symbol(name) => self.lookup(name, env),

            Value::Cons(_, _) => {
                let items = expr.to_vec()?;
                if items.is_empty() {
                    return Ok(Value::Nil);
                }

                // Check for special forms first
                if let Value::Symbol(ref name) = items[0] {
                    match name.as_str() {
                        "quote" => return self.sf_quote(&items),
                        "if" => return self.sf_if(&items, env),
                        "when" => return self.sf_when(&items, env),
                        "unless" => return self.sf_unless(&items, env),
                        "cond" => return self.sf_cond(&items, env),
                        "and" => return self.sf_and(&items, env),
                        "or" => return self.sf_or(&items, env),
                        "not" => return self.sf_not(&items, env),
                        "progn" => return self.sf_progn(&items[1..], env),
                        "prog1" => return self.sf_prog1(&items, env),
                        "let" => return self.sf_let(&items, env),
                        "let*" => return self.sf_letstar(&items, env),
                        "setq" => return self.sf_setq(&items, env),
                        "setq-local" => return self.sf_setq(&items, env), // same for now
                        "defvar" => return self.sf_defvar(&items, env),
                        "defun" => return self.sf_defun(&items, env),
                        "defmacro" => return Ok(Value::Nil), // stub
                        "lambda" => return self.sf_lambda(&items, env),
                        "function" | "function-quote" => {
                            // #'foo or (function foo)
                            if items.len() >= 2 {
                                return self.eval(&items[1], env);
                            }
                            return Ok(Value::Nil);
                        }
                        "while" => return self.sf_while(&items, env),
                        "dolist" => return self.sf_dolist(&items, env),
                        "dotimes" => return self.sf_dotimes(&items, env),
                        "unwind-protect" => return self.sf_unwind_protect(&items, env),
                        "condition-case" => return self.sf_condition_case(&items, env),
                        "with-temp-buffer" => return self.sf_with_temp_buffer(&items, env),
                        "with-current-buffer" => {
                            // For now, just evaluate the body
                            return self.sf_progn(&items[2..], env);
                        }
                        "save-excursion" => return self.sf_save_excursion(&items, env),
                        "save-restriction" => return self.sf_progn(&items[1..], env),
                        "ert-deftest" => return self.sf_ert_deftest(&items),
                        "should" => return self.sf_should(&items, env),
                        "should-not" => return self.sf_should_not(&items, env),
                        "should-error" => return self.sf_should_error(&items, env),
                        "require" | "provide" | "declare" | "eval-and-compile" => {
                            return Ok(Value::Nil);
                        }
                        "ert-info" => {
                            // (ert-info (msg) body...) — just run the body
                            return self.sf_progn(&items[2..], env);
                        }
                        _ => {}
                    }
                }

                // Regular function call
                self.eval_call(&items, env)
            }
        }
    }

    fn eval_call(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        let func = self.eval(&items[0], env)?;
        let mut args = Vec::new();
        for item in &items[1..] {
            args.push(self.eval(item, env)?);
        }

        match func {
            Value::BuiltinFunc(ref name) => primitives::call(self, name, &args, env),
            Value::Lambda(ref params, ref body, ref closure_env) => {
                if params.len() != args.len() {
                    // Handle &optional and &rest later; for now just check
                    let min_params = params
                        .iter()
                        .position(|p| p == "&optional" || p == "&rest")
                        .unwrap_or(params.len());
                    if args.len() < min_params {
                        return Err(LispError::WrongNumberOfArgs(
                            "lambda".to_string(),
                            args.len(),
                        ));
                    }
                }

                let mut new_env = closure_env.clone();
                let mut frame = Vec::new();
                let mut arg_idx = 0;
                let mut optional = false;
                let mut rest = false;

                for param in params {
                    if param == "&optional" {
                        optional = true;
                        continue;
                    }
                    if param == "&rest" {
                        rest = true;
                        continue;
                    }
                    if rest {
                        // Collect remaining args into a list
                        let rest_args: Vec<Value> = args[arg_idx..].to_vec();
                        frame.push((param.clone(), Value::list(rest_args)));
                        break;
                    }
                    let val = if arg_idx < args.len() {
                        args[arg_idx].clone()
                    } else if optional {
                        Value::Nil
                    } else {
                        return Err(LispError::WrongNumberOfArgs(
                            "lambda".to_string(),
                            args.len(),
                        ));
                    };
                    frame.push((param.clone(), val));
                    arg_idx += 1;
                }

                new_env.push(frame);
                self.sf_progn(body, &mut new_env)
            }
            _ => {
                // Maybe the head is a symbol naming a function we haven't resolved
                if let Value::Symbol(name) = &items[0] {
                    // Try as builtin one more time
                    if primitives::is_builtin(name) {
                        return primitives::call(self, name, &args, env);
                    }
                }
                Err(LispError::Signal(format!("Invalid function: {}", items[0])))
            }
        }
    }

    // ── Special forms ──

    fn sf_quote(&self, items: &[Value]) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Ok(Value::Nil);
        }
        Ok(items[1].clone())
    }

    fn sf_if(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        let cond = self.eval(&items[1], env)?;
        if cond.is_truthy() {
            self.eval(&items[2], env)
        } else {
            // else branches
            self.sf_progn(&items[3..], env)
        }
    }

    fn sf_when(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        let cond = self.eval(&items[1], env)?;
        if cond.is_truthy() {
            self.sf_progn(&items[2..], env)
        } else {
            Ok(Value::Nil)
        }
    }

    fn sf_unless(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        let cond = self.eval(&items[1], env)?;
        if cond.is_nil() {
            self.sf_progn(&items[2..], env)
        } else {
            Ok(Value::Nil)
        }
    }

    fn sf_cond(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        for clause in &items[1..] {
            let clause_items = clause.to_vec()?;
            if clause_items.is_empty() {
                continue;
            }
            let test = self.eval(&clause_items[0], env)?;
            if test.is_truthy() {
                if clause_items.len() == 1 {
                    return Ok(test);
                }
                return self.sf_progn(&clause_items[1..], env);
            }
        }
        Ok(Value::Nil)
    }

    fn sf_and(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        let mut result = Value::T;
        for item in &items[1..] {
            result = self.eval(item, env)?;
            if result.is_nil() {
                return Ok(Value::Nil);
            }
        }
        Ok(result)
    }

    fn sf_or(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        for item in &items[1..] {
            let val = self.eval(item, env)?;
            if val.is_truthy() {
                return Ok(val);
            }
        }
        Ok(Value::Nil)
    }

    fn sf_not(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Err(LispError::WrongNumberOfArgs("not".into(), 0));
        }
        let val = self.eval(&items[1], env)?;
        Ok(if val.is_nil() { Value::T } else { Value::Nil })
    }

    fn sf_progn(&mut self, body: &[Value], env: &mut Env) -> Result<Value, LispError> {
        let mut result = Value::Nil;
        for expr in body {
            result = self.eval(expr, env)?;
        }
        Ok(result)
    }

    fn sf_prog1(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Ok(Value::Nil);
        }
        let result = self.eval(&items[1], env)?;
        for expr in &items[2..] {
            self.eval(expr, env)?;
        }
        Ok(result)
    }

    fn sf_let(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        let bindings = items[1].to_vec()?;
        let mut frame = Vec::new();

        for binding in &bindings {
            match binding {
                Value::Symbol(name) => {
                    frame.push((name.clone(), Value::Nil));
                }
                Value::Cons(_, _) => {
                    let parts = binding.to_vec()?;
                    let name = parts[0].as_symbol()?.to_string();
                    let val = if parts.len() > 1 {
                        self.eval(&parts[1], env)?
                    } else {
                        Value::Nil
                    };
                    frame.push((name, val));
                }
                _ => return Err(LispError::ReadError("bad let binding".into())),
            }
        }

        env.push(frame);
        let result = self.sf_progn(&items[2..], env);
        env.pop();
        result
    }

    fn sf_letstar(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        let bindings = items[1].to_vec()?;
        env.push(Vec::new());

        for binding in &bindings {
            match binding {
                Value::Symbol(name) => {
                    let frame = env.last_mut().unwrap();
                    frame.push((name.clone(), Value::Nil));
                }
                Value::Cons(_, _) => {
                    let parts = binding.to_vec()?;
                    let name = parts[0].as_symbol()?.to_string();
                    let val = if parts.len() > 1 {
                        self.eval(&parts[1], env)?
                    } else {
                        Value::Nil
                    };
                    let frame = env.last_mut().unwrap();
                    frame.push((name, val));
                }
                _ => return Err(LispError::ReadError("bad let* binding".into())),
            }
        }

        let result = self.sf_progn(&items[2..], env);
        env.pop();
        result
    }

    fn sf_setq(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        let mut result = Value::Nil;
        let mut i = 1;
        while i + 1 < items.len() {
            let name = items[i].as_symbol()?.to_string();
            let val = self.eval(&items[i + 1], env)?;
            result = val.clone();
            self.set_variable(&name, val, env);
            i += 2;
        }
        Ok(result)
    }

    fn sf_defvar(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Ok(Value::Nil);
        }
        let name = items[1].as_symbol()?.to_string();
        // Only set if not already defined
        if self.lookup(&name, env).is_err() {
            let val = if items.len() > 2 {
                self.eval(&items[2], env)?
            } else {
                Value::Nil
            };
            self.globals.push((name, val));
        }
        Ok(Value::Nil)
    }

    fn sf_defun(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 4 {
            return Err(LispError::Signal("defun needs name, params, body".into()));
        }
        let name = items[1].as_symbol()?.to_string();
        let params = self.parse_params(&items[2])?;

        // Skip docstring if present
        let body_start = if items.len() > 4 {
            if let Value::String(_) = &items[3] {
                4
            } else {
                3
            }
        } else {
            3
        };

        let body: Vec<Value> = items[body_start..].to_vec();
        let lambda = Value::Lambda(params, body, env.clone());
        self.globals.push((name.clone(), lambda));
        Ok(Value::Symbol(name))
    }

    fn sf_lambda(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Err(LispError::Signal("lambda needs params and body".into()));
        }
        let params = self.parse_params(&items[1])?;
        let body: Vec<Value> = items[2..].to_vec();
        Ok(Value::Lambda(params, body, env.clone()))
    }

    fn parse_params(&self, spec: &Value) -> Result<Vec<String>, LispError> {
        match spec {
            Value::Nil => Ok(Vec::new()),
            Value::Cons(_, _) => {
                let items = spec.to_vec()?;
                items
                    .into_iter()
                    .map(|v| match v {
                        Value::Symbol(s) => Ok(s),
                        _ => Err(LispError::ReadError("expected symbol in param list".into())),
                    })
                    .collect()
            }
            _ => Err(LispError::ReadError("expected list for params".into())),
        }
    }

    fn sf_while(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        let limit = 100_000; // safety valve
        let mut iterations = 0;
        loop {
            let cond = self.eval(&items[1], env)?;
            if cond.is_nil() {
                break;
            }
            self.sf_progn(&items[2..], env)?;
            iterations += 1;
            if iterations > limit {
                return Err(LispError::Signal(
                    "while loop exceeded iteration limit".into(),
                ));
            }
        }
        Ok(Value::Nil)
    }

    fn sf_dolist(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        let spec = items[1].to_vec()?;
        let var_name = spec[0].as_symbol()?.to_string();
        let list_val = self.eval(&spec[1], env)?;
        let list_items = list_val.to_vec()?;

        env.push(vec![(var_name.clone(), Value::Nil)]);
        for item in list_items {
            let frame = env.last_mut().unwrap();
            frame[0] = (var_name.clone(), item);
            self.sf_progn(&items[2..], env)?;
        }
        let result = if spec.len() > 2 {
            self.eval(&spec[2], env)?
        } else {
            Value::Nil
        };
        env.pop();
        Ok(result)
    }

    fn sf_dotimes(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        let spec = items[1].to_vec()?;
        let var_name = spec[0].as_symbol()?.to_string();
        let count = self.eval(&spec[1], env)?.as_integer()?;

        env.push(vec![(var_name.clone(), Value::Integer(0))]);
        for i in 0..count {
            let frame = env.last_mut().unwrap();
            frame[0] = (var_name.clone(), Value::Integer(i));
            self.sf_progn(&items[2..], env)?;
        }
        let result = if spec.len() > 2 {
            self.eval(&spec[2], env)?
        } else {
            Value::Nil
        };
        env.pop();
        Ok(result)
    }

    fn sf_unwind_protect(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        let result = self.eval(&items[1], env);
        // Always run cleanup forms
        for form in &items[2..] {
            let _ = self.eval(form, env);
        }
        result
    }

    fn sf_condition_case(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        // (condition-case var bodyform handlers...)
        if items.len() < 3 {
            return Ok(Value::Nil);
        }
        let var = match &items[1] {
            Value::Symbol(s) => Some(s.clone()),
            Value::Nil => None,
            _ => None,
        };

        match self.eval(&items[2], env) {
            Ok(val) => Ok(val),
            Err(e) => {
                // Try to find a matching handler
                for handler in &items[3..] {
                    let parts = handler.to_vec()?;
                    if parts.is_empty() {
                        continue;
                    }
                    // For simplicity, match any handler
                    if let Some(ref var_name) = var {
                        env.push(vec![(var_name.clone(), Value::String(e.to_string()))]);
                    }
                    let result = self.sf_progn(&parts[1..], env);
                    if var.is_some() {
                        env.pop();
                    }
                    return result;
                }
                Err(e)
            }
        }
    }

    fn sf_with_temp_buffer(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        // Save current buffer, create a fresh one, run body, restore
        let saved_buffer =
            std::mem::replace(&mut self.buffer, crate::buffer::Buffer::new("*temp*"));
        let result = self.sf_progn(&items[1..], env);
        self.buffer = saved_buffer;
        result
    }

    fn sf_save_excursion(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        let saved_pt = self.buffer.point();
        let result = self.sf_progn(&items[1..], env);
        self.buffer.goto_char(saved_pt);
        result
    }

    // ── ERT support ──

    fn sf_ert_deftest(&mut self, items: &[Value]) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Ok(Value::Nil);
        }
        let name = match &items[1] {
            Value::Symbol(s) => s.clone(),
            _ => return Ok(Value::Nil),
        };

        // items[2] is the param list (always empty for ert-deftest)
        // items[3..] is the body, possibly starting with a docstring
        let body_start = if items.len() > 4 {
            if let Value::String(_) = &items[3] {
                4
            } else {
                3
            }
        } else {
            3
        };

        let body = Value::list(
            std::iter::once(Value::symbol("progn")).chain(items[body_start..].iter().cloned()),
        );
        self.ert_tests.push((name, body));
        Ok(Value::Nil)
    }

    fn sf_should(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Err(LispError::WrongNumberOfArgs("should".into(), 0));
        }
        let val = self.eval(&items[1], env)?;
        if val.is_truthy() {
            Ok(Value::T)
        } else {
            Err(LispError::Signal(format!(
                "Test failed: expected truthy value from {}",
                items[1]
            )))
        }
    }

    fn sf_should_not(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Err(LispError::WrongNumberOfArgs("should-not".into(), 0));
        }
        let val = self.eval(&items[1], env)?;
        if val.is_nil() {
            Ok(Value::Nil)
        } else {
            Err(LispError::Signal(format!(
                "Test failed: expected nil from {}",
                items[1]
            )))
        }
    }

    fn sf_should_error(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Err(LispError::WrongNumberOfArgs("should-error".into(), 0));
        }
        match self.eval(&items[1], env) {
            Err(_) => Ok(Value::T), // good, error was raised
            Ok(val) => Err(LispError::Signal(format!(
                "Test failed: expected error but got {}",
                val
            ))),
        }
    }

    /// Run all collected ERT tests. Returns (passed, failed, total).
    pub fn run_ert_tests(&mut self) -> (usize, usize, usize) {
        let tests: Vec<(String, Value)> = self.ert_tests.clone();
        let total = tests.len();
        let mut passed = 0;
        let mut failed = 0;

        for (name, body) in &tests {
            let mut env: Env = Vec::new();
            match self.eval(body, &mut env) {
                Ok(_) => {
                    passed += 1;
                    self.test_results.push((name.clone(), true, None));
                }
                Err(e) => {
                    failed += 1;
                    self.test_results
                        .push((name.clone(), false, Some(e.to_string())));
                }
            }
        }

        (passed, failed, total)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lisp::reader::Reader;

    fn eval_str(src: &str) -> Value {
        let mut interp = Interpreter::new();
        let mut env: Env = Vec::new();
        let forms = Reader::new(src).read_all().unwrap();
        let mut result = Value::Nil;
        for form in &forms {
            result = interp.eval(form, &mut env).unwrap();
        }
        result
    }

    fn eval_str_with(interp: &mut Interpreter, src: &str) -> Value {
        let mut env: Env = Vec::new();
        let forms = Reader::new(src).read_all().unwrap();
        let mut result = Value::Nil;
        for form in &forms {
            result = interp.eval(form, &mut env).unwrap();
        }
        result
    }

    #[test]
    fn eval_atoms() {
        assert_eq!(eval_str("42"), Value::Integer(42));
        assert_eq!(eval_str("\"hello\""), Value::String("hello".into()));
        assert_eq!(eval_str("nil"), Value::Nil);
        assert_eq!(eval_str("t"), Value::T);
    }

    #[test]
    fn eval_arithmetic() {
        assert_eq!(eval_str("(+ 1 2)"), Value::Integer(3));
        assert_eq!(eval_str("(- 10 3)"), Value::Integer(7));
        assert_eq!(eval_str("(* 4 5)"), Value::Integer(20));
        assert_eq!(eval_str("(+ 1 2 3 4)"), Value::Integer(10));
        assert_eq!(eval_str("(1+ 5)"), Value::Integer(6));
        assert_eq!(eval_str("(1- 5)"), Value::Integer(4));
    }

    #[test]
    fn eval_comparisons() {
        assert_eq!(eval_str("(= 3 3)"), Value::T);
        assert_eq!(eval_str("(= 3 4)"), Value::Nil);
        assert_eq!(eval_str("(< 1 2)"), Value::T);
        assert_eq!(eval_str("(> 1 2)"), Value::Nil);
        assert_eq!(eval_str("(<= 3 3)"), Value::T);
        assert_eq!(eval_str("(>= 4 3)"), Value::T);
    }

    #[test]
    fn eval_let() {
        assert_eq!(eval_str("(let ((x 10)) x)"), Value::Integer(10));
        assert_eq!(eval_str("(let ((x 2) (y 3)) (+ x y))"), Value::Integer(5));
    }

    #[test]
    fn eval_if() {
        assert_eq!(eval_str("(if t 1 2)"), Value::Integer(1));
        assert_eq!(eval_str("(if nil 1 2)"), Value::Integer(2));
        assert_eq!(eval_str("(if t 1)"), Value::Integer(1));
        assert_eq!(eval_str("(if nil 1)"), Value::Nil);
    }

    #[test]
    fn eval_progn() {
        assert_eq!(eval_str("(progn 1 2 3)"), Value::Integer(3));
    }

    #[test]
    fn eval_and_or() {
        assert_eq!(eval_str("(and 1 2 3)"), Value::Integer(3));
        assert_eq!(eval_str("(and 1 nil 3)"), Value::Nil);
        assert_eq!(eval_str("(or nil nil 3)"), Value::Integer(3));
        assert_eq!(eval_str("(or nil nil)"), Value::Nil);
    }

    #[test]
    fn eval_defun_and_call() {
        let mut interp = Interpreter::new();
        eval_str_with(&mut interp, "(defun double (x) (* x 2))");
        assert_eq!(
            eval_str_with(&mut interp, "(double 21)"),
            Value::Integer(42)
        );
    }

    #[test]
    fn eval_string_ops() {
        assert_eq!(
            eval_str(r#"(concat "hello" " " "world")"#),
            Value::String("hello world".into())
        );
        assert_eq!(eval_str(r#"(string= "abc" "abc")"#), Value::T);
        assert_eq!(eval_str(r#"(string= "abc" "def")"#), Value::Nil);
        assert_eq!(eval_str(r#"(length "hello")"#), Value::Integer(5));
    }

    #[test]
    fn eval_list_ops() {
        assert_eq!(eval_str("(car '(1 2 3))"), Value::Integer(1));
        assert_eq!(eval_str("(length '(1 2 3))"), Value::Integer(3));
    }

    #[test]
    fn eval_buffer_ops() {
        let mut interp = Interpreter::new();
        eval_str_with(
            &mut interp,
            r#"(with-temp-buffer
                 (insert "hello")
                 (should (= (point) 6))
                 (should (string= (buffer-string) "hello")))"#,
        );
    }

    #[test]
    fn eval_ert_deftest_and_run() {
        let mut interp = Interpreter::new();
        eval_str_with(
            &mut interp,
            r#"
            (ert-deftest basic-insert ()
              (with-temp-buffer
                (insert "hello")
                (should (= (point) 6))
                (should (string= (buffer-string) "hello"))))
            "#,
        );
        let (passed, failed, total) = interp.run_ert_tests();
        assert_eq!(total, 1);
        assert_eq!(passed, 1);
        assert_eq!(failed, 0);
    }

    #[test]
    fn eval_while_loop() {
        assert_eq!(
            eval_str("(let ((x 0)) (while (< x 5) (setq x (1+ x))) x)"),
            Value::Integer(5)
        );
    }
}
