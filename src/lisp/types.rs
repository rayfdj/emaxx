#![allow(dead_code)]

use num_bigint::BigInt;
use num_traits::ToPrimitive;
use std::collections::HashSet;
use std::fmt;
use std::{cell::RefCell, rc::Rc};

pub type ConsSlot = Rc<RefCell<Value>>;
pub type ConsCells = (ConsSlot, ConsSlot);

#[derive(Clone, Debug, PartialEq)]
pub struct StringPropertySpan {
    pub start: usize,
    pub end: usize,
    pub props: Vec<(String, Value)>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SharedStringState {
    pub text: String,
    pub props: Vec<StringPropertySpan>,
    pub multibyte: bool,
}

/// A Lisp value. This covers the subset we need for ERT tests.
#[derive(Clone, Debug)]
pub enum Value {
    Nil,
    T,
    Integer(i64),
    BigInteger(BigInt),
    Float(f64),
    String(String),
    StringObject(Rc<RefCell<SharedStringState>>),
    Symbol(String),
    Cons(ConsSlot, ConsSlot),
    /// Built-in function: name, arity (min, max), function pointer handled in eval
    BuiltinFunc(String),
    /// A lambda or closure: params, body, captured env
    Lambda(Vec<String>, Vec<Value>, Env),
    /// A buffer object: (id, name). The id is used for `eq` identity.
    Buffer(u64, String),
    /// A marker object, identified by unique id.
    Marker(u64),
    /// An overlay object, identified by unique id.
    Overlay(u64),
    /// A char-table object, identified by unique id.
    CharTable(u64),
    /// A record object, identified by unique id.
    Record(u64),
    /// A finalizer object, identified by unique id.
    Finalizer(u64),
}

/// An environment frame: a list of (name, value) bindings.
/// We use a simple vector of frames for lexical scoping.
pub type Env = Vec<Vec<(String, Value)>>;

impl Value {
    // Constructors

    pub fn int(n: i64) -> Self {
        Value::Integer(n)
    }

    pub fn string(s: &str) -> Self {
        Value::String(s.to_string())
    }

    pub fn symbol(s: &str) -> Self {
        Value::Symbol(s.to_string())
    }

    pub fn cons(car: Value, cdr: Value) -> Self {
        Value::Cons(Rc::new(RefCell::new(car)), Rc::new(RefCell::new(cdr)))
    }

    /// Build a proper list from an iterator of values.
    pub fn list(items: impl IntoIterator<Item = Value>) -> Self {
        let items: Vec<Value> = items.into_iter().collect();
        let mut result = Value::Nil;
        for item in items.into_iter().rev() {
            result = Value::cons(item, result);
        }
        result
    }

    // Predicates

    pub fn is_nil(&self) -> bool {
        matches!(self, Value::Nil)
    }

    pub fn is_truthy(&self) -> bool {
        !self.is_nil()
    }

    pub fn is_integer(&self) -> bool {
        matches!(self, Value::Integer(_) | Value::BigInteger(_))
    }

    pub fn is_string(&self) -> bool {
        matches!(self, Value::String(_) | Value::StringObject(_))
    }

    pub fn is_symbol(&self) -> bool {
        matches!(self, Value::Symbol(_))
    }

    pub fn is_cons(&self) -> bool {
        matches!(self, Value::Cons(_, _))
    }

    pub fn is_list(&self) -> bool {
        matches!(self, Value::Nil | Value::Cons(_, _))
    }

    // Accessors

    pub fn as_integer(&self) -> Result<i64, LispError> {
        match self {
            Value::Integer(n) => Ok(*n),
            Value::BigInteger(n) => n
                .to_i64()
                .ok_or_else(|| LispError::TypeError("fixnum".into(), self.type_name())),
            _ => Err(LispError::TypeError("integer".into(), self.type_name())),
        }
    }

    pub fn as_float(&self) -> Result<f64, LispError> {
        match self {
            Value::Float(f) => Ok(*f),
            Value::Integer(n) => Ok(*n as f64),
            Value::BigInteger(n) => n
                .to_f64()
                .ok_or_else(|| LispError::TypeError("number".into(), self.type_name())),
            _ => Err(LispError::TypeError("number".into(), self.type_name())),
        }
    }

    pub fn as_string(&self) -> Result<&str, LispError> {
        match self {
            Value::String(s) => Ok(s),
            _ => Err(LispError::TypeError("string".into(), self.type_name())),
        }
    }

    pub fn as_symbol(&self) -> Result<&str, LispError> {
        match self {
            Value::Symbol(s) => Ok(s),
            _ => Err(LispError::TypeError("symbol".into(), self.type_name())),
        }
    }

    pub fn car(&self) -> Result<Value, LispError> {
        match self {
            Value::Cons(car, _) => Ok(car.borrow().clone()),
            Value::Nil => Ok(Value::Nil),
            _ => Err(LispError::TypeError("list".into(), self.type_name())),
        }
    }

    pub fn cdr(&self) -> Result<Value, LispError> {
        match self {
            Value::Cons(_, cdr) => Ok(cdr.borrow().clone()),
            Value::Nil => Ok(Value::Nil),
            _ => Err(LispError::TypeError("list".into(), self.type_name())),
        }
    }

    pub fn set_car(&self, new_car: Value) -> Result<(), LispError> {
        match self {
            Value::Cons(car, _) => {
                *car.borrow_mut() = new_car;
                Ok(())
            }
            _ => Err(LispError::TypeError("cons".into(), self.type_name())),
        }
    }

    pub fn set_cdr(&self, new_cdr: Value) -> Result<(), LispError> {
        match self {
            Value::Cons(_, cdr) => {
                *cdr.borrow_mut() = new_cdr;
                Ok(())
            }
            _ => Err(LispError::TypeError("cons".into(), self.type_name())),
        }
    }

    pub fn cons_cells(&self) -> Option<ConsCells> {
        match self {
            Value::Cons(car, cdr) => Some((car.clone(), cdr.clone())),
            _ => None,
        }
    }

    pub fn cons_values(&self) -> Option<(Value, Value)> {
        self.cons_cells()
            .map(|(car, cdr)| (car.borrow().clone(), cdr.borrow().clone()))
    }

    /// Convert a proper list to a Vec.
    pub fn to_vec(&self) -> Result<Vec<Value>, LispError> {
        let mut result = Vec::new();
        let mut current = self.clone();
        let mut seen = HashSet::new();
        loop {
            match current {
                Value::Nil => return Ok(result),
                Value::Cons(car, cdr) => {
                    let id = Rc::as_ptr(&car) as usize;
                    if !seen.insert(id) {
                        return Err(circular_list_error());
                    }
                    result.push(car.borrow().clone());
                    current = cdr.borrow().clone();
                }
                _ => return Err(LispError::TypeError("list".into(), current.type_name())),
            }
        }
    }

    pub fn type_name(&self) -> String {
        match self {
            Value::Nil => "nil".into(),
            Value::T => "t".into(),
            Value::Integer(_) => "integer".into(),
            Value::BigInteger(_) => "integer".into(),
            Value::Float(_) => "float".into(),
            Value::String(_) => "string".into(),
            Value::StringObject(_) => "string".into(),
            Value::Symbol(_) => "symbol".into(),
            Value::Cons(_, _) => "cons".into(),
            Value::BuiltinFunc(name) => format!("builtin<{}>", name),
            Value::Lambda(_, _, _) => "lambda".into(),
            Value::Buffer(_, name) => format!("buffer<{}>", name),
            Value::Marker(id) => format!("marker<{}>", id),
            Value::Overlay(id) => format!("overlay<{}>", id),
            Value::CharTable(id) => format!("char-table<{}>", id),
            Value::Record(id) => format!("record<{}>", id),
            Value::Finalizer(id) => format!("finalizer<{}>", id),
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        values_equal_recursive(self, other, &mut HashSet::new())
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        format_value(self, f, &mut HashSet::new())
    }
}

fn circular_list_error() -> LispError {
    LispError::SignalValue(Value::list([
        Value::Symbol("circular-list".into()),
        Value::String("Circular list".into()),
    ]))
}

fn cons_identity(car: &Rc<RefCell<Value>>) -> usize {
    Rc::as_ptr(car) as usize
}

fn values_equal_recursive(left: &Value, right: &Value, seen: &mut HashSet<(usize, usize)>) -> bool {
    match (left, right) {
        (Value::Nil, Value::Nil) => true,
        (Value::T, Value::T) => true,
        (Value::Integer(a), Value::Integer(b)) => a == b,
        (Value::BigInteger(a), Value::BigInteger(b)) => a == b,
        (Value::Integer(a), Value::BigInteger(b)) | (Value::BigInteger(b), Value::Integer(a)) => {
            &BigInt::from(*a) == b
        }
        (Value::Float(a), Value::Float(b)) => a == b,
        (Value::String(a), Value::String(b)) => a == b,
        (Value::StringObject(a), Value::StringObject(b)) => Rc::ptr_eq(a, b),
        (Value::Symbol(a), Value::Symbol(b)) => a == b,
        (Value::Cons(a_car, a_cdr), Value::Cons(b_car, b_cdr)) => {
            let ids = (cons_identity(a_car), cons_identity(b_car));
            if !seen.insert(ids) {
                return true;
            }
            values_equal_recursive(&a_car.borrow(), &b_car.borrow(), seen)
                && values_equal_recursive(&a_cdr.borrow(), &b_cdr.borrow(), seen)
        }
        (Value::BuiltinFunc(a), Value::BuiltinFunc(b)) => a == b,
        (Value::Lambda(a_params, a_body, a_env), Value::Lambda(b_params, b_body, b_env)) => {
            a_params == b_params && a_body == b_body && a_env == b_env
        }
        (Value::Buffer(id_a, _), Value::Buffer(id_b, _)) => id_a == id_b,
        (Value::Marker(a), Value::Marker(b)) => a == b,
        (Value::Overlay(a), Value::Overlay(b)) => a == b,
        (Value::CharTable(a), Value::CharTable(b)) => a == b,
        (Value::Record(a), Value::Record(b)) => a == b,
        (Value::Finalizer(a), Value::Finalizer(b)) => a == b,
        _ => false,
    }
}

fn format_value(
    value: &Value,
    f: &mut fmt::Formatter<'_>,
    seen: &mut HashSet<usize>,
) -> fmt::Result {
    match value {
        Value::Nil => write!(f, "nil"),
        Value::T => write!(f, "t"),
        Value::Integer(n) => write!(f, "{}", n),
        Value::BigInteger(n) => write!(f, "{}", n),
        Value::Float(v) => write!(f, "{}", v),
        Value::String(s) => write!(f, "\"{}\"", s),
        Value::StringObject(state) => write!(f, "\"{}\"", state.borrow().text),
        Value::Symbol(s) => write!(f, "{}", s),
        Value::Cons(_, _) => {
            write!(f, "(")?;
            let mut current = value.clone();
            let mut first = true;
            loop {
                match current {
                    Value::Cons(car, cdr) => {
                        let id = cons_identity(&car);
                        if !seen.insert(id) {
                            if !first {
                                write!(f, " ")?;
                            }
                            write!(f, "#<circular-list>")?;
                            break;
                        }
                        if !first {
                            write!(f, " ")?;
                        }
                        format_value(&car.borrow(), f, seen)?;
                        first = false;
                        current = cdr.borrow().clone();
                    }
                    Value::Nil => break,
                    other => {
                        write!(f, " . ")?;
                        format_value(&other, f, seen)?;
                        break;
                    }
                }
            }
            write!(f, ")")
        }
        Value::BuiltinFunc(name) => write!(f, "#<builtin {}>", name),
        Value::Lambda(params, _, _) => write!(f, "#<lambda ({})>", params.join(" ")),
        Value::Buffer(_, name) => write!(f, "#<buffer {}>", name),
        Value::Marker(id) => write!(f, "#<marker id:{}>", id),
        Value::Overlay(id) => write!(f, "#<overlay id:{}>", id),
        Value::CharTable(id) => write!(f, "#<char-table id:{}>", id),
        Value::Record(id) => write!(f, "#<record id:{}>", id),
        Value::Finalizer(id) => write!(f, "#<finalizer id:{}>", id),
    }
}

/// Lisp errors.
#[derive(Clone, Debug)]
pub enum LispError {
    /// Type mismatch: expected, got
    TypeError(String, String),
    /// Unbound variable
    Void(String),
    /// Wrong number of arguments
    WrongNumberOfArgs(String, usize),
    /// Generic error with a message (like Emacs's `error` function)
    Signal(String),
    /// Generic error with explicit condition payload.
    SignalValue(Value),
    /// An ERT assertion failure.
    ErtTestFailed(String),
    /// Non-local exit via `throw`.
    Throw(Value, Value),
    /// An ERT skip condition.
    TestSkipped(String),
    /// End of input during read
    EndOfInput,
    /// Reader syntax error
    ReadError(String),
}

impl LispError {
    pub fn condition_type(&self) -> String {
        match self {
            LispError::TypeError(_, _) => "wrong-type-argument".into(),
            LispError::Void(_) => "void-variable".into(),
            LispError::WrongNumberOfArgs(_, _) => "wrong-number-of-arguments".into(),
            LispError::Signal(_) => "error".into(),
            LispError::SignalValue(value) => value
                .to_vec()
                .ok()
                .and_then(|items| match items.first() {
                    Some(Value::Symbol(symbol)) => Some(symbol.clone()),
                    _ => None,
                })
                .unwrap_or_else(|| "error".into()),
            LispError::ErtTestFailed(_) => "ert-test-failed".into(),
            LispError::Throw(_, _) => "no-catch".into(),
            LispError::TestSkipped(_) => "ert-test-skipped".into(),
            LispError::EndOfInput => "end-of-file".into(),
            LispError::ReadError(_) => "invalid-read-syntax".into(),
        }
    }
}

impl fmt::Display for LispError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LispError::TypeError(expected, got) => {
                write!(f, "Wrong type argument: {}, {}", expected, got)
            }
            LispError::Void(name) => write!(f, "Symbol's value as variable is void: {}", name),
            LispError::WrongNumberOfArgs(name, n) => {
                write!(f, "Wrong number of arguments: {}, {}", name, n)
            }
            LispError::Signal(msg) => write!(f, "{}", msg),
            LispError::SignalValue(value) => match value.to_vec() {
                Ok(items)
                    if items.len() >= 4
                        && matches!(items.first(), Some(Value::Symbol(kind)) if kind == "file-error" || kind == "file-missing") =>
                {
                    let message = match &items[1] {
                        Value::String(text) => text.as_str(),
                        _ => return write!(f, "{}", value),
                    };
                    let detail = match &items[2] {
                        Value::String(text) => text.as_str(),
                        _ => return write!(f, "{}", value),
                    };
                    let path = match &items[3] {
                        Value::String(text) => text.as_str(),
                        _ => return write!(f, "{}", value),
                    };
                    write!(f, "{}: {}, {}", message, detail, path)
                }
                Ok(items) if items.len() >= 2 => write!(f, "{}", items[1]),
                _ => write!(f, "{}", value),
            },
            LispError::ErtTestFailed(msg) => write!(f, "{}", msg),
            LispError::Throw(tag, value) => write!(f, "No catch for {}: {}", tag, value),
            LispError::TestSkipped(msg) => write!(f, "{}", msg),
            LispError::EndOfInput => write!(f, "End of file during parsing"),
            LispError::ReadError(msg) => write!(f, "Invalid read syntax: {}", msg),
        }
    }
}

impl std::error::Error for LispError {}

impl From<crate::buffer::BufferError> for LispError {
    fn from(e: crate::buffer::BufferError) -> Self {
        LispError::Signal(e.to_string())
    }
}
