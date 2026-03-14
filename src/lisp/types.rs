#![allow(dead_code)]

use num_bigint::BigInt;
use num_traits::ToPrimitive;
use std::fmt;
use std::{cell::RefCell, rc::Rc};

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
    Cons(Box<Value>, Box<Value>),
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
        Value::Cons(Box::new(car), Box::new(cdr))
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
            Value::Cons(car, _) => Ok(*car.clone()),
            Value::Nil => Ok(Value::Nil),
            _ => Err(LispError::TypeError("list".into(), self.type_name())),
        }
    }

    pub fn cdr(&self) -> Result<Value, LispError> {
        match self {
            Value::Cons(_, cdr) => Ok(*cdr.clone()),
            Value::Nil => Ok(Value::Nil),
            _ => Err(LispError::TypeError("list".into(), self.type_name())),
        }
    }

    /// Convert a proper list to a Vec.
    pub fn to_vec(&self) -> Result<Vec<Value>, LispError> {
        let mut result = Vec::new();
        let mut current = self.clone();
        loop {
            match current {
                Value::Nil => return Ok(result),
                Value::Cons(car, cdr) => {
                    result.push(*car);
                    current = *cdr;
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
        match (self, other) {
            (Value::Nil, Value::Nil) => true,
            (Value::T, Value::T) => true,
            (Value::Integer(a), Value::Integer(b)) => a == b,
            (Value::BigInteger(a), Value::BigInteger(b)) => a == b,
            (Value::Integer(a), Value::BigInteger(b))
            | (Value::BigInteger(b), Value::Integer(a)) => &BigInt::from(*a) == b,
            (Value::Float(a), Value::Float(b)) => a == b,
            (Value::String(a), Value::String(b)) => a == b,
            (Value::StringObject(a), Value::StringObject(b)) => Rc::ptr_eq(a, b),
            (Value::Symbol(a), Value::Symbol(b)) => a == b,
            (Value::Cons(a1, a2), Value::Cons(b1, b2)) => a1 == b1 && a2 == b2,
            (Value::Buffer(id_a, _), Value::Buffer(id_b, _)) => id_a == id_b,
            (Value::Marker(a), Value::Marker(b)) => a == b,
            (Value::Overlay(a), Value::Overlay(b)) => a == b,
            (Value::CharTable(a), Value::CharTable(b)) => a == b,
            (Value::Record(a), Value::Record(b)) => a == b,
            (Value::Finalizer(a), Value::Finalizer(b)) => a == b,
            _ => false,
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
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
                let mut current = self;
                let mut first = true;
                loop {
                    match current {
                        Value::Cons(car, cdr) => {
                            if !first {
                                write!(f, " ")?;
                            }
                            write!(f, "{}", car)?;
                            first = false;
                            current = cdr;
                        }
                        Value::Nil => break,
                        _ => {
                            write!(f, " . {}", current)?;
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
    pub fn condition_type(&self) -> &'static str {
        match self {
            LispError::TypeError(_, _) => "wrong-type-argument",
            LispError::Void(_) => "void-variable",
            LispError::WrongNumberOfArgs(_, _) => "wrong-number-of-arguments",
            LispError::Signal(_) | LispError::SignalValue(_) => "error",
            LispError::Throw(_, _) => "no-catch",
            LispError::TestSkipped(_) => "ert-test-skipped",
            LispError::EndOfInput => "end-of-file",
            LispError::ReadError(_) => "invalid-read-syntax",
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
                Ok(items) if items.len() >= 2 => write!(f, "{}", items[1]),
                _ => write!(f, "{}", value),
            },
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
