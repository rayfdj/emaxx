#![allow(dead_code)]

use std::fmt;

/// A Lisp value. This covers the subset we need for ERT tests.
#[derive(Clone, Debug)]
pub enum Value {
    Nil,
    T,
    Integer(i64),
    Float(f64),
    String(String),
    Symbol(String),
    Cons(Box<Value>, Box<Value>),
    /// Built-in function: name, arity (min, max), function pointer handled in eval
    BuiltinFunc(String),
    /// A lambda or closure: params, body, captured env
    Lambda(Vec<String>, Vec<Value>, Env),
    /// A buffer object: (id, name). The id is used for `eq` identity.
    Buffer(u64, String),
    /// An overlay object, identified by unique id.
    Overlay(u64),
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
        matches!(self, Value::Integer(_))
    }

    pub fn is_string(&self) -> bool {
        matches!(self, Value::String(_))
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
            _ => Err(LispError::TypeError("integer".into(), self.type_name())),
        }
    }

    pub fn as_float(&self) -> Result<f64, LispError> {
        match self {
            Value::Float(f) => Ok(*f),
            Value::Integer(n) => Ok(*n as f64),
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
            Value::Float(_) => "float".into(),
            Value::String(_) => "string".into(),
            Value::Symbol(_) => "symbol".into(),
            Value::Cons(_, _) => "cons".into(),
            Value::BuiltinFunc(name) => format!("builtin<{}>", name),
            Value::Lambda(_, _, _) => "lambda".into(),
            Value::Buffer(_, name) => format!("buffer<{}>", name),
            Value::Overlay(id) => format!("overlay<{}>", id),
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Nil, Value::Nil) => true,
            (Value::T, Value::T) => true,
            (Value::Integer(a), Value::Integer(b)) => a == b,
            (Value::Float(a), Value::Float(b)) => a == b,
            (Value::String(a), Value::String(b)) => a == b,
            (Value::Symbol(a), Value::Symbol(b)) => a == b,
            (Value::Cons(a1, a2), Value::Cons(b1, b2)) => a1 == b1 && a2 == b2,
            (Value::Buffer(id_a, _), Value::Buffer(id_b, _)) => id_a == id_b,
            (Value::Overlay(a), Value::Overlay(b)) => a == b,
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
            Value::Float(v) => write!(f, "{}", v),
            Value::String(s) => write!(f, "\"{}\"", s),
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
            Value::Overlay(id) => write!(f, "#<overlay id:{}>", id),
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
            LispError::Signal(_) => "error",
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
