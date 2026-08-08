#![allow(dead_code)]

use num_bigint::BigInt;
use num_traits::ToPrimitive;
use std::fmt;
use std::{
    borrow::Borrow,
    cell::{Ref, RefCell, RefMut},
    collections::HashSet,
    iter::FromIterator,
    ops::Deref,
    path::Path,
    rc::{Rc, Weak},
};

const UNINTERNED_SYMBOL_MARKER: &str = "\u{1F}";
const OBARRAY_SYMBOL_MARKER: &str = "\u{1E}";

/// Immutable shared text stored inside compact Lisp values.
#[repr(transparent)]
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SharedText(Rc<String>);

impl SharedText {
    pub fn new(text: String) -> Self {
        Self(Rc::new(text))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub fn into_string(self) -> String {
        Rc::try_unwrap(self.0).unwrap_or_else(|text| text.as_ref().clone())
    }
}

impl Deref for SharedText {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<str> for SharedText {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl AsRef<Path> for SharedText {
    fn as_ref(&self) -> &Path {
        Path::new(self.as_str())
    }
}

impl Borrow<str> for SharedText {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Debug for SharedText {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl fmt::Display for SharedText {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<String> for SharedText {
    fn from(text: String) -> Self {
        Self::new(text)
    }
}

impl From<&str> for SharedText {
    fn from(text: &str) -> Self {
        Self::new(text.to_owned())
    }
}

impl From<&String> for SharedText {
    fn from(text: &String) -> Self {
        Self::from(text.as_str())
    }
}

impl FromIterator<char> for SharedText {
    fn from_iter<T: IntoIterator<Item = char>>(iter: T) -> Self {
        String::from_iter(iter).into()
    }
}

impl<'a> FromIterator<&'a char> for SharedText {
    fn from_iter<T: IntoIterator<Item = &'a char>>(iter: T) -> Self {
        iter.into_iter().copied().collect::<String>().into()
    }
}

impl From<SharedText> for String {
    fn from(text: SharedText) -> Self {
        text.into_string()
    }
}

impl From<&SharedText> for String {
    fn from(text: &SharedText) -> Self {
        text.as_str().to_owned()
    }
}

impl PartialEq<str> for SharedText {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl PartialEq<&str> for SharedText {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl PartialEq<String> for SharedText {
    fn eq(&self, other: &String) -> bool {
        self.as_str() == other
    }
}

impl PartialEq<SharedText> for String {
    fn eq(&self, other: &SharedText) -> bool {
        self == other.as_str()
    }
}

impl PartialEq<SharedText> for str {
    fn eq(&self, other: &SharedText) -> bool {
        self == other.as_str()
    }
}

impl PartialEq<SymbolName> for SharedText {
    fn eq(&self, other: &SymbolName) -> bool {
        self.as_str() == other.as_str()
    }
}

impl PartialEq<SharedText> for SymbolName {
    fn eq(&self, other: &SharedText) -> bool {
        self.as_str() == other.as_str()
    }
}

/// A symbol name shared by every live occurrence of the same interned name.
///
/// The ordinary-symbol table deliberately owns its entries for the owning
/// runtime thread's lifetime, matching the standard obarray's ownership in
/// Emaxx's single-threaded Lisp runtime.  Encoded
/// `make-symbol` names bypass the table so transient uninterned symbols are
/// still released when their last Lisp value dies.
#[repr(transparent)]
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SymbolName(SharedText);

thread_local! {
    static INTERNED_SYMBOL_NAMES: RefCell<HashSet<SymbolName>> = RefCell::new(HashSet::new());
}

impl SymbolName {
    pub fn intern(text: String) -> Self {
        if text.contains(UNINTERNED_SYMBOL_MARKER) {
            return Self(SharedText::from(text));
        }
        INTERNED_SYMBOL_NAMES.with_borrow_mut(|names| {
            if let Some(name) = names.get(text.as_str()) {
                return name.clone();
            }
            let name = Self(SharedText::from(text));
            names.insert(name.clone());
            name
        })
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub fn into_string(self) -> String {
        self.0.as_str().to_owned()
    }
}

impl Deref for SymbolName {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<str> for SymbolName {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Borrow<str> for SymbolName {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Debug for SymbolName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl fmt::Display for SymbolName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<String> for SymbolName {
    fn from(text: String) -> Self {
        Self::intern(text)
    }
}

impl From<&str> for SymbolName {
    fn from(text: &str) -> Self {
        Self::intern(text.to_owned())
    }
}

impl From<&String> for SymbolName {
    fn from(text: &String) -> Self {
        Self::from(text.as_str())
    }
}

impl From<SymbolName> for String {
    fn from(name: SymbolName) -> Self {
        name.into_string()
    }
}

impl From<&SymbolName> for String {
    fn from(name: &SymbolName) -> Self {
        name.as_str().to_owned()
    }
}

impl From<SharedText> for SymbolName {
    fn from(text: SharedText) -> Self {
        Self::intern(text.into_string())
    }
}

impl From<SymbolName> for SharedText {
    fn from(name: SymbolName) -> Self {
        name.0
    }
}

impl PartialEq<str> for SymbolName {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl PartialEq<&str> for SymbolName {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl PartialEq<String> for SymbolName {
    fn eq(&self, other: &String) -> bool {
        self.as_str() == other
    }
}

impl PartialEq<SymbolName> for String {
    fn eq(&self, other: &SymbolName) -> bool {
        self == other.as_str()
    }
}

impl PartialEq<SymbolName> for str {
    fn eq(&self, other: &SymbolName) -> bool {
        self == other.as_str()
    }
}

impl PartialEq<SymbolName> for &str {
    fn eq(&self, other: &SymbolName) -> bool {
        *self == other.as_str()
    }
}

#[repr(transparent)]
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SharedBigInt(Rc<BigInt>);

impl Deref for SharedBigInt {
    type Target = BigInt;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<BigInt> for SharedBigInt {
    fn from(value: BigInt) -> Self {
        Self(Rc::new(value))
    }
}

impl From<SharedBigInt> for BigInt {
    fn from(value: SharedBigInt) -> Self {
        Rc::try_unwrap(value.0).unwrap_or_else(|value| value.as_ref().clone())
    }
}

impl PartialEq<BigInt> for SharedBigInt {
    fn eq(&self, other: &BigInt) -> bool {
        self.0.as_ref() == other
    }
}

impl PartialEq<SharedBigInt> for BigInt {
    fn eq(&self, other: &SharedBigInt) -> bool {
        self == other.0.as_ref()
    }
}

impl fmt::Display for SharedBigInt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

pub type SharedCons = Rc<ConsCell>;
pub type ConsCells = (ConsSlot, ConsSlot);
pub type SharedEnv = Rc<RefCell<Env>>;
pub type SharedLambdaParams = Rc<Vec<String>>;
pub type SharedLambdaBody = Rc<Vec<Value>>;

#[derive(Debug)]
pub struct LambdaValue {
    pub params: SharedLambdaParams,
    pub body: SharedLambdaBody,
    pub env: SharedEnv,
}

#[derive(Debug)]
pub struct BufferValue {
    pub id: u64,
    pub name: SharedText,
}

/// The mutable payload of one Lisp cons.
///
/// GNU allocates the car and cdr together as one `Lisp_Cons`.  Keeping the
/// same ownership shape halves the allocation and reference-count traffic of
/// Emaxx's former two-`Rc` representation while retaining independent field
/// borrows for `setcar`, `setcdr`, reader fixups, and vector element slots.
#[derive(Debug)]
pub struct ConsCell {
    pub(crate) car: RefCell<Value>,
    pub(crate) cdr: RefCell<Value>,
}

impl ConsCell {
    fn new(car: Value, cdr: Value) -> Self {
        Self {
            car: RefCell::new(car),
            cdr: RefCell::new(cdr),
        }
    }

    pub(crate) fn identity(cell: &SharedCons) -> usize {
        Rc::as_ptr(cell) as usize
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ConsField {
    Car,
    Cdr,
}

/// A retained reference to one mutable field of a cons.
///
/// This is deliberately the only field-address abstraction exported by the
/// Lisp value layer.  Callers cannot depend on the physical layout of
/// `ConsCell`, so future value-representation work stays localized here.
#[derive(Clone, Debug)]
pub struct ConsSlot {
    cell: SharedCons,
    field: ConsField,
}

impl ConsSlot {
    pub(crate) fn car(cell: &SharedCons) -> Self {
        Self {
            cell: cell.clone(),
            field: ConsField::Car,
        }
    }

    pub(crate) fn cdr(cell: &SharedCons) -> Self {
        Self {
            cell: cell.clone(),
            field: ConsField::Cdr,
        }
    }

    pub fn borrow(&self) -> Ref<'_, Value> {
        match self.field {
            ConsField::Car => self.cell.car.borrow(),
            ConsField::Cdr => self.cell.cdr.borrow(),
        }
    }

    pub fn borrow_mut(&self) -> RefMut<'_, Value> {
        match self.field {
            ConsField::Car => self.cell.car.borrow_mut(),
            ConsField::Cdr => self.cell.cdr.borrow_mut(),
        }
    }

    pub fn cell_id(&self) -> usize {
        ConsCell::identity(&self.cell)
    }

    pub fn ptr_eq(&self, other: &Self) -> bool {
        self.field == other.field && Rc::ptr_eq(&self.cell, &other.cell)
    }

    pub fn downgrade(&self) -> WeakConsSlot {
        WeakConsSlot {
            cell: Rc::downgrade(&self.cell),
            field: self.field,
        }
    }
}

#[derive(Clone, Debug)]
pub struct WeakConsSlot {
    cell: Weak<ConsCell>,
    field: ConsField,
}

impl WeakConsSlot {
    pub fn upgrade(&self) -> Option<ConsSlot> {
        Some(ConsSlot {
            cell: self.cell.upgrade()?,
            field: self.field,
        })
    }
}

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

/// Detects circular lists during traversal with Brent's algorithm, the same
/// scheme GNU's FOR_EACH_TAIL uses: constant memory and no hashing.
pub struct CycleGuard {
    tortoise: usize,
    power: usize,
    lam: usize,
}

impl CycleGuard {
    pub fn new() -> Self {
        CycleGuard {
            tortoise: 0,
            power: 1,
            lam: 0,
        }
    }

    /// Advance past a cons cell; returns true when the cell closes a cycle.
    pub fn step(&mut self, cell_id: usize) -> bool {
        if cell_id == self.tortoise {
            return true;
        }
        if self.lam == self.power {
            self.tortoise = cell_id;
            self.power <<= 1;
            self.lam = 0;
        }
        self.lam += 1;
        false
    }
}

impl Default for CycleGuard {
    fn default() -> Self {
        CycleGuard::new()
    }
}

/// A Lisp value. This covers the subset we need for ERT tests.
#[derive(Clone, Debug)]
pub enum Value {
    Nil,
    T,
    Integer(i64),
    BigInteger(SharedBigInt),
    Float(f64),
    String(SharedText),
    StringObject(Rc<RefCell<SharedStringState>>),
    Symbol(SymbolName),
    Cons(SharedCons),
    /// Built-in function: name, arity (min, max), function pointer handled in eval
    BuiltinFunc(SymbolName),
    /// A lambda or closure: params, immutable shared body, captured env.
    ///
    /// Function-cell lookup clones Lisp values on every call.  Sharing the
    /// immutable code keeps that clone O(1) while the captured environment
    /// retains its independent Lisp identity and mutability.
    Lambda(Rc<LambdaValue>),
    /// A buffer object: (id, name). The id is used for `eq` identity.
    Buffer(Rc<BufferValue>),
    /// A marker object, identified by unique id.
    Marker(u64),
    /// An overlay object, identified by unique id.
    Overlay(u64),
    /// A char-table object, identified by unique id.
    CharTable(u64),
    /// An opaque frame object, identified by unique id.
    Frame(u64),
    /// An opaque terminal object, identified by unique id.
    Terminal(u64),
    /// A record object, identified by unique id.
    Record(u64),
    /// A finalizer object, identified by unique id.
    Finalizer(u64),
    /// Internal marker for EIEIO slots that have not been bound.
    Unbound,
}

/// An environment frame: a list of (name, value) bindings.
/// We use a simple vector of frames for lexical scoping.
pub type Env = Vec<Vec<(String, Value)>>;

pub fn shared_env(env: Env) -> SharedEnv {
    Rc::new(RefCell::new(env))
}

pub(crate) fn make_uninterned_symbol_name(base: &str, id: u64) -> String {
    format!("{base}{UNINTERNED_SYMBOL_MARKER}{id}")
}

pub(crate) fn make_obarray_symbol_name(base: &str, obarray_id: u64) -> String {
    format!("{base}{OBARRAY_SYMBOL_MARKER}{obarray_id}")
}

pub(crate) fn is_uninterned_symbol(symbol: &str) -> bool {
    symbol.contains(UNINTERNED_SYMBOL_MARKER)
}

pub(crate) fn visible_symbol_name(symbol: &str) -> &str {
    symbol
        .split_once(UNINTERNED_SYMBOL_MARKER)
        .or_else(|| symbol.split_once(OBARRAY_SYMBOL_MARKER))
        .map(|(visible, _)| visible)
        .unwrap_or(symbol)
}

fn render_error_symbol_name(symbol: &str) -> String {
    let visible = visible_symbol_name(symbol);
    if visible.is_empty() {
        return "##".into();
    }

    let mut rendered = String::new();
    for ch in visible.chars() {
        if matches!(
            ch,
            '"' | '\\' | '\'' | ';' | '#' | '(' | ')' | ',' | '`' | '[' | ']'
        ) || ch <= ' '
            || ch == '\u{00A0}'
        {
            rendered.push('\\');
        }
        rendered.push(ch);
    }
    rendered
}

pub(crate) fn interned_symbol_value(symbol: String) -> Value {
    match symbol.as_str() {
        "nil" => Value::Nil,
        "t" => Value::T,
        _ => Value::Symbol(symbol.into()),
    }
}

pub(crate) fn format_float(value: f64) -> String {
    let mut rendered = value.to_string();
    if !rendered.contains(['.', 'e', 'E']) {
        rendered.push_str(".0");
    }
    rendered
}

impl Value {
    // Constructors

    pub fn int(n: i64) -> Self {
        Value::Integer(n)
    }

    pub fn big_integer(n: BigInt) -> Self {
        Value::BigInteger(n.into())
    }

    pub fn string(s: &str) -> Self {
        Value::String(s.into())
    }

    pub fn symbol(s: &str) -> Self {
        Value::Symbol(s.into())
    }

    pub fn cons(car: Value, cdr: Value) -> Self {
        Value::Cons(Rc::new(ConsCell::new(car, cdr)))
    }

    pub fn lambda(params: SharedLambdaParams, body: SharedLambdaBody, env: SharedEnv) -> Self {
        Value::Lambda(Rc::new(LambdaValue { params, body, env }))
    }

    pub fn buffer(id: u64, name: impl Into<SharedText>) -> Self {
        Value::Buffer(Rc::new(BufferValue {
            id,
            name: name.into(),
        }))
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
        matches!(self, Value::Nil | Value::T | Value::Symbol(_))
    }

    pub fn is_cons(&self) -> bool {
        matches!(self, Value::Cons(_))
    }

    pub fn is_list(&self) -> bool {
        matches!(self, Value::Nil | Value::Cons(_))
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
            Value::Nil => Ok("nil"),
            Value::T => Ok("t"),
            Value::Symbol(s) => Ok(s),
            _ => Err(LispError::TypeError("symbol".into(), self.type_name())),
        }
    }

    pub fn car(&self) -> Result<Value, LispError> {
        match self {
            Value::Cons(cell) => Ok(cell.car.borrow().clone()),
            Value::Nil => Ok(Value::Nil),
            _ => Err(LispError::TypeError("list".into(), self.type_name())),
        }
    }

    pub fn cdr(&self) -> Result<Value, LispError> {
        match self {
            Value::Cons(cell) => Ok(cell.cdr.borrow().clone()),
            Value::Nil => Ok(Value::Nil),
            _ => Err(LispError::TypeError("list".into(), self.type_name())),
        }
    }

    pub fn set_car(&self, new_car: Value) -> Result<(), LispError> {
        match self {
            Value::Cons(cell) => {
                *cell.car.borrow_mut() = new_car;
                Ok(())
            }
            _ => Err(LispError::TypeError("cons".into(), self.type_name())),
        }
    }

    pub fn set_cdr(&self, new_cdr: Value) -> Result<(), LispError> {
        match self {
            Value::Cons(cell) => {
                *cell.cdr.borrow_mut() = new_cdr;
                Ok(())
            }
            _ => Err(LispError::TypeError("cons".into(), self.type_name())),
        }
    }

    pub fn cons_cells(&self) -> Option<ConsCells> {
        match self {
            Value::Cons(cell) => Some((ConsSlot::car(cell), ConsSlot::cdr(cell))),
            _ => None,
        }
    }

    pub fn cons_id(&self) -> Option<usize> {
        match self {
            Value::Cons(cell) => Some(ConsCell::identity(cell)),
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
        let mut seen = CycleGuard::new();
        loop {
            match current {
                Value::Nil => return Ok(result),
                Value::Cons(cell) => {
                    if seen.step(ConsCell::identity(&cell)) {
                        return Err(circular_list_error());
                    }
                    result.push(cell.car.borrow().clone());
                    current = cell.cdr.borrow().clone();
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
            Value::Cons(_) => "cons".into(),
            Value::BuiltinFunc(name) => format!("builtin<{}>", name),
            Value::Lambda(_) => "lambda".into(),
            Value::Buffer(buffer) => format!("buffer<{}>", buffer.name),
            Value::Marker(id) => format!("marker<{}>", id),
            Value::Overlay(id) => format!("overlay<{}>", id),
            Value::CharTable(id) => format!("char-table<{}>", id),
            Value::Frame(id) => format!("frame<{}>", id),
            Value::Terminal(id) => format!("terminal<{}>", id),
            Value::Record(id) => format!("record<{}>", id),
            Value::Finalizer(id) => format!("finalizer<{}>", id),
            Value::Unbound => "unbound".into(),
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        values_equal_recursive(self, other, &mut None)
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

fn values_equal_recursive(
    left: &Value,
    right: &Value,
    seen: &mut Option<HashSet<(usize, usize)>>,
) -> bool {
    match (left, right) {
        (Value::Nil, Value::Nil) => true,
        (Value::T, Value::T) => true,
        (Value::Integer(a), Value::Integer(b)) => a == b,
        (Value::BigInteger(a), Value::BigInteger(b)) => a == b,
        (Value::Integer(a), Value::BigInteger(b)) | (Value::BigInteger(b), Value::Integer(a)) => {
            BigInt::from(*a) == **b
        }
        (Value::Float(a), Value::Float(b)) => a == b,
        (Value::String(a), Value::String(b)) => a == b,
        (Value::StringObject(a), Value::StringObject(b)) => {
            RefCell::borrow(a.as_ref()).text == RefCell::borrow(b.as_ref()).text
        }
        (Value::String(a), Value::StringObject(b)) => {
            a.as_str() == RefCell::borrow(b.as_ref()).text
        }
        (Value::StringObject(a), Value::String(b)) => {
            RefCell::borrow(a.as_ref()).text == b.as_str()
        }
        (Value::Symbol(a), Value::Symbol(b)) => a == b,
        (Value::Cons(a), Value::Cons(b)) => {
            if Rc::ptr_eq(a, b) {
                return true;
            }
            let ids = (ConsCell::identity(a), ConsCell::identity(b));
            if !seen.get_or_insert_with(HashSet::new).insert(ids) {
                return true;
            }
            values_equal_recursive(&a.car.borrow(), &b.car.borrow(), seen)
                && values_equal_recursive(&a.cdr.borrow(), &b.cdr.borrow(), seen)
        }
        (Value::BuiltinFunc(a), Value::BuiltinFunc(b)) => a == b,
        (Value::Lambda(a), Value::Lambda(b)) => {
            a.params == b.params && a.body == b.body && Rc::ptr_eq(&a.env, &b.env)
        }
        (Value::Buffer(a), Value::Buffer(b)) => a.id == b.id,
        (Value::Marker(a), Value::Marker(b)) => a == b,
        (Value::Overlay(a), Value::Overlay(b)) => a == b,
        (Value::CharTable(a), Value::CharTable(b)) => a == b,
        (Value::Frame(a), Value::Frame(b)) => a == b,
        (Value::Terminal(a), Value::Terminal(b)) => a == b,
        (Value::Record(a), Value::Record(b)) => a == b,
        (Value::Finalizer(a), Value::Finalizer(b)) => a == b,
        (Value::Unbound, Value::Unbound) => true,
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
        Value::Float(v) => write!(f, "{}", format_float(*v)),
        Value::String(s) => write!(f, "\"{}\"", s),
        Value::StringObject(state) => {
            write!(f, "\"{}\"", state.as_ref().borrow().text)
        }
        Value::Symbol(s) => write!(f, "{}", visible_symbol_name(s)),
        Value::Cons(cell) if matches!(&*cell.car.borrow(), Value::Symbol(head) if head == "vector-literal") =>
        {
            // Vector literals ride on conses internally but print as vectors.
            write!(f, "[")?;
            let mut current = cell.cdr.borrow().clone();
            let mut first = true;
            while let Value::Cons(cell) = current {
                if !first {
                    write!(f, " ")?;
                }
                format_value(&cell.car.borrow(), f, seen)?;
                first = false;
                current = cell.cdr.borrow().clone();
            }
            write!(f, "]")
        }
        Value::Cons(cell) => {
            // GNU prints reader shorthands: (quote X) as 'X and
            // (function X) as #'X.
            if let Value::Symbol(head) = &*cell.car.borrow()
                && (head == "quote" || head == "function")
                && let Value::Cons(inner) = &*cell.cdr.borrow()
                && matches!(&*inner.cdr.borrow(), Value::Nil)
            {
                write!(f, "{}", if head == "quote" { "'" } else { "#'" })?;
                return format_value(&inner.car.borrow(), f, seen);
            }
            write!(f, "(")?;
            let mut current = value.clone();
            let mut first = true;
            loop {
                match current {
                    Value::Cons(cell) => {
                        let id = ConsCell::identity(&cell);
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
                        format_value(&cell.car.borrow(), f, seen)?;
                        first = false;
                        current = cell.cdr.borrow().clone();
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
        Value::Lambda(lambda) => write!(f, "#<lambda ({})>", lambda.params.join(" ")),
        Value::Buffer(buffer) => write!(f, "#<buffer {}>", buffer.name),
        Value::Marker(id) => write!(f, "#<marker id:{}>", id),
        Value::Overlay(id) => write!(f, "#<overlay id:{}>", id),
        Value::CharTable(id) => write!(f, "#<char-table id:{}>", id),
        Value::Frame(id) => write!(f, "#<frame id:{}>", id),
        Value::Terminal(id) => write!(f, "#<terminal id:{}>", id),
        Value::Record(id) => write!(f, "#<record id:{}>", id),
        Value::Finalizer(id) => write!(f, "#<finalizer id:{}>", id),
        Value::Unbound => write!(f, "#<unbound>"),
    }
}

/// An orderly process termination requested by `kill-emacs`.
///
/// This is evaluator control flow, not a Lisp condition: GNU's native
/// `kill-emacs` is `noreturn`, so `condition-case`, `handler-bind`, and
/// `unwind-protect` cannot intercept it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EmacsTermination {
    pub exit_code: i32,
    pub restart: bool,
}

/// Lisp errors and non-local evaluator control flow.
#[derive(Clone, Debug)]
pub enum LispError {
    /// Type mismatch: expected, got
    TypeError(String, String),
    /// Unbound variable
    Void(String),
    /// Unbound function cell
    VoidFunction(String),
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
    /// Orderly, non-catchable process termination via `kill-emacs`.
    Terminate(EmacsTermination),
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
            LispError::VoidFunction(_) => "void-function".into(),
            LispError::WrongNumberOfArgs(_, _) => "wrong-number-of-arguments".into(),
            LispError::Signal(_) => "error".into(),
            LispError::SignalValue(value) => match value.car() {
                Ok(Value::Symbol(symbol)) => symbol.to_string(),
                _ => "error".into(),
            },
            LispError::ErtTestFailed(_) => "ert-test-failed".into(),
            LispError::Throw(_, _) => "no-catch".into(),
            // This name is only a defensive fallback for diagnostics.
            // Evaluator condition machinery must propagate Terminate before
            // asking for a condition type.
            LispError::Terminate(_) => "emaxx--process-termination".into(),
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
            LispError::Void(name) => write!(
                f,
                "Symbol's value as variable is void: {}",
                render_error_symbol_name(name)
            ),
            LispError::VoidFunction(name) => {
                write!(
                    f,
                    "Symbol's function definition is void: {}",
                    render_error_symbol_name(name)
                )
            }
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
            LispError::Terminate(termination) => {
                if termination.restart {
                    write!(
                        f,
                        "Emacs requested restart with exit code {}",
                        termination.exit_code
                    )
                } else {
                    write!(
                        f,
                        "Emacs requested exit with code {}",
                        termination.exit_code
                    )
                }
            }
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

#[cfg(test)]
mod tests {
    use super::{
        LispError, SharedCons, SymbolName, Value, make_uninterned_symbol_name, shared_env,
    };
    use std::rc::Rc;

    #[test]
    fn value_fits_in_two_machine_words() {
        assert_eq!(
            std::mem::size_of::<Value>(),
            2 * std::mem::size_of::<usize>(),
            "Value clone and stack traffic depends on the compact two-word representation",
        );
    }

    #[test]
    fn cloning_string_reuses_the_text_allocation() {
        let value = Value::string("shared text");
        let clone = value.clone();
        let (Value::String(text), Value::String(cloned_text)) = (&value, &clone) else {
            unreachable!("constructed string values")
        };

        assert!(Rc::ptr_eq(&text.0, &cloned_text.0));
    }

    #[test]
    fn cloning_big_integer_reuses_the_integer_allocation() {
        let value = Value::big_integer(num_bigint::BigInt::from(1_u8) << 256);
        let clone = value.clone();
        let (Value::BigInteger(integer), Value::BigInteger(cloned_integer)) = (&value, &clone)
        else {
            unreachable!("constructed big integer values")
        };

        assert!(Rc::ptr_eq(&integer.0, &cloned_integer.0));
    }

    #[test]
    fn interned_symbol_names_reuse_one_text_allocation() {
        let first = SymbolName::from("emaxx-compact-symbol-test");
        let second = SymbolName::from("emaxx-compact-symbol-test");

        assert!(Rc::ptr_eq(&first.0.0, &second.0.0));
    }

    #[test]
    fn uninterned_symbol_names_remain_reclaimable() {
        let weak = {
            let name = SymbolName::from(make_uninterned_symbol_name("temporary", 1));
            Rc::downgrade(&name.0.0)
        };

        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn nil_and_t_count_as_symbols() {
        assert!(Value::Nil.is_symbol());
        assert!(Value::T.is_symbol());
        assert_eq!(Value::Nil.as_symbol().expect("nil is a symbol"), "nil");
        assert_eq!(Value::T.as_symbol().expect("t is a symbol"), "t");
    }

    #[test]
    fn cloning_lambda_shares_immutable_parameters() {
        let lambda = Value::lambda(
            vec!["value".into()].into(),
            Vec::new().into(),
            shared_env(Vec::new()),
        );
        let clone = lambda.clone();

        let (Value::Lambda(lambda), Value::Lambda(cloned_lambda)) = (&lambda, &clone) else {
            unreachable!("constructed lambda values")
        };
        assert!(Rc::ptr_eq(lambda, cloned_lambda));
        assert!(Rc::ptr_eq(&lambda.params, &cloned_lambda.params));
    }

    #[test]
    fn cloning_buffer_reuses_the_buffer_descriptor() {
        let buffer = Value::buffer(7, "shared buffer");
        let clone = buffer.clone();
        let (Value::Buffer(buffer), Value::Buffer(cloned_buffer)) = (&buffer, &clone) else {
            unreachable!("constructed buffer values")
        };

        assert!(Rc::ptr_eq(buffer, cloned_buffer));
    }

    #[test]
    fn cons_fields_share_one_cell_and_mutate_independently() {
        assert_eq!(
            std::mem::size_of::<SharedCons>(),
            std::mem::size_of::<usize>(),
            "a Value::Cons must retain exactly one shared pointer",
        );

        let pair = Value::cons(Value::Integer(1), Value::Integer(2));
        let clone = pair.clone();
        let (car, cdr) = pair.cons_cells().expect("constructed cons");
        let (cloned_car, cloned_cdr) = clone.cons_cells().expect("cloned cons");

        assert_eq!(car.cell_id(), cdr.cell_id());
        assert!(!car.ptr_eq(&cdr), "car and cdr are distinct field handles");
        assert!(car.ptr_eq(&cloned_car));
        assert!(cdr.ptr_eq(&cloned_cdr));

        let mut car_value = car.borrow_mut();
        let mut cdr_value = cdr.borrow_mut();
        *car_value = Value::Integer(3);
        *cdr_value = Value::Integer(4);
        drop((car_value, cdr_value));

        assert_eq!(clone.car().expect("car"), Value::Integer(3));
        assert_eq!(clone.cdr().expect("cdr"), Value::Integer(4));
    }

    #[test]
    fn weak_cons_slot_does_not_keep_cell_alive() {
        let weak = {
            let pair = Value::cons(Value::T, Value::Nil);
            let (car, _) = pair.cons_cells().expect("constructed cons");
            car.downgrade()
        };

        assert!(weak.upgrade().is_none());
    }

    #[test]
    fn void_function_errors_print_function_symbols_readably() {
        assert_eq!(
            LispError::VoidFunction("not-defined".into()).to_string(),
            "Symbol's function definition is void: not-defined"
        );
        assert_eq!(
            LispError::VoidFunction("(setf gv-test-foo)".into()).to_string(),
            r"Symbol's function definition is void: \(setf\ gv-test-foo\)"
        );
    }
}
