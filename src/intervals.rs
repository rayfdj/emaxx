//! intervals.c, and the half of textprop.c that works on intervals: the
//! text properties of a buffer or a string as GNU keeps them.
//!
//! An object with intervals has them over its whole text, [BEG, Z) of a
//! buffer or [0, SCHARS) of a string, each with a plist that is a Lisp
//! list of its own: `text-properties-at' returns it, `add-text-properties'
//! changes a value in it with Fsetcar, and an interval split in two gives
//! the new one Fcopy_sequence of it.  Nothing merges two intervals because
//! their plists are equal: adjacent intervals with equal properties stay
//! apart, and an interval whose properties are all removed stays, with a
//! nil plist, until its text goes.  `object-intervals' and the printer
//! show where they fall; `next-property-change' passes over neighbours
//! with equal plists (intervals_equal), so it does not.
//!
//! GNU balances its intervals as a binary tree; here they are the same
//! sequence in a vector, each by its start, which is all a Lisp program
//! sees of them.

use crate::lisp::alloc::counters::note_intervals;
use crate::lisp::types::symbols::SymbolKey;
use crate::lisp::types::{Kind, LispError, Value};

const CATEGORY: SymbolKey = SymbolKey::new("category");
const FRONT_STICKY: SymbolKey = SymbolKey::new("front-sticky");
const REAR_NONSTICKY: SymbolKey = SymbolKey::new("rear-nonsticky");
const TEXT_PROPERTY_DEFAULT_NONSTICKY: SymbolKey =
    SymbolKey::new("text-property-default-nonsticky");
const CHAR_PROPERTY_ALIAS_ALIST: SymbolKey = SymbolKey::new("char-property-alias-alist");
const DEFAULT_TEXT_PROPERTIES: SymbolKey = SymbolKey::new("default-text-properties");

/// The symbol a key names, as a value.
#[inline]
pub(crate) fn symbol(key: SymbolKey) -> Value {
    key.with(|symbol| Value::Symbol(*symbol))
}

/// One interval: where it starts and its plist.  It ends where the next
/// one starts, or at the end of the text.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Interval {
    pub(crate) start: usize,
    pub(crate) plist: Value,
}

/// An object's intervals: never empty, in order, the first starting at
/// the object's first position.
#[derive(Clone, Debug)]
pub(crate) struct Intervals {
    items: Items,
    end: usize,
}

/// The intervals in order, with a gap where they last changed, as a
/// buffer's text keeps its gap: a split or merge there moves nothing, so
/// the splits a pass makes one after another (concat's and
/// copy_text_properties' from the last interval back, a graft's from the
/// first on) cost a move of the gap and not each a move of every interval
/// after it, where GNU's balanced tree splits in logarithmic time.  The
/// first few are held in place: a string's intervals (propertize's one,
/// concat's few) take one allocation, as GNU's take one block slot each.
/// The gap's slots hold stale copies.
#[derive(Clone, Debug)]
struct Items {
    slots: smallvec::SmallVec<[Interval; 3]>,
    gap_start: usize,
    gap_end: usize,
}

impl Items {
    fn from_slots(slots: smallvec::SmallVec<[Interval; 3]>) -> Self {
        let len = slots.len();
        Self {
            slots,
            gap_start: len,
            gap_end: len,
        }
    }

    fn one(item: Interval) -> Self {
        Self::from_slots(smallvec::smallvec![item])
    }

    #[inline]
    fn len(&self) -> usize {
        self.slots.len() - (self.gap_end - self.gap_start)
    }

    /// The slot of the Ith interval, past the slots for I out of range
    /// (an index there panics).
    #[inline(always)]
    fn slot(&self, i: usize) -> usize {
        if i < self.gap_start {
            i
        } else {
            i + (self.gap_end - self.gap_start)
        }
    }

    #[inline]
    fn get(&self, i: usize) -> Option<&Interval> {
        self.slots.get(self.slot(i))
    }

    /// The Ith interval and the start of the one after it, if any.
    #[inline]
    fn with_next_start(&self, i: usize) -> (&Interval, Option<usize>) {
        let slot = self.slot(i);
        let next = if slot + 1 == self.gap_start {
            self.gap_end
        } else {
            slot + 1
        };
        (
            &self.slots[slot],
            self.slots.get(next).map(|next| next.start),
        )
    }

    /// The gap moved to before the Ith interval.
    fn move_gap(&mut self, to: usize) {
        if to < self.gap_start {
            let count = self.gap_start - to;
            self.slots
                .copy_within(to..self.gap_start, self.gap_end - count);
            self.gap_start = to;
            self.gap_end -= count;
        } else if to > self.gap_start {
            let count = to - self.gap_start;
            self.slots
                .copy_within(self.gap_end..self.gap_end + count, self.gap_start);
            self.gap_start = to;
            self.gap_end += count;
        }
    }

    /// ITEM made the Ith interval.
    fn insert(&mut self, i: usize, item: Interval) {
        assert!(i <= self.len(), "interval index out of range");
        self.move_gap(i);
        if self.gap_start == self.gap_end {
            let grow = self.slots.len().max(4);
            self.slots
                .insert_many(self.gap_start, std::iter::repeat_n(item, grow));
            self.gap_end = self.gap_start + grow;
        }
        self.slots[self.gap_start] = item;
        self.gap_start += 1;
    }

    /// The Ith interval gone.
    fn remove(&mut self, i: usize) {
        assert!(i < self.len(), "interval index out of range");
        self.move_gap(i);
        self.gap_end += 1;
    }

    fn iter(&self) -> impl Iterator<Item = &Interval> + '_ {
        self.slots[..self.gap_start]
            .iter()
            .chain(&self.slots[self.gap_end..])
    }

    /// The intervals from the Ith on.
    fn iter_mut_from(&mut self, from: usize) -> impl Iterator<Item = &mut Interval> + '_ {
        let (gap_start, gap_end) = (self.gap_start, self.gap_end);
        let (before, after) = self.slots.split_at_mut(gap_end);
        let (left, right) = if from < gap_start {
            (&mut before[from..gap_start], after)
        } else {
            (&mut before[..0], &mut after[from - gap_start..])
        };
        left.iter_mut().chain(right.iter_mut())
    }

    /// The count of the leading intervals PRED holds of, which holds of
    /// a prefix of them.
    fn partition_point(&self, pred: impl Fn(&Interval) -> bool) -> usize {
        let before = &self.slots[..self.gap_start];
        let point = before.partition_point(&pred);
        if point < before.len() {
            return point;
        }
        point + self.slots[self.gap_end..].partition_point(pred)
    }

    /// The intervals as one slice, the gap moved after them.
    fn make_contiguous(&mut self) -> &mut [Interval] {
        self.move_gap(self.len());
        &mut self.slots[..self.gap_start]
    }

    /// All but the first LEN intervals gone.
    fn truncate(&mut self, len: usize) {
        self.move_gap(self.len());
        self.slots.truncate(len);
        self.gap_start = len;
        self.gap_end = len;
    }
}

impl std::ops::Index<usize> for Items {
    type Output = Interval;

    #[inline(always)]
    fn index(&self, i: usize) -> &Interval {
        &self.slots[self.slot(i)]
    }
}

impl std::ops::IndexMut<usize> for Items {
    #[inline(always)]
    fn index_mut(&mut self, i: usize) -> &mut Interval {
        let slot = self.slot(i);
        &mut self.slots[slot]
    }
}

impl FromIterator<Interval> for Items {
    fn from_iter<I: IntoIterator<Item = Interval>>(iter: I) -> Self {
        Self::from_slots(iter.into_iter().collect())
    }
}

/// EQ.
#[inline]
fn eq(left: Value, right: Value) -> bool {
    left.word() == right.word()
}

fn listp_error(value: Value) -> LispError {
    LispError::WrongTypeArgument("listp".into(), value)
}

/// Fcar.
#[inline]
pub(crate) fn fcar(value: Value) -> Result<Value, LispError> {
    match value.cons_ref() {
        Some(cell) => Ok(cell.car.get()),
        None if value.is_nil() => Ok(Value::Nil),
        None => Err(listp_error(value)),
    }
}

/// Fcdr.
#[inline]
pub(crate) fn fcdr(value: Value) -> Result<Value, LispError> {
    match value.cons_ref() {
        Some(cell) => Ok(cell.cdr.get()),
        None if value.is_nil() => Ok(Value::Nil),
        None => Err(listp_error(value)),
    }
}

/// XCAR of a cons.
#[inline]
fn xcar(value: Value) -> Value {
    value.cons_ref().map_or(Value::Nil, |cell| cell.car.get())
}

/// XCDR of a cons.
#[inline]
fn xcdr(value: Value) -> Value {
    value.cons_ref().map_or(Value::Nil, |cell| cell.cdr.get())
}

#[inline]
fn consp(value: Value) -> bool {
    value.cons_ref().is_some()
}

/// A plist's next pair: Fcdr (XCDR (TAIL)), the step of the plist walks
/// of intervals.c and textprop.c.
#[inline]
pub(crate) fn plist_next(tail: Value) -> Result<Value, LispError> {
    fcdr(xcdr(tail))
}

/// Fcopy_sequence of a plist: a fresh list of its elements.
pub(crate) fn copy_plist(plist: Value) -> Result<Value, LispError> {
    if plist.is_nil() {
        return Ok(Value::Nil);
    }
    let mut items = Vec::new();
    let mut tail = plist;
    let mut seen = crate::lisp::types::CycleGuard::new();
    while let Some(cell) = tail.cons_ref() {
        items.push(cell.car.get());
        if seen.step(crate::lisp::types::ConsCell::identity(&cell)) {
            return Err(LispError::SignalValue(Value::list([
                Value::symbol("circular-list"),
                plist,
            ])));
        }
        tail = cell.cdr.get();
    }
    if !tail.is_nil() {
        return Err(listp_error(tail));
    }
    Ok(Value::list(items))
}

/// Fmemq, a circular list's elements once.
pub(crate) fn memq(elt: Value, list: Value) -> bool {
    let mut tail = list;
    let mut seen = crate::lisp::types::CycleGuard::new();
    while let Some(cell) = tail.cons_ref() {
        if eq(cell.car.get(), elt) {
            return true;
        }
        if seen.step(crate::lisp::types::ConsCell::identity(&cell)) {
            return false;
        }
        tail = cell.cdr.get();
    }
    false
}

/// Fassq.
#[inline]
pub(crate) fn assq(key: Value, list: Value) -> Result<Value, LispError> {
    assq_by(|car| eq(car, key), list)
}

/// Fassq of the key IS_KEY holds of: FOR_EACH_TAIL over LIST for an
/// element that is a cons whose car is the key, then CHECK_LIST_END,
/// which reports LIST itself.
pub(crate) fn assq_by(is_key: impl Fn(Value) -> bool, list: Value) -> Result<Value, LispError> {
    let mut tail = list;
    let mut walk = crate::lisp::types::TailWalk::new(tail);
    while let Some(cell) = tail.cons_ref() {
        let entry = cell.car.get();
        if let Some(pair) = entry.cons_ref()
            && is_key(pair.car.get())
        {
            return Ok(entry);
        }
        tail = cell.cdr.get();
        if walk.cycled_no_quit(tail) {
            return Err(crate::lisp::types::circular_list(tail));
        }
    }
    if tail.is_nil() {
        Ok(Value::Nil)
    } else {
        Err(listp_error(list))
    }
}

/// textprop.c TMEM: SYM in SET when SET is a list, else SET non-nil.
pub(crate) fn tmem(sym: Value, set: Value) -> bool {
    if consp(set) {
        memq(sym, set)
    } else {
        !set.is_nil()
    }
}

/// fns.c plist_get: PROP's value in PLIST by EQ, nil if none, a malformed
/// tail ending the search.
pub(crate) fn plist_get(plist: Value, prop: Value) -> Value {
    let mut tail = plist;
    let mut seen = crate::lisp::types::CycleGuard::new();
    while let Some(cell) = tail.cons_ref() {
        let Some(value) = cell.cdr.get().cons_ref() else {
            break;
        };
        if eq(cell.car.get(), prop) {
            return value.car.get();
        }
        if seen.step(crate::lisp::types::ConsCell::identity(&cell)) {
            break;
        }
        tail = value.cdr.get();
    }
    Value::Nil
}

/// SYMBOLP.
#[inline]
fn symbolp(value: Value) -> bool {
    matches!(value.kind(), Kind::Symbol(_) | Kind::Nil | Kind::T)
}

/// A symbol's own plist (nil and t included), nil for anything else.
pub(crate) fn symbol_plist(value: Value) -> Value {
    match value.kind() {
        Kind::Symbol(name) => name.cell().plist.get(),
        Kind::Nil => crate::lisp::types::SymbolName::find("nil")
            .map_or(Value::Nil, |name| name.cell().plist.get()),
        Kind::T => crate::lisp::types::SymbolName::find("t")
            .map_or(Value::Nil, |name| name.cell().plist.get()),
        _ => Value::Nil,
    }
}

/// Fget of a symbol, from its own plist.
pub(crate) fn symbol_get(symbol: Value, prop: Value) -> Value {
    plist_get(symbol_plist(symbol), prop)
}

/// intervals.c:lookup_char_property: PROP's value in PLIST by EQ; else
/// the value PROP has for the last `category' symbol PLIST names, when
/// non-nil; else the first non-nil value in PLIST of an alternative
/// `char-property-alias-alist' gives PROP; else, for a text property
/// (TEXTPROP), PROP's value in `default-text-properties'.  CONTEXT gives
/// the symbol and the two variables, each read where first needed.  The
/// alternatives are looked up for an empty plist too: Fassq signals for
/// an alist that is not a list.
#[inline]
pub(crate) fn lookup_char_property(
    plist: Value,
    prop: Value,
    textprop: bool,
    context: &mut impl LookupContext,
) -> Result<Value, LispError> {
    let mut fallback = Value::Nil;
    if consp(plist) {
        let category = context.category();
        let mut tail = plist;
        while consp(tail) {
            let tem = xcar(tail);
            if eq(prop, tem) {
                return fcar(xcdr(tail));
            }
            if eq(tem, category) {
                let tem = fcar(xcdr(tail))?;
                if symbolp(tem) {
                    fallback = symbol_get(tem, prop);
                }
            }
            tail = plist_next(tail)?;
        }
        if !fallback.is_nil() {
            return Ok(fallback);
        }
    }
    // Alternative properties.
    let entry = assq(prop, context.aliases())?;
    if !entry.is_nil() {
        let mut tail = xcdr(entry);
        while fallback.is_nil() && consp(tail) {
            fallback = plist_get(plist, xcar(tail));
            tail = xcdr(tail);
        }
    }
    if textprop && fallback.is_nil() {
        let defaults = context.defaults();
        if consp(defaults) {
            fallback = plist_get(defaults, prop);
        }
    }
    Ok(fallback)
}

/// What lookup_char_property reads besides the plist: Qcategory, and
/// where the plist lacks the property the C variables as they are for the
/// current buffer.
pub(crate) trait LookupContext {
    /// Qcategory.
    fn category(&mut self) -> Value {
        symbol(CATEGORY)
    }
    /// Vchar_property_alias_alist.
    fn aliases(&mut self) -> Value;
    /// Vdefault_text_properties.
    fn defaults(&mut self) -> Value;
}

/// The variables lookup_char_property and an insertion's stickiness
/// read, as the C variables hold them for the current buffer:
/// `text-property-default-nonsticky', `char-property-alias-alist' and
/// `default-text-properties'.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PropertyVariables {
    pub(crate) default_nonsticky: Value,
    pub(crate) aliases: Value,
    pub(crate) defaults: Value,
}

impl PropertyVariables {
    /// All three nil, for text whose properties no Lisp program reads.
    pub(crate) const NONE: Self = Self {
        default_nonsticky: Value::Nil,
        aliases: Value::Nil,
        defaults: Value::Nil,
    };

    /// The three as INTERP reads them now.
    pub(crate) fn read(interp: &crate::lisp::eval::Interpreter) -> Self {
        let read = |key: SymbolKey| interp.symbol_value_key(key).unwrap_or(Value::Nil);
        Self {
            default_nonsticky: read(TEXT_PROPERTY_DEFAULT_NONSTICKY),
            aliases: read(CHAR_PROPERTY_ALIAS_ALIST),
            defaults: read(DEFAULT_TEXT_PROPERTIES),
        }
    }

    /// intervals.c:textget of these variables.
    pub(crate) fn textget(&self, plist: Value, prop: Value) -> Result<Value, LispError> {
        lookup_char_property(plist, prop, true, &mut { *self })
    }
}

impl LookupContext for PropertyVariables {
    fn aliases(&mut self) -> Value {
        self.aliases
    }

    fn defaults(&mut self) -> Value {
        self.defaults
    }
}

impl PropertyHost for PropertyVariables {
    fn textget(&mut self, plist: Value, prop: Value) -> Result<Value, LispError> {
        PropertyVariables::textget(self, plist, prop)
    }

    fn default_nonsticky(&mut self) -> Value {
        self.default_nonsticky
    }

    fn get(&mut self, symbol: Value, prop: Value) -> Value {
        symbol_get(symbol, prop)
    }
}

/// What the interval code needs of the interpreter: the plists' lookup
/// through categories and aliases, and the variable naming the default
/// stickiness.
pub(crate) trait PropertyHost {
    /// intervals.c:textget (lookup_char_property with TEXTPROP).
    fn textget(&mut self, plist: Value, prop: Value) -> Result<Value, LispError>;
    /// Vtext_property_default_nonsticky.
    fn default_nonsticky(&mut self) -> Value;
    /// Fget.
    fn get(&mut self, symbol: Value, prop: Value) -> Value;
}

/// undo.c:record_property_change, for the intervals of a buffer: the
/// interval [START, START + LENGTH) is about to lose PROP's VALUE (nil
/// for none).  A string's intervals record nothing.
pub(crate) trait PropertyRecorder {
    fn record(&mut self, start: usize, length: usize, prop: Value, value: Value);
}

/// The recorder of a string's intervals.
pub(crate) struct NoRecord;

impl PropertyRecorder for NoRecord {
    fn record(&mut self, _start: usize, _length: usize, _prop: Value, _value: Value) {}
}

/// textprop.c enum property_set_type.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum PropertySetType {
    Replace,
    Prepend,
    Append,
}

impl Intervals {
    /// create_root_interval: one interval over [ORIGIN, END), no
    /// properties.
    pub(crate) fn root(origin: usize, end: usize) -> Self {
        debug_assert!(origin < end);
        note_intervals(1);
        Self {
            items: Items::one(Interval {
                start: origin,
                plist: Value::Nil,
            }),
            end,
        }
    }

    /// One interval over [ORIGIN, END) with PLIST.
    #[cfg(test)]
    pub(crate) fn with_plist(origin: usize, end: usize, plist: Value) -> Self {
        let mut intervals = Self::root(origin, end);
        intervals.set_plist(0, plist);
        intervals
    }

    /// Intervals from ITEMS, in order, the last ending at END.
    pub(crate) fn from_items(items: Vec<Interval>, end: usize) -> Self {
        debug_assert!(!items.is_empty());
        debug_assert!(items.windows(2).all(|pair| pair[0].start < pair[1].start));
        debug_assert!(items.last().is_some_and(|last| last.start < end));
        Self {
            items: Items::from_slots(smallvec::SmallVec::from_vec(items)),
            end,
        }
    }

    pub(crate) fn origin(&self) -> usize {
        self.items[0].start
    }

    pub(crate) fn end(&self) -> usize {
        self.end
    }

    /// TOTAL_LENGTH of the tree.
    pub(crate) fn total_length(&self) -> usize {
        self.end - self.origin()
    }

    #[inline]
    pub(crate) fn count(&self) -> usize {
        self.items.len()
    }

    #[inline]
    pub(crate) fn start(&self, i: usize) -> usize {
        self.items[i].start
    }

    /// INTERVAL_LAST_POS: where interval I ends.
    #[inline]
    pub(crate) fn last_pos(&self, i: usize) -> usize {
        self.items.get(i + 1).map_or(self.end, |next| next.start)
    }

    /// Interval I's start, INTERVAL_LAST_POS and plist.
    #[inline]
    pub(crate) fn bounds(&self, i: usize) -> (usize, usize, Value) {
        let (item, next) = self.items.with_next_start(i);
        (item.start, next.unwrap_or(self.end), item.plist)
    }

    /// LENGTH.
    pub(crate) fn length(&self, i: usize) -> usize {
        self.last_pos(i) - self.start(i)
    }

    #[inline]
    pub(crate) fn plist(&self, i: usize) -> Value {
        self.items[i].plist
    }

    /// set_interval_plist.
    pub(crate) fn set_plist(&mut self, i: usize, plist: Value) {
        self.items[i].plist = plist;
    }

    /// Each interval's (start, end, plist).
    pub(crate) fn iter(&self) -> impl Iterator<Item = (usize, usize, Value)> + '_ {
        (0..self.items.len()).map(|i| (self.start(i), self.last_pos(i), self.plist(i)))
    }

    /// Each plist, for the collector.
    pub(crate) fn plists(&self) -> impl Iterator<Item = Value> + '_ {
        self.items.iter().map(|item| item.plist)
    }

    /// Each plist rewritten through COPY.
    pub(crate) fn rewrite_plists(&mut self, mut copy: impl FnMut(Value) -> Value) {
        for item in self.items.iter_mut_from(0) {
            item.plist = copy(item.plist);
        }
    }

    /// find_interval: the interval holding POS, the last one for POS at
    /// the end of the text.
    pub(crate) fn find(&self, pos: usize) -> usize {
        debug_assert!(self.origin() <= pos && pos <= self.end);
        self.items
            .partition_point(|item| item.start <= pos)
            .saturating_sub(1)
    }

    /// next_interval.
    #[inline]
    pub(crate) fn next(&self, i: usize) -> Option<usize> {
        (i + 1 < self.items.len()).then_some(i + 1)
    }

    /// previous_interval.
    #[inline]
    pub(crate) fn previous(&self, i: usize) -> Option<usize> {
        i.checked_sub(1)
    }

    /// split_interval_right: interval I keeps its first OFFSET characters
    /// and its plist; the rest is a new interval after it, with none,
    /// returned.
    pub(crate) fn split_right(&mut self, i: usize, offset: usize) -> usize {
        debug_assert!(0 < offset && offset < self.length(i));
        note_intervals(1);
        let start = self.items[i].start + offset;
        self.items.insert(
            i + 1,
            Interval {
                start,
                plist: Value::Nil,
            },
        );
        i + 1
    }

    /// split_interval_left: interval I's first OFFSET characters become a
    /// new interval before it, with no plist, returned; I, the rest,
    /// keeps its plist and is the one after.
    pub(crate) fn split_left(&mut self, i: usize, offset: usize) -> usize {
        debug_assert!(0 < offset && offset < self.length(i));
        note_intervals(1);
        let start = self.items[i].start;
        self.items[i].start = start + offset;
        self.items.insert(
            i,
            Interval {
                start,
                plist: Value::Nil,
            },
        );
        i
    }

    /// merge_interval_left: interval I's text joins its predecessor, whose
    /// plist the merged interval keeps; the predecessor returned.
    pub(crate) fn merge_left(&mut self, i: usize) -> usize {
        debug_assert!(i > 0);
        self.items.remove(i);
        i - 1
    }

    /// merge_interval_right: interval I's text joins its successor, whose
    /// plist the merged interval keeps; the successor returned.
    pub(crate) fn merge_right(&mut self, i: usize) -> usize {
        debug_assert!(i + 1 < self.items.len());
        let start = self.items[i].start;
        self.items.remove(i);
        self.items[i].start = start;
        i
    }

    /// copy_properties: interval TARGET gets Fcopy_sequence of SOURCE, a
    /// plist, unless both are nil.
    pub(crate) fn copy_properties(
        &mut self,
        source: Value,
        target: usize,
    ) -> Result<(), LispError> {
        if source.is_nil() && self.items[target].plist.is_nil() {
            return Ok(());
        }
        self.items[target].plist = copy_plist(source)?;
        Ok(())
    }

    /// The text grew by LENGTH characters at POSITION, after interval
    /// I's start: every interval after I moves.
    fn grow(&mut self, i: usize, length: usize) {
        for item in self.items.iter_mut_from(i + 1) {
            item.start += length;
        }
        self.end += length;
    }

    /// intervals.c:adjust_intervals_for_insertion: LENGTH characters
    /// inserted at POSITION, of a buffer's text (its origin BEG).  Text
    /// inserted inside an interval extends it unless one of its
    /// properties is not sticky there, when it is split, and the
    /// insertion joins the left side; text between two intervals goes to
    /// the left one, then is split off with what merge_properties_sticky
    /// gives it of the plists on either side, when that differs.
    pub(crate) fn adjust_for_insertion(
        &mut self,
        host: &mut dyn PropertyHost,
        position: usize,
        length: usize,
    ) -> Result<(), LispError> {
        debug_assert!(self.total_length() > 0);
        let beg = self.origin();
        let (position, eobp) = if position >= self.end {
            (self.end, true)
        } else {
            (position, false)
        };
        let mut i = self.find(position);

        // If in the middle of an interval which is not sticky either way,
        // we must not just give its properties to the insertion: split
        // it at the insertion point, checking each property.
        if !(position == self.start(i) || eobp) {
            let plist = self.plist(i);
            let mut tail = plist;
            let rear = quiet(host.textget(plist, symbol(REAR_NONSTICKY)));
            if consp(rear) || rear.is_nil() {
                let front = quiet(host.textget(plist, symbol(FRONT_STICKY)));
                if !consp(front) && !front.is_nil() {
                    // All properties are sticky: no split.
                    tail = Value::Nil;
                } else {
                    let defaults = host.default_nonsticky();
                    while consp(tail) {
                        let prop = xcar(tail);
                        // Is this particular property front-sticky?
                        if !(consp(front) && memq(prop, front)) {
                            // Rear-nonsticky?
                            if consp(rear) && memq(prop, rear) {
                                break;
                            }
                            // Recorded in Vtext_property_default_nonsticky?
                            // (GNU breaks here for any entry: its test of a
                            // nil entry is never true of a cons.)
                            if consp(quiet(assq(prop, defaults))) {
                                break;
                            }
                        }
                        tail = plist_next(tail)?;
                    }
                }
            }
            // Otherwise all properties are nonsticky: split.
            if !tail.is_nil() {
                let temp = self.split_right(i, position - self.start(i));
                self.copy_properties(self.plist(i), temp)?;
                i = temp;
            }
        }

        if position == self.start(i) || eobp {
            // Between intervals: the left one, if there is one, takes the
            // text; a part is split off later if stickiness demands it.
            let (prev, i) = if position == beg {
                (None, Some(i))
            } else if eobp {
                (Some(i), None)
            } else {
                (Some(i - 1), Some(i))
            };
            let grown = prev.or(i).expect("an interval at the insertion");
            self.grow(grown, length);
            let pleft = prev.map_or(Value::Nil, |prev| self.plist(prev));
            let pright = i.map_or(Value::Nil, |i| self.plist(i));
            let newi = merge_properties_sticky(host, pleft, pright)?;
            match prev {
                None => {
                    let i = i.expect("the interval at BEG");
                    if !intervals_equal(self.plist(i), newi) {
                        let new = self.split_left(i, length);
                        self.set_plist(new, newi);
                    }
                }
                Some(prev) => {
                    if !intervals_equal(self.plist(prev), newi) {
                        let new = self.split_right(prev, position - self.start(prev));
                        self.set_plist(new, newi);
                        if i.is_some() && intervals_equal(self.plist(new), self.plist(new + 1)) {
                            self.merge_right(new);
                        }
                    }
                }
            }
        } else {
            // Otherwise just extend the interval.
            self.grow(i, length);
        }
        Ok(())
    }

    /// intervals.c:adjust_intervals_for_deletion: LENGTH characters gone
    /// from START.  False where they are the whole text, whose intervals
    /// then go (set_buffer_intervals of NULL).  Each interval keeps what
    /// it had of the rest, one left with nothing going; no two merge.
    pub(crate) fn adjust_for_deletion(&mut self, start: usize, length: usize) -> bool {
        debug_assert!(self.origin() <= start && start + length <= self.end);
        if length == 0 {
            return true;
        }
        if length == self.total_length() {
            return false;
        }
        let to = start + length;
        // The intervals after the one holding START: those that began in
        // the deleted text begin at START, the rest move back.
        let holder = self.find(start);
        for item in self.items.iter_mut_from(holder + 1) {
            if item.start >= to {
                item.start -= length;
            } else {
                item.start = start;
            }
        }
        self.end -= length;
        // Delete each interval left with no text: one whose start the
        // next interval (or the end) now shares, all of them from HOLDER
        // to the first interval beginning after START.
        let mut i = holder;
        while i < self.items.len() {
            let next_start = self.items.get(i + 1).map_or(self.end, |next| next.start);
            if self.items[i].start == next_start {
                self.items.remove(i);
            } else if self.items[i].start > start {
                break;
            } else {
                i += 1;
            }
        }
        true
    }

    /// intervals.c:copy_intervals: the intervals of [START, START +
    /// LENGTH) as a new tree from 0, each plist a copy; none where the
    /// range lies inside one interval with no properties (by GNU's test,
    /// which leaves a range reaching within one character of its end
    /// copied).
    pub(crate) fn copy(&self, start: usize, length: usize) -> Result<Option<Self>, LispError> {
        if length == 0 {
            return Ok(None);
        }
        let mut i = self.find(start);
        if (start - self.start(i)) + 1 + length < self.length(i) && self.plist(i).is_nil() {
            return Ok(None);
        }
        let mut slots: smallvec::SmallVec<[Interval; 3]> = smallvec::smallvec![Interval {
            start: 0,
            plist: copy_plist(self.plist(i))?,
        }];
        let mut got = self.length(i) - (start - self.start(i));
        while got < length {
            i += 1;
            slots.push(Interval {
                start: got,
                plist: copy_plist(self.plist(i))?,
            });
            got += self.length(i);
        }
        // make_interval for the first, split_interval_right for the rest.
        note_intervals(slots.len());
        Ok(Some(Self {
            items: Items::from_slots(slots),
            end: length,
        }))
    }

    /// The same intervals counted from 0, their plists shared: a tree
    /// read as a string's (a buffer's whole tree grafted as GNU grafts
    /// it, from its own intervals).
    pub(crate) fn shared_from_zero(&self) -> Self {
        let origin = self.origin();
        Self {
            items: self
                .items
                .iter()
                .map(|item| Interval {
                    start: item.start - origin,
                    plist: item.plist,
                })
                .collect(),
            end: self.end - origin,
        }
    }

    /// Each interval's start (and the end) moved by REMAP, which keeps
    /// their order; an interval left with no text goes (the rounding of
    /// set_intervals_multibyte).
    pub(crate) fn remap(&mut self, mut remap: impl FnMut(usize) -> usize) {
        for item in self.items.iter_mut_from(0) {
            item.start = remap(item.start);
        }
        self.end = remap(self.end);
        let origin = self.items[0].start;
        self.drop_empty();
        self.items[0].start = origin;
    }

    /// Each interval with no text dropped, in place: one whose start the
    /// next interval (or the end) shares.  The last one has text.
    fn drop_empty(&mut self) {
        let end = self.end;
        let items = self.items.make_contiguous();
        let mut kept = 0;
        for index in 0..items.len() {
            let next_start = items.get(index + 1).map_or(end, |next| next.start);
            if items[index].start < next_start {
                items[kept] = items[index];
                kept += 1;
            }
        }
        self.items.truncate(kept);
    }
}

/// intervals.c:intervals_equal_1 with EQ (USE_EQUAL None) or with the
/// given `equal': whether two intervals' plists have the same properties
/// with the same values, in any order; a nil plist only another's equal.
pub(crate) fn intervals_equal_with(
    i0: Value,
    i1: Value,
    mut use_equal: Option<&mut dyn FnMut(Value, Value) -> bool>,
) -> bool {
    if i0.is_nil() && i1.is_nil() {
        return true;
    }
    if i0.is_nil() || i1.is_nil() {
        return false;
    }
    let mut i0_cdr = i0;
    let mut i1_cdr = i1;
    while consp(i0_cdr) && consp(i1_cdr) {
        let i0_sym = xcar(i0_cdr);
        i0_cdr = xcdr(i0_cdr);
        if !consp(i0_cdr) {
            return false;
        }
        let mut i1_val = i1;
        while consp(i1_val) && !eq(xcar(i1_val), i0_sym) {
            i1_val = xcdr(i1_val);
            if !consp(i1_val) {
                return false;
            }
            i1_val = xcdr(i1_val);
        }
        // i0 has something i1 doesn't.
        if i1_val.is_nil() {
            return false;
        }
        // Both have the symbol, with different values.
        if !consp(i1_val) {
            return false;
        }
        i1_val = xcdr(i1_val);
        if !consp(i1_val) {
            return false;
        }
        let same = match use_equal.as_mut() {
            Some(equal) => equal(xcar(i1_val), xcar(i0_cdr)),
            None => eq(xcar(i1_val), xcar(i0_cdr)),
        };
        if !same {
            return false;
        }
        i0_cdr = xcdr(i0_cdr);
        i1_cdr = xcdr(i1_cdr);
        if !consp(i1_cdr) {
            return false;
        }
        i1_cdr = xcdr(i1_cdr);
    }
    // The plists were of equal length.
    i0_cdr.is_nil() && i1_cdr.is_nil()
}

/// intervals.c:intervals_equal.
pub(crate) fn intervals_equal(i0: Value, i1: Value) -> bool {
    intervals_equal_with(i0, i1, None)
}

/// intervals.c:merge_properties: each property of SOURCE that TARGET's
/// plist lacks consed onto it.
pub(crate) fn merge_properties(source: Value, target: Value) -> Result<Value, LispError> {
    if source.is_nil() && target.is_nil() {
        return Ok(Value::Nil);
    }
    let mut target = target;
    let mut o = source;
    while consp(o) {
        let sym = xcar(o);
        o = xcdr(o);
        if !consp(o) {
            return Err(LispError::WrongTypeArgument("consp".into(), o));
        }
        let mut val = target;
        while consp(val) && !eq(xcar(val), sym) {
            val = xcdr(val);
            if !consp(val) {
                break;
            }
            val = xcdr(val);
        }
        if val.is_nil() {
            target = Value::cons(sym, Value::cons(xcar(o), target));
        }
        o = xcdr(o);
    }
    Ok(target)
}

/// A lookup an insertion's offsets make, which do not signal: the text is
/// in by then, and GNU's signal there (Fassq's, of a variable that is
/// not a list) leaves its tree part way through the change.  Fassq fails
/// only having walked the list without finding the key, so nil is what
/// the walk found.
fn quiet(lookup: Result<Value, LispError>) -> Value {
    lookup.unwrap_or(Value::Nil)
}

/// intervals.c:merge_properties_sticky: the plist of text inserted
/// between an interval with plist PLEFT and one with PRIGHT, each
/// property taken from the side whose stickiness faces the insertion
/// (`rear-nonsticky' on the left, `front-sticky' on the right, the
/// defaults of `text-property-default-nonsticky'), the left side's
/// non-nil value where both do; with the stickiness it inherits.
pub(crate) fn merge_properties_sticky(
    host: &mut dyn PropertyHost,
    pleft: Value,
    pright: Value,
) -> Result<Value, LispError> {
    let front_sticky = symbol(FRONT_STICKY);
    let rear_nonsticky = symbol(REAR_NONSTICKY);
    let mut props: Vec<(Value, Value)> = Vec::new();
    let mut front: Vec<Value> = Vec::new();
    let mut rear: Vec<Value> = Vec::new();
    let lfront = quiet(host.textget(pleft, front_sticky));
    let lrear = quiet(host.textget(pleft, rear_nonsticky));
    let rfront = quiet(host.textget(pright, front_sticky));
    let rrear = quiet(host.textget(pright, rear_nonsticky));
    let defaults = host.default_nonsticky();

    // Go through each element of PRIGHT.
    let mut tail1 = pright;
    while consp(tail1) {
        let sym = xcar(tail1);
        // Sticky properties get special treatment.
        if !(eq(sym, rear_nonsticky) || eq(sym, front_sticky)) {
            let rval = fcar(xcdr(tail1))?;
            let mut tail2 = pleft;
            while consp(tail2) {
                if eq(sym, xcar(tail2)) {
                    break;
                }
                tail2 = plist_next(tail2)?;
            }
            // Whether the property is explicitly defined on the left.
            let lpresent = !tail2.is_nil();
            let lval = if tail2.is_nil() {
                Value::Nil
            } else {
                fcar(fcdr(tail2)?)?
            };
            let tmp = quiet(assq(sym, defaults));
            let mut use_left =
                lpresent && !(tmem(sym, lrear) || (consp(tmp) && !xcdr(tmp).is_nil()));
            let mut use_right = tmem(sym, rfront) || (consp(tmp) && xcdr(tmp).is_nil());
            if use_left && use_right {
                if lval.is_nil() {
                    use_left = false;
                } else if rval.is_nil() {
                    use_right = false;
                }
            }
            if use_left {
                props.push((sym, lval));
                if tmem(sym, lfront) {
                    front.push(sym);
                }
                if tmem(sym, lrear) {
                    rear.push(sym);
                }
            } else if use_right {
                props.push((sym, rval));
                if tmem(sym, rfront) {
                    front.push(sym);
                }
                if tmem(sym, rrear) {
                    rear.push(sym);
                }
            }
        }
        tail1 = plist_next(tail1)?;
    }

    // Now go through each element of PLEFT.
    let mut tail2 = pleft;
    while consp(tail2) {
        let sym = xcar(tail2);
        if !(eq(sym, rear_nonsticky) || eq(sym, front_sticky)) {
            // If SYM is in PRIGHT, it has been considered.
            let mut tail1 = pright;
            while consp(tail1) {
                if eq(sym, xcar(tail1)) {
                    break;
                }
                tail1 = plist_next(tail1)?;
            }
            if tail1.is_nil() {
                let lval = fcar(xcdr(tail2))?;
                let tmp = quiet(assq(sym, defaults));
                // RVAL is nil here, so the test simplifies.
                if !(tmem(sym, lrear) || (consp(tmp) && !xcdr(tmp).is_nil())) {
                    props.push((sym, lval));
                    if tmem(sym, lfront) {
                        front.push(sym);
                    }
                } else if tmem(sym, rfront) || (consp(tmp) && xcdr(tmp).is_nil()) {
                    // The value is nil, but the stickiness comes from the
                    // right.
                    front.push(sym);
                    if tmem(sym, rrear) {
                        rear.push(sym);
                    }
                }
            }
        }
        tail2 = plist_next(tail2)?;
    }

    let mut result = Value::list(props.iter().flat_map(|&(sym, value)| [sym, value]));
    if !rear.is_empty() {
        result = Value::cons(rear_nonsticky, Value::cons(Value::list(rear), result));
    }
    let category = quiet(host.textget(result, symbol(CATEGORY)));
    if !front.is_empty()
        // A front-sticky category property of t needs no detailed list.
        && !(!category.is_nil()
            && symbolp(category)
            && matches!(host.get(category, front_sticky).kind(), Kind::T))
    {
        result = Value::cons(front_sticky, Value::cons(Value::list(front), result));
    }
    Ok(result)
}

/// textprop.c:validate_plist: nil none, a list checked to be of even
/// length ("Odd length text property list"), any other object that
/// property with nil.
pub(crate) fn validate_plist(list: Value) -> Result<Value, LispError> {
    if list.is_nil() {
        return Ok(Value::Nil);
    }
    if consp(list) {
        let mut tail = list;
        let mut seen = crate::lisp::types::CycleGuard::new();
        while let Some(cell) = tail.cons_ref() {
            tail = cell.cdr.get();
            if !consp(tail) {
                return Err(LispError::Signal("Odd length text property list".into()));
            }
            if seen.step(crate::lisp::types::ConsCell::identity(&cell)) {
                return Err(LispError::SignalValue(Value::list([
                    Value::symbol("circular-list"),
                    list,
                ])));
            }
            tail = xcdr(tail);
            if !consp(tail) {
                break;
            }
        }
        return Ok(list);
    }
    Ok(Value::list([list, Value::Nil]))
}

/// textprop.c:interval_has_all_properties: whether the interval's plist
/// IPLIST has every property of PLIST with an EQ value.
pub(crate) fn interval_has_all_properties(plist: Value, iplist: Value) -> Result<bool, LispError> {
    let mut tail1 = plist;
    while consp(tail1) {
        let sym1 = xcar(tail1);
        let mut found = false;
        let mut tail2 = iplist;
        while consp(tail2) {
            if eq(sym1, xcar(tail2)) {
                // The same property on both lists: unequal values fail.
                if !eq(fcar(xcdr(tail1))?, fcar(xcdr(tail2))?) {
                    return Ok(false);
                }
                found = true;
                break;
            }
            tail2 = plist_next(tail2)?;
        }
        if !found {
            return Ok(false);
        }
        tail1 = plist_next(tail1)?;
    }
    Ok(true)
}

/// textprop.c:interval_has_some_properties: whether IPLIST has any of
/// the properties of PLIST, whatever their values.
pub(crate) fn interval_has_some_properties(plist: Value, iplist: Value) -> Result<bool, LispError> {
    let mut tail1 = plist;
    while consp(tail1) {
        let sym = xcar(tail1);
        let mut tail2 = iplist;
        while consp(tail2) {
            if eq(sym, xcar(tail2)) {
                return Ok(true);
            }
            tail2 = plist_next(tail2)?;
        }
        tail1 = plist_next(tail1)?;
    }
    Ok(false)
}

/// textprop.c:interval_has_some_properties_list: whether IPLIST has any
/// of the property names of LIST.
pub(crate) fn interval_has_some_properties_list(list: Value, iplist: Value) -> bool {
    let mut tail1 = list;
    while consp(tail1) {
        let sym = xcar(tail1);
        let mut tail2 = iplist;
        while consp(tail2) {
            if eq(sym, xcar(tail2)) {
                return true;
            }
            tail2 = xcdr(xcdr(tail2));
        }
        tail1 = xcdr(tail1);
    }
    false
}

/// textprop.c:property_value: PROP's value in PLIST, None for unbound.
fn property_value(plist: Value, prop: Value) -> Option<Value> {
    let mut plist = plist;
    while let Some(cell) = plist.cons_ref() {
        let value = cell.cdr.get();
        let Some(value_cell) = value.cons_ref() else {
            break;
        };
        if eq(cell.car.get(), prop) {
            return Some(value_cell.car.get());
        }
        plist = value_cell.cdr.get();
    }
    None
}

/// textprop.c:set_properties: interval I's plist becomes a copy of
/// PROPERTIES, each property it loses or changes, and each it gains,
/// recorded first.
pub(crate) fn set_properties(
    intervals: &mut Intervals,
    properties: Value,
    i: usize,
    recorder: &mut dyn PropertyRecorder,
) -> Result<(), LispError> {
    let (start, length, old) = (intervals.start(i), intervals.length(i), intervals.plist(i));
    // Each property of the old plist missing from PROPERTIES or with a
    // different value there.
    let mut sym = old;
    while let Some(cell) = sym.cons_ref() {
        let Some(value) = cell.cdr.get().cons_ref() else {
            break;
        };
        let old_value = value.car.get();
        if !property_value(properties, cell.car.get()).is_some_and(|new| eq(new, old_value)) {
            recorder.record(start, length, cell.car.get(), old_value);
        }
        sym = value.cdr.get();
    }
    // Each new property with no value at all in the old plist, bound to
    // nil so that undo removes it.
    let mut sym = properties;
    while let Some(cell) = sym.cons_ref() {
        let Some(value) = cell.cdr.get().cons_ref() else {
            break;
        };
        if property_value(old, cell.car.get()).is_none() {
            recorder.record(start, length, cell.car.get(), Value::Nil);
        }
        sym = value.cdr.get();
    }
    intervals.set_plist(i, copy_plist(properties)?);
    Ok(())
}

/// Fappend of LIST and (VALUE): a copy of LIST's elements with VALUE
/// after them.
fn append_one(list: Value, value: Value) -> Result<Value, LispError> {
    let mut items = Vec::new();
    let mut tail = list;
    let mut seen = crate::lisp::types::CycleGuard::new();
    while let Some(cell) = tail.cons_ref() {
        items.push(cell.car.get());
        if seen.step(crate::lisp::types::ConsCell::identity(&cell)) {
            return Err(LispError::SignalValue(Value::list([
                Value::symbol("circular-list"),
                list,
            ])));
        }
        tail = cell.cdr.get();
    }
    if !tail.is_nil() {
        return Err(LispError::WrongTypeArgument("listp".into(), tail));
    }
    items.push(value);
    Ok(Value::list(items))
}

/// nconc2 of LIST and (VALUE): VALUE's cons made the last cdr of LIST.
fn nconc_one(list: Value, value: Value) -> Result<(), LispError> {
    let mut tail = list;
    let mut seen = crate::lisp::types::CycleGuard::new();
    loop {
        let Some(cell) = tail.cons_ref() else {
            return Err(LispError::WrongTypeArgument("consp".into(), tail));
        };
        let next = cell.cdr.get();
        if !consp(next) {
            if !next.is_nil() {
                return Err(LispError::WrongTypeArgument("listp".into(), next));
            }
            cell.cdr.set(Value::cons(value, Value::Nil));
            return Ok(());
        }
        if seen.step(crate::lisp::types::ConsCell::identity(&cell)) {
            return Err(LispError::SignalValue(Value::list([
                Value::symbol("circular-list"),
                list,
            ])));
        }
        tail = next;
    }
}

/// EQ (SYM, Qface).
fn is_face(sym: Value) -> bool {
    matches!(sym.kind(), Kind::Symbol(name) if name.as_str() == "face")
}

/// Fkeywordp of a face list's car.
fn is_keyword(value: Value) -> bool {
    matches!(value.kind(), Kind::Symbol(name) if name.is_keyword())
}

/// textprop.c:add_properties: each property of PLIST given interval I,
/// replacing (or with SET_TYPE prepended or appended to) a different
/// value in place, by Fsetcar of the value's cell, or consed onto the
/// front of its plist; each change recorded first.  Whether one changed.
pub(crate) fn add_properties(
    intervals: &mut Intervals,
    plist: Value,
    i: usize,
    recorder: &mut dyn PropertyRecorder,
    set_type: PropertySetType,
    destructive: bool,
) -> Result<bool, LispError> {
    let (start, length) = (intervals.start(i), intervals.length(i));
    let mut changed = false;
    let mut tail1 = plist;
    while consp(tail1) {
        let mut found = false;
        let sym1 = xcar(tail1);
        let val1 = fcar(xcdr(tail1))?;
        // Look for SYM1 in I's plist.
        let mut tail2 = intervals.plist(i);
        while consp(tail2) {
            if eq(sym1, xcar(tail2)) {
                let this_cdr = xcdr(tail2);
                found = true;
                let old = fcar(this_cdr)?;
                // The same value on both lists.
                if eq(val1, old) {
                    break;
                }
                recorder.record(start, length, sym1, old);
                if set_type == PropertySetType::Replace {
                    this_cdr.set_car(val1)?;
                } else if consp(old) && (!is_face(sym1) || !is_keyword(xcar(old))) {
                    // The previous value is a list (not an anonymous
                    // face): the new value goes on it.
                    if set_type == PropertySetType::Prepend {
                        this_cdr.set_car(Value::cons(val1, old))?;
                    } else if destructive {
                        nconc_one(old, val1)?;
                    } else {
                        this_cdr.set_car(append_one(old, val1)?)?;
                    }
                } else if set_type == PropertySetType::Prepend {
                    // A single value becomes a list.
                    this_cdr.set_car(Value::list([val1, old]))?;
                } else {
                    this_cdr.set_car(Value::list([old, val1]))?;
                }
                changed = true;
                break;
            }
            tail2 = plist_next(tail2)?;
        }
        if !found {
            recorder.record(start, length, sym1, Value::Nil);
            let old = intervals.plist(i);
            intervals.set_plist(i, Value::cons(sym1, Value::cons(val1, old)));
            changed = true;
        }
        tail1 = plist_next(tail1)?;
    }
    Ok(changed)
}

/// textprop.c:remove_properties: the properties named by PLIST's
/// properties (or, with PLIST nil, LIST's elements) gone from interval
/// I's plist, the head by stepping past it and the rest by Fsetcdr in
/// place; each removal recorded first.  Whether one went.
pub(crate) fn remove_properties(
    intervals: &mut Intervals,
    plist: Value,
    list: Value,
    i: usize,
    recorder: &mut dyn PropertyRecorder,
) -> Result<bool, LispError> {
    let (start, length) = (intervals.start(i), intervals.length(i));
    let mut changed = false;
    let use_plist = !plist.is_nil();
    let mut tail1 = if use_plist { plist } else { list };
    let mut current_plist = intervals.plist(i);
    while consp(tail1) {
        let sym = xcar(tail1);
        // First, remove the symbol if it's at the head of the list.
        while consp(current_plist) && eq(sym, xcar(current_plist)) {
            recorder.record(start, length, sym, xcar(xcdr(current_plist)));
            current_plist = xcdr(xcdr(current_plist));
            changed = true;
        }
        // Go through I's plist, looking for SYM.
        let mut tail2 = current_plist;
        while !tail2.is_nil() {
            let this = xcdr(xcdr(tail2));
            if consp(this) && eq(sym, xcar(this)) {
                recorder.record(start, length, sym, xcar(xcdr(this)));
                xcdr(tail2).set_cdr(xcdr(xcdr(this)))?;
                changed = true;
            }
            tail2 = this;
        }
        // Advance through TAIL1 one way or the other.
        tail1 = xcdr(tail1);
        if use_plist && consp(tail1) {
            tail1 = xcdr(tail1);
        }
    }
    if changed {
        intervals.set_plist(i, current_plist);
    }
    Ok(changed)
}

/// Whether add_text_properties_1 of PROPERTIES (validated) over [S, S +
/// LEN) would change anything: some interval of the range lacks one of
/// them (its first check, before it splits or calls the hooks).
pub(crate) fn would_add_properties(
    intervals: &Intervals,
    s: usize,
    len: usize,
    properties: Value,
) -> Result<bool, LispError> {
    let mut i = intervals.find(s);
    let mut got = intervals.length(i) - (s - intervals.start(i));
    let mut len = len;
    while interval_has_all_properties(properties, intervals.plist(i))? {
        if got >= len {
            return Ok(false);
        }
        len -= got;
        i += 1;
        got = intervals.length(i);
    }
    Ok(true)
}

/// textprop.c:add_text_properties_1 from its interval analysis on:
/// PROPERTIES (validated, non-nil) given the text [S, S + LEN), the
/// intervals at either end split where the range does not start or end
/// at a boundary, and each interval between given them by
/// add_properties.  Whether anything changed (GNU's Qt).
pub(crate) fn add_text_properties_range(
    intervals: &mut Intervals,
    s: usize,
    len: usize,
    properties: Value,
    recorder: &mut dyn PropertyRecorder,
    set_type: PropertySetType,
    destructive: bool,
) -> Result<bool, LispError> {
    let mut i = intervals.find(s);
    let mut len = len;
    // If this interval already has the properties, skip it.
    if interval_has_all_properties(properties, intervals.plist(i))? {
        let mut got = intervals.length(i) - (s - intervals.start(i));
        loop {
            if got >= len {
                return Ok(false);
            }
            len -= got;
            i += 1;
            got = intervals.length(i);
            if !interval_has_all_properties(properties, intervals.plist(i))? {
                break;
            }
        }
    } else if intervals.start(i) != s {
        // Not starting on an interval boundary: split.
        let unchanged = i;
        i = intervals.split_right(unchanged, s - intervals.start(unchanged));
        intervals.copy_properties(intervals.plist(unchanged), i)?;
    }

    // At the beginning of interval I, with LEN characters to scan.
    let mut modified = false;
    loop {
        if intervals.length(i) >= len {
            if interval_has_all_properties(properties, intervals.plist(i))? {
                debug_assert!(modified);
                return Ok(true);
            }
            if intervals.length(i) == len {
                add_properties(intervals, properties, i, recorder, set_type, destructive)?;
                return Ok(true);
            }
            // I doesn't have the properties, and goes past the range.
            let unchanged = i;
            i = intervals.split_left(unchanged, len);
            intervals.copy_properties(intervals.plist(unchanged + 1), i)?;
            add_properties(intervals, properties, i, recorder, set_type, destructive)?;
            return Ok(true);
        }
        len -= intervals.length(i);
        modified |= add_properties(intervals, properties, i, recorder, set_type, destructive)?;
        i += 1;
    }
}

/// textprop.c:set_text_properties_1: PROPERTIES (validated) the whole
/// plist of [S, S + LEN), its intervals split at the ends and merged
/// into one.
pub(crate) fn set_text_properties_range(
    intervals: &mut Intervals,
    s: usize,
    len: usize,
    properties: Value,
    recorder: &mut dyn PropertyRecorder,
) -> Result<(), LispError> {
    if len == 0 {
        return Ok(());
    }
    let mut i = intervals.find(s);
    let mut len = len;
    let mut prev_changed: Option<usize> = None;
    if intervals.start(i) != s {
        let unchanged = i;
        i = intervals.split_right(unchanged, s - intervals.start(unchanged));
        if intervals.length(i) > len {
            intervals.copy_properties(intervals.plist(unchanged), i)?;
            i = intervals.split_left(i, len);
            set_properties(intervals, properties, i, recorder)?;
            return Ok(());
        }
        set_properties(intervals, properties, i, recorder)?;
        if intervals.length(i) == len {
            return Ok(());
        }
        prev_changed = Some(i);
        len -= intervals.length(i);
        i += 1;
    }

    // At the beginning of an interval I, LEN positive.
    loop {
        if intervals.length(i) >= len {
            if intervals.length(i) > len {
                i = intervals.split_left(i, len);
            }
            // set_properties even where the intervals then merge, for the
            // undo records.
            set_properties(intervals, properties, i, recorder)?;
            if prev_changed.is_some() {
                intervals.merge_left(i);
            }
            return Ok(());
        }
        len -= intervals.length(i);
        set_properties(intervals, properties, i, recorder)?;
        if prev_changed.is_none() {
            prev_changed = Some(i);
        } else {
            i = intervals.merge_left(i);
            prev_changed = Some(i);
        }
        i += 1;
    }
}

/// Whether removing the properties of PLIST (or LIST's names, PLIST nil)
/// from [S, S + LEN) would change anything: some interval there has one.
pub(crate) fn would_remove_properties(
    intervals: &Intervals,
    s: usize,
    len: usize,
    plist: Value,
    list: Value,
) -> Result<bool, LispError> {
    let has_some = |iplist: Value| -> Result<bool, LispError> {
        if plist.is_nil() {
            Ok(interval_has_some_properties_list(list, iplist))
        } else {
            interval_has_some_properties(plist, iplist)
        }
    };
    let mut i = intervals.find(s);
    let mut got = intervals.length(i) - (s - intervals.start(i));
    let mut len = len;
    while !has_some(intervals.plist(i))? {
        if got >= len {
            return Ok(false);
        }
        len -= got;
        i += 1;
        got = intervals.length(i);
    }
    Ok(true)
}

/// textprop.c:Fremove_text_properties (PLIST the properties whose names
/// go) and Fremove_list_of_text_properties (PLIST nil, LIST the names)
/// from their interval analysis on, over [S, S + LEN).  Whether one
/// went.
pub(crate) fn remove_text_properties_range(
    intervals: &mut Intervals,
    s: usize,
    len: usize,
    plist: Value,
    list: Value,
    recorder: &mut dyn PropertyRecorder,
) -> Result<bool, LispError> {
    let list_form = plist.is_nil();
    let has_some = |iplist: Value| -> Result<bool, LispError> {
        if list_form {
            Ok(interval_has_some_properties_list(list, iplist))
        } else {
            interval_has_some_properties(plist, iplist)
        }
    };
    let mut i = intervals.find(s);
    let mut len = len;
    // No properties on this interval: skip to one with them.
    if !has_some(intervals.plist(i))? {
        let mut got = intervals.length(i) - (s - intervals.start(i));
        loop {
            if got >= len {
                return Ok(false);
            }
            len -= got;
            i += 1;
            got = intervals.length(i);
            if has_some(intervals.plist(i))? {
                break;
            }
        }
    } else if intervals.start(i) != s {
        // Split away the beginning of this interval, not modified.
        let unchanged = i;
        i = intervals.split_right(unchanged, s - intervals.start(unchanged));
        intervals.copy_properties(intervals.plist(unchanged), i)?;
    }

    let mut modified = false;
    loop {
        if intervals.length(i) >= len {
            if !has_some(intervals.plist(i))? {
                return Ok(modified || !list_form);
            }
            if intervals.length(i) == len {
                remove_properties(intervals, plist, list, i, recorder)?;
                return Ok(true);
            }
            // I has the properties, and goes past the range.
            let unchanged = i;
            i = intervals.split_left(unchanged, len);
            intervals.copy_properties(intervals.plist(unchanged + 1), i)?;
            remove_properties(intervals, plist, list, i, recorder)?;
            return Ok(true);
        }
        len -= intervals.length(i);
        if list_form {
            if has_some(intervals.plist(i))? {
                remove_properties(intervals, plist, list, i, recorder)?;
                modified = true;
            }
        } else {
            modified |= remove_properties(intervals, plist, list, i, recorder)?;
        }
        match intervals.next(i) {
            Some(next) => i = next,
            None => return Ok(modified),
        }
    }
}

/// textprop.c:text_property_list over [S, E): a list of (START END
/// PLIST) for each interval there with properties (with PROP non-nil,
/// only that one), the last interval's first.
pub(crate) fn text_property_list(
    intervals: &Intervals,
    s: usize,
    e: usize,
    prop: Value,
) -> Result<Value, LispError> {
    let mut result = Value::Nil;
    let mut i = intervals.find(s);
    let mut s = s;
    while s < e {
        let interval_end = intervals.last_pos(i).min(e);
        let len = interval_end - s;
        let mut plist = intervals.plist(i);
        if !prop.is_nil() {
            while consp(plist) {
                if eq(xcar(plist), prop) {
                    plist = Value::list([prop, fcar(xcdr(plist))?]);
                    break;
                }
                plist = plist_next(plist)?;
            }
        }
        if !plist.is_nil() {
            result = Value::cons(
                Value::list([
                    Value::Integer(s as i64),
                    Value::Integer((s + len) as i64),
                    plist,
                ]),
                result,
            );
        }
        match intervals.next(i) {
            Some(next) => {
                i = next;
                s = intervals.start(i);
            }
            None => break,
        }
    }
    Ok(result)
}

/// textprop.c:copy_text_properties' walk of the source: for each
/// interval of [START, END) with a plist, (P, P + LEN, PLIST) with P its
/// position in the destination from POS, in the order the function then
/// adds them (the last interval's first).
pub(crate) fn copy_text_properties_list(
    intervals: &Intervals,
    start: usize,
    end: usize,
    pos: usize,
) -> Result<Vec<(usize, usize, Value)>, LispError> {
    let mut stuff = Vec::new();
    let mut i = intervals.find(start);
    let (mut s, mut p) = (start, pos);
    while s < end {
        let e2 = intervals.last_pos(i).min(end);
        let len = e2 - s;
        let plist = intervals.plist(i);
        if !plist.is_nil() {
            stuff.push((p, p + len, plist));
        }
        match intervals.next(i) {
            Some(next) => i = next,
            None => break,
        }
        p += len;
        s = intervals.start(i);
    }
    stuff.reverse();
    Ok(stuff)
}

/// Fadd_text_properties of PROPERTIES over [START, END) of a string of
/// LEN characters whose intervals PROPS holds (the range validated, the
/// string impure): nothing for no properties or an empty range, else a
/// root interval created where there is none and add_text_properties_1
/// over it.  Whether anything changed.
pub(crate) fn add_string_text_properties(
    props: &mut crate::lisp::types::string_data::StringProperties,
    len: usize,
    start: usize,
    end: usize,
    properties: Value,
    set_type: PropertySetType,
    destructive: bool,
) -> Result<bool, LispError> {
    let properties = validate_plist(properties)?;
    if properties.is_nil() || start == end || len == 0 {
        return Ok(false);
    }
    if !props.has_intervals() {
        props.set(Some(Intervals::root(0, len)));
    }
    let intervals = props.intervals_mut().expect("intervals just made");
    add_text_properties_range(
        intervals,
        start,
        end - start,
        properties,
        &mut NoRecord,
        set_type,
        destructive,
    )
}

/// textprop.c:set_text_properties of PROPERTIES over [START, END) of a
/// string of LEN characters (the range validated, the string impure):
/// no properties for the whole string drop its intervals; otherwise
/// intervals are made for properties to set and set_text_properties_1
/// sets them.  Whether it set anything (GNU's non-nil).
pub(crate) fn set_string_text_properties(
    props: &mut crate::lisp::types::string_data::StringProperties,
    len: usize,
    start: usize,
    end: usize,
    properties: Value,
) -> Result<bool, LispError> {
    let properties = validate_plist(properties)?;
    if properties.is_nil() && start == 0 && end == len {
        if !props.has_intervals() {
            return Ok(false);
        }
        props.set(None);
        return Ok(true);
    }
    if start == end || len == 0 {
        return Ok(false);
    }
    if !props.has_intervals() {
        if properties.is_nil() {
            return Ok(false);
        }
        props.set(Some(Intervals::root(0, len)));
    }
    let intervals = props.intervals_mut().expect("intervals just made");
    set_text_properties_range(intervals, start, end - start, properties, &mut NoRecord)?;
    Ok(true)
}

/// textprop.c:add_text_properties_from_list: each (START END PLIST) of
/// LIST added, moved by DELTA, to the string whose intervals PROPS holds.
pub(crate) fn add_string_text_properties_from_list(
    props: &mut crate::lisp::types::string_data::StringProperties,
    len: usize,
    list: Value,
    delta: usize,
) -> Result<(), LispError> {
    let mut tail = list;
    while let Some(cell) = tail.cons_ref() {
        let item = cell.car.get();
        let start = fixnum_of(fcar(item)?)? + delta as i64;
        let end = fixnum_of(fcar(fcdr(item)?)?)? + delta as i64;
        let plist = fcar(fcdr(fcdr(item)?)?)?;
        let (start, end) = (start.min(end), start.max(end));
        if start < 0 || end as usize > len {
            return Err(LispError::SignalValue(Value::list([
                Value::symbol("args-out-of-range"),
                Value::Integer(start),
                Value::Integer(end),
            ])));
        }
        add_string_text_properties(
            props,
            len,
            start as usize,
            end as usize,
            plist,
            PropertySetType::Replace,
            true,
        )?;
        tail = cell.cdr.get();
    }
    Ok(())
}

fn fixnum_of(value: Value) -> Result<i64, LispError> {
    match value.kind() {
        Kind::Integer(n) => Ok(n),
        _ => Err(LispError::WrongTypeArgument("fixnump".into(), value)),
    }
}

/// composite.c:make_composition_value_copy for one plist: a
/// `composition' value that is a cons replaced, in the plist itself, by
/// a copy of that cons.
pub(crate) fn copy_composition_value(plist: Value) {
    let mut plist = plist;
    while let Some(pair) = plist.cons_ref() {
        let Some(value_cell) = pair.cdr.get().cons_ref() else {
            break;
        };
        if matches!(pair.car.get().kind(), Kind::Symbol(name) if name.as_str() == "composition")
            && let Some(value) = value_cell.car.get().cons_ref()
        {
            value_cell
                .car
                .set(Value::cons(value.car.get(), value.cdr.get()));
        }
        plist = value_cell.cdr.get();
    }
}

/// intervals.c:compare_string_intervals: whether two strings of LEN
/// characters, with intervals S1 and S2 (None for none), have the same
/// properties at each position, values compared by EQUAL.
pub(crate) fn compare_string_intervals(
    s1: Option<&Intervals>,
    s2: Option<&Intervals>,
    len: usize,
    equal: &mut dyn FnMut(Value, Value) -> bool,
) -> bool {
    let mut pos = 0;
    let mut i1 = s1.map(|intervals| intervals.find(0));
    let mut i2 = s2.map(|intervals| intervals.find(0));
    while pos < len {
        let last1 = match (s1, i1) {
            (Some(intervals), Some(i)) => intervals.last_pos(i),
            _ => len,
        };
        let last2 = match (s2, i2) {
            (Some(intervals), Some(i)) => intervals.last_pos(i),
            _ => len,
        };
        let (len1, len2) = (last1 - pos, last2 - pos);
        let distance = len1.min(len2);
        let p1 = match (s1, i1) {
            (Some(intervals), Some(i)) => intervals.plist(i),
            _ => Value::Nil,
        };
        let p2 = match (s2, i2) {
            (Some(intervals), Some(i)) => intervals.plist(i),
            _ => Value::Nil,
        };
        if !intervals_equal_with(p1, p2, Some(equal)) {
            return false;
        }
        pos += distance;
        if len1 == distance {
            i1 = match (s1, i1) {
                (Some(intervals), Some(i)) => intervals.next(i),
                _ => None,
            };
        }
        if len2 == distance {
            i2 = match (s2, i2) {
                (Some(intervals), Some(i)) => intervals.next(i),
                _ => None,
            };
        }
    }
    true
}

/// intervals.c:graft_intervals_into_buffer from where it has a tree:
/// SOURCE's intervals (from 0, LENGTH long) copied over the text just
/// inserted at POSITION, the interval it falls in split there; INHERIT
/// merges each source plist into what adjust_for_insertion gave the
/// text (merge_properties), otherwise each is copied over it.
pub(crate) fn graft_into(
    tree: &mut Intervals,
    source: &Intervals,
    position: usize,
    inherit: bool,
) -> Result<(), LispError> {
    let mut under = tree.find(position);
    if position > tree.start(under) {
        let end_unchanged = tree.split_left(under, position - tree.start(under));
        tree.copy_properties(tree.plist(under + 1), end_unchanged)?;
        under += 1;
    }
    // Insertion is now at the beginning of UNDER.
    let mut over = Some(0usize);
    let mut over_used = 0usize;
    while let Some(o) = over {
        let over_len = source.length(o);
        // If UNDER is longer than OVER, split it.
        let this = if over_len - over_used < tree.length(under) {
            let this = tree.split_left(under, over_len - over_used);
            tree.copy_properties(tree.plist(this + 1), this)?;
            this
        } else {
            under
        };
        // THIS is the interval to copy or merge into; OVER covers it.
        if inherit {
            let merged = merge_properties(source.plist(o), tree.plist(this))?;
            tree.set_plist(this, merged);
        } else {
            tree.copy_properties(source.plist(o), this)?;
        }
        if tree.length(this) == over_len - over_used {
            over = source.next(o);
            over_used = 0;
        } else {
            over_used += tree.length(this);
        }
        // Always advance to a new target interval.
        match tree.next(this) {
            Some(next) => under = next,
            None => break,
        }
    }
    Ok(())
}

/// intervals.c:reproduce_tree for text that is the whole buffer: a copy
/// of SOURCE's intervals (from 0) moved to start at ORIGIN, the plists
/// shared as reproduce_interval shares them.
pub(crate) fn reproduce(source: &Intervals, origin: usize) -> Intervals {
    // reproduce_interval's make_interval for each.
    note_intervals(source.count());
    Intervals {
        items: source
            .items
            .iter()
            .map(|item| Interval {
                start: item.start + origin,
                plist: item.plist,
            })
            .collect(),
        end: source.end + origin,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plist(items: &[(&str, i64)]) -> Value {
        Value::list(
            items
                .iter()
                .flat_map(|&(name, value)| [Value::symbol(name), Value::Integer(value)]),
        )
    }

    fn shape(intervals: &Intervals) -> Vec<(usize, usize, String)> {
        intervals
            .iter()
            .map(|(start, end, plist)| (start, end, plist.to_string()))
            .collect()
    }

    /// The gap vector against a plain vector: inserts and removals at
    /// moving places, reads through the gap, the search, the walk from an
    /// index and the compaction.
    #[test]
    fn interval_gap_keeps_order_through_moves() {
        let item = |start: usize| Interval {
            start,
            plist: Value::Nil,
        };
        let mut gapped = Items::one(item(0));
        let mut plain = vec![item(0)];
        let mut seed = 12_345_u64;
        let mut next = |bound: usize| {
            seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            (seed >> 33) as usize % bound
        };
        for step in 0..2_000 {
            if plain.len() > 1 && next(3) == 0 {
                let at = next(plain.len());
                plain.remove(at);
                gapped.remove(at);
            } else {
                let at = next(plain.len() + 1);
                plain.insert(at, item(step));
                gapped.insert(at, item(step));
            }
            assert_eq!(gapped.len(), plain.len());
            let probe = next(plain.len());
            assert_eq!(gapped[probe].start, plain[probe].start);
        }
        let starts = |items: &Items| items.iter().map(|item| item.start).collect::<Vec<_>>();
        assert_eq!(
            starts(&gapped),
            plain.iter().map(|item| item.start).collect::<Vec<_>>()
        );
        let from = plain.len() / 3;
        for item in gapped.iter_mut_from(from) {
            item.start += 1;
        }
        for item in &mut plain[from..] {
            item.start += 1;
        }
        assert_eq!(
            starts(&gapped),
            plain.iter().map(|item| item.start).collect::<Vec<_>>()
        );
        // A sorted run, searched across the gap wherever it lies.
        let mut sorted = Items::from_iter((0..50).map(|n| item(n * 2)));
        sorted.insert(20, item(39));
        sorted.remove(20);
        for pos in 0..100 {
            assert_eq!(
                sorted.partition_point(|item| item.start <= pos),
                pos / 2 + 1
            );
        }
        sorted.truncate(10);
        assert_eq!(starts(&sorted), (0..10).map(|n| n * 2).collect::<Vec<_>>());
    }

    #[test]
    fn concat_of_many_intervals_splits_from_the_last_back() {
        // concat's intervals come from text_property_list's list, the last
        // first: each split is next to the one before it.
        let mut intervals = Intervals::root(0, 4_000);
        for piece in (0..2_000).rev() {
            add_text_properties_range(
                &mut intervals,
                piece * 2,
                1,
                plist(&[("k", piece as i64)]),
                &mut NoRecord,
                PropertySetType::Replace,
                true,
            )
            .expect("valid plists");
        }
        assert_eq!(intervals.count(), 4_000);
        assert_eq!(intervals.start(1_999), 1_999);
        assert_eq!(
            plist_get(intervals.plist(10), Value::symbol("k")),
            Value::Integer(5)
        );
        assert!(intervals.adjust_for_deletion(1, 3_998));
        assert_eq!(shape(&intervals).len(), 2);
    }

    #[test]
    fn adding_properties_splits_and_never_merges() {
        let mut intervals = Intervals::root(0, 4);
        let face = plist(&[("face", 1)]);
        assert!(
            add_text_properties_range(
                &mut intervals,
                0,
                2,
                face,
                &mut NoRecord,
                PropertySetType::Replace,
                true
            )
            .expect("valid plists")
        );
        assert!(
            add_text_properties_range(
                &mut intervals,
                2,
                2,
                face,
                &mut NoRecord,
                PropertySetType::Replace,
                true
            )
            .expect("valid plists")
        );
        assert_eq!(
            shape(&intervals),
            vec![
                (0, 2, "(face 1)".to_string()),
                (2, 4, "(face 1)".to_string())
            ]
        );
        // Already there: nothing changes.
        assert!(
            !add_text_properties_range(
                &mut intervals,
                1,
                3,
                face,
                &mut NoRecord,
                PropertySetType::Replace,
                true
            )
            .expect("valid plists")
        );
    }

    #[test]
    fn set_text_properties_merges_only_its_range() {
        let mut intervals = Intervals::root(0, 6);
        for start in 0..6 {
            add_text_properties_range(
                &mut intervals,
                start,
                1,
                plist(&[("k", start as i64)]),
                &mut NoRecord,
                PropertySetType::Replace,
                true,
            )
            .expect("valid plists");
        }
        set_text_properties_range(&mut intervals, 1, 3, plist(&[("a", 1)]), &mut NoRecord)
            .expect("valid plists");
        assert_eq!(
            shape(&intervals),
            vec![
                (0, 1, "(k 0)".to_string()),
                (1, 4, "(a 1)".to_string()),
                (4, 5, "(k 4)".to_string()),
                (5, 6, "(k 5)".to_string())
            ]
        );
    }

    #[test]
    fn removing_properties_leaves_empty_intervals() {
        let mut intervals = Intervals::root(1, 7);
        add_text_properties_range(
            &mut intervals,
            1,
            2,
            plist(&[("face", 1)]),
            &mut NoRecord,
            PropertySetType::Replace,
            true,
        )
        .expect("valid plists");
        add_text_properties_range(
            &mut intervals,
            3,
            2,
            plist(&[("face", 1)]),
            &mut NoRecord,
            PropertySetType::Replace,
            true,
        )
        .expect("valid plists");
        assert!(
            remove_text_properties_range(
                &mut intervals,
                1,
                6,
                plist(&[("face", 0)]),
                Value::Nil,
                &mut NoRecord
            )
            .expect("valid plists")
        );
        assert_eq!(
            shape(&intervals),
            vec![
                (1, 3, "nil".to_string()),
                (3, 5, "nil".to_string()),
                (5, 7, "nil".to_string())
            ]
        );
    }

    #[test]
    fn deletion_drops_emptied_intervals_without_merging() {
        let mut intervals = Intervals::from_items(
            vec![
                Interval {
                    start: 1,
                    plist: plist(&[("face", 1)]),
                },
                Interval {
                    start: 3,
                    plist: plist(&[("face", 1)]),
                },
                Interval {
                    start: 5,
                    plist: Value::Nil,
                },
            ],
            7,
        );
        assert!(intervals.adjust_for_deletion(2, 2));
        assert_eq!(
            shape(&intervals),
            vec![
                (1, 2, "(face 1)".to_string()),
                (2, 3, "(face 1)".to_string()),
                (3, 5, "nil".to_string())
            ]
        );
        assert!(intervals.adjust_for_deletion(1, 2));
        assert_eq!(shape(&intervals), vec![(1, 3, "nil".to_string())]);
        assert!(!intervals.adjust_for_deletion(1, 2));
    }

    #[test]
    fn copies_keep_every_interval() {
        let intervals = Intervals::from_items(
            vec![
                Interval {
                    start: 0,
                    plist: Value::Nil,
                },
                Interval {
                    start: 1,
                    plist: plist(&[("face", 1)]),
                },
                Interval {
                    start: 3,
                    plist: Value::Nil,
                },
            ],
            4,
        );
        let copy = intervals.copy(0, 4).expect("valid plists").expect("a copy");
        assert_eq!(shape(&copy), shape(&intervals));
        let part = intervals.copy(1, 2).expect("valid plists").expect("a copy");
        assert_eq!(shape(&part), vec![(0, 2, "(face 1)".to_string())]);
        // Well inside the first, empty interval: nothing.
        let long = Intervals::root(0, 10);
        assert!(long.copy(2, 3).expect("valid plists").is_none());
        // Reaching within one character of its end: a copy.
        assert!(long.copy(2, 7).expect("valid plists").is_some());
    }

    #[test]
    fn intervals_equal_ignores_order() {
        assert!(intervals_equal(
            plist(&[("a", 1), ("b", 2)]),
            plist(&[("b", 2), ("a", 1)])
        ));
        assert!(!intervals_equal(plist(&[("a", 1)]), plist(&[("a", 2)])));
        assert!(!intervals_equal(Value::Nil, plist(&[("a", 1)])));
        assert!(intervals_equal(Value::Nil, Value::Nil));
    }
}
