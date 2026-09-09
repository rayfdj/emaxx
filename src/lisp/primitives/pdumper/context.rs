//! pdumper.c's `dump_context': the output buffer, the object queue with
//! its link weights, the fixup and relocation lists, the object-start and
//! Emacs-relocation tables, and the per-object writers (D08).
//!
//! Each function keeps the name of the C function it follows.  The
//! object records are Emaxx's (a `ConsCell' is not a `struct Lisp_Cons'),
//! but every record is written through `dump_object_start' /
//! `dump_object_finish', every reference through `dump_field_lv', and
//! every table through the same drain.

use super::super::*;
use super::image::*;
use crate::lisp::types::{ConsCell, SymbolName};
use std::collections::{HashMap, VecDeque};
use std::rc::Rc;

/// pdumper.c:link_weight.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct LinkWeight(i32);
pub(crate) const WEIGHT_NONE: LinkWeight = LinkWeight(0);
pub(crate) const WEIGHT_NORMAL: LinkWeight = LinkWeight(1000);
pub(crate) const WEIGHT_STRONG: LinkWeight = LinkWeight(1200);

/// The identity `objects_dumped' (an `eq' hash table in GNU) is keyed by.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ObjectKey {
    Cons(usize),
    String(usize),
    StringObject(usize),
    Symbol(u32),
    Vector(usize),
    Float(usize),
    Bignum(usize),
    /// An integer outside the fixnum range has no Rust identity; equal
    /// values are one bignum record.
    WideInteger(i64),
    Subr(u32),
    Obarray,
    Lambda(usize),
    Buffer(usize),
    Marker(u64),
    Overlay(u64),
    CharTable(u64),
    Frame(u64),
    Terminal(u64),
    Record(u64),
    Finalizer(u64),
    ReaderForm(usize),
}

pub(crate) fn object_key(value: &Value) -> Option<ObjectKey> {
    Some(match value {
        Value::Cons(cell) => ObjectKey::Cons(ConsCell::identity(cell)),
        Value::String(text) => ObjectKey::String(text.identity_ptr()),
        Value::StringObject(state) => ObjectKey::StringObject(Rc::as_ptr(state) as usize),
        Value::Symbol(name) => {
            if name == "nil" || name == "t" {
                return None;
            }
            ObjectKey::Symbol(name.id())
        }
        Value::Vector(vector) => ObjectKey::Vector(Rc::as_ptr(vector) as usize),
        Value::Float(float) => ObjectKey::Float(float.identity_ptr()),
        Value::BigInteger(integer) => ObjectKey::Bignum(integer.identity_ptr()),
        Value::Integer(integer) => {
            if fixnum_word(*integer).is_some() {
                return None;
            }
            ObjectKey::WideInteger(*integer)
        }
        Value::BuiltinFunc(name) => ObjectKey::Subr(name.id()),
        Value::Record(_) if is_obarray_value(value) => ObjectKey::Obarray,
        Value::Nil | Value::T | Value::Unbound => return None,
        // dump_object_needs_dumping_p: everything but a fixnum is queued,
        // and dump_object refuses what it cannot write.
        Value::Lambda(lambda) => ObjectKey::Lambda(Rc::as_ptr(lambda) as usize),
        Value::Buffer(buffer) => ObjectKey::Buffer(Rc::as_ptr(buffer) as usize),
        Value::Marker(id) => ObjectKey::Marker(*id),
        Value::Overlay(id) => ObjectKey::Overlay(*id),
        Value::CharTable(id) => ObjectKey::CharTable(*id),
        Value::Frame(id) => ObjectKey::Frame(*id),
        Value::Terminal(id) => ObjectKey::Terminal(*id),
        Value::Record(id) => ObjectKey::Record(*id),
        Value::Finalizer(id) => ObjectKey::Finalizer(*id),
        Value::ReaderForm(form) => ObjectKey::ReaderForm(Rc::as_ptr(form) as usize),
    })
}

/// dump_object_self_representing_p: fixnums and the built-in symbols
/// Emaxx represents specially.
pub(crate) fn self_representing_word(value: &Value) -> Option<u64> {
    match value {
        Value::Nil => Some(WORD_NIL),
        Value::T => Some(WORD_T),
        Value::Unbound => Some(WORD_UNBOUND),
        Value::Symbol(name) if name == "nil" => Some(WORD_NIL),
        Value::Symbol(name) if name == "t" => Some(WORD_T),
        Value::Integer(integer) => fixnum_word(*integer),
        _ => None,
    }
}

/// dump_object_emacs_ptr: objects that live in the Emacs image rather
/// than the Lisp heap.  A built-in function is one; a symbol is not (all
/// of Emaxx's symbols are heap objects addressed by name).
fn object_in_emacs_image(value: &Value) -> bool {
    matches!(value, Value::BuiltinFunc(_))
}

/// pdumper.c's DUMP_OBJECT_* states for an object `objects_dumped' knows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ObjectState {
    OnNormalQueue,
    OnColdQueue,
    OnCopiedQueue,
    Dumped(u32),
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct DumpFlags {
    pub(crate) dump_object_contents: bool,
    pub(crate) record_object_starts: bool,
    pub(crate) defer_cold_objects: bool,
    pub(crate) defer_copied_objects: bool,
    pub(crate) assert_already_seen: bool,
    pub(crate) pack_objects: bool,
}

enum Fixup {
    LispObject { offset: u32, value: Value },
    PtrDumpRaw { offset: u32, target: u32 },
    BignumData { offset: u32, value: Value },
}

impl Fixup {
    fn offset(&self) -> u32 {
        match self {
            Fixup::LispObject { offset, .. }
            | Fixup::PtrDumpRaw { offset, .. }
            | Fixup::BignumData { offset, .. } => *offset,
        }
    }
}

enum ColdOp {
    Object(Value),
    String(Value),
    Bignum(Value),
}

enum EmacsRelocPayload {
    Immediate(u64),
    Object(Value),
}

struct EmacsReloc {
    slot: RootSlot,
    payload: EmacsRelocPayload,
}

/// pdumper.c:dump_queue: four tail queues, link weights and sequence
/// numbers.
#[derive(Default)]
struct DumpQueue {
    zero_weight_objects: VecDeque<Value>,
    one_weight_normal_objects: VecDeque<Value>,
    one_weight_strong_objects: VecDeque<Value>,
    fancy_weight_objects: VecDeque<Value>,
    /// `t' in GNU (a zero-weight object) is `None' here; otherwise the
    /// (basis, weight) pairs, newest first.
    link_weights: HashMap<ObjectKey, Option<Vec<(u32, i32)>>>,
    sequence_numbers: HashMap<ObjectKey, u32>,
    next_sequence_number: u32,
}

impl DumpQueue {
    fn new() -> Self {
        Self {
            next_sequence_number: 1,
            ..Self::default()
        }
    }

    fn is_empty(&self) -> bool {
        self.sequence_numbers.is_empty()
    }

    /// dump_queue_enqueue.
    fn enqueue(&mut self, key: ObjectKey, object: Value, basis: u32, weight: LinkWeight) {
        match self.link_weights.get_mut(&key) {
            None => {
                // Object is new.
                let weights = if weight == WEIGHT_NONE {
                    self.zero_weight_objects.push_front(object);
                    None
                } else if weight == WEIGHT_NORMAL {
                    self.one_weight_normal_objects.push_front(object);
                    Some(vec![(basis, weight.0)])
                } else {
                    self.one_weight_strong_objects.push_front(object);
                    Some(vec![(basis, weight.0)])
                };
                self.link_weights.insert(key, weights);
                self.sequence_numbers.insert(key, self.next_sequence_number);
                self.next_sequence_number += 1;
            }
            Some(weights) => {
                // Object was already on the queue.
                if weight != WEIGHT_NONE {
                    match weights {
                        None => {
                            // Previously zero weight: now a single weight,
                            // on the matching single-weight queue.
                            if weight == WEIGHT_NORMAL {
                                self.one_weight_normal_objects.push_front(object);
                            } else {
                                self.one_weight_strong_objects.push_front(object);
                            }
                            *weights = Some(vec![(basis, weight.0)]);
                        }
                        Some(list) => {
                            if list.len() == 1 {
                                self.fancy_weight_objects.push_front(object);
                            }
                            list.insert(0, (basis, weight.0));
                        }
                    }
                }
            }
        }
    }

    /// dump_calc_link_score.
    fn link_score(basis: u32, link_basis: u32, link_weight: i32) -> f32 {
        let distance = (basis - link_basis) as f32;
        let link_score = distance.powf(-0.2);
        link_score.powf(link_weight as f32 / 1000.0)
    }

    /// dump_queue_compute_score.
    fn compute_score(&self, key: ObjectKey, basis: u32) -> f32 {
        let mut score = 0.0;
        if let Some(Some(list)) = self.link_weights.get(&key) {
            for &(link_basis, link_weight) in list {
                score += Self::link_score(basis, link_basis, link_weight);
            }
        }
        score
    }

    fn sequence(&self, key: ObjectKey) -> i64 {
        self.sequence_numbers
            .get(&key)
            .map_or(-1, |sequence| i64::from(*sequence))
    }

    /// dump_queue_scan_fancy: the index of the best-scoring live entry.
    fn scan_fancy(&mut self, basis: u32) -> Option<(usize, f32)> {
        loop {
            let mut best: Option<(usize, f32)> = None;
            for (index, object) in self.fancy_weight_objects.iter().enumerate() {
                let key = object_key(object).expect("queued objects have identity");
                let score = self.compute_score(key, basis);
                if best.is_none_or(|(_, highest)| score >= highest) {
                    best = Some((index, score));
                }
            }
            let (index, score) = best?;
            let key = object_key(&self.fancy_weight_objects[index]).expect("identity");
            if self.sequence(key) < 0 {
                // Discard the stale object and scan again.
                self.fancy_weight_objects.remove(index);
                continue;
            }
            return Some((index, score));
        }
    }

    /// dump_queue_find_score_of_one_weight_queue: discards stale heads.
    fn score_of_one_weight_queue(
        queue: &mut VecDeque<Value>,
        sequences: &HashMap<ObjectKey, u32>,
        score_of: impl Fn(ObjectKey) -> f32,
    ) -> Option<(f32, i64)> {
        loop {
            let head = queue.front()?;
            let key = object_key(head).expect("queued objects have identity");
            match sequences.get(&key) {
                None => {
                    queue.pop_front();
                }
                Some(sequence) => return Some((score_of(key), i64::from(*sequence))),
            }
        }
    }

    /// dump_queue_dequeue.
    fn dequeue(&mut self, basis: u32) -> Value {
        let fancy = self.scan_fancy(basis).map(|(index, score)| {
            let key = object_key(&self.fancy_weight_objects[index]).expect("identity");
            (index, score, self.sequence(key))
        });
        let link_weights = &self.link_weights;
        let score_of = |key: ObjectKey| {
            let mut score = 0.0;
            if let Some(Some(list)) = link_weights.get(&key) {
                for &(link_basis, link_weight) in list {
                    score += Self::link_score(basis, link_basis, link_weight);
                }
            }
            score
        };
        let normal = Self::score_of_one_weight_queue(
            &mut self.one_weight_normal_objects,
            &self.sequence_numbers,
            score_of,
        );
        let strong = Self::score_of_one_weight_queue(
            &mut self.one_weight_strong_objects,
            &self.sequence_numbers,
            score_of,
        );
        let candidates = [
            fancy.map(|(_, score, sequence)| (score, sequence)),
            normal,
            strong,
        ];
        let mut best: Option<usize> = None;
        for (index, candidate) in candidates.iter().enumerate() {
            let Some((score, sequence)) = candidate else {
                continue;
            };
            let better = match best {
                None => true,
                Some(current) => {
                    let (best_score, best_sequence) = candidates[current].expect("chosen");
                    *score > best_score || (*score == best_score && *sequence < best_sequence)
                }
            };
            if better {
                best = Some(index);
            }
        }
        let result = match best {
            None => self
                .zero_weight_objects
                .pop_front()
                .expect("the dump queue is not empty"),
            Some(0) => {
                let (index, _, _) = fancy.expect("fancy candidate");
                self.fancy_weight_objects
                    .remove(index)
                    .expect("fancy entry")
            }
            Some(1) => self
                .one_weight_normal_objects
                .pop_front()
                .expect("normal head"),
            Some(_) => self
                .one_weight_strong_objects
                .pop_front()
                .expect("strong head"),
        };
        let key = object_key(&result).expect("identity");
        self.link_weights.remove(&key);
        self.sequence_numbers.remove(&key);
        result
    }
}

/// An unsupported object, reported as pdumper.c's
/// error_unsupported_dump_object does after printing the referrer paths.
pub(crate) struct UnsupportedObject {
    pub(crate) object: Value,
    pub(crate) message: String,
}

pub(crate) enum DumpError {
    Unsupported(UnsupportedObject),
    Lisp(LispError),
}

impl From<LispError> for DumpError {
    fn from(error: LispError) -> Self {
        DumpError::Lisp(error)
    }
}

pub(crate) struct DumpContext {
    pub(crate) header: DumpHeader,
    buf: Vec<u8>,
    offset: u32,
    max_offset: u32,
    objects_dumped: HashMap<ObjectKey, ObjectState>,
    dump_queue: DumpQueue,
    fixups: Vec<Fixup>,
    copied_queue: Vec<Value>,
    cold_queue: Vec<ColdOp>,
    dump_relocs: [Vec<(u32, DumpRelocKind)>; RELOC_NUM_PHASES],
    object_starts: Vec<(u32, DumpType)>,
    emacs_relocs: Vec<EmacsReloc>,
    bignum_data: HashMap<ObjectKey, (u32, i64)>,
    pub(crate) flags: DumpFlags,
    /// The object being dumped (dump_object_start .. dump_object_finish).
    obj_offset: u32,
    end_heap: u32,
    pub(crate) number_hot_relocations: u32,
    pub(crate) number_discardable_relocations: u32,
    referrers: Option<HashMap<ObjectKey, Vec<Value>>>,
    current_referrer: Option<Value>,
}

impl DumpContext {
    pub(crate) fn new(track_referrers: bool) -> Self {
        Self {
            header: DumpHeader::incomplete(),
            buf: Vec::new(),
            offset: 0,
            max_offset: 0,
            objects_dumped: HashMap::new(),
            dump_queue: DumpQueue::new(),
            fixups: Vec::new(),
            copied_queue: Vec::new(),
            cold_queue: Vec::new(),
            dump_relocs: [Vec::new(), Vec::new(), Vec::new()],
            object_starts: Vec::new(),
            emacs_relocs: Vec::new(),
            bignum_data: HashMap::new(),
            // Fdump_emacs_portable's initial flags.
            flags: DumpFlags {
                dump_object_contents: true,
                record_object_starts: true,
                defer_cold_objects: true,
                defer_copied_objects: true,
                assert_already_seen: false,
                pack_objects: false,
            },
            obj_offset: 0,
            end_heap: 0,
            number_hot_relocations: 0,
            number_discardable_relocations: 0,
            referrers: track_referrers.then(HashMap::new),
            current_referrer: None,
        }
    }

    pub(crate) fn offset(&self) -> u32 {
        self.offset
    }

    pub(crate) fn buffer(&self) -> &[u8] {
        &self.buf[..self.max_offset as usize]
    }

    // ----- Buffer primitives -----

    /// dump_write.
    pub(crate) fn write(&mut self, bytes: &[u8]) -> Result<(), DumpError> {
        assert_eq!(self.obj_offset, 0, "dump_write during an object");
        assert!(self.flags.dump_object_contents);
        let end = self.offset as usize + bytes.len();
        if end > DUMP_OFF_MAX {
            return Err(LispError::Signal("dump file too large".into()).into());
        }
        if self.buf.len() < end {
            self.buf.resize(end, 0);
        }
        self.buf[self.offset as usize..end].copy_from_slice(bytes);
        self.offset = end as u32;
        self.max_offset = self.max_offset.max(self.offset);
        Ok(())
    }

    fn write_word(&mut self, word: u64) -> Result<(), DumpError> {
        self.write(&word.to_le_bytes())
    }

    /// dump_seek.
    fn seek(&mut self, offset: u32) {
        assert_eq!(self.obj_offset, 0, "dump_seek during an object");
        self.offset = offset;
    }

    /// dump_write_zero.
    fn write_zero(&mut self, nbytes: usize) -> Result<(), DumpError> {
        let zeros = vec![0_u8; nbytes];
        self.write(&zeros)
    }

    /// dump_align_output.
    pub(crate) fn align_output(&mut self, alignment: usize) -> Result<(), DumpError> {
        let rem = self.offset as usize % alignment;
        if rem != 0 {
            self.write_zero(alignment - rem)?;
        }
        Ok(())
    }

    /// dump_object_start: one object at a time, aligned unless packing.
    fn object_start(&mut self) -> Result<u32, DumpError> {
        assert_eq!(self.obj_offset, 0, "nested dump_object_start");
        let alignment = if self.flags.pack_objects {
            1
        } else {
            DUMP_ALIGNMENT
        };
        if self.flags.dump_object_contents {
            self.align_output(alignment)?;
        }
        self.obj_offset = self.offset;
        Ok(self.offset)
    }

    /// dump_object_finish: write the record words.
    fn object_finish(&mut self, words: &[u64]) -> Result<u32, DumpError> {
        let offset = self.obj_offset;
        assert!(offset > 0);
        assert_eq!(offset, self.offset, "no intervening writes");
        self.obj_offset = 0;
        if self.flags.dump_object_contents {
            let mut bytes = Vec::with_capacity(words.len() * 8);
            for word in words {
                bytes.extend_from_slice(&word.to_le_bytes());
            }
            self.write(&bytes)?;
        }
        Ok(offset)
    }

    // ----- Object bookkeeping -----

    /// dump_recall_object.
    pub(crate) fn recall_object(&self, value: &Value) -> Option<ObjectState> {
        object_key(value).and_then(|key| self.objects_dumped.get(&key).copied())
    }

    fn remember_object(&mut self, value: &Value, state: ObjectState) {
        let key = object_key(value).expect("remembered objects have identity");
        self.objects_dumped.insert(key, state);
    }

    fn tracking_referrers(&self) -> bool {
        self.referrers.is_some()
    }

    fn set_referrer(&mut self, referrer: Value) {
        if self.tracking_referrers() {
            self.current_referrer = Some(referrer);
        }
    }

    fn clear_referrer(&mut self) {
        self.current_referrer = None;
    }

    /// dump_note_reachable.
    fn note_reachable(&mut self, value: &Value) {
        let Some(referrers) = self.referrers.as_mut() else {
            return;
        };
        let Some(referrer) = self.current_referrer.clone() else {
            return;
        };
        let Some(key) = object_key(value) else {
            return;
        };
        let list = referrers.entry(key).or_default();
        if !list.iter().any(|known| values_eq(known, &referrer)) {
            list.push(referrer);
        }
    }

    /// print_paths_to_root, through the interpreter's printer.
    pub(crate) fn print_paths_to_root(
        &self,
        interp: &mut Interpreter,
        env: &mut crate::lisp::types::Env,
        object: &Value,
    ) {
        fn walk(
            ctx: &DumpContext,
            interp: &mut Interpreter,
            env: &mut crate::lisp::types::Env,
            object: &Value,
            level: usize,
        ) {
            let Some(referrers) = ctx.referrers.as_ref() else {
                return;
            };
            let Some(list) = object_key(object).and_then(|key| referrers.get(&key)) else {
                return;
            };
            for referrer in list {
                let printed = crate::lisp::native_comp::call_c_primitive(
                    interp,
                    env,
                    "prin1-to-string",
                    std::slice::from_ref(referrer),
                )
                .ok()
                .and_then(|value| string_like(&value).map(|string| string.text))
                .unwrap_or_default();
                eprintln!("{}{printed}", " ".repeat(level));
                walk(ctx, interp, env, referrer, level + 1);
            }
        }
        walk(self, interp, env, object, 0);
    }

    fn unsupported(&self, object: &Value, message: &str) -> DumpError {
        DumpError::Unsupported(UnsupportedObject {
            object: object.clone(),
            message: message.to_string(),
        })
    }

    /// dump_remember_cold_op.
    fn remember_cold_op(&mut self, op: ColdOp) {
        if self.flags.dump_object_contents {
            self.cold_queue.push(op);
        }
    }

    /// dump_reloc_dump_to_dump_ptr_raw / dump_reloc_dump_to_dump_lv /
    /// dump_reloc_dump_to_emacs_lv (early phase).
    fn push_dump_reloc(&mut self, offset: u32, kind: DumpRelocKind) {
        if self.flags.dump_object_contents {
            self.dump_relocs[EARLY_RELOCS].push((offset, kind));
        }
    }

    /// dump_remember_fixup_lv.
    fn remember_fixup_lv(&mut self, offset: u32, value: Value) {
        if self.flags.dump_object_contents {
            self.fixups.push(Fixup::LispObject { offset, value });
        }
    }

    /// dump_remember_fixup_ptr_raw.
    fn remember_fixup_ptr_raw(&mut self, offset: u32, target: u32) {
        if !self.flags.dump_object_contents {
            return;
        }
        // No relocations into the to-be-copied region.
        assert!(
            self.header.discardable_start == 0
                || target < self.header.discardable_start
                || (self.header.cold_start != 0 && target >= self.header.cold_start)
        );
        self.fixups.push(Fixup::PtrDumpRaw { offset, target });
    }

    /// dump_enqueue_object.
    fn enqueue_object(&mut self, value: &Value, weight: LinkWeight) {
        if let Some(key) = object_key(value) {
            let state = self.objects_dumped.get(&key).copied();
            let already_dumped = matches!(state, Some(ObjectState::Dumped(_)));
            if self.flags.assert_already_seen {
                assert!(already_dumped, "object enqueued after the scan");
            }
            if !already_dumped {
                let state = match state {
                    None => {
                        self.objects_dumped.insert(key, ObjectState::OnNormalQueue);
                        ObjectState::OnNormalQueue
                    }
                    Some(state) => state,
                };
                // Multiple enqueue calls can increase the object's weight.
                if state == ObjectState::OnNormalQueue {
                    self.dump_queue
                        .enqueue(key, value.clone(), self.offset, weight);
                }
            }
        }
        // Always remember the path to this object.
        self.note_reachable(value);
    }

    /// dump_emacs_reloc_to_lv: a root slot of the process gets VALUE.
    fn emacs_reloc_to_lv(&mut self, slot: RootSlot, value: &Value) {
        if let Some(word) = self_representing_word(value) {
            if self.flags.dump_object_contents {
                self.emacs_relocs.push(EmacsReloc {
                    slot,
                    payload: EmacsRelocPayload::Immediate(word),
                });
            }
            return;
        }
        if self.flags.dump_object_contents {
            self.emacs_relocs.push(EmacsReloc {
                slot,
                payload: EmacsRelocPayload::Object(value.clone()),
            });
        }
        self.enqueue_object(value, WEIGHT_NONE);
    }

    /// dump_field_lv: word INDEX of the record starting at START names
    /// VALUE.  Self-representing values are written directly; anything
    /// else gets a fixup resolved after the target is dumped.
    fn field_lv(
        &mut self,
        start: u32,
        words: &mut [u64],
        index: usize,
        value: &Value,
        weight: LinkWeight,
    ) {
        assert!(self.obj_offset > 0);
        if let Some(word) = self_representing_word(value) {
            words[index] = word;
            return;
        }
        words[index] = FIXUP_PLACEHOLDER;
        let field_offset = start + (index * 8) as u32;
        self.remember_fixup_lv(field_offset, value.clone());
        self.enqueue_object(value, weight);
    }

    // ----- Roots -----

    /// dump_roots: the interpreter's root slots and the obarray.
    pub(crate) fn dump_roots(&mut self, interp: &Interpreter) -> Result<(), DumpError> {
        self.set_referrer(Value::string("emacs root"));
        for (slot, value) in interp.dump_root_values() {
            self.emacs_reloc_to_lv(slot, &value);
        }
        self.set_referrer(Value::string("built-in symbol list"));
        self.emacs_reloc_to_lv(RootSlot::Obarray, &obarray_value());
        self.clear_referrer();
        Ok(())
    }

    /// dump_roots for an explicit root list (the round-trip controls).
    #[cfg(test)]
    pub(crate) fn dump_explicit_roots(&mut self, roots: &[(RootSlot, Value)]) {
        self.set_referrer(Value::string("emacs root"));
        for (slot, value) in roots {
            self.emacs_reloc_to_lv(*slot, value);
        }
        self.clear_referrer();
    }

    // ----- dump_object and the per-type writers -----

    /// dump_object.
    pub(crate) fn dump_object(
        &mut self,
        interp: &Interpreter,
        object: &Value,
    ) -> Result<ObjectState, DumpError> {
        if let Some(state @ ObjectState::Dumped(_)) = self.recall_object(object) {
            return Ok(state);
        }
        let state = self.recall_object(object);

        let cold = matches!(object, Value::Float(_));
        if cold && self.flags.defer_cold_objects {
            if state != Some(ObjectState::OnColdQueue) {
                assert!(matches!(state, None | Some(ObjectState::OnNormalQueue)));
                self.remember_object(object, ObjectState::OnColdQueue);
                self.remember_cold_op(ColdOp::Object(object.clone()));
            }
            return Ok(ObjectState::OnColdQueue);
        }

        if object_in_emacs_image(object) && self.flags.defer_copied_objects {
            if state != Some(ObjectState::OnCopiedQueue) {
                assert!(matches!(state, None | Some(ObjectState::OnNormalQueue)));
                // Scan and enqueue the referents now, dump the object later.
                let old_flags = self.flags;
                self.flags.dump_object_contents = false;
                self.flags.defer_copied_objects = false;
                self.dump_object(interp, object)?;
                self.flags = old_flags;
                self.remember_object(object, ObjectState::OnCopiedQueue);
                self.copied_queue.push(object.clone());
            }
            return Ok(ObjectState::OnCopiedQueue);
        }

        // Object needs to be dumped.
        self.set_referrer(object.clone());
        let (offset, kind) = match object {
            Value::String(_) | Value::StringObject(_) => self.dump_string(interp, object)?,
            Value::Vector(vector) => (self.dump_vector(vector)?, DumpType::Vector),
            Value::Symbol(_) => (self.dump_symbol(interp, object)?, DumpType::Symbol),
            Value::Cons(cell) => (self.dump_cons(cell)?, DumpType::Cons),
            Value::Float(float) => (self.dump_float(**float)?, DumpType::Float),
            Value::BigInteger(_) | Value::Integer(_) => {
                (self.dump_bignum(object)?, DumpType::Bignum)
            }
            Value::BuiltinFunc(name) => (self.dump_subr(name)?, DumpType::Subr),
            Value::Record(id) if is_obarray_value(object) => {
                let _ = id;
                (self.dump_obarray(interp)?, DumpType::Obarray)
            }
            Value::Nil | Value::T | Value::Unbound => {
                unreachable!("self-representing objects are never dumped")
            }
            Value::Lambda(_) => return Err(self.unsupported(object, "closure")),
            Value::Buffer(_) => return Err(self.unsupported(object, "buffer")),
            Value::Marker(_) => return Err(self.unsupported(object, "marker")),
            Value::Overlay(_) => return Err(self.unsupported(object, "overlay")),
            Value::CharTable(_) => return Err(self.unsupported(object, "char-table")),
            Value::Frame(_) => return Err(self.unsupported(object, "frame")),
            Value::Terminal(_) => return Err(self.unsupported(object, "terminal")),
            Value::Record(_) => return Err(self.unsupported(object, "record")),
            Value::Finalizer(_) => return Err(self.unsupported(object, "finalizer")),
            Value::ReaderForm(_) => return Err(self.unsupported(object, "reader form")),
        };
        self.clear_referrer();

        if self.flags.dump_object_contents {
            assert_eq!(offset as usize % DUMP_ALIGNMENT, 0);
            self.remember_object(object, ObjectState::Dumped(offset));
            if self.flags.record_object_starts {
                assert!(!self.flags.pack_objects);
                self.object_starts.push((offset, kind));
            }
        }
        Ok(ObjectState::Dumped(offset))
    }

    /// dump_cons: car (strong) and cdr (normal).
    fn dump_cons(&mut self, cell: &crate::lisp::types::SharedCons) -> Result<u32, DumpError> {
        let start = self.object_start()?;
        let mut words = [0_u64; 2];
        let car = Value::Cons(cell.clone()).car()?;
        let cdr = Value::Cons(cell.clone()).cdr()?;
        self.field_lv(start, &mut words, 0, &car, WEIGHT_STRONG);
        self.field_lv(start, &mut words, 1, &cdr, WEIGHT_NORMAL);
        self.object_finish(&words)
    }

    /// dump_string: size, size_byte, intervals, data.  The text goes to
    /// the cold section; the property spans follow the string, as GNU
    /// writes intervals after the string.
    fn dump_string(
        &mut self,
        interp: &Interpreter,
        object: &Value,
    ) -> Result<(u32, DumpType), DumpError> {
        let string = string_like(object).expect("a string");
        let kind = match object {
            Value::String(_) => DumpType::String,
            _ => DumpType::StringObject,
        };
        let size = string.text.chars().count() as u64;
        let size_byte = if string.multibyte {
            crate::lisp::primitives::strings::lisp_string_byte_len(
                &string.text,
                true,
                &string.extended_chars,
            )? as u64
        } else {
            // -1: a unibyte string.
            u64::MAX
        };
        self.object_start()?;
        let mut words = [size, size_byte, 0, 0];
        let has_props = !string.props.is_empty();
        if has_props {
            words[2] = FIXUP_PLACEHOLDER;
        }
        words[3] = FIXUP_PLACEHOLDER;
        self.remember_cold_op(ColdOp::String(object.clone()));
        let offset = self.object_finish(&words)?;
        if has_props {
            let properties = self.dump_text_properties(interp, &string.props)?;
            self.remember_fixup_ptr_raw(offset + 16, properties);
        }
        Ok((offset, kind))
    }

    /// The string's property spans: count, then (start, end, nprops,
    /// (name, value)*) per span, names as symbol words.
    fn dump_text_properties(
        &mut self,
        _interp: &Interpreter,
        props: &[TextPropertySpan],
    ) -> Result<u32, DumpError> {
        let start = self.object_start()?;
        let mut words = vec![props.len() as u64];
        let mut fields: Vec<(usize, Value, LinkWeight)> = Vec::new();
        for span in props {
            words.push(span.start as u64);
            words.push(span.end as u64);
            words.push(span.props.len() as u64);
            for (name, value) in &span.props {
                fields.push((words.len(), Value::symbol(name), WEIGHT_STRONG));
                words.push(0);
                fields.push((words.len(), value.clone(), WEIGHT_STRONG));
                words.push(0);
            }
        }
        for (index, value, weight) in fields {
            self.field_lv(start, &mut words, index, &value, weight);
        }
        let offset = self.object_finish(&words)?;
        if self.flags.dump_object_contents && self.flags.record_object_starts {
            self.object_starts.push((offset, DumpType::TextProperties));
        }
        Ok(offset)
    }

    /// dump_symbol: the flags, name, value cell, function, plist and the
    /// watcher list.
    fn dump_symbol(&mut self, interp: &Interpreter, object: &Value) -> Result<u32, DumpError> {
        let Value::Symbol(symbol) = object else {
            unreachable!()
        };
        let cell = interp.dump_symbol_cell(symbol);
        let function = interp
            .raw_function_binding(symbol.as_str(), &crate::lisp::types::Env::new())
            .unwrap_or(Value::Nil);
        let plist = interp.symbol_plist(symbol.as_str());
        let watchers = interp.variable_watchers(symbol.as_str());
        let start = self.object_start()?;
        let mut words = vec![
            symbol_flags_word(symbol, &cell, !watchers.is_empty()),
            0,
            0,
            0,
            0,
            0,
        ];
        self.field_lv(start, &mut words, 1, &symbol.lisp_name(), WEIGHT_STRONG);
        let val = match &cell.alias {
            Some(target) => Value::Symbol(target.clone()),
            None => cell.value.clone().unwrap_or(Value::Unbound),
        };
        self.field_lv(start, &mut words, 2, &val, WEIGHT_NORMAL);
        self.field_lv(start, &mut words, 3, &function, WEIGHT_NORMAL);
        self.field_lv(start, &mut words, 4, &plist, WEIGHT_NORMAL);
        // `next' (the obarray bucket chain) is rebuilt by interning.
        words[5] = watchers.len() as u64;
        for watcher in &watchers {
            let index = words.len();
            words.push(0);
            self.field_lv(start, &mut words, index, watcher, WEIGHT_NORMAL);
        }
        self.object_finish(&words)
    }

    /// dump_vectorlike_generic for an ordinary vector: size, then slots.
    fn dump_vector(
        &mut self,
        vector: &Rc<crate::lisp::types::VectorValue>,
    ) -> Result<u32, DumpError> {
        let slots = vector.slots().clone();
        let start = self.object_start()?;
        let mut words = vec![slots.len() as u64];
        words.resize(slots.len() + 1, 0);
        for (index, slot) in slots.iter().enumerate() {
            self.field_lv(start, &mut words, index + 1, slot, WEIGHT_STRONG);
        }
        self.object_finish(&words)
    }

    /// dump_float: the IEEE word, in the cold section.
    fn dump_float(&mut self, value: f64) -> Result<u32, DumpError> {
        assert!(self.header.cold_start != 0);
        self.object_start()?;
        self.object_finish(&[value.to_bits()])
    }

    /// dump_bignum: the record holds the sign and limb count and, through
    /// a fixup, the offset of its limbs in the cold section.
    fn dump_bignum(&mut self, object: &Value) -> Result<u32, DumpError> {
        let start = self.object_start()?;
        let words = [0, FIXUP_PLACEHOLDER];
        let offset = self.object_finish(&words)?;
        if self.flags.dump_object_contents {
            self.remember_cold_op(ColdOp::Bignum(object.clone()));
            self.fixups.push(Fixup::BignumData {
                offset: start + 8,
                value: object.clone(),
            });
            self.push_dump_reloc(offset, DumpRelocKind::Bignum);
        }
        Ok(offset)
    }

    /// dump_subr: a built-in function's copied record names it.
    fn dump_subr(&mut self, name: &SymbolName) -> Result<u32, DumpError> {
        let start = self.object_start()?;
        let mut words = [0_u64];
        self.field_lv(start, &mut words, 0, &name.lisp_name(), WEIGHT_STRONG);
        self.object_finish(&words)
    }

    /// dump_obarray: the interned symbols, in the order Emaxx keeps them.
    fn dump_obarray(&mut self, interp: &Interpreter) -> Result<u32, DumpError> {
        let names = interp.known_symbol_names();
        let start = self.object_start()?;
        let mut words = vec![names.len() as u64];
        words.resize(names.len() + 1, 0);
        for (index, name) in names.iter().enumerate() {
            let symbol = Value::Symbol(SymbolName::intern_str(name));
            self.field_lv(start, &mut words, index + 1, &symbol, WEIGHT_STRONG);
        }
        self.object_finish(&words)
    }

    // ----- Queues -----

    /// dump_drain_normal_queue.
    pub(crate) fn drain_normal_queue(&mut self, interp: &Interpreter) -> Result<(), DumpError> {
        while !self.dump_queue.is_empty() {
            let object = self.dump_queue.dequeue(self.offset);
            self.dump_object(interp, &object)?;
        }
        Ok(())
    }

    pub(crate) fn queue_is_empty(&self) -> bool {
        self.dump_queue.is_empty()
    }

    /// dump_sort_copied_objects: by the address in the Emacs image; here
    /// by the built-in's name, the identity a subr has.
    pub(crate) fn sort_copied_objects(&mut self) {
        self.copied_queue.sort_by(|a, b| {
            let name = |value: &Value| match value {
                Value::BuiltinFunc(name) => name.as_str().to_owned(),
                _ => String::new(),
            };
            name(a).cmp(&name(b))
        });
    }

    /// dump_drain_copied_objects: the discardable section.
    pub(crate) fn drain_copied_objects(&mut self, interp: &Interpreter) -> Result<(), DumpError> {
        let copied_queue = std::mem::take(&mut self.copied_queue);
        let old_flags = self.flags;
        self.flags.assert_already_seen = true;
        self.flags.defer_copied_objects = false;
        self.flags.record_object_starts = false;
        for copied in copied_queue {
            self.dump_object(interp, &copied)?;
        }
        self.flags = old_flags;
        Ok(())
    }

    /// dump_drain_cold_data.
    pub(crate) fn drain_cold_data(&mut self, interp: &Interpreter) -> Result<(), DumpError> {
        let cold_queue = std::mem::take(&mut self.cold_queue);
        let old_flags = self.flags;
        self.flags.assert_already_seen = true;
        self.flags.defer_cold_objects = false;
        for op in cold_queue {
            match op {
                ColdOp::String(string) => self.dump_cold_string(&string)?,
                ColdOp::Object(object) => {
                    assert!(self.dump_queue.is_empty());
                    assert!(self.flags.dump_object_contents);
                    self.dump_object(interp, &object)?;
                    assert!(self.dump_queue.is_empty());
                }
                ColdOp::Bignum(object) => self.dump_cold_bignum(&object)?,
            }
        }
        self.flags = old_flags;
        Ok(())
    }

    /// dump_cold_string: the bytes in GNU's internal representation plus
    /// the terminating NUL, and the fixup that points the record at them.
    fn dump_cold_string(&mut self, object: &Value) -> Result<(), DumpError> {
        let Some(ObjectState::Dumped(string_offset)) = self.recall_object(object) else {
            panic!("cold string was dumped");
        };
        let bytes = internal_string_bytes(object)?;
        if bytes.len() > DUMP_OFF_MAX - 1 {
            return Err(LispError::Signal("string too large".into()).into());
        }
        self.remember_fixup_ptr_raw(string_offset + 24, self.offset);
        self.write(&bytes)?;
        self.write(&[0])
    }

    /// dump_cold_bignum: the limbs, least significant first.
    fn dump_cold_bignum(&mut self, object: &Value) -> Result<(), DumpError> {
        let (negative, limbs) = bignum_limbs(object);
        self.align_output(8)?;
        let descriptor = (
            self.offset,
            if negative {
                -(limbs.len() as i64)
            } else {
                limbs.len() as i64
            },
        );
        self.bignum_data
            .insert(object_key(object).expect("bignum identity"), descriptor);
        for limb in limbs {
            self.write_word(limb)?;
        }
        Ok(())
    }

    // ----- Fixups and tables -----

    pub(crate) fn set_end_heap(&mut self) {
        self.end_heap = self.offset;
    }

    /// dump_do_fixups: in offset order.
    pub(crate) fn do_fixups(&mut self) -> Result<(), DumpError> {
        let saved_offset = self.offset;
        let mut fixups = std::mem::take(&mut self.fixups);
        fixups.sort_by_key(Fixup::offset);
        let mut previous_offset = None;
        for fixup in fixups {
            let offset = fixup.offset();
            assert_ne!(previous_offset, Some(offset), "two fixups at one offset");
            previous_offset = Some(offset);
            assert!(offset > 0 && offset < self.end_heap);
            self.seek(offset);
            match fixup {
                Fixup::LispObject { value, .. } => {
                    let Some(ObjectState::Dumped(target)) = self.recall_object(&value) else {
                        panic!("fixup target was dumped");
                    };
                    let kind = object_dump_type(&value);
                    let reloc = if object_in_emacs_image(&value) {
                        DumpRelocKind::DumpToEmacsLv(kind)
                    } else {
                        DumpRelocKind::DumpToDumpLv(kind)
                    };
                    self.write_word(u64::from(target))?;
                    self.push_dump_reloc(offset, reloc);
                }
                Fixup::PtrDumpRaw { target, .. } => {
                    self.write_word(u64::from(target))?;
                    self.push_dump_reloc(offset, DumpRelocKind::DumpToDumpPtrRaw);
                }
                Fixup::BignumData { value, .. } => {
                    let (data_offset, sign_limbs) = self
                        .bignum_data
                        .get(&object_key(&value).expect("bignum identity"))
                        .copied()
                        .expect("bignum data was written");
                    // The record's two words: sign/limb count, data offset.
                    self.seek(offset - 8);
                    self.write_word(sign_limbs as u64)?;
                    self.write_word(u64::from(data_offset))?;
                }
            }
        }
        self.seek(saved_offset);
        Ok(())
    }

    /// drain_reloc_list for the dump relocations of PHASE.
    pub(crate) fn emit_dump_relocs(&mut self, phase: usize) -> Result<(), DumpError> {
        let mut relocs = std::mem::take(&mut self.dump_relocs[phase]);
        relocs.sort_by_key(|(offset, _)| *offset);
        self.align_output(8)?;
        let locator_offset = self.offset;
        for (offset, kind) in &relocs {
            assert!(*offset > 0 && *offset < self.end_heap);
            self.write(&offset.to_le_bytes())?;
            self.write(&kind.to_u32().to_le_bytes())?;
            if *offset < self.header.discardable_start {
                self.number_hot_relocations += 1;
            } else {
                self.number_discardable_relocations += 1;
            }
        }
        self.header.dump_relocs[phase] = TableLocator {
            offset: locator_offset,
            nr_entries: relocs.len() as u32,
        };
        Ok(())
    }

    /// drain_reloc_list for the object starts.
    pub(crate) fn emit_object_starts(&mut self) -> Result<(), DumpError> {
        let mut starts = std::mem::take(&mut self.object_starts);
        starts.sort_by_key(|(offset, _)| *offset);
        self.align_output(8)?;
        let locator_offset = self.offset;
        for (offset, kind) in &starts {
            self.write(&offset.to_le_bytes())?;
            self.write(&(*kind as u32).to_le_bytes())?;
        }
        self.header.object_starts = TableLocator {
            offset: locator_offset,
            nr_entries: starts.len() as u32,
        };
        Ok(())
    }

    /// drain_reloc_list for the Emacs relocations, by root slot.
    pub(crate) fn emit_emacs_relocs(&mut self) -> Result<(), DumpError> {
        let mut relocs = std::mem::take(&mut self.emacs_relocs);
        relocs.sort_by_key(|reloc| reloc.slot as u32);
        self.align_output(8)?;
        let locator_offset = self.offset;
        for reloc in &relocs {
            let (kind, payload) = match &reloc.payload {
                EmacsRelocPayload::Immediate(word) => (EmacsRelocKind::Immediate, *word),
                EmacsRelocPayload::Object(value) => {
                    let Some(ObjectState::Dumped(target)) = self.recall_object(value) else {
                        panic!("root object was dumped");
                    };
                    let kind = object_dump_type(value);
                    let kind = if object_in_emacs_image(value) {
                        EmacsRelocKind::EmacsLv(kind)
                    } else {
                        EmacsRelocKind::DumpLv(kind)
                    };
                    (kind, u64::from(target))
                }
            };
            self.write(&kind.to_u32().to_le_bytes())?;
            self.write(&(reloc.slot as u32).to_le_bytes())?;
            self.write_word(payload)?;
        }
        self.header.emacs_relocs = TableLocator {
            offset: locator_offset,
            nr_entries: relocs.len() as u32,
        };
        Ok(())
    }

    /// The end-of-dump assertions Fdump_emacs_portable makes.
    pub(crate) fn assert_drained(&self) {
        assert!(self.dump_queue.is_empty());
        assert!(self.copied_queue.is_empty());
        assert!(self.cold_queue.is_empty());
        assert!(self.fixups.is_empty());
        assert!(self.dump_relocs.iter().all(Vec::is_empty));
        assert!(self.emacs_relocs.is_empty());
    }

    /// Write the header at offset 0 (the incomplete one first, the
    /// complete one at the end).
    pub(crate) fn write_header(&mut self) -> Result<(), DumpError> {
        let bytes = self.header.to_bytes();
        self.write(&bytes)
    }

    pub(crate) fn seek_to_start(&mut self) {
        self.seek(0);
    }

    pub(crate) fn mark_complete(&mut self) {
        self.header.magic = DUMP_MAGIC;
    }
}

/// The dump type an object's record carries.
pub(crate) fn object_dump_type(value: &Value) -> DumpType {
    match value {
        Value::Cons(_) => DumpType::Cons,
        Value::String(_) => DumpType::String,
        Value::StringObject(_) => DumpType::StringObject,
        Value::Symbol(_) => DumpType::Symbol,
        Value::Vector(_) => DumpType::Vector,
        Value::Float(_) => DumpType::Float,
        Value::BigInteger(_) | Value::Integer(_) => DumpType::Bignum,
        Value::BuiltinFunc(_) => DumpType::Subr,
        Value::Record(_) if is_obarray_value(value) => DumpType::Obarray,
        _ => panic!("no dump type for {value:?}"),
    }
}

/// The initial obarray as a root object.  Emaxx keeps the standard
/// obarray's symbols in interpreter tables; the image gives it one
/// record, reached through a record value that stands for it.
const OBARRAY_ROOT_RECORD_ID: u64 = u64::MAX;

pub(crate) fn obarray_value() -> Value {
    Value::Record(OBARRAY_ROOT_RECORD_ID)
}

fn is_obarray_value(value: &Value) -> bool {
    matches!(value, Value::Record(id) if *id == OBARRAY_ROOT_RECORD_ID)
}

fn values_eq(a: &Value, b: &Value) -> bool {
    match (object_key(a), object_key(b)) {
        (Some(a), Some(b)) => a == b,
        _ => matches!((a, b), (Value::String(x), Value::String(y)) if x == y),
    }
}

/// The string's bytes as GNU stores them: the internal multibyte form for
/// a multibyte string, the raw octets for a unibyte one.
pub(crate) fn internal_string_bytes(object: &Value) -> Result<Vec<u8>, DumpError> {
    let string = string_like(object).expect("a string");
    let mut bytes = Vec::new();
    if string.multibyte {
        for code in string.character_codes() {
            crate::lisp::primitives::strings::push_emacs_multibyte_char(&mut bytes, code as u32)?;
        }
    } else {
        for code in string.character_codes() {
            if !(0..=0xFF).contains(&code) {
                // GNU cannot hold this state: a unibyte string's bytes are
                // its characters.  A string built this way is an Emaxx
                // representation error at its construction site.
                return Err(LispError::Signal(format!(
                    "unibyte string holds character {code:#x}, not a byte: {:?}",
                    string.text
                ))
                .into());
            }
            bytes.push(code as u8);
        }
    }
    Ok(bytes)
}

/// mpz_export: sign and little-endian 64-bit limbs.
pub(crate) fn bignum_limbs(object: &Value) -> (bool, Vec<u64>) {
    match object {
        Value::BigInteger(integer) => {
            let (sign, limbs) = integer.to_u64_digits();
            (sign == num_bigint::Sign::Minus, limbs)
        }
        Value::Integer(integer) => {
            let magnitude = integer.unsigned_abs();
            (*integer < 0, vec![magnitude])
        }
        _ => panic!("not a bignum"),
    }
}

/// The first word of a symbol record: the `Lisp_Symbol' header bits
/// (redirect, trapped_write, interned, declared_special) and the Emaxx
/// cell flags that have no GNU header bit.
pub(crate) fn symbol_flags_word(
    symbol: &SymbolName,
    cell: &crate::lisp::eval::SymbolCellSnapshot,
    trapped_write: bool,
) -> u64 {
    use crate::lisp::eval::symbol_cell_flags as flags;
    let redirect: u64 = if cell.alias.is_some() {
        SYMBOL_VARALIAS
    } else if cell.flags & flags::LOCALIZED != 0 {
        SYMBOL_LOCALIZED
    } else if cell.flags & flags::FORWARDED != 0 {
        SYMBOL_FORWARDED
    } else {
        SYMBOL_PLAINVAL
    };
    let interned: u64 = if symbol.id() & crate::lisp::types::UNINTERNED_SYMBOL_ID_BIT != 0 {
        SYMBOL_UNINTERNED
    } else {
        SYMBOL_INTERNED_IN_INITIAL_OBARRAY
    };
    let mut word = redirect | (interned << 4);
    if cell.flags & flags::SPECIAL != 0 {
        word |= FLAG_DECLARED_SPECIAL;
    }
    if trapped_write {
        word |= FLAG_TRAPPED_WRITE;
    }
    for (cell_flag, bit) in [
        (flags::LOCAL_IF_SET, FLAG_LOCAL_IF_SET),
        (flags::PER_BUFFER, FLAG_PER_BUFFER),
        (flags::ALWAYS_LOCAL, FLAG_ALWAYS_LOCAL),
        (flags::FWD_BOOL, FLAG_FWD_BOOL),
        (flags::FWD_INT, FLAG_FWD_INT),
    ] {
        if cell.flags & cell_flag != 0 {
            word |= bit;
        }
    }
    word
}

// lisp.h:symbol_redirect.
pub(crate) const SYMBOL_PLAINVAL: u64 = 0;
pub(crate) const SYMBOL_VARALIAS: u64 = 1;
pub(crate) const SYMBOL_LOCALIZED: u64 = 2;
pub(crate) const SYMBOL_FORWARDED: u64 = 3;
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const SYMBOL_REDIRECT_MASK: u64 = 7;
// lisp.h:symbol_interned, in bits 4-5.
pub(crate) const SYMBOL_UNINTERNED: u64 = 0;
pub(crate) const SYMBOL_INTERNED_IN_INITIAL_OBARRAY: u64 = 2;
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) const SYMBOL_INTERNED_SHIFT: u32 = 4;
pub(crate) const FLAG_DECLARED_SPECIAL: u64 = 1 << 3;
pub(crate) const FLAG_TRAPPED_WRITE: u64 = 1 << 6;
pub(crate) const FLAG_LOCAL_IF_SET: u64 = 1 << 8;
pub(crate) const FLAG_PER_BUFFER: u64 = 1 << 9;
pub(crate) const FLAG_ALWAYS_LOCAL: u64 = 1 << 10;
pub(crate) const FLAG_FWD_BOOL: u64 = 1 << 11;
pub(crate) const FLAG_FWD_INT: u64 = 1 << 12;
