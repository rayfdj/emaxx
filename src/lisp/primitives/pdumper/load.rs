//! pdumper.c:pdumper_load's validation and object reconstruction (the
//! part the round-trip controls need; the process-level restore is
//! D12/D13).
//!
//! The loader validates the file exactly as `pdumper_load' does (size,
//! magic, the incomplete marker, the fingerprint), then rebuilds one Rust
//! object per object-start entry and applies the relocation tables to
//! its fields, so sharing and cycles come back as they were written.
//! Records and char-tables are installed in the interpreter with the ids
//! the image gave them: the id is the identity every `Value::Record' and
//! `Value::CharTable' carries.

use super::super::*;
use super::context::*;
use super::image::*;
use crate::lisp::eval::RecordKind;
use crate::lisp::types::{EnvFrame, LambdaValue, SharedEnv, SharedText, SymbolName};
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

/// pdumper.c:pdumper_load_result.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum LoadError {
    /// PDUMPER_LOAD_FILE_NOT_FOUND belongs to the process-level loader.
    BadFileType,
    /// The incomplete marker is still set.
    FailedDump,
    /// The fingerprint is another binary's.
    VersionMismatch,
    Error(String),
}

/// One symbol record, as the image describes the symbol.
#[derive(Clone, Debug)]
pub(crate) struct LoadedSymbol {
    pub(crate) symbol: SymbolName,
    pub(crate) flags: u64,
    /// `None' is the unbound marker.
    pub(crate) value: Option<Value>,
    pub(crate) alias: Option<SymbolName>,
    pub(crate) function: Value,
    pub(crate) plist: Value,
    pub(crate) watchers: Vec<Value>,
}

pub(crate) struct LoadedImage {
    pub(crate) header: DumpHeader,
    pub(crate) roots: Vec<(RootSlot, Value)>,
    pub(crate) symbols: Vec<LoadedSymbol>,
    /// The initial obarray record's symbols in image order.
    pub(crate) obarray: Vec<SymbolName>,
    /// The cells of `nil' and `t'.
    pub(crate) builtin_cells: Vec<LoadedSymbol>,
}

struct Reader<'a> {
    bytes: &'a [u8],
}

impl Reader<'_> {
    fn word(&self, offset: u32) -> Result<u64, LoadError> {
        let start = offset as usize;
        self.bytes
            .get(start..start + 8)
            .map(|slice| u64::from_le_bytes(slice.try_into().expect("eight bytes")))
            .ok_or_else(|| LoadError::Error(format!("word at {offset} is outside the image")))
    }

    fn u32(&self, offset: u32) -> Result<u32, LoadError> {
        let start = offset as usize;
        self.bytes
            .get(start..start + 4)
            .map(|slice| u32::from_le_bytes(slice.try_into().expect("four bytes")))
            .ok_or_else(|| {
                LoadError::Error(format!("table entry at {offset} is outside the image"))
            })
    }
}

/// The validation pdumper_load performs before the point of no return.
pub(crate) fn validate_header(bytes: &[u8]) -> Result<DumpHeader, LoadError> {
    let Some(mut header) = DumpHeader::from_bytes(bytes) else {
        return Err(LoadError::BadFileType);
    };
    if header.magic != DUMP_MAGIC {
        if header.magic[0] == INCOMPLETE_MAGIC_BYTE && {
            header.magic[0] = DUMP_MAGIC[0];
            header.magic == DUMP_MAGIC
        } {
            return Err(LoadError::FailedDump);
        }
        return Err(LoadError::BadFileType);
    }
    let desired = executable_fingerprint();
    if header.fingerprint != *desired {
        eprintln!("desired fingerprint: {}", hex(desired));
        eprintln!("found fingerprint: {}", hex(&header.fingerprint));
        return Err(LoadError::VersionMismatch);
    }
    Ok(header)
}

pub(crate) fn load_image(bytes: &[u8], interp: &mut Interpreter) -> Result<LoadedImage, LoadError> {
    let header = validate_header(bytes)?;
    let mut loader = Loader {
        reader: Reader { bytes },
        relocs: HashMap::new(),
        types: HashMap::new(),
        objects: HashMap::new(),
        params: HashMap::new(),
        bodies: HashMap::new(),
        envs: HashMap::new(),
        envs_filled: HashSet::new(),
        frames: HashMap::new(),
        closures_in_progress: HashSet::new(),
        interp,
    };
    loader.load(header)
}

struct Loader<'a> {
    reader: Reader<'a>,
    relocs: HashMap<u32, DumpRelocKind>,
    /// Object type by start offset.
    types: HashMap<u32, DumpType>,
    objects: HashMap<u32, Value>,
    params: HashMap<u32, Rc<Vec<SymbolName>>>,
    bodies: HashMap<u32, Rc<Vec<Value>>>,
    envs: HashMap<u32, SharedEnv>,
    envs_filled: HashSet<u32>,
    frames: HashMap<u32, EnvFrame>,
    closures_in_progress: HashSet<u32>,
    interp: &'a mut Interpreter,
}

impl Loader<'_> {
    fn load(&mut self, header: DumpHeader) -> Result<LoadedImage, LoadError> {
        // The tables.
        let mut object_starts = Vec::new();
        for index in 0..header.object_starts.nr_entries {
            let at = header.object_starts.offset + index * TABLE_ENTRY_LEN as u32;
            let offset = self.reader.u32(at)?;
            let kind = DumpType::from_u32(self.reader.u32(at + 4)?)
                .ok_or_else(|| LoadError::Error(format!("unknown object type at {at}")))?;
            object_starts.push((offset, kind));
            self.types.insert(offset, kind);
        }
        for phase in 0..RELOC_NUM_PHASES {
            let locator = header.dump_relocs[phase];
            for index in 0..locator.nr_entries {
                let at = locator.offset + index * TABLE_ENTRY_LEN as u32;
                let offset = self.reader.u32(at)?;
                let kind = DumpRelocKind::from_u32(self.reader.u32(at + 4)?)
                    .ok_or_else(|| LoadError::Error(format!("unknown relocation at {at}")))?;
                self.relocs.insert(offset, kind);
            }
        }

        // Phase 1: objects that need nothing but their own bytes.
        let mut string_props: Vec<(u32, u32)> = Vec::new();
        for &(offset, kind) in &object_starts {
            match kind {
                DumpType::String | DumpType::StringObject => {
                    let (value, props) = self.load_string(offset, kind)?;
                    if let Some(props) = props {
                        string_props.push((offset, props));
                    }
                    self.objects.insert(offset, value);
                }
                DumpType::Float => {
                    let value = Value::float(f64::from_bits(self.reader.word(offset)?));
                    self.objects.insert(offset, value);
                }
                DumpType::Bignum => {
                    let value = self.load_bignum(offset)?;
                    self.objects.insert(offset, value);
                }
                DumpType::BoolVector => {
                    let value = self.load_bool_vector(offset)?;
                    self.objects.insert(offset, value);
                }
                _ => {}
            }
        }

        // Phase 2: objects whose construction needs a name, and the
        // containers as placeholders.
        let mut symbol_records = Vec::new();
        let mut obarray_records = Vec::new();
        let mut hash_table_records = Vec::new();
        let mut char_tables = Vec::new();
        let mut buffers = Vec::new();
        let mut markers = Vec::new();
        let mut overlays = Vec::new();
        let mut finalizers = Vec::new();
        for &(offset, kind) in &object_starts {
            match kind {
                DumpType::Symbol => {
                    let flags = self.reader.word(offset)?;
                    let name = self.value_at(offset + 8)?;
                    let name_text = string_like(&name)
                        .map(|string| string.text)
                        .ok_or_else(|| LoadError::Error("symbol name is not a string".into()))?;
                    let interned = (flags >> SYMBOL_INTERNED_SHIFT) & 3;
                    let symbol = if interned == SYMBOL_UNINTERNED {
                        SymbolName::make_uninterned(name, &name_text, next_make_symbol_id())
                    } else {
                        SymbolName::intern_str(&name_text)
                    };
                    self.objects.insert(offset, Value::Symbol(symbol.clone()));
                    symbol_records.push((offset, symbol, flags));
                }
                DumpType::Cons => {
                    self.objects
                        .insert(offset, Value::cons(Value::Nil, Value::Nil));
                }
                DumpType::Vector => {
                    let size = self.reader.word(offset)? as usize;
                    self.objects
                        .insert(offset, Value::vector(vec![Value::Nil; size]));
                }
                DumpType::Record | DumpType::Obarray | DumpType::HashTable => {
                    let id = self.reader.word(offset)?;
                    let kind_code = self.reader.word(offset + 8)? as u32;
                    let record_kind = record_kind_from_code(kind_code).ok_or_else(|| {
                        LoadError::Error(format!("unknown record kind {kind_code} at {offset}"))
                    })?;
                    let nslots = self.reader.word(offset + 24)? as usize;
                    let type_tag = self.symbol_or_immediate_at(offset + 16)?;
                    self.interp.install_record(record_state_for_load(
                        id,
                        record_kind,
                        type_tag,
                        vec![Value::Nil; nslots],
                    ));
                    self.objects.insert(offset, Value::Record(id));
                    if kind == DumpType::Obarray {
                        obarray_records.push((offset, id, nslots));
                    }
                    if kind == DumpType::HashTable {
                        hash_table_records.push((offset, id, nslots));
                    }
                }
                DumpType::CharTable => {
                    let id = self.reader.word(offset)?;
                    self.objects.insert(offset, Value::CharTable(id));
                    char_tables.push((offset, id));
                }
                DumpType::Buffer => {
                    let id = self.reader.word(offset)?;
                    let flags = self.reader.word(offset + 8 * BUFFER_FLAGS)?;
                    if flags & BUFFER_FLAG_DEAD != 0 {
                        // A killed buffer: the object, and nothing to
                        // install.
                        self.objects
                            .insert(offset, Value::buffer(id, String::new()));
                        continue;
                    }
                    let name = self.value_at(offset + 8 * BUFFER_NAME)?;
                    let name = string_like(&name)
                        .map(|string| string.text)
                        .ok_or_else(|| LoadError::Error("buffer name is not a string".into()))?;
                    self.objects.insert(offset, Value::buffer(id, name));
                    buffers.push((offset, id));
                }
                DumpType::Marker => {
                    let id = self.reader.word(offset)?;
                    self.objects.insert(offset, Value::Marker(id));
                    markers.push((offset, id));
                }
                DumpType::Overlay => {
                    let id = self.reader.word(offset)?;
                    self.objects.insert(offset, Value::Overlay(id));
                    overlays.push((offset, id));
                }
                DumpType::Finalizer => {
                    let id = self.reader.word(offset)?;
                    self.objects.insert(offset, Value::Finalizer(id));
                    finalizers.push((offset, id));
                }
                DumpType::Frame => {
                    let id = self.reader.word(offset)?;
                    self.interp.install_dead_frame(id);
                    self.objects.insert(offset, Value::Frame(id));
                }
                DumpType::Terminal => {
                    let id = self.reader.word(offset)?;
                    self.objects.insert(offset, Value::Terminal(id));
                }
                _ => {}
            }
        }

        // Phase 3: closures, materialized on demand with their shared
        // parameter vectors, bodies and environments.
        for &(offset, kind) in &object_starts {
            if kind == DumpType::Closure {
                self.closure_at(offset)?;
            }
        }

        // Phase 4: the containers' fields.
        let mut obarray = Vec::new();
        for &(offset, kind) in &object_starts {
            match kind {
                DumpType::Cons => {
                    let car = self.value_at(offset)?;
                    let cdr = self.value_at(offset + 8)?;
                    let cell = &self.objects[&offset];
                    cell.set_car(car).map_err(lisp_error)?;
                    cell.set_cdr(cdr).map_err(lisp_error)?;
                }
                DumpType::Vector => {
                    let size = self.reader.word(offset)? as usize;
                    let Value::Vector(vector) = self.objects[&offset].clone() else {
                        unreachable!()
                    };
                    for index in 0..size {
                        let slot = self.value_at(offset + 8 * (index as u32 + 1))?;
                        vector.slots_mut()[index] = slot;
                    }
                }
                DumpType::Record | DumpType::Obarray | DumpType::HashTable => {
                    let id = self.reader.word(offset)?;
                    let nslots = self.reader.word(offset + 24)? as usize;
                    let mut slots = Vec::with_capacity(nslots);
                    for index in 0..nslots {
                        slots.push(self.value_at(offset + 32 + 8 * index as u32)?);
                    }
                    let record = self
                        .interp
                        .find_record_mut(id)
                        .ok_or_else(|| LoadError::Error(format!("record {id} was installed")))?;
                    record.slots = slots;
                }
                _ => {}
            }
        }
        // thaw_hash_tables: every table on the hash list, from its frozen
        // contents.
        let thaw_list = self.hash_list_tables(&header)?;
        for (offset, id, nslots) in hash_table_records {
            if !thaw_list.contains(&id) {
                return Err(LoadError::Error(format!(
                    "hash table {id} is not on the hash list"
                )));
            }
            let mut at = offset + 32 + 8 * nslots as u32;
            let count = self.reader.word(at)? as usize;
            let weakness = self.value_at(at + 8)?;
            let test_code = self.reader.word(at + 16)?;
            let mutable = self.reader.word(at + 24)? != 0;
            at += 32;
            let mut entries = Vec::with_capacity(count);
            for _ in 0..count {
                let key = self.value_at(at)?;
                let value = self.value_at(at + 8)?;
                entries.push((key, value));
                at += 16;
            }
            let test = match test_code {
                HASH_TEST_EQ => "eq",
                HASH_TEST_EQL => "eql",
                HASH_TEST_EQUAL => "equal",
                other => {
                    return Err(LoadError::Error(format!(
                        "hash table {id} has frozen test {other}"
                    )));
                }
            };
            let record = self
                .interp
                .find_record_mut(id)
                .ok_or_else(|| LoadError::Error(format!("hash table {id} was installed")))?;
            if record.slots.len() > 5 {
                record.slots[5] = weakness;
            }
            self.interp.thaw_hash_table(id, test, entries);
            if !mutable {
                self.interp.mark_hash_table_immutable(id);
            }
        }
        for (offset, id, nslots) in obarray_records {
            let count_at = offset + 32 + 8 * nslots as u32;
            let count = self.reader.word(count_at)? as usize;
            for index in 0..count {
                let value = self.value_at(count_at + 8 * (index as u32 + 1))?;
                let symbol = symbol_of(value, "obarray entry")?;
                if self.interp.is_standard_obarray_id(id) {
                    obarray.push(symbol);
                }
            }
        }
        for (offset, id) in char_tables {
            let table = self.load_char_table(offset, id)?;
            self.interp.install_char_table(table);
        }
        for (string_offset, props_offset) in string_props {
            let spans = self.load_text_properties(props_offset)?;
            match &self.objects[&string_offset] {
                Value::StringObject(state) => {
                    state.borrow_mut().props = spans;
                }
                other => {
                    return Err(LoadError::Error(format!(
                        "text properties on a non-object string: {other:?}"
                    )));
                }
            }
        }
        // The buffers with their text, then the markers into them, the
        // deleted overlays on their lists, and the finalizers in list
        // order (the `finalizers.next' root leads the chain; a finalizer
        // off the chain follows in image order).
        for (offset, id) in buffers {
            self.load_buffer(offset, id)?;
        }
        for (offset, id) in markers {
            let state = self.load_marker(offset, id)?;
            self.interp.install_marker(state);
        }
        for (offset, id) in overlays {
            let (holder, overlay) = self.load_overlay(offset, id)?;
            self.interp.install_overlay(holder, overlay);
        }
        let mut finalizer_records = Vec::new();
        for (offset, id) in finalizers {
            let function = self.value_at(offset + 8)?;
            let next = match self.value_at(offset + 24)? {
                Value::Finalizer(next) => Some(next),
                Value::Nil => None,
                other => {
                    return Err(LoadError::Error(format!(
                        "finalizer {id}'s next is not a finalizer: {other:?}"
                    )));
                }
            };
            finalizer_records.push((id, function, next));
        }

        // Phase 5: the symbol records.
        let mut symbols = Vec::new();
        for (offset, symbol, flags) in symbol_records {
            let val = self.value_at(offset + 16)?;
            let function = self.value_at(offset + 24)?;
            let plist = self.value_at(offset + 32)?;
            let nwatchers = self.reader.word(offset + 40)? as u32;
            let mut watchers = Vec::new();
            for index in 0..nwatchers {
                watchers.push(self.value_at(offset + 48 + 8 * index)?);
            }
            let (value, alias) = if flags & SYMBOL_REDIRECT_MASK == SYMBOL_VARALIAS {
                (None, Some(symbol_of(val, "alias target")?))
            } else {
                ((!matches!(val, Value::Unbound)).then_some(val), None)
            };
            symbols.push(LoadedSymbol {
                symbol,
                flags,
                value,
                alias,
                function,
                plist,
                watchers,
            });
        }

        // Phase 6: the Emacs relocations, the root slots.
        let mut roots = Vec::new();
        let mut builtin_cells = Vec::new();
        for index in 0..header.emacs_relocs.nr_entries {
            let at = header.emacs_relocs.offset + index * EMACS_RELOC_LEN as u32;
            let kind = EmacsRelocKind::from_u32(self.reader.u32(at)?)
                .ok_or_else(|| LoadError::Error(format!("unknown Emacs relocation at {at}")))?;
            let slot = RootSlot::from_u32(self.reader.u32(at + 4)?)
                .ok_or_else(|| LoadError::Error(format!("unknown root slot at {at}")))?;
            let payload = self.reader.word(at + 8)?;
            if matches!(slot, RootSlot::NilCells | RootSlot::TCells)
                && matches!(kind, EmacsRelocKind::DumpLv(_))
            {
                let name = if slot == RootSlot::NilCells {
                    "nil"
                } else {
                    "t"
                };
                builtin_cells.push(self.load_builtin_cells(payload as u32, name)?);
                continue;
            }
            let value = match kind {
                EmacsRelocKind::Immediate => immediate_value(payload)?,
                EmacsRelocKind::DumpLv(_) => self
                    .objects
                    .get(&(payload as u32))
                    .cloned()
                    .ok_or_else(|| {
                        LoadError::Error(format!("root points at no object: {payload}"))
                    })?,
                EmacsRelocKind::EmacsLv(kind) => self.emacs_image_object(payload as u32, kind)?,
            };
            roots.push((slot, value));
        }
        let mut chain = Vec::new();
        let mut cursor = roots.iter().find_map(|(slot, value)| match (slot, value) {
            (RootSlot::FinalizersNext, Value::Finalizer(id)) => Some(*id),
            _ => None,
        });
        while let Some(id) = cursor {
            if chain.contains(&id) {
                return Err(LoadError::Error(format!("finalizer chain loops at {id}")));
            }
            chain.push(id);
            cursor = finalizer_records
                .iter()
                .find(|(candidate, _, _)| *candidate == id)
                .and_then(|(_, _, next)| *next);
        }
        for id in &chain {
            let Some((_, function, _)) = finalizer_records
                .iter()
                .find(|(candidate, _, _)| candidate == id)
            else {
                return Err(LoadError::Error(format!(
                    "finalizer {id} is on the chain but not in the image"
                )));
            };
            self.interp.install_finalizer(*id, function.clone());
        }
        for (id, function, _) in finalizer_records {
            if !chain.contains(&id) {
                self.interp.install_finalizer(id, function);
            }
        }
        // The root groups: each reinstalled from its value.
        for (slot, value) in &roots {
            self.interp
                .install_root_group(*slot, value)
                .map_err(|message| LoadError::Error(format!("{slot:?}: {message}")))?;
        }

        Ok(LoadedImage {
            header,
            roots,
            symbols,
            obarray,
            builtin_cells,
        })
    }

    /// The record ids of the vector at `header.hash_list'.
    fn hash_list_tables(&mut self, header: &DumpHeader) -> Result<Vec<u64>, LoadError> {
        if header.hash_list == 0 {
            return Ok(Vec::new());
        }
        let offset = header.hash_list;
        let size = self.reader.word(offset)? as usize;
        let mut ids = Vec::with_capacity(size);
        for index in 0..size {
            match self.value_at(offset + 8 * (index as u32 + 1))? {
                Value::Record(id) => ids.push(id),
                other => {
                    return Err(LoadError::Error(format!(
                        "hash list entry is not a hash table: {other:?}"
                    )));
                }
            }
        }
        Ok(ids)
    }

    /// The Lisp value a record field holds: the relocation says which
    /// object the word names, or the word is immediate.  A closure named
    /// before its own record was reached is materialized here.
    fn value_at(&mut self, field_offset: u32) -> Result<Value, LoadError> {
        let word = self.reader.word(field_offset)?;
        match self.relocs.get(&field_offset).copied() {
            None => immediate_value(word),
            Some(DumpRelocKind::DumpToDumpLv(_)) => {
                let target = word as u32;
                if let Some(value) = self.objects.get(&target) {
                    return Ok(value.clone());
                }
                if self.types.get(&target) == Some(&DumpType::Closure) {
                    return self.closure_at(target);
                }
                Err(LoadError::Error(format!(
                    "field at {field_offset} names no object ({word})"
                )))
            }
            Some(DumpRelocKind::DumpToEmacsLv(kind)) => self.emacs_image_object(word as u32, kind),
            Some(other) => Err(LoadError::Error(format!(
                "field at {field_offset} has a non-object relocation {other:?}"
            ))),
        }
    }

    /// A field that holds a symbol or an immediate, read before the
    /// symbol objects exist: the symbol record's name is a phase-1 string.
    fn symbol_or_immediate_at(&mut self, field_offset: u32) -> Result<Value, LoadError> {
        let word = self.reader.word(field_offset)?;
        match self.relocs.get(&field_offset).copied() {
            Some(DumpRelocKind::DumpToDumpLv(DumpType::Symbol)) => {
                let target = word as u32;
                if let Some(value) = self.objects.get(&target) {
                    return Ok(value.clone());
                }
                let flags = self.reader.word(target)?;
                let name = self.value_at(target + 8)?;
                let name_text = string_like(&name)
                    .map(|string| string.text)
                    .ok_or_else(|| LoadError::Error("symbol name is not a string".into()))?;
                if (flags >> SYMBOL_INTERNED_SHIFT) & 3 == SYMBOL_UNINTERNED {
                    return Err(LoadError::Error(
                        "a record's type tag is an uninterned symbol read early".into(),
                    ));
                }
                Ok(Value::Symbol(SymbolName::intern_str(&name_text)))
            }
            _ => self.value_at(field_offset),
        }
    }

    /// An object of the Emacs image, reached through its copied record in
    /// the discardable section (pdumper.c relocates such a word to the
    /// Emacs address; the copied record is what names it here).
    fn emacs_image_object(&mut self, offset: u32, kind: DumpType) -> Result<Value, LoadError> {
        match kind {
            DumpType::Subr => {
                let name = self.value_at(offset)?;
                let name_text = string_like(&name)
                    .map(|string| string.text)
                    .ok_or_else(|| LoadError::Error("subr name is not a string".into()))?;
                Ok(Value::BuiltinFunc(SymbolName::intern_str(&name_text)))
            }
            DumpType::MainThread => Ok(Value::Record(self.interp.main_thread_record_id())),
            other => Err(LoadError::Error(format!(
                "{other:?} is not an Emacs-image object kind"
            ))),
        }
    }

    /// A closure record: parameters, body and environment through their
    /// raw-pointer words, the Lisp-object slots directly.  The
    /// environment is created as an empty shell first so a closure that
    /// reaches itself through its own frame terminates, as
    /// ImageGraphCopier does for the test template.
    fn closure_at(&mut self, offset: u32) -> Result<Value, LoadError> {
        if let Some(value) = self.objects.get(&offset) {
            return Ok(value.clone());
        }
        if !self.closures_in_progress.insert(offset) {
            return Err(LoadError::Error(format!(
                "closure at {offset} names itself through its body or parameters"
            )));
        }
        let params_offset = self.reader.word(offset)? as u32;
        let body_offset = self.reader.word(offset + 16)? as u32;
        let env_offset = self.reader.word(offset + 24)? as u32;
        let params = self.params_at(params_offset)?;
        let body = self.body_at(body_offset)?;
        let env = self.env_shell_at(env_offset);
        let public_parameters = self.optional_at(offset + 8)?;
        let documentation = self.optional_at(offset + 32)?;
        let interactive = self.optional_at(offset + 40)?;
        let public_environment = self.optional_at(offset + 48)?;
        let closure = Value::allocated_lambda(LambdaValue {
            params,
            public_parameters,
            body,
            env: env.clone(),
            documentation,
            interactive,
            public_environment,
        });
        self.objects.insert(offset, closure.clone());
        self.closures_in_progress.remove(&offset);
        self.fill_env(env_offset)?;
        // The closure owns its captured frames again, so an assignment to
        // a captured variable is shared as it was before the dump.
        self.interp.register_captured_lexical_frames(&env);
        Ok(closure)
    }

    /// A slot that is absent (the unbound word, no relocation) or a value.
    fn optional_at(&mut self, field: u32) -> Result<Option<Value>, LoadError> {
        let word = self.reader.word(field)?;
        if !self.relocs.contains_key(&field) && word == WORD_UNBOUND {
            return Ok(None);
        }
        self.value_at(field).map(Some)
    }

    fn params_at(&mut self, offset: u32) -> Result<Rc<Vec<SymbolName>>, LoadError> {
        if let Some(params) = self.params.get(&offset) {
            return Ok(params.clone());
        }
        let count = self.reader.word(offset)? as usize;
        let mut symbols = Vec::with_capacity(count);
        for index in 0..count {
            let value = self.value_at(offset + 8 * (index as u32 + 1))?;
            symbols.push(symbol_of(value, "closure parameter")?);
        }
        let params = Rc::new(symbols);
        self.params.insert(offset, params.clone());
        Ok(params)
    }

    fn body_at(&mut self, offset: u32) -> Result<Rc<Vec<Value>>, LoadError> {
        if let Some(body) = self.bodies.get(&offset) {
            return Ok(body.clone());
        }
        let count = self.reader.word(offset)? as usize;
        let mut forms = Vec::with_capacity(count);
        for index in 0..count {
            forms.push(self.value_at(offset + 8 * (index as u32 + 1))?);
        }
        let body = Rc::new(forms);
        self.bodies.insert(offset, body.clone());
        Ok(body)
    }

    fn env_shell_at(&mut self, offset: u32) -> SharedEnv {
        if let Some(env) = self.envs.get(&offset) {
            return env.clone();
        }
        let env = crate::lisp::types::shared_env(Vec::new());
        self.envs.insert(offset, env.clone());
        env
    }

    fn fill_env(&mut self, offset: u32) -> Result<(), LoadError> {
        if !self.envs_filled.insert(offset) {
            return Ok(());
        }
        let env = self.env_shell_at(offset);
        let count = self.reader.word(offset)? as usize;
        let mut frames = Vec::with_capacity(count);
        for index in 0..count {
            let frame_offset = self.reader.word(offset + 8 * (index as u32 + 1))? as u32;
            frames.push(self.frame_at(frame_offset)?);
        }
        *env.borrow_mut() = frames;
        Ok(())
    }

    fn frame_at(&mut self, offset: u32) -> Result<EnvFrame, LoadError> {
        if let Some(frame) = self.frames.get(&offset) {
            return Ok(frame.clone());
        }
        let flags = self.reader.word(offset)?;
        let identity = if flags & FRAME_HAS_IDENTITY != 0 {
            Some(self.reader.word(offset + 8)? as i64)
        } else {
            None
        };
        let nbindings = self.reader.word(offset + 16)? as usize;
        let mut at = offset + 24;
        let mut bindings = Vec::with_capacity(nbindings);
        for _ in 0..nbindings {
            let symbol = symbol_of(self.value_at(at)?, "lexical binding")?;
            let value = self.value_at(at + 8)?;
            bindings.push((symbol, value));
            at += 16;
        }
        let ndeclarations = self.reader.word(at)? as usize;
        at += 8;
        let mut declarations = Vec::with_capacity(ndeclarations);
        for _ in 0..ndeclarations {
            let position = self.reader.word(at)? as usize;
            let name = symbol_of(self.value_at(at + 8)?, "locally special declaration")?;
            declarations.push((position, name.as_str().to_owned()));
            at += 16;
        }
        let mut frame = EnvFrame::from_parts(
            bindings,
            identity,
            flags & FRAME_FUNCTION_BINDINGS != 0,
            declarations,
        );
        if let Some(environment) = self.optional_at(at)? {
            frame.set_lisp_environment(environment);
        }
        self.frames.insert(offset, frame.clone());
        Ok(frame)
    }

    /// A string record: size, size_byte, intervals, data; the bytes at the
    /// cold offset in GNU's internal representation.
    fn load_string(
        &mut self,
        offset: u32,
        kind: DumpType,
    ) -> Result<(Value, Option<u32>), LoadError> {
        let size = self.reader.word(offset)? as usize;
        let size_byte = self.reader.word(offset + 8)?;
        let intervals = self.reader.word(offset + 16)? as u32;
        let data = self.reader.word(offset + 24)? as u32;
        let multibyte = size_byte != u64::MAX;
        let nbytes = if multibyte { size_byte as usize } else { size };
        let bytes = self
            .reader
            .bytes
            .get(data as usize..data as usize + nbytes)
            .ok_or_else(|| {
                LoadError::Error(format!("string data at {data} is outside the image"))
            })?;
        let (text, extended_chars) = decode_internal_bytes(bytes, multibyte)?;
        if text.chars().count() != size {
            return Err(LoadError::Error(format!(
                "string at {offset} decodes to {} characters, record says {size}",
                text.chars().count()
            )));
        }
        let value = match kind {
            DumpType::String => Value::String(SharedText::new(text)),
            _ => crate::lisp::primitives::strings::make_shared_string_value_with_extended_chars(
                text,
                Vec::new(),
                multibyte,
                extended_chars,
            ),
        };
        Ok((value, (intervals != 0).then_some(intervals)))
    }

    /// A bignum record after its fixup: sign/limb count and the cold limbs.
    fn load_bignum(&mut self, offset: u32) -> Result<Value, LoadError> {
        let sign_limbs = self.reader.word(offset)? as i64;
        let data = self.reader.word(offset + 8)? as u32;
        let nlimbs = sign_limbs.unsigned_abs() as u32;
        let mut limbs = Vec::with_capacity(nlimbs as usize);
        for index in 0..nlimbs {
            limbs.push(self.reader.word(data + 8 * index)?);
        }
        let magnitude = num_bigint::BigUint::from_slice(
            &limbs
                .iter()
                .flat_map(|limb| [*limb as u32, (*limb >> 32) as u32])
                .collect::<Vec<_>>(),
        );
        let sign = if sign_limbs < 0 {
            num_bigint::Sign::Minus
        } else if num_traits::Zero::is_zero(&magnitude) {
            num_bigint::Sign::NoSign
        } else {
            num_bigint::Sign::Plus
        };
        let integer = num_bigint::BigInt::from_biguint(sign, magnitude);
        Ok(match i64::try_from(&integer) {
            Ok(value) if fixnum_word(value).is_none() => Value::Integer(value),
            _ => Value::big_integer(integer),
        })
    }

    /// A bool-vector's cold record: id, bit count, packed bits.
    fn load_bool_vector(&mut self, offset: u32) -> Result<Value, LoadError> {
        let id = self.reader.word(offset)?;
        let nbits = self.reader.word(offset + 8)? as usize;
        let mut slots = Vec::with_capacity(nbits);
        for index in 0..nbits {
            let word = self.reader.word(offset + 16 + 8 * (index / 64) as u32)?;
            slots.push(if word & (1 << (index % 64)) != 0 {
                Value::T
            } else {
                Value::Nil
            });
        }
        self.interp.install_record(record_state_for_load(
            id,
            RecordKind::BoolVector,
            Value::symbol("bool-vector"),
            slots,
        ));
        Ok(Value::Record(id))
    }

    /// A char-table record: id, subtype, default, parent, extra slots,
    /// range entries, category docstrings.
    fn load_char_table(
        &mut self,
        offset: u32,
        id: u64,
    ) -> Result<crate::lisp::eval::CharTableState, LoadError> {
        let subtype = self
            .optional_at(offset + 8)?
            .map(|value| {
                symbol_of(value, "char-table subtype").map(|symbol| symbol.as_str().to_owned())
            })
            .transpose()?;
        let default = self.value_at(offset + 16)?;
        let parent_word = self.reader.word(offset + 24)?;
        let parent = (parent_word != u64::MAX).then_some(parent_word);
        let mut at = offset + 32;
        let nextra = self.reader.word(at)? as usize;
        at += 8;
        let mut extra_slots = Vec::with_capacity(nextra);
        for _ in 0..nextra {
            extra_slots.push(self.value_at(at)?);
            at += 8;
        }
        let nentries = self.reader.word(at)? as usize;
        at += 8;
        let mut entries = Vec::with_capacity(nentries);
        for _ in 0..nentries {
            let start = self.reader.word(at)? as u32;
            let end = self.reader.word(at + 8)? as u32;
            let value = self.value_at(at + 16)?;
            entries.push((start, end, value));
            at += 24;
        }
        let ndocs = self.reader.word(at)? as usize;
        at += 8;
        let mut category_docs = Vec::with_capacity(ndocs);
        for _ in 0..ndocs {
            let character = self.reader.word(at)? as u32;
            let doc = string_like(&self.value_at(at + 8)?)
                .map(|string| string.text)
                .ok_or_else(|| LoadError::Error("category docstring is not a string".into()))?;
            category_docs.push((character, doc));
            at += 16;
        }
        Ok(char_table_state_for_load(
            id,
            subtype,
            default,
            parent,
            entries,
            extra_slots,
            category_docs,
        ))
    }

    /// A text-properties record: count, then (start, end, nprops, (name,
    /// value)*).
    fn load_text_properties(
        &mut self,
        offset: u32,
    ) -> Result<Vec<crate::lisp::types::StringPropertySpan>, LoadError> {
        let count = self.reader.word(offset)? as usize;
        let mut at = offset + 8;
        let mut spans = Vec::with_capacity(count);
        for _ in 0..count {
            let start = self.reader.word(at)? as usize;
            let end = self.reader.word(at + 8)? as usize;
            let nprops = self.reader.word(at + 16)? as usize;
            at += 24;
            let mut props = Vec::with_capacity(nprops);
            for _ in 0..nprops {
                let name = symbol_of(self.value_at(at)?, "property name")?;
                let value = self.value_at(at + 8)?;
                at += 16;
                props.push((name.as_str().to_owned(), value));
            }
            spans.push(crate::lisp::types::StringPropertySpan { start, end, props });
        }
        Ok(spans)
    }

    /// A buffer record: the fields `dump_buffer' wrote, the text and the
    /// saved snapshot from the cold section, the property spans, the
    /// local bindings, the syntax and case tables and the undo entries;
    /// the buffer is installed with its id.
    fn load_buffer(&mut self, offset: u32, id: u64) -> Result<(), LoadError> {
        let word = |loader: &Self, index: u32| loader.reader.word(offset + 8 * index);
        let flags = word(self, BUFFER_FLAGS)?;
        let multibyte = flags & BUFFER_FLAG_MULTIBYTE != 0;
        let name = string_like(&self.value_at(offset + 8 * BUFFER_NAME)?)
            .map(|string| string.text)
            .ok_or_else(|| LoadError::Error("buffer name is not a string".into()))?;
        let file = self.optional_string_at(offset + 8 * BUFFER_FILE)?;
        let file_truename = self.optional_string_at(offset + 8 * BUFFER_FILE_TRUENAME)?;
        let size = word(self, BUFFER_Z)? as usize;
        let (text, extended_chars) = self.cold_text_at(
            word(self, BUFFER_TEXT)? as u32,
            word(self, BUFFER_Z_BYTE)? as usize,
            multibyte,
        )?;
        if text.chars().count() != size {
            return Err(LoadError::Error(format!(
                "buffer {id} decodes to {} characters, record says {size}",
                text.chars().count()
            )));
        }
        let (saved_text, _) = self.cold_text_at(
            word(self, BUFFER_SAVED_TEXT)? as u32,
            word(self, BUFFER_SAVED_BYTES)? as usize,
            multibyte,
        )?;
        let position = |word: u64| (word != NO_POSITION).then_some(word as usize);
        let visited_file_modtime = (flags & BUFFER_FLAG_HAS_MODTIME != 0).then(|| {
            let secs = word(self, BUFFER_MODTIME_SECS).unwrap_or(0) as i64;
            let nanos = word(self, BUFFER_MODTIME_NANOS).unwrap_or(0) as u32;
            let duration = std::time::Duration::new(secs.unsigned_abs(), nanos);
            let modified = if secs < 0 {
                std::time::UNIX_EPOCH - duration
            } else {
                std::time::UNIX_EPOCH + duration
            };
            crate::buffer::FileModTime { modified }
        });
        let base = match self.value_at(offset + 8 * BUFFER_BASE)? {
            Value::Buffer(base) => Some(base.id),
            Value::Nil => None,
            other => {
                return Err(LoadError::Error(format!(
                    "buffer {id}'s base is not a buffer: {other:?}"
                )));
            }
        };
        let intervals = word(self, BUFFER_INTERVALS)? as u32;
        let text_properties = if intervals == 0 {
            Vec::new()
        } else {
            self.load_text_properties(intervals)?
                .into_iter()
                .map(|span| crate::buffer::TextPropertySpan {
                    start: span.start,
                    end: span.end,
                    props: span.props,
                })
                .collect()
        };
        // The mark marker installs its own relation; the field is checked.
        match self.value_at(offset + 8 * BUFFER_MARK_MARKER)? {
            Value::Marker(_) | Value::Nil => {}
            other => {
                return Err(LoadError::Error(format!(
                    "buffer {id}'s mark is not a marker: {other:?}"
                )));
            }
        }
        let syntax_table = self.optional_char_table_at(offset + 8 * BUFFER_SYNTAX_TABLE)?;
        let case_table = self.optional_char_table_at(offset + 8 * BUFFER_CASE_TABLE)?;
        let mut at = offset + 8 * BUFFER_VARIABLE_PART;
        let nlocals = self.reader.word(at)? as usize;
        at += 8;
        let mut locals = Vec::with_capacity(nlocals);
        for _ in 0..nlocals {
            let symbol = symbol_of(self.value_at(at)?, "buffer-local variable")?;
            let value = self.value_at(at + 8)?;
            locals.push((symbol, value));
            at += 16;
        }
        let nhooks = self.reader.word(at)? as usize;
        at += 8;
        let mut hooks = Vec::with_capacity(nhooks);
        for _ in 0..nhooks {
            let name = symbol_of(self.value_at(at)?, "buffer-local hook")?;
            let nfunctions = self.reader.word(at + 8)? as usize;
            at += 16;
            let mut functions = Vec::with_capacity(nfunctions);
            for _ in 0..nfunctions {
                functions.push(self.value_at(at)?);
                at += 8;
            }
            hooks.push((name.as_str().to_owned(), functions));
        }
        let nmarkers = self.reader.word(at)? as usize;
        at += 8;
        for _ in 0..nmarkers {
            if !matches!(self.value_at(at)?, Value::Marker(_)) {
                return Err(LoadError::Error(format!(
                    "buffer {id}'s marker chain holds a non-marker"
                )));
            }
            at += 8;
        }
        let nundo = self.reader.word(at)? as usize;
        at += 8;
        let mut undo_list = Vec::with_capacity(nundo);
        for _ in 0..nundo {
            let (entry, next) = self.undo_entry_at(at)?;
            undo_list.push(entry);
            at = next;
        }
        let buffer = crate::buffer::Buffer::from_image_parts(crate::buffer::BufferImage {
            name,
            text,
            pt: word(self, BUFFER_PT)? as usize,
            mark: position(word(self, BUFFER_MARK)?),
            mark_active: flags & BUFFER_FLAG_MARK_ACTIVE != 0,
            modiff: word(self, BUFFER_MODIFF)? as i64,
            chars_modiff: word(self, BUFFER_CHARS_MODIFF)? as i64,
            save_modiff: word(self, BUFFER_SAVE_MODIFF)? as i64,
            saved_text,
            forced_modified: flags & BUFFER_FLAG_FORCED_MODIFIED != 0,
            autosaved: flags & BUFFER_FLAG_AUTOSAVED != 0,
            begv: word(self, BUFFER_BEGV)? as usize,
            zv: word(self, BUFFER_ZV)? as usize,
            file,
            file_truename,
            visited_file_modtime,
            undo_list,
            undo_disabled: flags & BUFFER_FLAG_UNDO_DISABLED != 0,
            point_before_last_boundary: position(word(self, BUFFER_POINT_BEFORE_BOUNDARY)?),
            text_properties,
            extended_chars: extended_chars
                .into_iter()
                .map(|(position, code)| (position + 1, code))
                .collect(),
            inhibit_hooks: flags & BUFFER_FLAG_INHIBIT_HOOKS != 0,
            multibyte,
        });
        self.interp.install_buffer(id, buffer);
        if let Some(base) = base {
            self.interp.register_indirect_buffer(id, base);
        }
        self.interp.install_buffer_local_cells(id, locals);
        for (name, functions) in hooks {
            self.interp.set_buffer_local_hook(id, &name, functions);
        }
        if let Some(table) = syntax_table {
            self.interp.install_buffer_syntax_table(id, table);
        }
        if let Some(table) = case_table {
            self.interp.install_buffer_case_table(id, table);
        }
        Ok(())
    }

    /// One undo entry at AT: the entry and the offset after it.
    fn undo_entry_at(&mut self, at: u32) -> Result<(crate::buffer::UndoEntry, u32), LoadError> {
        use crate::buffer::UndoEntry;
        let kind = self.reader.word(at)?;
        Ok(match kind {
            UNDO_INSERT => (
                UndoEntry::Insert {
                    pos: self.reader.word(at + 8)? as usize,
                    len: self.reader.word(at + 16)? as usize,
                },
                at + 24,
            ),
            UNDO_DELETE => {
                let pos = self.reader.word(at + 8)? as usize;
                let point_after = self.reader.word(at + 16)? != 0;
                let text = string_like(&self.value_at(at + 24)?)
                    .map(|string| string.text)
                    .ok_or_else(|| LoadError::Error("deleted text is not a string".into()))?;
                let props_offset = self.reader.word(at + 32)? as u32;
                let props = if props_offset == 0 {
                    Vec::new()
                } else {
                    self.load_text_properties(props_offset)?
                        .into_iter()
                        .map(|span| crate::buffer::TextPropertySpan {
                            start: span.start,
                            end: span.end,
                            props: span.props,
                        })
                        .collect()
                };
                let mut next = at + 40;
                let nextended = self.reader.word(next)? as usize;
                next += 8;
                let mut extended_chars = Vec::with_capacity(nextended);
                for _ in 0..nextended {
                    extended_chars.push((
                        self.reader.word(next)? as usize,
                        self.reader.word(next + 8)? as u32,
                    ));
                    next += 16;
                }
                let nmarkers = self.reader.word(next)? as usize;
                next += 8;
                let mut markers = Vec::with_capacity(nmarkers);
                for _ in 0..nmarkers {
                    markers.push(crate::buffer::UndoMarker {
                        id: self.reader.word(next)?,
                        original_pos: self.reader.word(next + 8)? as usize,
                        collapsed_pos: self.reader.word(next + 16)? as usize,
                    });
                    next += 24;
                }
                (
                    UndoEntry::Delete {
                        pos,
                        point_after,
                        text,
                        props,
                        extended_chars,
                        markers,
                    },
                    next,
                )
            }
            UNDO_COMBINED => {
                let display = self.value_at(at + 8)?;
                let count = self.reader.word(at + 16)? as usize;
                let mut next = at + 24;
                let mut entries = Vec::with_capacity(count);
                for _ in 0..count {
                    let (entry, after) = self.undo_entry_at(next)?;
                    entries.push(entry);
                    next = after;
                }
                (UndoEntry::Combined { display, entries }, next)
            }
            UNDO_OPAQUE => (UndoEntry::Opaque(self.value_at(at + 8)?), at + 16),
            UNDO_BOUNDARY => (UndoEntry::Boundary, at + 8),
            other => {
                return Err(LoadError::Error(format!(
                    "unknown undo entry kind {other} at {at}"
                )));
            }
        })
    }

    /// A marker record: buffer, positions, insertion type, mark buffer.
    fn load_marker(
        &mut self,
        offset: u32,
        id: u64,
    ) -> Result<crate::lisp::eval::MarkerState, LoadError> {
        let buffer_id = self.optional_buffer_id_at(offset + 8)?;
        let position = |word: u64| (word != NO_POSITION).then_some(word as usize);
        let position_word = self.reader.word(offset + 16)?;
        let last_position_word = self.reader.word(offset + 24)?;
        let insertion_type = self.reader.word(offset + 32)? != 0;
        let mark_buffer_id = self.optional_buffer_id_at(offset + 40)?;
        Ok(crate::lisp::eval::MarkerState {
            id,
            buffer_id,
            position: position(position_word),
            last_position: position(last_position_word),
            insertion_type,
            mark_buffer_id,
        })
    }

    /// An overlay record: flags, bounds, the holding buffer's id, the
    /// buffer field (nil for the deleted overlays that can be written),
    /// and the property list.
    fn load_overlay(
        &mut self,
        offset: u32,
        id: u64,
    ) -> Result<(u64, crate::overlay::Overlay), LoadError> {
        let flags = self.reader.word(offset + 8)?;
        let beg = self.reader.word(offset + 16)? as usize;
        let end = self.reader.word(offset + 24)? as usize;
        let holder = self.reader.word(offset + 32)?;
        let buffer_id = self.optional_buffer_id_at(offset + 40)?;
        let nprops = self.reader.word(offset + 48)? as usize;
        let mut at = offset + 56;
        let mut plist = Vec::with_capacity(nprops);
        for _ in 0..nprops {
            let key = self.value_at(at)?;
            let value = self.value_at(at + 8)?;
            plist.push((key, value));
            at += 16;
        }
        Ok((
            holder,
            crate::overlay::Overlay {
                id,
                beg,
                end,
                front_advance: flags & OVERLAY_FRONT_ADVANCE != 0,
                rear_advance: flags & OVERLAY_REAR_ADVANCE != 0,
                buffer_id,
                plist,
            },
        ))
    }

    /// A cold text run: NBYTES of GNU's internal representation at DATA.
    fn cold_text_at(
        &mut self,
        data: u32,
        nbytes: usize,
        multibyte: bool,
    ) -> Result<(String, Vec<(usize, u32)>), LoadError> {
        let bytes = self
            .reader
            .bytes
            .get(data as usize..data as usize + nbytes)
            .ok_or_else(|| LoadError::Error(format!("text at {data} is outside the image")))?;
        decode_internal_bytes(bytes, multibyte)
    }

    /// A field that is a string or nil.
    fn optional_string_at(&mut self, field: u32) -> Result<Option<String>, LoadError> {
        match self.value_at(field)? {
            Value::Nil => Ok(None),
            value => string_like(&value)
                .map(|string| Some(string.text))
                .ok_or_else(|| LoadError::Error(format!("field at {field} is not a string"))),
        }
    }

    /// A field that is a buffer or nil.
    fn optional_buffer_id_at(&mut self, field: u32) -> Result<Option<u64>, LoadError> {
        match self.value_at(field)? {
            Value::Nil => Ok(None),
            Value::Buffer(buffer) => Ok(Some(buffer.id)),
            other => Err(LoadError::Error(format!(
                "field at {field} is not a buffer: {other:?}"
            ))),
        }
    }

    /// A field that is a char-table or nil.
    fn optional_char_table_at(&mut self, field: u32) -> Result<Option<u64>, LoadError> {
        match self.value_at(field)? {
            Value::Nil => Ok(None),
            Value::CharTable(id) => Ok(Some(id)),
            other => Err(LoadError::Error(format!(
                "field at {field} is not a char-table: {other:?}"
            ))),
        }
    }

    /// The cells record of `nil' or `t': flags, function, plist, watchers.
    fn load_builtin_cells(&mut self, offset: u32, name: &str) -> Result<LoadedSymbol, LoadError> {
        let flags = self.reader.word(offset)?;
        let function = self.value_at(offset + 8)?;
        let plist = self.value_at(offset + 16)?;
        let nwatchers = self.reader.word(offset + 24)? as u32;
        let mut watchers = Vec::with_capacity(nwatchers as usize);
        for index in 0..nwatchers {
            watchers.push(self.value_at(offset + 32 + 8 * index)?);
        }
        Ok(LoadedSymbol {
            symbol: SymbolName::intern_str(name),
            flags,
            value: None,
            alias: None,
            function,
            plist,
            watchers,
        })
    }
}

/// A symbol-valued field: `nil' and `t' arrive as their immediate words.
fn symbol_of(value: Value, what: &str) -> Result<SymbolName, LoadError> {
    match value {
        Value::Symbol(symbol) => Ok(symbol),
        Value::Nil => Ok(SymbolName::intern_str("nil")),
        Value::T => Ok(SymbolName::intern_str("t")),
        other => Err(LoadError::Error(format!(
            "{what} is not a symbol: {other:?}"
        ))),
    }
}

fn lisp_error(error: LispError) -> LoadError {
    LoadError::Error(format!("{error:?}"))
}

/// A word without a relocation is self-representing.
fn immediate_value(word: u64) -> Result<Value, LoadError> {
    if let Some(fixnum) = word_fixnum(word) {
        return Ok(Value::Integer(fixnum));
    }
    Ok(match word {
        WORD_NIL => Value::Nil,
        WORD_T => Value::T,
        WORD_UNBOUND => Value::Unbound,
        _ => {
            return Err(LoadError::Error(format!(
                "unknown immediate word {word:#x}"
            )));
        }
    })
}

/// GNU's internal multibyte form back to Emaxx's text plus the
/// characters Rust cannot spell (a placeholder in the text, the code in
/// the side list); a unibyte string's octets back to its characters.
pub(crate) fn decode_internal_bytes(
    bytes: &[u8],
    multibyte: bool,
) -> Result<(String, Vec<(usize, u32)>), LoadError> {
    let mut text = String::new();
    let mut extended_chars = Vec::new();
    if !multibyte {
        for &byte in bytes {
            text.push(if byte < 0x80 {
                byte as char
            } else {
                crate::lisp::primitives::case::raw_byte_regex_char(byte)
            });
        }
        return Ok((text, extended_chars));
    }
    let mut index = 0;
    let mut chars = 0;
    while index < bytes.len() {
        let lead = bytes[index];
        let (width, mut code) = match lead {
            0x00..=0x7F => (1, u32::from(lead)),
            0xC0..=0xDF => (2, u32::from(lead & 0x1F)),
            0xE0..=0xEF => (3, u32::from(lead & 0x0F)),
            0xF0..=0xF7 => (4, u32::from(lead & 0x07)),
            0xF8 => (5, 0),
            _ => {
                return Err(LoadError::Error(format!(
                    "invalid internal multibyte lead byte {lead:#x} at {index}"
                )));
            }
        };
        if index + width > bytes.len() {
            return Err(LoadError::Error(
                "truncated internal multibyte sequence".into(),
            ));
        }
        for &continuation in &bytes[index + 1..index + width] {
            if continuation & 0xC0 != 0x80 {
                return Err(LoadError::Error(format!(
                    "invalid internal multibyte continuation {continuation:#x}"
                )));
            }
            code = (code << 6) | u32::from(continuation & 0x3F);
        }
        // The two-byte C0/C1 forms carry a raw byte (0x3FFF80..0x3FFFFF).
        if width == 2 && (0xC0..=0xC1).contains(&lead) {
            code += 0x3F_FF80;
        }
        index += width;
        if (0x3F_FF80..=0x3F_FFFF).contains(&code) {
            // character.h:CHAR_TO_BYTE8: the byte is the code less 0x3FFF00.
            text.push(crate::lisp::primitives::case::raw_byte_regex_char(
                (code - 0x3F_FF00) as u8,
            ));
        } else if let Some(ch) = char::from_u32(code) {
            text.push(ch);
        } else {
            extended_chars.push((chars, code));
            text.push(crate::lisp::json::INVALID_UNICODE_SENTINEL);
        }
        chars += 1;
    }
    Ok((text, extended_chars))
}
