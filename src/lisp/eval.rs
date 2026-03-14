use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use super::primitives;
use super::types::{Env, LispError, Value};
use crate::compat::{BatchSummary, DiscoveredTest, TestOutcome, TestStatus};
use regex::{escape as regex_escape, Regex};

#[derive(Clone, Debug)]
pub struct ErtTestDefinition {
    pub name: String,
    pub body: Value,
    pub tags: Vec<String>,
    pub expected_result: String,
}

impl ErtTestDefinition {
    fn discovered(&self) -> DiscoveredTest {
        DiscoveredTest {
            name: self.name.clone(),
            tags: self.tags.clone(),
            expected_result: self.expected_result.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct MarkerState {
    pub id: u64,
    pub buffer_id: Option<u64>,
    pub position: Option<usize>,
    pub last_position: Option<usize>,
    pub insertion_type: bool,
}

#[derive(Clone, Debug)]
pub struct CharTableState {
    pub id: u64,
    pub subtype: Option<String>,
    pub default: Value,
    pub parent: Option<u64>,
    pub extra_slots: Vec<Value>,
    pub entries: Vec<CharTableEntry>,
    pub category_docs: Vec<(u32, String)>,
}

#[derive(Clone, Debug)]
pub struct CharTableEntry {
    pub start: u32,
    pub end: u32,
    pub value: Value,
}

#[derive(Clone, Debug)]
pub struct RecordState {
    pub id: u64,
    pub type_name: String,
    pub slots: Vec<Value>,
}

#[derive(Clone, Debug, Default)]
struct UndoSequenceState {
    original_groups: Vec<Vec<crate::buffer::UndoEntry>>,
    undone_count: usize,
    redo_groups: Vec<Vec<crate::buffer::UndoEntry>>,
    had_error: bool,
}

/// The interpreter state: holds the global environment, the current buffer,
/// and ERT test results.
pub struct Interpreter {
    /// Global variable bindings (defvar, setq at top level).
    globals: Vec<(String, Value)>,
    /// The current buffer being operated on.
    pub buffer: crate::buffer::Buffer,
    /// The ID of the current buffer.
    current_buffer_id: u64,
    /// Inactive buffers keyed by ID.
    inactive_buffers: Vec<(u64, crate::buffer::Buffer)>,
    /// Known buffers: (id, name) pairs.
    pub buffer_list: Vec<(u64, String)>,
    /// Next buffer ID for identity tracking.
    next_buffer_id: u64,
    /// Next overlay ID for identity tracking.
    next_overlay_id: u64,
    /// Next marker ID for identity tracking.
    next_marker_id: u64,
    /// All markers currently known to the interpreter.
    markers: Vec<MarkerState>,
    /// Char tables allocated by the interpreter.
    char_tables: Vec<CharTableState>,
    /// Charset aliases defined at runtime.
    charset_aliases: Vec<(String, String)>,
    /// Charset plist overrides keyed by canonical charset name.
    charset_plists: Vec<(String, Value)>,
    /// Current charset priority order.
    charset_priority: Vec<String>,
    /// ISO charset associations keyed by (dimension, chars, final).
    iso_charsets: Vec<(i64, i64, u32, String)>,
    /// Shared standard category table.
    standard_category_table_id: Option<u64>,
    /// Next char-table ID for identity tracking.
    next_char_table_id: u64,
    /// Allocated record objects.
    records: Vec<RecordState>,
    /// Next record ID for identity tracking.
    next_record_id: u64,
    /// Next finalizer ID for identity tracking.
    next_finalizer_id: u64,
    /// Buffer-local hook lists keyed by (buffer id, hook name).
    buffer_local_hooks: Vec<(u64, String, Vec<Value>)>,
    /// Buffer-local variable values keyed by (buffer id, variable name).
    buffer_locals: Vec<(u64, String, Value)>,
    /// Variables that automatically become buffer-local when set.
    auto_buffer_locals: Vec<String>,
    /// Active labeled restrictions keyed by (buffer id, label, start, end).
    labeled_restrictions: Vec<(u64, String, usize, usize)>,
    /// Indirect buffer mapping: (buffer id, base buffer id).
    indirect_buffers: Vec<(u64, u64)>,
    /// Prevent recursive before/after-change hook re-entry.
    change_hooks_running: usize,
    /// User-defined macros: name → (params, body).
    macros: Vec<(String, Vec<String>, Vec<Value>)>,
    /// User-defined functions in the function namespace.
    functions: Vec<(String, Value)>,
    /// Features currently available in this interpreter.
    provided_features: Vec<String>,
    /// File currently being loaded, if any.
    current_load_file: Option<String>,
    /// Collected ERT test definitions.
    pub ert_tests: Vec<ErtTestDefinition>,
    /// Results from the most recent ERT run.
    pub test_results: Vec<TestOutcome>,
    /// Selected test names from the most recent ERT run.
    pub last_selected_tests: Vec<String>,
    /// The latest regexp match data in buffer coordinates.
    pub last_match_data: Option<Vec<Option<(usize, usize)>>>,
    pub profiler_memory_running: bool,
    pub profiler_memory_log_pending: bool,
    pub profiler_cpu_running: bool,
    pub profiler_cpu_log_pending: bool,
    pub lossage_size: i64,
    face_inheritance: Vec<(String, Option<String>)>,
    undo_sequence: Option<UndoSequenceState>,
    load_path: Vec<PathBuf>,
    loading_features: Vec<String>,
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
            current_buffer_id: 0,
            inactive_buffers: Vec::new(),
            buffer_list: vec![(0, "*test*".to_string())],
            next_buffer_id: 1,
            next_overlay_id: 1,
            next_marker_id: 1,
            markers: Vec::new(),
            char_tables: Vec::new(),
            charset_aliases: Vec::new(),
            charset_plists: Vec::new(),
            charset_priority: vec!["unicode".into(), "ascii".into()],
            iso_charsets: vec![(1, 94, 'B' as u32, "ascii".into())],
            standard_category_table_id: None,
            next_char_table_id: 1,
            records: Vec::new(),
            next_record_id: 1,
            next_finalizer_id: 1,
            buffer_local_hooks: Vec::new(),
            buffer_locals: Vec::new(),
            auto_buffer_locals: Vec::new(),
            labeled_restrictions: Vec::new(),
            indirect_buffers: Vec::new(),
            change_hooks_running: 0,
            macros: Vec::new(),
            functions: Vec::new(),
            provided_features: vec!["emaxx".into()],
            current_load_file: None,
            ert_tests: Vec::new(),
            test_results: Vec::new(),
            last_selected_tests: Vec::new(),
            last_match_data: None,
            profiler_memory_running: false,
            profiler_memory_log_pending: false,
            profiler_cpu_running: false,
            profiler_cpu_log_pending: false,
            lossage_size: 300,
            face_inheritance: Vec::new(),
            undo_sequence: None,
            load_path: Vec::new(),
            loading_features: Vec::new(),
        }
    }

    pub fn set_load_path(&mut self, load_path: Vec<PathBuf>) {
        self.load_path = load_path;
    }

    /// Allocate a new unique buffer ID.
    pub fn alloc_buffer_id(&mut self) -> u64 {
        let id = self.next_buffer_id;
        self.next_buffer_id += 1;
        id
    }

    /// Check if a buffer name exists in the buffer list.
    pub fn has_buffer(&self, name: &str) -> bool {
        self.buffer_list.iter().any(|(_, n)| n == name)
    }

    /// Check if a buffer ID exists in the live buffer list.
    pub fn has_buffer_id(&self, id: u64) -> bool {
        self.buffer_list.iter().any(|(buffer_id, _)| *buffer_id == id)
    }

    /// Find a buffer by name, returning (id, name).
    pub fn find_buffer(&self, name: &str) -> Option<(u64, String)> {
        self.buffer_list.iter().find(|(_, n)| n == name).cloned()
    }

    /// Return the current buffer ID.
    pub fn current_buffer_id(&self) -> u64 {
        self.current_buffer_id
    }

    pub fn set_current_load_file(&mut self, path: Option<String>) -> Option<String> {
        std::mem::replace(&mut self.current_load_file, path)
    }

    pub fn current_load_file(&self) -> Option<&str> {
        self.current_load_file.as_deref()
    }

    fn resolve_load_target(&self, target: &str) -> Option<PathBuf> {
        let direct = PathBuf::from(target);
        if direct.exists() {
            return Some(direct);
        }

        let with_el = if target.ends_with(".el") {
            None
        } else {
            Some(format!("{target}.el"))
        };
        for root in &self.load_path {
            let candidate = root.join(target);
            if candidate.exists() {
                return Some(candidate);
            }
            if let Some(with_el) = &with_el {
                let candidate = root.join(with_el);
                if candidate.exists() {
                    return Some(candidate);
                }
            }
        }
        None
    }

    fn require_feature(&mut self, feature: &str) -> Result<Value, LispError> {
        if self.has_feature(feature) || self.loading_features.iter().any(|name| name == feature) {
            return Ok(Value::Symbol(feature.to_string()));
        }
        let Some(path) = self.resolve_load_target(feature) else {
            return Err(LispError::Signal(format!("Cannot open load file: {feature}")));
        };
        self.loading_features.push(feature.to_string());
        let load_result = crate::lisp::load_file_strict(self, &path);
        self.loading_features.pop();
        load_result?;
        if !self.has_feature(feature) {
            self.provide_feature(feature);
        }
        Ok(Value::Symbol(feature.to_string()))
    }

    /// Resolve a Lisp string-or-buffer value to a live buffer ID.
    pub fn resolve_buffer_id(&self, value: &Value) -> Result<u64, LispError> {
        match value {
            Value::Buffer(id, _) if self.has_buffer_id(*id) => Ok(*id),
            Value::Buffer(_, name) | Value::String(name) => self
                .find_buffer(name)
                .map(|(id, _)| id)
                .ok_or_else(|| LispError::Signal(format!("No buffer named {}", name))),
            _ => Err(LispError::TypeError(
                "string-or-buffer".into(),
                value.type_name(),
            )),
        }
    }

    /// Create and register a new empty buffer.
    pub fn create_buffer(&mut self, name: &str) -> (u64, String) {
        let id = self.alloc_buffer_id();
        self.inactive_buffers
            .push((id, crate::buffer::Buffer::new(name)));
        self.buffer_list.push((id, name.to_string()));
        (id, name.to_string())
    }

    /// Switch the current buffer to a different live buffer ID.
    pub fn switch_to_buffer_id(&mut self, id: u64) -> Result<(), LispError> {
        if id == self.current_buffer_id {
            return Ok(());
        }
        let pos = self
            .inactive_buffers
            .iter()
            .position(|(buffer_id, _)| *buffer_id == id)
            .ok_or_else(|| LispError::Signal(format!("No buffer with id {}", id)))?;
        let (_, next_buffer) = self.inactive_buffers.swap_remove(pos);
        let current_id = self.current_buffer_id;
        let current_buffer = std::mem::replace(&mut self.buffer, next_buffer);
        self.inactive_buffers.push((current_id, current_buffer));
        self.current_buffer_id = id;
        Ok(())
    }

    /// Remove a non-current buffer from the live buffer list.
    pub fn remove_buffer_id(&mut self, id: u64) -> Option<crate::buffer::Buffer> {
        if id == self.current_buffer_id {
            return None;
        }
        self.buffer_list.retain(|(buffer_id, _)| *buffer_id != id);
        self.inactive_buffers
            .iter()
            .position(|(buffer_id, _)| *buffer_id == id)
            .map(|pos| self.inactive_buffers.swap_remove(pos).1)
    }

    /// Kill a buffer by ID, switching away if it is current.
    pub fn kill_buffer_id(&mut self, id: u64) {
        self.detach_markers_for_buffer(id);
        self.buffer_locals
            .retain(|(buffer_id, _, _)| *buffer_id != id);
        self.indirect_buffers
            .retain(|(buffer_id, base_id)| *buffer_id != id && *base_id != id);
        if id == self.current_buffer_id {
            self.buffer_list.retain(|(buffer_id, _)| *buffer_id != id);
            if let Some((next_id, next_buffer)) = self.inactive_buffers.pop() {
                self.buffer = next_buffer;
                self.current_buffer_id = next_id;
            } else {
                let scratch_id = self.alloc_buffer_id();
                self.buffer = crate::buffer::Buffer::new("*scratch*");
                self.current_buffer_id = scratch_id;
                self.buffer_list.push((scratch_id, "*scratch*".to_string()));
            }
        } else {
            let _ = self.remove_buffer_id(id);
        }
    }

    /// Allocate a new unique overlay ID.
    pub fn alloc_overlay_id(&mut self) -> u64 {
        let id = self.next_overlay_id;
        self.next_overlay_id += 1;
        id
    }

    pub fn alloc_record_id(&mut self) -> u64 {
        let id = self.next_record_id;
        self.next_record_id += 1;
        id
    }

    pub fn alloc_finalizer_id(&mut self) -> u64 {
        let id = self.next_finalizer_id;
        self.next_finalizer_id += 1;
        id
    }

    /// Allocate a new marker.
    pub fn make_marker(&mut self) -> Value {
        let id = self.next_marker_id;
        self.next_marker_id += 1;
        self.markers.push(MarkerState {
            id,
            buffer_id: None,
            position: None,
            last_position: None,
            insertion_type: false,
        });
        Value::Marker(id)
    }

    pub fn find_marker(&self, id: u64) -> Option<&MarkerState> {
        self.markers.iter().find(|marker| marker.id == id)
    }

    pub fn find_marker_mut(&mut self, id: u64) -> Option<&mut MarkerState> {
        self.markers.iter_mut().find(|marker| marker.id == id)
    }

    pub fn marker_position(&self, id: u64) -> Option<usize> {
        self.find_marker(id).and_then(|marker| marker.position)
    }

    pub fn marker_buffer_id(&self, id: u64) -> Option<u64> {
        self.find_marker(id).and_then(|marker| marker.buffer_id)
    }

    pub fn marker_last_position(&self, id: u64) -> Option<usize> {
        self.find_marker(id).and_then(|marker| marker.last_position)
    }

    pub fn marker_insertion_type(&self, id: u64) -> Option<bool> {
        self.find_marker(id).map(|marker| marker.insertion_type)
    }

    pub fn set_marker_insertion_type(&mut self, id: u64, insertion_type: bool) {
        if let Some(marker) = self.find_marker_mut(id) {
            marker.insertion_type = insertion_type;
        }
    }

    pub fn set_marker(
        &mut self,
        id: u64,
        position: Option<usize>,
        buffer_id: Option<u64>,
    ) -> Result<(), LispError> {
        let marker = self
            .find_marker_mut(id)
            .ok_or_else(|| LispError::TypeError("marker".into(), format!("marker<{}>", id)))?;
        marker.buffer_id = buffer_id;
        marker.position = position;
        if let Some(pos) = position {
            marker.last_position = Some(pos);
        }
        Ok(())
    }

    pub fn copy_marker_value(
        &mut self,
        value: &Value,
        insertion_type: bool,
    ) -> Result<Value, LispError> {
        let marker_value = self.make_marker();
        let Value::Marker(marker_id) = marker_value else {
            unreachable!("make_marker always returns a marker")
        };
        match value {
            Value::Nil => {
                self.set_marker(marker_id, None, None)?;
            }
            Value::Marker(source_id) => {
                let source = self.find_marker(*source_id).cloned().ok_or_else(|| {
                    LispError::TypeError("marker".into(), format!("marker<{}>", source_id))
                })?;
                self.set_marker(marker_id, source.position, source.buffer_id)?;
            }
            Value::Integer(position) => {
                self.set_marker(
                    marker_id,
                    Some(*position as usize),
                    Some(self.current_buffer_id()),
                )?;
            }
            _ => {
                return Err(LispError::TypeError(
                    "integer-or-marker-p".into(),
                    value.type_name(),
                ));
            }
        }
        self.set_marker_insertion_type(marker_id, insertion_type);
        Ok(marker_value)
    }

    pub fn make_char_table(&mut self, subtype: Option<String>, default: Value) -> Value {
        let id = self.next_char_table_id;
        self.next_char_table_id += 1;
        self.char_tables.push(CharTableState {
            id,
            subtype,
            default,
            parent: None,
            extra_slots: Vec::new(),
            entries: Vec::new(),
            category_docs: Vec::new(),
        });
        Value::CharTable(id)
    }

    pub fn find_char_table(&self, id: u64) -> Option<&CharTableState> {
        self.char_tables.iter().find(|table| table.id == id)
    }

    pub fn find_char_table_mut(&mut self, id: u64) -> Option<&mut CharTableState> {
        self.char_tables.iter_mut().find(|table| table.id == id)
    }

    pub fn char_table_set(&mut self, id: u64, key: u32, value: Value) -> Result<(), LispError> {
        self.char_table_set_range(id, key, key, value)
    }

    pub fn char_table_set_range(
        &mut self,
        id: u64,
        start: u32,
        end: u32,
        value: Value,
    ) -> Result<(), LispError> {
        let table = self
            .find_char_table_mut(id)
            .ok_or_else(|| LispError::TypeError("char-table".into(), format!("char-table<{id}>")))?;
        table.entries.push(CharTableEntry {
            start: start.min(end),
            end: start.max(end),
            value,
        });
        Ok(())
    }

    pub fn char_table_set_default(&mut self, id: u64, value: Value) -> Result<(), LispError> {
        let table = self
            .find_char_table_mut(id)
            .ok_or_else(|| LispError::TypeError("char-table".into(), format!("char-table<{id}>")))?;
        table.default = value;
        Ok(())
    }

    pub fn char_table_get(&self, id: u64, key: u32) -> Option<Value> {
        let table = self.find_char_table(id)?;
        if let Some(entry) = table
            .entries
            .iter()
            .rev()
            .find(|entry| entry.start <= key && key <= entry.end)
        {
            return Some(entry.value.clone());
        }
        if let Some(parent_id) = table.parent
            && let Some(value) = self.char_table_get(parent_id, key)
        {
            return Some(value);
        }
        Some(table.default.clone())
    }

    pub fn char_table_range(&self, id: u64, start: u32, end: u32) -> Option<Value> {
        let table = self.find_char_table(id)?;
        if let Some(entry) = table
            .entries
            .iter()
            .rev()
            .find(|entry| entry.start == start.min(end) && entry.end == start.max(end))
        {
            return Some(entry.value.clone());
        }
        if let Some(parent_id) = table.parent
            && let Some(value) = self.char_table_range(parent_id, start, end)
        {
            return Some(value);
        }
        Some(table.default.clone())
    }

    pub fn char_table_subtype(&self, id: u64) -> Option<Option<String>> {
        self.find_char_table(id).map(|table| table.subtype.clone())
    }

    pub fn char_table_parent(&self, id: u64) -> Option<Option<u64>> {
        self.find_char_table(id).map(|table| table.parent)
    }

    pub fn set_char_table_parent(&mut self, id: u64, parent: Option<u64>) -> Result<(), LispError> {
        let table = self
            .find_char_table_mut(id)
            .ok_or_else(|| LispError::TypeError("char-table".into(), format!("char-table<{id}>")))?;
        table.parent = parent;
        Ok(())
    }

    pub fn char_table_extra_slot(&self, id: u64, slot: usize) -> Option<Value> {
        self.find_char_table(id)
            .and_then(|table| table.extra_slots.get(slot).cloned())
    }

    pub fn set_char_table_extra_slot(
        &mut self,
        id: u64,
        slot: usize,
        value: Value,
    ) -> Result<(), LispError> {
        let table = self
            .find_char_table_mut(id)
            .ok_or_else(|| LispError::TypeError("char-table".into(), format!("char-table<{id}>")))?;
        while table.extra_slots.len() <= slot {
            table.extra_slots.push(Value::Nil);
        }
        table.extra_slots[slot] = value;
        Ok(())
    }

    pub fn char_table_purpose(&self, id: u64) -> Option<&str> {
        self.find_char_table(id)
            .and_then(|table| table.subtype.as_deref())
    }

    pub fn clone_char_table(&mut self, id: u64) -> Result<Value, LispError> {
        let source = self
            .find_char_table(id)
            .cloned()
            .ok_or_else(|| LispError::TypeError("char-table".into(), format!("char-table<{id}>")))?;
        let new_id = self.next_char_table_id;
        self.next_char_table_id += 1;
        self.char_tables.push(CharTableState {
            id: new_id,
            ..source
        });
        Ok(Value::CharTable(new_id))
    }

    pub fn create_record(&mut self, type_name: &str, slots: Vec<Value>) -> Value {
        let id = self.alloc_record_id();
        self.records.push(RecordState {
            id,
            type_name: type_name.to_string(),
            slots,
        });
        Value::Record(id)
    }

    pub fn find_record(&self, id: u64) -> Option<&RecordState> {
        self.records.iter().find(|record| record.id == id)
    }

    pub fn copy_record(&mut self, id: u64) -> Result<Value, LispError> {
        let record = self
            .find_record(id)
            .cloned()
            .ok_or_else(|| LispError::TypeError("record".into(), format!("record<{id}>")))?;
        Ok(self.create_record(&record.type_name, record.slots))
    }

    pub fn provide_feature(&mut self, feature: &str) {
        if !self.provided_features.iter().any(|name| name == feature) {
            self.provided_features.push(feature.to_string());
        }
    }

    pub fn has_feature(&self, feature: &str) -> bool {
        self.provided_features.iter().any(|name| name == feature)
    }

    fn builtin_charset_name(name: &str) -> bool {
        matches!(name, "ascii" | "unicode")
    }

    pub fn charset_canonical_name(&self, name: &str) -> Option<String> {
        let mut current = name.to_string();
        for _ in 0..16 {
            if Self::builtin_charset_name(&current) {
                return Some(current);
            }
            let (_, target) = self
                .charset_aliases
                .iter()
                .rev()
                .find(|(alias, _)| alias == &current)?;
            current = target.clone();
        }
        None
    }

    pub fn has_charset(&self, name: &str) -> bool {
        self.charset_canonical_name(name).is_some()
    }

    pub fn charset_id(&self, name: &str) -> Option<i64> {
        match self.charset_canonical_name(name)?.as_str() {
            "ascii" => Some(0),
            "unicode" => Some(1),
            _ => None,
        }
    }

    pub fn charset_plist_value(&self, name: &str) -> Option<Value> {
        let canonical = self.charset_canonical_name(name)?;
        self.charset_plists
            .iter()
            .rev()
            .find(|(charset, _)| charset == &canonical)
            .map(|(_, value)| value.clone())
    }

    pub fn set_charset_plist_value(&mut self, name: &str, value: Value) -> Result<(), LispError> {
        let canonical = self
            .charset_canonical_name(name)
            .ok_or_else(|| LispError::Void(name.to_string()))?;
        if let Some((_, existing)) = self
            .charset_plists
            .iter_mut()
            .rev()
            .find(|(charset, _)| charset == &canonical)
        {
            *existing = value;
        } else {
            self.charset_plists.push((canonical, value));
        }
        Ok(())
    }

    pub fn define_charset_alias(&mut self, alias: &str, target: &str) -> Result<(), LispError> {
        let canonical = self
            .charset_canonical_name(target)
            .ok_or_else(|| LispError::Void(target.to_string()))?;
        if let Some((_, existing)) = self
            .charset_aliases
            .iter_mut()
            .rev()
            .find(|(existing_alias, _)| existing_alias == alias)
        {
            *existing = canonical;
        } else {
            self.charset_aliases.push((alias.to_string(), canonical));
        }
        Ok(())
    }

    pub fn charset_priority_list(&self) -> Vec<String> {
        self.charset_priority.clone()
    }

    pub fn set_charset_priority(&mut self, names: &[String]) {
        let mut reordered = Vec::new();
        for name in names {
            if let Some(canonical) = self.charset_canonical_name(name)
                && !reordered.iter().any(|existing| existing == &canonical)
            {
                reordered.push(canonical);
            }
        }
        for default in ["unicode", "ascii"] {
            if !reordered.iter().any(|existing| existing == default) {
                reordered.push(default.to_string());
            }
        }
        self.charset_priority = reordered;
    }

    pub fn charset_priority_rank(&self, name: &str) -> usize {
        let canonical = self.charset_canonical_name(name).unwrap_or_else(|| name.to_string());
        self.charset_priority
            .iter()
            .position(|existing| existing == &canonical)
            .unwrap_or(usize::MAX)
    }

    pub fn declare_iso_charset(&mut self, dimension: i64, chars: i64, final_char: u32, charset: &str) {
        let canonical = self
            .charset_canonical_name(charset)
            .unwrap_or_else(|| charset.to_string());
        if let Some((_, _, _, existing)) = self
            .iso_charsets
            .iter_mut()
            .rev()
            .find(|(d, c, f, _)| *d == dimension && *c == chars && *f == final_char)
        {
            *existing = canonical;
        } else {
            self.iso_charsets
                .push((dimension, chars, final_char, canonical));
        }
    }

    pub fn iso_charset(&self, dimension: i64, chars: i64, final_char: u32) -> Option<String> {
        self.iso_charsets
            .iter()
            .rev()
            .find(|(d, c, f, _)| *d == dimension && *c == chars && *f == final_char)
            .map(|(_, _, _, charset)| charset.clone())
    }

    pub fn ensure_standard_category_table(&mut self) -> u64 {
        if let Some(id) = self.standard_category_table_id {
            return id;
        }
        let Value::CharTable(id) = self.make_char_table(Some("category-table".into()), Value::String(String::new())) else {
            unreachable!("make_char_table returns a char-table");
        };
        self.standard_category_table_id = Some(id);
        id
    }

    pub fn category_docstring(&self, id: u64, category: u32) -> Option<String> {
        self.find_char_table(id).and_then(|table| {
            table
                .category_docs
                .iter()
                .find(|(ch, _)| *ch == category)
                .map(|(_, doc)| doc.clone())
        })
    }

    pub fn define_category(
        &mut self,
        id: u64,
        category: u32,
        doc: String,
    ) -> Result<(), LispError> {
        let table = self
            .find_char_table_mut(id)
            .ok_or_else(|| LispError::TypeError("char-table".into(), format!("char-table<{id}>")))?;
        if table.category_docs.iter().any(|(ch, _)| *ch == category) {
            return Err(LispError::Signal("Category already defined".into()));
        }
        table.category_docs.push((category, doc));
        Ok(())
    }

    pub fn detach_markers_for_buffer(&mut self, buffer_id: u64) {
        for marker in &mut self.markers {
            if marker.buffer_id == Some(buffer_id) {
                marker.last_position = marker.position.or(marker.last_position);
                marker.position = None;
                marker.buffer_id = None;
            }
        }
    }

    pub fn adjust_markers_for_insert(
        &mut self,
        buffer_id: u64,
        pos: usize,
        nchars: usize,
        before_markers: bool,
    ) {
        if nchars == 0 {
            return;
        }
        for marker in &mut self.markers {
            if marker.buffer_id != Some(buffer_id) {
                continue;
            }
            let Some(position) = marker.position else {
                continue;
            };
            if position > pos || (position == pos && (before_markers || marker.insertion_type)) {
                let new_pos = position + nchars;
                marker.position = Some(new_pos);
                marker.last_position = Some(new_pos);
            }
        }
    }

    pub fn adjust_markers_for_delete(&mut self, buffer_id: u64, from: usize, to: usize) {
        if from >= to {
            return;
        }
        let nchars = to - from;
        for marker in &mut self.markers {
            if marker.buffer_id != Some(buffer_id) {
                continue;
            }
            let Some(position) = marker.position else {
                continue;
            };
            let new_pos = if position > to {
                position - nchars
            } else if position > from {
                from
            } else {
                position
            };
            marker.position = Some(new_pos);
            marker.last_position = Some(new_pos);
        }
    }

    pub fn live_marker_positions_for_buffer(&self, buffer_id: u64) -> Vec<(u64, Option<usize>)> {
        self.markers
            .iter()
            .filter(|marker| marker.buffer_id == Some(buffer_id))
            .map(|marker| (marker.id, marker.position))
            .collect()
    }

    pub fn change_hooks_are_running(&self) -> bool {
        self.change_hooks_running > 0
    }

    pub fn enter_change_hooks(&mut self) {
        self.change_hooks_running += 1;
    }

    pub fn leave_change_hooks(&mut self) {
        self.change_hooks_running = self.change_hooks_running.saturating_sub(1);
    }

    pub fn buffer_local_hook(&self, buffer_id: u64, hook_name: &str) -> Option<Vec<Value>> {
        self.buffer_local_hooks
            .iter()
            .find(|(id, name, _)| *id == buffer_id && name == hook_name)
            .map(|(_, _, hooks)| hooks.clone())
    }

    pub fn set_buffer_local_hook(
        &mut self,
        buffer_id: u64,
        hook_name: &str,
        hooks: Vec<Value>,
    ) {
        if let Some((_, _, existing)) = self
            .buffer_local_hooks
            .iter_mut()
            .find(|(id, name, _)| *id == buffer_id && name == hook_name)
        {
            *existing = hooks;
        } else {
            self.buffer_local_hooks
                .push((buffer_id, hook_name.to_string(), hooks));
        }
    }

    pub fn buffer_local_value(&self, buffer_id: u64, name: &str) -> Option<Value> {
        self.buffer_locals
            .iter()
            .rev()
            .find(|(id, var, _)| *id == buffer_id && var == name)
            .map(|(_, _, value)| value.clone())
    }

    pub fn set_buffer_local_value(&mut self, buffer_id: u64, name: &str, value: Value) {
        for (id, var, existing) in self.buffer_locals.iter_mut().rev() {
            if *id == buffer_id && var == name {
                *existing = value;
                return;
            }
        }
        self.buffer_locals
            .push((buffer_id, name.to_string(), value));
    }

    pub fn buffer_local_variables(&self, buffer_id: u64) -> Vec<(String, Value)> {
        let mut vars = Vec::new();
        for (id, name, value) in &self.buffer_locals {
            if *id == buffer_id && !vars.iter().any(|(existing, _)| existing == name) {
                vars.push((name.clone(), value.clone()));
            }
        }
        vars
    }

    pub fn mark_auto_buffer_local(&mut self, name: &str) {
        if !self.auto_buffer_locals.iter().any(|existing| existing == name) {
            self.auto_buffer_locals.push(name.to_string());
        }
    }

    pub fn is_auto_buffer_local(&self, name: &str) -> bool {
        self.auto_buffer_locals.iter().any(|existing| existing == name)
    }

    pub fn effective_labeled_restriction(
        &self,
        buffer_id: u64,
        skip_label: Option<&str>,
    ) -> Option<(usize, usize)> {
        let mut result: Option<(usize, usize)> = None;
        for (id, label, start, end) in &self.labeled_restrictions {
            if *id != buffer_id || skip_label == Some(label.as_str()) {
                continue;
            }
            result = Some(match result {
                Some((cur_start, cur_end)) => (cur_start.max(*start), cur_end.min(*end)),
                None => (*start, *end),
            });
        }
        result
    }

    pub fn register_indirect_buffer(&mut self, buffer_id: u64, base_id: u64) {
        self.indirect_buffers.push((buffer_id, base_id));
    }

    pub fn buffer_base_id(&self, buffer_id: u64) -> Option<u64> {
        self.indirect_buffers
            .iter()
            .find(|(id, _)| *id == buffer_id)
            .map(|(_, base_id)| *base_id)
    }

    pub fn root_buffer_id(&self, mut buffer_id: u64) -> u64 {
        while let Some(base_id) = self.buffer_base_id(buffer_id) {
            buffer_id = base_id;
        }
        buffer_id
    }

    pub fn related_buffer_ids(&self, buffer_id: u64) -> Vec<u64> {
        let root = self.root_buffer_id(buffer_id);
        self.buffer_list
            .iter()
            .map(|(id, _)| *id)
            .filter(|id| self.root_buffer_id(*id) == root)
            .collect()
    }

    fn mirror_insert_to_related_buffers(
        &mut self,
        related: &[u64],
        pos: usize,
        s: &str,
        props: Option<Vec<(String, Value)>>,
        before_markers: bool,
        inherit: bool,
    ) {
        let current_id = self.current_buffer_id();
        let nchars = s.chars().count();
        for buffer_id in related {
            if *buffer_id == current_id {
                continue;
            }
            if let Some(buffer) = self.get_buffer_by_id_mut(*buffer_id) {
                let saved_point = buffer.point();
                buffer.goto_char(pos);
                if inherit {
                    buffer.insert_and_inherit(s);
                } else if let Some(props) = props.clone() {
                    buffer.insert_with_properties(s, Some(props));
                } else {
                    buffer.insert(s);
                }
                let restored = if saved_point > pos || (saved_point == pos && before_markers) {
                    saved_point + nchars
                } else {
                    saved_point
                };
                buffer.goto_char(restored);
            }
            self.adjust_markers_for_insert(*buffer_id, pos, nchars, before_markers);
        }
    }

    fn mirror_delete_to_related_buffers(&mut self, related: &[u64], from: usize, to: usize) {
        let current_id = self.current_buffer_id();
        for buffer_id in related {
            if *buffer_id == current_id {
                continue;
            }
            if let Some(buffer) = self.get_buffer_by_id_mut(*buffer_id) {
                let saved_point = buffer.point();
                let _ = buffer.delete_region(from, to);
                let restored = if saved_point > to {
                    saved_point - (to - from)
                } else if saved_point > from {
                    from
                } else {
                    saved_point
                };
                buffer.goto_char(restored);
            }
            self.adjust_markers_for_delete(*buffer_id, from, to);
        }
    }

    pub fn insert_current_buffer(&mut self, s: &str) {
        let pos = self.buffer.point();
        let nchars = s.chars().count();
        let related = self.related_buffer_ids(self.current_buffer_id());
        self.buffer.insert(s);
        self.adjust_markers_for_insert(self.current_buffer_id(), pos, nchars, false);
        self.mirror_insert_to_related_buffers(&related, pos, s, None, false, false);
    }

    pub fn insert_current_buffer_with_properties(
        &mut self,
        s: &str,
        props: Option<Vec<(String, Value)>>,
    ) {
        let pos = self.buffer.point();
        let nchars = s.chars().count();
        let related = self.related_buffer_ids(self.current_buffer_id());
        self.buffer.insert_with_properties(s, props.clone());
        self.adjust_markers_for_insert(self.current_buffer_id(), pos, nchars, false);
        self.mirror_insert_to_related_buffers(&related, pos, s, props, false, false);
    }

    pub fn insert_current_buffer_and_inherit(&mut self, s: &str) {
        let pos = self.buffer.point();
        let nchars = s.chars().count();
        let related = self.related_buffer_ids(self.current_buffer_id());
        self.buffer.insert_and_inherit(s);
        self.adjust_markers_for_insert(self.current_buffer_id(), pos, nchars, false);
        self.mirror_insert_to_related_buffers(&related, pos, s, None, false, true);
    }

    pub fn insert_current_buffer_before_markers(&mut self, s: &str) {
        let pos = self.buffer.point();
        let nchars = s.chars().count();
        let related = self.related_buffer_ids(self.current_buffer_id());
        self.buffer.insert(s);
        self.adjust_markers_for_insert(self.current_buffer_id(), pos, nchars, true);
        self.mirror_insert_to_related_buffers(&related, pos, s, None, true, false);
    }

    pub fn insert_current_buffer_before_markers_and_inherit(&mut self, s: &str) {
        let pos = self.buffer.point();
        let nchars = s.chars().count();
        let related = self.related_buffer_ids(self.current_buffer_id());
        self.buffer.insert_and_inherit(s);
        self.adjust_markers_for_insert(self.current_buffer_id(), pos, nchars, true);
        self.mirror_insert_to_related_buffers(&related, pos, s, None, true, true);
    }

    pub fn delete_region_current_buffer(
        &mut self,
        from: usize,
        to: usize,
    ) -> Result<String, crate::buffer::BufferError> {
        let from = from.max(self.buffer.point_min());
        let to = to.min(self.buffer.point_max());
        let affected_markers = self.affected_markers_for_delete(self.current_buffer_id(), from, to);
        let related = self.related_buffer_ids(self.current_buffer_id());
        let deleted = self.buffer.delete_region(from, to)?;
        self.buffer.attach_markers_to_last_delete(affected_markers);
        self.adjust_markers_for_delete(self.current_buffer_id(), from, to);
        self.mirror_delete_to_related_buffers(&related, from, to);
        Ok(deleted)
    }

    pub fn delete_char_current_buffer(
        &mut self,
        n: isize,
    ) -> Result<String, crate::buffer::BufferError> {
        if n >= 0 {
            let from = self.buffer.point();
            let to = from + n as usize;
            if to > self.buffer.point_max() {
                return Err(crate::buffer::BufferError::EndOfBuffer);
            }
            self.delete_region_current_buffer(from, to)
        } else {
            let count = (-n) as usize;
            let to = self.buffer.point();
            if to < self.buffer.point_min() + count {
                return Err(crate::buffer::BufferError::BeginningOfBuffer);
            }
            let from = to - count;
            self.delete_region_current_buffer(from, to)
        }
    }

    pub fn undo_current_buffer(&mut self) -> Result<(), LispError> {
        let region = if self.buffer.mark_active() {
            self.buffer.region()
        } else {
            None
        };
        if region.is_none() {
            let redo_groups = self
                .undo_sequence
                .as_ref()
                .filter(|state| !state.had_error && !state.redo_groups.is_empty())
                .map(|state| state.redo_groups.clone());
            if let Some(redo_groups) = redo_groups {
                for group in redo_groups.iter().rev() {
                    self.replay_sequence_group(group)?;
                }
                if let Some(state) = self.undo_sequence.as_mut() {
                    state.undone_count = 1;
                    state.redo_groups.clear();
                }
                return Ok(());
            }
            if self
                .undo_sequence
                .as_ref()
                .is_some_and(|state| !state.had_error && state.redo_groups.is_empty())
            {
                self.start_undo_sequence_step()?;
                return self.undo_more_current_buffer();
            }
            return self.start_undo_sequence_step();
        }
        let group = self
            .buffer
            .take_undo_group(region)
            .map_err(|error| LispError::Signal(error.to_string()))?;
        self.buffer.push_undo_boundary();
        for entry in group.iter().rev() {
            self.apply_current_buffer_undo_entry(entry)?;
        }
        Ok(())
    }

    pub fn undo_more_current_buffer(&mut self) -> Result<(), LispError> {
        if self.undo_sequence.is_none() {
            return self.start_undo_sequence_step();
        }
        let group = {
            let state = self.undo_sequence.as_ref().expect("checked above");
            if state.undone_count >= state.original_groups.len() {
                return Err(LispError::Signal(
                    crate::buffer::BufferError::NoFurtherUndoInformation.to_string(),
                ));
            }
            let start = state.original_groups.len() - 1 - state.undone_count;
            state.original_groups[start].clone()
        };
        let before = self.buffer.undo_entries().len();
        if let Err(error) = self.replay_sequence_group(&group) {
            if let Some(state) = self.undo_sequence.as_mut() {
                state.had_error = true;
            }
            return Err(error);
        }
        let state = self.undo_sequence.as_mut().expect("sequence active");
        state
            .redo_groups
            .push(latest_generated_undo_group(&self.buffer.undo_entries()[before..]));
        state.undone_count += 1;
        state.had_error = false;
        Ok(())
    }

    pub fn reset_undo_sequence(&mut self) {
        self.undo_sequence = None;
    }

    fn start_undo_sequence_step(&mut self) -> Result<(), LispError> {
        let original_groups = self.buffer.undo_groups();
        let group = original_groups
            .last()
            .cloned()
            .ok_or_else(|| {
                LispError::Signal(crate::buffer::BufferError::NoFurtherUndoInformation.to_string())
            })?;
        self.replay_sequence_group(&group)?;
        self.undo_sequence = Some(UndoSequenceState {
            original_groups,
            undone_count: 1,
            redo_groups: Vec::new(),
            had_error: false,
        });
        Ok(())
    }

    fn replay_sequence_group(
        &mut self,
        group: &[crate::buffer::UndoEntry],
    ) -> Result<(), LispError> {
        for entry in group.iter().rev() {
            self.apply_current_buffer_undo_entry(entry)?;
        }
        Ok(())
    }

    fn apply_current_buffer_undo_entry(
        &mut self,
        entry: &crate::buffer::UndoEntry,
    ) -> Result<(), LispError> {
        match entry {
            crate::buffer::UndoEntry::Insert { pos, len } => {
                self.buffer.goto_char(*pos);
                self.delete_region_current_buffer(*pos, *pos + *len)
                    .map_err(|error| LispError::Signal(error.to_string()))?;
                Ok(())
            }
            crate::buffer::UndoEntry::Delete {
                pos,
                text,
                props,
                markers,
            } => {
                self.buffer.goto_char(*pos);
                let insert_at = self.buffer.point();
                self.insert_current_buffer(text);
                for span in props {
                    self.buffer.add_text_properties(
                        insert_at + span.start,
                        insert_at + span.end,
                        &span.props,
                    );
                }
                let inserted = text.chars().count();
                for marker in markers {
                    let expected_auto_pos = match self.marker_insertion_type(marker.id) {
                        Some(true) => marker.collapsed_pos + inserted,
                        _ => marker.collapsed_pos,
                    };
                    if self.marker_buffer_id(marker.id) == Some(self.current_buffer_id())
                        && self.marker_position(marker.id) == Some(expected_auto_pos)
                    {
                        let _ = self.set_marker(
                            marker.id,
                            Some(marker.original_pos),
                            Some(self.current_buffer_id()),
                        );
                    }
                }
                Ok(())
            }
            crate::buffer::UndoEntry::Combined { entries, .. } => {
                for inner in entries.iter().rev() {
                    self.apply_current_buffer_undo_entry(inner)?;
                }
                Ok(())
            }
            crate::buffer::UndoEntry::Opaque(value) => Err(LispError::Signal(format!(
                "Unrecognized entry in undo list {}",
                render_undo_value(value)
            ))),
            crate::buffer::UndoEntry::Boundary => Ok(()),
        }
    }

    fn affected_markers_for_delete(
        &self,
        buffer_id: u64,
        from: usize,
        to: usize,
    ) -> Vec<crate::buffer::UndoMarker> {
        self.markers
            .iter()
            .filter(|marker| marker.buffer_id == Some(buffer_id))
            .filter_map(|marker| {
                let pos = marker.position?;
                if pos >= from && pos <= to {
                    Some(crate::buffer::UndoMarker {
                        id: marker.id,
                        original_pos: pos,
                        collapsed_pos: from,
                    })
                } else {
                    None
                }
            })
            .collect()
    }

    /// Borrow a live buffer by ID.
    pub fn get_buffer_by_id(&self, id: u64) -> Option<&crate::buffer::Buffer> {
        if id == self.current_buffer_id {
            Some(&self.buffer)
        } else {
            self.inactive_buffers
                .iter()
                .find(|(buffer_id, _)| *buffer_id == id)
                .map(|(_, buffer)| buffer)
        }
    }

    /// Borrow a live buffer mutably by ID.
    pub fn get_buffer_by_id_mut(&mut self, id: u64) -> Option<&mut crate::buffer::Buffer> {
        if id == self.current_buffer_id {
            Some(&mut self.buffer)
        } else {
            self.inactive_buffers
                .iter_mut()
                .find(|(buffer_id, _)| *buffer_id == id)
                .map(|(_, buffer)| buffer)
        }
    }

    pub fn buffer_hooks_inhibited(&self, id: u64) -> bool {
        self.get_buffer_by_id(id)
            .map(|buffer| buffer.inhibit_hooks)
            .unwrap_or(false)
    }

    pub fn set_buffer_hooks_inhibited(&mut self, id: u64, inhibit: bool) {
        if let Some(buffer) = self.get_buffer_by_id_mut(id) {
            buffer.inhibit_hooks = inhibit;
        }
    }

    pub fn swap_buffer_text_state(&mut self, left_id: u64, right_id: u64) -> Result<(), LispError> {
        if left_id == right_id {
            return Ok(());
        }
        if left_id == self.current_buffer_id {
            let pos = self
                .inactive_buffers
                .iter()
                .position(|(buffer_id, _)| *buffer_id == right_id)
                .ok_or_else(|| LispError::Signal(format!("No buffer with id {}", right_id)))?;
            let (buffer, inactive_buffers) = (&mut self.buffer, &mut self.inactive_buffers);
            buffer.swap_text_state(&mut inactive_buffers[pos].1);
            return Ok(());
        }
        if right_id == self.current_buffer_id {
            let pos = self
                .inactive_buffers
                .iter()
                .position(|(buffer_id, _)| *buffer_id == left_id)
                .ok_or_else(|| LispError::Signal(format!("No buffer with id {}", left_id)))?;
            let (buffer, inactive_buffers) = (&mut self.buffer, &mut self.inactive_buffers);
            buffer.swap_text_state(&mut inactive_buffers[pos].1);
            return Ok(());
        }

        let left_index = self
            .inactive_buffers
            .iter()
            .position(|(buffer_id, _)| *buffer_id == left_id)
            .ok_or_else(|| LispError::Signal(format!("No buffer with id {}", left_id)))?;
        let right_index = self
            .inactive_buffers
            .iter()
            .position(|(buffer_id, _)| *buffer_id == right_id)
            .ok_or_else(|| LispError::Signal(format!("No buffer with id {}", right_id)))?;
        let (first, second) = if left_index < right_index {
            let (left_slice, right_slice) = self.inactive_buffers.split_at_mut(right_index);
            (&mut left_slice[left_index].1, &mut right_slice[0].1)
        } else {
            let (right_slice, left_slice) = self.inactive_buffers.split_at_mut(left_index);
            (&mut left_slice[0].1, &mut right_slice[right_index].1)
        };
        first.swap_text_state(second);
        Ok(())
    }

    pub fn face_inherit_target(&self, face: &str) -> Option<String> {
        self.face_inheritance
            .iter()
            .rev()
            .find(|(name, _)| name == face)
            .and_then(|(_, inherit)| inherit.clone())
    }

    pub fn set_face_inherit_target(
        &mut self,
        face: &str,
        inherit: Option<String>,
    ) -> Result<(), LispError> {
        if let Some(target) = inherit.as_ref()
            && self.face_inheritance_creates_cycle(face, target)
        {
            return Err(LispError::SignalValue(Value::list([
                Value::Symbol("error".into()),
                Value::String("Face inheritance results in inheritance cycle".into()),
                Value::Symbol(target.clone()),
            ])));
        }
        if let Some((_, slot)) = self
            .face_inheritance
            .iter_mut()
            .rev()
            .find(|(name, _)| name == face)
        {
            *slot = inherit;
        } else {
            self.face_inheritance.push((face.to_string(), inherit));
        }
        Ok(())
    }

    fn face_inheritance_creates_cycle(&self, face: &str, target: &str) -> bool {
        let mut current = Some(target.to_string());
        while let Some(name) = current {
            if name == face {
                return true;
            }
            current = self.face_inherit_target(&name);
        }
        false
    }

    /// Find an overlay by ID in any live buffer.
    pub fn find_overlay(&self, id: u64) -> Option<&crate::overlay::Overlay> {
        self.buffer
            .overlays
            .iter()
            .find(|ov| ov.id == id)
            .or_else(|| {
                self.inactive_buffers.iter().find_map(|(_, buffer)| {
                    buffer.overlays.iter().find(|ov| ov.id == id)
                })
            })
    }

    /// Find a mutable overlay by ID in any live buffer.
    pub fn find_overlay_mut(&mut self, id: u64) -> Option<&mut crate::overlay::Overlay> {
        if let Some(overlay) = self.buffer.overlays.iter_mut().find(|ov| ov.id == id) {
            return Some(overlay);
        }
        self.inactive_buffers
            .iter_mut()
            .find_map(|(_, buffer)| buffer.overlays.iter_mut().find(|ov| ov.id == id))
    }

    /// Remove and return an overlay by ID from any live buffer.
    pub fn take_overlay(&mut self, id: u64) -> Option<crate::overlay::Overlay> {
        if let Some(pos) = self.buffer.overlays.iter().position(|ov| ov.id == id) {
            return Some(self.buffer.overlays.swap_remove(pos));
        }
        for (_, buffer) in &mut self.inactive_buffers {
            if let Some(pos) = buffer.overlays.iter().position(|ov| ov.id == id) {
                return Some(buffer.overlays.swap_remove(pos));
            }
        }
        None
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
        if let Some(value) = self.buffer_local_value(self.current_buffer_id(), name) {
            return Some(value);
        }
        for (k, v) in self.globals.iter().rev() {
            if k == name {
                return Some(v.clone());
            }
        }
        self.builtin_var_value(name)
    }

    fn builtin_var_value(&self, name: &str) -> Option<Value> {
        match name {
            "nil" => Some(Value::Nil),
            "t" => Some(Value::T),
            "float-pi" => Some(Value::Float(std::f64::consts::PI)),
            "most-positive-fixnum" => Some(Value::Integer(i64::MAX)),
            "most-negative-fixnum" => Some(Value::Integer(i64::MIN)),
            "enable-multibyte-characters" => Some(if self.buffer.is_multibyte() {
                Value::T
            } else {
                Value::Nil
            }),
            "buffer-file-name" => Some(
                self.buffer
                    .file
                    .clone()
                    .map(Value::String)
                    .unwrap_or(Value::Nil),
            ),
            "temporary-file-directory" => {
                Some(Value::String(std::env::temp_dir().display().to_string()))
            }
            "source-directory" => Some(Value::String(
                std::env::var("EMACS_TEST_DIRECTORY")
                    .ok()
                    .and_then(|path| {
                        std::path::PathBuf::from(path)
                            .parent()
                            .map(|path| path.display().to_string())
                    })
                    .unwrap_or_else(primitives::default_directory),
            )),
            "default-directory" => Some(Value::String(primitives::default_directory())),
            "tab-width" => Some(Value::Integer(8)),
            "system-type" => Some(Value::Symbol(std::env::consts::OS.replace("macos", "darwin"))),
            "system-configuration-features" => Some(Value::String(
                std::env::var("EMAXX_SYSTEM_CONFIGURATION_FEATURES").unwrap_or_default(),
            )),
            "charset-list" => Some(Value::list(
                self.charset_priority_list()
                    .into_iter()
                    .map(Value::Symbol)
                    .collect::<Vec<_>>(),
            )),
            "ert-resource-directory-format" => Some(Value::String("%s-resources/".into())),
            "ert-resource-directory-trim-left-regexp" => Some(Value::String(String::new())),
            "ert-resource-directory-trim-right-regexp" => {
                Some(Value::String("\\(-tests?\\)?\\.el".into()))
            }
            "load-file-name" | "macroexp-file-name" => Some(
                self.current_load_file
                    .clone()
                    .map(Value::String)
                    .unwrap_or(Value::Nil),
            ),
            "invocation-name" => Some(Value::String(
                primitives::current_invocation_name().unwrap_or_else(|| "emaxx".into()),
            )),
            "invocation-directory" => Some(Value::String(
                primitives::current_invocation_directory()
                    .unwrap_or_else(primitives::default_directory),
            )),
            "process-environment" | "initial-environment" => Some(Value::list(
                std::env::vars()
                    .map(|(name, value)| Value::String(format!("{name}={value}")))
                    .collect::<Vec<_>>(),
            )),
            _ if name.starts_with('.') => Some(Value::Nil),
            _ if name.starts_with(':') => Some(Value::Symbol(name.to_string())),
            _ => None,
        }
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
        if let Some(value) = self.buffer_local_value(self.current_buffer_id(), name) {
            return Ok(value);
        }
        for (k, v) in self.globals.iter().rev() {
            if k == name {
                return Ok(v.clone());
            }
        }
        if name == "buffer-undo-list" {
            return Ok(crate::lisp::primitives::buffer_undo_list_value(&self.buffer));
        }
        self.builtin_var_value(name)
            .ok_or_else(|| LispError::Void(name.to_string()))
    }

    pub fn lookup_function(&self, name: &str, env: &Env) -> Result<Value, LispError> {
        for frame in env.iter().rev() {
            for (k, v) in frame.iter().rev() {
                if k == name && matches!(v, Value::BuiltinFunc(_) | Value::Lambda(_, _, _)) {
                    return Ok(v.clone());
                }
            }
        }
        for (k, v) in self.functions.iter().rev() {
            if k == name {
                return Ok(v.clone());
            }
        }
        if primitives::is_builtin(name) {
            Ok(Value::BuiltinFunc(name.to_string()))
        } else {
            Err(LispError::Void(name.to_string()))
        }
    }

    /// Set a variable in the innermost local frame, or in globals.
    pub fn set_variable(&mut self, name: &str, value: Value, env: &mut Env) {
        if name == "buffer-undo-list" {
            if value.is_nil() {
                self.undo_sequence = None;
                self.buffer.clear_undo_history();
            } else if let Value::Cons(head, tail) = &value
                && **tail == crate::lisp::primitives::buffer_undo_list_value(&self.buffer)
            {
                let entry = buffer_undo_head_to_entry(head);
                self.buffer.push_undo_entry(entry);
            } else {
                self.undo_sequence = None;
            }
            return;
        }
        if self.buffer_local_value(self.current_buffer_id(), name).is_some()
            || self.is_auto_buffer_local(name)
        {
            self.set_buffer_local_value(self.current_buffer_id(), name, value);
            return;
        }
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
            Value::Nil
            | Value::T
            | Value::Integer(_)
            | Value::BigInteger(_)
            | Value::Float(_)
            | Value::String(_)
            | Value::StringObject(_) => {
                Ok(expr.clone())
            }

            Value::BuiltinFunc(_)
            | Value::Lambda(_, _, _)
            | Value::Buffer(_, _)
            | Value::Marker(_)
            | Value::Overlay(_)
            | Value::CharTable(_)
            | Value::Record(_)
            | Value::Finalizer(_) => Ok(expr.clone()),

            Value::Symbol(name) => self.lookup(name, env),

            Value::Cons(_, _) => {
                let items = expr.to_vec()?;
                if items.is_empty() {
                    return Ok(Value::Nil);
                }

                if matches!(items.first(), Some(Value::Symbol(name)) if name == "vector-literal") {
                    return Ok(expr.clone());
                }

                // Check for special forms first
                if let Value::Symbol(ref name) = items[0] {
                    match name.as_str() {
                        "quote" => return self.sf_quote(&items),
                        "if" => return self.sf_if(&items, env),
                        "when" => return self.sf_when(&items, env),
                        "unless" => return self.sf_unless(&items, env),
                        "cond" => return self.sf_cond(&items, env),
                        "pcase-exhaustive" => return self.sf_pcase_exhaustive(&items, env),
                        "and" => return self.sf_and(&items, env),
                        "or" => return self.sf_or(&items, env),
                        "not" => return self.sf_not(&items, env),
                        "progn" => return self.sf_progn(&items[1..], env),
                        "prog1" => return self.sf_prog1(&items, env),
                        "let" => return self.sf_let(&items, env),
                        "let*" => return self.sf_letstar(&items, env),
                        "pcase-let" => return self.sf_pcase_let(&items, env, false),
                        "pcase-let*" => return self.sf_pcase_let(&items, env, true),
                        "let-alist" => return self.sf_let_alist(&items, env),
                        "setq" => return self.sf_setq(&items, env),
                        "setq-local" => return self.sf_setq_local(&items, env),
                        "incf" | "cl-incf" => return self.sf_incf(&items, env, 1),
                        "decf" | "cl-decf" => return self.sf_incf(&items, env, -1),
                        "defvar" | "defconst" => return self.sf_defvar(&items, env),
                        "defvar-local" => return self.sf_defvar_local(&items, env),
                        "defun" | "defsubst" => return self.sf_defun(&items, env),
                        "defmacro" => return self.sf_defmacro(&items),
                        "defalias" => return self.sf_defalias(&items, env),
                        "backquote" => return self.eval_backquote(&items[1], env),
                        "lambda" => return self.sf_lambda(&items, env),
                        "function" | "function-quote" => {
                            // #'foo or (function foo)
                            if items.len() >= 2 {
                                if let Value::Symbol(name) = &items[1] {
                                    return self.lookup_function(name, env);
                                }
                                return self.eval(&items[1], env);
                            }
                            return Ok(Value::Nil);
                        }
                        "while" => return self.sf_while(&items, env),
                        "dolist" => return self.sf_dolist(&items, env),
                        "pcase-dolist" => return self.sf_pcase_dolist(&items, env),
                        "dotimes" => return self.sf_dotimes(&items, env),
                        "cl-loop" => return self.sf_cl_loop(&items, env),
                        "unwind-protect" => return self.sf_unwind_protect(&items, env),
                        "condition-case" => return self.sf_condition_case(&items, env),
                        "cl-assert" => return self.sf_cl_assert(&items, env),
                        "with-temp-buffer" => return self.sf_with_temp_buffer(&items, env),
                        "with-output-to-string" => {
                            return self.sf_with_output_to_string(&items, env)
                        }
                        "ert-with-temp-file" => return self.sf_ert_with_temp_file(&items, env),
                        "with-current-buffer" => return self.sf_with_current_buffer(&items, env),
                        "with-restriction" => return self.sf_with_restriction(&items, env),
                        "without-restriction" => {
                            return self.sf_without_restriction(&items, env)
                        }
                        "with-selected-window" => return self.sf_progn(&items[2..], env),
                        "save-excursion" => return self.sf_save_excursion(&items, env),
                        "save-restriction" => return self.sf_save_restriction(&items, env),
                        "with-silent-modifications" => {
                            return self.sf_with_silent_modifications(&items, env)
                        }
                        "combine-change-calls" => {
                            return self.sf_combine_change_calls(&items, env)
                        }
                        "cl-destructuring-bind" => {
                            return self.sf_cl_destructuring_bind(&items, env)
                        }
                        "cl-letf" => return self.sf_cl_letf(&items, env),
                        "aset" => return self.sf_aset(&items, env),
                        "cl-flet" | "cl-labels" => {
                            return self.sf_cl_flet(&items, env)
                        }
                        "cl-macrolet" => {
                            return self.sf_cl_macrolet(&items, env)
                        }
                        "push" => {
                            // (push NEWELT PLACE)
                            if items.len() < 3 {
                                return Err(LispError::WrongNumberOfArgs(
                                    "push".into(),
                                    items.len() - 1,
                                ));
                            }
                            let val = self.eval(&items[1], env)?;
                            match &items[2] {
                                Value::Symbol(name) => {
                                    let cur = self.lookup(name, env)?;
                                    let new_val = Value::cons(val, cur);
                                    self.set_variable(name, new_val.clone(), env);
                                    return Ok(new_val);
                                }
                                Value::Cons(_, _) => {
                                    let place_items = items[2].to_vec()?;
                                    if place_items.len() == 3
                                        && matches!(
                                            &place_items[0],
                                            Value::Symbol(symbol) if symbol == "overlay-get"
                                        )
                                    {
                                        let overlay = self.eval(&place_items[1], env)?;
                                        let prop = self.eval(&place_items[2], env)?;
                                        let overlay_id = match overlay {
                                            Value::Overlay(id) => id,
                                            other => {
                                                return Err(LispError::TypeError(
                                                    "overlay".into(),
                                                    other.type_name(),
                                                ))
                                            }
                                        };
                                        let prop_name = prop.as_symbol()?.to_string();
                                        let cur = self
                                            .find_overlay(overlay_id)
                                            .and_then(|overlay| overlay.get_prop(&prop_name).cloned())
                                            .unwrap_or(Value::Nil);
                                        let new_val = Value::cons(val, cur);
                                        if let Some(overlay) = self.find_overlay_mut(overlay_id) {
                                            overlay.put_prop(&prop_name, new_val.clone());
                                        }
                                        return Ok(new_val);
                                    }
                                }
                                _ => {}
                            }
                            return Err(LispError::Signal("Unsupported push place".into()));
                        }
                        "pop" => {
                            // (pop PLACE)
                            if items.len() < 2 {
                                return Err(LispError::WrongNumberOfArgs(
                                    "pop".into(),
                                    items.len() - 1,
                                ));
                            }
                            let name = items[1].as_symbol()?.to_string();
                            let cur = self.lookup(&name, env)?;
                            let result = cur.car()?;
                            let rest = cur.cdr()?;
                            self.set_variable(&name, rest, env);
                            return Ok(result);
                        }
                        "catch" => return self.sf_catch(&items, env),
                        "add-to-list" => return self.sf_add_to_list(&items, env),
                        "ert-deftest" => return self.sf_ert_deftest(&items),
                        "should" => return self.sf_should(&items, env),
                        "should-not" => return self.sf_should_not(&items, env),
                        "should-error" => return self.sf_should_error(&items, env),
                        "skip-unless" => return self.sf_skip_unless(&items, env),
                        "skip-when" => return self.sf_skip_when(&items, env),
                        "rx" => return self.sf_rx(&items),
                        "require" => {
                            if let Some(feature) = items.get(1).and_then(feature_name) {
                                return self.require_feature(&feature);
                            }
                            return Ok(Value::Nil);
                        }
                        "provide" => {
                            if let Some(feature) = items.get(1).and_then(feature_name) {
                                self.provide_feature(&feature);
                                return Ok(Value::Symbol(feature));
                            }
                            return Ok(Value::Nil);
                        }
                        "declare" | "declare-function" => return Ok(Value::Nil),
                        "eval-and-compile" | "eval-when-compile" => {
                            return self.sf_progn(&items[1..], env);
                        }
                        "ert-info" => {
                            // (ert-info (msg) body...) — just run the body
                            return self.sf_progn(&items[2..], env);
                        }
                        "minibuffer-with-setup-hook" => {
                            if items.len() < 3 {
                                return Err(LispError::WrongNumberOfArgs(
                                    "minibuffer-with-setup-hook".into(),
                                    items.len().saturating_sub(1),
                                ));
                            }
                            let hook = self.eval(&items[1], env)?;
                            let call = vec![hook];
                            self.eval_call(&call, env)?;
                            return self.sf_progn(&items[2..], env);
                        }
                        _ => {}
                    }
                }

                // Check for macro expansion
                if let Value::Symbol(name) = &items[0]
                    && let Some(expanded) = self.try_macroexpand(name, &items[1..], env)?
                {
                    return self.eval(&expanded, env);
                }

                // Regular function call
                self.eval_call(&items, env)
            }
        }
    }

    fn eval_call(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        let func = if let Value::Symbol(name) = &items[0] {
            self.lookup_function(name, env)?
        } else {
            self.eval(&items[0], env)?
        };
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

                let mut captured_frames = 0;
                for captured in closure_env.iter().rev() {
                    env.insert(0, captured.clone());
                    captured_frames += 1;
                }
                env.push(frame);
                let result = self.sf_progn(function_executable_body(body), env);
                env.pop();
                for _ in 0..captured_frames {
                    env.remove(0);
                }
                result
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

    fn sf_pcase_exhaustive(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Ok(Value::Nil);
        }
        let value = self.eval(&items[1], env)?;
        for clause in &items[2..] {
            let clause_items = clause.to_vec()?;
            if clause_items.is_empty() {
                continue;
            }
            let pattern = match &clause_items[0] {
                Value::Cons(_, _) => {
                    let parts = clause_items[0].to_vec()?;
                    if matches!(parts.first(), Some(Value::Symbol(name)) if name == "backquote") {
                        parts.get(1).cloned().unwrap_or(Value::Nil)
                    } else {
                        clause_items[0].clone()
                    }
                }
                other => other.clone(),
            };
            if pattern == value {
                return self.sf_progn(&clause_items[1..], env);
            }
        }
        Err(LispError::Signal("pcase-exhaustive: no matching clause".into()))
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

    fn sf_catch(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Err(LispError::WrongNumberOfArgs("catch".into(), 0));
        }
        let tag = self.eval(&items[1], env)?;
        match self.sf_progn(&items[2..], env) {
            Ok(value) => Ok(value),
            Err(LispError::Throw(thrown_tag, value)) if thrown_tag == tag => Ok(value),
            Err(error) => Err(error),
        }
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
                    let frame = env.last_mut().expect("env frame just pushed");
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
                    let frame = env.last_mut().expect("env frame just pushed");
                    frame.push((name, val));
                }
                _ => return Err(LispError::ReadError("bad let* binding".into())),
            }
        }

        let result = self.sf_progn(&items[2..], env);
        env.pop();
        result
    }

    fn sf_pcase_let(
        &mut self,
        items: &[Value],
        env: &mut Env,
        sequential: bool,
    ) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Ok(Value::Nil);
        }
        let bindings = items[1].to_vec()?;
        if sequential {
            env.push(Vec::new());
            for binding in &bindings {
                let parts = binding.to_vec()?;
                if parts.len() < 2 {
                    return Err(LispError::ReadError("bad pcase-let* binding".into()));
                }
                let value = self.eval(&parts[1], env)?;
                let mut frame_bindings = Vec::new();
                if !pcase_pattern_bindings(&parts[0], &value, &mut frame_bindings)? {
                    env.pop();
                    return Err(LispError::Signal("pcase-let*: no matching clause".into()));
                }
                let frame = env.last_mut().expect("env frame just pushed");
                frame.extend(frame_bindings);
            }
            let result = self.sf_progn(&items[2..], env);
            env.pop();
            return result;
        }

        let mut frame = Vec::new();
        for binding in &bindings {
            let parts = binding.to_vec()?;
            if parts.len() < 2 {
                return Err(LispError::ReadError("bad pcase-let binding".into()));
            }
            let value = self.eval(&parts[1], env)?;
            if !pcase_pattern_bindings(&parts[0], &value, &mut frame)? {
                return Err(LispError::Signal("pcase-let: no matching clause".into()));
            }
        }
        env.push(frame);
        let result = self.sf_progn(&items[2..], env);
        env.pop();
        result
    }

    fn sf_let_alist(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Ok(Value::Nil);
        }
        let alist = self.eval(&items[1], env)?;
        let mut frame = Vec::new();
        for entry in alist.to_vec().unwrap_or_default() {
            let Value::Cons(car, cdr) = entry else {
                continue;
            };
            let Ok(symbol) = car.as_symbol() else {
                continue;
            };
            let value = match *cdr {
                Value::Cons(value, tail) if matches!(*tail, Value::Nil) => *value,
                other => other,
            };
            frame.push((format!(".{symbol}"), value));
        }
        env.push(frame);
        let result = self.sf_progn(&items[2..], env);
        env.pop();
        result
    }

    fn sf_setq(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        self.sf_setq_internal(items, env, false)
    }

    fn sf_setq_local(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        self.sf_setq_internal(items, env, true)
    }

    fn sf_setq_internal(
        &mut self,
        items: &[Value],
        env: &mut Env,
        local_only: bool,
    ) -> Result<Value, LispError> {
        let mut result = Value::Nil;
        let mut i = 1;
        while i + 1 < items.len() {
            let name = items[i].as_symbol()?.to_string();
            let val = self.eval(&items[i + 1], env)?;
            result = val.clone();
            if local_only {
                self.set_buffer_local_value(self.current_buffer_id(), &name, val);
            } else {
                self.set_variable(&name, val, env);
            }
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

    fn sf_defvar_local(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() >= 2 {
            self.mark_auto_buffer_local(items[1].as_symbol()?);
        }
        self.sf_defvar(items, env)
    }

    fn sf_incf(&mut self, items: &[Value], env: &mut Env, sign: i64) -> Result<Value, LispError> {
        if items.len() < 2 || items.len() > 3 {
            return Err(LispError::WrongNumberOfArgs(
                if sign >= 0 { "incf".into() } else { "decf".into() },
                items.len().saturating_sub(1),
            ));
        }
        let name = items[1].as_symbol()?.to_string();
        let delta = if let Some(amount) = items.get(2) {
            self.eval(amount, env)?
        } else {
            Value::Integer(1)
        };
        let current = self.lookup(&name, env)?;
        let updated = primitives::call(
            self,
            if sign >= 0 { "+" } else { "-" },
            &[current, delta],
            env,
        )?;
        self.set_variable(&name, updated.clone(), env);
        Ok(updated)
    }

    fn sf_add_to_list(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Err(LispError::WrongNumberOfArgs(
                "add-to-list".into(),
                items.len().saturating_sub(1),
            ));
        }

        let place = quoted_symbol_name(&items[1]).ok_or_else(|| {
            LispError::TypeError("symbol".into(), unquote(&items[1]).type_name())
        })?;
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
        if values.iter().any(|existing| existing == &value) {
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
        self.functions.push((name.clone(), lambda));
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
            let frame = env.last_mut().expect("env frame just pushed");
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

    fn sf_pcase_dolist(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        let spec = items[1].to_vec()?;
        if spec.len() < 2 {
            return Err(LispError::WrongNumberOfArgs(
                "pcase-dolist".into(),
                items.len().saturating_sub(1),
            ));
        }
        let pattern = &spec[0];
        let list_val = self.eval(&spec[1], env)?;
        let list_items = list_val.to_vec()?;

        for item in list_items {
            let mut bindings = Vec::new();
            if !pcase_pattern_bindings(pattern, &item, &mut bindings)? {
                return Err(LispError::Signal("pcase-dolist: no matching clause".into()));
            }
            env.push(bindings);
            self.sf_progn(&items[2..], env)?;
            env.pop();
        }

        if spec.len() > 2 {
            self.eval(&spec[2], env)
        } else {
            Ok(Value::Nil)
        }
    }

    fn sf_dotimes(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        let spec = items[1].to_vec()?;
        let var_name = spec[0].as_symbol()?.to_string();
        let count = self.eval(&spec[1], env)?.as_integer()?;

        env.push(vec![(var_name.clone(), Value::Integer(0))]);
        for i in 0..count {
            let frame = env.last_mut().expect("env frame just pushed");
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

    fn sf_cl_loop(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        enum LoopSpec {
            Range(String, Vec<Value>),
            List(String, Vec<Value>),
        }

        if items.len() < 5 {
            return Err(LispError::WrongNumberOfArgs(
                "cl-loop".into(),
                items.len().saturating_sub(1),
            ));
        }

        let mut specs = Vec::new();
        let mut index = 1;
        while index < items.len() && matches!(&items[index], Value::Symbol(s) if s == "for") {
            let var_name = items
                .get(index + 1)
                .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?
                .as_symbol()?
                .to_string();
            match items.get(index + 2) {
                Some(Value::Symbol(kind)) if kind == "from" => {
                    let start = self
                        .eval(
                            items
                                .get(index + 3)
                                .ok_or_else(|| {
                                    LispError::Signal("Unsupported cl-loop syntax".into())
                                })?,
                            env,
                        )?
                        .as_integer()?;
                    if !matches!(items.get(index + 4), Some(Value::Symbol(s)) if s == "to") {
                        return Err(LispError::Signal("Unsupported cl-loop syntax".into()));
                    }
                    let end = self
                        .eval(
                            items
                                .get(index + 5)
                                .ok_or_else(|| {
                                    LispError::Signal("Unsupported cl-loop syntax".into())
                                })?,
                            env,
                        )?
                        .as_integer()?;
                    let values = if start <= end {
                        (start..=end).map(Value::Integer).collect()
                    } else {
                        Vec::new()
                    };
                    specs.push(LoopSpec::Range(var_name, values));
                    index += 6;
                }
                Some(Value::Symbol(kind)) if kind == "in" => {
                    let values = self
                        .eval(
                            items
                                .get(index + 3)
                                .ok_or_else(|| {
                                    LispError::Signal("Unsupported cl-loop syntax".into())
                                })?,
                            env,
                        )?
                        .to_vec()?;
                    specs.push(LoopSpec::List(var_name, values));
                    index += 4;
                }
                _ => return Err(LispError::Signal("Unsupported cl-loop syntax".into())),
            }
        }

        if specs.is_empty() || index >= items.len() {
            return Err(LispError::Signal("Unsupported cl-loop syntax".into()));
        }

        let lengths: Vec<usize> = specs
            .iter()
            .map(|spec| match spec {
                LoopSpec::Range(_, values) | LoopSpec::List(_, values) => values.len(),
            })
            .collect();
        let iterations = lengths.into_iter().min().unwrap_or(0);
        let bindings = specs
            .iter()
            .map(|spec| match spec {
                LoopSpec::Range(name, _) | LoopSpec::List(name, _) => (name.clone(), Value::Nil),
            })
            .collect::<Vec<_>>();
        env.push(bindings);

        let mut result = Value::Nil;
        match items.get(index) {
            Some(Value::Symbol(kind)) if kind == "do" => {
                for iteration in 0..iterations {
                    let frame = env.last_mut().expect("env frame just pushed");
                    for (slot, spec) in frame.iter_mut().zip(&specs) {
                        *slot = match spec {
                            LoopSpec::Range(name, values) | LoopSpec::List(name, values) => {
                                (name.clone(), values[iteration].clone())
                            }
                        };
                    }
                    result = self.sf_progn(&items[index + 1..], env)?;
                }
            }
            Some(Value::Symbol(kind)) if kind == "thereis" => {
                let thereis_expr = items
                    .get(index + 1)
                    .ok_or_else(|| LispError::Signal("Unsupported cl-loop syntax".into()))?;
                let until_expr =
                    if matches!(items.get(index + 2), Some(Value::Symbol(s)) if s == "until") {
                        items.get(index + 3)
                    } else {
                        None
                    };
                for iteration in 0..iterations {
                    let frame = env.last_mut().expect("env frame just pushed");
                    for (slot, spec) in frame.iter_mut().zip(&specs) {
                        *slot = match spec {
                            LoopSpec::Range(name, values) | LoopSpec::List(name, values) => {
                                (name.clone(), values[iteration].clone())
                            }
                        };
                    }
                    if let Some(until) = until_expr
                        && self.eval(until, env)?.is_truthy()
                    {
                        result = Value::Nil;
                        break;
                    }
                    let value = self.eval(thereis_expr, env)?;
                    if value.is_truthy() {
                        result = value;
                        break;
                    }
                }
            }
            _ => return Err(LispError::Signal("Unsupported cl-loop syntax".into())),
        }

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
                        env.push(vec![(var_name.clone(), error_condition_value(&e))]);
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
        let saved_buffer_id = self.current_buffer_id;
        let base_name = " *temp*";
        let temp_name = if self.has_buffer(base_name) {
            let mut n = 2;
            loop {
                let candidate = format!("{}<{}>", base_name, n);
                if !self.has_buffer(&candidate) {
                    break candidate;
                }
                n += 1;
            }
        } else {
            base_name.to_string()
        };
        let (temp_id, _) = self.create_buffer(&temp_name);
        self.set_buffer_hooks_inhibited(temp_id, true);
        self.switch_to_buffer_id(temp_id)?;
        let result = self.sf_progn(&items[1..], env);
        let _ = self.switch_to_buffer_id(saved_buffer_id);
        self.kill_buffer_id(temp_id);
        result
    }

    fn sf_with_output_to_string(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let saved_buffer_id = self.current_buffer_id;
        let base_name = " *with-output-to-string*";
        let temp_name = if self.has_buffer(base_name) {
            let mut n = 2;
            loop {
                let candidate = format!("{}<{}>", base_name, n);
                if !self.has_buffer(&candidate) {
                    break candidate;
                }
                n += 1;
            }
        } else {
            base_name.to_string()
        };
        let (temp_id, _) = self.create_buffer(&temp_name);
        self.set_buffer_hooks_inhibited(temp_id, true);
        self.switch_to_buffer_id(temp_id)?;
        let body_result = self.sf_progn(&items[1..], env);
        let output = Value::String(self.buffer.buffer_string());
        let _ = self.switch_to_buffer_id(saved_buffer_id);
        self.kill_buffer_id(temp_id);
        body_result?;
        Ok(output)
    }

    fn sf_ert_with_temp_file(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Err(LispError::WrongNumberOfArgs(
                "ert-with-temp-file".into(),
                items.len().saturating_sub(1),
            ));
        }
        let name = items[1].as_symbol()?.to_string();
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| LispError::Signal(error.to_string()))?
            .as_nanos();
        let path = std::env::temp_dir().join(format!("emaxx-{stamp}.tmp"));
        fs::write(&path, "").map_err(|error| LispError::Signal(error.to_string()))?;
        env.push(vec![(name, Value::String(path.display().to_string()))]);
        let result = self.sf_progn(&items[2..], env);
        env.pop();
        let _ = fs::remove_file(&path);
        result
    }

    fn sf_with_current_buffer(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Ok(Value::Nil);
        }
        let target = self.eval(&items[1], env)?;
        let target_id = self.resolve_buffer_id(&target)?;
        let saved_buffer_id = self.current_buffer_id;
        self.switch_to_buffer_id(target_id)?;
        let result = self.sf_progn(&items[2..], env);
        let _ = self.switch_to_buffer_id(saved_buffer_id);
        result
    }

    fn sf_with_restriction(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Ok(Value::Nil);
        }
        let start = self.eval(&items[1], env)?.as_integer()? as usize;
        let end = self.eval(&items[2], env)?.as_integer()? as usize;
        let mut body_index = 3;
        let label = if matches!(items.get(3), Some(Value::Symbol(s)) if s == ":label") {
            body_index = 5;
            match items.get(4) {
                Some(Value::Symbol(symbol)) => symbol.clone(),
                Some(Value::Cons(_, _)) => {
                    let quoted = items[4].to_vec()?;
                    quoted
                        .get(1)
                        .ok_or_else(|| LispError::Signal("Invalid with-restriction label".into()))?
                        .as_symbol()?
                        .to_string()
                }
                _ => "default".into(),
            }
        } else {
            "default".into()
        };
        let saved = (self.buffer.point_min(), self.buffer.point_max());
        let current = self
            .effective_labeled_restriction(self.current_buffer_id(), None)
            .unwrap_or(saved);
        let effective = (start.max(current.0), end.min(current.1));
        self.labeled_restrictions
            .push((self.current_buffer_id(), label, effective.0, effective.1));
        self.buffer.narrow_to_region(effective.0, effective.1);
        let result = self.sf_progn(&items[body_index..], env);
        self.labeled_restrictions.pop();
        self.buffer.restore_restriction(saved.0, saved.1);
        result
    }

    fn sf_without_restriction(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Ok(Value::Nil);
        }
        let mut body_index = 1;
        let label = if matches!(items.get(1), Some(Value::Symbol(s)) if s == ":label") {
            body_index = 3;
            match items.get(2) {
                Some(Value::Symbol(symbol)) => symbol.clone(),
                Some(Value::Cons(_, _)) => {
                    let quoted = items[2].to_vec()?;
                    quoted
                        .get(1)
                        .ok_or_else(|| {
                            LispError::Signal("Invalid without-restriction label".into())
                        })?
                        .as_symbol()?
                        .to_string()
                }
                _ => "default".into(),
            }
        } else {
            "default".into()
        };
        let saved = (self.buffer.point_min(), self.buffer.point_max());
        let pos = self
            .labeled_restrictions
            .iter()
            .rposition(|(buffer_id, active_label, _, _)| {
                *buffer_id == self.current_buffer_id() && *active_label == label
            });
        let removed = pos.map(|index| self.labeled_restrictions.remove(index));
        if let Some((start, end)) = self
            .effective_labeled_restriction(self.current_buffer_id(), None)
        {
            self.buffer.narrow_to_region(start, end);
        } else {
            self.buffer.widen();
        }
        let result = self.sf_progn(&items[body_index..], env);
        if let Some(entry) = removed {
            self.labeled_restrictions.push(entry);
        }
        self.buffer.restore_restriction(saved.0, saved.1);
        result
    }

    fn sf_save_excursion(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        let saved_pt = self.buffer.point();
        let result = self.sf_progn(&items[1..], env);
        self.buffer.goto_char(saved_pt);
        result
    }

    fn sf_with_silent_modifications(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let was_modified = self.buffer.is_modified();
        let was_autosaved = self.buffer.is_autosaved();
        let result = self.sf_progn(&items[1..], env);
        if !was_modified {
            self.buffer.set_unmodified();
        } else if was_autosaved {
            self.buffer.set_modified();
            self.buffer.set_autosaved();
        } else {
            self.buffer.set_modified();
        }
        result
    }

    fn sf_save_restriction(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let saved_buffer_id = self.current_buffer_id();
        let saved_begv = self.buffer.point_min();
        let saved_zv = self.buffer.point_max();
        let beg_marker = self.make_marker();
        let end_marker = self.make_marker();
        let beg_id = match beg_marker {
            Value::Marker(id) => id,
            _ => unreachable!("make_marker returns a marker"),
        };
        let end_id = match end_marker {
            Value::Marker(id) => id,
            _ => unreachable!("make_marker returns a marker"),
        };
        let _ = self.set_marker(beg_id, Some(saved_begv), Some(saved_buffer_id));
        let _ = self.set_marker(end_id, Some(saved_zv), Some(saved_buffer_id));
        self.buffer
            .push_undo_meta(Value::cons(Value::Marker(beg_id), Value::Integer(-(saved_begv as i64))));
        self.buffer
            .push_undo_meta(Value::cons(Value::Marker(end_id), Value::Integer(saved_zv as i64)));
        let result = self.sf_progn(&items[1..], env);
        let restore_begv = self.marker_position(beg_id).unwrap_or(saved_begv);
        let restore_zv = self.marker_position(end_id).unwrap_or(saved_zv);
        self.buffer.restore_restriction(restore_begv, restore_zv);
        let _ = self.set_marker(beg_id, None, None);
        let _ = self.set_marker(end_id, None, None);
        result
    }

    fn sf_combine_change_calls(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let start_undo = self.buffer.undo_len();
        let result = self.sf_progn(&items[3..], env)?;
        let entries = self.buffer.take_undo_entries_since(start_undo);
        if !entries.is_empty() {
            self.buffer.push_undo_entry(crate::buffer::UndoEntry::Combined {
                display: combined_undo_display(&entries),
                entries,
            });
        }
        Ok(result)
    }

    fn sf_cl_assert(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Ok(Value::Nil);
        }
        let result = self.eval(&items[1], env)?;
        if result.is_truthy() {
            Ok(result)
        } else {
            Err(LispError::Signal("Assertion failed".into()))
        }
    }

    // ── cl-destructuring-bind ──
    // (cl-destructuring-bind (var1 var2 ... &optional opt1 ...) expr body...)
    fn sf_cl_destructuring_bind(
        &mut self,
        items: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Err(LispError::WrongNumberOfArgs(
                "cl-destructuring-bind".into(),
                items.len() - 1,
            ));
        }
        let pattern = items[1].to_vec()?;
        let val = self.eval(&items[2], env)?;
        let vals = val.to_vec()?;

        let mut frame = Vec::new();
        let mut pi = 0; // pattern index
        let mut vi = 0; // value index
        let mut optional = false;

        while pi < pattern.len() {
            let p = &pattern[pi];
            if let Value::Symbol(s) = p {
                match s.as_str() {
                    "&optional" => {
                        optional = true;
                        pi += 1;
                        continue;
                    }
                    "&rest" => {
                        pi += 1;
                        if pi < pattern.len()
                            && let Value::Symbol(rest_name) = &pattern[pi]
                        {
                            let rest_vals: Vec<Value> = vals[vi..].to_vec();
                            frame.push((rest_name.clone(), Value::list(rest_vals)));
                        }
                        break;
                    }
                    name => {
                        let consumed = vi < vals.len();
                        let v = if consumed {
                            vals[vi].clone()
                        } else if optional {
                            Value::Nil
                        } else {
                            return Err(LispError::WrongNumberOfArgs(
                                "cl-destructuring-bind".into(),
                                vals.len(),
                            ));
                        };
                        frame.push((name.to_string(), v));
                        if consumed {
                            vi += 1;
                        }
                    }
                }
            }
            pi += 1;
        }

        env.push(frame);
        let result = self.sf_progn(&items[3..], env);
        env.pop();
        result
    }

    fn sf_cl_letf(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Err(LispError::WrongNumberOfArgs("cl-letf".into(), items.len() - 1));
        }
        let bindings = items[1].to_vec()?;
        let mut rebound = Vec::new();
        for binding in &bindings {
            let parts = binding.to_vec()?;
            if parts.len() < 2 {
                continue;
            }
            let place = parts[0].to_vec()?;
            if !matches!(place.first(), Some(Value::Symbol(name)) if name == "symbol-function") {
                return Err(LispError::Signal("Unsupported cl-letf place".into()));
            }
            let Some(target) = place.get(1) else {
                return Err(LispError::Signal("Unsupported cl-letf place".into()));
            };
            let function_name = function_name_from_binding_form(target)?;
            let value = self.eval(&parts[1], env)?;
            self.functions.push((function_name.clone(), value));
            rebound.push(function_name);
        }
        let result = self.sf_progn(&items[2..], env);
        for name in rebound.into_iter().rev() {
            if let Some(index) = self.functions.iter().rposition(|(fname, _)| *fname == name) {
                self.functions.remove(index);
            }
        }
        result
    }

    fn sf_aset(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() != 4 {
            return Err(LispError::WrongNumberOfArgs(
                "aset".into(),
                items.len() - 1,
            ));
        }
        if let Value::Symbol(name) = &items[1] {
            let current = self.lookup(name, env)?;
            let new_value = self.eval(&items[3], env)?;
            let index_value = self.eval(&items[2], env)?;
            if matches!(current, Value::CharTable(_)) {
                primitives::call(self, "aset", &[current, index_value, new_value.clone()], env)?;
                return Ok(new_value);
            }
            let index = index_value.as_integer()? as usize;
            let mut entries = current.to_vec()?;
            let tagged = matches!(
                entries.first(),
                Some(Value::Symbol(symbol)) if symbol == "vector" || symbol == "vector-literal"
            );
            let slot = if tagged { index + 1 } else { index };
            if slot >= entries.len() {
                return Err(LispError::Signal("Args out of range".into()));
            }
            entries[slot] = new_value.clone();
            self.set_variable(name, Value::list(entries), env);
            return Ok(new_value);
        }

        let vector = self.eval(&items[1], env)?;
        let index = self.eval(&items[2], env)?;
        let new_value = self.eval(&items[3], env)?;
        primitives::call(self, "aset", &[vector, index, new_value.clone()], env)?;
        Ok(new_value)
    }

    // ── cl-flet ──
    // (cl-flet ((name (args) body...) ...) body...)
    fn sf_cl_flet(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Err(LispError::WrongNumberOfArgs("cl-flet".into(), items.len() - 1));
        }
        let bindings = items[1].to_vec()?;
        let mut frame = Vec::new();
        for binding in &bindings {
            let parts = binding.to_vec()?;
            if parts.len() < 2 {
                continue;
            }
            let fname = parts[0].as_symbol()?.to_string();
            let params_val = parts[1].to_vec()?;
            let mut params = Vec::new();
            for p in &params_val {
                params.push(p.as_symbol()?.to_string());
            }
            let body: Vec<Value> = parts[2..].to_vec();
            let lambda = Value::Lambda(params, body, env.clone());
            frame.push((fname, lambda));
        }
        env.push(frame);
        let result = self.sf_progn(&items[2..], env);
        env.pop();
        result
    }

    // ── cl-macrolet ──
    // (cl-macrolet ((name (args) body...) ...) body...)
    fn sf_cl_macrolet(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Err(LispError::WrongNumberOfArgs(
                "cl-macrolet".into(),
                items.len() - 1,
            ));
        }
        let bindings = items[1].to_vec()?;
        // Register macros temporarily
        let saved_macros_len = self.macros.len();
        for binding in &bindings {
            let parts = binding.to_vec()?;
            if parts.len() < 2 {
                continue;
            }
            let mname = parts[0].as_symbol()?.to_string();
            let params_val = parts[1].to_vec()?;
            let mut params = Vec::new();
            for p in &params_val {
                params.push(p.as_symbol()?.to_string());
            }
            let body: Vec<Value> = parts[2..].to_vec();
            self.macros.push((mname, params, body));
        }
        let result = self.sf_progn(&items[2..], env);
        self.macros.truncate(saved_macros_len);
        result
    }

    // ── Backquote ──

    fn eval_backquote(&mut self, expr: &Value, env: &mut Env) -> Result<Value, LispError> {
        match expr {
            Value::Cons(car, cdr) => {
                // Check for (comma expr) or (comma-at expr) at the top level
                if let Value::Symbol(s) = car.as_ref()
                    && (s == "comma" || s == "comma-at")
                    && let Value::Cons(val, rest) = cdr.as_ref()
                    && matches!(rest.as_ref(), Value::Nil)
                {
                    return self.eval(val, env);
                }
                // Walk the cons structure, handling splicing and dotted pairs
                let mut result: Vec<Value> = Vec::new();
                let mut current: &Value = expr;
                loop {
                    match current {
                        Value::Cons(car, cdr) => {
                            // Is current itself a (comma x) or (comma-at x)?
                            if let Value::Symbol(s) = car.as_ref()
                                && (s == "comma" || s == "comma-at")
                                && let Value::Cons(val, rest) = cdr.as_ref()
                                && matches!(rest.as_ref(), Value::Nil)
                            {
                                // This is a comma form used as a dotted tail
                                let tail = self.eval(val, env)?;
                                let mut out = tail;
                                for item in result.into_iter().rev() {
                                    out = Value::cons(item, out);
                                }
                                return Ok(out);
                            }
                            // Check if car is (comma x) or (comma-at x)
                            if let Value::Cons(inner_car, inner_cdr) = car.as_ref()
                                && let Value::Symbol(s) = inner_car.as_ref()
                                && let Value::Cons(val, rest) = inner_cdr.as_ref()
                                && matches!(rest.as_ref(), Value::Nil)
                            {
                                if s == "comma" {
                                    result.push(self.eval(val, env)?);
                                    current = cdr;
                                    continue;
                                }
                                if s == "comma-at" {
                                    let evaled = self.eval(val, env)?;
                                    if let Ok(elems) = evaled.to_vec() {
                                        result.extend(elems);
                                    }
                                    current = cdr;
                                    continue;
                                }
                            }
                            result.push(self.eval_backquote(car, env)?);
                            current = cdr;
                        }
                        Value::Nil => break,
                        other => {
                            // Dotted pair tail (non-cons) — evaluate and attach
                            let tail = self.eval_backquote(other, env)?;
                            let mut out = tail;
                            for item in result.into_iter().rev() {
                                out = Value::cons(item, out);
                            }
                            return Ok(out);
                        }
                    }
                }
                Ok(Value::list(result))
            }
            // Non-list: return as-is (like quote)
            _ => Ok(expr.clone()),
        }
    }

    // ── Macros ──

    fn sf_defmacro(&mut self, items: &[Value]) -> Result<Value, LispError> {
        if items.len() < 4 {
            return Err(LispError::WrongNumberOfArgs("defmacro".into(), items.len()));
        }
        let name = items[1].as_symbol()?.to_string();
        let params_list = items[2].to_vec()?;
        let mut params = Vec::new();
        for p in &params_list {
            params.push(p.as_symbol()?.to_string());
        }
        // Body starts at index 3, skip docstrings
        let body_start = if items.len() > 4 {
            if let Value::String(_) = &items[3] {
                4
            } else {
                3
            }
        } else {
            3
        };
        // Skip (declare ...) forms
        let body_start = if body_start < items.len() {
            if let Value::Cons(_, _) = &items[body_start] {
                if let Ok(decl) = items[body_start].to_vec() {
                    if let Some(Value::Symbol(s)) = decl.first() {
                        if s == "declare" {
                            body_start + 1
                        } else {
                            body_start
                        }
                    } else {
                        body_start
                    }
                } else {
                    body_start
                }
            } else {
                body_start
            }
        } else {
            body_start
        };
        let body: Vec<Value> = items[body_start..].to_vec();
        self.macros.push((name.clone(), params, body));
        Ok(Value::Symbol(name))
    }

    fn sf_defalias(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 3 {
            return Err(LispError::WrongNumberOfArgs(
                "defalias".into(),
                items.len().saturating_sub(1),
            ));
        }
        let name = quoted_symbol_name(&items[1])
            .or_else(|| items[1].as_symbol().ok().map(str::to_string))
            .ok_or_else(|| LispError::TypeError("symbol".into(), items[1].type_name()))?;
        let function = match &items[2] {
            Value::Symbol(symbol) => self.lookup_function(symbol, env)?,
            other => self.eval(other, env)?,
        };
        self.functions.push((name.clone(), function));
        Ok(Value::Symbol(name))
    }

    fn try_macroexpand(
        &mut self,
        name: &str,
        args: &[Value],
        env: &mut Env,
    ) -> Result<Option<Value>, LispError> {
        // Find the macro
        let macro_def = self.macros.iter().find(|(n, _, _)| n == name).cloned();
        let Some((_, params, body)) = macro_def else {
            return Ok(None);
        };

        // Bind params to unevaluated args
        let mut frame = Vec::new();
        let mut arg_idx = 0;
        let mut rest = false;

        for param in &params {
            if param == "&optional" {
                continue;
            }
            if param == "&rest" || param == "&body" {
                rest = true;
                continue;
            }
            if rest {
                let rest_args = Value::list(args[arg_idx..].iter().cloned());
                frame.push((param.clone(), rest_args));
                break;
            }
            let val = if arg_idx < args.len() {
                args[arg_idx].clone()
            } else {
                Value::Nil
            };
            frame.push((param.clone(), val));
            arg_idx += 1;
        }

        env.push(frame);
        let expanded = if body.len() == 1 {
            self.eval(&body[0], env)?
        } else {
            let progn =
                Value::list(std::iter::once(Value::symbol("progn")).chain(body.iter().cloned()));
            self.eval(&progn, env)?
        };
        env.pop();
        Ok(Some(expanded))
    }

    fn sf_skip_unless(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Ok(Value::Nil);
        }
        let val = self.eval(&items[1], env)?;
        if val.is_truthy() {
            Ok(Value::Nil)
        } else {
            Err(LispError::TestSkipped("Test skipped".into()))
        }
    }

    fn sf_skip_when(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        if items.len() < 2 {
            return Ok(Value::Nil);
        }
        let val = self.eval(&items[1], env)?;
        if val.is_truthy() {
            Err(LispError::TestSkipped("Test skipped".into()))
        } else {
            Ok(Value::Nil)
        }
    }

    fn sf_rx(&mut self, items: &[Value]) -> Result<Value, LispError> {
        Ok(Value::String(compile_rx_sequence(&items[1..])?))
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
        if self.ert_tests.iter().any(|test| test.name == name) {
            return Err(LispError::Signal(format!(
                "Test `{name}` redefined (or loaded twice)"
            )));
        }

        // items[2] is the param list (always empty for ert-deftest)
        // items[3..] is docstring, keyword metadata, then body forms.
        let mut cursor = 3;
        if items.get(cursor).is_some_and(|value| matches!(value, Value::String(_))) {
            cursor += 1;
        }

        let mut tags = Vec::new();
        let mut expected_result = ":passed".to_string();
        while cursor + 1 < items.len()
            && items
                .get(cursor)
                .and_then(keyword_symbol_name)
                .is_some_and(|name| name.starts_with(':'))
        {
            let keyword = keyword_symbol_name(&items[cursor]).unwrap_or_default();
            let value = &items[cursor + 1];
            match keyword.as_str() {
                ":tags" => tags = parse_ert_tags(value),
                ":expected-result" => expected_result = selector_atom(value),
                _ => {}
            }
            cursor += 2;
        }

        let body = Value::list(
            std::iter::once(Value::symbol("progn")).chain(items[cursor..].iter().cloned()),
        );
        self.ert_tests.push(ErtTestDefinition {
            name,
            body,
            tags,
            expected_result,
        });
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
            Err(e) => {
                if let Some(expected_type) = should_error_type(items)
                    && expected_type != e.condition_type()
                {
                    return Err(LispError::Signal(format!(
                        "Test failed: expected error type {} but got {}",
                        expected_type,
                        e.condition_type()
                    )));
                }
                Ok(error_condition_value(&e))
            }
            Ok(val) => Err(LispError::Signal(format!(
                "Test failed: expected error but got {}",
                val
            ))),
        }
    }

    pub fn discovered_tests(&self) -> Vec<DiscoveredTest> {
        self.ert_tests
            .iter()
            .map(ErtTestDefinition::discovered)
            .collect()
    }

    /// Run all collected ERT tests. Returns (passed, failed, total).
    pub fn run_ert_tests(&mut self) -> (usize, usize, usize) {
        let summary = self.run_ert_tests_with_selector(None);
        (summary.passed, summary.failed, summary.total)
    }

    pub fn run_ert_tests_with_selector(&mut self, selector: Option<&Value>) -> BatchSummary {
        let tests: Vec<ErtTestDefinition> = self
            .ert_tests
            .iter()
            .filter(|test| selector.is_none_or(|selector| selector_matches(selector, test)))
            .cloned()
            .collect();
        let mut summary = BatchSummary::default();
        self.test_results.clear();
        self.last_selected_tests = tests.iter().map(|test| test.name.clone()).collect();
        summary.total = tests.len();

        for test in &tests {
            let mut env: Env = Vec::new();
            match self.eval(&test.body, &mut env) {
                Ok(_) => {
                    summary.passed += 1;
                    if test.expected_result == ":failed" {
                        summary.unexpected += 1;
                    }
                    self.test_results.push(TestOutcome {
                        name: test.name.clone(),
                        status: TestStatus::Passed,
                        condition_type: None,
                        message: None,
                    });
                }
                Err(e) => {
                    let status = match e {
                        LispError::TestSkipped(_) => TestStatus::Skipped,
                        _ => TestStatus::Failed,
                    };
                    let expected_failure = test.expected_result == ":failed";
                    match status {
                        TestStatus::Passed => summary.passed += 1,
                        TestStatus::Failed => {
                            summary.failed += 1;
                            if !expected_failure {
                                summary.unexpected += 1;
                            }
                        }
                        TestStatus::Skipped => summary.skipped += 1,
                    }
                    self.test_results.push(TestOutcome {
                        name: test.name.clone(),
                        status,
                        condition_type: Some(e.condition_type().to_string()),
                        message: Some(e.to_string()),
                    });
                }
            }
        }
        summary
    }
}

fn symbol_name(value: &Value) -> Option<String> {
    match value {
        Value::Symbol(name) => Some(name.clone()),
        _ => None,
    }
}

fn keyword_symbol_name(value: &Value) -> Option<String> {
    symbol_name(value)
}

fn quoted_symbol_name(value: &Value) -> Option<String> {
    match unquote(value) {
        Value::Symbol(name) => Some(name),
        _ => None,
    }
}

fn function_name_from_binding_form(value: &Value) -> Result<String, LispError> {
    match value {
        Value::Cons(_, _) => {
            let items = value.to_vec()?;
            if items.len() == 2
                && matches!(items.first(), Some(Value::Symbol(name)) if name == "function" || name == "function-quote" || name == "quote")
            {
                return function_name_from_binding_form(&items[1]);
            }
            let other = unquote(value);
            Err(LispError::TypeError("symbol".into(), other.type_name()))
        }
        _ => match unquote(value) {
            Value::Symbol(name) => Ok(name),
            other => Err(LispError::TypeError("symbol".into(), other.type_name())),
        },
    }
}

fn unquote(value: &Value) -> Value {
    match value {
        Value::Cons(_, _) => {
            if let Ok(items) = value.to_vec()
                && items.len() == 2
                && matches!(items.first(), Some(Value::Symbol(name)) if name == "quote")
            {
                return items[1].clone();
            }
            value.clone()
        }
        _ => value.clone(),
    }
}

fn selector_atom(value: &Value) -> String {
    match unquote(value) {
        Value::Symbol(name) => name.clone(),
        Value::String(value) => value.clone(),
        other => other.to_string(),
    }
}

fn parse_ert_tags(value: &Value) -> Vec<String> {
    match unquote(value) {
        Value::Nil => Vec::new(),
        Value::Cons(_, _) => unquote(value)
            .to_vec()
            .map(|values| values.iter().map(selector_atom).collect())
            .unwrap_or_default(),
        other => vec![selector_atom(&other)],
    }
}

fn should_error_type(items: &[Value]) -> Option<String> {
    let mut cursor = 2;
    while cursor + 1 < items.len() {
        match keyword_symbol_name(&items[cursor]).as_deref() {
            Some(":type") => return Some(selector_atom(&items[cursor + 1])),
            Some(_) => cursor += 2,
            None => break,
        }
    }
    None
}

fn selector_matches(selector: &Value, test: &ErtTestDefinition) -> bool {
    match unquote(selector) {
        Value::Nil => false,
        Value::T => true,
        Value::Symbol(name) if name == "t" => true,
        Value::Symbol(name) if name == "nil" => false,
        Value::Symbol(name) => test.name == name,
        Value::String(pattern) => Regex::new(&pattern)
            .map(|regex| regex.is_match(&test.name))
            .unwrap_or(false),
        Value::Cons(_, _) => {
            let Ok(items) = unquote(selector).to_vec() else {
                return false;
            };
            if items.is_empty() {
                return false;
            }
            match symbol_name(&items[0]).as_deref() {
                Some("tag") => items
                    .get(1)
                    .map(|tag| test.tags.iter().any(|candidate| candidate == &selector_atom(tag)))
                    .unwrap_or(false),
                Some("not") => items.get(1).is_some_and(|inner| !selector_matches(inner, test)),
                Some("or") => items[1..].iter().any(|inner| selector_matches(inner, test)),
                Some("and") => items[1..].iter().all(|inner| selector_matches(inner, test)),
                Some("member") => items[1..].iter().any(|item| selector_atom(item) == test.name),
                Some("eql") => items.get(1).is_some_and(|item| selector_atom(item) == test.name),
                _ => false,
            }
        }
        _ => false,
    }
}

fn error_condition_value(error: &LispError) -> Value {
    match error {
        LispError::TypeError(expected, got) => Value::list([
            Value::Symbol("wrong-type-argument".into()),
            Value::Symbol(expected.clone()),
            match got.as_str() {
                "nil" => Value::Nil,
                _ => Value::String(got.clone()),
            },
        ]),
        LispError::Void(symbol) => Value::list([
            Value::Symbol("void-variable".into()),
            Value::Symbol(symbol.clone()),
        ]),
        LispError::WrongNumberOfArgs(name, count) => Value::list([
            Value::Symbol("wrong-number-of-arguments".into()),
            Value::Symbol(name.clone()),
            Value::Integer(*count as i64),
        ]),
        LispError::EndOfInput => Value::list([
            Value::Symbol("end-of-file".into()),
            Value::Nil,
        ]),
        LispError::TestSkipped(message) => Value::list([
            Value::Symbol("ert-test-skipped".into()),
            Value::String(message.clone()),
        ]),
        LispError::ReadError(message) | LispError::Signal(message) => Value::list([
            Value::Symbol("error".into()),
            Value::String(message.clone()),
        ]),
        LispError::Throw(tag, value) => Value::list([
            Value::Symbol("no-catch".into()),
            tag.clone(),
            value.clone(),
        ]),
        LispError::SignalValue(value) => value.clone(),
    }
}

fn buffer_undo_head_to_entry(value: &Value) -> crate::buffer::UndoEntry {
    match value {
        Value::Nil => crate::buffer::UndoEntry::Boundary,
        Value::Cons(car, cdr) => match (&**car, &**cdr) {
            (Value::Integer(pos), Value::Integer(len)) if *pos >= 0 && *len >= 0 => {
                crate::buffer::UndoEntry::Insert {
                    pos: *pos as usize,
                    len: *len as usize,
                }
            }
            (Value::String(text), Value::Integer(pos)) if *pos >= 0 => {
                crate::buffer::UndoEntry::Delete {
                    pos: *pos as usize,
                    text: text.clone(),
                    props: Vec::new(),
                    markers: Vec::new(),
                }
            }
            _ => crate::buffer::UndoEntry::Opaque(value.clone()),
        },
        _ => crate::buffer::UndoEntry::Opaque(value.clone()),
    }
}

fn combined_undo_display(entries: &[crate::buffer::UndoEntry]) -> Value {
    Value::list([
        Value::Symbol("apply".into()),
        Value::Integer(2),
        Value::Integer(1),
        Value::Integer(1),
        Value::Symbol("undo--wrap-and-run-primitive-undo".into()),
        Value::Integer(1),
        Value::Integer(1),
        Value::list(entries.iter().map(undo_entry_display)),
    ])
}

fn undo_entry_display(entry: &crate::buffer::UndoEntry) -> Value {
    match entry {
        crate::buffer::UndoEntry::Insert { pos, len } => {
            Value::cons(Value::Integer(*pos as i64), Value::Integer(*len as i64))
        }
        crate::buffer::UndoEntry::Delete { pos, text, .. } => {
            Value::cons(Value::String(text.clone()), Value::Integer(*pos as i64))
        }
        crate::buffer::UndoEntry::Combined { display, .. }
        | crate::buffer::UndoEntry::Opaque(display) => display.clone(),
        crate::buffer::UndoEntry::Boundary => Value::Nil,
    }
}

fn latest_generated_undo_group(entries: &[crate::buffer::UndoEntry]) -> Vec<crate::buffer::UndoEntry> {
    entries
        .iter()
        .filter(|entry| !matches!(entry, crate::buffer::UndoEntry::Boundary))
        .cloned()
        .collect()
}

fn render_undo_value(value: &Value) -> String {
    match value {
        Value::Nil => "nil".into(),
        Value::T => "t".into(),
        Value::Integer(n) => n.to_string(),
        Value::BigInteger(n) => n.to_string(),
        Value::Float(n) => {
            if n.fract() == 0.0 {
                format!("{n:.1}")
            } else {
                n.to_string()
            }
        }
        Value::String(s) => format!("\"{}\"", s),
        Value::StringObject(state) => format!("\"{}\"", state.borrow().text),
        Value::Symbol(s) => s.clone(),
        Value::Cons(_, _) => {
            let mut rendered = String::from("(");
            let mut current = value;
            let mut first = true;
            loop {
                match current {
                    Value::Cons(car, cdr) => {
                        if !first {
                            rendered.push(' ');
                        }
                        rendered.push_str(&render_undo_value(car));
                        first = false;
                        current = cdr;
                    }
                    Value::Nil => break,
                    other => {
                        rendered.push_str(" . ");
                        rendered.push_str(&render_undo_value(other));
                        break;
                    }
                }
            }
            rendered.push(')');
            rendered
        }
        Value::BuiltinFunc(name) => format!("#<builtin {name}>"),
        Value::Lambda(params, _, _) => format!("#<lambda ({})>", params.join(" ")),
        Value::Buffer(_, name) => format!("#<buffer {name}>"),
        Value::Marker(id) => format!("#<marker id:{id}>"),
        Value::Overlay(id) => format!("#<overlay id:{id}>"),
        Value::CharTable(id) => format!("#<char-table id:{id}>"),
        Value::Record(id) => format!("#<record id:{id}>"),
        Value::Finalizer(id) => format!("#<finalizer id:{id}>"),
    }
}

fn function_executable_body(body: &[Value]) -> &[Value] {
    let mut start = 0usize;
    while start < body.len() {
        if is_function_declare_form(&body[start]) || is_function_interactive_form(&body[start]) {
            start += 1;
        } else {
            break;
        }
    }
    &body[start..]
}

fn is_function_declare_form(form: &Value) -> bool {
    form.to_vec().ok().is_some_and(|items| {
        matches!(items.first(), Some(Value::Symbol(name)) if name == "declare")
    })
}

fn is_function_interactive_form(form: &Value) -> bool {
    form.to_vec().ok().is_some_and(|items| {
        matches!(items.first(), Some(Value::Symbol(name)) if name == "interactive")
    })
}

fn pcase_pattern_bindings(
    pattern: &Value,
    value: &Value,
    bindings: &mut Vec<(String, Value)>,
) -> Result<bool, LispError> {
    if let Ok(parts) = pattern.to_vec() {
        if matches!(parts.first(), Some(Value::Symbol(name)) if name == "backquote") {
            return pcase_pattern_bindings(parts.get(1).unwrap_or(&Value::Nil), value, bindings);
        }
        if matches!(parts.first(), Some(Value::Symbol(name)) if name == "comma" || name == "comma-at")
            && let Some(Value::Symbol(name)) = parts.get(1)
        {
            bindings.push((name.clone(), value.clone()));
            return Ok(true);
        }
    }

    match (pattern, value) {
        (Value::Cons(pattern_car, pattern_cdr), Value::Cons(value_car, value_cdr)) => {
            let start = bindings.len();
            if !pcase_pattern_bindings(pattern_car, value_car, bindings)? {
                bindings.truncate(start);
                return Ok(false);
            }
            if !pcase_pattern_bindings(pattern_cdr, value_cdr, bindings)? {
                bindings.truncate(start);
                return Ok(false);
            }
            Ok(true)
        }
        (Value::Nil, Value::Nil) => Ok(true),
        _ => Ok(pattern == value),
    }
}

fn feature_name(value: &Value) -> Option<String> {
    match value {
        Value::Symbol(symbol) => Some(symbol.clone()),
        Value::Cons(_, _) => value.to_vec().ok().and_then(|items| match items.as_slice() {
            [Value::Symbol(name), Value::Symbol(symbol)] if name == "quote" => Some(symbol.clone()),
            _ => None,
        }),
        _ => None,
    }
}

fn compile_rx_sequence(items: &[Value]) -> Result<String, LispError> {
    let mut regex = String::new();
    for item in items {
        regex.push_str(&compile_rx_form(item)?);
    }
    Ok(regex)
}

fn compile_rx_form(value: &Value) -> Result<String, LispError> {
    match value {
        Value::String(text) => Ok(regex_escape(text)),
        Value::StringObject(state) => Ok(regex_escape(&state.borrow().text)),
        Value::Integer(codepoint) => {
            let ch = char::from_u32(*codepoint as u32)
                .ok_or_else(|| LispError::Signal(format!("Invalid rx character: {codepoint}")))?;
            Ok(regex_escape(&ch.to_string()))
        }
        Value::Symbol(symbol) => match symbol.as_str() {
            "bos" | "bol" => Ok("^".into()),
            "eos" | "eol" => Ok("$".into()),
            "bow" | "eow" => Ok("\\b".into()),
            "digit" => Ok("[0-9]".into()),
            "blank" => Ok("[[:blank:]]".into()),
            "space" => Ok("[[:space:]]".into()),
            other => Err(LispError::Signal(format!("Unsupported rx atom: {other}"))),
        },
        Value::Cons(_, _) => {
            let items = value.to_vec()?;
            let Some(Value::Symbol(head)) = items.first() else {
                return compile_rx_sequence(&items);
            };
            match head.as_str() {
                "group" => Ok(format!("\\({}\\)", compile_rx_sequence(&items[1..])?)),
                "+" => Ok(format!("\\(?:{}\\)+", compile_rx_sequence(&items[1..])?)),
                "*" => Ok(format!("\\(?:{}\\)*", compile_rx_sequence(&items[1..])?)),
                "?" => Ok(format!("\\(?:{}\\)?", compile_rx_sequence(&items[1..])?)),
                "seq" => compile_rx_sequence(&items[1..]),
                "or" => Ok(format!(
                    "\\(?:{}\\)",
                    items[1..]
                        .iter()
                        .map(compile_rx_form)
                        .collect::<Result<Vec<_>, _>>()?
                        .join("\\|")
                )),
                _ => compile_rx_sequence(&items),
            }
        }
        other => Err(LispError::Signal(format!(
            "Unsupported rx form: {}",
            other.type_name()
        ))),
    }
}


#[cfg(test)]
#[allow(clippy::unwrap_used)]
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
        assert_eq!(eval_str("(cadr '(1 2 3))"), Value::Integer(2));
        assert_eq!(eval_str("(cddr '(1 2 3))"), Value::list([Value::Integer(3)]));
        assert_eq!(eval_str("(identity 'ok)"), Value::Symbol("ok".into()));
        assert_eq!(eval_str("(length '(1 2 3))"), Value::Integer(3));
    }

    #[test]
    fn eval_symbol_with_escaped_trailing_space() {
        assert_eq!(eval_str("'GNU\\ "), Value::Symbol("GNU ".into()));
        assert_eq!(eval_str("(eq 'GNU\\  'GNU\\ )"), Value::T);
    }

    #[test]
    fn eval_font_get_returns_xlfd_foundry_symbol() {
        assert_eq!(
            eval_str(
                "(equal (font-get (font-spec :name \"-GNU -FreeSans-semibold-italic-normal-*-*-*-*-*-*-0-iso10646-1\") :foundry) 'GNU\\ )"
            ),
            Value::T
        );
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
    fn keyword_symbols_self_evaluate() {
        assert_eq!(eval_str(":default"), Value::Symbol(":default".into()));
    }

    #[test]
    fn defconst_binds_global_like_defvar() {
        let mut interp = Interpreter::new();
        assert_eq!(
            eval_str_with(&mut interp, "(defconst sample-constant 42)"),
            Value::Nil
        );
        assert_eq!(eval_str_with(&mut interp, "sample-constant"), Value::Integer(42));
    }

    #[test]
    fn defvar_local_loads_like_defvar() {
        let mut interp = Interpreter::new();
        assert_eq!(
            eval_str_with(&mut interp, "(defvar-local sample-local :default)"),
            Value::Nil
        );
        assert_eq!(
            eval_str_with(&mut interp, "sample-local"),
            Value::Symbol(":default".into())
        );
    }

    #[test]
    fn let_alist_binds_dotted_pair_keys() {
        assert_eq!(
            eval_str("(let ((x '((buffer-text . \"hi\")))) (let-alist x .buffer-text))"),
            Value::String("hi".into())
        );
    }

    #[test]
    fn cl_loop_supports_simple_for_from_to_do() {
        assert_eq!(
            eval_str("(let ((n 0)) (cl-loop for i from 1 to 3 do (setq n (+ n i))) n)"),
            Value::Integer(6)
        );
    }

    #[test]
    fn cl_loop_supports_parallel_in_thereis_until() {
        assert_eq!(
            eval_str(
                "(cl-loop for a in '(1 2 3) for b in '(1 3 2) thereis (< a b) until (> a b))"
            ),
            Value::T
        );
    }

    #[test]
    fn add_to_list_updates_quoted_variable() {
        let mut interp = Interpreter::new();
        eval_str_with(&mut interp, "(setq sample-list '(b c))");
        assert_eq!(
            eval_str_with(&mut interp, "(add-to-list 'sample-list 'a)"),
            Value::list([
                Value::Symbol("a".into()),
                Value::Symbol("b".into()),
                Value::Symbol("c".into()),
            ])
        );
        assert_eq!(
            eval_str_with(&mut interp, "sample-list"),
            Value::list([
                Value::Symbol("a".into()),
                Value::Symbol("b".into()),
                Value::Symbol("c".into()),
            ])
        );
        assert_eq!(
            eval_str_with(&mut interp, "(add-to-list 'sample-list 'c t)"),
            Value::list([
                Value::Symbol("a".into()),
                Value::Symbol("b".into()),
                Value::Symbol("c".into()),
            ])
        );
    }

    #[test]
    fn eval_and_compile_runs_its_body_when_loading_helpers() {
        let mut interp = Interpreter::new();
        eval_str_with(
            &mut interp,
            r#"
            (eval-and-compile
              (defun helper-name () 'loaded))
            (helper-name)
            "#,
        );
        assert_eq!(eval_str_with(&mut interp, "(helper-name)"), Value::Symbol("loaded".into()));
    }

    #[test]
    fn eval_when_compile_runs_its_body_when_loading_helpers() {
        let mut interp = Interpreter::new();
        eval_str_with(
            &mut interp,
            r#"
            (eval-when-compile
              (defun compile-only-helper () 'loaded))
            (compile-only-helper)
            "#,
        );
        assert_eq!(
            eval_str_with(&mut interp, "(compile-only-helper)"),
            Value::Symbol("loaded".into())
        );
    }

    #[test]
    fn expand_file_name_joins_invocation_components() {
        let exe = std::env::current_exe().unwrap();
        let expected = exe.display().to_string();
        assert_eq!(
            eval_str("(expand-file-name invocation-name invocation-directory)"),
            Value::String(expected)
        );
    }

    #[test]
    fn cl_destructuring_bind_keeps_missing_optional_slots_nil() {
        assert_eq!(
            eval_str(
                "(cl-destructuring-bind (a b &optional c d &rest rest) '(1 2) (list a b c d rest))"
            ),
            Value::list([
                Value::Integer(1),
                Value::Integer(2),
                Value::Nil,
                Value::Nil,
                Value::Nil,
            ])
        );
    }

    #[test]
    fn pcase_dolist_binds_backquoted_variables() {
        assert_eq!(
            eval_str(
                "(let (pairs) \
                   (pcase-dolist (`(,left ,right) '((1 2) (3 4))) \
                     (push (list left right) pairs)) \
                   (nreverse pairs))"
            ),
            Value::list([
                Value::list([Value::Integer(1), Value::Integer(2)]),
                Value::list([Value::Integer(3), Value::Integer(4)]),
            ])
        );
    }

    #[test]
    fn replace_match_updates_match_data_for_subexpressions() {
        assert_eq!(
            eval_str(
                r#"
                (with-temp-buffer
                  (let (mismatch)
                    (pcase-dolist (`(,pre ,post) '(("" "")
                                                   ("a" "")
                                                   ("" "b")
                                                   ("a" "b")))
                      (unless mismatch
                        (erase-buffer)
                        (insert "hello ")
                        (save-excursion (insert pre post " world"))
                        (looking-at
                         (concat "\\(\\)" pre "\\(\\)\\(\\(\\)\\)\\(\\)" post "\\(\\)"))
                        (let* ((beg0 (match-beginning 0))
                               (beg4 (+ beg0 (length pre)))
                               (end4 (+ beg4 (length "BOO")))
                               (end0 (+ end4 (length post))))
                          (replace-match "BOO" t t nil 4)
                          (unless (and (equal (match-beginning 0) beg0)
                                       (equal (match-end 0) end0))
                            (setq mismatch
                                  (list pre post
                                        (match-beginning 0)
                                        (match-end 0)
                                        beg0
                                        end0))))))
                    mismatch))"#,
            ),
            Value::Nil
        );
    }

    #[test]
    fn ert_selector_excludes_expensive_tests_by_tag() {
        let mut interp = Interpreter::new();
        eval_str_with(
            &mut interp,
            r#"
            (ert-deftest cheap-test ()
              (should t))
            (ert-deftest expensive-test ()
              :tags '(:expensive-test)
              (should t))
            "#,
        );
        let selector = Reader::new("(not (or (tag :expensive-test) (tag :unstable)))")
            .read_all()
            .unwrap()
            .remove(0);
        let summary = interp.run_ert_tests_with_selector(Some(&selector));
        assert_eq!(summary.total, 1);
        assert_eq!(interp.last_selected_tests, vec!["cheap-test".to_string()]);
    }

    #[test]
    fn should_error_checks_error_type() {
        let mut interp = Interpreter::new();
        eval_str_with(
            &mut interp,
            r#"
            (ert-deftest typed-error ()
              (should-error (car 1) :type 'wrong-type-argument))
            "#,
        );
        let summary = interp.run_ert_tests_with_selector(None);
        assert_eq!(summary.passed, 1);
        assert_eq!(summary.failed, 0);
    }

    #[test]
    fn skip_unless_records_skip_in_summary() {
        let mut interp = Interpreter::new();
        eval_str_with(
            &mut interp,
            r#"
            (ert-deftest skipped-test ()
              (skip-unless nil))
            "#,
        );
        let summary = interp.run_ert_tests_with_selector(None);
        assert_eq!(summary.skipped, 1);
        assert_eq!(interp.test_results[0].status, TestStatus::Skipped);
        assert_eq!(
            interp.test_results[0].condition_type.as_deref(),
            Some("ert-test-skipped")
        );
    }

    #[test]
    fn call_interactively_consumes_unread_events_for_k_specs() {
        assert_eq!(
            eval_str(
                "(let ((unread-command-events '(?a ?b))) \
                   (call-interactively \
                     (lambda (a b) \
                       (interactive \"ka\0a: \nkb: \") \
                       (list a b))))"
            ),
            Value::list([Value::String("a".into()), Value::String("b".into())])
        );
    }

    #[test]
    fn call_interactively_records_declared_history_arguments() {
        let mut interp = Interpreter::new();
        eval_str_with(
            &mut interp,
            "(defun callint-test-int-args (foo bar &optional zot) \
               (declare (interactive-args (bar 10) (zot 11))) \
               (interactive (list 1 1 1)) \
               (+ foo bar zot))",
        );
        assert_eq!(
            eval_str_with(
                &mut interp,
                "(let ((history-length 1) (command-history ())) \
                   (list (call-interactively 'callint-test-int-args t) command-history))"
            ),
            Value::list([
                Value::Integer(3),
                Value::list([Value::list([
                    Value::Symbol("callint-test-int-args".into()),
                    Value::Integer(1),
                    Value::Integer(10),
                    Value::Integer(11),
                ])]),
            ])
        );
    }

    #[test]
    fn call_interactively_rejects_invalid_control_letters() {
        assert_eq!(
            eval_str(
                "(cdr (should-error (call-interactively (lambda () (interactive \"ÿ\")))))"
            ),
            Value::list([Value::String(
                "Invalid control letter `ÿ' (#o377, #x00ff) in interactive calling string".into(),
            )])
        );
    }

    #[cfg(unix)]
    #[test]
    fn call_process_region_can_delete_entire_buffer() {
        assert_eq!(
            eval_str(
                "(let ((shell (executable-find \"sh\"))) \
                   (with-temp-buffer \
                     (insert \"Buffer contents\\n\") \
                     (list \
                       (call-process-region nil nil shell :delete nil nil \"-c\" \"cat >/dev/null\") \
                       (buffer-size))))"
            ),
            Value::list([Value::Integer(0), Value::Integer(0)])
        );
    }

    #[test]
    fn format_binary_negative() {
        assert_eq!(
            eval_str(r#"(format "%b" #x-5A)"#),
            Value::String("-1011010".into())
        );
        assert_eq!(
            eval_str(r#"(format "%b" #x5A)"#),
            Value::String("1011010".into())
        );
    }

    #[test]
    fn backquote_dotted_pair() {
        assert_eq!(
            eval_str(r#"(car '(#x-5A . "1011010"))"#),
            Value::Integer(-90)
        );
        assert_eq!(
            eval_str(r#"(cdr '(#x-5A . "1011010"))"#),
            Value::String("1011010".into())
        );
    }

    #[test]
    fn dolist_dotted_pairs() {
        assert_eq!(
            eval_str(
                r#"(let ((result nil))
                     (dolist (pair `((1 . "a") (2 . "b")))
                       (setq result (concat (cdr pair) (or result ""))))
                     result)"#
            ),
            Value::String("ba".into())
        );
    }

    #[test]
    fn format_binary_nonzero_simple() {
        // Simplified version of the ERT test
        assert_eq!(
            eval_str(
                r#"(let* ((n #x-5A) (bits "1011010")
                          (sgn- (if (< n 0) "-" "")))
                     (concat sgn- bits))"#
            ),
            Value::String("-1011010".into())
        );
        // The actual assertion from the test
        assert_eq!(
            eval_str(
                r#"(let* ((n #x-5A) (bits "1011010")
                          (sgn- (if (< n 0) "-" "")))
                     (string-equal (format "%b" n) (concat sgn- bits)))"#
            ),
            Value::T
        );
    }

    #[test]
    fn format_binary_via_dolist() {
        assert_eq!(
            eval_str(
                r#"(let ((ok t))
                     (dolist (nbits `((#x-5A . "1011010")
                                      (#x5A . "1011010")))
                       (let* ((n (car nbits)) (bits (cdr nbits))
                              (sgn- (if (< n 0) "-" "")))
                         (unless (string-equal (format "%b" n) (concat sgn- bits))
                           (setq ok nil))))
                     ok)"#
            ),
            Value::T
        );
    }

    #[test]
    fn backtick_comma_in_dotted_pair() {
        assert_eq!(
            eval_str(r#"`(#xFFF . ,(make-string 12 ?1))"#),
            Value::cons(Value::Integer(0xFFF), Value::String("111111111111".into()))
        );
    }

    #[test]
    fn eval_while_loop() {
        assert_eq!(
            eval_str("(let ((x 0)) (while (< x 5) (setq x (1+ x))) x)"),
            Value::Integer(5)
        );
    }

    #[test]
    fn overlay_modification_hooks_record_insert_inside_overlay() {
        assert_eq!(
            eval_str(
                r#"
                (with-temp-buffer
                  (insert "1234")
                  (let ((overlay (make-overlay 2 4)))
                    (dolist (hooks-property '(insert-in-front-hooks
                                              modification-hooks
                                              insert-behind-hooks))
                      (overlay-put
                       overlay
                       hooks-property
                       (list (lambda (ov &rest args)
                               (push (list hooks-property args)
                                     (overlay-get overlay
                                                  'recorded-modification-hook-calls)))))
                      (overlay-put overlay 'recorded-modification-hook-calls nil))
                    (goto-char 3)
                    (insert "x")
                    (overlay-get overlay 'recorded-modification-hook-calls)))"#
            ),
            Value::list([
                Value::list([
                    Value::Symbol("modification-hooks".into()),
                    Value::list([
                        Value::T,
                        Value::Integer(3),
                        Value::Integer(4),
                        Value::Integer(0),
                    ]),
                ]),
                Value::list([
                    Value::Symbol("modification-hooks".into()),
                    Value::list([Value::Nil, Value::Integer(3), Value::Integer(3)]),
                ]),
            ])
        );
    }

    #[test]
    fn overlay_modification_hooks_record_insert_at_overlay_start() {
        assert_eq!(
            eval_str(
                r#"
                (with-temp-buffer
                  (insert "1234")
                  (let ((overlay (make-overlay 2 4)))
                    (dolist (hooks-property '(insert-in-front-hooks
                                              modification-hooks
                                              insert-behind-hooks))
                      (overlay-put
                       overlay
                       hooks-property
                       (list (lambda (ov &rest args)
                               (push (list hooks-property args)
                                     (overlay-get overlay
                                                  'recorded-modification-hook-calls)))))
                      (overlay-put overlay 'recorded-modification-hook-calls nil))
                    (goto-char 2)
                    (insert "x")
                    (overlay-get overlay 'recorded-modification-hook-calls)))"#
            ),
            Value::list([
                Value::list([
                    Value::Symbol("insert-in-front-hooks".into()),
                    Value::list([
                        Value::T,
                        Value::Integer(2),
                        Value::Integer(3),
                        Value::Integer(0),
                    ]),
                ]),
                Value::list([
                    Value::Symbol("insert-in-front-hooks".into()),
                    Value::list([Value::Nil, Value::Integer(2), Value::Integer(2)]),
                ]),
            ])
        );
    }

    #[test]
    fn overlay_modification_hooks_record_replace_two_chars() {
        assert_eq!(
            eval_str(
                r#"
                (with-temp-buffer
                  (insert "1234")
                  (let ((overlay (make-overlay 2 4)))
                    (dolist (hooks-property '(insert-in-front-hooks
                                              modification-hooks
                                              insert-behind-hooks))
                      (overlay-put
                       overlay
                       hooks-property
                       (list (lambda (ov &rest args)
                               (push (list hooks-property args)
                                     (overlay-get overlay
                                                  'recorded-modification-hook-calls)))))
                      (overlay-put overlay 'recorded-modification-hook-calls nil))
                    (goto-char (point-min))
                    (search-forward "23")
                    (replace-match "x")
                    (overlay-get overlay 'recorded-modification-hook-calls)))"#
            ),
            Value::list([
                Value::list([
                    Value::Symbol("modification-hooks".into()),
                    Value::list([
                        Value::T,
                        Value::Integer(2),
                        Value::Integer(3),
                        Value::Integer(2),
                    ]),
                ]),
                Value::list([
                    Value::Symbol("modification-hooks".into()),
                    Value::list([Value::Nil, Value::Integer(2), Value::Integer(4)]),
                ]),
            ])
        );
    }

    #[test]
    fn overlay_modification_hooks_record_zero_length_insert() {
        assert_eq!(
            eval_str(
                r#"
                (with-temp-buffer
                  (let ((overlay (make-overlay 1 1)))
                    (dolist (hooks-property '(insert-in-front-hooks
                                              modification-hooks
                                              insert-behind-hooks))
                      (overlay-put
                       overlay
                       hooks-property
                       (list (lambda (ov &rest args)
                               (push (list hooks-property args)
                                     (overlay-get overlay
                                                  'recorded-modification-hook-calls)))))
                      (overlay-put overlay 'recorded-modification-hook-calls nil))
                    (insert "x")
                    (overlay-get overlay 'recorded-modification-hook-calls)))"#
            ),
            Value::list([
                Value::list([
                    Value::Symbol("insert-behind-hooks".into()),
                    Value::list([
                        Value::T,
                        Value::Integer(1),
                        Value::Integer(2),
                        Value::Integer(0),
                    ]),
                ]),
                Value::list([
                    Value::Symbol("insert-in-front-hooks".into()),
                    Value::list([
                        Value::T,
                        Value::Integer(1),
                        Value::Integer(2),
                        Value::Integer(0),
                    ]),
                ]),
                Value::list([
                    Value::Symbol("insert-behind-hooks".into()),
                    Value::list([Value::Nil, Value::Integer(1), Value::Integer(1)]),
                ]),
                Value::list([
                    Value::Symbol("insert-in-front-hooks".into()),
                    Value::list([Value::Nil, Value::Integer(1), Value::Integer(1)]),
                ]),
            ])
        );
    }

    #[test]
    fn overlay_modification_hooks_data_driven_cases() {
        assert_eq!(
            eval_str(
                r#"
                (let ((mismatch nil))
                  (dolist (test-case
                           '(((insert-at . 1))
                             ((insert-at . 2)
                              (expected-calls . ((insert-in-front-hooks (nil 2 2))
                                                 (insert-in-front-hooks (t 2 3 0)))))
                             ((insert-at . 3)
                              (expected-calls . ((modification-hooks (nil 3 3))
                                                 (modification-hooks (t 3 4 0)))))
                             ((insert-at . 4)
                              (expected-calls . ((insert-behind-hooks (nil 4 4))
                                                 (insert-behind-hooks (t 4 5 0)))))
                             ((insert-at . 5))
                             ((replace . "1"))
                             ((replace . "2")
                              (expected-calls . ((modification-hooks (nil 2 3))
                                                 (modification-hooks (t 2 3 1)))))
                             ((replace . "3")
                              (expected-calls . ((modification-hooks (nil 3 4))
                                                 (modification-hooks (t 3 4 1)))))
                             ((replace . "4"))
                             ((replace . "4") (overlay-beg . 4))
                             ((replace . "12")
                              (expected-calls . ((modification-hooks (nil 1 3))
                                                 (modification-hooks (t 1 2 2)))))
                             ((replace . "23")
                              (expected-calls . ((modification-hooks (nil 2 4))
                                                 (modification-hooks (t 2 3 2)))))
                             ((replace . "34")
                              (expected-calls . ((modification-hooks (nil 3 5))
                                                 (modification-hooks (t 3 4 2)))))
                             ((replace . "123")
                              (expected-calls . ((modification-hooks (nil 1 4))
                                                 (modification-hooks (t 1 2 3)))))
                             ((replace . "234")
                              (expected-calls . ((modification-hooks (nil 2 5))
                                                 (modification-hooks (t 2 3 3)))))
                             ((replace . "1234")
                              (expected-calls . ((modification-hooks (nil 1 5))
                                                 (modification-hooks (t 1 2 4)))))
                             ((buffer-text . "") (overlay-beg . 1) (overlay-end . 1)
                              (insert-at . 1)
                              (expected-calls . ((insert-in-front-hooks (nil 1 1))
                                                 (insert-behind-hooks (nil 1 1))
                                                 (insert-in-front-hooks (t 1 2 0))
                                                 (insert-behind-hooks (t 1 2 0)))))))
                    (when (null mismatch)
                      (dolist (advance '(nil t))
                        (when (null mismatch)
                          (let-alist test-case
                            (with-temp-buffer
                              (insert (or .buffer-text "1234"))
                              (let ((overlay (make-overlay
                                              (or .overlay-beg 2)
                                              (or .overlay-end 4)
                                              nil
                                              advance advance)))
                                (dolist (hooks-property '(insert-in-front-hooks
                                                          modification-hooks
                                                          insert-behind-hooks))
                                  (overlay-put
                                   overlay
                                   hooks-property
                                   (list (lambda (ov &rest args)
                                           (push (list hooks-property args)
                                                 (overlay-get overlay
                                                              'recorded-modification-hook-calls)))))
                                  (overlay-put overlay 'recorded-modification-hook-calls nil))
                                (when .insert-at
                                  (goto-char .insert-at)
                                  (insert "x"))
                                (when .replace
                                  (goto-char (point-min))
                                  (search-forward .replace)
                                  (replace-match "x"))
                                (let ((actual (reverse (overlay-get overlay 'recorded-modification-hook-calls))))
                                  (unless (equal .expected-calls actual)
                                    (setq mismatch (list test-case advance actual)))))))))))
                  mismatch)"#
            ),
            Value::Nil
        );
    }

    #[test]
    fn overlay_complex_insert_2_regions() {
        assert_eq!(
            eval_str(
                r#"
                (with-temp-buffer
                  (insert (make-string 100 ?\s))
                  (make-overlay 77 7 nil nil t)
                  (make-overlay 21 53 nil t t)
                  (make-overlay 84 14 nil nil nil)
                  (make-overlay 38 69 nil t nil)
                  (make-overlay 93 15 nil nil t)
                  (make-overlay 73 48 nil t t)
                  (make-overlay 96 51 nil t t)
                  (make-overlay 6 43 nil t t)
                  (make-overlay 15 100 nil t t)
                  (make-overlay 22 17 nil nil nil)
                  (make-overlay 72 45 nil t nil)
                  (make-overlay 2 74 nil nil t)
                  (make-overlay 15 29 nil t t)
                  (make-overlay 17 34 nil t t)
                  (make-overlay 101 66 nil t nil)
                  (make-overlay 94 24 nil nil nil)
                  (goto-char 78)
                  (insert "           ")
                  (narrow-to-region 47 19)
                  (goto-char 46)
                  (widen)
                  (narrow-to-region 13 3)
                  (goto-char 9)
                  (delete-char 0)
                  (goto-char 11)
                  (insert "           ")
                  (goto-char 3)
                  (insert "          ")
                  (goto-char 8)
                  (insert "       ")
                  (goto-char 26)
                  (insert "  ")
                  (goto-char 14)
                  (widen)
                  (narrow-to-region 71 35)
                  (sort (mapcar (lambda (ov)
                                  (cons (overlay-start ov)
                                        (overlay-end ov)))
                                (overlays-in (point-min)
                                             (point-max)))
                        (lambda (o1 o2)
                          (or (< (car o1) (car o2))
                              (and (= (car o1) (car o2))
                                   (< (cdr o1) (cdr o2)))))))"#
            ),
            Value::list([
                Value::cons(Value::Integer(2), Value::Integer(104)),
                Value::cons(Value::Integer(23), Value::Integer(73)),
                Value::cons(Value::Integer(24), Value::Integer(107)),
                Value::cons(Value::Integer(44), Value::Integer(125)),
                Value::cons(Value::Integer(45), Value::Integer(59)),
                Value::cons(Value::Integer(45), Value::Integer(134)),
                Value::cons(Value::Integer(45), Value::Integer(141)),
                Value::cons(Value::Integer(47), Value::Integer(52)),
                Value::cons(Value::Integer(47), Value::Integer(64)),
                Value::cons(Value::Integer(51), Value::Integer(83)),
                Value::cons(Value::Integer(54), Value::Integer(135)),
                Value::cons(Value::Integer(68), Value::Integer(99)),
            ])
        );
    }

    #[test]
    fn overlay_positions_survive_unibyte_to_multibyte_transition() {
        assert_eq!(
            eval_str(
                r#"
                (with-temp-buffer
                  (set-buffer-multibyte t)
                  (insert "ääää")
                  (set-buffer-multibyte nil)
                  (let ((nonempty-bob-end (make-overlay 1 2))
                        (nonempty-bob-beg (make-overlay 1 3))
                        (empty-bob        (make-overlay 1 1))
                        (empty-beg        (make-overlay 3 3))
                        (empty-end        (make-overlay 2 2))
                        (nonempty-beg-beg (make-overlay 3 7))
                        (nonempty-beg-end (make-overlay 3 8))
                        (nonempty-end-beg (make-overlay 4 7))
                        (nonempty-end-end (make-overlay 4 8))
                        (nonempty-eob-beg (make-overlay 5 9))
                        (nonempty-eob-end (make-overlay 6 9))
                        (empty-eob        (make-overlay 9 9)))
                    (set-buffer-multibyte t)
                    (list
                     (list (overlay-start nonempty-bob-end) (overlay-end nonempty-bob-end))
                     (list (overlay-start nonempty-bob-beg) (overlay-end nonempty-bob-beg))
                     (list (overlay-start empty-bob) (overlay-end empty-bob))
                     (list (overlay-start empty-beg) (overlay-end empty-beg))
                     (list (overlay-start empty-end) (overlay-end empty-end))
                     (list (overlay-start nonempty-beg-beg) (overlay-end nonempty-beg-beg))
                     (list (overlay-start nonempty-beg-end) (overlay-end nonempty-beg-end))
                     (list (overlay-start nonempty-end-beg) (overlay-end nonempty-end-beg))
                     (list (overlay-start nonempty-end-end) (overlay-end nonempty-end-end))
                     (list (overlay-start nonempty-eob-beg) (overlay-end nonempty-eob-beg))
                     (list (overlay-start nonempty-eob-end) (overlay-end nonempty-eob-end))
                     (list (overlay-start empty-eob) (overlay-end empty-eob)))))
                "#
            ),
            Value::list([
                Value::list([Value::Integer(1), Value::Integer(2)]),
                Value::list([Value::Integer(1), Value::Integer(2)]),
                Value::list([Value::Integer(1), Value::Integer(1)]),
                Value::list([Value::Integer(2), Value::Integer(2)]),
                Value::list([Value::Integer(2), Value::Integer(2)]),
                Value::list([Value::Integer(2), Value::Integer(4)]),
                Value::list([Value::Integer(2), Value::Integer(5)]),
                Value::list([Value::Integer(3), Value::Integer(4)]),
                Value::list([Value::Integer(3), Value::Integer(5)]),
                Value::list([Value::Integer(3), Value::Integer(5)]),
                Value::list([Value::Integer(4), Value::Integer(5)]),
                Value::list([Value::Integer(5), Value::Integer(5)]),
            ])
        );
    }

    #[test]
    fn current_column_uses_lexically_bound_tab_width() {
        assert_eq!(
            eval_str(
                r#"
                (with-temp-buffer
                  (let ((tab-width 4))
                    (insert "ab\tcd")
                    (goto-char (point-min))
                    (forward-char 3)
                    (current-column)))
                "#
            ),
            Value::Integer(4)
        );
    }

    #[test]
    fn rx_compiles_common_test_patterns() {
        assert_eq!(
            eval_str(r#"(rx bos (group (+ digit)) (+ blank) "Hi" eol)"#),
            Value::String("^\\(\\(?:[0-9]\\)+\\)\\(?:[[:blank:]]\\)+Hi$".into())
        );
        assert_eq!(
            eval_str(r#"(rx bow "SECCOMP" eow)"#),
            Value::String("\\bSECCOMP\\b".into())
        );
    }
}
