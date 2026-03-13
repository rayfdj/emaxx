use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

use super::primitives;
use super::types::{Env, LispError, Value};
use crate::compat::{BatchSummary, DiscoveredTest, TestOutcome, TestStatus};
use regex::Regex;

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
    /// User-defined macros: name → (params, body).
    macros: Vec<(String, Vec<String>, Vec<Value>)>,
    /// Collected ERT test definitions.
    pub ert_tests: Vec<ErtTestDefinition>,
    /// Results from the most recent ERT run.
    pub test_results: Vec<TestOutcome>,
    /// Selected test names from the most recent ERT run.
    pub last_selected_tests: Vec<String>,
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
            macros: Vec::new(),
            ert_tests: Vec::new(),
            test_results: Vec::new(),
            last_selected_tests: Vec::new(),
        }
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

    pub fn insert_current_buffer(&mut self, s: &str) {
        let pos = self.buffer.point();
        let nchars = s.chars().count();
        self.buffer.insert(s);
        self.adjust_markers_for_insert(self.current_buffer_id(), pos, nchars, false);
    }

    pub fn insert_current_buffer_with_properties(
        &mut self,
        s: &str,
        props: Option<Vec<(String, Value)>>,
    ) {
        let pos = self.buffer.point();
        let nchars = s.chars().count();
        self.buffer.insert_with_properties(s, props);
        self.adjust_markers_for_insert(self.current_buffer_id(), pos, nchars, false);
    }

    pub fn insert_current_buffer_and_inherit(&mut self, s: &str) {
        let pos = self.buffer.point();
        let nchars = s.chars().count();
        self.buffer.insert_and_inherit(s);
        self.adjust_markers_for_insert(self.current_buffer_id(), pos, nchars, false);
    }

    pub fn insert_current_buffer_before_markers(&mut self, s: &str) {
        let pos = self.buffer.point();
        let nchars = s.chars().count();
        self.buffer.insert(s);
        self.adjust_markers_for_insert(self.current_buffer_id(), pos, nchars, true);
    }

    pub fn insert_current_buffer_before_markers_and_inherit(&mut self, s: &str) {
        let pos = self.buffer.point();
        let nchars = s.chars().count();
        self.buffer.insert_and_inherit(s);
        self.adjust_markers_for_insert(self.current_buffer_id(), pos, nchars, true);
    }

    pub fn delete_region_current_buffer(
        &mut self,
        from: usize,
        to: usize,
    ) -> Result<String, crate::buffer::BufferError> {
        let from = from.max(self.buffer.point_min());
        let to = to.min(self.buffer.point_max());
        let deleted = self.buffer.delete_region(from, to)?;
        self.adjust_markers_for_delete(self.current_buffer_id(), from, to);
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
        if name == "buffer-undo-list" {
            return Ok(crate::lisp::primitives::buffer_undo_list_value(&self.buffer));
        }
        // Built-in constants and functions
        match name {
            "nil" => Ok(Value::Nil),
            "t" => Ok(Value::T),
            "float-pi" => Ok(Value::Float(std::f64::consts::PI)),
            "most-positive-fixnum" => Ok(Value::Integer(i64::MAX)),
            "most-negative-fixnum" => Ok(Value::Integer(i64::MIN)),
            "enable-multibyte-characters" => Ok(Value::T),
            "system-type" => Ok(Value::Symbol(std::env::consts::OS.replace("macos", "darwin"))),
            _ if name.starts_with(':') => Ok(Value::Symbol(name.to_string())),
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
    pub fn set_variable(&mut self, name: &str, value: Value, env: &mut Env) {
        if name == "buffer-undo-list" {
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
            Value::Nil | Value::T | Value::Integer(_) | Value::Float(_) | Value::String(_) => {
                Ok(expr.clone())
            }

            Value::BuiltinFunc(_)
            | Value::Lambda(_, _, _)
            | Value::Buffer(_, _)
            | Value::Marker(_)
            | Value::Overlay(_) => Ok(expr.clone()),

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
                        "and" => return self.sf_and(&items, env),
                        "or" => return self.sf_or(&items, env),
                        "not" => return self.sf_not(&items, env),
                        "progn" => return self.sf_progn(&items[1..], env),
                        "prog1" => return self.sf_prog1(&items, env),
                        "let" => return self.sf_let(&items, env),
                        "let*" => return self.sf_letstar(&items, env),
                        "setq" => return self.sf_setq(&items, env),
                        "setq-local" => return self.sf_setq(&items, env), // same for now
                        "defvar" | "defconst" | "defvar-local" => {
                            return self.sf_defvar(&items, env)
                        }
                        "defun" => return self.sf_defun(&items, env),
                        "defmacro" => return self.sf_defmacro(&items),
                        "backquote" => return self.eval_backquote(&items[1], env),
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
                        "cl-assert" => return self.sf_cl_assert(&items, env),
                        "with-temp-buffer" => return self.sf_with_temp_buffer(&items, env),
                        "ert-with-temp-file" => return self.sf_ert_with_temp_file(&items, env),
                        "with-current-buffer" => return self.sf_with_current_buffer(&items, env),
                        "with-selected-window" => return self.sf_progn(&items[2..], env),
                        "save-excursion" => return self.sf_save_excursion(&items, env),
                        "save-restriction" => return self.sf_progn(&items[1..], env),
                        "combine-change-calls" => return self.sf_progn(&items[3..], env),
                        "cl-destructuring-bind" => {
                            return self.sf_cl_destructuring_bind(&items, env)
                        }
                        "cl-flet" | "cl-labels" => {
                            return self.sf_cl_flet(&items, env)
                        }
                        "cl-macrolet" => {
                            return self.sf_cl_macrolet(&items, env)
                        }
                        "incf" | "cl-incf" => {
                            // (incf PLACE &optional DELTA)
                            if items.len() < 2 {
                                return Err(LispError::WrongNumberOfArgs(
                                    "incf".into(),
                                    items.len() - 1,
                                ));
                            }
                            let delta = if items.len() > 2 {
                                self.eval(&items[2], env)?.as_integer()?
                            } else {
                                1
                            };
                            let name = items[1].as_symbol()?.to_string();
                            let cur = self.lookup(&name, env)?.as_integer()?;
                            let new_val = Value::Integer(cur + delta);
                            self.set_variable(&name, new_val.clone(), env);
                            return Ok(new_val);
                        }
                        "decf" | "cl-decf" => {
                            if items.len() < 2 {
                                return Err(LispError::WrongNumberOfArgs(
                                    "decf".into(),
                                    items.len() - 1,
                                ));
                            }
                            let delta = if items.len() > 2 {
                                self.eval(&items[2], env)?.as_integer()?
                            } else {
                                1
                            };
                            let name = items[1].as_symbol()?.to_string();
                            let cur = self.lookup(&name, env)?.as_integer()?;
                            let new_val = Value::Integer(cur - delta);
                            self.set_variable(&name, new_val.clone(), env);
                            return Ok(new_val);
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
                            let name = items[2].as_symbol()?.to_string();
                            let cur = self.lookup(&name, env)?;
                            let new_val = Value::cons(val, cur);
                            self.set_variable(&name, new_val.clone(), env);
                            return Ok(new_val);
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
                        "add-to-list" => return self.sf_add_to_list(&items, env),
                        "ert-deftest" => return self.sf_ert_deftest(&items),
                        "should" => return self.sf_should(&items, env),
                        "should-not" => return self.sf_should_not(&items, env),
                        "should-error" => return self.sf_should_error(&items, env),
                        "skip-unless" => return self.sf_skip_unless(&items, env),
                        "skip-when" => return self.sf_skip_when(&items, env),
                        "require" | "provide" | "declare" => return Ok(Value::Nil),
                        "eval-and-compile" => return self.sf_progn(&items[1..], env),
                        "ert-info" => {
                            // (ert-info (msg) body...) — just run the body
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
        self.switch_to_buffer_id(temp_id)?;
        let result = self.sf_progn(&items[1..], env);
        let _ = self.switch_to_buffer_id(saved_buffer_id);
        self.kill_buffer_id(temp_id);
        result
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

    fn sf_save_excursion(&mut self, items: &[Value], env: &mut Env) -> Result<Value, LispError> {
        let saved_pt = self.buffer.point();
        let result = self.sf_progn(&items[1..], env);
        self.buffer.goto_char(saved_pt);
        result
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
                // Return the error as (error "message") like Emacs does
                Ok(Value::list([
                    Value::symbol(e.condition_type()),
                    Value::String(e.to_string()),
                ]))
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
}
