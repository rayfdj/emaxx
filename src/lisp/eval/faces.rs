use super::*;
use crate::lisp::primitives::{aset_vector_value, vector_slot_value};

impl Interpreter {
    /// Install the variables owned by GNU's xfaces.c at the same native
    /// boundary.  Lisp libraries may subsequently customize these values,
    /// but compiled libraries must never depend on source-loading order for
    /// the underlying C-owned value cells to exist.
    pub(crate) fn initialize_native_face_variables(&mut self) {
        let entries = self
            .lisp_face_states
            .iter()
            .filter_map(|face| {
                Some((
                    Value::symbol(&face.name),
                    Value::cons(Value::Integer(face.id?), face.global.clone()?),
                ))
            })
            .collect();
        let defaults = crate::lisp::json::make_hash_table(self, "eq", entries);
        self.define_special_variable("face--new-frame-defaults", defaults);
        self.define_special_variable("face-filters-always-match", Value::Nil);
        self.define_special_variable("face-default-stipple", Value::String("gray3".into()));
        self.define_special_variable("scalable-fonts-allowed", Value::Nil);
        self.define_special_variable("face-ignored-fonts", Value::Nil);
        self.define_special_variable("face-remapping-alist", Value::Nil);
        self.define_special_variable("face-font-rescale-alist", Value::Nil);
        self.define_special_variable("face-near-same-color-threshold", Value::Integer(30_000));
        self.define_special_variable("face-font-lax-matched-attributes", Value::T);
    }

    fn lisp_face_state_index(&self, name: &str) -> Option<usize> {
        self.lisp_face_states
            .iter()
            .position(|state| state.name == name)
    }

    pub(crate) fn lisp_face_exists(&self, name: &str) -> bool {
        self.lisp_face_state_index(name).is_some()
    }

    pub(crate) fn lisp_face_names(&self) -> impl Iterator<Item = &str> {
        self.lisp_face_states
            .iter()
            .map(|state| state.name.as_str())
    }

    pub(crate) fn lisp_face_vector(&self, name: &str, global: bool) -> Option<Value> {
        let state = self
            .lisp_face_state_index(name)
            .and_then(|index| self.lisp_face_states.get(index))?;
        if global {
            state.global.clone()
        } else {
            state.selected_frame.clone()
        }
    }

    pub(crate) fn ensure_lisp_face(
        &mut self,
        name: &str,
        selected_frame: bool,
        reset: bool,
    ) -> Result<Value, LispError> {
        let index = match self.lisp_face_state_index(name) {
            Some(index) => index,
            None => {
                self.lisp_face_states.push(LispFaceState {
                    name: name.to_string(),
                    id: None,
                    global: Some(empty_lisp_face_vector()),
                    selected_frame: None,
                });
                self.lisp_face_states.len() - 1
            }
        };

        if self.lisp_face_states[index].global.is_none() {
            self.lisp_face_states[index].global = Some(empty_lisp_face_vector());
        }
        let vector = {
            let target = if selected_frame {
                &mut self.lisp_face_states[index].selected_frame
            } else {
                &mut self.lisp_face_states[index].global
            };
            if target.is_none() {
                *target = Some(empty_lisp_face_vector());
            } else if reset {
                let vector = target
                    .as_ref()
                    .expect("an existing Lisp face target must have a vector");
                for slot in 1..LFACE_VECTOR_SIZE {
                    aset_vector_value(vector, slot, Value::symbol("unspecified"))?;
                }
            }
            target
                .as_ref()
                .expect("ensuring a Lisp face must produce a vector")
                .clone()
        };
        if selected_frame {
            self.sync_selected_frame_face_hash_entry(name, vector.clone())?;
        }
        Ok(vector)
    }

    /// GNU's tty color-mode switch clears the face cache so every face
    /// re-realizes from its stored specs against the new display
    /// (term.c tty_set_color_mode → clear_face_cache).  emaxx realizes
    /// eagerly, so walk the registry now: reset each defface'd face and
    /// re-apply its spec layers in face-spec-recalc's order.
    pub(crate) fn rerealize_defface_faces(&mut self) -> Result<(), LispError> {
        let faces: Vec<String> = self.lisp_face_names().map(str::to_string).collect();
        for face in faces {
            let Some(spec) = self
                .get_symbol_property(&face, "face-defface-spec")
                .filter(|spec| !spec.is_nil())
            else {
                continue;
            };
            self.ensure_lisp_face(&face, false, true)?;
            self.ensure_lisp_face(&face, true, true)?;
            self.record_defface_runtime_attributes(&face, &spec)?;
            for layer in ["saved-face", "customized-face", "face-override-spec"] {
                if let Some(extra) = self
                    .get_symbol_property(&face, layer)
                    .filter(|extra| !extra.is_nil())
                {
                    self.record_defface_runtime_attributes(&face, &extra)?;
                }
            }
        }
        self.face_change_count += 1;
        Ok(())
    }

    fn sync_selected_frame_face_hash_entry(
        &mut self,
        name: &str,
        vector: Value,
    ) -> Result<(), LispError> {
        let Some(table) = self.selected_frame_face_hash_table.clone() else {
            return Ok(());
        };
        let Some((_, mut entries)) = crate::lisp::json::hash_table_entries(self, &table) else {
            unreachable!("the frame face table must remain a hash table");
        };
        if let Some((_, value)) = entries
            .iter_mut()
            .find(|(key, _)| matches!(key, Value::Symbol(symbol) if symbol == name))
        {
            *value = vector;
        } else {
            entries.push((Value::symbol(name), vector));
        }
        crate::lisp::primitives::set_hash_table_entries(self, &table, entries)
    }

    fn sync_new_frame_face_hash_entry(&mut self, name: &str) -> Result<(), LispError> {
        let Some(index) = self.lisp_face_state_index(name) else {
            return Ok(());
        };
        let Some(id) = self.lisp_face_states[index].id else {
            return Ok(());
        };
        let Some(vector) = self.lisp_face_states[index].global.clone() else {
            return Ok(());
        };
        let Some(table) = self.global_binding_value("face--new-frame-defaults") else {
            return Ok(());
        };
        let Some((_, mut entries)) = crate::lisp::json::hash_table_entries(self, &table) else {
            return Ok(());
        };
        let spec = Value::cons(Value::Integer(id), vector);
        if let Some((_, value)) = entries
            .iter_mut()
            .find(|(key, _)| matches!(key, Value::Symbol(symbol) if symbol == name))
        {
            *value = spec;
        } else {
            entries.push((Value::symbol(name), spec));
        }
        crate::lisp::primitives::set_hash_table_entries(self, &table, entries)
    }

    pub(crate) fn selected_frame_face_hash_table(&mut self) -> Value {
        if let Some(table) = &self.selected_frame_face_hash_table {
            return table.clone();
        }
        let entries = self
            .lisp_face_states
            .iter()
            .filter_map(|face| {
                face.selected_frame
                    .clone()
                    .map(|vector| (Value::symbol(&face.name), vector))
            })
            .collect();
        let table = crate::lisp::json::make_hash_table(self, "eq", entries);
        self.selected_frame_face_hash_table = Some(table.clone());
        table
    }

    pub(crate) fn register_lisp_face_id(&mut self, name: &str) -> i64 {
        let index = self
            .lisp_face_state_index(name)
            .expect("registering a Lisp face ID requires an existing face");
        if let Some(id) = self.lisp_face_states[index].id {
            return id;
        }
        let id = self.next_lisp_face_id;
        self.next_lisp_face_id += 1;
        self.lisp_face_states[index].id = Some(id);
        self.put_symbol_property(name, "face", Value::Integer(id));
        self.sync_new_frame_face_hash_entry(name)
            .expect("the native face defaults table must remain a hash table");
        id
    }

    pub(crate) fn lisp_face_attribute(
        &self,
        name: &str,
        index: usize,
        global: bool,
    ) -> Option<Value> {
        self.lisp_face_vector(name, global)
            .and_then(|vector| vector_slot_value(&vector, index).ok())
    }

    pub(crate) fn face_definitions_generation(&self) -> u64 {
        self.face_change_count
    }

    pub(crate) fn set_lisp_face_attribute(
        &mut self,
        name: &str,
        index: usize,
        value: Value,
        global: bool,
    ) -> Result<Value, LispError> {
        let vector = self.ensure_lisp_face(name, !global, false)?;
        aset_vector_value(&vector, index, value.clone())?;
        self.face_change_count += 1;
        if global {
            self.sync_new_frame_face_hash_entry(name)?;
        }
        Ok(value)
    }

    pub(crate) fn copy_lisp_face_attributes(
        &mut self,
        from: &str,
        to: &str,
        global: bool,
    ) -> Result<(), LispError> {
        let source = self
            .lisp_face_vector(from, global)
            .ok_or_else(|| LispError::Signal(format!("Invalid face: {from}")))?;
        let target = self.ensure_lisp_face(to, !global, true)?;
        for index in 0..LFACE_VECTOR_SIZE {
            aset_vector_value(&target, index, vector_slot_value(&source, index)?)?;
        }
        Ok(())
    }

    pub fn face_inherit_target(&self, face: &str) -> Option<String> {
        self.lisp_face_attribute(face, LFACE_INHERIT_INDEX, false)
            .and_then(|value| match value {
                Value::Symbol(symbol) if symbol != "unspecified" => Some(symbol.to_string()),
                _ => None,
            })
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
                Value::Symbol(target.clone().into()),
            ])));
        }
        let value = inherit
            .map(|value| Value::Symbol(value.into()))
            .unwrap_or(Value::Nil);
        self.set_lisp_face_attribute(face, LFACE_INHERIT_INDEX, value, false)?;
        Ok(())
    }

    pub(super) fn face_inheritance_creates_cycle(&self, face: &str, target: &str) -> bool {
        let mut visited = HashSet::new();
        let mut current = Some(target.to_string());
        while let Some(name) = current {
            if name == face {
                return true;
            }
            if !visited.insert(name.clone()) {
                return false;
            }
            current = self.face_inherit_target(&name);
        }
        false
    }
}
