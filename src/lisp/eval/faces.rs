use super::*;
use crate::lisp::primitives::{aset_vector_value, vector_slot_value};

impl Interpreter {
    /// Install the variables owned by GNU's xfaces.c at the same native
    /// boundary.  Lisp libraries may subsequently customize these values,
    /// but compiled libraries must never depend on source-loading order for
    /// the underlying C-owned value cells to exist.
    pub(crate) fn initialize_native_face_variables(&mut self) {
        let initial_faces = self
            .lisp_face_states
            .iter()
            .filter_map(|face| Some((face.name.clone(), face.id?, face.global.clone()?)))
            .collect::<Vec<_>>();
        let entries = initial_faces
            .iter()
            .map(|(name, id, vector)| {
                (
                    Value::symbol(name),
                    Value::cons(Value::Integer(*id), vector.clone()),
                )
            })
            .collect();
        let defaults = crate::lisp::json::make_hash_table(self, "eq", entries);
        self.define_special_variable("face--new-frame-defaults", defaults);
        for (name, id, _) in initial_faces {
            // GNU Finternal_make_lisp_face owns both directions of the
            // Lisp-face-ID mapping: the native hash entry above and the
            // symbol's `face' property.  The precreated default face must
            // enter through the same lifecycle as later faces.
            self.put_symbol_property(&name, "face", Value::Integer(id));
        }
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

    pub(crate) fn lisp_face_vector(&self, name: &str, global: bool) -> Option<Value> {
        self.lisp_face_vector_on(name, (!global).then_some(self.selected_frame_id))
    }

    pub(crate) fn lisp_face_vector_on(&self, name: &str, frame: Option<u64>) -> Option<Value> {
        let state = self
            .lisp_face_state_index(name)
            .and_then(|index| self.lisp_face_states.get(index))?;
        match frame {
            None => state.global.clone(),
            Some(id) => state.frames.get(&id).cloned(),
        }
    }

    pub(crate) fn ensure_lisp_face(
        &mut self,
        name: &str,
        selected_frame: bool,
        reset: bool,
    ) -> Result<Value, LispError> {
        self.ensure_lisp_face_on(
            name,
            selected_frame.then_some(self.selected_frame_id),
            reset,
        )
    }

    pub(crate) fn ensure_lisp_face_on(
        &mut self,
        name: &str,
        frame: Option<u64>,
        reset: bool,
    ) -> Result<Value, LispError> {
        let index = match self.lisp_face_state_index(name) {
            Some(index) => index,
            None => {
                self.lisp_face_states.push(LispFaceState {
                    name: name.to_string(),
                    id: None,
                    global: Some(empty_lisp_face_vector()),
                    frames: HashMap::new(),
                });
                self.lisp_face_states.len() - 1
            }
        };

        if self.lisp_face_states[index].global.is_none() {
            self.lisp_face_states[index].global = Some(empty_lisp_face_vector());
        }
        let vector = match frame {
            Some(id) => self.lisp_face_states[index]
                .frames
                .entry(id)
                .or_insert_with(empty_lisp_face_vector)
                .clone(),
            None => self.lisp_face_states[index]
                .global
                .as_ref()
                .expect("face has an initialized global definition")
                .clone(),
        };
        if reset {
            for slot in 1..LFACE_VECTOR_SIZE {
                aset_vector_value(&vector, slot, Value::symbol("unspecified"))?;
            }
        }
        if let Some(id) = frame {
            self.sync_frame_face_hash_entry(id, name, vector.clone())?;
        }
        Ok(vector)
    }

    fn sync_frame_face_hash_entry(
        &mut self,
        frame: u64,
        name: &str,
        vector: Value,
    ) -> Result<(), LispError> {
        let Some(table) = self
            .frame_state(frame)
            .expect("decoded frame has state")
            .face_hash_table
            .clone()
        else {
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

    pub(crate) fn frame_face_hash_table(&mut self, frame: u64) -> Value {
        if let Some(table) = &self
            .frame_state(frame)
            .expect("decoded frame has state")
            .face_hash_table
        {
            return table.clone();
        }
        let entries = self
            .lisp_face_states
            .iter()
            .filter_map(|face| {
                face.frames
                    .get(&frame)
                    .cloned()
                    .map(|vector| (Value::symbol(&face.name), vector))
            })
            .collect();
        let table = crate::lisp::json::make_hash_table(self, "eq", entries);
        self.frame_state_mut(frame)
            .expect("decoded frame has state")
            .face_hash_table = Some(table.clone());
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
        self.set_lisp_face_attribute_on(
            name,
            index,
            value,
            (!global).then_some(self.selected_frame_id),
        )
    }

    pub(crate) fn set_lisp_face_attribute_on(
        &mut self,
        name: &str,
        index: usize,
        value: Value,
        frame: Option<u64>,
    ) -> Result<Value, LispError> {
        let vector = self.ensure_lisp_face_on(name, frame, false)?;
        aset_vector_value(&vector, index, value.clone())?;
        self.face_change_count += 1;
        if frame.is_none() {
            self.sync_new_frame_face_hash_entry(name)?;
        }
        Ok(value)
    }

    pub(crate) fn copy_lisp_face_attributes(
        &mut self,
        from: &str,
        to: &str,
        source_frame: Option<u64>,
        target_frame: Option<u64>,
    ) -> Result<(), LispError> {
        let source = self
            .lisp_face_vector_on(from, source_frame)
            .ok_or_else(|| LispError::Signal(format!("Invalid face: {from}")))?;
        let target = self.ensure_lisp_face_on(to, target_frame, true)?;
        self.register_lisp_face_id(to);
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

impl Interpreter {
    pub(crate) fn copy_frame_faces(&mut self, from: u64, to: u64) {
        for face in &mut self.lisp_face_states {
            if let Some(vector) = face.frames.get(&from) {
                let slots = (0..LFACE_VECTOR_SIZE)
                    .map(|index| {
                        vector_slot_value(vector, index)
                            .expect("face vector has LFACE_VECTOR_SIZE slots")
                    })
                    .collect::<Vec<_>>();
                face.frames.insert(
                    to,
                    Value::list(std::iter::once(Value::symbol("vector-literal")).chain(slots)),
                );
            }
        }
    }
}
