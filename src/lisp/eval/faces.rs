use super::*;
use crate::lisp::primitives::{aset_vector_value, vector_items, vector_slot_value};
use crate::lisp::types::{FrameRef, HashTableRef, Kind};

/// signal_error ("Invalid face", FACE).
pub(crate) fn invalid_face_error(face: Value) -> LispError {
    LispError::SignalValue(Value::list([
        Value::symbol("error"),
        Value::string("Invalid face"),
        face,
    ]))
}

impl Interpreter {
    /// Install the variables owned by GNU's xfaces.c at the same native
    /// boundary.  Lisp libraries may subsequently customize these values,
    /// but compiled libraries must never depend on source-loading order for
    /// the underlying C-owned value cells to exist.
    ///
    /// syms_of_xfaces makes Vface_new_frame_defaults, the face definitions
    /// for new frames: an `eq' table of each face's symbol to its id and
    /// global lface vector.  The default face is face 0 from the start,
    /// and the selected frame's own definition of it is the one
    /// realize_default_face gives a tty frame.
    pub(crate) fn initialize_native_face_variables(&mut self) {
        let default = Value::symbol("default");
        let defaults = crate::lisp::json::make_hash_table(
            self,
            "eq",
            vec![(
                default,
                Value::cons(Value::Integer(0), empty_lisp_face_vector()),
            )],
        );
        self.define_special_variable("face--new-frame-defaults", defaults);
        self.lface_id_to_name = vec![default];
        // GNU Finternal_make_lisp_face owns both directions of the
        // Lisp-face-ID mapping: the native hash entry above and the
        // symbol's `face' property.  The precreated default face must
        // enter through the same lifecycle as later faces.
        self.put_symbol_property("default", "face", Value::Integer(0));
        let frame_faces = crate::lisp::json::make_hash_table(
            self,
            "eq",
            vec![(default, tty_default_lisp_face_vector())],
        );
        self.selected_frame_id.borrow_mut().face_hash_table = Some(frame_faces);
        self.define_special_variable("face-filters-always-match", Value::Nil);
        self.define_special_variable("face-default-stipple", Value::String("gray3".into()));
        self.define_special_variable("scalable-fonts-allowed", Value::Nil);
        self.define_special_variable("face-ignored-fonts", Value::Nil);
        self.define_special_variable("face-remapping-alist", Value::Nil);
        self.define_special_variable("face-font-rescale-alist", Value::Nil);
        self.define_special_variable("face-near-same-color-threshold", Value::Integer(30_000));
        self.define_special_variable("face-font-lax-matched-attributes", Value::T);
    }

    /// xfaces.c:init_xfaces after a dump loads: lface_id_to_name from
    /// Vface_new_frame_defaults, each face at its id.
    pub(crate) fn rebuild_lface_id_to_name(&mut self) {
        let Some(table) = self.face_new_frame_defaults() else {
            self.lface_id_to_name.clear();
            return;
        };
        let mut names = vec![Value::Nil; table.count()];
        for (_, face, spec) in table.entries() {
            if let Some((id, _)) = spec.cons_values()
                && let Kind::Integer(id) = id.kind()
                && let Some(slot) = usize::try_from(id).ok().and_then(|id| names.get_mut(id))
            {
                *slot = face;
            }
        }
        self.lface_id_to_name = names;
    }

    /// Vface_new_frame_defaults, while it holds a hash table.
    fn face_new_frame_defaults(&self) -> Option<HashTableRef> {
        let table = cached_symbol!("face--new-frame-defaults")
            .with(|symbol| self.global_binding_value_symbol(symbol))?;
        match table.kind() {
            Kind::HashTable(table) => Some(table),
            _ => None,
        }
    }

    /// lface_id_to_name: the face whose id is ID.
    pub(crate) fn lisp_face_name_of_id(&self, id: i64) -> Option<Value> {
        usize::try_from(id)
            .ok()
            .and_then(|id| self.lface_id_to_name.get(id))
            .copied()
            .filter(|face| !face.is_nil())
    }

    /// lface_from_face_name_no_resolve without the signal: FACE's
    /// definition on FRAME, or the one for new frames.
    pub(crate) fn lisp_face_vector_on(
        &self,
        face: Value,
        frame: Option<FrameRef>,
    ) -> Option<Value> {
        let env = Env::new();
        match frame {
            Some(frame) => {
                let table = frame.borrow().face_hash_table?;
                let Kind::HashTable(table) = table.kind() else {
                    return None;
                };
                self.equal_hash_lookup(table, &face, &env).ok()??
            }
            None => self
                .equal_hash_lookup(self.face_new_frame_defaults()?, &face, &env)
                .ok()???
                .cons_values()
                .map(|(_, lface)| lface),
        }
    }

    pub(crate) fn lisp_face_vector(&self, face: Value, global: bool) -> Option<Value> {
        self.lisp_face_vector_on(face, (!global).then_some(self.selected_frame_id))
    }

    /// Finternal_make_lisp_face: FACE's definition on FRAME (the one for
    /// new frames when FRAME is None), made with every attribute
    /// unspecified where FACE has none, else reset when RESET.  A face new
    /// to Vface_new_frame_defaults takes the next id, which its `face'
    /// property records.
    pub(crate) fn make_lisp_face_on(
        &mut self,
        face: Value,
        frame: Option<FrameRef>,
        reset: bool,
    ) -> Result<Value, LispError> {
        let env = Env::new();
        let global = match self.lisp_face_vector_on(face, None) {
            Some(global) => {
                if frame.is_none() && reset {
                    reset_lisp_face_vector(&global)?;
                }
                global
            }
            None => {
                let table = self.face_new_frame_defaults().ok_or_else(|| {
                    LispError::WrongTypeArgument("hash-table-p".into(), Value::Nil)
                })?;
                let id = self.lface_id_to_name.len() as i64;
                self.lface_id_to_name.push(face);
                if let Kind::Symbol(name) = face.kind() {
                    self.put_symbol_property_on(&name, "face", Value::Integer(id));
                }
                let global = empty_lisp_face_vector();
                self.equal_hash_put(table, face, Value::cons(Value::Integer(id), global), &env)?;
                global
            }
        };
        self.face_change_count += 1;
        let Some(frame) = frame else {
            return Ok(global);
        };
        let Kind::HashTable(table) = self.frame_face_hash_table(frame).kind() else {
            unreachable!("a frame's face table is a hash table");
        };
        match self.equal_hash_lookup(table, &face, &env)?.flatten() {
            Some(lface) => {
                if reset {
                    reset_lisp_face_vector(&lface)?;
                }
                Ok(lface)
            }
            None => {
                let lface = empty_lisp_face_vector();
                self.equal_hash_put(table, face, lface, &env)?;
                Ok(lface)
            }
        }
    }

    /// The frame's face_hash_table, made empty for a frame that has none.
    pub(crate) fn frame_face_hash_table(&mut self, frame: FrameRef) -> Value {
        if let Some(table) = frame.borrow().face_hash_table {
            return table;
        }
        let table = crate::lisp::json::make_hash_table(self, "eq", Vec::new());
        frame.borrow_mut().face_hash_table = Some(table);
        table
    }

    pub(crate) fn face_definitions_generation(&self) -> u64 {
        self.face_change_count
    }

    /// Store VALUE in attribute INDEX of FACE's definition on FRAME (the
    /// one for new frames when FRAME is None), making a frame's missing
    /// definition first as Finternal_set_lisp_face_attribute does.
    pub(crate) fn set_lisp_face_attribute_on(
        &mut self,
        face: Value,
        index: usize,
        value: Value,
        frame: Option<FrameRef>,
    ) -> Result<Value, LispError> {
        let vector = self.make_lisp_face_on(face, frame, false)?;
        aset_vector_value(&vector, index, value)?;
        self.face_change_count += 1;
        Ok(value)
    }

    /// Finternal_copy_lisp_face: FROM's definition on SOURCE_FRAME copied
    /// over TO's, made or reset on TARGET_FRAME (None for both: the
    /// definitions for new frames).
    pub(crate) fn copy_lisp_face_attributes(
        &mut self,
        from: Value,
        to: Value,
        source_frame: Option<FrameRef>,
        target_frame: Option<FrameRef>,
    ) -> Result<(), LispError> {
        let source = self
            .lisp_face_vector_on(from, source_frame)
            .ok_or_else(|| invalid_face_error(from))?;
        let target = self.make_lisp_face_on(to, target_frame, true)?;
        for index in 0..LFACE_VECTOR_SIZE {
            aset_vector_value(&target, index, vector_slot_value(&source, index)?)?;
        }
        Ok(())
    }

    /// frame.c:make_terminal_frame: TO's face_hash_table is a copy of
    /// FROM's with each definition copied too (Fcopy_hash_table, then
    /// Fcopy_sequence of every value).
    pub(crate) fn copy_frame_faces(&mut self, from: FrameRef, to: FrameRef) {
        let Kind::HashTable(source) = self.frame_face_hash_table(from).kind() else {
            unreachable!("a frame's face table is a hash table");
        };
        let table = source.copy();
        let entries = table.entries().collect::<Vec<_>>();
        for (slot, _, lface) in entries {
            if let Ok(slots) = vector_items(&lface) {
                table.set_value(slot, Value::vector(slots));
            }
        }
        to.borrow_mut().face_hash_table = Some(Value::HashTable(table));
    }
}

/// Every attribute of the lface vector LFACE back to `unspecified'.
fn reset_lisp_face_vector(lface: &Value) -> Result<(), LispError> {
    for slot in 1..LFACE_VECTOR_SIZE {
        aset_vector_value(lface, slot, Value::symbol("unspecified"))?;
    }
    Ok(())
}
