use super::*;

impl Interpreter {
    /// buffer.c:Fmake_indirect_buffer's link to the base, whose undo list
    /// the new buffer shares (Bug#18180).
    pub fn register_indirect_buffer(&mut self, buffer_id: u64, base_id: u64) {
        self.indirect_buffers.push((buffer_id, base_id));
        let base_undo = self.get_buffer_by_id(base_id).map(|base| base.undo_list());
        if let Some(list) = base_undo
            && let Some(mut buffer) = self.get_buffer_by_id_mut(buffer_id)
        {
            buffer.set_undo_list(list);
        }
    }

    /// undo.c:Fundo_boundary.
    pub(crate) fn undo_boundary(&mut self, env: &mut Env) -> Result<Value, LispError> {
        if self.buffer.borrow().undo_recording_disabled() {
            return Ok(Value::Nil);
        }
        if let Err(list) = self.buffer.borrow_mut().undo_boundary() {
            return Err(LispError::WrongTypeArgument("listp".into(), list));
        }
        // Fset (Qundo_auto__last_boundary_cause, Qexplicit).
        let cause = cached_symbol!("undo-auto--last-boundary-cause").with(Clone::clone);
        let explicit = Value::Symbol(cached_symbol!("explicit").with(Clone::clone));
        crate::lisp::primitives::set_internal_symbol(self, &cause, explicit, env)?;
        self.note_point_before_last_command_or_undo();
        Ok(Value::Nil)
    }

    /// keyboard.c's `point_before_last_command_or_undo = PT;
    /// buffer_before_last_command_or_undo = current_buffer;', which the
    /// command loop and `undo-boundary' make: the buffer that held the
    /// point before gives it up.
    pub(crate) fn note_point_before_last_command_or_undo(&mut self) {
        let current = self.current_buffer_id();
        if let Some(previous) = self.buffer_before_last_command_or_undo.replace(current)
            && previous != current
            && let Some(mut buffer) = self.get_buffer_by_id_mut(previous)
        {
            buffer.set_point_before_last_command_or_undo(None);
        }
        let point = self.buffer.borrow().point();
        self.buffer
            .borrow_mut()
            .set_point_before_last_command_or_undo(Some(point));
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

    pub fn related_buffer_ids(&self, buffer_id: u64) -> smallvec::SmallVec<[u64; 4]> {
        if self.indirect_buffers.is_empty() {
            // Every buffer is its own root: BUFFER_ID's text is its own,
            // and the current buffer is live.
            if buffer_id == self.current_buffer_id {
                return smallvec::smallvec![buffer_id];
            }
            return self
                .buffer_list
                .iter()
                .map(|(id, _)| *id)
                .filter(|id| *id == buffer_id)
                .collect();
        }
        let root = self.root_buffer_id(buffer_id);
        self.buffer_list
            .iter()
            .map(|(id, _)| *id)
            .filter(|id| self.root_buffer_id(*id) == root)
            .collect()
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn mirror_insert_to_related_buffers(
        &mut self,
        related: &[u64],
        pos: usize,
        s: &str,
        nchars: usize,
        props: crate::buffer::InsertedProperties<'_>,
        before_markers: bool,
        vars: &crate::intervals::PropertyVariables,
    ) {
        let current_id = self.current_buffer_id();
        for buffer_id in related {
            if *buffer_id == current_id {
                continue;
            }
            if let Some(mut buffer) = self.get_buffer_by_id_mut(*buffer_id) {
                let saved_point = buffer.point();
                buffer.goto_char(pos);
                // The text is one text in GNU, the change recorded once.
                let undo_list = buffer.undo_list();
                buffer.set_undo_list(Value::T);
                buffer.insert_with(s, props, before_markers, vars);
                buffer.set_undo_list(undo_list);
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

    pub(super) fn mirror_delete_to_related_buffers(
        &mut self,
        related: &[u64],
        from: usize,
        to: usize,
    ) {
        let current_id = self.current_buffer_id();
        for buffer_id in related {
            if *buffer_id == current_id {
                continue;
            }
            if let Some(mut buffer) = self.get_buffer_by_id_mut(*buffer_id) {
                let saved_point = buffer.point();
                let undo_list = buffer.undo_list();
                buffer.set_undo_list(Value::T);
                let _ = buffer.delete_region(from, to);
                buffer.set_undo_list(undo_list);
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

    /// The same family-wide property mirroring for an explicit target
    /// buffer.  Returns whether any buffer received the change.
    pub fn apply_text_property_change_shared_for(
        &mut self,
        target_id: u64,
        apply: &dyn Fn(&mut crate::buffer::Buffer),
    ) -> bool {
        let current_id = self.current_buffer_id();
        if self.indirect_buffers.is_empty() {
            if target_id == current_id {
                apply(&mut self.buffer.borrow_mut());
                return true;
            }
            if let Some(mut buffer) = self.get_buffer_by_id_mut(target_id) {
                apply(&mut buffer);
                return true;
            }
            return false;
        }
        let mut applied = false;
        for buffer_id in self.related_buffer_ids(target_id) {
            if buffer_id == current_id {
                apply(&mut self.buffer.borrow_mut());
                applied = true;
            } else if let Some(mut buffer) = self.get_buffer_by_id_mut(buffer_id) {
                apply(&mut buffer);
                applied = true;
            }
        }
        applied
    }

    /// The variables an insertion into the current buffer reads, when its
    /// text has intervals for them to matter to.
    pub(crate) fn insertion_property_variables(&self) -> crate::intervals::PropertyVariables {
        if self.buffer.borrow().has_interval_tree() {
            crate::intervals::PropertyVariables::read(self)
        } else {
            crate::intervals::PropertyVariables::NONE
        }
    }

    /// insdel.c's insertion of S at point in the current buffer and the
    /// rest of its indirect family, PROPS what its intervals get, markers
    /// at point advanced with BEFORE_MARKERS.
    pub(crate) fn insert_current_buffer_with(
        &mut self,
        s: &str,
        props: crate::buffer::InsertedProperties<'_>,
        before_markers: bool,
    ) {
        let pos = self.buffer.borrow().point();
        let related = self.related_buffer_ids(self.current_buffer_id());
        let vars = self.insertion_property_variables();
        let nchars = self
            .buffer
            .borrow_mut()
            .insert_with(s, props, before_markers, &vars)
            - pos;
        self.adjust_markers_for_insert(self.current_buffer_id(), pos, nchars, before_markers);
        self.mirror_insert_to_related_buffers(
            &related,
            pos,
            s,
            nchars,
            props,
            before_markers,
            &vars,
        );
    }

    pub fn insert_current_buffer(&mut self, s: &str) {
        self.insert_current_buffer_with(s, crate::buffer::InsertedProperties::PLAIN, false);
    }

    /// `insert_current_buffer' of a text whose COUNTS its maker has.
    pub(crate) fn insert_current_buffer_counted(
        &mut self,
        s: &str,
        counts: crate::buffer::TextCounts,
    ) {
        let pos = self.buffer.borrow().point();
        let related = self.related_buffer_ids(self.current_buffer_id());
        let vars = self.insertion_property_variables();
        let nchars = self
            .buffer
            .borrow_mut()
            .insert_counted_with(s, counts, &vars)
            - pos;
        self.adjust_markers_for_insert(self.current_buffer_id(), pos, nchars, false);
        self.mirror_insert_to_related_buffers(
            &related,
            pos,
            s,
            nchars,
            crate::buffer::InsertedProperties::PLAIN,
            false,
            &vars,
        );
    }

    /// Attach non-Unicode GNU character codes to text just inserted into the
    /// current buffer and every indirect/base buffer sharing that text.
    pub(crate) fn set_inserted_extended_chars(&mut self, start: usize, chars: &[(usize, u32)]) {
        if chars.is_empty() {
            return;
        }
        let current_id = self.current_buffer_id();
        let related = self.related_buffer_ids(current_id);
        self.buffer
            .borrow_mut()
            .set_inserted_extended_chars(start, chars);
        for buffer_id in related {
            if buffer_id == current_id {
                continue;
            }
            if let Some(mut buffer) = self.get_buffer_by_id_mut(buffer_id) {
                buffer.set_inserted_extended_chars(start, chars);
            }
        }
    }

    /// insdel.c:replace_range's change of the current buffer (see
    /// `Buffer::replace_range'), made through its whole indirect-buffer
    /// family; another member's point moves as a marker does.
    pub(crate) fn replace_range_current_buffer(
        &mut self,
        from: usize,
        to: usize,
        text: &str,
        extended: &[(usize, u32)],
        props: crate::buffer::InsertedProperties<'_>,
    ) -> usize {
        let current_id = self.current_buffer_id();
        let related = self.related_buffer_ids(current_id);
        let (from, to) = {
            let buffer = self.buffer.borrow();
            let from = from.max(buffer.point_min());
            (from, to.min(buffer.point_max()).max(from))
        };
        let inschars = text.chars().count();
        let vars = self.insertion_property_variables();
        self.buffer
            .borrow_mut()
            .replace_range(from, to, text, extended, true, props, &vars);
        self.adjust_markers_for_replace(current_id, from, to, inschars);
        for buffer_id in &related {
            if *buffer_id == current_id {
                continue;
            }
            if let Some(mut buffer) = self.get_buffer_by_id_mut(*buffer_id) {
                let saved_point = buffer.point();
                buffer.replace_range(from, to, text, extended, true, props, &vars);
                let restored = if saved_point >= to {
                    saved_point + inschars - (to - from)
                } else if saved_point > from {
                    from
                } else {
                    saved_point
                };
                buffer.goto_char(restored);
            }
            self.adjust_markers_for_replace(*buffer_id, from, to, inschars);
        }
        from
    }

    pub fn delete_region_current_buffer(
        &mut self,
        from: usize,
        to: usize,
    ) -> Result<String, crate::buffer::BufferError> {
        self.delete_region_current_buffer_1(from, to, true)
            .map(Option::unwrap_or_default)
    }

    /// `delete_region_current_buffer' for a caller that has no use for the
    /// deleted text (del_range_1 without RET_STRING).
    pub fn delete_region_current_buffer_unreturned(
        &mut self,
        from: usize,
        to: usize,
    ) -> Result<(), crate::buffer::BufferError> {
        self.delete_region_current_buffer_1(from, to, false)
            .map(drop)
    }

    fn delete_region_current_buffer_1(
        &mut self,
        from: usize,
        to: usize,
        ret_string: bool,
    ) -> Result<Option<String>, crate::buffer::BufferError> {
        let from = from.max(self.buffer.borrow().point_min());
        let to = to.min(self.buffer.borrow().point_max());
        // undo.c:record_delete's record_marker_adjustments, read before
        // the deletion moves the markers and recorded under it after.
        let marker_adjustments = if self.buffer.borrow().undo_recording_disabled() {
            Vec::new()
        } else {
            self.marker_adjustments_for_delete(self.current_buffer_id(), from, to)
        };
        let related = self.related_buffer_ids(self.current_buffer_id());
        let deleted = if ret_string {
            Some(self.buffer.borrow_mut().delete_region(from, to)?)
        } else {
            self.buffer
                .borrow_mut()
                .delete_region_unreturned(from, to)?;
            None
        };
        self.buffer
            .borrow_mut()
            .record_marker_adjustments(marker_adjustments);
        self.adjust_markers_for_delete(self.current_buffer_id(), from, to);
        self.mirror_delete_to_related_buffers(&related, from, to);
        Ok(deleted)
    }

    /// undo.c:record_marker_adjustments: (MARKER . ADJUSTMENT) for each
    /// marker in [FROM, TO] that undoing the deletion would not put back
    /// where it is, ADJUSTMENT taking it from where the reinsertion leaves
    /// it (TO for an insertion type t marker, FROM for another).
    fn marker_adjustments_for_delete(&self, buffer_id: u64, from: usize, to: usize) -> Vec<Value> {
        self.buffer_object(buffer_id)
            .into_iter()
            .flat_map(|buffer| buffer.markers())
            .filter_map(|marker| {
                let charpos = marker.position()?;
                if charpos < from || charpos > to {
                    return None;
                }
                let automatic = if marker.insertion_type() { to } else { from };
                let adjustment = automatic as i64 - charpos as i64;
                (adjustment != 0)
                    .then(|| Value::cons(Value::Marker(marker), Value::Integer(adjustment)))
            })
            .collect()
    }

    /// Return the actual live object; constructing a Lisp reference allocates nothing.
    pub fn buffer_object(&self, id: u64) -> Option<crate::lisp::types::BufferRef> {
        if id == self.current_buffer_id {
            Some(self.buffer)
        } else {
            self.inactive_buffers
                .iter()
                .find(|(buffer_id, _)| *buffer_id == id)
                .map(|(_, buffer)| *buffer)
        }
    }

    /// Borrow a live buffer by ID.
    pub fn get_buffer_by_id(&self, id: u64) -> Option<std::cell::Ref<'_, crate::buffer::Buffer>> {
        if id == self.current_buffer_id {
            Some(self.buffer.borrow())
        } else {
            self.inactive_buffers
                .iter()
                .find(|(buffer_id, _)| *buffer_id == id)
                .map(|(_, buffer)| buffer.borrow())
        }
    }

    /// Borrow a live buffer mutably by ID.
    pub fn get_buffer_by_id_mut(
        &mut self,
        id: u64,
    ) -> Option<std::cell::RefMut<'_, crate::buffer::Buffer>> {
        if id == self.current_buffer_id {
            Some(self.buffer.borrow_mut())
        } else {
            self.inactive_buffers
                .iter_mut()
                .find(|(buffer_id, _)| *buffer_id == id)
                .map(|(_, buffer)| buffer.borrow_mut())
        }
    }

    pub fn buffer_hooks_inhibited(&self, id: u64) -> bool {
        self.get_buffer_by_id(id)
            .map(|buffer| buffer.inhibit_hooks)
            .unwrap_or(false)
    }

    pub fn set_buffer_hooks_inhibited(&mut self, id: u64, inhibit: bool) {
        if let Some(mut buffer) = self.get_buffer_by_id_mut(id) {
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
            let state = &mut **self;
            let (buffer, inactive_buffers) = (&mut state.buffer, &mut state.inactive_buffers);
            buffer
                .borrow_mut()
                .swap_text_state(&mut inactive_buffers[pos].1.borrow_mut());
            return Ok(());
        }
        if right_id == self.current_buffer_id {
            let pos = self
                .inactive_buffers
                .iter()
                .position(|(buffer_id, _)| *buffer_id == left_id)
                .ok_or_else(|| LispError::Signal(format!("No buffer with id {}", left_id)))?;
            let state = &mut **self;
            let (buffer, inactive_buffers) = (&mut state.buffer, &mut state.inactive_buffers);
            buffer
                .borrow_mut()
                .swap_text_state(&mut inactive_buffers[pos].1.borrow_mut());
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
        first.borrow_mut().swap_text_state(&mut second.borrow_mut());
        Ok(())
    }

    /// buffer.c:delete_all_overlays unlinks objects without destroying them.
    pub(crate) fn delete_buffer_overlays(&mut self, id: u64) {
        if let Some(mut buffer) = self.get_buffer_by_id_mut(id) {
            buffer.overlays.clear();
        }
    }
}
