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

    /// alloc.c:compact_buffer of every live buffer, newest first, as a
    /// collection starts: a base buffer changed since its last compaction
    /// has its undo list cut by truncate_undo_list, unless the list is t.
    pub(crate) fn compact_buffers(&mut self, env: &mut Env) -> Result<(), LispError> {
        let mut ids: Vec<u64> = std::iter::once(self.current_buffer_id)
            .chain(self.inactive_buffers.iter().map(|(id, _)| *id))
            .collect();
        // all_buffers holds the newest buffer first.
        ids.sort_unstable_by(|a, b| b.cmp(a));
        for id in ids {
            if self.buffer_base_id(id).is_some() {
                continue;
            }
            let Some((modiff, compact, disabled)) = self.get_buffer_by_id(id).map(|buffer| {
                (
                    buffer.modified_tick(),
                    buffer.compact_tick(),
                    buffer.undo_recording_disabled(),
                )
            }) else {
                continue;
            };
            if compact == modiff {
                continue;
            }
            if !disabled {
                self.truncate_undo_list(id, env)?;
            }
            if let Some(mut buffer) = self.get_buffer_by_id_mut(id) {
                let modiff = buffer.modified_tick();
                buffer.set_compact_tick(modiff);
            }
        }
        Ok(())
    }

    /// undo.c:truncate_undo_list of buffer ID, made current for its values
    /// of `undo-limit', `undo-strong-limit' and `undo-outer-limit' and for
    /// `undo-outer-limit-function', with collection inhibited.
    fn truncate_undo_list(&mut self, id: u64, env: &mut Env) -> Result<(), LispError> {
        self.inhibit_garbage_collection();
        let saved = self.current_buffer_id;
        let mut result = self
            .set_current_buffer_id(id)
            .and_then(|()| self.truncate_current_undo_list(env));
        // record_unwind_current_buffer: back to the buffer if it lives.
        if self.has_buffer_id(saved) {
            let restored = self.set_current_buffer_id(saved);
            result = result.and(restored);
        }
        self.allow_garbage_collection();
        result
    }

    fn truncate_current_undo_list(&mut self, env: &mut Env) -> Result<(), LispError> {
        // sizeof (struct Lisp_Cons) and sizeof (struct Lisp_String).
        const CONS_SIZE: i64 = 16;
        const STRING_SIZE: i64 = 32;
        let element_size = |element: Value| -> i64 {
            let mut size = CONS_SIZE;
            if let Some((head, _)) = element.cons_values() {
                size += CONS_SIZE;
                if let Kind::StringObject(string) = head.kind() {
                    size += STRING_SIZE - 1 + string.borrow().len() as i64;
                }
            }
            size
        };
        let list = self.buffer.borrow().undo_list();
        let mut size: i64 = 0;
        let mut prev = Value::Nil;
        let mut next = list;
        // If the first element is an undo boundary, skip past it.
        if let Some((head, tail)) = next.cons_values()
            && head.is_nil()
        {
            size += CONS_SIZE;
            prev = next;
            next = tail;
        }
        // Always preserve at least the most recent undo record.
        while let Some((element, tail)) = next.cons_values()
            && !element.is_nil()
        {
            size += element_size(element);
            prev = next;
            next = tail;
        }
        // Past `undo-outer-limit' by the first boundary, the function is
        // offered the list.
        let outer = self
            .lookup_var_key(cached_symbol!("undo-outer-limit"), env)
            .unwrap_or(Value::Nil);
        let past_outer = match outer.kind() {
            Kind::Integer(limit) => limit < size,
            Kind::BigInteger(limit) => match num_traits::ToPrimitive::to_i64(&*limit) {
                Some(limit) => limit < size,
                None => limit.sign() == num_bigint::Sign::Minus,
            },
            _ => false,
        };
        let function = self
            .lookup_var_key(cached_symbol!("undo-outer-limit-function"), env)
            .unwrap_or(Value::Nil);
        if past_outer && !function.is_nil() {
            let handled = self.call_function_value(function, None, &[Value::Integer(size)], env)?;
            if !handled.is_nil() {
                return Ok(());
            }
        }
        let mut last_boundary = if next.is_cons() { prev } else { Value::Nil };
        let limit_of = |interp: &Self, name: crate::lisp::types::symbols::SymbolKey| {
            interp
                .lookup_var_key(name, env)
                .and_then(|value| value.integer_value())
                .unwrap_or(0)
        };
        let undo_limit = limit_of(self, cached_symbol!("undo-limit"));
        let undo_strong_limit = limit_of(self, cached_symbol!("undo-strong-limit"));
        // Keep additional undo data, if it fits in the limits.
        while let Some((element, tail)) = next.cons_values() {
            if element.is_nil() {
                if size > undo_strong_limit {
                    break;
                }
                last_boundary = prev;
                if size > undo_limit {
                    break;
                }
            }
            size += element_size(element);
            prev = next;
            next = tail;
        }
        if next.is_nil() {
            // The whole list is short enough.
        } else if !last_boundary.is_nil() {
            last_boundary.set_cdr(Value::Nil)?;
        } else {
            self.buffer.borrow_mut().set_undo_list(Value::Nil);
        }
        Ok(())
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

    /// BUF_PT, BUF_BEGV and BUF_ZV of the rest of RELATED, the current
    /// buffer's indirect family, after an edit of their one text: where
    /// their markers now stand.
    pub(crate) fn fetch_family_positions(&mut self, related: &[u64]) {
        if related.len() < 2 {
            return;
        }
        let current_id = self.current_buffer_id();
        for buffer_id in related {
            if *buffer_id != current_id
                && let Some(mut buffer) = self.get_buffer_by_id_mut(*buffer_id)
            {
                buffer.fetch_position_markers();
            }
        }
    }

    /// insdel.c's adjust_overlays_for_insert over the rest of RELATED,
    /// whose overlays are their own over the one text, then their
    /// positions.
    fn family_after_insert(
        &mut self,
        related: &[u64],
        pos: usize,
        nchars: usize,
        before_markers: bool,
    ) {
        if related.len() < 2 {
            return;
        }
        let current_id = self.current_buffer_id();
        for buffer_id in related {
            if *buffer_id != current_id
                && let Some(mut buffer) = self.get_buffer_by_id_mut(*buffer_id)
            {
                buffer.overlays.insert_gap(pos, nchars, before_markers);
            }
        }
        self.fetch_family_positions(related);
    }

    /// adjust_overlays_for_delete over the rest of RELATED (their empty
    /// overlays that evaporate going), then their positions.
    fn family_after_delete(&mut self, related: &[u64], from: usize, to: usize) {
        if related.len() < 2 {
            return;
        }
        let current_id = self.current_buffer_id();
        for buffer_id in related {
            if *buffer_id != current_id
                && let Some(mut buffer) = self.get_buffer_by_id_mut(*buffer_id)
            {
                buffer.overlays.delete_gap(from, to - from);
            }
        }
        self.fetch_family_positions(related);
    }

    /// An edit of the current buffer's text where it stands (casing,
    /// subst-char-in-region, translate-region, transpose-regions): the rest
    /// of its family shares the text and its markers, and reads its point
    /// and bounds from them.
    pub(crate) fn family_after_in_place_edit(&mut self) {
        if self.indirect_buffers.is_empty() {
            return;
        }
        let related = self.related_buffer_ids(self.current_buffer_id());
        self.fetch_family_positions(&related);
    }

    /// A change of the text properties of TARGET_ID's text, which its
    /// indirect family shares.  Whether the buffer is live.
    pub fn apply_text_property_change_shared_for(
        &mut self,
        target_id: u64,
        apply: &dyn Fn(&mut crate::buffer::Buffer),
    ) -> bool {
        if target_id == self.current_buffer_id() {
            apply(&mut self.buffer.borrow_mut());
            return true;
        }
        if let Some(mut buffer) = self.get_buffer_by_id_mut(target_id) {
            apply(&mut buffer);
            return true;
        }
        false
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
        self.family_after_insert(&related, pos, nchars, before_markers);
    }

    pub fn insert_current_buffer(&mut self, s: &str) {
        self.insert_current_buffer_with(s, crate::buffer::InsertedProperties::PLAIN, false);
    }

    /// insert_from_gap_1 of BYTES at point in the current unibyte buffer,
    /// their characters written straight into its gap.
    pub(crate) fn insert_current_buffer_raw_bytes(&mut self, bytes: &[u8]) {
        let pos = self.buffer.borrow().point();
        let related = self.related_buffer_ids(self.current_buffer_id());
        let vars = self.insertion_property_variables();
        let nchars = self.buffer.borrow_mut().insert_raw_bytes(bytes, &vars) - pos;
        self.adjust_markers_for_insert(self.current_buffer_id(), pos, nchars, false);
        self.family_after_insert(&related, pos, nchars, false);
    }

    /// Attach non-Unicode GNU character codes to text just inserted into the
    /// current buffer and every indirect/base buffer sharing that text.
    pub(crate) fn set_inserted_extended_chars(&mut self, start: usize, chars: &[(usize, u32)]) {
        if chars.is_empty() {
            return;
        }
        self.buffer
            .borrow_mut()
            .set_inserted_extended_chars(start, chars);
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
        let (from, to, multibyte) = {
            let buffer = self.buffer.borrow();
            let from = from.max(buffer.point_min());
            (
                from,
                to.min(buffer.point_max()).max(from),
                buffer.is_multibyte(),
            )
        };
        // copy_text: a unibyte buffer takes each character's byte.
        let unibyte_copy = if multibyte {
            None
        } else {
            crate::lisp::primitives::unibyte_buffer_copy(text, extended)
        };
        let (text, extended) = match &unibyte_copy {
            Some(copy) => (copy.as_str(), &[][..]),
            None => (text, extended),
        };
        let inschars = text.chars().count();
        let vars = self.insertion_property_variables();
        self.buffer
            .borrow_mut()
            .replace_range(from, to, text, extended, true, props, &vars);
        if related.len() > 1 {
            // replace_range's adjust_overlays_for_insert at TO, then for
            // the deletion, in each other buffer of the family.
            for buffer_id in &related {
                if *buffer_id != current_id
                    && let Some(mut buffer) = self.get_buffer_by_id_mut(*buffer_id)
                {
                    buffer.overlays.insert_gap(to, inschars, true);
                    if to > from {
                        buffer.overlays.delete_gap(from, to - from);
                    }
                }
            }
            self.fetch_family_positions(&related);
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

    /// `Buffer::delete_chars_each' in the current buffer, its windows'
    /// points and starts and its indirect family moved as each deletion
    /// moves them.
    pub(crate) fn delete_chars_each_current_buffer(&mut self, positions: &[usize]) {
        if positions.is_empty() {
            return;
        }
        let related = self.related_buffer_ids(self.current_buffer_id());
        self.buffer.borrow_mut().delete_chars_each(positions);
        for &position in positions {
            self.family_after_delete(&related, position, position + 1);
        }
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
        if from < to {
            self.family_after_delete(&related, from, to);
        }
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
