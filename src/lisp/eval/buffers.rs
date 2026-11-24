use super::*;

/// buffer.c's `CHECK_BUFFER' and `BUFFER_LIVE_P' of VALUE: the buffer
/// VALUE is, when it is a live one.
#[inline]
pub(crate) fn decode_live_buffer(value: &Value) -> Option<crate::lisp::types::BufferRef> {
    match value.kind() {
        Kind::Buffer(buffer) if buffer.is_live() => Some(buffer),
        _ => None,
    }
}

impl Interpreter {
    /// buffer.c:Fmake_indirect_buffer's `bset_undo_list (b, BVAR
    /// (b->base_buffer, undo_list))': the new buffer shares its base's
    /// undo list (Bug#18180).
    pub fn take_base_undo_list(&mut self, buffer_id: u64, base_id: u64) {
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
            .symbol_value_key(cached_symbol!("undo-outer-limit"))
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
            .symbol_value_key(cached_symbol!("undo-outer-limit-function"))
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
                .symbol_value_key(name)
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
        let point = self.current_buffer().pt();
        self.buffer
            .borrow_mut()
            .set_point_before_last_command_or_undo(Some(point));
    }

    /// The base of buffer BUFFER_ID, a live indirect buffer, while the
    /// base is live too.
    pub fn buffer_base_id(&self, buffer_id: u64) -> Option<u64> {
        self.buffer_object(buffer_id)?
            .text_base()
            .filter(crate::lisp::types::BufferRef::is_live)
            .map(|base| base.id)
    }

    pub fn root_buffer_id(&self, mut buffer_id: u64) -> u64 {
        while let Some(base_id) = self.buffer_base_id(buffer_id) {
            buffer_id = base_id;
        }
        buffer_id
    }

    /// The buffers of Vbuffer_alist that share the text of buffer
    /// BUFFER_ID, in the list's order: its indirect family, or the buffer
    /// alone when its text is its own (the current buffer counted even
    /// when the list does not hold it).
    pub fn related_buffer_ids(&self, buffer_id: u64) -> smallvec::SmallVec<[u64; 4]> {
        let root = self.buffer_object(buffer_id).and_then(|buffer| {
            let root = buffer
                .text_base()
                .filter(crate::lisp::types::BufferRef::is_live)
                .unwrap_or(buffer);
            (root.is_live() && root.indirections() > 0).then_some(root)
        });
        let Some(root) = root else {
            if buffer_id == self.current_buffer_id {
                return smallvec::smallvec![buffer_id];
            }
            return self
                .buffer_list
                .iter()
                .map(|(id, _)| *id)
                .filter(|id| *id == buffer_id)
                .collect();
        };
        // insdel.c's FOR_EACH_LIVE_BUFFER walk for the buffers whose
        // base_buffer is ROOT.
        let family: smallvec::SmallVec<[u64; 4]> = std::iter::once(self.buffer)
            .chain(self.inactive_buffers.iter().map(|(_, buffer)| *buffer))
            .filter(|buffer| {
                buffer.ptr_eq(&root) || buffer.text_base().is_some_and(|base| base.ptr_eq(&root))
            })
            .map(|buffer| buffer.id)
            .collect();
        self.buffer_list
            .iter()
            .map(|(id, _)| *id)
            .filter(|id| family.contains(id))
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
        let aliases = self.property_alias_alist();
        for buffer_id in related {
            if *buffer_id != current_id
                && let Some(mut buffer) = self.get_buffer_by_id_mut(*buffer_id)
            {
                buffer.overlays.delete_gap_with(from, to - from, aliases);
            }
        }
        self.fetch_family_positions(related);
    }

    /// An edit of the current buffer's text where it stands (casing,
    /// subst-char-in-region, translate-region, transpose-regions): the rest
    /// of its family shares the text and its markers, and reads its point
    /// and bounds from them.
    pub(crate) fn family_after_in_place_edit(&mut self) {
        if !self.current_buffer().shares_text() {
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

    /// Vchar_property_alias_alist as the current buffer holds it.
    pub(crate) fn property_alias_alist(&self) -> Value {
        self.symbol_value_key(cached_symbol!("char-property-alias-alist"))
            .unwrap_or(Value::Nil)
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

    /// insdel.c's adjust_suspend_auto_hscroll, which each adjustment of
    /// the markers for a change from FROM to TO in the current buffer
    /// makes before it moves them: when the selected window shows the
    /// buffer and its old point (w->old_pointm) is in the change or at
    /// its ends, auto-hscroll works there again (suspend_auto_hscroll,
    /// which `set-window-hscroll' sets, cleared).
    pub(crate) fn adjust_suspend_auto_hscroll(&mut self, from: usize, to: usize) {
        if self
            .suspended_old_point()
            .is_some_and(|old_point| from <= old_point && old_point <= to)
        {
            self.resume_auto_hscroll();
        }
    }

    /// The selected window's old point while its auto-hscroll is
    /// suspended and it shows the current buffer: what
    /// adjust_suspend_auto_hscroll compares a change with.
    fn suspended_old_point(&self) -> Option<usize> {
        self.suspended_old_point_in(self.current_buffer_id())
    }

    /// `suspended_old_point' for a change made in BUFFER_ID as the
    /// current buffer.
    pub(crate) fn suspended_old_point_in(&self, buffer_id: u64) -> Option<usize> {
        use crate::lisp::primitives::WindowMarker;
        let window = self.selected_window()?;
        if !window.suspend_auto_hscroll() || window.live_buffer_id(self) != Some(buffer_id) {
            return None;
        }
        crate::lisp::primitives::window_marker_position(window, WindowMarker::OldPointm)
    }

    /// The selected window's w->suspend_auto_hscroll.
    pub(crate) fn auto_hscroll_suspended(&self) -> bool {
        self.selected_window()
            .is_some_and(crate::lisp::primitives::WindowRef::suspend_auto_hscroll)
    }

    /// w->suspend_auto_hscroll set again for the selected window, after
    /// edits made in another order than GNU's that its own check, made
    /// apart, found leave it suspended.
    pub(crate) fn suspend_auto_hscroll_again(&mut self) {
        if let Some(window) = self.selected_window() {
            window.set_suspend_auto_hscroll(true);
        }
    }

    /// w->suspend_auto_hscroll = 0 for the selected window.
    pub(crate) fn resume_auto_hscroll(&mut self) {
        if let Some(window) = self.selected_window() {
            window.set_suspend_auto_hscroll(false);
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
        let pos = self.current_buffer().pt();
        let related = self.related_buffer_ids(self.current_buffer_id());
        let vars = self.insertion_property_variables();
        // adjust_markers_for_insert (PT, ..., PT + nchars, ...).
        if let Some(old_point) = self.suspended_old_point()
            && pos <= old_point
            && old_point <= pos + s.chars().count()
        {
            self.resume_auto_hscroll();
        }
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
        let pos = self.current_buffer().pt();
        let related = self.related_buffer_ids(self.current_buffer_id());
        let vars = self.insertion_property_variables();
        // The insertion's adjust_markers_for_insert, a character a byte.
        self.adjust_suspend_auto_hscroll(pos, pos + bytes.len());
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
        // replace_range's adjust_markers_for_replace (FROM, ..., TO - FROM,
        // ...), after its return when nothing is deleted or inserted.
        if to > from || inschars > 0 {
            self.adjust_suspend_auto_hscroll(from, to);
        }
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
                        buffer
                            .overlays
                            .delete_gap_with(from, to - from, vars.aliases);
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
        // Each deletion's adjust_markers_for_delete, against the old point
        // as the deletions before it have moved it.
        if let Some(mut old_point) = self.suspended_old_point() {
            for &position in positions {
                if position <= old_point && old_point <= position + 1 {
                    self.resume_auto_hscroll();
                    break;
                }
                if old_point > position {
                    old_point -= 1;
                }
            }
        }
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
        let from = from.max(self.current_buffer().begv());
        let to = to.min(self.current_buffer().zv());
        // undo.c:record_delete's record_marker_adjustments, read before
        // the deletion moves the markers and recorded under it after.
        let marker_adjustments = if self.buffer.borrow().undo_recording_disabled() {
            Vec::new()
        } else {
            self.marker_adjustments_for_delete(self.current_buffer_id(), from, to)
        };
        let related = self.related_buffer_ids(self.current_buffer_id());
        // evaporate_overlays' Foverlay_get reads the alternatives.
        let aliases = self.property_alias_alist();
        // del_range_2's adjust_markers_for_delete.
        if from < to {
            self.adjust_suspend_auto_hscroll(from, to);
        }
        let deleted = self
            .buffer
            .borrow_mut()
            .delete_region_aliased(from, to, ret_string, aliases)?;
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

    /// buffer.c:Fbuffer_swap_text's exchange of the two buffers' texts
    /// and the fields that go with them (`Buffer::swap_text_state'), then
    /// `swapfield (indirections, ptrdiff_t)'; each buffer then reads its
    /// point and bounds from the markers it was given unless it is the
    /// current one.
    pub fn swap_buffer_text_state(&mut self, left_id: u64, right_id: u64) -> Result<(), LispError> {
        if left_id == right_id {
            return Ok(());
        }
        let object = |interp: &Self, id: u64| {
            interp
                .buffer_object(id)
                .ok_or_else(|| LispError::Signal(format!("No buffer with id {}", id)))
        };
        let left = object(self, left_id)?;
        let right = object(self, right_id)?;
        left.borrow_mut().swap_text_state(&mut right.borrow_mut());
        // No indirect buffer is in sight, so both counts are 0.
        let indirections = left.indirections.get();
        left.indirections.set(right.indirections.get());
        right.indirections.set(indirections);
        for buffer in [left, right] {
            let current = buffer.ptr_eq(&self.buffer);
            buffer.borrow_mut().take_swapped_position_markers(current);
        }
        Ok(())
    }

    /// buffer.c:delete_all_overlays unlinks objects without destroying them.
    /// buffer.c:delete_all_overlays: each overlay's region shown anew
    /// (modify_overlay), then the buffer left without overlays.
    pub(crate) fn delete_buffer_overlays(&mut self, id: u64) {
        let regions: Vec<(usize, usize)> = self
            .get_buffer_by_id(id)
            .map(|buffer| {
                buffer
                    .overlays
                    .iter()
                    .map(|overlay| (overlay.beg(), overlay.end()))
                    .collect()
            })
            .unwrap_or_default();
        for (beg, end) in regions {
            self.modify_overlay(id, beg, end);
        }
        if let Some(mut buffer) = self.get_buffer_by_id_mut(id) {
            buffer.overlays.clear();
        }
    }
}
