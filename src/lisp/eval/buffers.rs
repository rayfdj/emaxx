use super::*;

impl Interpreter {
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

    pub(super) fn mirror_insert_to_related_buffers(
        &mut self,
        related: &[u64],
        pos: usize,
        s: &str,
        props: Option<Vec<(String, Value)>>,
        before_markers: bool,
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
                if let Some(props) = props.clone() {
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

    /// GNU stores text properties in the intervals of the shared text
    /// itself, so a property change made through any member of an
    /// indirect-buffer family is visible in every other member (comint's
    /// input fontification reads the base buffer's `field' properties
    /// through its indirect buffer).  The native model keeps one buffer
    /// struct per member and mirrors text edits between them;
    /// property-only edits must mirror the same way.
    pub fn apply_text_property_change_shared(
        &mut self,
        apply: &dyn Fn(&mut crate::buffer::Buffer),
    ) {
        apply(&mut self.buffer);
        if self.indirect_buffers.is_empty() {
            return;
        }
        let current_id = self.current_buffer_id();
        for buffer_id in self.related_buffer_ids(current_id) {
            if buffer_id == current_id {
                continue;
            }
            if let Some(buffer) = self.get_buffer_by_id_mut(buffer_id) {
                apply(buffer);
            }
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
                apply(&mut self.buffer);
                return true;
            }
            if let Some(buffer) = self.get_buffer_by_id_mut(target_id) {
                apply(buffer);
                return true;
            }
            return false;
        }
        let mut applied = false;
        for buffer_id in self.related_buffer_ids(target_id) {
            if buffer_id == current_id {
                apply(&mut self.buffer);
                applied = true;
            } else if let Some(buffer) = self.get_buffer_by_id_mut(buffer_id) {
                apply(buffer);
                applied = true;
            }
        }
        applied
    }

    pub fn insert_current_buffer(&mut self, s: &str) {
        let pos = self.buffer.point();
        let nchars = s.chars().count();
        let related = self.related_buffer_ids(self.current_buffer_id());
        self.buffer.insert(s);
        self.adjust_markers_for_insert(self.current_buffer_id(), pos, nchars, false);
        self.mirror_insert_to_related_buffers(&related, pos, s, None, false);
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
        self.mirror_insert_to_related_buffers(&related, pos, s, props, false);
    }

    pub fn insert_current_buffer_and_inherit(&mut self, s: &str) {
        let pos = self.buffer.point();
        let nchars = s.chars().count();
        let related = self.related_buffer_ids(self.current_buffer_id());
        let defaults = self.lookup_var("text-property-default-nonsticky", &Env::new());
        let props = self
            .buffer
            .inherited_text_properties(pos, defaults.as_ref());
        self.buffer.insert_with_properties(s, Some(props.clone()));
        self.adjust_markers_for_insert(self.current_buffer_id(), pos, nchars, false);
        self.mirror_insert_to_related_buffers(&related, pos, s, Some(props), false);
    }

    pub fn insert_current_buffer_before_markers(&mut self, s: &str) {
        let pos = self.buffer.point();
        let nchars = s.chars().count();
        let related = self.related_buffer_ids(self.current_buffer_id());
        self.buffer.insert(s);
        self.adjust_markers_for_insert(self.current_buffer_id(), pos, nchars, true);
        self.mirror_insert_to_related_buffers(&related, pos, s, None, true);
    }

    pub fn insert_current_buffer_before_markers_and_inherit(&mut self, s: &str) {
        let pos = self.buffer.point();
        let nchars = s.chars().count();
        let related = self.related_buffer_ids(self.current_buffer_id());
        let defaults = self.lookup_var("text-property-default-nonsticky", &Env::new());
        let props = self
            .buffer
            .inherited_text_properties(pos, defaults.as_ref());
        self.buffer.insert_with_properties(s, Some(props.clone()));
        self.adjust_markers_for_insert(self.current_buffer_id(), pos, nchars, true);
        self.mirror_insert_to_related_buffers(&related, pos, s, Some(props), true);
    }

    /// Attach non-Unicode GNU character codes to text just inserted into the
    /// current buffer and every indirect/base buffer sharing that text.
    pub(crate) fn set_inserted_extended_chars(&mut self, start: usize, chars: &[(usize, u32)]) {
        if chars.is_empty() {
            return;
        }
        let current_id = self.current_buffer_id();
        let related = self.related_buffer_ids(current_id);
        self.buffer.set_inserted_extended_chars(start, chars);
        for buffer_id in related {
            if buffer_id == current_id {
                continue;
            }
            if let Some(buffer) = self.get_buffer_by_id_mut(buffer_id) {
                buffer.set_inserted_extended_chars(start, chars);
            }
        }
    }

    pub fn delete_region_current_buffer(
        &mut self,
        from: usize,
        to: usize,
    ) -> Result<String, crate::buffer::BufferError> {
        let from = from.max(self.buffer.point_min());
        let to = to.min(self.buffer.point_max());
        let affected_markers = self.affected_markers_for_delete(self.current_buffer_id(), from, to);
        // undo.c records marker adjustments immediately before recording
        // the deletion, so the Lisp undo list exposes the deletion followed
        // directly by its marker riders — `primitive-undo' relies on that
        // adjacency, and the first-change `(t . TIME)' entry the deletion
        // itself records must stay below both.  The riders are spliced in
        // under the deletion record once it exists.
        let marker_adjustments: Vec<crate::buffer::UndoEntry> = if self.buffer.undo_enabled() {
            affected_markers
                .iter()
                .filter_map(|marker| {
                    let automatic_position = if self.marker_insertion_type(marker.id) == Some(true)
                    {
                        to
                    } else {
                        from
                    };
                    let adjustment = automatic_position as i64 - marker.original_pos as i64;
                    (adjustment != 0).then(|| {
                        crate::buffer::UndoEntry::Opaque(Value::cons(
                            Value::Marker(marker.id),
                            Value::Integer(adjustment),
                        ))
                    })
                })
                .collect()
        } else {
            Vec::new()
        };
        let related = self.related_buffer_ids(self.current_buffer_id());
        let deleted = self.buffer.delete_region(from, to)?;
        self.buffer
            .splice_undo_entries_before_last(marker_adjustments);
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

    pub(super) fn affected_markers_for_delete(
        &self,
        buffer_id: u64,
        from: usize,
        to: usize,
    ) -> Vec<crate::buffer::UndoMarker> {
        self.markers_by_buffer
            .get(&buffer_id)
            .into_iter()
            .flatten()
            .filter_map(|marker_id| {
                let marker = self.find_marker(*marker_id)?;
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
            let state = &mut **self;
            let (buffer, inactive_buffers) = (&mut state.buffer, &mut state.inactive_buffers);
            buffer.swap_text_state(&mut inactive_buffers[pos].1);
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

    /// Find an overlay by ID in any live buffer.
    pub fn find_overlay(&self, id: u64) -> Option<&crate::overlay::Overlay> {
        self.buffer
            .overlays
            .iter()
            .find(|ov| ov.id == id)
            .or_else(|| {
                self.inactive_buffers
                    .iter()
                    .find_map(|(_, buffer)| buffer.overlays.iter().find(|ov| ov.id == id))
            })
    }

    /// Find a mutable overlay by ID in any live buffer.
    pub fn find_overlay_mut(&mut self, id: u64) -> Option<&mut crate::overlay::Overlay> {
        let state = &mut **self;
        if let Some(overlay) = state.buffer.overlays.iter_mut().find(|ov| ov.id == id) {
            return Some(overlay);
        }
        state
            .inactive_buffers
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
}
