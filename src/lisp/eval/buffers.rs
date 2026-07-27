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
        state.redo_groups.push(latest_generated_undo_group(
            &self.buffer.undo_entries()[before..],
        ));
        state.undone_count += 1;
        state.had_error = false;
        Ok(())
    }

    pub fn reset_undo_sequence(&mut self) {
        self.undo_sequence = None;
    }

    pub(super) fn start_undo_sequence_step(&mut self) -> Result<(), LispError> {
        let original_groups = self.buffer.undo_groups();
        let group = original_groups.last().cloned().ok_or_else(|| {
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

    pub(super) fn replay_sequence_group(
        &mut self,
        group: &[crate::buffer::UndoEntry],
    ) -> Result<(), LispError> {
        for entry in group.iter().rev() {
            self.apply_current_buffer_undo_entry(entry)?;
        }
        Ok(())
    }

    pub(super) fn apply_current_buffer_undo_entry(
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
                    self.buffer.set_text_properties(
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
            crate::buffer::UndoEntry::Opaque(value) => {
                // Marker adjustments and modtime records are bookkeeping
                // riders on their neighboring entries, not undoable changes
                // themselves; GNU consumes or ignores them.
                if let Some((car, _)) = value.cons_values()
                    && matches!(car, Value::Marker(_) | Value::T)
                {
                    return Ok(());
                }
                Err(LispError::Signal(format!(
                    "Unrecognized entry in undo list {}",
                    render_undo_value(value)
                )))
            }
            crate::buffer::UndoEntry::Boundary => Ok(()),
        }
    }

    pub(super) fn affected_markers_for_delete(
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
}
