use super::*;

impl Interpreter {
    pub fn charset_canonical_name(&self, name: &str) -> Option<String> {
        let mut current = name.to_string();
        for _ in 0..16 {
            if self
                .charset_ids
                .iter()
                .any(|(registered, _)| registered == &current)
            {
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
        let canonical = self.charset_canonical_name(name)?;
        self.charset_ids
            .iter()
            .rev()
            .find(|(registered, _)| registered == &canonical)
            .map(|(_, id)| *id)
    }

    pub fn define_charset(&mut self, name: &str, plist: Value) -> i64 {
        let id = self
            .charset_ids
            .iter()
            .rev()
            .find(|(registered, _)| registered == name)
            .map(|(_, id)| *id)
            .unwrap_or_else(|| {
                let id = self
                    .charset_ids
                    .iter()
                    .map(|(_, id)| *id)
                    .max()
                    .unwrap_or(-1)
                    + 1;
                self.charset_ids.push((name.to_string(), id));
                id
            });
        if let Some((_, existing)) = self
            .charset_plists
            .iter_mut()
            .rev()
            .find(|(registered, _)| registered == name)
        {
            *existing = plist;
        } else {
            self.charset_plists.push((name.to_string(), plist));
        }
        id
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
        for default in ["unicode", "ascii", "eight-bit"] {
            if !reordered.iter().any(|existing| existing == default) {
                reordered.push(default.to_string());
            }
        }
        self.charset_priority = reordered;
    }

    pub fn charset_priority_rank(&self, name: &str) -> usize {
        let canonical = self
            .charset_canonical_name(name)
            .unwrap_or_else(|| name.to_string());
        self.charset_priority
            .iter()
            .position(|existing| existing == &canonical)
            .unwrap_or(usize::MAX)
    }

    pub fn declare_iso_charset(
        &mut self,
        dimension: i64,
        chars: i64,
        final_char: u32,
        charset: &str,
    ) {
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

    pub fn coding_system_canonical_name(&self, name: &str) -> Option<String> {
        let mut current = name.to_string();
        for _ in 0..16 {
            if self
                .coding_systems
                .iter()
                .any(|coding| coding.name == current)
            {
                return Some(current);
            }
            let (_, target) = self
                .coding_aliases
                .iter()
                .rev()
                .find(|(alias, _)| alias == &current)?;
            current = target.clone();
        }
        None
    }

    pub fn has_coding_system(&self, name: &str) -> bool {
        self.coding_system_canonical_name(name).is_some()
    }

    pub fn coding_system(&self, name: &str) -> Option<CodingSystemState> {
        let canonical = self.coding_system_canonical_name(name)?;
        self.coding_systems
            .iter()
            .rev()
            .find(|coding| coding.name == canonical)
            .cloned()
    }

    pub fn coding_system_base_name(&self, name: &str) -> Option<String> {
        self.coding_system(name).map(|coding| coding.base)
    }

    pub fn coding_system_kind_name(&self, name: &str) -> Option<String> {
        self.coding_system(name).map(|coding| coding.kind)
    }

    pub fn coding_system_eol_type_value(&self, name: &str) -> Option<i64> {
        self.coding_system(name).and_then(|coding| coding.eol_type)
    }

    pub fn coding_system_plist_value(&self, name: &str) -> Option<Value> {
        self.coding_system(name).map(|coding| coding.plist)
    }

    pub fn set_coding_system_plist_property(
        &mut self,
        name: &str,
        key: &str,
        value: Value,
    ) -> Result<(), LispError> {
        let canonical = self
            .coding_system_canonical_name(name)
            .ok_or_else(|| LispError::Void(name.to_string()))?;
        let Some(coding) = self
            .coding_systems
            .iter_mut()
            .rev()
            .find(|coding| coding.name == canonical)
        else {
            return Err(LispError::Void(name.to_string()));
        };
        let mut items = coding.plist.to_vec()?;
        let key_value = Value::Symbol(key.to_string().into());
        if let Some(index) = items.iter().position(|item| item == &key_value) {
            if index + 1 < items.len() {
                items[index + 1] = value;
            } else {
                items.push(value);
            }
        } else {
            items.push(key_value);
            items.push(value);
        }
        coding.plist = Value::list(items);
        Ok(())
    }

    pub fn define_coding_system_alias(
        &mut self,
        alias: &str,
        target: &str,
    ) -> Result<(), LispError> {
        let canonical = self
            .coding_system_canonical_name(target)
            .ok_or_else(|| LispError::Void(target.to_string()))?;
        if let Some((_, existing)) = self
            .coding_aliases
            .iter_mut()
            .rev()
            .find(|(existing_alias, _)| existing_alias == alias)
        {
            *existing = canonical;
        } else {
            self.coding_aliases.push((alias.to_string(), canonical));
        }
        Ok(())
    }

    pub fn coding_system_alias_list(&self, name: &str) -> Option<Vec<String>> {
        let canonical = self.coding_system_canonical_name(name)?;
        let mut aliases = vec![canonical.clone()];
        for (alias, target) in &self.coding_aliases {
            if target == &canonical && !aliases.iter().any(|existing| existing == alias) {
                aliases.push(alias.clone());
            }
        }
        Some(aliases)
    }

    pub fn coding_system_priority_list(&self) -> Vec<String> {
        self.coding_priority.clone()
    }

    pub fn coding_system_list(&self, base_only: bool) -> Vec<String> {
        let mut names = Vec::new();
        let mut push_coding = |coding: &CodingSystemState| {
            if (!base_only || coding.name == coding.base)
                && !names.iter().any(|existing| existing == &coding.name)
            {
                names.push(coding.name.clone());
            }
        };

        for priority in &self.coding_priority {
            for coding in &self.coding_systems {
                if &coding.name == priority || (!base_only && &coding.base == priority) {
                    push_coding(coding);
                }
            }
        }
        for coding in &self.coding_systems {
            push_coding(coding);
        }
        names
    }

    pub fn set_coding_system_priority(&mut self, names: &[String]) -> Result<(), LispError> {
        let mut reordered = Vec::new();
        for name in names {
            let canonical = self
                .coding_system_canonical_name(name)
                .ok_or_else(|| LispError::Void(name.clone()))?;
            if !reordered.iter().any(|existing| existing == &canonical) {
                reordered.push(canonical);
            }
        }
        for default in builtin_coding_priority() {
            if !reordered.iter().any(|existing| existing == &default) {
                reordered.push(default);
            }
        }
        self.coding_priority = reordered;
        Ok(())
    }

    pub fn coding_system_priority_rank(&self, name: &str) -> usize {
        let canonical = self
            .coding_system_canonical_name(name)
            .unwrap_or_else(|| name.to_string());
        self.coding_priority
            .iter()
            .position(|existing| existing == &canonical)
            .unwrap_or(usize::MAX)
    }

    pub fn define_coding_system(
        &mut self,
        name: &str,
        mnemonic: i64,
        kind: &str,
        plist: Value,
        eol_type: Option<i64>,
    ) -> Result<(), LispError> {
        let kind_canonical = self
            .coding_system_canonical_name(kind)
            .unwrap_or_else(|| kind.to_string());
        let mut items = plist.to_vec().unwrap_or_default();
        let mnemonic_key = Value::Symbol(":mnemonic".into());
        if let Some(index) = items.iter().position(|item| item == &mnemonic_key) {
            if index + 1 < items.len() {
                items[index + 1] = Value::Integer(mnemonic);
            } else {
                items.push(Value::Integer(mnemonic));
            }
        } else {
            items.push(mnemonic_key);
            items.push(Value::Integer(mnemonic));
        }
        let definition = CodingSystemState {
            name: name.to_string(),
            base: name.to_string(),
            kind: self
                .coding_system_kind_name(&kind_canonical)
                .unwrap_or(kind_canonical),
            eol_type,
            plist: Value::list(items),
        };
        if let Some(existing) = self
            .coding_systems
            .iter_mut()
            .rev()
            .find(|coding| coding.name == name)
        {
            *existing = definition.clone();
        } else {
            self.coding_systems.push(definition.clone());
        }
        // GNU's C primitive creates the complete EOL subsidiary family when
        // :eol-type is omitted.  High-level mule.el relies on the base query
        // returning this vector; it does not define the three variants itself.
        if eol_type.is_none() && name != "no-conversion" {
            for (suffix, variant_eol) in [("unix", 0), ("dos", 1), ("mac", 2)] {
                let variant_name = format!("{name}-{suffix}");
                let variant = CodingSystemState {
                    name: variant_name.clone(),
                    base: name.to_string(),
                    kind: definition.kind.clone(),
                    eol_type: Some(variant_eol),
                    plist: definition.plist.clone(),
                };
                if let Some(existing) = self
                    .coding_systems
                    .iter_mut()
                    .rev()
                    .find(|coding| coding.name == variant_name)
                {
                    *existing = variant;
                } else {
                    self.coding_systems.push(variant);
                }
            }
        }
        if !self.coding_priority.iter().any(|existing| existing == name) {
            self.coding_priority.push(name.to_string());
        }
        Ok(())
    }

    pub fn terminal_coding_system(&self) -> Option<String> {
        self.terminal_coding.clone()
    }

    pub fn set_terminal_coding_system(&mut self, coding: Option<String>) {
        self.terminal_coding = coding;
    }

    pub fn keyboard_coding_system(&self) -> Option<String> {
        self.keyboard_coding.clone()
    }

    pub fn set_keyboard_coding_system(&mut self, coding: Option<String>) {
        self.keyboard_coding = coding;
    }

    pub fn input_interrupt_mode(&self) -> bool {
        self.input_interrupt_mode
    }

    pub fn set_input_interrupt_mode(&mut self, enabled: bool) {
        self.input_interrupt_mode = enabled;
    }

    pub fn ensure_standard_category_table(&mut self) -> u64 {
        if let Some(id) = self.standard_category_table_id {
            return id;
        }
        let Value::CharTable(id) = self.make_char_table(
            Some("category-table".into()),
            Value::String(String::new().into()),
        ) else {
            unreachable!("make_char_table returns a char-table");
        };
        self.standard_category_table_id = Some(id);
        id
    }

    pub(crate) fn initialized_standard_category_table_id(&self) -> Option<u64> {
        self.standard_category_table_id
    }

    pub(crate) fn initialized_current_category_table_id(&self) -> Option<u64> {
        self.buffer_local_value(self.current_buffer_id(), "category-table")
            .and_then(|value| match value {
                Value::CharTable(id) => Some(id),
                _ => None,
            })
            .or(self.standard_category_table_id)
    }

    pub fn ensure_standard_case_table(&mut self) -> u64 {
        if let Some(id) = self.standard_case_table_id {
            return id;
        }
        let Value::CharTable(down_id) = self.make_char_table(Some("case-table".into()), Value::Nil)
        else {
            unreachable!("make_char_table returns a char-table");
        };
        let Value::CharTable(up_id) =
            self.make_char_table(Some("case-table-up".into()), Value::Nil)
        else {
            unreachable!("make_char_table returns a char-table");
        };
        self.set_char_table_extra_slot(down_id, 0, Value::CharTable(up_id))
            .expect("new case table accepts upcase slot");
        self.standard_case_table_id = Some(down_id);
        down_id
    }

    pub fn current_case_table_id(&mut self) -> u64 {
        if let Some((_, id)) = self
            .buffer_case_tables
            .iter()
            .rev()
            .find(|(buffer_id, _)| *buffer_id == self.current_buffer_id())
        {
            *id
        } else {
            self.ensure_standard_case_table()
        }
    }

    pub fn set_current_case_table(&mut self, id: u64) {
        let current_buffer_id = self.current_buffer_id();
        if let Some((_, slot)) = self
            .buffer_case_tables
            .iter_mut()
            .rev()
            .find(|(buffer_id, _)| *buffer_id == current_buffer_id)
        {
            *slot = id;
        } else {
            self.buffer_case_tables.push((current_buffer_id, id));
        }
    }

    pub fn standard_case_table_id(&mut self) -> u64 {
        self.ensure_standard_case_table()
    }

    pub fn set_standard_case_table(&mut self, id: u64) {
        self.standard_case_table_id = Some(id);
    }

    pub fn mark_ascii_case_table(&mut self, id: u64) {
        if !self.ascii_case_table_ids.contains(&id) {
            self.ascii_case_table_ids.push(id);
        }
    }

    pub fn is_ascii_case_table(&self, id: u64) -> bool {
        self.ascii_case_table_ids.contains(&id)
    }

    pub fn standard_syntax_table_id(&self) -> u64 {
        self.standard_syntax_table_id
    }

    /// The static GNU lisp-data-mode-syntax-table built at interpreter
    /// startup (see Interpreter::new).
    pub fn lisp_data_syntax_table_id(&self) -> u64 {
        3
    }

    /// The dumped GNU emacs-lisp-mode-syntax-table child, whose `@' entry
    /// differs from lisp-data-mode until syntax-propertize sees `,@'.
    pub fn emacs_lisp_mode_syntax_table_id(&self) -> u64 {
        4
    }

    pub fn current_syntax_table_id(&self) -> u64 {
        self.buffer_syntax_tables
            .iter()
            .rev()
            .find_map(|(buffer_id, table_id)| {
                (*buffer_id == self.current_buffer_id()).then_some(*table_id)
            })
            .unwrap_or(self.standard_syntax_table_id())
    }

    pub fn set_current_syntax_table(&mut self, id: u64) {
        let current_buffer_id = self.current_buffer_id();
        if let Some((_, table_id)) = self
            .buffer_syntax_tables
            .iter_mut()
            .rev()
            .find(|(buffer_id, _)| *buffer_id == current_buffer_id)
        {
            *table_id = id;
        } else {
            self.buffer_syntax_tables.push((current_buffer_id, id));
        }
    }

    pub fn set_syntax_word_char(&mut self, code: u32, enabled: bool) {
        if enabled {
            if !self.syntax_word_chars.contains(&code) {
                self.syntax_word_chars.push(code);
            }
        } else {
            self.syntax_word_chars.retain(|existing| *existing != code);
        }
    }

    pub fn is_syntax_word_char(&self, code: u32) -> bool {
        self.syntax_word_chars.contains(&code)
    }

    pub fn syntax_word_chars(&self) -> Vec<u32> {
        self.syntax_word_chars.clone()
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
        let table = self.find_char_table_mut(id).ok_or_else(|| {
            LispError::TypeError("char-table".into(), format!("char-table<{id}>"))
        })?;
        if table.category_docs.iter().any(|(ch, _)| *ch == category) {
            return Err(LispError::Signal("Category already defined".into()));
        }
        table.category_docs.push((category, doc));
        Ok(())
    }

    pub fn detach_markers_for_buffer(&mut self, buffer_id: u64) {
        if let Some(marker_ids) = self.markers_by_buffer.remove(&buffer_id) {
            for marker_id in marker_ids {
                let Some(index) = Self::marker_index(marker_id) else {
                    continue;
                };
                let Some(marker) = self.markers.get_mut(index) else {
                    continue;
                };
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
        let Some(marker_ids) = self.markers_by_buffer.get(&buffer_id) else {
            return;
        };
        for marker_id in marker_ids {
            let Some(index) = Self::marker_index(*marker_id) else {
                continue;
            };
            let Some(marker) = self.markers.get_mut(index) else {
                continue;
            };
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
        let Some(marker_ids) = self.markers_by_buffer.get(&buffer_id) else {
            return;
        };
        for marker_id in marker_ids {
            let Some(index) = Self::marker_index(*marker_id) else {
                continue;
            };
            let Some(marker) = self.markers.get_mut(index) else {
                continue;
            };
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
        self.markers_by_buffer
            .get(&buffer_id)
            .into_iter()
            .flatten()
            .filter_map(|marker_id| {
                let marker = self.find_marker(*marker_id)?;
                Some((marker.id, marker.position))
            })
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
}
