use super::*;
use crate::lisp::types::CharTableRef;
use crate::lisp::types::Kind;

// coding.c `enum coding_category', in order.
pub(crate) const CODING_CATEGORY_ISO_7: usize = 0;
pub(crate) const CODING_CATEGORY_ISO_7_TIGHT: usize = 1;
pub(crate) const CODING_CATEGORY_ISO_8_1: usize = 2;
pub(crate) const CODING_CATEGORY_ISO_8_2: usize = 3;
pub(crate) const CODING_CATEGORY_ISO_7_ELSE: usize = 4;
pub(crate) const CODING_CATEGORY_ISO_8_ELSE: usize = 5;
pub(crate) const CODING_CATEGORY_UTF_8_AUTO: usize = 6;
pub(crate) const CODING_CATEGORY_UTF_8_NOSIG: usize = 7;
pub(crate) const CODING_CATEGORY_UTF_8_SIG: usize = 8;
pub(crate) const CODING_CATEGORY_UTF_16_AUTO: usize = 9;
pub(crate) const CODING_CATEGORY_UTF_16_BE: usize = 10;
pub(crate) const CODING_CATEGORY_UTF_16_LE: usize = 11;
pub(crate) const CODING_CATEGORY_UTF_16_BE_NOSIG: usize = 12;
pub(crate) const CODING_CATEGORY_UTF_16_LE_NOSIG: usize = 13;
pub(crate) const CODING_CATEGORY_CHARSET: usize = 14;
pub(crate) const CODING_CATEGORY_SJIS: usize = 15;
pub(crate) const CODING_CATEGORY_BIG5: usize = 16;
pub(crate) const CODING_CATEGORY_CCL: usize = 17;
pub(crate) const CODING_CATEGORY_EMACS_MULE: usize = 18;
pub(crate) const CODING_CATEGORY_RAW_TEXT: usize = 19;
pub(crate) const CODING_CATEGORY_UNDECIDED: usize = 20;
pub(crate) const CODING_CATEGORY_COUNT: usize = 21;

/// The Lisp symbols of `coding-category-list', by category index.
pub(crate) const CODING_CATEGORY_NAMES: [&str; CODING_CATEGORY_COUNT] = [
    "coding-category-iso-7",
    "coding-category-iso-7-tight",
    "coding-category-iso-8-1",
    "coding-category-iso-8-2",
    "coding-category-iso-7-else",
    "coding-category-iso-8-else",
    "coding-category-utf-8-auto",
    "coding-category-utf-8",
    "coding-category-utf-8-sig",
    "coding-category-utf-16-auto",
    "coding-category-utf-16-be",
    "coding-category-utf-16-le",
    "coding-category-utf-16-be-nosig",
    "coding-category-utf-16-le-nosig",
    "coding-category-charset",
    "coding-category-sjis",
    "coding-category-big5",
    "coding-category-ccl",
    "coding-category-emacs-mule",
    "coding-category-raw-text",
    "coding-category-undecided",
];

// coding.h CODING_ISO_FLAG_*.
pub(crate) const ISO_FLAG_LONG_FORM: u32 = 0x0001;
pub(crate) const ISO_FLAG_RESET_AT_EOL: u32 = 0x0002;
pub(crate) const ISO_FLAG_RESET_AT_CNTL: u32 = 0x0004;
pub(crate) const ISO_FLAG_SEVEN_BITS: u32 = 0x0008;
pub(crate) const ISO_FLAG_LOCKING_SHIFT: u32 = 0x0010;
pub(crate) const ISO_FLAG_SINGLE_SHIFT: u32 = 0x0020;
pub(crate) const ISO_FLAG_DESIGNATION: u32 = 0x0040;
pub(crate) const ISO_FLAG_REVISION: u32 = 0x0080;
pub(crate) const ISO_FLAG_DIRECTION: u32 = 0x0100;
pub(crate) const ISO_FLAG_INIT_AT_BOL: u32 = 0x0200;
pub(crate) const ISO_FLAG_DESIGNATE_AT_BOL: u32 = 0x0400;
pub(crate) const ISO_FLAG_SAFE: u32 = 0x0800;
pub(crate) const ISO_FLAG_LATIN_EXTRA: u32 = 0x1000;
pub(crate) const ISO_FLAG_COMPOSITION: u32 = 0x2000;
pub(crate) const ISO_FLAG_USE_ROMAN: u32 = 0x8000;
pub(crate) const ISO_FLAG_USE_OLDJIS: u32 = 0x10000;
pub(crate) const ISO_FLAG_LEVEL_4: u32 = 0x20000;
pub(crate) const ISO_FLAG_FULL_SUPPORT: u32 = 0x100000;

/// What a conversion names a coding system by: its name, looked up in
/// Vcoding_system_hash_table through the interned symbol of that name (the
/// callers that still keep a coding system by its name), or the coding id
/// already found.
pub(crate) trait CodingRef {
    /// CODING_SYSTEM_SPEC, by index.
    fn coding_spec_index_in(&self, interp: &Interpreter) -> Option<usize>;
    /// The coding id this names (`Interpreter::coding_id').
    fn coding_id_in(&self, interp: &Interpreter) -> Option<CodingId>;
    /// The name this is, as text: a coding id's canonical name.
    fn coding_text(&self) -> &str;
}

impl CodingRef for str {
    #[inline]
    fn coding_spec_index_in(&self, interp: &Interpreter) -> Option<usize> {
        interp.coding_spec_index(self)
    }

    fn coding_id_in(&self, interp: &Interpreter) -> Option<CodingId> {
        interp.coding_id_symbol(&SymbolName::find_by_host_text(self)?)
    }

    #[inline]
    fn coding_text(&self) -> &str {
        self
    }
}

impl CodingRef for String {
    #[inline]
    fn coding_spec_index_in(&self, interp: &Interpreter) -> Option<usize> {
        interp.coding_spec_index(self)
    }

    fn coding_id_in(&self, interp: &Interpreter) -> Option<CodingId> {
        self.as_str().coding_id_in(interp)
    }

    #[inline]
    fn coding_text(&self) -> &str {
        self
    }
}

/// coding.c's coding id, as setup_coding_system finds it once: the spec
/// the hash table holds for a coding system symbol, and the symbol whose
/// name the conversion reads, the first name of that spec
/// (`coding_system_canonical_name_symbol').
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CodingId {
    pub(crate) spec: usize,
    pub(crate) name: SymbolName,
}

impl CodingId {
    /// The canonical name's text.
    pub(crate) fn as_str(&self) -> &str {
        self.name.as_str()
    }
}

impl CodingRef for CodingId {
    #[inline]
    fn coding_spec_index_in(&self, _interp: &Interpreter) -> Option<usize> {
        Some(self.spec)
    }

    #[inline]
    fn coding_id_in(&self, _interp: &Interpreter) -> Option<CodingId> {
        Some(*self)
    }

    #[inline]
    fn coding_text(&self) -> &str {
        self.as_str()
    }
}

impl std::fmt::Display for CodingId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl CodingSystemState {
    /// The Lisp objects the attribute vector holds, for the collector.
    pub(crate) fn values(&self) -> impl Iterator<Item = &Value> {
        [
            &self.ascii_compatible,
            &self.decode_table,
            &self.encode_table,
            &self.post_read,
            &self.pre_write,
            &self.plist,
            &self.charset_list,
        ]
        .into_iter()
        .chain(self.type_args.iter())
    }
}

impl Interpreter {
    /// Empty `charset_table', the next read of each charset reading its
    /// definition again, and advance its generation.
    pub(crate) fn invalidate_charset_table(&self) {
        self.charset_table.borrow_mut().clear();
        self.charset_epoch
            .set(self.charset_epoch.get().wrapping_add(1));
    }

    /// The charset an ISO-2022 designation names: DIMENSION (1 or 2), the
    /// 94/96 flavor and the final byte, through the table
    /// `define-charset-internal' and `declare-equiv-charset' fill.
    pub fn iso_charset_for(
        &self,
        dimension: i64,
        chars_96: bool,
        final_char: u8,
    ) -> Option<String> {
        let chars = if chars_96 { 96 } else { 94 };
        self.iso_charsets
            .iter()
            .rev()
            .find(|(d, c, f, _)| *d == dimension && *c == chars && *f == u32::from(final_char))
            .map(|(_, _, _, charset)| charset.clone())
    }

    pub fn charset_canonical_name(&self, name: &str) -> Option<String> {
        self.charset_canonical(name).map(str::to_owned)
    }

    /// The charset NAME stands for, through at most 16 aliases.
    pub(crate) fn charset_canonical<'a>(&'a self, name: &'a str) -> Option<&'a str> {
        let mut current = name;
        for _ in 0..16 {
            if self.charset_id_index.contains_key(current) {
                return Some(current);
            }
            current = self.charset_alias_index.get(current)?;
        }
        None
    }

    pub fn has_charset(&self, name: &str) -> bool {
        self.charset_canonical(name).is_some()
    }

    /// CHARSET_FROM_ID: the name of the charset whose id is ID.
    pub(crate) fn charset_name_of_id(&self, id: i64) -> Option<&str> {
        let direct = usize::try_from(id)
            .ok()
            .and_then(|index| self.charset_ids.get(index))
            .filter(|(_, found)| *found == id);
        direct
            .or_else(|| self.charset_ids.iter().find(|(_, found)| *found == id))
            .map(|(name, _)| name.as_str())
    }

    pub fn charset_id(&self, name: &str) -> Option<i64> {
        self.charset_id_index
            .get(self.charset_canonical(name)?)
            .copied()
    }

    /// The tail of Fdefine_charset_internal: NAME, its DEF (the arguments
    /// from dimension to unify-map, normalized) and PLIST registered.  A new
    /// name gets the next id, the head of `charset-list' and its place in
    /// the ordered list (a supplementary charset last, any other before
    /// the first supplementary one); an ISO final byte fills
    /// ISO_CHARSET_TABLE (a new charset joining Viso_2022_charset_list), an
    /// emacs-mule id joins Vemacs_mule_charset_list.  NAME already known
    /// (an alias too) is redefined under its id.
    /// Fdefine_charset_internal's hash_put and charset_table entry: NAME's
    /// id (a new one for a new name) with DEF and PLIST stored.
    pub(crate) fn register_charset(&mut self, name: &str, def: Value, plist: Value) -> (i64, bool) {
        use crate::lisp::primitives::coding::charset_table as table;
        // The charset table reads what this changes.
        self.invalidate_charset_table();
        let canonical = self
            .charset_canonical_name(name)
            .unwrap_or_else(|| name.to_string());
        let existing = self.charset_id_index.get(&canonical).copied();
        let new_definition = existing.is_none();
        let id = existing.unwrap_or_else(|| {
            let id = self
                .charset_ids
                .iter()
                .map(|(_, id)| *id)
                .max()
                .unwrap_or(-1)
                + 1;
            self.charset_ids.push((canonical.clone(), id));
            self.charset_id_index.insert(canonical.clone(), id);
            if !self.charset_alias_index.contains_key(&canonical) {
                self.charset_table_books.insert();
            }
            id
        });
        if let Some((_, existing)) = self
            .charset_defs
            .iter_mut()
            .rev()
            .find(|(registered, _)| registered == &canonical)
        {
            *existing = def;
        } else {
            self.charset_defs.push((canonical.clone(), def));
        }
        if let Some((_, existing)) = self
            .charset_plists
            .iter_mut()
            .rev()
            .find(|(registered, _)| registered == &canonical)
        {
            *existing = plist;
        } else {
            self.charset_plists.push((canonical.clone(), plist));
        }
        let supplementary = crate::lisp::primitives::vector_or_list_items(&def)
            .ok()
            .and_then(|items| items.get(table::DEF_SUPPLEMENTARY_P).copied())
            .is_some_and(|value| value.is_truthy());
        if supplementary {
            self.charset_supplementary.insert(canonical);
        } else {
            self.charset_supplementary.remove(&canonical);
        }
        (id, new_definition)
    }

    /// The rest of Fdefine_charset_internal once the charset (and a map
    /// charset's map) is in: ISO_CHARSET_TABLE and Viso_2022_charset_list,
    /// Vemacs_mule_charset_list, and for a new name `charset-list' and the
    /// ordered list.
    pub(crate) fn finish_charset_definition(
        &mut self,
        name: &str,
        _id: i64,
        new_definition: bool,
        def: Value,
    ) {
        use crate::lisp::primitives::coding::charset_table as table;
        self.invalidate_charset_table();
        let canonical = self
            .charset_canonical_name(name)
            .unwrap_or_else(|| name.to_string());
        let field = |slot: usize| {
            crate::lisp::primitives::vector_or_list_items(&def)
                .ok()
                .and_then(|items| items.get(slot).copied())
                .unwrap_or(Value::Nil)
        };
        let supplementary = field(table::DEF_SUPPLEMENTARY_P).is_truthy();
        let dimension = field(table::DEF_DIMENSION).integer_value().unwrap_or(1);
        let iso_chars_96 =
            crate::lisp::primitives::vector_or_list_items(&field(table::DEF_CODE_SPACE))
                .ok()
                .and_then(|space| {
                    Some(space.get(1)?.integer_value()? - space.first()?.integer_value()?)
                })
                .is_some_and(|span| span + 1 == 96);
        let iso_final = field(table::DEF_ISO_FINAL)
            .integer_value()
            .and_then(|value| u32::try_from(value).ok());
        let emacs_mule = !field(table::DEF_EMACS_MULE_ID).is_nil();
        if let Some(final_char) = iso_final {
            self.declare_iso_charset(
                dimension,
                if iso_chars_96 { 96 } else { 94 },
                final_char,
                &canonical,
            );
            if new_definition {
                self.iso_2022_charset_list.push(canonical.clone());
            }
        }
        if emacs_mule && new_definition {
            self.emacs_mule_charset_list.push(canonical.clone());
        }
        if new_definition {
            self.push_charset_list(Value::symbol(name));
            if supplementary {
                self.charset_priority.push(canonical);
            } else {
                let position = self
                    .charset_priority
                    .iter()
                    .position(|existing| self.charset_supplementary.contains(existing))
                    .unwrap_or(self.charset_priority.len());
                self.charset_priority.insert(position, canonical);
            }
        }
    }

    /// The `define-charset-internal' arguments CHARSET (a name or an alias)
    /// was defined with, from its dimension to its unify-map.
    pub(crate) fn charset_def_value(&self, charset: &str) -> Option<Value> {
        let canonical = self.charset_canonical(charset)?;
        self.charset_defs
            .iter()
            .rev()
            .find(|(registered, _)| registered == canonical)
            .map(|(_, def)| *def)
    }

    /// set_charset_attr of SLOT of CHARSET's definition (`unify-charset'
    /// storing its unify-map).
    pub(crate) fn set_charset_def_slot(&mut self, charset: &str, slot: usize, value: Value) {
        self.invalidate_charset_table();
        if let Some(def) = self.charset_def_value(charset) {
            let _ = crate::lisp::primitives::coding::aset_vector_value(&def, slot, value);
        }
    }

    /// charset.c Viso_2022_charset_list: the ISO-2022 designatable
    /// charsets, in priority order since the last `set-charset-priority'.
    pub fn iso_2022_charset_list(&self) -> Vec<String> {
        self.iso_2022_charset_list.clone()
    }

    /// The (dimension, chars, final byte) slot of ISO_CHARSET_TABLE that
    /// names CHARSET, for charsets whose plist does not carry the final
    /// byte (the C-defined `ascii').
    pub fn iso_charset_entry_for(&self, charset: &str) -> Option<(i64, i64, u32)> {
        let canonical = self.charset_canonical_name(charset)?;
        self.iso_charsets
            .iter()
            .rev()
            .find(|(_, _, _, name)| *name == canonical)
            .map(|(dimension, chars, final_char, _)| (*dimension, *chars, *final_char))
    }

    pub fn charset_is_unified(&self, name: &str) -> bool {
        self.charset_canonical_name(name)
            .is_some_and(|canonical| self.charset_unified.contains(&canonical))
    }

    pub fn set_charset_unified(&mut self, name: &str, unified: bool) {
        // The charset table reads what this changes.
        self.invalidate_charset_table();
        if let Some(canonical) = self.charset_canonical_name(name) {
            if unified {
                self.charset_unified.insert(canonical);
            } else {
                self.charset_unified.remove(&canonical);
            }
        }
    }

    pub fn charset_plist_value(&self, name: &str) -> Option<Value> {
        let canonical = self.charset_canonical_name(name)?;
        self.charset_plists
            .iter()
            .rev()
            .find(|(charset, _)| charset == &canonical)
            .map(|(_, value)| *value)
    }

    pub fn set_charset_plist_value(&mut self, name: &str, value: Value) -> Result<(), LispError> {
        // The charset table reads what this changes.
        self.invalidate_charset_table();
        let canonical = self
            .charset_canonical_name(name)
            .ok_or_else(|| LispError::WrongTypeArgument("charsetp".into(), Value::symbol(name)))?;
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

    /// pdumper.c's thaw_hash_tables of the two tables charset.c and
    /// coding.c keep in Lisp, Vcharset_hash_table and
    /// Vcoding_system_hash_table: a key for each name and alias the loaded
    /// image holds.
    pub(crate) fn thaw_host_hash_tables(&mut self) {
        let charsets = self.charset_id_index.len()
            + self
                .charset_alias_index
                .keys()
                .filter(|alias| !self.charset_id_index.contains_key(*alias))
                .count();
        self.charset_table_books.thaw(charsets);
        let codings = self.coding_system_hash.len();
        self.coding_table_books.thaw(codings);
    }

    /// Fdefine_charset_alias after CHECK_CHARSET_GET_ATTR: ALIAS's hash
    /// entry is TARGET's attributes (a charset of that name now answers as
    /// TARGET), and `charset-list' gains ALIAS at its head again.
    pub fn define_charset_alias(&mut self, alias: &str, target: &str) -> Result<(), LispError> {
        // The charset table reads what this changes.
        self.invalidate_charset_table();
        let canonical = self.charset_canonical_name(target).ok_or_else(|| {
            LispError::WrongTypeArgument("charsetp".into(), Value::symbol(target))
        })?;
        if let Some((_, existing)) = self
            .charset_aliases
            .iter_mut()
            .rev()
            .find(|(existing_alias, _)| existing_alias == alias)
        {
            existing.clone_from(&canonical);
        } else {
            self.charset_aliases
                .push((alias.to_string(), canonical.clone()));
        }
        let known = self.charset_id_index.contains_key(alias);
        let fresh = self
            .charset_alias_index
            .insert(alias.to_string(), canonical.clone())
            .is_none();
        if fresh && !known {
            self.charset_table_books.insert();
        }
        if known && alias != canonical {
            // Fputhash replaces the charset's own attributes under its
            // name; its charset_table entry stays for its id.
            self.charset_id_index.remove(alias);
        }
        self.push_charset_list(Value::symbol(alias));
        Ok(())
    }

    /// Vcharset_list = Fcons (NAME, Vcharset_list): the variable's current
    /// value (a let-bound one while it is bound) gains NAME at its head.
    pub(crate) fn push_charset_list(&mut self, name: Value) {
        let current = self
            .symbol_value_symbol(&SymbolName::intern_str("charset-list"))
            .unwrap_or(Value::Nil);
        self.set_global_binding_key(cached_symbol!("charset-list"), Value::cons(name, current));
    }

    pub fn charset_priority_list(&self) -> Vec<String> {
        self.charset_priority.clone()
    }

    /// Vcharset_ordered_list, borrowed.
    pub(crate) fn charset_priority(&self) -> &[String] {
        &self.charset_priority
    }

    pub fn set_charset_priority(&mut self, names: &[String]) {
        self.charset_epoch
            .set(self.charset_epoch.get().wrapping_add(1));
        // charset.c Fset_charset_priority: the given charsets move to the
        // front in the given order; every other charset keeps its old
        // relative order behind them.
        let mut new_head = Vec::new();
        for name in names {
            if let Some(canonical) = self.charset_canonical_name(name)
                && !new_head.iter().any(|existing| existing == &canonical)
                && self
                    .charset_priority
                    .iter()
                    .any(|existing| existing == &canonical)
            {
                new_head.push(canonical);
            }
        }
        let tail = self
            .charset_priority
            .iter()
            .filter(|existing| !new_head.iter().any(|head| &head == existing))
            .cloned()
            .collect::<Vec<_>>();
        // Vcharset_non_preferred_head = old_list: the rest of the order.
        self.charset_non_preferred_head = tail.first().cloned();
        new_head.extend(tail);
        self.charset_priority = new_head;
        // charset.c Fset_charset_priority rebuilds Viso_2022_charset_list
        // and Vemacs_mule_charset_list in the new ordered-list order.
        let iso_2022 = std::mem::take(&mut self.iso_2022_charset_list);
        self.iso_2022_charset_list = self
            .charset_priority
            .iter()
            .filter(|charset| iso_2022.contains(charset))
            .cloned()
            .collect();
        let emacs_mule = std::mem::take(&mut self.emacs_mule_charset_list);
        self.emacs_mule_charset_list = self
            .charset_priority
            .iter()
            .filter(|charset| emacs_mule.contains(charset))
            .cloned()
            .collect();
    }

    pub fn charset_non_preferred_head(&self) -> Option<String> {
        self.charset_non_preferred_head.clone()
    }

    /// Vcharset_non_preferred_head, borrowed.
    pub(crate) fn charset_non_preferred_head_ref(&self) -> Option<&str> {
        self.charset_non_preferred_head.as_deref()
    }

    /// The dump boundary for charset.c's Vcharset_non_preferred_head: the
    /// variable is not staticpro'd, so the pdumper does not carry the
    /// value loadup's `set-language-environment' left in temacs, and a
    /// freshly started GNU Emacs has it nil (all-zero bits) until the
    /// first `set-charset-priority' of the session.  Under a LANG-less
    /// environment nothing calls it, and `char-charset' then walks the
    /// whole ordered list without the unicode short cut.
    pub fn forget_charset_non_preferred_head(&mut self) {
        self.charset_non_preferred_head = None;
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

    /// CODING_SYSTEM_SPEC: the spec Vcoding_system_hash_table holds for
    /// NAME, by index.
    pub(crate) fn coding_spec_index(&self, name: &str) -> Option<usize> {
        self.coding_spec_index_symbol(&SymbolName::find_by_host_text(name)?)
    }

    /// CODING_SYSTEM_SPEC of the symbol CODING_SYSTEM itself:
    /// Vcoding_system_hash_table's entry, by EQ.
    pub(crate) fn coding_spec_index_symbol(&self, coding_system: &SymbolName) -> Option<usize> {
        self.coding_system_hash.get(coding_system).copied()
    }

    /// `coding_system_canonical_name_symbol' of a Lisp value: the symbol
    /// it is (t among them); None for nil or any other object.
    pub(crate) fn coding_system_canonical_name_value(&self, value: &Value) -> Option<String> {
        let symbol = match value.kind() {
            Kind::T => crate::lisp::types::symbols::builtin_boolean(true),
            _ => value.symbol_cell()?,
        };
        self.coding_system_canonical_name_symbol(&symbol)
    }

    /// CODING_SYSTEM_P's table test for the symbol CODING_SYSTEM.
    pub(crate) fn has_coding_system_symbol(&self, coding_system: &SymbolName) -> bool {
        self.coding_system_hash.contains_key(coding_system)
    }

    pub(crate) fn coding_spec(&self, coding: &(impl CodingRef + ?Sized)) -> Option<&CodingSpec> {
        self.coding_specs.get(coding.coding_spec_index_in(self)?)
    }

    /// The coding id CODING stands for: its spec, found once, and the
    /// name `coding_system_canonical_name' reads for it.
    #[inline]
    pub(crate) fn coding_id(&self, coding: &(impl CodingRef + ?Sized)) -> Option<CodingId> {
        coding.coding_id_in(self)
    }

    /// `coding_id' of the symbol CODING_SYSTEM: the first name of its
    /// spec while that name still has the spec, else the symbol itself.
    pub(crate) fn coding_id_symbol(&self, coding_system: &SymbolName) -> Option<CodingId> {
        let spec = self.coding_spec_index_symbol(coding_system)?;
        let name = self.coding_specs[spec]
            .aliases
            .cons_values()
            .and_then(|(first, _)| first.symbol_cell())
            .filter(|first| self.coding_spec_index_symbol(first) == Some(spec))
            .unwrap_or(*coding_system);
        Some(CodingId { spec, name })
    }

    /// The name NAME stands for: the first of its spec's names, the one
    /// the spec was defined as, while that name still has the spec (an
    /// alias made before its target's redefinition keeps the old spec,
    /// and stands for itself).
    pub(crate) fn coding_system_canonical_name(
        &self,
        coding: &(impl CodingRef + ?Sized),
    ) -> Option<String> {
        self.coding_id(coding).map(|id| id.as_str().to_owned())
    }

    /// `coding_system_canonical_name' of the symbol CODING_SYSTEM, looked
    /// up by EQ.
    pub fn coding_system_canonical_name_symbol(
        &self,
        coding_system: &SymbolName,
    ) -> Option<String> {
        self.coding_id_symbol(coding_system)
            .map(|id| id.as_str().to_owned())
    }

    pub fn has_coding_system(&self, name: &str) -> bool {
        SymbolName::find_by_host_text(name)
            .is_some_and(|symbol| self.has_coding_system_symbol(&symbol))
    }

    /// CODING_ID_ATTRS: the attribute vector of CODING's spec.
    pub(crate) fn coding_system(
        &self,
        coding: &(impl CodingRef + ?Sized),
    ) -> Option<&CodingSystemState> {
        self.coding_attrs.get(self.coding_spec(coding)?.attrs)
    }

    pub(crate) fn coding_system_base_name(
        &self,
        coding: &(impl CodingRef + ?Sized),
    ) -> Option<String> {
        self.coding_system(coding)
            .map(|coding| coding.base.as_str().to_owned())
    }

    pub(crate) fn coding_system_kind_name(
        &self,
        coding: &(impl CodingRef + ?Sized),
    ) -> Option<String> {
        self.coding_system(coding).map(|coding| coding.kind.clone())
    }

    /// The eol type of CODING's spec as Fcoding_system_eol_type numbers
    /// it, None when it is the vector of subsidiaries.
    pub(crate) fn coding_system_eol_type_value(
        &self,
        coding: &(impl CodingRef + ?Sized),
    ) -> Option<i64> {
        let eol_type = self.coding_spec(coding)?.eol_type;
        if eol_type.is_key(cached_symbol!("unix")) {
            Some(0)
        } else if eol_type.is_key(cached_symbol!("dos")) {
            Some(1)
        } else if eol_type.is_key(cached_symbol!("mac")) {
            Some(2)
        } else {
            None
        }
    }

    /// Fputhash (NAME, SPEC, Vcoding_system_hash_table).
    fn puthash_coding_system(&mut self, name: SymbolName, spec: usize) {
        if self.coding_system_hash.insert(name, spec).is_none() {
            self.coding_table_books.insert();
        }
    }

    fn push_coding_spec(&mut self, spec: CodingSpec) -> usize {
        self.coding_specs.push(spec);
        self.coding_specs.len() - 1
    }

    /// coding.c make_subsidiaries: NAME-unix, NAME-dos and NAME-mac,
    /// interned.
    /// coding.c:make_subsidiaries: the symbols interned from BASE's name
    /// (SYMBOL_NAME, the same for an uninterned BASE) and each suffix.
    fn make_subsidiaries(&mut self, base: SymbolName) -> [SymbolName; 3] {
        let name = base.name_text();
        ["unix", "dos", "mac"].map(|suffix| SymbolName::intern_str(&format!("{name}-{suffix}")))
    }

    /// The end of Fdefine_coding_system_internal: ATTRS, its arguments
    /// checked, become NAME's spec with EOL_TYPE (`unix', `dos', `mac'
    /// or nil), and for nil each subsidiary's with its own; the category
    /// gets NAME as its system when it has none or had NAME itself.
    pub(crate) fn define_coding_system(
        &mut self,
        name: SymbolName,
        attrs: CodingSystemState,
        eol_type: Value,
    ) {
        let category = attrs.category;
        if attrs.kind == "shift-jis" {
            self.sjis_coding_system = name.as_str().to_owned();
        } else if attrs.kind == "big5" {
            self.big5_coding_system = name.as_str().to_owned();
        }
        self.coding_attrs.push(attrs);
        let attrs = self.coding_attrs.len() - 1;
        let eol_type = if eol_type.is_nil() {
            let subsidiaries = self.make_subsidiaries(name);
            for (subsidiary, this_eol_type) in subsidiaries.into_iter().zip(["unix", "dos", "mac"])
            {
                let spec = self.push_coding_spec(CodingSpec {
                    attrs,
                    aliases: Value::list([Value::Symbol(subsidiary)]),
                    eol_type: Value::symbol(this_eol_type),
                });
                self.puthash_coding_system(subsidiary, spec);
                self.record_coding_system_name(subsidiary);
            }
            Value::vector(subsidiaries.into_iter().map(Value::Symbol))
        } else {
            eol_type
        };
        let spec = self.push_coding_spec(CodingSpec {
            attrs,
            aliases: Value::list([Value::Symbol(name)]),
            eol_type,
        });
        self.puthash_coding_system(name, spec);
        self.record_coding_system_name(name);
        if self.coding_category_representatives[category]
            .as_deref()
            .is_none_or(|representative| representative == name.as_str())
        {
            self.coding_category_representatives[category] = Some(name.as_str().to_owned());
        }
    }

    /// Fdefine_coding_system_alias once ALIAS is a symbol and SPEC the
    /// spec of the coding system: ALIAS joins the end of the spec's
    /// names in place, each of ALIAS's subsidiaries becomes an alias of
    /// the matching subsidiary when the eol type is a vector of them, and
    /// ALIAS's hash entry is the spec itself.
    pub(crate) fn define_coding_system_alias(
        &mut self,
        alias: SymbolName,
        spec: usize,
    ) -> Result<(), LispError> {
        let mut aliases = self.coding_specs[spec].aliases;
        while let Some((_, rest)) = aliases.cons_values() {
            if rest.is_nil() {
                break;
            }
            aliases = rest;
        }
        aliases.set_cdr(Value::list([Value::Symbol(alias)]))?;
        let eol_type = self.coding_specs[spec].eol_type;
        if let Kind::Vector(targets) = eol_type.kind() {
            let subsidiaries = self.make_subsidiaries(alias);
            for (subsidiary, target) in subsidiaries.into_iter().zip(targets.slots()) {
                let target_spec = target
                    .symbol_cell()
                    .and_then(|target| self.coding_spec_index_symbol(&target))
                    .ok_or_else(|| {
                        LispError::WrongTypeArgument("coding-system-p".into(), target)
                    })?;
                self.define_coding_system_alias(subsidiary, target_spec)?;
            }
        }
        self.puthash_coding_system(alias, spec);
        self.record_coding_system_name(alias);
        Ok(())
    }

    /// coding.c Fcoding_system_priority_list: the representative of each
    /// category that has one, in category priority order, as base names.
    pub fn coding_system_priority_list(&self) -> Vec<String> {
        self.coding_category_priorities
            .iter()
            .filter_map(|category| self.coding_category_representatives[*category].clone())
            .map(|name| self.coding_system_base_name(&name).unwrap_or(name))
            .collect()
    }

    /// coding.c Fdefine_coding_system_internal's category selection for a
    /// coding TYPE with its :charset-list and type-specific arguments.
    pub fn coding_category_index(
        &self,
        coding_type: &str,
        charset_list: &Value,
        type_args: &[Value],
    ) -> usize {
        match coding_type {
            "utf-8" => match type_args.first().map(|v| v.kind()) {
                None | Some(Kind::Nil) => CODING_CATEGORY_UTF_8_NOSIG,
                Some(Kind::T) => CODING_CATEGORY_UTF_8_SIG,
                Some(_) => CODING_CATEGORY_UTF_8_AUTO,
            },
            "utf-16" => {
                let bom = type_args.first().cloned().unwrap_or(Value::Nil);
                let big_endian = !matches!(type_args.get(1).map(|v| v.kind()), Some(Kind::Symbol(endian)) if endian.is_named("little"));
                match bom.kind() {
                    Kind::Cons(_) => CODING_CATEGORY_UTF_16_AUTO,
                    Kind::Nil if big_endian => CODING_CATEGORY_UTF_16_BE_NOSIG,
                    Kind::Nil => CODING_CATEGORY_UTF_16_LE_NOSIG,
                    _ if big_endian => CODING_CATEGORY_UTF_16_BE,
                    _ => CODING_CATEGORY_UTF_16_LE,
                }
            }
            "iso-2022" => {
                let full_support =
                    matches!(charset_list.kind(), Kind::Symbol(name) if name.is_named("iso-2022"));
                let flags = type_args
                    .get(3)
                    .and_then(|flags| flags.integer_value())
                    .unwrap_or(0) as u32
                    | if full_support {
                        ISO_FLAG_FULL_SUPPORT
                    } else {
                        0
                    };
                if flags & ISO_FLAG_SEVEN_BITS != 0 {
                    if flags & (ISO_FLAG_LOCKING_SHIFT | ISO_FLAG_SINGLE_SHIFT) != 0 {
                        CODING_CATEGORY_ISO_7_ELSE
                    } else if full_support {
                        CODING_CATEGORY_ISO_7
                    } else {
                        CODING_CATEGORY_ISO_7_TIGHT
                    }
                } else {
                    let g1 = type_args
                        .first()
                        .and_then(|initial| {
                            crate::lisp::primitives::vector_or_list_items(initial).ok()
                        })
                        .and_then(|items| items.get(1).cloned())
                        .filter(|charset| !charset.is_nil());
                    let g1_dimension = g1
                        .and_then(|charset| charset.symbol_str().map(str::to_string))
                        .and_then(|charset| {
                            crate::lisp::primitives::coding::charset_table::charset_info(
                                self, &charset,
                            )
                        })
                        .map(|info| i64::from(info.dimension));
                    match g1_dimension {
                        _ if flags & ISO_FLAG_LOCKING_SHIFT != 0 || full_support => {
                            CODING_CATEGORY_ISO_8_ELSE
                        }
                        None => CODING_CATEGORY_ISO_8_ELSE,
                        Some(1) => CODING_CATEGORY_ISO_8_1,
                        Some(_) => CODING_CATEGORY_ISO_8_2,
                    }
                }
            }
            "charset" => CODING_CATEGORY_CHARSET,
            "ccl" => CODING_CATEGORY_CCL,
            "emacs-mule" => CODING_CATEGORY_EMACS_MULE,
            "shift-jis" => CODING_CATEGORY_SJIS,
            "big5" => CODING_CATEGORY_BIG5,
            "raw-text" => CODING_CATEGORY_RAW_TEXT,
            _ => CODING_CATEGORY_UNDECIDED,
        }
    }

    /// coding.c Fset_coding_system_priority over NAMES, coding systems
    /// as given: each name's category moves to the front in the given
    /// order (later names of a category are ignored), the category's
    /// system becoming the name when it has one already, and its
    /// `coding-category-*' variable the name; the remaining categories
    /// keep their previous relative order, and `coding-category-list'
    /// follows.
    pub fn set_coding_system_categories_priority(
        &mut self,
        codings: &[SymbolName],
        env: &mut Env,
    ) -> Result<(), LispError> {
        let mut changed = [false; CODING_CATEGORY_COUNT];
        let mut priorities = Vec::with_capacity(CODING_CATEGORY_COUNT);
        for coding in codings {
            // CHECK_CODING_SYSTEM_GET_SPEC, by EQ.
            let spec = self.coding_spec_index_symbol(coding).ok_or_else(|| {
                LispError::WrongTypeArgument("coding-system-p".into(), Value::Symbol(*coding))
            })?;
            let category = self.coding_attrs[self.coding_specs[spec].attrs].category;
            if changed[category] {
                continue;
            }
            changed[category] = true;
            priorities.push(category);
            if self.coding_category_representatives[category].is_some() {
                // setup_coding_system of the spec, which the category's
                // representative names.
                self.coding_category_representatives[category] =
                    self.coding_system_canonical_name_symbol(coding);
            }
            self.set_variable(CODING_CATEGORY_NAMES[category], Value::Symbol(*coding), env);
        }
        for category in self.coding_category_priorities.clone() {
            if !changed[category] {
                priorities.push(category);
            }
        }
        self.coding_category_priorities = priorities;
        let list = Value::list(
            self.coding_category_priorities
                .iter()
                .map(|category| Value::Symbol(CODING_CATEGORY_NAMES[*category].into())),
        );
        self.set_variable_key(cached_symbol!("coding-category-list"), list, env);
        Ok(())
    }

    /// Fdefine_coding_system_internal's and Fdefine_coding_system_alias's
    /// record of a name they define: pushed onto `coding-system-list'
    /// every time, and onto `coding-system-alist' as a list of the
    /// symbol's name unless an element already holds that name.
    /// The end of Fdefine_coding_system_internal and
    /// Fdefine_coding_system_alias: NAME consed onto `coding-system-list',
    /// and (SYMBOL-NAME) onto `coding-system-alist' unless a string of
    /// that name is a key there already (Fassoc).
    pub(crate) fn record_coding_system_name(&mut self, name: SymbolName) {
        let mut env = Env::new();
        let list = self
            .lookup_var_key(cached_symbol!("coding-system-list"), &env)
            .unwrap_or(Value::Nil);
        self.set_variable_key(
            cached_symbol!("coding-system-list"),
            Value::cons(Value::Symbol(name), list),
            &mut env,
        );
        let alist = self
            .lookup_var_key(cached_symbol!("coding-system-alist"), &env)
            .unwrap_or(Value::Nil);
        let text = name.name_text();
        let mut tail = alist;
        while let Some((element, rest)) = tail.cons_values() {
            if element
                .cons_values()
                .and_then(|(key, _)| crate::lisp::primitives::string_like(&key))
                .is_some_and(|key| key.text == text)
            {
                return;
            }
            tail = rest;
        }
        self.set_variable_key(
            cached_symbol!("coding-system-alist"),
            Value::cons(Value::list([name.lisp_name()]), alist),
            &mut env,
        );
    }

    pub fn terminal_coding_system(&self) -> Option<String> {
        self.terminal_state(self.selected_terminal_id())
            .and_then(|terminal| terminal.terminal_coding.clone())
    }

    /// The coding system whose charsets the selected terminal's
    /// t->charset_list holds, None while it is nil.
    pub fn terminal_charset_list_coding(&self) -> Option<String> {
        self.terminal_state(self.selected_terminal_id())
            .and_then(|terminal| terminal.charset_list_coding.clone())
    }

    /// Return the coder term.c uses when terminal output needs encoding.
    /// A nil coder and coders that do not perform an encoding conversion use
    /// GNU's safe US-ASCII fallback instead of passing multibyte text through.
    pub fn effective_terminal_coding_system(&self) -> String {
        self.terminal_coding_system()
            .filter(|coding| {
                !matches!(
                    self.coding_system_kind_name(coding).as_deref(),
                    Some("no-conversion" | "raw-text" | "undecided")
                )
            })
            .unwrap_or_else(|| {
                self.safe_terminal_coding
                    .clone()
                    .unwrap_or_else(|| "us-ascii".into())
            })
    }

    /// The terminal as `set-terminal-coding-system-internal' leaves it
    /// for CODING (a None coding converts nothing: ASCII glyphs alone).
    #[cfg(test)]
    pub fn set_terminal_coding_system(&mut self, coding: Option<String>) {
        let id = self.selected_terminal_id();
        let charset_list_coding = Some(coding.clone().unwrap_or_else(|| "us-ascii".into()));
        let terminal = self
            .terminals
            .iter_mut()
            .find(|terminal| terminal.id == id)
            .expect("decoded terminal has state");
        let mut terminal = terminal.borrow_mut();
        terminal.terminal_coding = coding;
        terminal.charset_list_coding = charset_list_coding;
    }

    pub fn input_interrupt_mode(&self) -> bool {
        self.input_interrupt_mode
    }

    pub fn set_input_interrupt_mode(&mut self, enabled: bool) {
        self.input_interrupt_mode = enabled;
    }

    /// The standard category table, if one exists yet.
    pub(crate) fn standard_category_table(&self) -> Option<CharTableRef> {
        self.standard_category_table_id
    }

    pub fn ensure_standard_category_table(&mut self) -> CharTableRef {
        if let Some(id) = self.standard_category_table_id {
            return id;
        }
        let id = CharTableRef::new(Value::symbol("category-table"), Value::Nil, 2);
        id.set_default(primitives::make_bool_vector_value([false; 128]));
        id.set_extra(0, Value::vector(std::iter::repeat_n(Value::Nil, 95)));
        self.standard_category_table_id = Some(id);
        id
    }

    /// BVAR (current_buffer, category_table), None before the standard
    /// table exists.
    pub(crate) fn initialized_current_category_table_id(&self) -> Option<CharTableRef> {
        self.buffer_category_tables
            .get(self.current_buffer_id())
            .or(self.standard_category_table_id)
    }

    /// Fset_category_table's bset_category_table of the current buffer.
    pub(crate) fn set_current_category_table(&mut self, table: CharTableRef) {
        let current_buffer_id = self.current_buffer_id();
        self.buffer_category_tables.insert(current_buffer_id, table);
    }

    pub fn ensure_standard_case_table(&mut self) -> CharTableRef {
        self.standard_case_tables().down
    }

    /// casetab.c's Vascii_downcase_table and its three companions, made
    /// by init_casetab_once at the first use.
    pub(crate) fn standard_case_tables(&mut self) -> CaseTables {
        if let Some(tables) = self.standard_case_tables {
            return tables;
        }
        let make = |interp: &mut Self| {
            let Kind::CharTable(id) = interp
                .make_char_table(Some("case-table".into()), Value::Nil)
                .kind()
            else {
                unreachable!("make_char_table returns a char-table");
            };
            id
        };
        let down_id = make(self);
        let up_id = make(self);
        let canonical_id = make(self);
        let equivalent_id = make(self);
        // casetab.c:init_casetab installs only ASCII mappings. The unchanged
        // characters.el later populates non-ASCII entries; nil entries must
        // remain nil rather than acquire implicit Unicode mappings.
        for code in 0..128u8 {
            let down = code.to_ascii_lowercase();
            let up = code.to_ascii_uppercase();
            let equivalent = if code.is_ascii_uppercase() { down } else { up };
            for (table, mapped) in [
                (down_id, down),
                (up_id, up),
                (canonical_id, down),
                (equivalent_id, equivalent),
            ] {
                self.char_table_set(table, u32::from(code), Value::Integer(i64::from(mapped)))
                    .expect("initial case-table entry");
            }
        }
        for (slot, table) in [up_id, canonical_id, equivalent_id].into_iter().enumerate() {
            self.set_char_table_extra_slot(down_id, slot, Value::CharTable(table))
                .expect("initial case-table extra slot");
        }
        self.set_char_table_extra_slot(canonical_id, 2, Value::CharTable(equivalent_id))
            .expect("initial canonical equivalence slot");
        let tables = CaseTables {
            down: down_id,
            up: up_id,
            canon: canonical_id,
            eqv: equivalent_id,
        };
        self.standard_case_tables = Some(tables);
        tables
    }

    /// BVAR (current_buffer, downcase_table).
    pub fn current_case_table_id(&mut self) -> CharTableRef {
        self.current_case_tables().down
    }

    /// The current buffer's four case tables.
    pub(crate) fn current_case_tables(&mut self) -> CaseTables {
        match self.buffer_case_tables(self.current_buffer_id()) {
            Some(tables) => tables,
            None => self.standard_case_tables(),
        }
    }

    /// The current buffer's four case tables, unless none exist yet.
    pub(crate) fn initialized_current_case_tables(&self) -> Option<CaseTables> {
        self.buffer_case_tables(self.current_buffer_id())
            .or(self.standard_case_tables)
    }

    /// casetab.c:set_case_table's final stores for the current buffer.
    pub(crate) fn set_current_case_tables(&mut self, tables: CaseTables) {
        let buffer_id = self.current_buffer_id();
        self.set_buffer_case_tables(buffer_id, tables);
    }

    /// BUFFER's four slots set to TABLES; a buffer holding the standard
    /// four keeps no entry.
    pub(crate) fn set_buffer_case_tables(&mut self, buffer_id: u64, tables: CaseTables) {
        if Some(tables) == self.standard_case_tables {
            self.buffer_case_tables.remove(buffer_id);
        } else {
            self.install_buffer_case_tables(buffer_id, tables);
        }
    }

    pub fn standard_case_table_id(&mut self) -> CharTableRef {
        self.ensure_standard_case_table()
    }

    /// casetab.c:set_case_table's stores for the standard tables.  Each
    /// live buffer holding the old standard four keeps them, as its slots
    /// in GNU do, now as an entry of its own.
    pub(crate) fn set_standard_case_tables(&mut self, tables: CaseTables) {
        if let Some(old) = self.standard_case_tables
            && old != tables
        {
            let holders = self
                .buffer_list
                .iter()
                .map(|(id, _)| *id)
                .filter(|id| self.buffer_case_tables(*id).is_none())
                .collect::<Vec<_>>();
            for id in holders {
                self.buffer_case_tables.install(id, old);
            }
        }
        self.standard_case_tables = Some(tables);
        self.buffer_case_tables.retain(|(_, held)| *held != tables);
    }

    pub fn standard_syntax_table_id(&self) -> CharTableRef {
        self.standard_syntax_table_id
    }

    /// BVAR (current_buffer, syntax_table).
    #[inline]
    pub fn current_syntax_table_id(&self) -> CharTableRef {
        self.buffer_syntax_tables
            .get(self.current_buffer_id())
            .unwrap_or(self.standard_syntax_table_id())
    }

    pub fn set_current_syntax_table(&mut self, id: CharTableRef) {
        let current_buffer_id = self.current_buffer_id();
        self.buffer_syntax_tables.insert(current_buffer_id, id);
    }

    pub fn category_docstring(&self, table: CharTableRef, category: u32) -> Value {
        let Some(index) = category.checked_sub(32) else {
            return Value::Nil;
        };
        let Some(Kind::Vector(docs)) = table.extra(0).map(Value::kind) else {
            return Value::Nil;
        };
        docs.get(index as usize).unwrap_or(Value::Nil)
    }

    pub fn define_category(
        &mut self,
        table: CharTableRef,
        category: u32,
        doc: Value,
    ) -> Result<(), LispError> {
        let Kind::Vector(docs) = table.extra(0).unwrap_or(Value::Nil).kind() else {
            return Err(LispError::WrongTypeArgument(
                "category-table-p".into(),
                Value::CharTable(table),
            ));
        };
        if !self.category_docstring(table, category).is_nil() {
            return Err(crate::lisp::primitives::values::doprnt_error(
                self,
                "Category `{}' is already defined",
                &[&char::from_u32(category).unwrap_or('?')],
            ));
        }
        docs.set((category - 32) as usize, doc);
        Ok(())
    }

    pub fn detach_markers_for_buffer(&mut self, buffer_id: u64) {
        if let Some(buffer) = self.buffer_object(buffer_id) {
            buffer.detach_markers();
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
        // Buffer::insert adjusts every ordinary marker, the windows'
        // starts and points among them. Only insertion before
        // all markers additionally moves the nil-insertion markers at POS.
        if before_markers && let Some(buffer) = self.buffer_object(buffer_id) {
            let state = buffer.borrow();
            let bytepos = state.marker_byte_position(pos + nchars);
            for marker in buffer.markers() {
                if marker.position() == Some(pos) {
                    marker.set_positions(pos + nchars, bytepos);
                }
            }
        }
    }

    /// coding.c:decode_coding_object's and encode_coding_object's marks
    /// before they replace [FROM, TO) of the current buffer where it
    /// stands: `need_adjustment' on each marker of its text (the whole
    /// indirect family's, one text in GNU, the windows' starts and points
    /// among them) at TO, or at FROM for an insertion-type one.
    pub(crate) fn mark_markers_for_conversion(&mut self, from: usize, to: usize) {
        for marker in self.buffer.markers() {
            let at = if marker.insertion_type() { from } else { to };
            marker.set_need_adjustment(marker.position() == Some(at));
        }
    }

    /// The marked markers put back once the conversion has left its text
    /// in [FROM, END): an insertion-type one at FROM, any other at END.
    pub(crate) fn settle_markers_after_conversion(&mut self, from: usize, end: usize) {
        {
            let state = self.buffer.borrow();
            let limit = state.size_total() + 1;
            for marker in self.buffer.markers() {
                if marker.need_adjustment() {
                    marker.set_need_adjustment(false);
                    let position = if marker.insertion_type() { from } else { end };
                    let position = position.clamp(1, limit);
                    marker.set_positions(position, state.marker_byte_position(position));
                }
            }
        }
        self.family_after_in_place_edit();
    }

    #[cfg(test)]
    pub(crate) fn attached_marker_count(&self) -> usize {
        std::iter::once(&self.buffer)
            .chain(self.inactive_buffers.iter().map(|(_, buffer)| buffer))
            .map(|buffer| buffer.markers().count())
            .sum()
    }
}

/// A buffer's four case tables (buffer.h's downcase_table, upcase_table,
/// case_canon_table and case_eqv_table), or casetab.c's four
/// Vascii_*_table.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CaseTables {
    pub(crate) down: CharTableRef,
    pub(crate) up: CharTableRef,
    pub(crate) canon: CharTableRef,
    pub(crate) eqv: CharTableRef,
}

impl CaseTables {
    pub(crate) fn each(&self) -> [CharTableRef; 4] {
        [self.down, self.up, self.canon, self.eqv]
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn map(self, mut f: impl FnMut(CharTableRef) -> CharTableRef) -> Self {
        CaseTables {
            down: f(self.down),
            up: f(self.up),
            canon: f(self.canon),
            eqv: f(self.eqv),
        }
    }
}

/// One of buffer.h's char-table slots (syntax_table, category_table) of
/// each live buffer whose table is not the standard one, with the last
/// buffer's found: the scans, the matcher, `char-syntax' and the
/// category lookups read BVAR (current_buffer, ...) at each call.
#[derive(Clone, Default)]
pub(crate) struct BufferCharTables {
    tables: HashMap<u64, CharTableRef, crate::lisp::types::IdentityBuildHasher>,
    /// The buffer looked up last and its table, until the tables change.
    last: Cell<Option<(u64, Option<CharTableRef>)>>,
}

impl BufferCharTables {
    /// BUFFER's own table.
    #[inline]
    pub(crate) fn get(&self, buffer: u64) -> Option<CharTableRef> {
        if let Some((id, table)) = self.last.get()
            && id == buffer
        {
            return table;
        }
        let table = self.tables.get(&buffer).copied();
        self.last.set(Some((buffer, table)));
        table
    }

    pub(crate) fn insert(&mut self, buffer: u64, table: CharTableRef) {
        self.last.set(None);
        self.tables.insert(buffer, table);
    }

    pub(crate) fn remove(&mut self, buffer: u64) {
        self.last.set(None);
        self.tables.remove(&buffer);
    }

    pub(crate) fn values(&self) -> impl Iterator<Item = &CharTableRef> {
        self.tables.values()
    }

    #[cfg(test)]
    pub(crate) fn values_mut(&mut self) -> impl Iterator<Item = &mut CharTableRef> {
        self.last.set(None);
        self.tables.values_mut()
    }
}

/// buffer.h's four case-table slots of each live buffer whose four are
/// not the standard ones (a buffer without an entry holds those), with
/// the last buffer's found: a search reads its buffer's slots each call,
/// as BVAR (current_buffer, case_canon_table) is read.
#[derive(Clone, Default)]
pub(crate) struct BufferCaseTables {
    entries: Vec<(u64, CaseTables)>,
    /// The buffer looked up last and its entry, until the entries change.
    last: Cell<Option<(u64, Option<CaseTables>)>>,
}

impl BufferCaseTables {
    /// BUFFER's entry.
    #[inline]
    pub(crate) fn get(&self, buffer: u64) -> Option<CaseTables> {
        if let Some((id, tables)) = self.last.get()
            && id == buffer
        {
            return tables;
        }
        let tables = self
            .entries
            .iter()
            .find(|(id, _)| *id == buffer)
            .map(|(_, tables)| *tables);
        self.last.set(Some((buffer, tables)));
        tables
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &(u64, CaseTables)> {
        self.entries.iter()
    }

    #[cfg(test)]
    pub(crate) fn iter_mut(&mut self) -> impl Iterator<Item = &mut (u64, CaseTables)> {
        self.last.set(None);
        self.entries.iter_mut()
    }

    /// BUFFER's four set to TABLES, its own entry.
    pub(crate) fn install(&mut self, buffer: u64, tables: CaseTables) {
        self.remove(buffer);
        self.entries.push((buffer, tables));
    }

    /// BUFFER back to the standard four (or killed).
    pub(crate) fn remove(&mut self, buffer: u64) {
        self.retain(|(id, _)| *id != buffer);
    }

    pub(crate) fn retain(&mut self, keep: impl FnMut(&(u64, CaseTables)) -> bool) {
        self.last.set(None);
        self.entries.retain(keep);
    }
}
