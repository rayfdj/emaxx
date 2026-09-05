use super::*;

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

impl Interpreter {
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

    pub fn define_charset(&mut self, name: &str, plist: Value, supplementary: bool) -> i64 {
        let existing = self
            .charset_ids
            .iter()
            .rev()
            .find(|(registered, _)| registered == name)
            .map(|(_, id)| *id);
        let new_definition = existing.is_none();
        let id = existing.unwrap_or_else(|| {
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
        if new_definition {
            // charset.c Fdefine_charset_internal: `charset-list' gains the
            // name at its head; the ordered list appends a supplementary
            // charset at the end and inserts a normal one before the first
            // supplementary entry.
            self.charset_names.insert(0, name.to_string());
            if supplementary {
                self.charset_supplementary.insert(name.to_string());
                self.charset_priority.push(name.to_string());
            } else {
                let position = self
                    .charset_priority
                    .iter()
                    .position(|existing| self.charset_supplementary.contains(existing))
                    .unwrap_or(self.charset_priority.len());
                self.charset_priority.insert(position, name.to_string());
            }
        }
        if let Some((_, existing)) = self
            .charset_plists
            .iter_mut()
            .rev()
            .find(|(registered, _)| registered == name)
        {
            *existing = plist.clone();
        } else {
            self.charset_plists.push((name.to_string(), plist.clone()));
        }
        // charset.c Fdefine_charset_internal: a charset with an ISO final
        // byte enters the ISO_CHARSET_TABLE slot for its dimension and
        // 94/96 flavor (iso_chars_96 is a 96-wide first byte range), and a
        // new one joins Viso_2022_charset_list at the end.
        let items = plist.to_vec().unwrap_or_default();
        let property = |key: &str| {
            items.windows(2).find_map(|pair| {
                matches!(&pair[0], Value::Symbol(name) if name == key).then(|| pair[1].clone())
            })
        };
        if let Some(final_char) = property(":iso-final-char")
            .and_then(|value| value.as_integer().ok())
            .and_then(|value| u32::try_from(value).ok())
        {
            let dimension = property(":dimension")
                .and_then(|value| value.as_integer().ok())
                .unwrap_or(1);
            let chars_96 = property(":code-space")
                .and_then(|space| primitives::vector_items(&space).ok())
                .and_then(|values| {
                    Some((
                        values.first()?.as_integer().ok()?,
                        values.get(1)?.as_integer().ok()?,
                    ))
                })
                .is_some_and(|(min, max)| max - min + 1 == 96);
            self.declare_iso_charset(dimension, if chars_96 { 96 } else { 94 }, final_char, name);
            if new_definition
                && !self
                    .iso_2022_charset_list
                    .iter()
                    .any(|existing| existing == name)
            {
                self.iso_2022_charset_list.push(name.to_string());
            }
        }
        id
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
        // charset.c Fdefine_charset_alias: the alias joins `charset-list'.
        if !self.charset_names.iter().any(|existing| existing == alias) {
            self.charset_names.insert(0, alias.to_string());
        }
        Ok(())
    }

    pub fn charset_name_list(&self) -> Vec<String> {
        self.charset_names.clone()
    }

    pub fn charset_priority_list(&self) -> Vec<String> {
        self.charset_priority.clone()
    }

    pub fn set_charset_priority(&mut self, names: &[String]) {
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
        new_head.extend(tail);
        self.charset_priority = new_head;
        // charset.c Fset_charset_priority rebuilds Viso_2022_charset_list
        // in the new ordered-list order.
        let iso_2022 = std::mem::take(&mut self.iso_2022_charset_list);
        self.iso_2022_charset_list = self
            .charset_priority
            .iter()
            .filter(|charset| iso_2022.contains(charset))
            .cloned()
            .collect();
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

    /// The final chars claimed through `declare-equiv-charset' for this
    /// DIMENSION and CHARS bucket.  charset.c:1440 writes those straight into
    /// ISO_CHARSET_TABLE, which is the same table
    /// `get-unused-iso-final-char' reads (charset.c:1421), so a scan that
    /// only walks charset plists misses them.
    pub fn iso_charset_finals(&self, dimension: i64, chars_96: bool) -> Vec<i64> {
        self.iso_charsets
            .iter()
            .filter(|(d, c, _, _)| *d == dimension && (*c == 96) == chars_96)
            .map(|(_, _, final_char, _)| i64::from(*final_char))
            .collect()
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
            *existing = canonical.clone();
        } else {
            self.coding_aliases
                .push((alias.to_string(), canonical.clone()));
        }
        // coding.c:Fdefine_coding_system_alias recurses over the target's eol
        // subsidiaries: when CODING-SYSTEM has a vector eol-type, ALIAS-unix,
        // ALIAS-dos and ALIAS-mac are defined as aliases of the corresponding
        // subsidiary.  Without this, `euc-jp' resolves but `euc-jp-unix' does
        // not, though GNU has both.
        for suffix in ["unix", "dos", "mac"] {
            let alias_variant = format!("{alias}-{suffix}");
            let target_variant = format!("{canonical}-{suffix}");
            if self.coding_system_canonical_name(&alias_variant).is_none()
                && self.coding_system_canonical_name(&target_variant).is_some()
            {
                // coding.c reaches this via make_subsidiaries, which
                // interns each subsidiary name in the standard obarray.
                self.intern_symbol_name(&alias_variant);
                self.coding_aliases.push((alias_variant, target_variant));
            }
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
            "utf-8" => match type_args.first() {
                None | Some(Value::Nil) => CODING_CATEGORY_UTF_8_NOSIG,
                Some(Value::T) => CODING_CATEGORY_UTF_8_SIG,
                Some(_) => CODING_CATEGORY_UTF_8_AUTO,
            },
            "utf-16" => {
                let bom = type_args.first().cloned().unwrap_or(Value::Nil);
                let big_endian =
                    !matches!(type_args.get(1), Some(Value::Symbol(endian)) if endian == "little");
                match bom {
                    Value::Cons(_) => CODING_CATEGORY_UTF_16_AUTO,
                    Value::Nil if big_endian => CODING_CATEGORY_UTF_16_BE_NOSIG,
                    Value::Nil => CODING_CATEGORY_UTF_16_LE_NOSIG,
                    _ if big_endian => CODING_CATEGORY_UTF_16_BE,
                    _ => CODING_CATEGORY_UTF_16_LE,
                }
            }
            "iso-2022" => {
                let full_support =
                    matches!(charset_list, Value::Symbol(name) if name == "iso-2022");
                let flags = type_args
                    .get(3)
                    .and_then(|flags| flags.as_integer().ok())
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
                        .and_then(|initial| primitives::vector_items(initial).ok())
                        .and_then(|items| items.get(1).cloned())
                        .filter(|charset| !charset.is_nil());
                    let g1_dimension = g1
                        .and_then(|charset| charset.as_symbol().ok().map(str::to_string))
                        .and_then(|charset| self.charset_plist_value(&charset))
                        .and_then(|plist| plist.to_vec().ok())
                        .and_then(|items| {
                            items.windows(2).find_map(|pair| {
                                matches!(&pair[0], Value::Symbol(key) if key == ":dimension")
                                    .then(|| pair[1].as_integer().ok())
                                    .flatten()
                            })
                        });
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

    /// coding.c Fset_coding_system_priority: the given systems' categories
    /// move to the front in the given order (later duplicates of a category
    /// are ignored), each becoming its category's representative; the
    /// remaining categories keep their previous relative order.  The Lisp
    /// `coding-category-list' and the `coding-category-*' variables follow.
    pub fn set_coding_system_categories_priority(
        &mut self,
        names: &[String],
        env: &mut Env,
    ) -> Result<(), LispError> {
        let mut changed = [false; CODING_CATEGORY_COUNT];
        let mut priorities = Vec::with_capacity(CODING_CATEGORY_COUNT);
        for name in names {
            let canonical = self
                .coding_system_canonical_name(name)
                .ok_or_else(|| LispError::Void(name.clone()))?;
            let category = self
                .coding_system(&canonical)
                .map(|coding| coding.category)
                .unwrap_or(CODING_CATEGORY_UNDECIDED);
            if changed[category] {
                continue;
            }
            changed[category] = true;
            priorities.push(category);
            self.coding_category_representatives[category] = Some(canonical.clone());
            self.set_variable(
                CODING_CATEGORY_NAMES[category],
                Value::Symbol(name.clone().into()),
                env,
            );
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
        self.set_variable("coding-category-list", list, env);
        Ok(())
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
        self.set_coding_system_categories_priority(names, &mut Vec::new())?;
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

    #[allow(clippy::too_many_arguments)]
    pub fn define_coding_system(
        &mut self,
        name: &str,
        mnemonic: i64,
        kind: &str,
        plist: Value,
        eol_type: Option<i64>,
        charset_list: Value,
        default_char: Option<u32>,
        type_args: Vec<Value>,
    ) -> Result<(), LispError> {
        if eol_type.is_none() {
            // An undecided eol-type creates the three eol subsidiaries via
            // coding.c's make_subsidiaries, which interns NAME-unix,
            // NAME-dos and NAME-mac in the standard obarray.
            for suffix in ["-unix", "-dos", "-mac"] {
                self.intern_symbol_name(&format!("{name}{suffix}"));
            }
        }
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
        // The Rust `kind' field is the internal codec discriminator, not
        // the public coding type (bootstrap gives its euc-jp entry kind
        // "euc-jp" while `coding-system-type' answers iso-2022 from the
        // plist).  When japanese.el re-defines japanese-iso-8bit with
        // :coding-type iso-2022, keep routing it to the EUC-JP codec;
        // without this the Lisp definition shadowed the bootstrap entry
        // and every euc-jp conversion fell to the raw-bytes default arms.
        let resolved_kind = self
            .coding_system_kind_name(&kind_canonical)
            .unwrap_or_else(|| kind_canonical.clone());
        let resolved_kind = if resolved_kind == "iso-2022" && name == "japanese-iso-8bit" {
            "euc-jp".to_string()
        } else if resolved_kind == "shift-jis" && name == "japanese-shift-jis" {
            // Same routing for the Shift-JIS codec: japanese.el's
            // re-definition must keep reaching it.
            "sjis".to_string()
        } else {
            resolved_kind
        };
        // The three type checks below compare the RAW :coding-type
        // argument, not its canonicalization: `big5' is also a coding
        // system name, and once chinese.el defines chinese-big5 and
        // re-points the alias, canonicalizing would stop matching on a
        // reload.  GNU compares the type symbol itself.
        if kind == "big5" {
            // coding.c's big5 twin of the shift-jis block below.
            self.big5_coding_system = name.to_string();
        }
        if kind == "shift-jis" || kind == "big5" {
            // coding.c:11357: Vsjis_coding_system tracks the most recent
            // shift-jis definition (japanese-shift-jis-2004 in a full
            // load), and the sjis-char primitives read their charsets
            // from it; Vbig5_coding_system is handled above.  Either
            // type's ascii compatibility comes from the FIRST charset
            // in :charset-list (ascii, hence t).
            if kind == "shift-jis" {
                self.sjis_coding_system = name.to_string();
            }
            let first_charset_ascii_compatible = items
                .windows(2)
                .find_map(|pair| {
                    matches!(&pair[0], Value::Symbol(key) if key == ":charset-list")
                        .then(|| pair[1].clone())
                })
                .and_then(|list| list.to_vec().ok())
                .and_then(|charsets| charsets.first().cloned())
                .is_some_and(|charset| matches!(&charset, Value::Symbol(name) if name == "ascii"));
            if first_charset_ascii_compatible {
                if let Some(index) = items.iter().position(
                    |item| matches!(item, Value::Symbol(key) if key == ":ascii-compatible-p"),
                ) {
                    if index + 1 < items.len() {
                        items[index + 1] = Value::T;
                    }
                } else {
                    items.push(Value::Symbol(":ascii-compatible-p".into()));
                    items.push(Value::T);
                }
            }
        }
        let category = self.coding_category_index(&kind_canonical, &charset_list, &type_args);
        // coding.c Fdefine_coding_system_internal: an iso-2022 system's
        // ascii compatibility is the :ascii-compatible-p argument, set to t
        // when its INITIAL G0 designation is an ascii-compatible charset
        // (coding.c:11285) -- and then reset to nil unless the category is
        // iso-8-1 or iso-8-2 (coding.c:11341).  So euc-jp's [ascii ...]
        // yields t though japanese.el passes nil, while iso-2022-jp's
        // (ascii ...) G0 request and the 7-bit systems stay nil; the
        // decoder's ASCII fast path and the lcsu naming rules read the
        // result.
        if kind_canonical == "iso-2022" {
            let g0_ascii_compatible = items
                .windows(2)
                .find_map(|pair| {
                    matches!(&pair[0], Value::Symbol(key) if key == ":designation")
                        .then(|| pair[1].clone())
                })
                .and_then(|designation| primitives::vector_items(&designation).ok())
                .and_then(|values| values.first().cloned())
                .is_some_and(|initial| match initial {
                    Value::Symbol(charset) => {
                        charset == "ascii"
                            || self
                                .charset_plist_value(&charset)
                                .and_then(|plist| plist.to_vec().ok())
                                .is_some_and(|items| {
                                    items.windows(2).any(|pair| {
                                        matches!(&pair[0], Value::Symbol(key)
                                            if key == ":ascii-compatible-p")
                                            && pair[1].is_truthy()
                                    })
                                })
                    }
                    _ => false,
                });
            let key_index = items.iter().position(
                |item| matches!(item, Value::Symbol(key) if key == ":ascii-compatible-p"),
            );
            let supplied = key_index
                .and_then(|index| items.get(index + 1))
                .is_some_and(Value::is_truthy);
            let eight_bit_category =
                matches!(category, CODING_CATEGORY_ISO_8_1 | CODING_CATEGORY_ISO_8_2);
            let ascii_compatible = eight_bit_category && (supplied || g0_ascii_compatible);
            let value = if ascii_compatible {
                Value::T
            } else {
                Value::Nil
            };
            match key_index {
                Some(index) if index + 1 < items.len() => items[index + 1] = value,
                _ => {
                    items.push(Value::Symbol(":ascii-compatible-p".into()));
                    items.push(value);
                }
            }
        }
        let definition = CodingSystemState {
            name: name.to_string(),
            base: name.to_string(),
            kind: resolved_kind,
            eol_type,
            plist: Value::list(items),
            category,
            charset_list: charset_list.clone(),
            default_char: default_char.unwrap_or(b' ' as u32),
            type_args: type_args.clone(),
        };
        // coding.c Fdefine_coding_system_internal: the category's
        // representative becomes this system when the category has none
        // yet, or when this is a redefinition of the representative itself.
        if self.coding_category_representatives[category]
            .as_deref()
            .is_none_or(|representative| representative == name)
        {
            self.coding_category_representatives[category] = Some(name.to_string());
        }
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
                    category: definition.category,
                    charset_list: definition.charset_list.clone(),
                    default_char: definition.default_char,
                    type_args: definition.type_args.clone(),
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
            .unwrap_or_else(|| "us-ascii".into())
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

    /// GNU keeps each window's point as real markers (w->pointm and
    /// w->old_pointm), so insdel.c's marker adjustment drags them with
    /// every insertion and deletion — a process filter inserting before
    /// markers at a non-selected window's point moves that window's
    /// point exactly like any other marker.  The native window records
    /// hold integer point slots instead; this sweep applies the same
    /// insertion-type-nil marker rules to those integers.  (The
    /// window-start slot stays under the native scroll recomputation.)
    fn adjust_window_point_slots(&mut self, buffer_id: u64, adjust: impl Fn(usize) -> usize) {
        const POINT_SLOTS: [usize; 2] = [
            crate::lisp::primitives::WINDOW_OLD_POINT_SLOT,
            crate::lisp::primitives::WINDOW_POINT_SLOT,
        ];
        for window_id in self.record_ids_by_type("window") {
            let Some(record) = self.find_record_mut(window_id) else {
                continue;
            };
            let shows_buffer = matches!(
                record.slots.get(crate::lisp::primitives::WINDOW_BUFFER_SLOT),
                Some(Value::Integer(id)) if *id == buffer_id as i64
            );
            if !shows_buffer {
                continue;
            }
            for slot in POINT_SLOTS {
                if let Some(Value::Integer(position)) = record.slots.get(slot)
                    && let Ok(position) = usize::try_from(*position)
                {
                    record.slots[slot] = Value::Integer(adjust(position) as i64);
                }
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
        self.adjust_window_point_slots(buffer_id, |position| {
            if position > pos || (position == pos && before_markers) {
                position + nchars
            } else {
                position
            }
        });
        let state = &mut **self;
        let Some(marker_ids) = state.markers_by_buffer.get(&buffer_id) else {
            return;
        };
        for marker_id in marker_ids {
            let Some(index) = Self::marker_index(*marker_id) else {
                continue;
            };
            let Some(marker) = state.markers.get_mut(index) else {
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
        self.adjust_window_point_slots(buffer_id, |position| {
            if position > to {
                position - nchars
            } else if position > from {
                from
            } else {
                position
            }
        });
        let state = &mut **self;
        let Some(marker_ids) = state.markers_by_buffer.get(&buffer_id) else {
            return;
        };
        for marker_id in marker_ids {
            let Some(index) = Self::marker_index(*marker_id) else {
                continue;
            };
            let Some(marker) = state.markers.get_mut(index) else {
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
