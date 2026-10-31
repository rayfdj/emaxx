use super::*;
use crate::lisp::types::Kind;

pub(crate) fn check_hash_table(
    value: &Value,
) -> Result<crate::lisp::types::HashTableRef, LispError> {
    match value.kind() {
        Kind::HashTable(table) => Ok(table),
        _ => Err(LispError::WrongTypeArgument("hash-table-p".into(), *value)),
    }
}

pub(crate) fn check_hash_table_mutable(
    table: crate::lisp::types::HashTableRef,
) -> Result<(), LispError> {
    if table.is_mutable() {
        Ok(())
    } else {
        Err(LispError::Signal("hash table test modifies table".into()))
    }
}

pub(crate) fn hash_table_weakness(
    interp: &Interpreter,
    value: Value,
    env: &Env,
) -> Result<u8, LispError> {
    if value.is_nil() {
        return Ok(0);
    }
    if values_eq_in_env(interp, &value, &Value::T, env) {
        return Ok(4);
    }
    for (name, code) in [
        ("key", 1),
        ("value", 2),
        ("key-or-value", 3),
        ("key-and-value", 4),
    ] {
        if values_eq_in_env(interp, &value, &Value::symbol(name), env) {
            return Ok(code);
        }
    }
    Err(LispError::SignalValue(Value::list([
        Value::symbol("error"),
        Value::string("Invalid hash table weakness"),
        value,
    ])))
}

pub(crate) fn hash_table_weakness_value(table: crate::lisp::types::HashTableRef) -> Value {
    match table.weakness() {
        0 => Value::Nil,
        1 => Value::symbol("key"),
        2 => Value::symbol("value"),
        3 => Value::symbol("key-or-value"),
        4 => Value::symbol("key-and-value"),
        _ => unreachable!("valid hash-table weakness"),
    }
}

pub(crate) fn hash_table_test_descriptor(
    interp: &Interpreter,
    mut test: Value,
    env: &Env,
) -> Result<&'static crate::lisp::alloc::vectors::hash_tables::HashTableTest, LispError> {
    use crate::lisp::alloc::vectors::hash_tables::descriptor;
    use crate::lisp::eval::RuntimeHashTest;
    if symbols_with_pos_enabled(interp, env)
        && let Kind::SymbolWithPos(symbol) = test.kind()
    {
        test = symbol.symbol();
    }
    let standard = [
        ("eq", RuntimeHashTest::Eq),
        ("eql", RuntimeHashTest::Eql),
        ("equal", RuntimeHashTest::Equal),
    ]
    .into_iter()
    .find_map(|(name, kind)| (test.word() == Value::symbol(name).word()).then_some(kind));
    let (compare, hash) = if standard.is_some() {
        (Value::Nil, Value::Nil)
    } else {
        let name = test.as_symbol()?;
        let functions = interp
            .get_symbol_property(name, "hash-table-test")
            .and_then(|prop| prop.cons_values())
            .and_then(|(compare, tail)| tail.cons_values().map(|(hash, _)| (compare, hash)));
        functions.ok_or_else(|| {
            LispError::SignalValue(Value::list([
                Value::symbol("error"),
                Value::string("Invalid hash table test"),
                test,
            ]))
        })?
    };
    Ok(descriptor(test, compare, hash, standard, |left, right| {
        values_eq_in_env(interp, left, right, env)
    }))
}

pub(crate) fn hash_table_from_test(
    descriptor: &'static crate::lisp::alloc::vectors::hash_tables::HashTableTest,
    capacity: usize,
    weak: u8,
    purecopy: Value,
) -> Result<Value, LispError> {
    if capacity > i32::MAX as usize / 2 {
        return Err(LispError::Signal("Hash table too large".into()));
    }
    Ok(Value::HashTable(crate::lisp::types::HashTableRef::new(
        descriptor,
        capacity,
        weak | if purecopy.is_truthy() { 1 << 5 } else { 0 },
    )))
}

pub(crate) fn make_hash_table_value(
    interp: &Interpreter,
    test: Value,
    capacity: usize,
    weakness: Value,
    purecopy: Value,
    env: &Env,
) -> Result<Value, LispError> {
    let descriptor = hash_table_test_descriptor(interp, test, env)?;
    let weak = hash_table_weakness(interp, weakness, env)?;
    hash_table_from_test(descriptor, capacity, weak, purecopy)
}

pub(crate) fn call_hash_table_test_function(
    interp: &mut Interpreter,
    table: &Value,
    function: &Value,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    let table = check_hash_table(table)?;
    // fns.c:hash_table_user_defined_call protects only the outer mutable
    // call. Nested calls retain the previous flag and GC inhibition.
    if !table.is_mutable() {
        return call_function_value(interp, function, args, env);
    }
    interp.inhibit_garbage_collection();
    table.set_mutable(false);
    let result = call_function_value(interp, function, args, env);
    table.set_mutable(true);
    interp.allow_garbage_collection();
    result
}

pub(crate) fn reduce_hash_code(hash: u64) -> u32 {
    // lisp.h:reduce_emacs_uint_to_hash_hash.
    (hash ^ (hash >> 32)) as u32
}

pub(crate) fn standard_hash_code(
    interp: &Interpreter,
    test: crate::lisp::eval::RuntimeHashTest,
    key: &Value,
) -> u32 {
    reduce_hash_code(standard_table_hash(interp, test, *key))
}

pub(crate) fn hash_table_hash_code(
    interp: &mut Interpreter,
    table: crate::lisp::types::HashTableRef,
    key: &Value,
    env: &mut Env,
) -> Result<u32, LispError> {
    if let Some(test) = table.test().standard_test() {
        return Ok(standard_hash_code(interp, test, key));
    }
    let result = call_hash_table_test_function(
        interp,
        &Value::HashTable(table),
        &table.test().user_hash,
        std::slice::from_ref(key),
        env,
    )?;
    // hashfn_user_defined: a fixnum's XUFIXNUM, any other object's sxhash.
    let hash = match result.kind() {
        Kind::Integer(_) => (result.word() >> 2) as u64,
        _ => sxhash(interp, result),
    };
    Ok(reduce_hash_code(hash))
}

pub(crate) fn hash_table_key_matches(
    interp: &mut Interpreter,
    table: crate::lisp::types::HashTableRef,
    left: &Value,
    right: &Value,
    env: &mut Env,
) -> Result<bool, LispError> {
    use crate::lisp::eval::RuntimeHashTest;
    match table.test().standard_test() {
        Some(RuntimeHashTest::Eq) => Ok(false),
        Some(RuntimeHashTest::Eql) => Ok(values_eql_in_env(interp, left, right, env)),
        Some(RuntimeHashTest::Equal) => values_equal(interp, left, right),
        None => Ok(call_hash_table_test_function(
            interp,
            &Value::HashTable(table),
            &table.test().user_compare,
            &[*left, *right],
            env,
        )?
        .is_truthy()),
    }
}

pub(crate) fn hash_table_lookup(
    interp: &mut Interpreter,
    table: crate::lisp::types::HashTableRef,
    key: &Value,
    env: &mut Env,
) -> Result<(u32, Option<usize>), LispError> {
    let hash = hash_table_hash_code(interp, table, key, env)?;
    let mut slot = table.first_in_bucket(hash);
    while slot >= 0 {
        let index = slot as usize;
        let (stored, _) = table.entry(index).expect("live hash chain");
        if values_eq_in_env(interp, key, &stored, env)
            || (table.stored_hash(index) == hash
                && hash_table_key_matches(interp, table, key, &stored, env)?)
        {
            return Ok((hash, Some(index)));
        }
        // A user comparator cannot resize this table; no array reference
        // spans that call, and nested probes restore the same mutable flag.
        slot = table.next_in_bucket(index);
    }
    Ok((hash, None))
}

pub(crate) fn hash_table_put(
    interp: &mut Interpreter,
    table: crate::lisp::types::HashTableRef,
    key: Value,
    value: Value,
    env: &mut Env,
) -> Result<(), LispError> {
    check_hash_table_mutable(table)?;
    let (hash, slot) = hash_table_lookup(interp, table, &key, env)?;
    if let Some(slot) = slot {
        table.set_value(slot, value);
    } else {
        table.insert(hash, key, value);
    }
    Ok(())
}

pub(crate) fn sweep_weak_hash_tables(
    interp: &mut Interpreter,
    reachability: crate::lisp::eval::WeakHashReachability,
) {
    for (table, remove) in reachability.tables {
        for slot in remove {
            table.remove(slot);
        }
    }
    let epoch = reachability.epoch;
    let live = interp
        .modules
        .record_ids()
        .into_iter()
        .filter(|id| {
            interp
                .record_ref(*id)
                .is_some_and(|record| record.mark_bit().is_marked(epoch))
        })
        .collect::<crate::lisp::eval::MarkedIds>();
    interp.modules.collect(&live);
}

pub(crate) fn parse_xml_region(xml: &str, discard_comments: bool) -> Value {
    parse_libxml_region(xml, discard_comments, false)
}

pub(crate) fn parse_html_region(html: &str, discard_comments: bool) -> Value {
    parse_libxml_region(html, discard_comments, true)
}

/// Parse using the same libxml2 modes as GNU Emacs' `xml.c`.
///
/// In particular, libxml2's `NOBLANKS` behavior cannot be reproduced by
/// dropping every whitespace-only text node after parsing: it retains some
/// whitespace adjacent to mixed content.  Using the same parser also preserves
/// GNU's recovery behavior for malformed HTML instead of substituting HTML5's
/// different adoption-agency rules.
fn parse_libxml_region(source: &str, discard_comments: bool, html: bool) -> Value {
    use libxml::bindings;
    // xml.c parse_region's exact htmlReadMemory/xmlReadMemory calls, made
    // directly so the "utf-8" encoding string stays OWNED across the call:
    // the libxml crate's parse_string_with_options builds its encoding
    // CString inside a match arm and passes the pointer after the CString
    // is dropped (use-after-free), so every parse after the first read a
    // reused heap block as the encoding name and failed on non-ASCII input
    // nondeterministically (shr-tests' nonbr.html stopped at its first
    // no-break space on the second parse in a session).
    let encoding = std::ffi::CString::new("utf-8").expect("static text has no NUL");
    let doc_ptr = unsafe {
        bindings::xmlInitParser();
        if html {
            bindings::htmlReadMemory(
                source.as_ptr() as *const std::os::raw::c_char,
                std::os::raw::c_int::try_from(source.len()).unwrap_or(std::os::raw::c_int::MAX),
                std::ptr::null(),
                encoding.as_ptr(),
                (bindings::htmlParserOption_HTML_PARSE_RECOVER
                    | bindings::htmlParserOption_HTML_PARSE_NONET
                    | bindings::htmlParserOption_HTML_PARSE_NOWARNING
                    | bindings::htmlParserOption_HTML_PARSE_NOERROR
                    | bindings::htmlParserOption_HTML_PARSE_NOBLANKS)
                    as std::os::raw::c_int,
            )
        } else {
            bindings::xmlReadMemory(
                source.as_ptr() as *const std::os::raw::c_char,
                std::os::raw::c_int::try_from(source.len()).unwrap_or(std::os::raw::c_int::MAX),
                std::ptr::null(),
                encoding.as_ptr(),
                (bindings::xmlParserOption_XML_PARSE_NONET
                    | bindings::xmlParserOption_XML_PARSE_NOWARNING
                    | bindings::xmlParserOption_XML_PARSE_NOBLANKS
                    | bindings::xmlParserOption_XML_PARSE_NOERROR)
                    as std::os::raw::c_int,
            )
        }
    };
    drop(encoding);
    if doc_ptr.is_null() {
        // `xmlReadMemory' and `htmlReadMemory' return NULL on failure, which
        // GNU exposes as nil rather than as a Lisp signal.
        return Value::Nil;
    }
    let document = libxml::tree::Document::new_ptr(doc_ptr);
    let Some(root) = document.get_root_element() else {
        return Value::Nil;
    };

    // GNU's legacy DISCARD-COMMENTS argument skips only the document-level
    // sibling scan.  Comments inside the root remain in the DOM in both modes.
    if discard_comments {
        return libxml_node_value(&root);
    }

    let mut first = root.clone();
    while let Some(previous) = first.get_prev_sibling() {
        first = previous;
    }
    let mut nodes = Vec::new();
    let mut previous = Value::Nil;
    let mut current = Some(first);
    while let Some(node) = current {
        current = node.get_next_sibling();
        if !previous.is_nil() {
            nodes.push(previous);
        }
        previous = libxml_node_value(&node);
    }
    if nodes.is_empty() {
        // This intentionally asks libxml2 for the root again rather than
        // returning `previous': GNU does the same when leading DTD/unsupported
        // document nodes converted to nil and never seeded its accumulator.
        document
            .get_root_element()
            .as_ref()
            .map_or(Value::Nil, libxml_node_value)
    } else {
        Value::list(
            [Value::symbol("top"), Value::Nil]
                .into_iter()
                .chain(nodes)
                .chain([previous]),
        )
    }
}

fn libxml_node_value(node: &LibxmlNode) -> Value {
    match node.get_type() {
        Some(LibxmlNodeType::ElementNode) => {
            let attributes = libxml_attributes_in_source_order(node);
            let attributes = if attributes.is_empty() {
                Value::Nil
            } else {
                Value::list(attributes)
            };
            Value::list(
                [Value::symbol(&node.get_name()), attributes]
                    .into_iter()
                    .chain(node.get_child_nodes().iter().map(libxml_node_value)),
            )
        }
        Some(LibxmlNodeType::TextNode | LibxmlNodeType::CDataSectionNode) => {
            Value::String(node.get_content().into())
        }
        Some(LibxmlNodeType::CommentNode) => Value::list([
            Value::symbol("comment"),
            Value::Nil,
            Value::String(node.get_content().into()),
        ]),
        _ => Value::Nil,
    }
}

fn libxml_attributes_in_source_order(node: &LibxmlNode) -> Vec<Value> {
    let mut attributes = Vec::new();
    let node_ptr = node.node_ptr();
    if node_ptr.is_null() {
        return attributes;
    }

    // `libxml::Node::get_properties' returns a HashMap and therefore loses the
    // observable source order that GNU preserves by walking xmlAttr::next.
    // The document owns these pointers for the full duration of this scan.
    let mut attribute = unsafe { (*node_ptr).properties };
    while !attribute.is_null() {
        // SAFETY: `attribute' is a non-null node in this document's linked
        // xmlAttr list, so its name and next pointers remain valid here.
        let (name, next) = unsafe {
            let name = if (*attribute).name.is_null() {
                String::new()
            } else {
                std::ffi::CStr::from_ptr((*attribute).name.cast())
                    .to_string_lossy()
                    .into_owned()
            };
            (name, (*attribute).next)
        };
        if !name.is_empty() {
            attributes.push(Value::cons(
                Value::symbol(&name),
                Value::String(node.get_property(&name).unwrap_or_default().into()),
            ));
        }
        attribute = next;
    }
    attributes
}

pub(crate) fn display_property_value(value: &Value, property: &str) -> Option<Value> {
    if let Kind::Vector(vector) = value.kind() {
        return vector
            .slots()
            .find_map(|item| display_property_value(&item, property));
    }
    if let Ok(items) = value.to_vec() {
        if let Some(Kind::Symbol(name)) = items.first().map(|v| v.kind())
            && name == property
        {
            return items.get(1).cloned();
        }
        for item in items {
            if let Some(found) = display_property_value(&item, property) {
                return Some(found);
            }
        }
    }
    None
}

/// The first position from FROM below TO in TEXT (whose first character
/// is at position START) where a directional embedding, override or
/// isolate makes a character display at a level its class does not have.
pub(crate) fn find_bidi_override(
    text: &str,
    start: usize,
    from: usize,
    to: usize,
    right_to_left: bool,
) -> Option<usize> {
    if !text
        .chars()
        .any(|character| matches!(character as u32, 0x202A..=0x202E | 0x2066..=0x2069))
    {
        return None;
    }

    use unicode_bidi::{BidiClass, BidiInfo, LTR_LEVEL, RTL_LEVEL};
    let bidi = BidiInfo::new(
        text,
        Some(if right_to_left { RTL_LEVEL } else { LTR_LEVEL }),
    );
    // The levels a strong character keeps under the paragraph's base
    // level; embeddings and isolates may move a weak or neutral one a
    // level above the base without making it confusing.
    let base = u8::from(right_to_left);
    let left_level = if right_to_left { 2 } else { 0 };
    for (character_index, (byte_index, _)) in text.char_indices().enumerate() {
        let position = start + character_index;
        if position < from {
            continue;
        }
        if position >= to {
            break;
        }
        let class = bidi.original_classes[byte_index];
        let level = bidi.levels[byte_index].number();
        let suspicious = match class {
            BidiClass::L | BidiClass::EN => level > left_level,
            BidiClass::R | BidiClass::AL => level > 1,
            BidiClass::AN
            | BidiClass::BN
            | BidiClass::CS
            | BidiClass::ES
            | BidiClass::ET
            | BidiClass::NSM
            | BidiClass::ON => level > base + 1,
            _ => false,
        };
        if suspicious {
            return Some(position);
        }
    }
    None
}

/// editfns.c:general_insert_function: each argument inserted by itself,
/// its own change -- a character as its multibyte form, or in a unibyte
/// buffer as CHAR_TO_BYTE8's byte (insert_1_both, nothing for no text),
/// a string as insert_from_string puts it -- anything else
/// `char-or-string-p'.
pub(crate) fn insert_impl(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
    inherit: bool,
    before_markers: bool,
) -> Result<Value, LispError> {
    for arg in args {
        if let Kind::Integer(code) = arg.kind()
            && (0..=0x3F_FFFF).contains(&code)
        {
            let multibyte = interp.buffer.borrow().is_multibyte();
            let mut text = String::new();
            let mut extended_chars = Vec::new();
            push_buffer_character(&mut text, &mut extended_chars, 0, code as u32, multibyte);
            insert_text_with_hooks(
                interp,
                &text,
                None,
                &extended_chars,
                inherit,
                before_markers,
                env,
            )?;
        } else if let Some(string) = string_like(arg) {
            if string.text.is_empty() {
                continue;
            }
            // insdel.c:insert_from_string_1 grafts the string's own
            // intervals.
            insert_text_with_hooks(
                interp,
                &string.text,
                string.props.as_ref(),
                &string.extended_chars,
                inherit,
                before_markers,
                env,
            )?;
        } else {
            return Err(LispError::WrongTypeArgument(
                "char-or-string-p".into(),
                *arg,
            ));
        }
    }
    Ok(Value::Nil)
}

pub(crate) fn insert_char_impl(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    need_arg_range("insert-char", args, 1, 3)?;
    // editfns.c:Finsert_char: CHECK_CHARACTER, CHECK_FIXNUM of a count
    // given, nothing for one below 1; the character's multibyte form in a
    // multibyte buffer, CHAR_TO_BYTE8 of it in a unibyte one.
    let ch = match args[0].kind() {
        Kind::Integer(ch) if (0..=0x3F_FFFF).contains(&ch) => ch as u32,
        _ => return Err(LispError::WrongTypeArgument("characterp".into(), args[0])),
    };
    let count = match args.get(1).filter(|value| !value.is_nil()) {
        Some(value) => match value.kind() {
            Kind::Integer(count) => count,
            _ => return Err(LispError::WrongTypeArgument("fixnump".into(), *value)),
        },
        None => 1,
    };
    let inherit = args.get(2).is_some_and(Value::is_truthy);
    if count <= 0 {
        return Ok(Value::Nil);
    }
    let multibyte = interp.buffer.borrow().is_multibyte();
    let mut text = String::new();
    let mut extended_chars = Vec::new();
    for offset in 0..count as usize {
        push_buffer_character(&mut text, &mut extended_chars, offset, ch, multibyte);
    }
    insert_text_with_hooks(interp, &text, None, &extended_chars, inherit, false, env)?;
    Ok(Value::Nil)
}

pub(crate) fn insert_text_with_hooks(
    interp: &mut Interpreter,
    text: &str,
    props: Option<&crate::intervals::Intervals>,
    extended_chars: &[(usize, u32)],
    inherit: bool,
    before_markers: bool,
    env: &mut crate::lisp::types::Env,
) -> Result<(), LispError> {
    insert_text_and_signal(
        interp,
        text,
        props,
        extended_chars,
        inherit,
        before_markers,
        None,
        env,
    )
}

/// What encode_coding_object and decode_coding_object do for a buffer
/// destination: prepare_to_modify_buffer at point, the text inserted
/// there by insert_from_gap, which leaves point and the markers at it
/// before the text, and signal_after_change for PRODUCED characters
/// (coding->produced_char, which counts a character decoded into a
/// unibyte buffer once however many bytes it took).
pub(crate) fn insert_text_before_point(
    interp: &mut Interpreter,
    text: &str,
    props: Option<&crate::intervals::Intervals>,
    extended_chars: &[(usize, u32)],
    produced: usize,
    env: &mut crate::lisp::types::Env,
) -> Result<(), LispError> {
    insert_text_and_signal(
        interp,
        text,
        props,
        extended_chars,
        false,
        false,
        Some(produced),
        env,
    )
}

#[allow(clippy::too_many_arguments)]
fn insert_text_and_signal(
    interp: &mut Interpreter,
    text: &str,
    props: Option<&crate::intervals::Intervals>,
    extended_chars: &[(usize, u32)],
    inherit: bool,
    before_markers: bool,
    // Some for an insertion that leaves point before the text and
    // reports this many characters to the after-change functions.
    keep_point: Option<usize>,
    env: &mut crate::lisp::types::Env,
) -> Result<(), LispError> {
    if text.is_empty() {
        return Ok(());
    }
    let point = interp.buffer.borrow().point();
    // insdel.c inserts at point as the before-change hooks left it.
    modify_current_buffer(interp, point, point, false, env, |interp, _, _| {
        let start = interp.buffer.borrow().point();
        // insdel.c:copy_text: a unibyte buffer takes each character's byte.
        let unibyte_copy = if interp.buffer.borrow().is_multibyte() {
            None
        } else {
            crate::lisp::primitives::unibyte_buffer_copy(text, extended_chars)
        };
        let (text, extended_chars) = match &unibyte_copy {
            Some(copy) => (copy.as_str(), &[][..]),
            None => (text, extended_chars),
        };
        // insdel.c: offset_intervals, then graft_intervals_into_buffer of
        // the string's intervals (copied over the text, or with INHERIT
        // merged into what the insertion gave it), or with no intervals
        // and no INHERIT the new text's properties cleared.
        let source = match props {
            Some(intervals) => crate::buffer::InsertedProperties::Intervals {
                source: intervals,
                inherit,
            },
            None => crate::buffer::InsertedProperties::None { inherit },
        };
        interp.insert_current_buffer_with(text, source, before_markers);
        interp.set_inserted_extended_chars(start, extended_chars);
        let mut inserted = text.chars().count();
        if let Some(produced) = keep_point {
            interp.buffer.borrow_mut().goto_char(start);
            inserted = produced;
        }
        Ok((Some(AfterChange::new(start, 0, inserted)), ()))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overlay_collection_follows_buffer_and_lisp_ownership() {
        let mut interp = Interpreter::new();
        let mut env = Env::new();
        let table = json::make_hash_table(&mut interp, "eq", Vec::new());
        let Kind::HashTable(table_id) = table.kind() else {
            panic!("hash table is not a record");
        };
        table_id.set_weakness(1);
        interp.set_global_binding("overlay-weak-table", table);
        #[inline(never)]
        fn exercise_reachable(
            interp: &mut Interpreter,
            table_id: crate::lisp::types::HashTableRef,
            env: &mut Env,
        ) {
            let overlay = call(
                interp,
                "make-overlay",
                &[Value::Integer(1), Value::Integer(1)],
                env,
            )
            .expect("make overlay");
            let Kind::Overlay(id) = overlay.kind() else {
                panic!("not an overlay");
            };
            assert!(matches!(
                interp.equal_hash_put(table_id, overlay, Value::T, env),
                Ok(true)
            ));
            call(interp, "garbage-collect", &[], env).expect("collect attached overlay");
            assert_eq!(table_id.count(), 1);

            interp.set_global_binding("overlay-root", overlay);
            // A self-cycle does not make the detached object an independent root.
            call(
                interp,
                "overlay-put",
                &[overlay, Value::symbol("self"), overlay],
                env,
            )
            .expect("set cycle");
            call(interp, "delete-overlay", &[overlay], env).expect("detach");
            call(interp, "garbage-collect", &[], env).expect("collect rooted detached overlay");
            assert!(id.is_dead());
            assert_eq!(
                id.get_symbol_prop("self").expect("surviving plist").word(),
                overlay.word()
            );
            assert_eq!(table_id.count(), 1);
        }
        // No overlay address escapes this frame; retain the attached and
        // detached live-root checks before testing eventual reclamation.
        exercise_reachable(&mut interp, table_id, &mut env);
        let retained_bytes = crate::lisp::alloc::vectors::live_overlay_node_bytes();
        interp.set_global_binding("overlay-root", Value::Nil);
        crate::lisp::alloc::clobber_stack();
        call(&mut interp, "garbage-collect", &[], &mut env).expect("collect unreachable cycle");
        assert_eq!(
            crate::lisp::alloc::vectors::live_overlay_node_bytes(),
            retained_bytes - 80,
            "the unreachable object's separately owned interval was reclaimed"
        );
        assert!((table_id.count() == 0));
    }

    #[test]
    fn weak_key_collection_uses_reachability() {
        let mut interp = Interpreter::new();
        let mut env = Env::new();
        let table = json::make_hash_table(&mut interp, "equal", Vec::new());
        let Kind::HashTable(id) = table.kind() else {
            panic!("hash table is not a record");
        };
        id.set_weakness(1);

        // The unrooted key is consed in a frame of its own and the stack
        // under the test cleared: a string's address in a local of a
        // scanned frame keeps it (a C local's object).
        #[inline(never)]
        fn insert_keys(
            interp: &mut Interpreter,
            id: crate::lisp::types::HashTableRef,
            env: &Env,
        ) -> Value {
            let rooted_key = Value::string("rooted key");
            let unrooted_key = Value::string("unrooted key");
            assert!(matches!(
                interp.equal_hash_put(id, rooted_key, Value::Integer(1), env),
                Ok(true)
            ));
            assert!(matches!(
                interp.equal_hash_put(id, unrooted_key, Value::Integer(2), env),
                Ok(true)
            ));
            interp.set_global_binding("weak-key-root", Value::cons(rooted_key, Value::Nil));
            rooted_key
        }
        let rooted_key = insert_keys(&mut interp, id, &env);
        interp.set_global_binding("weak-table-root", table);
        crate::lisp::alloc::clobber_stack();

        crate::lisp::primitives::call(&mut interp, "garbage-collect", &[], &mut env)
            .expect("collect weak table through the ordinary C-owned entry point");
        let entries = id
            .entries()
            .map(|(_, key, value)| (key, value))
            .collect::<Vec<_>>();
        assert_eq!(entries.len(), 1);
        assert!(
            crate::lisp::primitives::values_equal(&interp, &entries[0].0, &rooted_key,)
                .expect("equal")
        );
    }
}
