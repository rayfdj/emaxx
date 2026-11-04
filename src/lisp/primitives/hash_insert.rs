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

/// fns.c:make_hash_table of DESCRIPTOR, CAPACITY entries, WEAK and
/// PURECOPY: memory_full when an array cannot be allocated, "Hash table
/// too large" when its index would be.
pub(crate) fn hash_table_from_test(
    interp: &mut Interpreter,
    env: &Env,
    descriptor: &'static crate::lisp::alloc::vectors::hash_tables::HashTableTest,
    capacity: usize,
    weak: u8,
    purecopy: Value,
) -> Result<Value, LispError> {
    use crate::lisp::alloc::vectors::hash_tables::HashTableFailure;
    crate::lisp::types::HashTableRef::try_new(
        descriptor,
        capacity,
        weak | if purecopy.is_truthy() { 1 << 5 } else { 0 },
    )
    .map(Value::HashTable)
    .map_err(|failure| match failure {
        HashTableFailure::Allocation(failed) => allocation_error(interp, env, failed),
        HashTableFailure::TooLarge => LispError::Signal("Hash table too large".into()),
    })
}

pub(crate) fn make_hash_table_value(
    interp: &mut Interpreter,
    test: Value,
    capacity: usize,
    weakness: Value,
    purecopy: Value,
    env: &Env,
) -> Result<Value, LispError> {
    let descriptor = hash_table_test_descriptor(interp, test, env)?;
    let weak = hash_table_weakness(interp, weakness, env)?;
    hash_table_from_test(interp, env, descriptor, capacity, weak, purecopy)
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
            && name.is_named(property)
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
    // The bytes of the character's form in the buffer (CHAR_STRING's in a
    // multibyte one, one in a unibyte one), COUNT copies within
    // BUF_BYTES_MAX or a buffer_overflow; then a STRINGLEN-byte string of
    // copies (under 4000 bytes, whole characters) inserted while more
    // than that remains, and the rest last, each an `insert' of its own
    // with its change hooks.
    let len = if multibyte { char_string_length(ch) } else { 1 };
    if crate::lisp::primitives::file_io::BUF_BYTES_MAX / (len as i64) < count {
        return Err(crate::lisp::primitives::file_io::buffer_overflow());
    }
    let copies = |chars: usize| {
        let mut text = String::new();
        let mut extended_chars = Vec::new();
        for offset in 0..chars {
            push_buffer_character(&mut text, &mut extended_chars, offset, ch, multibyte);
        }
        (text, extended_chars)
    };
    let mut n = count as usize * len;
    let stringlen = n.min(4000 - 4000 % len);
    let (text, extended_chars) = copies(stringlen / len);
    while n > stringlen {
        interp.maybe_quit(env)?;
        insert_text_with_hooks(interp, &text, None, &extended_chars, inherit, false, env)?;
        n -= stringlen;
    }
    if n == stringlen {
        insert_text_with_hooks(interp, &text, None, &extended_chars, inherit, false, env)?;
    } else {
        let (rest, rest_extended) = copies(n / len);
        insert_text_with_hooks(interp, &rest, None, &rest_extended, inherit, false, env)?;
    }
    Ok(Value::Nil)
}

/// character.h CHAR_STRING's length: the bytes of character C in a
/// multibyte buffer (a raw byte's two).
fn char_string_length(c: u32) -> usize {
    match c {
        0..=0x7F => 1,
        0x80..=0x7FF => 2,
        0x800..=0xFFFF => 3,
        0x1_0000..=0x1F_FFFF => 4,
        0x20_0000..=0x3F_FF7F => 5,
        _ => 2,
    }
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
        InsertedIntervals::Given(props),
        extended_chars,
        inherit,
        before_markers,
        false,
        env,
    )
}

/// json.c Fjson_insert's insert_from_gap_1 of TEXT at point: point stays
/// before the text while the change is signaled and update_compositions
/// (PT, PT, CHECK_BORDER) runs, then SET_PT_BOTH moves it past.
pub(crate) fn insert_text_from_gap(
    interp: &mut Interpreter,
    text: &str,
    extended_chars: &[(usize, u32)],
    env: &mut crate::lisp::types::Env,
) -> Result<(), LispError> {
    insert_text_and_signal(
        interp,
        text,
        InsertedIntervals::Given(None),
        extended_chars,
        false,
        false,
        true,
        env,
    )
}

/// insdel.c:insert_from_buffer: TEXT, BUFFER_ID's text from FROM, inserted
/// at point with the intervals insert_from_buffer_1 takes for it once the
/// before-change hooks have run.
pub(crate) fn insert_buffer_text_with_hooks(
    interp: &mut Interpreter,
    text: &str,
    buffer_id: u64,
    from: usize,
    extended_chars: &[(usize, u32)],
    env: &mut crate::lisp::types::Env,
) -> Result<(), LispError> {
    insert_text_and_signal(
        interp,
        text,
        InsertedIntervals::Buffer { buffer_id, from },
        extended_chars,
        false,
        false,
        false,
        env,
    )
}

/// Where an insertion's intervals come from.
#[derive(Clone, Copy)]
enum InsertedIntervals<'a> {
    /// A string's, or none.
    Given(Option<&'a crate::intervals::Intervals>),
    /// insert_from_buffer_1's of a buffer's text from FROM: the buffer's
    /// tree itself for all its text, else copy_intervals of the part, read
    /// after the before-change hooks; for the current buffer's own text
    /// (its indirect family's in GNU, one text) read once the insertion
    /// has offset the intervals.
    Buffer { buffer_id: u64, from: usize },
}

/// What encode_coding_object and decode_coding_object do for a buffer
/// destination: prepare_to_modify_buffer at point, the text inserted
/// there (`insert_converted_text_at'), which leaves point and the markers
/// at it before the text, and signal_after_change for PRODUCED characters
/// (coding->produced_char, which counts a character decoded into a
/// unibyte buffer once however many bytes it took).
#[allow(clippy::too_many_arguments)]
pub(crate) fn insert_text_before_point(
    interp: &mut Interpreter,
    text: &str,
    props: Option<&crate::intervals::Intervals>,
    extended_chars: &[(usize, u32)],
    produced: usize,
    decoded: bool,
    layout: Option<&crate::lisp::primitives::coding::DecodeLayout>,
    env: &mut crate::lisp::types::Env,
) -> Result<(), LispError> {
    if text.is_empty() {
        return Ok(());
    }
    let point = interp.buffer.borrow().point();
    modify_current_buffer(interp, point, point, false, env, |interp, _, env| {
        let start = interp.buffer.borrow().point();
        insert_converted_text_at(
            interp,
            start,
            text,
            props,
            extended_chars,
            produced,
            decoded,
            layout,
            env,
        )?;
        interp.buffer.borrow_mut().goto_char(start);
        Ok((
            Some(AfterChange::new(start, 0, produced).composing(
                start,
                start + produced,
                CHECK_ALL,
            )),
            (),
        ))
    })
}

/// A conversion's text inserted at START, point, as insert_from_gap
/// inserts it (copy_text's bytes into a unibyte buffer) with no
/// properties of its own; then each `charset' run of PROPS, from START,
/// put through produce_charset's Fput_text_property, change hooks and
/// all.  A decoding does this with `buffer-undo-list' t and records the
/// PRODUCED characters after, as decode_coding does; an encoding's
/// insert_from_gap records what it inserts.
#[allow(clippy::too_many_arguments)]
pub(crate) fn insert_converted_text_at(
    interp: &mut Interpreter,
    start: usize,
    text: &str,
    props: Option<&crate::intervals::Intervals>,
    extended_chars: &[(usize, u32)],
    produced: usize,
    decoded: bool,
    layout: Option<&crate::lisp::primitives::coding::DecodeLayout>,
    env: &mut crate::lisp::types::Env,
) -> Result<(), LispError> {
    if let Some(layout) = layout {
        return insert_decoded_calls_at(interp, start, text, extended_chars, produced, layout, env);
    }
    let unibyte_copy = if interp.buffer.borrow().is_multibyte() {
        None
    } else {
        crate::lisp::primitives::unibyte_buffer_copy(text, extended_chars)
    };
    let (text, extended_chars) = match &unibyte_copy {
        Some(copy) => (copy.as_str(), &[][..]),
        None => (text, extended_chars),
    };
    let undo_list = if decoded {
        interp.buffer.borrow_mut().suspend_undo_for_decoding()
    } else {
        Value::T
    };
    interp.insert_current_buffer_with(
        text,
        crate::buffer::InsertedProperties::None { inherit: false },
        false,
    );
    interp.set_inserted_extended_chars(start, extended_chars);
    // Each run's plist, and the list the decoding set aside, held while
    // the change hooks run.
    let runs: Vec<(usize, usize, Value)> = props
        .map(|intervals| {
            intervals
                .iter()
                .filter(|(_, _, plist)| !plist.is_nil())
                .collect()
        })
        .unwrap_or_default();
    let mut held: Vec<Value> = runs.iter().map(|(_, _, plist)| *plist).collect();
    held.push(undo_list);
    let result = interp.with_lisp_stack_roots(&held, |interp| {
        for (from, to, plist) in &runs {
            crate::lisp::primitives::textprop::add_text_properties(
                interp,
                &Value::Integer((start + from) as i64),
                &Value::Integer((start + to) as i64),
                *plist,
                Value::Nil,
                env,
            )?;
        }
        Ok(())
    });
    if decoded {
        interp
            .buffer
            .borrow_mut()
            .resume_undo_after_decoding(undo_list, start, produced);
    }
    result
}

/// decode_coding's text in a buffer destination at START, point, a
/// decoder call at a time: each call's characters inserted after the
/// last's as produce_chars' insert_from_gap inserts them (point staying at
/// START), the `charset' runs and the compositions of the call then put
/// through produce_annotation's Fput_text_property, in the order it
/// reaches them; then decode_eol's change of the
/// text inserted, under dos each CR before a newline deleted (del_range_2,
/// uncounted by the change hooks) and under mac each CR made a newline in
/// place.  TEXT and EXTENDED_CHARS are the text as decode_eol leaves it,
/// LAYOUT says how it was made; the PRODUCED characters are recorded for
/// undo after, as decode_coding records them.
fn insert_decoded_calls_at(
    interp: &mut Interpreter,
    start: usize,
    text: &str,
    extended_chars: &[(usize, u32)],
    produced: usize,
    layout: &crate::lisp::primitives::coding::DecodeLayout,
    env: &mut crate::lisp::types::Env,
) -> Result<(), LispError> {
    // The text as produce_chars made it, before decode_eol.
    let (inserted, inserted_extended): (&str, &[(usize, u32)]) = match layout.eol.as_deref() {
        Some(eol) => (&eol.text, &eol.extended),
        None => (text, extended_chars),
    };
    let multibyte = interp.buffer.borrow().is_multibyte();
    let undo_list = interp.buffer.borrow_mut().suspend_undo_for_decoding();
    let total = inserted.chars().count();
    let chunks: Vec<usize> = if layout.chunks.is_empty() {
        vec![total]
    } else {
        layout.chunks.clone()
    };
    let result = interp.with_lisp_stack_roots(&[undo_list], |interp| {
        let mut from_byte = 0;
        let mut done = 0;
        let mut at = start;
        // The buffer bytes of a unibyte destination, for decode_eol's scan.
        let mut unibyte_text = String::new();
        for &count in &chunks {
            let to_byte = byte_after_chars(inserted, from_byte, count);
            let piece = &inserted[from_byte..to_byte];
            from_byte = to_byte;
            let piece_extended: Vec<(usize, u32)> = inserted_extended
                .iter()
                .filter(|(index, _)| (done..done + count).contains(index))
                .map(|&(index, code)| (index - done, code))
                .collect();
            let unibyte_copy = if multibyte {
                None
            } else {
                Some(unibyte_destination_text(piece, &piece_extended)?)
            };
            let (piece, piece_extended) = match &unibyte_copy {
                Some(copy) => (copy.as_str(), &[][..]),
                None => (piece, &piece_extended[..]),
            };
            if !multibyte {
                unibyte_text.push_str(piece);
            }
            interp.buffer.borrow_mut().goto_char(at);
            interp.insert_current_buffer_with(
                piece,
                crate::buffer::InsertedProperties::None { inherit: false },
                false,
            );
            interp.set_inserted_extended_chars(at, piece_extended);
            at = interp.buffer.borrow().point();
            interp.buffer.borrow_mut().goto_char(start);
            for annotation in
                crate::lisp::primitives::coding::annotations(&layout.charsets, &layout.compositions)
            {
                use crate::lisp::primitives::coding::Annotation;
                let (from, to) = match annotation {
                    Annotation::Charset(from, to, _) => (from, to),
                    Annotation::Composition(composition) => (composition.start, composition.end),
                };
                if from < done || to > done + count || from >= to {
                    continue;
                }
                let plist = match annotation {
                    Annotation::Charset(_, _, charset) => {
                        Value::list([Value::symbol("charset"), Value::symbol(charset)])
                    }
                    Annotation::Composition(composition) => match composition.property() {
                        Some(value) => Value::list([Value::symbol("composition"), value]),
                        None => continue,
                    },
                };
                interp.with_lisp_stack_roots(&[plist], |interp| {
                    crate::lisp::primitives::textprop::add_text_properties(
                        interp,
                        &Value::Integer((start + from) as i64),
                        &Value::Integer((start + to) as i64),
                        plist,
                        Value::Nil,
                        env,
                    )
                })?;
            }
            done += count;
        }
        let Some(eol) = layout.eol.as_deref() else {
            return Ok(());
        };
        // decode_eol over the inserted text: the CRs in it, a unibyte
        // destination's as its bytes have them.
        let (crs, length) = if multibyte {
            (eol.crs.clone(), total)
        } else {
            let bytes: Vec<char> = unibyte_text.chars().collect();
            let crs = bytes
                .iter()
                .enumerate()
                .filter(|&(index, &byte)| {
                    byte == '\r' && (!eol.dos || bytes.get(index + 1) == Some(&'\n'))
                })
                .map(|(index, _)| index)
                .collect();
            (crs, bytes.len())
        };
        if eol.dos {
            let positions: Vec<usize> = crs
                .iter()
                .enumerate()
                .map(|(deleted, &cr)| start + cr - deleted)
                .collect();
            interp.delete_chars_each_current_buffer(&positions);
        } else if !crs.is_empty() {
            let end = interp.buffer.borrow().point_max().min(start + length);
            let replaced = {
                let buffer = interp.buffer.borrow();
                buffer
                    .buffer_substring(start, end)
                    .map_err(|error| LispError::Signal(error.to_string()))?
                    .replace('\r', "\n")
            };
            let extended = interp.buffer.borrow().substring_extended_chars(start, end);
            interp
                .buffer
                .borrow_mut()
                .replace_text_in_place(start, end, &replaced, &extended);
            interp.family_after_in_place_edit();
        }
        Ok(())
    });
    interp.buffer.borrow_mut().goto_char(start);
    interp
        .buffer
        .borrow_mut()
        .resume_undo_after_decoding(undo_list, start, produced);
    result
}

/// The byte offset of TEXT COUNT characters after its byte FROM (its end
/// when fewer follow).
fn byte_after_chars(text: &str, from: usize, count: usize) -> usize {
    let mut seen = 0;
    for (offset, &byte) in text.as_bytes()[from..].iter().enumerate() {
        if (byte as i8) >= -0x40 {
            if seen == count {
                return from + offset;
            }
            seen += 1;
        }
    }
    text.len()
}

/// What produce_chars writes into a unibyte destination for TEXT: a
/// raw-byte character as its byte, any other in its multibyte form.
fn unibyte_destination_text(text: &str, extended: &[(usize, u32)]) -> Result<String, LispError> {
    let source = StringLike {
        text: text.to_string(),
        props: None,
        multibyte: true,
        extended_chars: extended.to_vec(),
    };
    let mut out = String::with_capacity(text.len());
    for code in source.character_codes_iter() {
        let code = code as u32;
        if crate::lisp::primitives::RAW_BYTE8_CHARS.contains(&code) {
            out.push(crate::lisp::primitives::raw_byte_regex_char(
                (code - crate::lisp::primitives::RAW_BYTE8_BASE) as u8,
            ));
        } else {
            let (encoded, width) = crate::lisp::types::string_data::encode_character(code)?;
            for &byte in &encoded[..width] {
                out.push(crate::lisp::primitives::raw_byte_regex_char(byte));
            }
        }
    }
    Ok(out)
}

#[allow(clippy::too_many_arguments)]
fn insert_text_and_signal(
    interp: &mut Interpreter,
    text: &str,
    props: InsertedIntervals<'_>,
    extended_chars: &[(usize, u32)],
    inherit: bool,
    before_markers: bool,
    from_gap: bool,
    env: &mut crate::lisp::types::Env,
) -> Result<(), LispError> {
    if text.is_empty() {
        return Ok(());
    }
    let point = interp.buffer.borrow().point();
    // insdel.c inserts at point as the before-change hooks left it.
    let inserted = modify_current_buffer(interp, point, point, false, env, |interp, _, env| {
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
        let copied;
        let source = match props {
            InsertedIntervals::Given(Some(intervals)) => {
                crate::buffer::InsertedProperties::Intervals {
                    source: intervals,
                    inherit,
                }
            }
            InsertedIntervals::Given(None) => crate::buffer::InsertedProperties::None { inherit },
            InsertedIntervals::Buffer { buffer_id, from } => {
                let current = interp.current_buffer_id();
                if interp.related_buffer_ids(current).contains(&buffer_id) {
                    crate::buffer::InsertedProperties::OwnText {
                        from,
                        current: buffer_id == current,
                    }
                } else {
                    let nchars = text.chars().count();
                    copied = interp.get_buffer_by_id(buffer_id).and_then(|source| {
                        if nchars < source.size_total() {
                            source.copy_intervals(from, from + nchars)
                        } else {
                            source
                                .intervals()
                                .map(|intervals| intervals.shared_from_zero())
                        }
                    });
                    match &copied {
                        Some(intervals) => crate::buffer::InsertedProperties::Intervals {
                            source: intervals,
                            inherit,
                        },
                        None => crate::buffer::InsertedProperties::None { inherit },
                    }
                }
            }
        };
        // insert_1_both's make_gap, after the before-change hooks: the
        // failed xrealloc of enlarge_buffer_text reaches memory_full.
        let reserved = interp.buffer.borrow_mut().try_reserve_insertion(text.len());
        if let Err(nbytes) = reserved {
            return Err(memory_full(interp, env, nbytes));
        }
        interp.insert_current_buffer_with(text, source, before_markers);
        interp.set_inserted_extended_chars(start, extended_chars);
        let inserted = text.chars().count();
        let change = AfterChange::new(start, 0, inserted);
        if from_gap {
            interp.buffer.borrow_mut().goto_char(start);
            return Ok((Some(change), inserted));
        }
        // insert, insert_from_string, insert_from_buffer: update
        // (opoint, PT, CHECK_BORDER).
        Ok((
            Some(change.composing_to_point(start, CHECK_BORDER)),
            inserted,
        ))
    })?;
    if from_gap {
        let point = interp.buffer.borrow().point();
        update_compositions(interp, point, point, CHECK_BORDER, env)?;
        let target = (point + inserted).min(interp.buffer.borrow().point_max());
        interp.buffer.borrow_mut().goto_char(target);
    }
    Ok(())
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
