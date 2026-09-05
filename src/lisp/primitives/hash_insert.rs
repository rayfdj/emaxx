use super::*;

pub(crate) fn hash_table_user_test_functions(
    interp: &Interpreter,
    test: &str,
) -> Option<(Value, Value)> {
    let spec = interp.get_symbol_property(test, "hash-table-test")?;
    let items = spec.to_vec().ok()?;
    if items.len() != 2 {
        return None;
    }
    Some((items[0].clone(), items[1].clone()))
}

pub(crate) fn call_hash_table_test_function(
    interp: &mut Interpreter,
    table: &Value,
    function: &Value,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    let Value::Record(id) = table else {
        return Err(LispError::WrongTypeArgument(
            "hash-table-p".into(),
            table.clone(),
        ));
    };
    if !json::is_hash_table(interp, table) {
        return Err(LispError::WrongTypeArgument(
            "hash-table-p".into(),
            table.clone(),
        ));
    }

    // fns.c's hash_table_user_defined_call makes only this table immutable
    // while the callback runs and inhibits collection because the table's
    // temporary probing state is not markable.
    let entered = interp.enter_hash_table_test(*id);
    interp.inhibit_garbage_collection();
    let result = call_function_value(interp, function, args, env);
    interp.allow_garbage_collection();
    interp.leave_hash_table_test(*id, entered);
    result
}

pub(crate) fn touch_hash_table_key(
    interp: &mut Interpreter,
    table: &Value,
    test: &str,
    key: &Value,
    env: &mut Env,
) -> Result<(), LispError> {
    let Some((_, hash_fn)) = hash_table_user_test_functions(interp, test) else {
        return Ok(());
    };
    let _ = call_hash_table_test_function(interp, table, &hash_fn, std::slice::from_ref(key), env)?;
    Ok(())
}

fn custom_hash_code(
    interp: &mut Interpreter,
    table: &Value,
    test: &str,
    key: &Value,
    env: &mut Env,
) -> Result<i64, LispError> {
    let Some((_, hash_fn)) = hash_table_user_test_functions(interp, test) else {
        return Err(LispError::Signal("Invalid hash table test".into()));
    };
    let hash =
        call_hash_table_test_function(interp, table, &hash_fn, std::slice::from_ref(key), env)?;
    Ok(match hash {
        Value::Integer(hash) => hash,
        other => sxhash_value_in_env(interp, &other, HashMode::Equal, env),
    })
}

fn custom_hash_matching_index(
    interp: &mut Interpreter,
    table: &Value,
    id: u64,
    test: &str,
    key: &Value,
    hash: i64,
    env: &mut Env,
) -> Result<Option<(usize, Value)>, LispError> {
    let Some((compare_fn, _)) = hash_table_user_test_functions(interp, test) else {
        return Err(LispError::Signal("Invalid hash table test".into()));
    };
    let candidates = interp
        .custom_hash_candidates(id, hash)
        .expect("custom hash index disappeared during lookup");
    for (index, existing_key, value) in candidates {
        let identity_match = values_eq_in_env(interp, &existing_key, key, env);
        let comparison_match = !identity_match
            && call_hash_table_test_function(
                interp,
                table,
                &compare_fn,
                &[key.clone(), existing_key],
                env,
            )?
            .is_truthy();
        if identity_match || comparison_match {
            return Ok(Some((index, value)));
        }
    }
    Ok(None)
}

pub(crate) fn custom_hash_lookup_indexed(
    interp: &mut Interpreter,
    table: &Value,
    id: u64,
    test: &str,
    key: &Value,
    env: &mut Env,
) -> Result<Option<Value>, LispError> {
    let hash = custom_hash_code(interp, table, test, key, env)?;
    Ok(
        custom_hash_matching_index(interp, table, id, test, key, hash, env)?
            .map(|(_, value)| value),
    )
}

pub(crate) fn custom_hash_put_indexed(
    interp: &mut Interpreter,
    table: &Value,
    id: u64,
    test: &str,
    key: Value,
    value: Value,
    env: &mut Env,
) -> Result<bool, LispError> {
    let hash = custom_hash_code(interp, table, test, &key, env)?;
    let existing = custom_hash_matching_index(interp, table, id, test, &key, hash, env)?
        .map(|(index, _)| index);
    Ok(interp.custom_hash_put_at(id, hash, existing, key, value))
}

pub(crate) fn custom_hash_remove_indexed(
    interp: &mut Interpreter,
    table: &Value,
    id: u64,
    test: &str,
    key: &Value,
    env: &mut Env,
) -> Result<bool, LispError> {
    let hash = custom_hash_code(interp, table, test, key, env)?;
    let Some((index, _)) = custom_hash_matching_index(interp, table, id, test, key, hash, env)?
    else {
        return Ok(true);
    };
    Ok(interp.custom_hash_remove_at(id, index))
}

pub(crate) fn hash_table_key_matches(
    interp: &mut Interpreter,
    table: &Value,
    test: &str,
    left: &Value,
    right: &Value,
    env: &mut Env,
) -> Result<bool, LispError> {
    match test {
        "equal" => Ok(values_equal_in_env(interp, left, right, env)),
        "eq" => Ok(values_eq_in_env(interp, left, right, env)),
        "eql" => Ok(values_eql(left, right)),
        _ => {
            let Some((compare_fn, _)) = hash_table_user_test_functions(interp, test) else {
                return Err(LispError::Signal("Invalid hash table test".into()));
            };
            Ok(call_hash_table_test_function(
                interp,
                table,
                &compare_fn,
                &[left.clone(), right.clone()],
                env,
            )?
            .is_truthy())
        }
    }
}

pub(crate) fn collect_weak_hash_tables(
    interp: &mut Interpreter,
    env: &Env,
    native_roots: &[Value],
) -> Result<(), LispError> {
    let reachability = interp.weak_hash_reachability(env, native_roots);
    interp.install_gc_finalizers(reachability.live_finalizers, reachability.doomed_finalizers);
    for (id, entries, keep) in reachability.tables {
        interp.sweep_weak_hash_table(id, entries, &keep);
    }
    interp.install_gc_record_census(reachability.live_records);
    Ok(())
}

pub(crate) fn hash_table_metadata_slot(
    interp: &Interpreter,
    table: &Value,
    slot: usize,
    default: Value,
) -> Result<Value, LispError> {
    let Value::Record(id) = table else {
        return Err(LispError::WrongTypeArgument(
            "hash-table-p".into(),
            table.clone(),
        ));
    };
    let Some(record) = interp.find_record(*id) else {
        return Err(LispError::WrongTypeArgument(
            "hash-table-p".into(),
            table.clone(),
        ));
    };
    if record.kind != crate::lisp::eval::RecordKind::HashTable {
        return Err(LispError::WrongTypeArgument(
            "hash-table-p".into(),
            table.clone(),
        ));
    }
    Ok(record.slots.get(slot).cloned().unwrap_or(default))
}

pub(crate) fn hash_table_entries_to_value(entries: Vec<(Value, Value)>) -> Value {
    Value::list(
        entries
            .into_iter()
            .map(|(key, value)| Value::cons(key, value)),
    )
}

pub(crate) fn keymap_list_items(
    interp: &Interpreter,
    value: &Value,
) -> Result<Option<Vec<Value>>, LispError> {
    keymap_list_items_inner(interp, value, &mut HashSet::new(), &mut HashSet::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn weak_key_collection_uses_reachability() {
        let mut interp = Interpreter::new();
        let env = Env::new();
        let table = json::make_hash_table(&mut interp, "equal", Vec::new());
        let Value::Record(id) = table.clone() else {
            panic!("hash table is not a record");
        };
        interp.find_record_mut(id).expect("new hash table").slots[5] = Value::symbol("key");

        let rooted_key = Value::string("rooted key");
        let unrooted_key = Value::string("unrooted key");
        assert!(interp.equal_hash_put(id, rooted_key.clone(), Value::Integer(1), &env,));
        assert!(interp.equal_hash_put(id, unrooted_key, Value::Integer(2), &env));
        interp.set_global_binding("weak-table-root", table);
        interp.set_global_binding("weak-key-root", Value::cons(rooted_key.clone(), Value::Nil));

        collect_weak_hash_tables(&mut interp, &env, &[]).expect("collect weak table");
        let entries = interp
            .hash_table_runtime_entries(id)
            .expect("indexed hash table entries");
        assert_eq!(entries.len(), 1);
        assert!(crate::lisp::primitives::values_equal(
            &interp,
            &entries[0].0,
            &rooted_key,
        ));
    }

    #[test]
    fn weak_collection_retains_only_real_detached_c_slots() {
        use super::super::generated_gnu_c_forwarded_variables::{
            ForwardedVariableKind, GNU_C_FORWARDED_VARIABLES,
        };

        let mut interp = Interpreter::new();
        let mut env = Env::new();
        let table = json::make_hash_table(&mut interp, "equal", Vec::new());
        let Value::Record(id) = table.clone() else {
            panic!("hash table is not a record");
        };
        interp.find_record_mut(id).expect("new hash table").slots[5] = Value::symbol("key");
        interp.set_global_binding("weak-table-root", table);

        // A declaration belonging to an unavailable platform does not create
        // a live C slot when ordinary Lisp later uses the same symbol name.
        let inactive = GNU_C_FORWARDED_VARIABLES
            .iter()
            .find_map(|(name, kind)| {
                (*kind == ForwardedVariableKind::Lisp && interp.symbol_value_cell(name).is_err())
                    .then_some(*name)
            })
            .expect("the all-platform inventory contains unavailable C variables");
        let retained_key = Value::string("retained C value");
        let keys = [
            ("delayed-warnings-list", retained_key.clone()),
            ("plain-weak-root", Value::string("ordinary Lisp value")),
            (inactive, Value::string("unavailable C declaration")),
        ];
        for (name, key) in keys {
            assert!(interp.equal_hash_put(id, key.clone(), Value::Nil, &env));
            interp.set_global_binding(name, Value::cons(key, Value::Nil));
            call(&mut interp, "makunbound", &[Value::symbol(name)], &mut env)
                .expect("void the symbol");
            assert!(interp.symbol_value_cell(name).is_err());
        }
        assert!(
            !interp
                .detached_forwarded_variables
                .contains_key("plain-weak-root")
        );
        assert!(!interp.detached_forwarded_variables.contains_key(inactive));
        assert!(
            interp
                .detached_forwarded_variables
                .contains_key("delayed-warnings-list")
        );

        // Rebinding and voiding the Lisp symbol a second time must not
        // overwrite the independent C slot (data.c:set_internal).
        let replacement_key = Value::string("replacement plain Lisp value");
        assert!(interp.equal_hash_put(id, replacement_key.clone(), Value::Nil, &env));
        interp.set_global_binding(
            "delayed-warnings-list",
            Value::cons(replacement_key, Value::Nil),
        );
        call(
            &mut interp,
            "makunbound",
            &[Value::symbol("delayed-warnings-list")],
            &mut env,
        )
        .expect("void the new plain binding");
        let retained = interp
            .forwarded_c_value("delayed-warnings-list", &env)
            .expect("the original C slot still exists");
        assert!(values_eq_in_env(
            &interp,
            &retained
                .car()
                .expect("retained C slot contains its key cons"),
            &retained_key,
            &env
        ));

        // Deep image cloning must keep this independent root and its object
        // graph too. The weak table's copied key must alias the copied C slot.
        let mut copy = interp.deep_clone_image();
        for interpreter in [&mut interp, &mut copy] {
            collect_weak_hash_tables(interpreter, &env, &[]).expect("collect weak table");
            let entries = interpreter
                .hash_table_runtime_entries(id)
                .expect("indexed hash table");
            assert_eq!(entries.len(), 1);
            let slot_key = interpreter
                .forwarded_c_value("delayed-warnings-list", &env)
                .expect("retained C root")
                .car()
                .expect("copied C slot contains its key cons");
            assert!(values_eq_in_env(
                interpreter,
                &entries[0].0,
                &slot_key,
                &env
            ));
        }

        // An actual C assignment releases the old root, without rebinding
        // the detached Lisp symbol or consulting variable watchers.
        interp.set_forwarded_lisp_value("delayed-warnings-list", Value::Nil);
        assert!(interp.symbol_value_cell("delayed-warnings-list").is_err());
        collect_weak_hash_tables(&mut interp, &env, &[]).expect("collect released C value");
        assert!(
            interp
                .hash_table_runtime_entries(id)
                .expect("live weak table")
                .is_empty()
        );
    }

    #[test]
    fn clearing_detached_quit_slot_releases_its_old_gc_root() {
        let mut interp = Interpreter::new();
        let mut env = Env::new();
        let table = json::make_hash_table(&mut interp, "equal", Vec::new());
        let Value::Record(id) = table.clone() else {
            panic!("hash table is not a record");
        };
        interp.find_record_mut(id).expect("new hash table").slots[5] = Value::symbol("key");
        interp.set_global_binding("weak-table-root", table);
        let key = Value::string("quit payload");
        assert!(interp.equal_hash_put(id, key.clone(), Value::Nil, &env));
        interp.set_global_binding("quit-flag", Value::cons(key, Value::Nil));
        call(
            &mut interp,
            "makunbound",
            &[Value::symbol("quit-flag")],
            &mut env,
        )
        .expect("detach quit-flag");
        collect_weak_hash_tables(&mut interp, &env, &[]).expect("collect while quit is pending");
        assert_eq!(
            interp
                .hash_table_runtime_entries(id)
                .expect("live weak table")
                .len(),
            1
        );
        let error = interp
            .maybe_quit(&mut env)
            .expect_err("deliver pending quit");
        assert!(
            matches!(error, LispError::SignalValue(value) if value == Value::list([Value::symbol("quit")]))
        );
        assert_eq!(
            interp.forwarded_c_value("quit-flag", &env),
            Some(Value::Nil)
        );
        assert!(interp.symbol_value_cell("quit-flag").is_err());
        collect_weak_hash_tables(&mut interp, &env, &[]).expect("collect delivered quit payload");
        assert!(
            interp
                .hash_table_runtime_entries(id)
                .expect("live weak table")
                .is_empty()
        );
    }
}

fn keymap_list_items_inner(
    interp: &Interpreter,
    value: &Value,
    seen_keymaps: &mut HashSet<u64>,
    seen_cons: &mut HashSet<usize>,
) -> Result<Option<Vec<Value>>, LispError> {
    let Some(id) = keymap_record_id(interp, value) else {
        return Ok(None);
    };
    if let Some(view) = runtime_keymap_public_view(interp, value) {
        return view.to_vec().map(Some);
    }
    if !seen_keymaps.insert(id) {
        // GNU keymaps are cons graphs and may contain recursive prefix
        // bindings.  A repeated node is still recognizably a keymap, but
        // must not recurse forever while projecting the list interface.
        return Ok(Some(vec![Value::Symbol("keymap".into())]));
    }
    let Some(record) = interp.find_record(id) else {
        return Ok(None);
    };
    let char_table = keymap_char_table(record);
    let name = record.slots.first().cloned().filter(|name| !name.is_nil());
    let bindings = keymap_bindings(record)?;
    let mut items = vec![Value::Symbol("keymap".into())];
    if let Some(char_table) = char_table {
        items.push(char_table);
    }
    if let Some(name) = name {
        items.push(name);
    }
    for binding in bindings
        .iter()
        .filter(|binding| !binding.after_prompt)
        .chain(bindings.iter().filter(|binding| binding.after_prompt))
    {
        let value = project_embedded_keymaps(interp, &binding.value, seen_keymaps, seen_cons)?;
        items.push(Value::cons(
            keymap_entry_key_value(&binding_key_parts(binding), &binding.key),
            value,
        ));
    }
    seen_keymaps.remove(&id);
    Ok(Some(items))
}

fn project_embedded_keymaps(
    interp: &Interpreter,
    value: &Value,
    seen_keymaps: &mut HashSet<u64>,
    seen_cons: &mut HashSet<usize>,
) -> Result<Value, LispError> {
    if keymap_record_id(interp, value).is_some() {
        return Ok(Value::list(
            keymap_list_items_inner(interp, value, seen_keymaps, seen_cons)?
                .expect("keymap identity was checked above"),
        ));
    }

    let Some((car, cdr)) = (value).cons_cells() else {
        return Ok(value.clone());
    };
    let identity = car.cell_id();
    if !seen_cons.insert(identity) {
        // Preserve a circular non-keymap cons graph.  The caller's list
        // primitive remains responsible for reporting or traversing it.
        return Ok(value.clone());
    }
    let original_car = car.borrow().clone();
    let original_cdr = cdr.borrow().clone();
    let projected_car = project_embedded_keymaps(interp, &original_car, seen_keymaps, seen_cons)?;
    let projected_cdr = project_embedded_keymaps(interp, &original_cdr, seen_keymaps, seen_cons)?;
    seen_cons.remove(&identity);

    if values_eql(&projected_car, &original_car) && values_eql(&projected_cdr, &original_cdr) {
        Ok(value.clone())
    } else {
        Ok(Value::cons(projected_car, projected_cdr))
    }
}

#[derive(Clone)]
pub(crate) struct RuntimeKeymapBinding {
    pub(crate) key: String,
    pub(crate) parts: Option<Vec<String>>,
    pub(crate) value: Value,
    pub(crate) after_prompt: bool,
}

pub(crate) fn keymap_entry_key_value(parts: &[String], key: &str) -> Value {
    if let [part] = parts {
        let (_, _, saw_prefix) = parse_kbd_prefixes(part);
        if !part.starts_with('<')
            && !part.ends_with('>')
            && !saw_prefix
            && named_kbd_key_code(part).is_none()
            && part.chars().count() > 1
        {
            return Value::Symbol(part.clone().into());
        }

        let mut events = parse_kbd_token(part)
            .into_iter()
            .map(reader_key_event_value)
            .collect::<Vec<_>>();
        if events.len() == 1 {
            return events.remove(0);
        }
    }

    Value::String(key.into())
}

pub(crate) fn set_hash_table_entries(
    interp: &mut Interpreter,
    table: &Value,
    entries: Vec<(Value, Value)>,
) -> Result<(), LispError> {
    let Value::Record(id) = table else {
        return Err(LispError::WrongTypeArgument(
            "hash-table-p".into(),
            table.clone(),
        ));
    };
    if !interp.hash_table_is_mutable(*id) {
        return Err(LispError::Signal("hash table test modifies table".into()));
    }
    let Some(test) = interp
        .find_record(*id)
        .filter(|record| record.kind == crate::lisp::eval::RecordKind::HashTable)
        .and_then(|record| record.slots.first())
        .and_then(|value| value.as_symbol().ok())
        .map(str::to_string)
    else {
        return Err(LispError::WrongTypeArgument(
            "hash-table-p".into(),
            table.clone(),
        ));
    };
    let indexed = matches!(test.as_str(), "eq" | "eql" | "equal") || entries.is_empty();
    let stored_entries = if indexed {
        Value::Nil
    } else {
        hash_table_entries_to_value(entries.clone())
    };
    let Some(record) = interp.find_record_mut(*id) else {
        return Err(LispError::WrongTypeArgument(
            "hash-table-p".into(),
            table.clone(),
        ));
    };
    if record.slots.len() < 2 {
        record.slots.resize(2, Value::Nil);
    }
    record.slots[1] = stored_entries;
    interp.replace_hash_table_runtime_entries(*id, &test, entries);
    Ok(())
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
    if let Value::Vector(vector) = value {
        return vector
            .slots()
            .iter()
            .find_map(|item| display_property_value(item, property));
    }
    if let Ok(items) = value.to_vec() {
        if let Some(Value::Symbol(name)) = items.first()
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

pub(crate) fn find_bidi_override(interp: &Interpreter, start: usize, end: usize) -> Option<usize> {
    let text = interp.buffer.buffer_substring(start, end).ok()?;
    if !text
        .chars()
        .any(|character| matches!(character as u32, 0x202A..=0x202E | 0x2066..=0x2069))
    {
        return None;
    }

    use unicode_bidi::{BidiClass, BidiInfo, LTR_LEVEL};
    let bidi = BidiInfo::new(&text, Some(LTR_LEVEL));
    for (character_index, (byte_index, _)) in text.char_indices().enumerate() {
        let class = bidi.original_classes[byte_index];
        let level = bidi.levels[byte_index].number();
        let suspicious = match class {
            // GNU allows only the paragraph base level for L/EN and
            // the first RTL level for R/AL.
            BidiClass::L | BidiClass::EN => level > 0,
            BidiClass::R | BidiClass::AL => level > 1,
            // Explicit embeddings/isolates may move weak and neutral
            // characters by one level without creating a confusing
            // override; deeper nesting is suspicious.
            BidiClass::AN
            | BidiClass::BN
            | BidiClass::CS
            | BidiClass::ES
            | BidiClass::ET
            | BidiClass::NSM
            | BidiClass::ON => level > 1,
            _ => false,
        };
        if suspicious {
            return Some(start + character_index);
        }
    }
    None
}

pub(crate) fn insert_impl(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
    inherit: bool,
    before_markers: bool,
) -> Result<Value, LispError> {
    let combined = combine_insert_args(args)?;
    let insert_at = interp.buffer.point();
    let nchars = combined.text.chars().count();
    insert_text_with_hooks(
        interp,
        &combined.text,
        &combined.props,
        &combined.extended_chars,
        inherit,
        before_markers,
        env,
    )?;
    if before_markers {
        for overlay in &mut interp.buffer.overlays {
            if overlay.is_dead() {
                continue;
            }
            if overlay.beg == insert_at {
                overlay.beg += nchars;
            }
            if overlay.end == insert_at {
                overlay.end += nchars;
            }
        }
    }
    Ok(Value::Nil)
}

pub(crate) fn insert_char_impl(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    need_args("insert-char", args, 1)?;
    let ch = args[0].as_integer()?;
    let count = match args.get(1) {
        Some(value) if !value.is_nil() => value.as_integer()?.max(0) as usize,
        _ => 1,
    };
    let inherit = args.get(2).is_some_and(Value::is_truthy);
    if (RAW_BYTE8_BASE as i64..=RAW_BYTE8_BASE as i64 + 0xFF).contains(&ch) {
        let byte = (ch - RAW_BYTE8_BASE as i64) as u8;
        let text: String = std::iter::repeat_n(raw_byte_regex_char(byte), count).collect();
        insert_text_with_hooks(interp, &text, &[], &[], inherit, false, env)?;
    } else if let Some(c) = char::from_u32(ch as u32) {
        let text: String = std::iter::repeat_n(c, count).collect();
        insert_text_with_hooks(interp, &text, &[], &[], inherit, false, env)?;
    } else if (0..=0x3F_FFFF).contains(&ch) {
        let text: String = std::iter::repeat_n(RAW_CHAR_SENTINEL, count).collect();
        let extended_chars = (0..count)
            .map(|offset| (offset, ch as u32))
            .collect::<Vec<_>>();
        insert_text_with_hooks(interp, &text, &[], &extended_chars, inherit, false, env)?;
    } else {
        return Err(LispError::Signal(format!("Invalid character: {}", ch)));
    }
    Ok(Value::Nil)
}

pub(crate) fn insert_text_with_hooks(
    interp: &mut Interpreter,
    text: &str,
    props: &[TextPropertySpan],
    extended_chars: &[(usize, u32)],
    inherit: bool,
    before_markers: bool,
    env: &mut crate::lisp::types::Env,
) -> Result<(), LispError> {
    if text.is_empty() {
        return Ok(());
    }
    ensure_insert_modifiable(interp, env)?;
    ensure_no_supersession_threat(interp, env)?;
    let start = interp.buffer.point();
    let overlay_calls = overlay_insert_hook_calls(&interp.buffer, start, text.chars().count());
    run_overlay_hook_calls(interp, &overlay_calls, false, env)?;
    run_change_hooks(
        interp,
        "before-change-functions",
        &[Value::Integer(start as i64), Value::Integer(start as i64)],
        env,
    )?;
    if before_markers {
        if inherit {
            interp.insert_current_buffer_before_markers_and_inherit(text);
        } else {
            interp.insert_current_buffer_before_markers(text);
        }
    } else if inherit {
        interp.insert_current_buffer_and_inherit(text);
    } else {
        interp.insert_current_buffer(text);
    }
    for span in props {
        if inherit {
            // graft_intervals_into_buffer with inherit: the string's own
            // intervals are grafted MERGED with what the insertion point
            // inherited -- the string's keys win, inherited keys the
            // string does not define stay (format-spec relies on a
            // propertized replacement keeping the spec region's face).
            // The string's plist order leads, inherited keys follow.
            let (span_start, span_end) = (start + span.start, start + span.end);
            let mut position = span_start;
            while position < span_end {
                let existing = interp.buffer.text_properties_at(position);
                let mut run_end = position + 1;
                while run_end < span_end && interp.buffer.text_properties_at(run_end) == existing {
                    run_end += 1;
                }
                let mut merged = span.props.clone();
                for (key, value) in existing {
                    if !merged.iter().any(|(present, _)| *present == key) {
                        merged.push((key, value));
                    }
                }
                interp
                    .buffer
                    .set_text_properties(position, run_end, &merged);
                position = run_end;
            }
        } else {
            // Freshly inserted text: graft the string's plist verbatim so
            // the stored order matches GNU (add_text_properties would
            // reverse it).
            interp
                .buffer
                .set_text_properties(start + span.start, start + span.end, &span.props);
        }
    }
    interp.set_inserted_extended_chars(start, extended_chars);
    let end = start + text.chars().count();
    run_change_hooks(
        interp,
        "after-change-functions",
        &[
            Value::Integer(start as i64),
            Value::Integer(end as i64),
            Value::Integer(0),
        ],
        env,
    )?;
    let _ = maybe_lock_current_buffer_on_change(interp, env);
    run_overlay_hook_calls(interp, &overlay_calls, true, env)?;
    Ok(())
}

pub(crate) fn combine_insert_args(args: &[Value]) -> Result<StringLike, LispError> {
    let mut text = String::new();
    let mut props = Vec::new();
    let mut extended_chars = Vec::new();
    for arg in args {
        if let Some(string) = string_like(arg) {
            let offset = text.chars().count();
            text.push_str(&string.text);
            props.extend(shift_string_props(&string.props, offset));
            extended_chars.extend(
                string
                    .extended_chars
                    .into_iter()
                    .map(|(position, code)| (offset + position, code)),
            );
        } else {
            let fragment = match arg {
                Value::Integer(n) => {
                    let offset = text.chars().count();
                    if (RAW_BYTE8_BASE as i64..=RAW_BYTE8_BASE as i64 + 0xFF).contains(n) {
                        raw_byte_regex_char((*n - RAW_BYTE8_BASE as i64) as u8).to_string()
                    } else if let Some(c) = char::from_u32(*n as u32) {
                        c.to_string()
                    } else if (0..=0x3F_FFFF).contains(n) {
                        extended_chars.push((offset, *n as u32));
                        RAW_CHAR_SENTINEL.to_string()
                    } else {
                        String::new()
                    }
                }
                Value::Nil => String::new(),
                _ => arg.to_string(),
            };
            text.push_str(&fragment);
        }
    }
    Ok(StringLike {
        multibyte: text.chars().any(|ch| (ch as u32) > 0x7F),
        text,
        props: merge_string_props(props),
        extended_chars,
    })
}
