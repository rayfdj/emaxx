use super::*;
use std::path::{Path, PathBuf};

fn treesit_signal(kind: &str, data: impl IntoIterator<Item = Value>) -> LispError {
    LispError::SignalValue(Value::list([Value::symbol(kind)].into_iter().chain(data)))
}

fn load_error(kind: &str, data: impl IntoIterator<Item = Value>) -> LispError {
    treesit_signal(
        "treesit-load-language-error",
        [Value::symbol(kind)].into_iter().chain(data),
    )
}

fn value_string(value: &Value) -> Result<String, LispError> {
    primitives::string_like(value)
        .map(|string| string.text)
        .ok_or_else(|| LispError::TypeError("stringp".into(), value.type_name()))
}

fn library_suffixes(interp: &Interpreter) -> Result<Vec<String>, LispError> {
    interp
        .lookup_var("dynamic-library-suffixes", &Env::new())
        .unwrap_or(Value::Nil)
        .to_vec()?
        .iter()
        .map(value_string)
        .collect()
}

fn language_override(
    interp: &Interpreter,
    language: &str,
) -> Result<Option<(String, String)>, LispError> {
    for entry in interp
        .lookup_var("treesit-load-name-override-list", &Env::new())
        .unwrap_or(Value::Nil)
        .to_vec()?
    {
        let fields = entry.to_vec()?;
        let Some(entry_language) = fields.first() else {
            continue;
        };
        entry_language.as_symbol()?;
        if entry_language.as_symbol()? == language {
            let library = fields
                .get(1)
                .ok_or_else(|| LispError::TypeError("stringp".into(), "nil".into()))?;
            let function = fields
                .get(2)
                .ok_or_else(|| LispError::TypeError("stringp".into(), "nil".into()))?;
            return Ok(Some((value_string(library)?, value_string(function)?)));
        }
    }
    Ok(None)
}

fn library_names(base: &Path, suffixes: &[String]) -> Vec<PathBuf> {
    let mut names = Vec::new();
    for suffix in suffixes {
        let plain = PathBuf::from(format!("{}{suffix}", base.display()));
        names.push(plain.clone());
        #[cfg(not(windows))]
        {
            for abi in
                (tree_sitter::MIN_COMPATIBLE_LANGUAGE_VERSION..=tree_sitter::LANGUAGE_VERSION).rev()
            {
                names.push(PathBuf::from(format!("{}.{}.0", plain.display(), abi)));
            }
            names.push(PathBuf::from(format!("{}.0", plain.display())));
            names.push(PathBuf::from(format!("{}.0.0", plain.display())));
        }
    }
    names
}

fn language_candidates(
    interp: &Interpreter,
    library_base: &str,
) -> Result<Vec<PathBuf>, LispError> {
    let suffixes = library_suffixes(interp)?;
    let mut candidates = Vec::new();

    for directory in interp
        .lookup_var("treesit-extra-load-path", &Env::new())
        .unwrap_or(Value::Nil)
        .to_vec()?
    {
        candidates.extend(library_names(
            &PathBuf::from(value_string(&directory)?).join(library_base),
            &suffixes,
        ));
    }

    if let Some(user_directory) = interp.lookup_var("user-emacs-directory", &Env::new()) {
        candidates.extend(library_names(
            &PathBuf::from(value_string(&user_directory)?)
                .join("tree-sitter")
                .join(library_base),
            &suffixes,
        ));
    }
    candidates.extend(library_names(Path::new(library_base), &suffixes));
    Ok(candidates)
}

fn point_at(text: &str, byte: usize) -> tree_sitter::Point {
    let prefix = &text[..byte];
    let row = prefix.bytes().filter(|byte| *byte == b'\n').count();
    let column = prefix
        .rfind('\n')
        .map_or(prefix.len(), |newline| prefix.len() - newline - 1);
    tree_sitter::Point::new(row, column)
}

fn included_ranges(
    buffer: &crate::buffer::Buffer,
    ranges: &Value,
) -> Result<Vec<tree_sitter::Range>, LispError> {
    if ranges.is_nil() {
        return Ok(Vec::new());
    }
    let visible_text = buffer.buffer_string();
    let visible_start = buffer
        .position_bytes(buffer.point_min())
        .expect("point-min is a valid buffer position")
        - 1;
    let mut previous_end = buffer.point_min();
    let mut result = Vec::new();
    for range in ranges.to_vec()? {
        let start = range.car()?.as_integer()?;
        let end = range.cdr()?.as_integer()?;
        if start < 0 || end < 0 {
            return Err(treesit_signal(
                "treesit-range-invalid",
                [Value::String(
                    "RANGE is either overlapping, out-of-order or out-of-range".into(),
                )],
            ));
        }
        let (start, end) = (start as usize, end as usize);
        if previous_end > start || start > end || end > buffer.point_max() {
            return Err(treesit_signal(
                "treesit-range-invalid",
                [Value::String(
                    "RANGE is either overlapping, out-of-order or out-of-range".into(),
                )],
            ));
        }
        let start_byte = buffer
            .position_bytes(start)
            .expect("validated included-range start")
            - 1
            - visible_start;
        let end_byte = buffer
            .position_bytes(end)
            .expect("validated included-range end")
            - 1
            - visible_start;
        result.push(tree_sitter::Range {
            start_byte,
            end_byte,
            start_point: point_at(&visible_text, start_byte),
            end_point: point_at(&visible_text, end_byte),
        });
        previous_end = end;
    }
    Ok(result)
}

fn find_node<'tree>(
    root: tree_sitter::Node<'tree>,
    wanted: usize,
) -> Option<tree_sitter::Node<'tree>> {
    let mut pending = vec![root];
    while let Some(node) = pending.pop() {
        if node.id() == wanted {
            return Some(node);
        }
        pending.extend((0..node.child_count()).rev().filter_map(|index| {
            u32::try_from(index)
                .ok()
                .and_then(|index| node.child(index))
        }));
    }
    None
}

impl Interpreter {
    pub(crate) fn require_treesit_language(
        &mut self,
        language: &str,
    ) -> Result<tree_sitter::Language, LispError> {
        if let Some(state) = self
            .treesit_languages
            .iter()
            .find(|state| state.symbol == language)
        {
            return Ok(state.language.clone());
        }

        let default_library = format!("libtree-sitter-{language}");
        let default_function = format!("tree_sitter_{}", language.replace('-', "_"));
        let (library_base, function) =
            language_override(self, language)?.unwrap_or((default_library, default_function));
        let mut errors = Vec::new();

        for candidate in language_candidates(self, &library_base)? {
            // SAFETY: Grammar modules expose a no-argument constructor with the
            // Tree-sitter C ABI.  `Language` is repr(transparent) over that
            // pointer, and the owning Library remains in interpreter state
            // until after every cloned Language and parser has been dropped.
            let library = match unsafe { libloading::Library::new(&candidate) } {
                Ok(library) => library,
                Err(error) => {
                    errors.push(Value::String(error.to_string().into()));
                    continue;
                }
            };
            let language_value = unsafe {
                let constructor = library
                    .get::<unsafe extern "C" fn() -> tree_sitter::Language>(function.as_bytes())
                    .map_err(|error| {
                        load_error("symbol-error", [Value::String(error.to_string().into())])
                    })?;
                constructor()
            };
            let mut parser = tree_sitter::Parser::new();
            if parser.set_language(&language_value).is_err() {
                return Err(load_error(
                    "version-mismatch",
                    [Value::Integer(language_value.abi_version() as i64)],
                ));
            }
            self.treesit_languages.push(TreeSitterLanguageState {
                symbol: language.to_string(),
                language: language_value.clone(),
                _library: Some(library),
            });
            return Ok(language_value);
        }
        Err(load_error("not-found", errors))
    }

    #[cfg(test)]
    pub(crate) fn register_treesit_language_for_test(
        &mut self,
        symbol: &str,
        language: tree_sitter::Language,
    ) {
        self.treesit_languages.push(TreeSitterLanguageState {
            symbol: symbol.to_string(),
            language,
            _library: None,
        });
    }

    pub(crate) fn treesit_parser_state(&self, value: &Value) -> Option<&TreeSitterParserState> {
        let Value::Record(record_id) = value else {
            return None;
        };
        self.treesit_parsers
            .iter()
            .find(|parser| parser.record_id == *record_id)
    }

    fn treesit_parser_index(&self, value: &Value) -> Result<usize, LispError> {
        let Value::Record(record_id) = value else {
            return Err(LispError::TypeError(
                "treesit-parser-p".into(),
                value.type_name(),
            ));
        };
        let index = self
            .treesit_parsers
            .iter()
            .position(|parser| parser.record_id == *record_id)
            .ok_or_else(|| LispError::TypeError("treesit-parser-p".into(), value.type_name()))?;
        if self.treesit_parsers[index].deleted {
            return Err(treesit_signal("treesit-parser-deleted", [value.clone()]));
        }
        Ok(index)
    }

    pub(crate) fn reusable_treesit_parser(
        &self,
        language: &Value,
        list_buffer_id: u64,
        tag: &Value,
    ) -> Option<Value> {
        self.treesit_parsers
            .iter()
            .rev()
            .find(|parser| {
                !parser.deleted
                    && parser.list_buffer_id == list_buffer_id
                    && parser.language == *language
                    && parser.tag == *tag
            })
            .map(|parser| Value::Record(parser.record_id))
    }

    pub(crate) fn create_treesit_parser(
        &mut self,
        language: Value,
        buffer_id: u64,
        list_buffer_id: u64,
        tag: Value,
    ) -> Result<Value, LispError> {
        let buffer = self
            .buffer_identity_value(buffer_id)
            .expect("parser creation resolved a live buffer");
        let language_name = language.as_symbol()?.to_string();
        let grammar = self.require_treesit_language(&language_name)?;
        let mut parser = tree_sitter::Parser::new();
        parser.set_language(&grammar).map_err(|_| {
            load_error(
                "version-mismatch",
                [Value::Integer(grammar.abi_version() as i64)],
            )
        })?;
        let value =
            self.create_pseudovector(RecordKind::TreeSitterParser, "treesit-parser", Vec::new());
        let Value::Record(record_id) = value else {
            unreachable!("Tree-sitter parsers use opaque record identities");
        };
        self.treesit_parsers.push(TreeSitterParserState {
            record_id,
            parser,
            tree: None,
            language,
            buffer_id,
            buffer,
            list_buffer_id,
            tag,
            deleted: false,
            included_ranges: Value::Nil,
            notifiers: Vec::new(),
            parsed_tick: None,
            visible_region: None,
            generation: 0,
        });
        Ok(Value::Record(record_id))
    }

    pub(crate) fn delete_treesit_parser(&mut self, value: &Value) -> Result<(), LispError> {
        let index = self.treesit_parser_index(value)?;
        self.treesit_parsers[index].deleted = true;
        Ok(())
    }

    pub(crate) fn treesit_parser_details(
        &self,
        value: &Value,
    ) -> Result<(Value, Value, Value, Value, Vec<Value>), LispError> {
        let index = self.treesit_parser_index(value)?;
        let parser = &self.treesit_parsers[index];
        Ok((
            parser.buffer.clone(),
            parser.language.clone(),
            parser.tag.clone(),
            parser.included_ranges.clone(),
            parser.notifiers.clone(),
        ))
    }

    pub(crate) fn add_treesit_notifier(
        &mut self,
        parser: &Value,
        function: Value,
    ) -> Result<(), LispError> {
        let index = self.treesit_parser_index(parser)?;
        if !self.treesit_parsers[index].notifiers.contains(&function) {
            self.treesit_parsers[index].notifiers.push(function);
        }
        Ok(())
    }

    pub(crate) fn remove_treesit_notifier(
        &mut self,
        parser: &Value,
        function: &Value,
    ) -> Result<(), LispError> {
        let index = self.treesit_parser_index(parser)?;
        self.treesit_parsers[index]
            .notifiers
            .retain(|notifier| notifier != function);
        Ok(())
    }

    pub(crate) fn treesit_parser_list(
        &self,
        list_buffer_id: u64,
        language: Option<&Value>,
        tag: &Value,
    ) -> Vec<Value> {
        self.treesit_parsers
            .iter()
            .rev()
            .filter(|parser| {
                !parser.deleted
                    && parser.list_buffer_id == list_buffer_id
                    && language.is_none_or(|language| parser.language == *language)
                    && (*tag == Value::T || parser.tag == *tag)
            })
            .map(|parser| Value::Record(parser.record_id))
            .collect()
    }

    pub(crate) fn ensure_treesit_parsed(&mut self, value: &Value) -> Result<(), LispError> {
        let index = self.treesit_parser_index(value)?;
        let buffer_id = self.treesit_parsers[index].buffer_id;
        let buffer = self
            .get_buffer_by_id(buffer_id)
            .ok_or_else(|| treesit_signal("treesit-parser-buffer-killed", [value.clone()]))?;
        let tick = buffer.chars_modified_tick();
        let visible_region = (buffer.point_min(), buffer.point_max());
        if self.treesit_parsers[index].tree.is_some()
            && self.treesit_parsers[index].parsed_tick == Some(tick)
            && self.treesit_parsers[index].visible_region == Some(visible_region)
        {
            return Ok(());
        }
        let text = buffer.buffer_string();
        let ranges = included_ranges(buffer, &self.treesit_parsers[index].included_ranges)?;
        let state = &mut self.treesit_parsers[index];
        state.parser.set_included_ranges(&ranges).map_err(|_| {
            treesit_signal("treesit-range-invalid", [state.included_ranges.clone()])
        })?;
        state.parser.reset();
        state.tree = state.parser.parse(&text, None);
        if state.tree.is_none() {
            return Err(treesit_signal(
                "treesit-error",
                [Value::String("Tree-sitter parser returned no tree".into())],
            ));
        }
        state.parsed_tick = Some(tick);
        state.visible_region = Some(visible_region);
        state.generation = state.generation.saturating_add(1);
        Ok(())
    }

    pub(crate) fn treesit_root_node(&mut self, parser: &Value) -> Result<Value, LispError> {
        self.ensure_treesit_parsed(parser)?;
        let index = self.treesit_parser_index(parser)?;
        let state = &self.treesit_parsers[index];
        let node_id = state
            .tree
            .as_ref()
            .expect("ensure_treesit_parsed installed a tree")
            .root_node()
            .id();
        let (parser_id, generation) = (state.record_id, state.generation);
        Ok(self.create_treesit_node(parser_id, node_id, generation))
    }

    fn create_treesit_node(&mut self, parser_id: u64, node_id: usize, generation: u64) -> Value {
        let node = self.create_pseudovector(RecordKind::TreeSitterNode, "treesit-node", Vec::new());
        let Value::Record(record_id) = node else {
            unreachable!("Tree-sitter nodes use opaque record identities");
        };
        self.treesit_nodes.push(TreeSitterNodeState {
            record_id,
            parser_id,
            node_id,
            generation,
        });
        Value::Record(record_id)
    }

    pub(crate) fn treesit_node_state(&self, value: &Value) -> Option<&TreeSitterNodeState> {
        let Value::Record(record_id) = value else {
            return None;
        };
        self.treesit_nodes
            .iter()
            .find(|node| node.record_id == *record_id)
    }

    pub(crate) fn treesit_node_outdated(&self, value: &Value) -> Result<bool, LispError> {
        let node = self
            .treesit_node_state(value)
            .ok_or_else(|| LispError::TypeError("treesit-node-p".into(), value.type_name()))?;
        let parser = self
            .treesit_parsers
            .iter()
            .find(|parser| parser.record_id == node.parser_id)
            .expect("Tree-sitter node retains its parser state");
        Ok(node.generation != parser.generation)
    }

    pub(crate) fn treesit_node_live(&self, value: &Value) -> Result<bool, LispError> {
        let node = self
            .treesit_node_state(value)
            .ok_or_else(|| LispError::TypeError("treesit-node-p".into(), value.type_name()))?;
        let parser = self
            .treesit_parsers
            .iter()
            .find(|parser| parser.record_id == node.parser_id)
            .expect("Tree-sitter node retains its parser state");
        Ok(!parser.deleted && self.has_buffer_id(parser.buffer_id))
    }

    pub(crate) fn with_treesit_node<R>(
        &self,
        value: &Value,
        f: impl FnOnce(tree_sitter::Node<'_>, &TreeSitterParserState) -> R,
    ) -> Result<R, LispError> {
        let node = self
            .treesit_node_state(value)
            .cloned()
            .ok_or_else(|| LispError::TypeError("treesit-node-p".into(), value.type_name()))?;
        let parser = self
            .treesit_parsers
            .iter()
            .find(|parser| parser.record_id == node.parser_id)
            .expect("Tree-sitter node retains its parser state");
        if parser.generation != node.generation {
            return Err(treesit_signal("treesit-node-outdated", [value.clone()]));
        }
        if !self.has_buffer_id(parser.buffer_id) {
            return Err(treesit_signal(
                "treesit-node-buffer-killed",
                [value.clone()],
            ));
        }
        let tree = parser
            .tree
            .as_ref()
            .expect("a Tree-sitter node always belongs to a parsed tree");
        let resolved = find_node(tree.root_node(), node.node_id)
            .expect("node identity remains present until the next parse");
        Ok(f(resolved, parser))
    }

    pub(crate) fn related_treesit_node(
        &mut self,
        source: &Value,
        node_id: Option<usize>,
    ) -> Result<Value, LispError> {
        let Some(node_id) = node_id else {
            return Ok(Value::Nil);
        };
        let state = self
            .treesit_node_state(source)
            .cloned()
            .ok_or_else(|| LispError::TypeError("treesit-node-p".into(), source.type_name()))?;
        Ok(self.create_treesit_node(state.parser_id, node_id, state.generation))
    }

    pub(crate) fn treesit_node_position(
        &self,
        node: &Value,
        start: bool,
    ) -> Result<usize, LispError> {
        self.with_treesit_node(node, |node, parser| {
            let byte = if start {
                node.start_byte()
            } else {
                node.end_byte()
            };
            let buffer = self
                .get_buffer_by_id(parser.buffer_id)
                .expect("node buffer liveness was checked");
            let visible_start = buffer
                .position_bytes(buffer.point_min())
                .expect("point-min is a valid buffer position");
            buffer
                .byte_to_position(visible_start + byte)
                .expect("Tree-sitter returned an in-buffer byte position")
        })
    }

    pub(crate) fn treesit_node_relative_byte(
        &self,
        node: &Value,
        position: i64,
    ) -> Result<usize, LispError> {
        self.with_treesit_node(node, |_, parser| {
            let buffer = self
                .get_buffer_by_id(parser.buffer_id)
                .expect("node buffer liveness was checked");
            let Ok(position) = usize::try_from(position) else {
                return Err(treesit_signal(
                    "args-out-of-range",
                    [Value::Integer(position)],
                ));
            };
            if !(buffer.point_min()..=buffer.point_max()).contains(&position) {
                return Err(treesit_signal(
                    "args-out-of-range",
                    [Value::Integer(position as i64)],
                ));
            }
            let visible_start = buffer
                .position_bytes(buffer.point_min())
                .expect("point-min is a valid buffer position");
            Ok(buffer
                .position_bytes(position)
                .expect("validated buffer position")
                - visible_start)
        })?
    }

    pub(crate) fn set_treesit_included_ranges(
        &mut self,
        parser: &Value,
        ranges: Value,
    ) -> Result<(), LispError> {
        let index = self.treesit_parser_index(parser)?;
        let buffer_id = self.treesit_parsers[index].buffer_id;
        let buffer = self
            .get_buffer_by_id(buffer_id)
            .ok_or_else(|| treesit_signal("treesit-parser-buffer-killed", [parser.clone()]))?;
        included_ranges(buffer, &ranges)?;
        self.treesit_parsers[index].included_ranges = ranges;
        self.treesit_parsers[index].parsed_tick = None;
        Ok(())
    }
}
