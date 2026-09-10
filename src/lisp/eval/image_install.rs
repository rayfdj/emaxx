//! pdumper.c:pdumper_load's point of no return, on the interpreter side
//! (D12): the objects the loader rebuilt are installed as this process's
//! state -- the symbols in the initial obarray's order, the cells of
//! `nil' and `t', the staticpro'd root slots -- and the load is recorded
//! for `pdumper-stats'.  The heap objects, records, buffers, markers,
//! finalizers and root groups were installed by the loader as it read
//! them; this is the symbol table and the static roots.

use super::*;
use crate::lisp::primitives::pdumper::image::RootSlot;
use crate::lisp::primitives::pdumper::load::{LoadedImage, LoadedSymbol};

/// What pdumper.c keeps of a load: `dump_private.dump_filename' and
/// `load_time', for `pdumper-stats'.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PdumperLoadRecord {
    pub(crate) filename: String,
    pub(crate) load_time: std::time::Duration,
    pub(crate) dump_size: u64,
}

impl Interpreter {
    /// dump_loaded_p.
    pub(crate) fn dump_loaded_p(&self) -> bool {
        self.pdumper_loaded.is_some()
    }

    pub(crate) fn pdumper_load_record(&self) -> Option<&PdumperLoadRecord> {
        self.pdumper_loaded.as_ref()
    }

    /// Install the image's symbols and static roots, then record the load.
    pub(crate) fn install_image(
        &mut self,
        image: &LoadedImage,
        record: PdumperLoadRecord,
    ) -> Result<(), String> {
        let mut by_name: HashMap<&str, &LoadedSymbol> = HashMap::new();
        for symbol in &image.symbols {
            by_name.insert(symbol.symbol.as_str(), symbol);
        }
        // The initial obarray's symbols, in its order: every table that
        // `known_symbol_names' enumerates is filled in that order, so
        // `mapatoms' walks the symbols as the writer's process did.
        let mut installed: HashSet<&str> = HashSet::new();
        for name in &image.obarray {
            let text = name.as_str();
            if text == "nil" || text == "t" {
                continue;
            }
            self.intern_symbol_name(text);
            if let Some(symbol) = by_name.get(text) {
                self.install_symbol(symbol);
                installed.insert(text);
            }
        }
        // Symbols the obarray does not list: one `unintern' removed keeps
        // its name out of the obarray; a made-uninterned one has its own
        // identity; one interned in another obarray keeps its own cells
        // under its internal name (the obarray record lists the object);
        // any other interned symbol belongs in the obarray, as every
        // interned symbol does in GNU (the writer's obarray list can miss
        // a symbol whose only mention is an autoload not yet read into
        // its function cell).
        for symbol in &image.symbols {
            use crate::lisp::primitives::pdumper::context::FLAG_UNINTERNED_FROM_OBARRAY;
            let text = symbol.symbol.as_str();
            if installed.contains(text) || text == "nil" || text == "t" {
                continue;
            }
            if symbol.flags & FLAG_UNINTERNED_FROM_OBARRAY != 0 {
                self.uninterned_standard_symbol_names
                    .insert(text.to_owned());
            } else if symbol.symbol.id() & crate::lisp::types::UNINTERNED_SYMBOL_ID_BIT == 0
                && !crate::lisp::types::is_private_obarray_symbol(text)
            {
                self.intern_symbol_name(text);
            }
            self.install_symbol(symbol);
        }
        for cells in &image.builtin_cells {
            let name = cells.symbol.as_str();
            self.install_function_cell(name, &cells.function);
            self.install_plist(name, &cells.plist);
            self.install_watchers(name, &cells.watchers);
        }
        for (slot, value) in &image.roots {
            self.install_root_slot(*slot, value);
        }
        self.pdumper_loaded = Some(record);
        Ok(())
    }

    /// editfns.c:init_editfns: the user names of this process, computed
    /// once and kept in their variables (Vuser_real_login_name from the
    /// real uid's account, Vuser_login_name from LOGNAME or USER, else
    /// the effective uid's account, Vuser_full_name from NAME, else the
    /// account the login name claims, with "unknown" where GNU answers
    /// it).  emacs.c:main runs it in every process, dumped or not; a
    /// name looked up again on every reference answered differently when
    /// one account lookup failed under load (the 2026-09-10 gate).
    pub(crate) fn init_editfns(&mut self) {
        for (name, value) in editfns_identity() {
            self.set_global_binding(name, value);
        }
    }

    /// emacs.c:main after load_pdump: the process state the image does
    /// not carry is set from the new process.  `Interpreter::new' takes
    /// these from the process once as GNU's `init_*' functions do; after
    /// a load the image's dump-time values have replaced them, and this
    /// is the second application GNU makes in an initialized process.
    pub(crate) fn init_after_pdump_load(&mut self) -> Result<(), LispError> {
        // callproc.c:set_initial_environment fills both lists from
        // environ (Fdump_emacs_portable dumped `process-environment' as
        // nil for exactly this).
        let environment = || {
            Value::list(
                initial_process_environment()
                    .iter()
                    .map(|(name, value)| Value::String(format!("{name}={value}").into()))
                    .collect::<Vec<_>>(),
            )
        };
        self.set_global_binding("initial-environment", environment());
        self.set_global_binding("process-environment", environment());
        // editfns.c:init_editfns, which emacs.c:main runs in every process
        // after load_pdump: the user names are this process's.
        self.init_editfns();
        // buffer.c:init_buffer selects *scratch* and initializes its and
        // the first minibuffer's directory from this process's cwd. Other
        // saved buffers retain their own directories.
        let scratch = self
            .find_buffer("*scratch*")
            .map(|(id, _)| id)
            .unwrap_or_else(|| self.create_buffer("*scratch*").0);
        self.set_current_buffer_id(scratch)?;
        let mut directory =
            primitives::bytes_to_shared_unibyte_value(primitives::default_directory().as_bytes());
        let handler = primitives::call(
            self,
            "find-file-name-handler",
            &[directory.clone(), Value::T],
            &mut Vec::new(),
        )?;
        if handler.is_truthy() && primitives::string_text(&directory)? != "/" {
            directory = primitives::call(
                self,
                "concat",
                &[Value::string("/:"), directory],
                &mut Vec::new(),
            )?;
        }
        self.set_buffer_local_value(scratch, "default-directory", directory.clone());
        let minibuffer = self
            .find_buffer(" *Minibuf-0*")
            .map(|(id, _)| id)
            .unwrap_or_else(|| self.create_buffer(" *Minibuf-0*").0);
        self.set_buffer_local_value(minibuffer, "default-directory", directory);
        // emacs.c:init_cmdargs.
        self.set_global_binding("command-line-args", primitives::command_line_args_value());
        for name in [
            "invocation-name",
            "invocation-directory",
            "installation-directory",
        ] {
            if let Some(value) = self.builtin_var_value(name) {
                self.set_global_binding(name, value);
            }
        }
        // callproc.c:init_callproc.
        self.set_global_binding("exec-path", current_exec_path());
        for name in ["shell-file-name", "data-directory", "doc-directory"] {
            if let Some(value) = self.builtin_var_value(name) {
                self.set_global_binding(name, value);
            }
        }
        self.set_global_binding(
            "exec-directory",
            Value::string(
                &primitives::current_invocation_directory()
                    .unwrap_or_else(primitives::default_directory),
            ),
        );
        // timefns.c:init_timefns takes the new process's TZ.
        self.local_time_zone_rule = std::env::var("TZ")
            .map(|value| Value::String(value.into()))
            .unwrap_or_else(|_| Value::Symbol("wall".into()));
        // emacs.c: "Erase any pre-dump messages in the message log, to
        // avoid confusion" (message_dolog with message-log-max 0).
        if let Some(buffer_id) = self.find_buffer("*Messages*").map(|(id, _)| id)
            && let Some(buffer) = self.get_buffer_by_id_mut(buffer_id)
        {
            let end = buffer.point_max();
            let _ = buffer.delete_region(1, end);
        }
        Ok(())
    }

    /// One symbol record: the value cell with its redirect and flags, the
    /// function cell, the plist, the watchers.
    fn install_symbol(&mut self, symbol: &LoadedSymbol) {
        use crate::lisp::primitives::pdumper::context::{
            FLAG_ALWAYS_LOCAL, FLAG_DECLARED_SPECIAL, FLAG_FWD_BOOL, FLAG_FWD_INT,
            FLAG_LOCAL_IF_SET, FLAG_LOCALIZED_BESIDE_FORWARDED, FLAG_PER_BUFFER, SYMBOL_FORWARDED,
            SYMBOL_LOCALIZED, SYMBOL_REDIRECT_MASK,
        };
        let name = symbol.symbol.as_str().to_owned();
        let redirect = symbol.flags & SYMBOL_REDIRECT_MASK;
        let mut flags = 0_u8;
        if redirect == SYMBOL_LOCALIZED || symbol.flags & FLAG_LOCALIZED_BESIDE_FORWARDED != 0 {
            flags |= symbol_cell_flags::LOCALIZED;
        }
        if redirect == SYMBOL_FORWARDED {
            flags |= symbol_cell_flags::FORWARDED;
        }
        for (bit, flag) in [
            (FLAG_DECLARED_SPECIAL, symbol_cell_flags::SPECIAL),
            (FLAG_LOCAL_IF_SET, symbol_cell_flags::LOCAL_IF_SET),
            (FLAG_PER_BUFFER, symbol_cell_flags::PER_BUFFER),
            (FLAG_ALWAYS_LOCAL, symbol_cell_flags::ALWAYS_LOCAL),
            (FLAG_FWD_BOOL, symbol_cell_flags::FWD_BOOL),
            (FLAG_FWD_INT, symbol_cell_flags::FWD_INT),
        ] {
            if symbol.flags & bit != 0 {
                flags |= flag;
            }
        }
        self.globals.install_cell(
            &symbol.symbol,
            SymbolCellSnapshot {
                value: symbol.value.clone(),
                alias: symbol.alias.clone(),
                flags,
            },
        );
        if let Some(target) = &symbol.alias {
            let target = target.as_str().to_owned();
            if let Some(index) = self
                .variable_aliases
                .iter()
                .rposition(|(existing, _)| *existing == name)
            {
                self.variable_aliases[index].1 = target;
            } else {
                self.variable_aliases.push((name.clone(), target));
            }
        }
        if let Some(value) = &symbol.value {
            self.note_installed_value(&name, value);
        }
        self.install_function_cell(&name, &symbol.function);
        self.install_plist(&name, &symbol.plist);
        self.install_watchers(&name, &symbol.watchers);
    }

    /// The derived state a value carries beside its cell: the forwarded C
    /// slots and the feature list (`set_global_binding' keeps them too).
    fn note_installed_value(&mut self, name: &str, value: &Value) {
        if name == "features" {
            self.provided_features = value
                .to_vec()
                .unwrap_or_default()
                .into_iter()
                .filter_map(|feature| feature.as_symbol().ok().map(str::to_string))
                .collect();
        }
        self.update_forwarded_eval_cell(name, value);
    }

    /// The function cell: a built-in's own subr is the dispatch default
    /// and needs no entry; anything else is bound.
    fn install_function_cell(&mut self, name: &str, function: &Value) {
        if function.is_nil() {
            if self.functions_index.contains_key(name) {
                self.set_function_binding(name, None);
            }
            return;
        }
        if let Value::BuiltinFunc(subr) = function
            && subr.as_str() == name
            && !self.functions_index.contains_key(name)
        {
            return;
        }
        self.set_function_binding(name, Some(function.clone()));
    }

    fn install_plist(&mut self, name: &str, plist: &Value) {
        if plist.is_nil() && self.symbol_property_index(name).is_none() {
            return;
        }
        // Replacing the plist keeps the property index coherent.
        let _ = self.set_symbol_plist(name, plist.clone());
    }

    fn install_watchers(&mut self, name: &str, watchers: &[Value]) {
        if let Some(index) = self.variable_watcher_index(name) {
            if watchers.is_empty() {
                self.variable_watchers.remove(index);
            } else {
                self.variable_watchers[index].1 = watchers.to_vec();
            }
        } else if !watchers.is_empty() {
            self.variable_watchers
                .push((name.to_owned(), watchers.to_vec()));
        }
    }

    /// The inverse of `dump_root_values' for the single slots; the
    /// obarray, the built-in cells, the finalizer heads and the groups
    /// are installed elsewhere.
    fn install_root_slot(&mut self, slot: RootSlot, value: &Value) {
        match slot {
            RootSlot::QuitFlag => self.quit_flag = value.clone(),
            RootSlot::InhibitQuit => self.inhibit_quit = value.clone(),
            RootSlot::ThrowOnInput => self.throw_on_input = value.clone(),
            RootSlot::OverridingPlistEnvironment => {
                self.overriding_plist_environment = value.clone();
            }
            RootSlot::LoadPath => self.load_path = value.clone(),
            RootSlot::LoadsInProgress => self.loads_in_progress = value.clone(),
            RootSlot::LocalTimeZoneRule => self.local_time_zone_rule = value.clone(),
            RootSlot::FrameAndBufferState => self.frame_and_buffer_state = value.clone(),
            RootSlot::CurrentGlobalMap => {
                self.current_global_map = (!matches!(value, Value::Unbound)).then(|| value.clone());
            }
            _ => {}
        }
    }
}

/// init_editfns's three values, in the order it computes them.
pub(crate) fn editfns_identity() -> [(&'static str, Value); 3] {
    let real_login = primitives::current_real_user_login_name().unwrap_or_else(|| "unknown".into());
    let login = primitives::current_user_login_name().unwrap_or_else(|| "unknown".into());
    // If the user name claimed in the environment vars differs from the
    // real uid, use the claimed name to find the full name.
    let full_name = std::env::var("NAME")
        .ok()
        .or_else(|| {
            if login == real_login {
                primitives::user_full_name_from_login(&login)
            } else {
                primitives::current_user_id()
                    .ok()
                    .and_then(primitives::user_full_name_from_uid)
            }
        })
        .unwrap_or_else(|| "unknown".into());
    [
        ("user-real-login-name", Value::String(real_login.into())),
        ("user-login-name", Value::String(login.into())),
        ("user-full-name", Value::String(full_name.into())),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restored_process_replaces_build_paths_and_preserves_other_buffers() {
        // emacs.c:init_cmdargs, callproc.c:init_callproc and
        // buffer.c:init_buffer recreate these values after loading an image.
        let mut interpreter = Interpreter::new();
        let names = [
            "invocation-name",
            "invocation-directory",
            "installation-directory",
            "shell-file-name",
            "data-directory",
            "doc-directory",
            "exec-directory",
        ];
        let expected = names.map(|name| {
            interpreter
                .symbol_value_cell(name)
                .expect("the process initializer installed the variable")
        });
        for name in names {
            interpreter.set_global_binding(name, Value::string("/dump-builder/"));
        }
        let other = interpreter.create_buffer("saved-other-buffer").0;
        interpreter.set_buffer_local_value(other, "default-directory", Value::string("/saved/"));
        interpreter
            .set_current_buffer_id(other)
            .expect("select saved buffer");
        interpreter
            .init_after_pdump_load()
            .expect("initialize the restored process");
        for (name, expected) in names.into_iter().zip(expected) {
            assert_eq!(
                interpreter
                    .symbol_value_cell(name)
                    .expect("restored variable"),
                expected,
                "{name} retained the builder's value"
            );
        }
        assert_eq!(interpreter.buffer.name, "*scratch*");
        assert_eq!(
            interpreter.lookup_var("default-directory", &Vec::new()),
            Some(Value::string(&primitives::default_directory()))
        );
        interpreter
            .set_current_buffer_id(other)
            .expect("select the retained buffer");
        assert_eq!(
            interpreter.lookup_var("default-directory", &Vec::new()),
            Some(Value::string("/saved/"))
        );
    }
}
