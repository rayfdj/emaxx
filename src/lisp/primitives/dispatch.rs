use super::*;

mod buffer_edit;
mod buffer_meta;
mod collections;
mod comp;
mod composition;
mod display;
mod emacs_module;
mod faces;
mod files_process;
mod fonts;
mod frames;
mod gnutls;
mod gui_actions;
mod lists;
pub(super) mod misc;
mod misc_keymaps;
mod numeric;

pub(crate) use lists::{
    prepare_kbd_macro_minibuffer_entry, read_minibuffer_text_from_kbd_macro_inner,
};
pub(crate) use misc_keymaps::oclosure_type_of;
mod overlays;
mod predicates;
mod search_coding;
mod strings;
mod terminals;
mod treesit;

/// Memoized per-name facts.  Every predicate cached here is a pure
/// function of the name (giant static `matches!` lists), but they are
/// consulted on every form evaluation — the linear string matching was
/// a top profile entry under erc's message-processing load.
#[derive(Clone, Copy)]
pub(crate) struct NameFacts {
    pub(crate) builtin: bool,
    pub(crate) special_form: bool,
    pub(crate) prefer_override: bool,
    resets_undo: bool,
    file_name_handler: Option<FileNameHandlerOperation>,
    /// Whether `builtin_autoload_function' has an entry for this name, so
    /// function lookup only walks its match tables when one exists.
    pub(crate) autoloadable: bool,
    module: DispatchModule,
}

#[derive(Clone, Copy, PartialEq)]
enum DispatchModule {
    Sqlite,
    Time,
    Lcms,
    Ccl,
    Numeric,
    Fonts,
    Frames,
    Terminals,
    Treesit,
    Gnutls,
    GuiActions,
    Comp,
    Predicates,
    Lists,
    Modes,
    Composition,
    Strings,
    BufferEdit,
    BufferMeta,
    FilesProcess,
    Display,
    EmacsModule,
    Faces,
    Misc,
    MiscKeymaps,
    Overlays,
    Collections,
    SearchCoding,
    ComposedAccessor,
    None,
}

fn compute_name_facts(name: &str) -> NameFacts {
    // Probe order mirrors `call' so the cached route dispatches to the
    // same module the sequential scan would have reached.
    let module = if sqlite::handles(name) {
        DispatchModule::Sqlite
    } else if numeric_time::handles(name) {
        DispatchModule::Time
    } else if color_lcms::handles(name) {
        DispatchModule::Lcms
    } else if ccl::handles(name) {
        DispatchModule::Ccl
    } else if numeric::handles(name) {
        DispatchModule::Numeric
    } else if fonts::handles(name) {
        DispatchModule::Fonts
    } else if frames::handles(name) {
        DispatchModule::Frames
    } else if terminals::handles(name) {
        DispatchModule::Terminals
    } else if treesit::handles(name) {
        DispatchModule::Treesit
    } else if gnutls::handles(name) {
        DispatchModule::Gnutls
    } else if gui_actions::handles(name) {
        DispatchModule::GuiActions
    } else if comp::handles(name) {
        DispatchModule::Comp
    } else if predicates::handles(name) {
        DispatchModule::Predicates
    } else if lists::handles(name) {
        DispatchModule::Lists
    } else if modes::handles(name) {
        DispatchModule::Modes
    } else if composition::handles(name) {
        DispatchModule::Composition
    } else if strings::handles(name) {
        DispatchModule::Strings
    } else if buffer_edit::handles(name) {
        DispatchModule::BufferEdit
    } else if buffer_meta::handles(name) {
        DispatchModule::BufferMeta
    } else if files_process::handles(name) {
        DispatchModule::FilesProcess
    } else if display::handles(name) {
        DispatchModule::Display
    } else if emacs_module::handles(name) {
        DispatchModule::EmacsModule
    } else if faces::handles(name) {
        DispatchModule::Faces
    } else if misc::handles(name) {
        DispatchModule::Misc
    } else if misc_keymaps::handles(name) {
        DispatchModule::MiscKeymaps
    } else if overlays::handles(name) {
        DispatchModule::Overlays
    } else if collections::handles(name) {
        DispatchModule::Collections
    } else if search_coding::handles(name) {
        DispatchModule::SearchCoding
    } else if is_composed_accessor_name(name) {
        DispatchModule::ComposedAccessor
    } else {
        DispatchModule::None
    };
    NameFacts {
        // A callable native route is the builtin contract.  Keeping a
        // second list of the same names made every new primitive require
        // two coordinated edits and allowed function lookup to drift from
        // dispatch.
        builtin: module != DispatchModule::None,
        special_form: crate::lisp::primitives::is_special_form_name(name),
        prefer_override: crate::lisp::primitives::prefer_builtin_override(name),
        resets_undo: resets_undo_sequence(name),
        file_name_handler: file_name_handler_operation(name),
        autoloadable: crate::lisp::eval::builtin_autoload_function(name).is_some(),
        module,
    }
}

/// FNV-1a, keyed by short primitive names: far cheaper than SipHash for
/// the per-call cache lookups below, and DoS resistance is irrelevant for
/// a cache of function-name metadata.
#[derive(Default)]
pub(crate) struct FnvHasher(u64);

impl std::hash::Hasher for FnvHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        let mut hash = if self.0 == 0 {
            0xcbf2_9ce4_8422_2325
        } else {
            self.0
        };
        for byte in bytes {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        self.0 = hash;
    }
}

pub(crate) type FnvBuildHasher = std::hash::BuildHasherDefault<FnvHasher>;

pub(crate) fn name_facts(name: &str) -> NameFacts {
    thread_local! {
        static NAME_FACTS: std::cell::RefCell<
            std::collections::HashMap<String, NameFacts, FnvBuildHasher>,
        > = std::cell::RefCell::new(std::collections::HashMap::default());
    }
    NAME_FACTS.with(|cache| {
        if let Some(facts) = cache.borrow().get(name) {
            return *facts;
        }
        let facts = compute_name_facts(name);
        cache.borrow_mut().insert(name.to_string(), facts);
        facts
    })
}

pub fn is_builtin(name: &str) -> bool {
    name_facts(name).builtin
}

#[cfg(test)]
pub(crate) fn has_dispatch_handler(name: &str) -> bool {
    name_facts(name).module != DispatchModule::None
}

fn resets_undo_sequence(name: &str) -> bool {
    matches!(
        name,
        "undo-boundary"
            | "insert"
            | "insert-char"
            | "insert-before-markers"
            | "insert-before-markers-and-inherit"
            | "delete-region"
            | "delete-and-extract-region"
            | "kill-region"
            | "delete-line"
            | "kill-whole-line"
            | "delete-horizontal-space"
            | "delete-char"
            | "replace-buffer-contents"
            | "delete-forward-char"
            | "kill-word"
            | "erase-buffer"
            | "put-text-property"
            | "add-text-properties"
            | "set-text-properties"
            | "remove-list-of-text-properties"
            | "remove-text-properties"
            | "add-face-text-property"
            | "font-lock-append-text-property"
            | "font-lock-prepend-text-property"
            | "font-lock--remove-face-from-text-property"
            | "set-buffer-multibyte"
            | "write-region"
            | "save-buffer"
    )
}

/// Dispatch a builtin function call.
pub fn call(
    interp: &mut Interpreter,
    name: &str,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    call_with_facts(interp, name, name_facts(name), args, env)
}

/// `call' for callers that already fetched the name's facts this call.
pub(crate) fn call_with_facts(
    interp: &mut Interpreter,
    name: &str,
    facts: NameFacts,
    args: &[Value],
    env: &mut crate::lisp::types::Env,
) -> Result<Value, LispError> {
    if let Some(specification) = facts.file_name_handler
        && let Some(result) = dispatch_file_name_handler(interp, env, name, specification, args)?
    {
        return Ok(result);
    }

    if facts.resets_undo {
        interp.reset_undo_sequence();
    }

    match facts.module {
        DispatchModule::Sqlite => sqlite::call(interp, name, args, env),
        DispatchModule::Time => call_time_builtin(interp, name, args, env),
        DispatchModule::Lcms => call_lcms_builtin(name, args),
        DispatchModule::Ccl => ccl::call(interp, name, args, env),
        DispatchModule::Numeric => numeric::call(interp, name, args, env),
        DispatchModule::Fonts => fonts::call(interp, name, args, env),
        DispatchModule::Frames => frames::call(interp, name, args, env),
        DispatchModule::Terminals => terminals::call(interp, name, args, env),
        DispatchModule::Treesit => treesit::call(interp, name, args, env),
        DispatchModule::Gnutls => gnutls::call(interp, name, args),
        DispatchModule::GuiActions => gui_actions::call(interp, name, args),
        DispatchModule::Comp => comp::call(interp, name, args, env),
        DispatchModule::Predicates => predicates::call(interp, name, args, env),
        DispatchModule::Lists => lists::call(interp, name, args, env),
        DispatchModule::Modes => modes::call(interp, name),
        DispatchModule::Composition => composition::call(interp, name, args, env),
        DispatchModule::Strings => strings::call(interp, name, args, env),
        DispatchModule::BufferEdit => buffer_edit::call(interp, name, args, env),
        DispatchModule::BufferMeta => buffer_meta::call(interp, name, args, env),
        DispatchModule::FilesProcess => files_process::call(interp, name, args, env),
        DispatchModule::Display => display::call(interp, name, args, env),
        DispatchModule::EmacsModule => emacs_module::call(name, args),
        DispatchModule::Faces => faces::call(interp, name, args, env),
        DispatchModule::Misc => misc::call(interp, name, args, env),
        DispatchModule::MiscKeymaps => misc_keymaps::call(interp, name, args, env),
        DispatchModule::Overlays => overlays::call(interp, name, args, env),
        DispatchModule::Collections => collections::call(interp, name, args, env),
        DispatchModule::SearchCoding => search_coding::call(interp, name, args, env),
        DispatchModule::ComposedAccessor => call_composed_accessor(interp, name, args),
        DispatchModule::None => Err(LispError::Signal(format!("Unknown function: {}", name))),
    }
}
