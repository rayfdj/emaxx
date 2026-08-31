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
pub(crate) mod gnutls;
mod gui_actions;
mod lists;
pub(super) mod misc;
mod misc_keymaps;
mod numeric;

pub(crate) use buffer_edit::{visual_line_bounds, visual_segment_starts};
#[cfg(test)]
pub(crate) use display::echo_area_message_with_spans;
#[cfg(test)]
pub(crate) use display::render_mode_line_glass;
pub(crate) use display::{
    EchoSpans, LineNumberLayout, LineNumberMode, TtyFaceAttrs, WindowRenderInfo,
    render_window_header_line, render_window_mode_line, render_window_tab_line,
    resolve_tty_face_attrs, store_window_hscroll_state, string_face_spans, window_face_spans,
    window_hscroll_state, window_line_number_layout, window_render_layout,
};
pub(crate) use display::{
    echo_area_message, echo_area_message_tick, echo_area_print, echo_display_message,
    expire_echo_area_message, set_echo_area_message, set_echo_area_message_with_spans,
};
pub(crate) use lists::{
    prepare_kbd_macro_minibuffer_entry, read_minibuffer_text_from_kbd_macro_inner,
};
pub(crate) use misc_keymaps::oclosure_type_of;
mod overlays;
mod predicates;
mod search_coding;
pub(crate) mod strings;
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
    file_name_handler: Option<FileNameHandlerOperation>,
    module: DispatchModule,
}

macro_rules! define_dispatch_modules {
    (
        call($interp:ident, $name:ident, $args:ident, $env:ident);
        $(
            $variant:ident => $module:ident => $call:expr
        ),+ $(,)?
    ) => {
        #[derive(Clone, Copy, PartialEq)]
        enum DispatchModule {
            $($variant,)+
            None,
        }

        impl DispatchModule {
            fn for_name(name: &str) -> Self {
                $(
                    if $module::handles(name) {
                        return Self::$variant;
                    }
                )+
                Self::None
            }

            fn prefer_builtin(self, name: &str) -> bool {
                match self {
                    $(Self::$variant => $module::prefer_builtin(name),)+
                    Self::None => false,
                }
            }

            fn call(
                self,
                $interp: &mut Interpreter,
                $name: &str,
                $args: &[Value],
                $env: &mut crate::lisp::types::Env,
            ) -> Result<Value, LispError> {
                match self {
                    $(Self::$variant => $call,)+
                    Self::None => Err(LispError::Signal(format!(
                        "Unknown function: {}",
                        $name
                    ))),
                }
            }
        }

        #[cfg(test)]
        pub(crate) fn visit_handled_patterns(
            visitor: &mut impl FnMut(&'static str, &'static str),
        ) {
            $($module::visit_handled_patterns(&mut |pattern| {
                visitor(stringify!($module), pattern)
            });)+
        }
    };
}

define_dispatch_modules! {
    call(interp, name, args, env);
    Sqlite => sqlite => sqlite::call(interp, name, args, env),
    Time => numeric_time => call_time_builtin(interp, name, args, env),
    Lcms => color_lcms => call_lcms_builtin(name, args),
    Ccl => ccl => ccl::call(interp, name, args, env),
    Numeric => numeric => numeric::call(interp, name, args, env),
    Fonts => fonts => fonts::call(interp, name, args, env),
    Frames => frames => frames::call(interp, name, args, env),
    Terminals => terminals => terminals::call(interp, name, args, env),
    Treesit => treesit => treesit::call(interp, name, args, env),
    Gnutls => gnutls => gnutls::call(interp, name, args),
    GuiActions => gui_actions => gui_actions::call(interp, name, args),
    Comp => comp => comp::call(interp, name, args, env),
    Predicates => predicates => predicates::call(interp, name, args, env),
    Lists => lists => lists::call(interp, name, args, env),
    Composition => composition => composition::call(interp, name, args, env),
    Strings => strings => strings::call(interp, name, args, env),
    BufferEdit => buffer_edit => buffer_edit::call(interp, name, args, env),
    BufferMeta => buffer_meta => buffer_meta::call(interp, name, args, env),
    FilesProcess => files_process => files_process::call(interp, name, args, env),
    Display => display => display::call(interp, name, args, env),
    EmacsModule => emacs_module => emacs_module::call(name, args),
    Faces => faces => faces::call(interp, name, args, env),
    Misc => misc => misc::call(interp, name, args, env),
    MiscKeymaps => misc_keymaps => misc_keymaps::call(interp, name, args, env),
    Overlays => overlays => overlays::call(interp, name, args, env),
    Collections => collections => collections::call(interp, name, args, env),
    SearchCoding => search_coding => search_coding::call(interp, name, args, env),
}

fn compute_name_facts(name: &str) -> NameFacts {
    let module = DispatchModule::for_name(name);
    // The GNU C manifest is the authority for the public native boundary.
    // Absence from it means Elisp-owned (or not a GNU function), never
    // "probably native".  There is deliberately no private Lisp-callable
    // exception: an internal host operation must use a typed Rust path, not
    // a renamed function cell.
    let native_owner =
        crate::lisp::primitives::generated_gnu_c_primitive_available(name).unwrap_or(false);
    NameFacts {
        // A callable native route is the builtin contract.  Keeping a
        // second list of the same names made every new primitive require
        // two coordinated edits and allowed function lookup to drift from
        // dispatch.
        builtin: module != DispatchModule::None && native_owner,
        special_form: crate::lisp::primitives::is_special_form_name(name),
        prefer_override: native_owner && module.prefer_builtin(name),
        file_name_handler: file_name_handler_operation(name),
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

pub(crate) fn prefer_builtin_override(name: &str) -> bool {
    name_facts(name).prefer_override
}

#[cfg(test)]
pub(crate) fn has_dispatch_handler(name: &str) -> bool {
    name_facts(name).module != DispatchModule::None
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
    if !facts.builtin && !facts.special_form {
        return Err(LispError::Signal(format!("Unknown function: {name}")));
    }
    if let Some(specification) = facts.file_name_handler
        && let Some(result) = dispatch_file_name_handler(interp, env, name, specification, args)?
    {
        return Ok(result);
    }

    facts.module.call(interp, name, args, env)
}
