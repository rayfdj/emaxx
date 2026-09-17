use super::*;

mod buffer_edit;
mod buffer_meta;
mod collections;
mod comp;
pub(crate) use comp::{comp_el_to_eln_rel_filename, native_elisp_load};
mod composition;
pub(crate) mod display;
mod emacs_module;
mod faces;
mod files_process;
mod fonts;
mod frames;
pub(crate) mod gnutls;
mod gui_actions;
mod lists;
pub(super) mod misc;
pub(crate) use misc::set_internal_symbol;
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
    clear_message, echo_area_message, echo_area_message_tick, echo_area_print,
    echo_display_message, expire_echo_area_message, set_echo_area_message,
    set_echo_area_message_with_spans,
};
pub(crate) use lists::{
    prepare_kbd_macro_minibuffer_entry, read_minibuffer_text_from_kbd_macro_inner,
    sync_kbd_macro_execution,
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
/// A primitive's body as a function pointer: what a Lisp_Subr's `function'
/// field is.  A name with one is called through it, not through its
/// module's name match.
pub(crate) type DirectPrimitive =
    fn(&mut Interpreter, &[Value], &mut crate::lisp::types::Env) -> Result<Value, LispError>;

/// The primitives with a function pointer: the list, plist, symbol and
/// predicate primitives the byte code and compiled Lisp call most.
fn direct_primitive(name: &str) -> Option<DirectPrimitive> {
    Some(match name {
        "null" => predicates::direct_null,
        "integerp" => predicates::direct_integerp,
        "numberp" => predicates::direct_numberp,
        "stringp" => predicates::direct_stringp,
        "symbolp" => predicates::direct_symbolp,
        "listp" => predicates::direct_listp,
        "car-safe" => lists::direct_car_safe,
        "cdr-safe" => lists::direct_cdr_safe,
        "nth" => lists::direct_nth,
        "nthcdr" => lists::direct_nthcdr,
        "elt" => lists::direct_elt,
        "length" => lists::direct_length,
        "memq" => lists::direct_memq,
        "memql" => lists::direct_memql,
        "member" => lists::direct_member,
        "assq" => lists::direct_assq,
        "rassq" => lists::direct_rassq,
        "setcar" => collections::direct_setcar,
        "setcdr" => collections::direct_setcdr,
        "get" => misc::direct_get,
        "symbol-value" => misc::direct_symbol_value,
        "symbol-function" => misc_keymaps::direct_symbol_function,
        "symbol-name" => misc_keymaps::direct_symbol_name,
        "equal" => numeric::direct_equal,
        "string-match" => strings::direct_string_match,
        "string-match-p" => strings::direct_string_match_p,
        // The buffer and search primitives the byte code of a mode calls
        // per character or per token.
        "point" => buffer_edit::direct_point,
        "point-min" => buffer_edit::direct_point_min,
        "point-max" => buffer_edit::direct_point_max,
        "bolp" => buffer_edit::direct_bolp,
        "eolp" => buffer_edit::direct_eolp,
        "bobp" => buffer_edit::direct_bobp,
        "eobp" => buffer_edit::direct_eobp,
        "goto-char" => buffer_edit::direct_goto_char,
        "forward-char" => buffer_edit::direct_forward_char,
        "backward-char" => buffer_edit::direct_backward_char,
        "char-after" => buffer_edit::direct_char_after,
        "char-before" => buffer_edit::direct_char_before,
        "skip-chars-forward" => buffer_edit::direct_skip_chars_forward,
        "skip-chars-backward" => buffer_edit::direct_skip_chars_backward,
        "skip-syntax-forward" => buffer_edit::direct_skip_syntax_forward,
        "skip-syntax-backward" => buffer_edit::direct_skip_syntax_backward,
        "beginning-of-line" => buffer_edit::direct_beginning_of_line,
        "end-of-line" => buffer_edit::direct_end_of_line,
        "forward-line" => buffer_edit::direct_forward_line,
        "re-search-forward" => buffer_edit::direct_re_search_forward,
        "re-search-backward" => buffer_edit::direct_re_search_backward,
        "search-forward" => buffer_edit::direct_search_forward,
        "search-backward" => buffer_edit::direct_search_backward,
        "buffer-substring" => buffer_edit::direct_buffer_substring,
        "buffer-substring-no-properties" => buffer_edit::direct_buffer_substring_no_properties,
        "insert" => buffer_edit::direct_insert,
        "delete-region" => buffer_edit::direct_delete_region,
        "looking-at" => search_coding::direct_looking_at,
        "posix-looking-at" => search_coding::direct_posix_looking_at,
        "match-beginning" => search_coding::direct_match_beginning,
        "match-end" => search_coding::direct_match_end,
        "following-char" => display::direct_following_char,
        "preceding-char" => display::direct_preceding_char,
        _ => return None,
    })
}

#[derive(Clone, Copy)]
pub(crate) struct NameFacts {
    pub(crate) builtin: bool,
    pub(crate) special_form: bool,
    pub(crate) prefer_override: bool,
    file_name_handler: Option<FileNameHandlerOperation>,
    module: DispatchModule,
    /// The body as a function pointer, for the names that have one.
    direct: Option<DirectPrimitive>,
    /// The GNU subr's declared maximum argument count; None for MANY,
    /// UNEVALLED, and names outside the generated C manifest.
    max_args: Option<u16>,
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
    EmacsModule => emacs_module => emacs_module::call(interp, name, args, env),
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
        // Only a builtin the module owns is called through its pointer;
        // an overriding Lisp definition or a non-native owner keeps the
        // module's own routing.
        direct: if module != DispatchModule::None && native_owner {
            direct_primitive(name)
        } else {
            None
        },
        // The source-tree arity table is regenerated from the pinned Darwin
        // oracle for its audit.  Dispatch ownership is host-specific, so use
        // the selected host C contract for the runtime maximum as well; this
        // supplies Linux-only primitives such as inotify without reviving
        // Darwin-only kqueue cells.
        max_args: crate::lisp::primitives::GNU_C_PRIMITIVES
            .binary_search_by_key(&name, |contract| contract.name)
            .ok()
            .and_then(|index| crate::lisp::primitives::GNU_C_PRIMITIVES[index].arity)
            .and_then(|(_, maximum)| u16::try_from(maximum).ok()),
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
    // eval.c's funcall_subr rejects a call beyond the subr's declared
    // maximum before the primitive body runs; the per-impl need_args
    // checks only police the minimum, which let extra arguments slip
    // through (and even get const-folded away by the byte optimizer).
    if !facts.special_form
        && let Some(maximum) = facts.max_args
        && args.len() > usize::from(maximum)
    {
        return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
    }
    // The subr's function pointer, past the arity check.
    if let Some(direct) = facts.direct {
        return direct(interp, args, env);
    }
    if let Some(specification) = facts.file_name_handler {
        match dispatch_file_name_handler(interp, env, name, specification, args)? {
            FileNameDispatch::Handled(result) => return Ok(result),
            // fileio.c's copy family runs its native body on the names it
            // expanded for the handler lookup (Fexpand_file_name and
            // expand_cp_target go through handlers), not on the raw
            // arguments; a handler that rewrites names during expansion
            // and then declines the operation must see its rewrite honored.
            FileNameDispatch::Native(Some(normalized)) => {
                return facts.module.call(interp, name, &normalized, env);
            }
            FileNameDispatch::Native(None) => {}
        }
    }

    facts.module.call(interp, name, args, env)
}
