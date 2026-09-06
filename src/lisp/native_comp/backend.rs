//! GNU-compatible libgccjit code generation.
//!
//! This is the Rust owner of the work performed by GNU's `comp.c`.  It does
//! not parse or optimize Elisp: the unchanged `comp.el` frontend hands its
//! final LIMPLE context to this backend.

use super::abi::{
    HANDLER_JMP_OFFSET, HANDLER_NEXT_OFFSET, HANDLER_SIZE, HANDLER_VALUE_OFFSET, LISP_CONS_SIZE,
    NativeMaxArgs, NativeSubr, SYS_JMP_BUF_SIZE, THREAD_HANDLERLIST_OFFSET, THREAD_STATE_SIZE,
};
use super::gccjit::{
    self, BinaryOp, BlockOpaque, BoolOption, Comparison, Context, FieldOpaque, FunctionKind,
    FunctionOpaque, GlobalKind, LValueOpaque, OutputKind, RValueOpaque, StructOpaque, TypeKind,
    TypeOpaque, UnaryOp,
};
use crate::lisp::eval::Interpreter;
use crate::lisp::primitives::string_like;
use crate::lisp::types::{Env, LispError, Value};
use std::ffi::CString;

const HELPER_COUNT: usize = 15;
pub(crate) const HELPER_NAMES: [&str; HELPER_COUNT] = [
    "wrong_type_argument",
    "helper_PSEUDOVECTOR_TYPEP_XUNTAG",
    "pure_write_error",
    "push_handler",
    "record_unwind_protect_excursion",
    "helper_unbind_n",
    "helper_save_restriction",
    "helper_GET_SYMBOL_WITH_POSITION",
    "helper_sanitizer_assert",
    "record_unwind_current_buffer",
    "set_internal",
    "helper_unwind_protect",
    "specbind",
    "maybe_gc",
    "maybe_quit",
];
const INTTYPEBITS: i32 = 2;
const LISP_INT0: i32 = 2;
const MOST_POSITIVE_FIXNUM: i64 = (1_i64 << 61) - 1;
const MOST_NEGATIVE_FIXNUM: i64 = -(1_i64 << 61);
const GCTYPEBITS: i32 = 3;
const LISP_SYMBOL_TAG: i64 = 0;
const LISP_CONS_TAG: i64 = 3;
const LISP_VECTORLIKE_TAG: i64 = 5;
const LISP_FLOAT_TAG: i64 = 7;
const PVEC_BIGNUM: i32 = 2;
const PVEC_SYMBOL_WITH_POS: i32 = 6;

pub(crate) const CURRENT_THREAD_RELOC_SYM: &str = "current_thread_reloc";
pub(crate) const F_SYMBOLS_WITH_POS_ENABLED_RELOC_SYM: &str = "f_symbols_with_pos_enabled_reloc";
pub(crate) const PURE_RELOC_SYM: &str = "pure_reloc";
pub(crate) const DATA_RELOC_SYM: &str = "d_reloc";
pub(crate) const DATA_RELOC_IMPURE_SYM: &str = "d_reloc_imp";
pub(crate) const DATA_RELOC_EPHEMERAL_SYM: &str = "d_reloc_eph";
pub(crate) const FUNC_LINK_TABLE_SYM: &str = "freloc_link_table";
pub(crate) const COMP_UNIT_SYM: &str = "comp_unit";
pub(crate) const TEXT_DATA_RELOC_SYM: &str = "text_data_reloc";
pub(crate) const TEXT_DATA_RELOC_IMPURE_SYM: &str = "text_data_reloc_imp";
pub(crate) const TEXT_DATA_RELOC_EPHEMERAL_SYM: &str = "text_data_reloc_eph";
pub(crate) const TEXT_OPTIM_QLY_SYM: &str = "text_optim_qly";
pub(crate) const TEXT_FDOC_SYM: &str = "text_data_fdoc";
pub(crate) const LINK_TABLE_HASH_SYM: &str = "freloc_hash";

pub(crate) struct SerializedRelocation<'a> {
    pub(crate) len: usize,
    pub(crate) printed: &'a [u8],
}

#[derive(Clone, Copy)]
pub(crate) enum RelocationArrayKind {
    Default,
    Impure,
    Ephemeral,
}

#[derive(Clone, Copy)]
pub(crate) struct Relocation {
    pub(crate) array: RelocationArrayKind,
    pub(crate) index: usize,
}

#[derive(Clone, Copy)]
pub(crate) struct CoreRelocations {
    pub(crate) t: Relocation,
    pub(crate) listp: Relocation,
    pub(crate) consp: Relocation,
    pub(crate) symbol_with_pos_p: Relocation,
}

#[derive(Clone, Copy)]
pub(crate) enum FunctionCallingConvention {
    Fixed(usize),
    Nargs,
    NoArgs,
}

pub(crate) struct FunctionDeclaration {
    pub(crate) c_name: String,
    pub(crate) calling_convention: FunctionCallingConvention,
}

pub(crate) struct UnitInput<'a> {
    pub(crate) debug: i64,
    pub(crate) optimization_qualities: &'a [u8],
    pub(crate) function_docs: &'a [u8],
    pub(crate) abi_hash: &'a [u8],
    pub(crate) data: SerializedRelocation<'a>,
    pub(crate) impure_data: SerializedRelocation<'a>,
    pub(crate) ephemeral_data: SerializedRelocation<'a>,
    pub(crate) helper_c_names: &'a [String; HELPER_COUNT],
    pub(crate) subr_c_names: &'a [String],
    pub(crate) core_relocations: CoreRelocations,
    pub(crate) functions: &'a [FunctionDeclaration],
}

struct Types {
    void: *mut TypeOpaque,
    void_ptr: *mut TypeOpaque,
    bool_: *mut TypeOpaque,
    char_: *mut TypeOpaque,
    int: *mut TypeOpaque,
    unsigned: *mut TypeOpaque,
    emacs_int: *mut TypeOpaque,
    emacs_uint: *mut TypeOpaque,
    uintptr: *mut TypeOpaque,
    ptrdiff: *mut TypeOpaque,
    lisp_obj: *mut TypeOpaque,
    lisp_obj_ptr: *mut TypeOpaque,
    lisp_word_tag: *mut TypeOpaque,
    bool_ptr: *mut TypeOpaque,
    lisp_cons: *mut StructOpaque,
    lisp_cons_u: *mut FieldOpaque,
    lisp_cons_u_s: *mut FieldOpaque,
    lisp_cons_car: *mut FieldOpaque,
    lisp_cons_cdr_union: *mut FieldOpaque,
    lisp_cons_cdr: *mut FieldOpaque,
    symbol_with_position_symbol: *mut FieldOpaque,
    handler_ptr: *mut TypeOpaque,
    handler_jmp: *mut FieldOpaque,
    handler_value: *mut FieldOpaque,
    handler_next: *mut FieldOpaque,
    symbol_with_position_ptr: *mut TypeOpaque,
    thread_state_ptr: *mut TypeOpaque,
    thread_handlerlist: *mut FieldOpaque,
}

impl Types {
    fn declare(context: &Context) -> (Self, Constants) {
        // Keep declaration order identical to GNU `Fcomp__init_ctxt`; GCC's
        // deterministic output depends on the sequence in which nodes enter
        // the context.
        let void = context.c_type(TypeKind::Void);
        let void_ptr = context.c_type(TypeKind::VoidPtr);
        let bool_ = context.c_type(TypeKind::Bool);
        let char_ = context.c_type(TypeKind::Char);
        let int = context.c_type(TypeKind::Int);
        let unsigned = context.c_type(TypeKind::UnsignedInt);
        let _long = context.c_type(TypeKind::Long);
        let _unsigned_long = context.c_type(TypeKind::UnsignedLong);
        let _long_long = context.c_type(TypeKind::LongLong);
        let _unsigned_long_long = context.c_type(TypeKind::UnsignedLongLong);
        let bool_ptr = context.pointer_type(bool_);
        let _char_ptr = context.pointer_type(char_);
        let emacs_int = context.int_type(std::mem::size_of::<isize>(), true);
        let emacs_uint = context.int_type(std::mem::size_of::<usize>(), false);

        // GNU's pointer-word build declares Lisp_Object as `struct Lisp_X *`.
        // Emaxx presents that exact ABI to generated code even though its
        // interpreter uses the higher-level Rust Value enum internally.
        let lisp_x = context.new_opaque_struct(&c_string("Lisp_X"));
        let lisp_obj = context.pointer_type(context.struct_type(lisp_x));
        let lisp_word_tag = context.int_type(std::mem::size_of::<usize>(), false);
        let lisp_obj_ptr = context.pointer_type(lisp_obj);

        let _zero = context.int(emacs_int, 0);
        let constants = Constants {
            one: context.int(emacs_int, 1),
            inttypebits: context.int(emacs_uint, INTTYPEBITS),
            lisp_int0: context.int(emacs_int, LISP_INT0),
        };
        let ptrdiff = context.int_type(std::mem::size_of::<isize>(), true);
        let uintptr = context.int_type(std::mem::size_of::<usize>(), false);
        let size_t = context.int_type(std::mem::size_of::<usize>(), false);

        let _memcpy = declare_memcpy(context, void_ptr, size_t);

        let lisp_cons = context.new_opaque_struct(&c_string("comp_Lisp_Cons"));
        let lisp_cons_type = context.struct_type(lisp_cons);
        let lisp_cons_ptr = context.pointer_type(lisp_cons_type);
        let lisp_cons_cdr = context.new_field(lisp_obj, &c_string("cdr"));
        let lisp_cons_chain = context.new_field(lisp_cons_ptr, &c_string("chain"));
        let cdr_union = context.new_union_type(
            &c_string("comp_cdr_u"),
            &mut [lisp_cons_cdr, lisp_cons_chain],
        );
        let lisp_cons_car = context.new_field(lisp_obj, &c_string("car"));
        let lisp_cons_cdr_union = context.new_field(cdr_union, &c_string("u"));
        let cons_body = context.new_struct_type(
            &c_string("comp_cons_s"),
            &mut [lisp_cons_car, lisp_cons_cdr_union],
        );
        let lisp_cons_u_s = context.new_field(context.struct_type(cons_body), &c_string("s"));
        let align_pad = context.new_field(
            context.new_array_type(char_, LISP_CONS_SIZE),
            &c_string("align_pad"),
        );
        let cons_union =
            context.new_union_type(&c_string("comp_cons_u"), &mut [lisp_cons_u_s, align_pad]);
        let lisp_cons_u = context.new_field(cons_union, &c_string("u"));
        context.set_struct_fields(lisp_cons, &mut [lisp_cons_u]);

        let symbol_with_position_header = context.new_field(ptrdiff, &c_string("header"));
        let symbol_with_position_symbol = context.new_field(lisp_obj, &c_string("sym"));
        let symbol_with_position_position = context.new_field(lisp_obj, &c_string("pos"));
        let symbol_with_position = context.new_struct_type(
            &c_string("comp_lisp_symbol_with_position"),
            &mut [
                symbol_with_position_header,
                symbol_with_position_symbol,
                symbol_with_position_position,
            ],
        );
        let symbol_with_position_ptr =
            context.pointer_type(context.struct_type(symbol_with_position));

        let jmp_stuff = context.new_field(
            context.new_array_type(char_, SYS_JMP_BUF_SIZE),
            &c_string("stuff"),
        );
        let jmp_buf = context.new_struct_type(&c_string("comp_jmp_buf"), &mut [jmp_stuff]);

        let handler = context.new_opaque_struct(&c_string("comp_handler"));
        let handler_ptr = context.pointer_type(context.struct_type(handler));
        let handler_jmp = context.new_field(context.struct_type(jmp_buf), &c_string("jmp"));
        let handler_value = context.new_field(lisp_obj, &c_string("val"));
        let handler_next = context.new_field(handler_ptr, &c_string("next"));
        let handler_pad0 = context.new_field(
            context.new_array_type(char_, HANDLER_VALUE_OFFSET),
            &c_string("pad0"),
        );
        let after_next = HANDLER_NEXT_OFFSET + std::mem::size_of::<usize>();
        let handler_pad1 = context.new_field(
            context.new_array_type(char_, HANDLER_JMP_OFFSET - after_next),
            &c_string("pad1"),
        );
        let handler_pad2 = context.new_field(
            context.new_array_type(char_, HANDLER_SIZE - HANDLER_JMP_OFFSET - SYS_JMP_BUF_SIZE),
            &c_string("pad2"),
        );
        context.set_struct_fields(
            handler,
            &mut [
                handler_pad0,
                handler_value,
                handler_next,
                handler_pad1,
                handler_jmp,
                handler_pad2,
            ],
        );

        let thread_handlerlist = context.new_field(handler_ptr, &c_string("m_handlerlist"));
        let thread_pad0 = context.new_field(
            context.new_array_type(char_, THREAD_HANDLERLIST_OFFSET),
            &c_string("pad0"),
        );
        let thread_pad1 = context.new_field(
            context.new_array_type(
                char_,
                THREAD_STATE_SIZE - THREAD_HANDLERLIST_OFFSET - std::mem::size_of::<usize>(),
            ),
            &c_string("pad1"),
        );
        let thread_state = context.new_struct_type(
            &c_string("comp_thread_state"),
            &mut [thread_pad0, thread_handlerlist, thread_pad1],
        );
        let thread_state_ptr = context.pointer_type(context.struct_type(thread_state));

        (
            Self {
                void,
                void_ptr,
                bool_,
                char_,
                int,
                unsigned,
                emacs_int,
                emacs_uint,
                uintptr,
                ptrdiff,
                lisp_obj,
                lisp_obj_ptr,
                lisp_word_tag,
                bool_ptr,
                lisp_cons,
                lisp_cons_u,
                lisp_cons_u_s,
                lisp_cons_car,
                lisp_cons_cdr_union,
                lisp_cons_cdr,
                symbol_with_position_symbol,
                handler_ptr,
                handler_jmp,
                handler_value,
                handler_next,
                symbol_with_position_ptr,
                thread_state_ptr,
                thread_handlerlist,
            },
            constants,
        )
    }
}

struct Constants {
    one: *mut RValueOpaque,
    inttypebits: *mut RValueOpaque,
    lisp_int0: *mut RValueOpaque,
}

fn declare_memcpy(
    context: &Context,
    void_ptr: *mut TypeOpaque,
    size_t: *mut TypeOpaque,
) -> *mut FunctionOpaque {
    let destination = context.new_param(void_ptr, &c_string("dest"));
    let source = context.new_param(void_ptr, &c_string("src"));
    let count = context.new_param(size_t, &c_string("n"));
    context.new_function(
        FunctionKind::Imported,
        void_ptr,
        &c_string("memcpy"),
        &mut [destination, source, count],
        false,
    )
}

struct RuntimeImports {
    global: *mut LValueOpaque,
    pointer_type: *mut TypeOpaque,
    fields: std::collections::HashMap<
        String,
        *mut FieldOpaque,
        crate::lisp::primitives::FnvBuildHasher,
    >,
}

impl RuntimeImports {
    fn declare(
        context: &Context,
        types: &Types,
        helper_c_names: &[String; HELPER_COUNT],
        native_subrs: &[(NativeSubr, String)],
        abi_hash: &[u8],
    ) -> Self {
        let mut fields = Vec::with_capacity(HELPER_COUNT + native_subrs.len());
        let mut fields_by_name = std::collections::HashMap::with_capacity_and_hasher(
            HELPER_COUNT + native_subrs.len(),
            crate::lisp::primitives::FnvBuildHasher::default(),
        );

        let mut add_helper = |index: usize, return_type, params: &[*mut TypeOpaque]| {
            let field = function_field(context, &helper_c_names[index], return_type, params);
            fields.push(field);
            assert!(
                fields_by_name
                    .insert(HELPER_NAMES[index].to_string(), field)
                    .is_none(),
                "duplicate native runtime helper {}",
                HELPER_NAMES[index]
            );
        };

        add_helper(0, types.void, &[types.lisp_obj, types.lisp_obj]);
        add_helper(1, types.bool_, &[types.lisp_obj, types.int]);
        add_helper(2, types.void, &[types.lisp_obj]);
        add_helper(3, types.handler_ptr, &[types.lisp_obj, types.int]);
        add_helper(4, types.void, &[]);
        add_helper(5, types.lisp_obj, &[types.lisp_obj]);
        add_helper(6, types.void, &[]);
        add_helper(7, types.symbol_with_position_ptr, &[types.lisp_obj]);
        add_helper(8, types.lisp_obj, &[types.lisp_obj, types.lisp_obj]);
        add_helper(9, types.void, &[]);
        add_helper(
            10,
            types.void,
            &[types.lisp_obj, types.lisp_obj, types.lisp_obj, types.int],
        );
        add_helper(11, types.void, &[types.lisp_obj]);
        add_helper(12, types.void, &[types.lisp_obj, types.lisp_obj]);
        add_helper(13, types.void, &[]);
        add_helper(14, types.void, &[]);
        debug_assert_eq!(fields.len(), HELPER_COUNT);

        emit_static_object(context, types.char_, LINK_TABLE_HASH_SYM, abi_hash);

        for (subr, c_name) in native_subrs {
            let params = match subr.max_args {
                NativeMaxArgs::Fixed(count) => vec![types.lisp_obj; usize::from(count)],
                NativeMaxArgs::Many => vec![types.ptrdiff, types.lisp_obj_ptr],
                NativeMaxArgs::Unevalled => vec![types.lisp_obj],
            };
            let field = function_field(context, c_name, types.lisp_obj, &params);
            fields.push(field);
            assert!(
                fields_by_name
                    .insert(subr.name.to_string(), field)
                    .is_none(),
                "duplicate native runtime subroutine {}",
                subr.name
            );
        }

        let structure = context.new_struct_type(&c_string(FUNC_LINK_TABLE_SYM), &mut fields);
        let pointer = context.pointer_type(context.struct_type(structure));
        let global = context.new_global(
            GlobalKind::Exported,
            pointer,
            &c_string(FUNC_LINK_TABLE_SYM),
        );
        Self {
            global,
            pointer_type: pointer,
            fields: fields_by_name,
        }
    }

    fn field(&self, name: &str) -> Result<*mut FieldOpaque, String> {
        self.fields
            .get(name)
            .copied()
            .ok_or_else(|| format!("native compiler ABI has no runtime subroutine `{name}`"))
    }
}

struct RelocationArray {
    value: *mut RValueOpaque,
    len: usize,
}

struct UnitGlobals {
    debug: i64,
    current_thread_ref: *mut RValueOpaque,
    symbols_with_positions_enabled_ref: *mut RValueOpaque,
    pure: *mut RValueOpaque,
    data: RelocationArray,
    impure_data: RelocationArray,
    ephemeral_data: RelocationArray,
    core_relocations: CoreRelocations,
}

struct NumericInliners {
    add1: *mut FunctionOpaque,
    sub1: *mut FunctionOpaque,
    negate: *mut FunctionOpaque,
    maybe_gc_or_quit: *mut FunctionOpaque,
}

struct ConsInliners {
    car: *mut FunctionOpaque,
    cdr: *mut FunctionOpaque,
}

struct TypeInliners {
    pseudovectorp: *mut FunctionOpaque,
    get_symbol_with_position: *mut FunctionOpaque,
    check_type: *mut FunctionOpaque,
}

struct ObjectInliners {
    symbol_with_pos_sym: *mut FunctionOpaque,
    bool_to_lisp_obj: *mut FunctionOpaque,
    setcar: *mut FunctionOpaque,
    setcdr: *mut FunctionOpaque,
}

struct FunctionState {
    function: *mut FunctionOpaque,
    has_non_local: bool,
    speed: i64,
    safety: i64,
    function_relocations: *mut LValueOpaque,
    frame: Vec<*mut LValueOpaque>,
    scratch: Option<*mut LValueOpaque>,
    handler: *mut LValueOpaque,
    blocks: std::collections::HashMap<
        String,
        *mut BlockOpaque,
        crate::lisp::primitives::FnvBuildHasher,
    >,
    current_block: *mut BlockOpaque,
}

/// One live `comp.c`-equivalent compilation context.
pub(crate) struct Compiler {
    context: Context,
    types: Types,
    constants: Constants,
    runtime: Option<RuntimeImports>,
    unit_globals: Option<UnitGlobals>,
    numeric_inliners: Option<NumericInliners>,
    cons_inliners: Option<ConsInliners>,
    type_inliners: Option<TypeInliners>,
    object_inliners: Option<ObjectInliners>,
    exported_functions: std::collections::HashMap<
        String,
        *mut FunctionOpaque,
        crate::lisp::primitives::FnvBuildHasher,
    >,
    current_function: Option<FunctionState>,
    call_array_index: usize,
}

impl Compiler {
    pub(crate) fn acquire() -> Result<Self, String> {
        let context = gccjit::api()?.context()?;
        let (types, constants) = Types::declare(&context);
        if let Some(error) = context.first_error() {
            return Err(error);
        }
        Ok(Self {
            context,
            types,
            constants,
            runtime: None,
            unit_globals: None,
            numeric_inliners: None,
            cons_inliners: None,
            type_inliners: None,
            object_inliners: None,
            exported_functions: std::collections::HashMap::with_hasher(
                crate::lisp::primitives::FnvBuildHasher::default(),
            ),
            current_function: None,
            call_array_index: 0,
        })
    }

    /// Emit the per-unit globals in GNU `emit_ctxt_code` order.  Every byte
    /// slice is the ordinary Lisp printer output produced by the runtime;
    /// this backend only performs `comp.c`'s binary wrapping.
    pub(crate) fn begin_unit(&mut self, input: &UnitInput<'_>) -> Result<(), String> {
        if self.unit_globals.is_some() || self.runtime.is_some() {
            return Err("native compiler unit globals already declared".to_string());
        }
        let subrs = super::abi::native_subrs();
        if input.subr_c_names.len() != subrs.len() {
            return Err(format!(
                "native runtime has {} C names for {} registered subroutines",
                input.subr_c_names.len(),
                subrs.len()
            ));
        }

        self.emit_static_object(TEXT_OPTIM_QLY_SYM, input.optimization_qualities);
        self.emit_static_object(TEXT_FDOC_SYM, input.function_docs);

        let current_thread_ref = self.context.lvalue_as_rvalue(self.context.new_global(
            GlobalKind::Exported,
            self.context.pointer_type(self.types.thread_state_ptr),
            &c_string(CURRENT_THREAD_RELOC_SYM),
        ));
        let symbols_with_positions_enabled_ref =
            self.context.lvalue_as_rvalue(self.context.new_global(
                GlobalKind::Exported,
                self.types.bool_ptr,
                &c_string(F_SYMBOLS_WITH_POS_ENABLED_RELOC_SYM),
            ));
        let pure = self.context.lvalue_as_rvalue(self.context.new_global(
            GlobalKind::Exported,
            self.types.void_ptr,
            &c_string(PURE_RELOC_SYM),
        ));
        let _comp_unit = self.context.new_global(
            GlobalKind::Exported,
            self.types.lisp_obj,
            &c_string(COMP_UNIT_SYM),
        );

        let data = self.declare_data_relocation(&input.data, DATA_RELOC_SYM, TEXT_DATA_RELOC_SYM);
        let impure_data = self.declare_data_relocation(
            &input.impure_data,
            DATA_RELOC_IMPURE_SYM,
            TEXT_DATA_RELOC_IMPURE_SYM,
        );
        let ephemeral_data = self.declare_data_relocation(
            &input.ephemeral_data,
            DATA_RELOC_EPHEMERAL_SYM,
            TEXT_DATA_RELOC_EPHEMERAL_SYM,
        );

        let native_subrs = subrs
            .iter()
            .copied()
            .zip(input.subr_c_names.iter().cloned())
            .collect::<Vec<_>>();
        self.runtime = Some(RuntimeImports::declare(
            &self.context,
            &self.types,
            input.helper_c_names,
            &native_subrs,
            input.abi_hash,
        ));

        self.unit_globals = Some(UnitGlobals {
            debug: input.debug,
            current_thread_ref,
            symbols_with_positions_enabled_ref,
            pure,
            data,
            impure_data,
            ephemeral_data,
            core_relocations: input.core_relocations,
        });
        self.define_car_cdr()?;
        self.define_pseudovectorp()?;
        self.define_object_inliners()?;
        self.define_numeric_inliners()?;
        self.declare_functions(input.functions)?;
        if let Some(error) = self.context.first_error() {
            return Err(error);
        }
        Ok(())
    }

    fn declare_functions(&mut self, declarations: &[FunctionDeclaration]) -> Result<(), String> {
        for declaration in declarations {
            let function = match declaration.calling_convention {
                FunctionCallingConvention::Fixed(count) => {
                    let mut params = Vec::with_capacity(count);
                    for index in 0..count {
                        params.push(
                            self.context
                                .new_param(self.types.lisp_obj, &c_string(&format!("par_{index}"))),
                        );
                    }
                    self.context.new_function(
                        FunctionKind::Exported,
                        self.types.lisp_obj,
                        &c_string(&declaration.c_name),
                        &mut params,
                        false,
                    )
                }
                FunctionCallingConvention::Nargs => {
                    let nargs = self
                        .context
                        .new_param(self.types.ptrdiff, &c_string("nargs"));
                    let args = self
                        .context
                        .new_param(self.types.lisp_obj_ptr, &c_string("args"));
                    self.context.new_function(
                        FunctionKind::Exported,
                        self.types.lisp_obj,
                        &c_string(&declaration.c_name),
                        &mut [nargs, args],
                        false,
                    )
                }
                FunctionCallingConvention::NoArgs => self.context.new_function(
                    FunctionKind::Exported,
                    self.types.lisp_obj,
                    &c_string(&declaration.c_name),
                    &mut [],
                    false,
                ),
            };
            if self
                .exported_functions
                .insert(declaration.c_name.clone(), function)
                .is_some()
            {
                return Err(format!(
                    "native compiler declared duplicate function `{}`",
                    declaration.c_name
                ));
            }
        }
        Ok(())
    }

    fn begin_function(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        function_value: &Value,
    ) -> Result<Vec<(String, Value, Vec<Value>)>, LispError> {
        if self.current_function.is_some() {
            return Err(super::lisp::native_ice(
                "native compiler function state is already active",
            ));
        }
        let c_name = lisp_string(call_lisp_one(
            interp,
            env,
            "comp-func-c-name",
            function_value.clone(),
        )?)?;
        let function = self
            .exported_functions
            .get(&c_name)
            .copied()
            .ok_or_else(|| super::lisp::native_ice("missing compiled function declaration"))?;
        let frame_size =
            call_lisp_one(interp, env, "comp-func-frame-size", function_value.clone())?
                .as_integer()?;
        let frame_size = usize::try_from(frame_size)
            .map_err(|_| super::lisp::native_ice("negative native function frame size"))?;
        let has_non_local = call_lisp_one(
            interp,
            env,
            "comp-func-has-non-local",
            function_value.clone(),
        )?
        .is_truthy();
        let speed =
            call_lisp_one(interp, env, "comp-func-speed", function_value.clone())?.as_integer()?;
        let safety =
            call_lisp_one(interp, env, "comp-func-safety", function_value.clone())?.as_integer()?;

        let runtime = self
            .runtime
            .as_ref()
            .ok_or_else(|| super::lisp::native_ice("native runtime imports are not declared"))?;
        let function_relocations =
            self.context
                .new_local(function, runtime.pointer_type, &c_string("freloc"));
        let mut frame = Vec::with_capacity(frame_size);
        if has_non_local || speed == 0 {
            let array = self.context.new_local(
                function,
                self.context.new_array_type(self.types.lisp_obj, frame_size),
                &c_string("frame"),
            );
            let array_value = self.context.lvalue_as_rvalue(array);
            for index in 0..frame_size {
                frame.push(self.context.array_access(
                    array_value,
                    self.context.int(
                        self.types.int,
                        i32::try_from(index).map_err(|_| {
                            super::lisp::native_ice("native frame index exceeds c_int")
                        })?,
                    ),
                ));
            }
        } else {
            for index in 0..frame_size {
                frame.push(self.context.new_local(
                    function,
                    self.types.lisp_obj,
                    &c_string(&format!("slot_{index}")),
                ));
            }
        }
        let handler = self
            .context
            .new_local(function, self.types.handler_ptr, &c_string("c"));

        let blocks_value = call_lisp_one(interp, env, "comp-func-blocks", function_value.clone())?;
        let (_, block_entries) = crate::lisp::json::hash_table_entries(interp, &blocks_value)
            .ok_or_else(|| super::lisp::native_ice("comp-func-blocks returned a non-hash-table"))?;
        let mut blocks = std::collections::HashMap::with_capacity_and_hasher(
            block_entries.len(),
            crate::lisp::primitives::FnvBuildHasher::default(),
        );
        let entry = self.context.new_block(function, &c_string("entry"));
        blocks.insert("entry".to_string(), entry);
        for (name, _) in &block_entries {
            let name = lisp_symbol(name)?;
            if name != "entry" {
                let block = self.context.new_block(function, &c_string(name));
                if blocks.insert(name.to_string(), block).is_some() {
                    return Err(super::lisp::native_ice("double basic block declaration"));
                }
            }
        }
        self.context.assign(
            entry,
            function_relocations,
            self.context.lvalue_as_rvalue(runtime.global),
        );

        let mut bodies = Vec::with_capacity(block_entries.len());
        for (name, block) in block_entries {
            let name = lisp_symbol(&name)?.to_string();
            if block.is_nil() {
                return Err(super::lisp::native_ice("basic block is missing or empty"));
            }
            let instructions = call_lisp_one(interp, env, "comp-block-insns", block.clone())?;
            if instructions.is_nil() {
                return Err(super::lisp::native_ice("basic block is missing or empty"));
            }
            bodies.push((name, block, instructions.to_vec()?));
        }

        self.current_function = Some(FunctionState {
            function,
            has_non_local,
            speed,
            safety,
            function_relocations,
            frame,
            scratch: None,
            handler,
            blocks,
            current_block: entry,
        });
        Ok(bodies)
    }

    fn emit_static_object(&self, name: &str, printed: &[u8]) {
        emit_static_object(&self.context, self.types.char_, name, printed);
    }

    fn declare_data_relocation(
        &self,
        relocation: &SerializedRelocation<'_>,
        code_symbol: &str,
        text_symbol: &str,
    ) -> RelocationArray {
        let global = self.context.new_global(
            GlobalKind::Exported,
            self.context
                .new_array_type(self.types.lisp_obj, relocation.len),
            &c_string(code_symbol),
        );
        self.emit_static_object(text_symbol, relocation.printed);
        RelocationArray {
            value: self.context.lvalue_as_rvalue(global),
            len: relocation.len,
        }
    }

    #[cfg(test)]
    pub(crate) fn libgccjit_version() -> Option<(i32, i32, i32)> {
        gccjit::version()
    }

    fn coerce(&self, value: *mut RValueOpaque, destination: *mut TypeOpaque) -> *mut RValueOpaque {
        let source = self.context.rvalue_type(value);
        if source == destination {
            return value;
        }
        let source_is_pointer = self.context.pointed_type(source).is_some();
        let destination_is_pointer = self.context.pointed_type(destination).is_some();
        let value = match (source_is_pointer, destination_is_pointer) {
            (true, false) => {
                let pointer = self.context.cast(value, self.types.void_ptr);
                self.context.bitcast(pointer, self.types.uintptr)
            }
            (false, true) => {
                let integer = self.context.cast(value, self.types.uintptr);
                self.context.bitcast(integer, self.types.void_ptr)
            }
            _ => value,
        };
        self.context.cast(value, destination)
    }

    fn binary(
        &self,
        operation: BinaryOp,
        result_type: *mut TypeOpaque,
        left: *mut RValueOpaque,
        right: *mut RValueOpaque,
    ) -> *mut RValueOpaque {
        self.context.binary(
            operation,
            result_type,
            self.coerce(left, result_type),
            self.coerce(right, result_type),
        )
    }

    fn fixnump(&self, value: *mut RValueOpaque) -> *mut RValueOpaque {
        let without_tag = self.binary(
            BinaryOp::Minus,
            self.types.unsigned,
            value,
            self.context.int(self.types.unsigned, LISP_INT0),
        );
        let tag_bits = self.binary(
            BinaryOp::BitwiseAnd,
            self.types.unsigned,
            without_tag,
            self.context.int(self.types.unsigned, 3),
        );
        self.context
            .unary(UnaryOp::LogicalNegate, self.types.int, tag_bits)
    }

    fn xfixnum(&self, value: *mut RValueOpaque) -> *mut RValueOpaque {
        let signed_word = self.coerce(value, self.types.emacs_int);
        let unsigned_word = self.coerce(signed_word, self.types.emacs_uint);
        self.coerce(
            self.binary(
                BinaryOp::RightShift,
                self.types.emacs_int,
                unsigned_word,
                self.constants.inttypebits,
            ),
            self.types.emacs_int,
        )
    }

    fn make_fixnum(&self, value: *mut RValueOpaque) -> *mut RValueOpaque {
        let shifted = self.binary(
            BinaryOp::LeftShift,
            self.types.emacs_int,
            value,
            self.constants.inttypebits,
        );
        let tagged = self.binary(
            BinaryOp::Plus,
            self.types.emacs_int,
            shifted,
            self.constants.lisp_int0,
        );
        self.coerce(tagged, self.types.lisp_obj)
    }

    fn unit_globals(&self) -> Result<&UnitGlobals, String> {
        self.unit_globals
            .as_ref()
            .ok_or_else(|| "native compiler unit globals are not declared".to_string())
    }

    fn relocation_lvalue(&self, relocation: Relocation) -> Result<*mut LValueOpaque, String> {
        let globals = self.unit_globals()?;
        let array = match relocation.array {
            RelocationArrayKind::Default => &globals.data,
            RelocationArrayKind::Impure => &globals.impure_data,
            RelocationArrayKind::Ephemeral => &globals.ephemeral_data,
        };
        if relocation.index >= array.len {
            return Err(format!(
                "native data relocation {} is outside array length {}",
                relocation.index, array.len
            ));
        }
        Ok(self.context.array_access(
            array.value,
            self.context.int(
                self.types.ptrdiff,
                i32::try_from(relocation.index)
                    .expect("native relocation index exceeds libgccjit int literal"),
            ),
        ))
    }

    fn relocated_value(&self, relocation: Relocation) -> Result<*mut RValueOpaque, String> {
        Ok(self
            .context
            .lvalue_as_rvalue(self.relocation_lvalue(relocation)?))
    }

    fn nil_value(&self) -> *mut RValueOpaque {
        self.context
            .pointer(self.types.lisp_obj, std::ptr::null_mut())
    }

    /// Materialize the one-word representation used by GNU's pointer-word
    /// build.  This is the Rust equivalent of `emit_rvalue_from_lisp_obj`
    /// for a self-contained fixnum; no interpreter object crosses into JIT
    /// code here.
    fn fixnum_value(&self, value: i64) -> Result<*mut RValueOpaque, String> {
        if !(MOST_NEGATIVE_FIXNUM..=MOST_POSITIVE_FIXNUM).contains(&value) {
            return Err(format!(
                "integer {value} does not fit the native fixnum ABI"
            ));
        }
        let word = value
            .wrapping_shl(INTTYPEBITS as u32)
            .wrapping_add(i64::from(LISP_INT0)) as usize;
        Ok(self
            .context
            .pointer(self.types.lisp_obj, word as *mut std::ffi::c_void))
    }

    fn current_function(&self) -> Result<&FunctionState, String> {
        self.current_function
            .as_ref()
            .ok_or_else(|| "native compiler function state is not active".to_string())
    }

    fn current_block(&self) -> Result<*mut BlockOpaque, String> {
        Ok(self.current_function()?.current_block)
    }

    fn block(&self, name: &Value) -> Result<*mut BlockOpaque, LispError> {
        let name = lisp_symbol(name)?;
        self.current_function()
            .map_err(|error| super::lisp::native_ice(&error))?
            .blocks
            .get(name)
            .copied()
            .ok_or_else(|| super::lisp::native_ice("missing basic block"))
    }

    /// Return the storage owned by an existing `comp-mvar`.  Slot selection
    /// stays in `comp.el`; this backend only implements the frame/scratch
    /// storage operation owned by GNU `comp.c`.
    fn mvar_lvalue(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        mvar: &Value,
    ) -> Result<*mut LValueOpaque, LispError> {
        let slot = call_lisp_one(interp, env, "comp-mvar-slot", mvar.clone())?;
        if slot.as_symbol().is_ok_and(|name| name == "scratch") {
            let existing = self
                .current_function()
                .map_err(|error| super::lisp::native_ice(&error))?
                .scratch;
            if let Some(scratch) = existing {
                return Ok(scratch);
            }
            let function = self
                .current_function()
                .map_err(|error| super::lisp::native_ice(&error))?
                .function;
            let scratch =
                self.context
                    .new_local(function, self.types.lisp_obj, &c_string("scratch"));
            self.current_function
                .as_mut()
                .expect("function state checked above")
                .scratch = Some(scratch);
            return Ok(scratch);
        }

        let slot = slot.as_integer()?;
        let slot = usize::try_from(slot)
            .map_err(|_| super::lisp::native_ice("negative native frame slot"))?;
        self.current_function()
            .map_err(|error| super::lisp::native_ice(&error))?
            .frame
            .get(slot)
            .copied()
            .ok_or_else(|| super::lisp::native_ice("native frame slot is out of bounds"))
    }

    fn lisp_object_rvalue(
        &self,
        interp: &mut Interpreter,
        env: &mut Env,
        unit: &super::lisp::UnitData,
        object: &Value,
    ) -> Result<*mut RValueOpaque, LispError> {
        if object.is_nil() {
            return Ok(self.nil_value());
        }
        let relocation = unit.relocation(interp, env, object)?;
        self.relocated_value(relocation)
            .map_err(|error| super::lisp::native_ice(&error))
    }

    /// Emit a value from a `comp-mvar`, preserving `comp.c`'s distinction
    /// between a directly encoded fixnum, a relocation, and a frame load.
    fn mvar_rvalue(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        unit: &super::lisp::UnitData,
        mvar: &Value,
    ) -> Result<*mut RValueOpaque, LispError> {
        let constant_valid =
            call_lisp_one(interp, env, "comp-cstr-imm-vld-p", mvar.clone())?.is_truthy();
        if constant_valid {
            let value = call_lisp_one(interp, env, "comp-cstr-imm", mvar.clone())?;
            if let Value::Integer(integer) = value {
                if (MOST_NEGATIVE_FIXNUM..=MOST_POSITIVE_FIXNUM).contains(&integer) {
                    return self
                        .fixnum_value(integer)
                        .map_err(|error| super::lisp::native_ice(&error));
                }
                return self.lisp_object_rvalue(interp, env, unit, &Value::Integer(integer));
            }
            return self.lisp_object_rvalue(interp, env, unit, &value);
        }

        let lvalue = self.mvar_lvalue(interp, env, mvar)?;
        Ok(self.context.lvalue_as_rvalue(lvalue))
    }

    fn assign_mvar(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        destination: &Value,
        value: *mut RValueOpaque,
    ) -> Result<(), LispError> {
        let block = self
            .current_block()
            .map_err(|error| super::lisp::native_ice(&error))?;
        let destination = self.mvar_lvalue(interp, env, destination)?;
        self.context.assign(block, destination, value);
        Ok(())
    }

    fn pointer_arithmetic(
        &self,
        pointer: *mut RValueOpaque,
        pointer_type: *mut TypeOpaque,
        element_size: usize,
        index: *mut RValueOpaque,
    ) -> *mut RValueOpaque {
        let offset = self.binary(
            BinaryOp::Multiply,
            self.types.uintptr,
            self.context.int(
                self.types.uintptr,
                i32::try_from(element_size).expect("native pointer element size exceeds c_int"),
            ),
            index,
        );
        let address = self.binary(BinaryOp::Plus, self.types.uintptr, pointer, offset);
        self.coerce(address, pointer_type)
    }

    fn add_comment(&self, text: &str) -> Result<(), LispError> {
        let globals = self
            .unit_globals()
            .map_err(|error| super::lisp::native_ice(&error))?;
        if globals.debug == 0 {
            return Ok(());
        }
        let text = text.split('\0').next().unwrap_or_default();
        let text = CString::new(text)
            .map_err(|_| super::lisp::native_ice("native compiler comment contains NUL"))?;
        let block = self
            .current_block()
            .map_err(|error| super::lisp::native_ice(&error))?;
        self.context.comment(block, &text);
        Ok(())
    }

    pub(crate) fn emit_functions(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        unit: &super::lisp::UnitData,
    ) -> Result<(), LispError> {
        for function in unit.function_values() {
            let bodies = self.begin_function(interp, env, function)?;
            for (name, _, instructions) in bodies {
                let block = self
                    .current_function()
                    .map_err(|error| super::lisp::native_ice(&error))?
                    .blocks
                    .get(&name)
                    .copied()
                    .ok_or_else(|| super::lisp::native_ice("missing basic block"))?;
                self.current_function
                    .as_mut()
                    .expect("function state created by begin_function")
                    .current_block = block;
                for instruction in instructions {
                    self.emit_instruction(interp, env, unit, &instruction)?;
                }
            }
            if let Some(error) = self.context.first_error() {
                self.current_function = None;
                return Err(super::lisp::native_ice(&format!(
                    "failing to compile function: {error}"
                )));
            }
            self.current_function = None;
        }
        Ok(())
    }

    /// Apply the libgccjit settings GNU applies before creating per-unit
    /// declarations.  The output filename is used only for Darwin's dylib ID.
    pub(crate) fn configure_unit(
        &self,
        speed: i64,
        debug: i64,
        output_filename: &str,
    ) -> Result<(), String> {
        self.context.set_debug_info(debug != 0);
        if debug >= 3 {
            self.context
                .set_bool_option(BoolOption::KeepIntermediates, true);
            self.context
                .set_bool_option(BoolOption::DumpEverything, true);
        }
        self.context
            .set_optimization_level(i32::try_from(speed).unwrap_or_else(|_| {
                if speed.is_negative() {
                    i32::MIN
                } else {
                    i32::MAX
                }
            }));
        self.add_platform_driver_options(output_filename);
        Ok(())
    }

    /// comp.c sets a unique dylib ID on Darwin; other targets add nothing.
    #[cfg(target_os = "macos")]
    fn add_platform_driver_options(&self, output_filename: &str) {
        let basename = output_filename
            .rsplit('/')
            .next()
            .unwrap_or(output_filename);
        self.context.add_driver_option(&c_string("-install_name"));
        self.context.add_driver_option(&c_string(basename));
    }

    #[cfg(not(target_os = "macos"))]
    fn add_platform_driver_options(&self, _output_filename: &str) {}

    /// Add the file-local/global compiler flags in GNU order, after the JIT
    /// graph is complete and before GNU asks Lisp to allocate a temp file.
    pub(crate) fn prepare_output(
        &self,
        unit: &super::lisp::UnitData,
        output_filename: &str,
    ) -> Result<(), String> {
        for option in unit.compiler_options() {
            self.context.add_command_line_option(&c_string(option));
        }
        for option in unit.driver_options() {
            self.context.add_driver_option(&c_string(option));
        }

        let base = output_filename
            .strip_suffix(".eln")
            .unwrap_or(output_filename);
        if unit.debug() > 1 {
            self.context
                .dump_to_file(&c_string(&format!("{base}.c")), true);
        }
        if unit.reproducer() {
            self.context
                .dump_reproducer(&c_string(&format!("{base}_libgccjit_repro.c")));
        }
        Ok(())
    }

    pub(crate) fn compile_to_file(&self, temporary_filename: &str) -> Result<(), String> {
        self.context
            .compile_to_file(OutputKind::DynamicLibrary, &c_string(temporary_filename))
    }

    fn named_call(
        &self,
        callee: &Value,
        arguments: &mut [*mut RValueOpaque],
        direct: bool,
    ) -> Result<*mut RValueOpaque, LispError> {
        if direct {
            let name = lisp_string(callee.clone())?;
            let function =
                self.exported_functions.get(&name).copied().ok_or_else(|| {
                    super::lisp::native_ice("missing direct function declaration")
                })?;
            return Ok(self.context.call(function, arguments));
        }
        let name = lisp_symbol(callee)?;
        self.runtime_call(name, arguments)
            .map_err(|error| super::lisp::native_ice(&error))
    }

    fn named_call_ref(
        &self,
        callee: &Value,
        argument_count: usize,
        first_argument: *mut LValueOpaque,
        direct: bool,
    ) -> Result<*mut RValueOpaque, LispError> {
        let mut arguments = [
            self.context.int(
                self.types.ptrdiff,
                i32::try_from(argument_count)
                    .map_err(|_| super::lisp::native_ice("native call arity exceeds c_int"))?,
            ),
            self.context.lvalue_address(first_argument),
        ];
        self.named_call(callee, &mut arguments, direct)
    }

    fn simple_call(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        unit: &super::lisp::UnitData,
        fields: &[Value],
        direct: bool,
    ) -> Result<*mut RValueOpaque, LispError> {
        let (callee, mvars) = fields
            .split_first()
            .ok_or_else(|| super::lisp::native_ice("LIMPLE call has no callee"))?;
        let mut arguments = Vec::with_capacity(mvars.len());
        for mvar in mvars {
            arguments.push(self.mvar_rvalue(interp, env, unit, mvar)?);
        }
        self.named_call(callee, &mut arguments, direct)
    }

    fn call_ref(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        unit: &super::lisp::UnitData,
        fields: &[Value],
        direct: bool,
    ) -> Result<*mut RValueOpaque, LispError> {
        let (callee, mvars) = fields
            .split_first()
            .ok_or_else(|| super::lisp::native_ice("LIMPLE callref has no callee"))?;
        let state = self
            .current_function()
            .map_err(|error| super::lisp::native_ice(&error))?;
        if mvars.is_empty() {
            let first = state
                .frame
                .first()
                .copied()
                .ok_or_else(|| super::lisp::native_ice("native callref has an empty frame"))?;
            return self.named_call_ref(callee, 0, first, direct);
        }
        if state.has_non_local || state.speed == 0 {
            let first_slot =
                call_lisp_one(interp, env, "comp-mvar-slot", mvars[0].clone())?.as_integer()?;
            let first_slot = usize::try_from(first_slot)
                .map_err(|_| super::lisp::native_ice("negative native callref frame slot"))?;
            let first = self
                .current_function()
                .map_err(|error| super::lisp::native_ice(&error))?
                .frame
                .get(first_slot)
                .copied()
                .ok_or_else(|| {
                    super::lisp::native_ice("native callref frame slot is out of bounds")
                })?;
            return self.named_call_ref(callee, mvars.len(), first, direct);
        }

        let function = state.function;
        let name = format!("call_arr_{}", self.call_array_index);
        self.call_array_index += 1;
        let temporary = self.context.new_local(
            function,
            self.context
                .new_array_type(self.types.lisp_obj, mvars.len()),
            &c_string(&name),
        );
        let temporary_value = self.context.lvalue_as_rvalue(temporary);
        let block = self
            .current_block()
            .map_err(|error| super::lisp::native_ice(&error))?;
        for (index, mvar) in mvars.iter().enumerate() {
            let destination = self.context.array_access(
                temporary_value,
                self.context.int(
                    self.types.int,
                    i32::try_from(index).map_err(|_| {
                        super::lisp::native_ice("native call argument exceeds c_int")
                    })?,
                ),
            );
            let value = self.mvar_rvalue(interp, env, unit, mvar)?;
            self.context.assign(block, destination, value);
        }
        let first = self
            .context
            .array_access(temporary_value, self.context.int(self.types.int, 0));
        self.named_call_ref(callee, mvars.len(), first, direct)
    }

    fn type_hint_matches(
        &self,
        interp: &mut Interpreter,
        env: &mut Env,
        mvar: &Value,
        expected: &str,
    ) -> Result<bool, LispError> {
        if self
            .current_function()
            .map_err(|error| super::lisp::native_ice(&error))?
            .safety
            != 0
        {
            return Ok(false);
        }
        Ok(super::lisp::call(
            interp,
            env,
            "comp-mvar-type-hint-match-p",
            &[mvar.clone(), Value::symbol(expected)],
        )?
        .is_truthy())
    }

    fn call_typed_inliner(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        unit: &super::lisp::UnitData,
        fields: &[Value],
        inliner: (*mut FunctionOpaque, &str, usize),
    ) -> Result<*mut RValueOpaque, LispError> {
        let (function, expected, arity) = inliner;
        if fields.len() != arity + 1 {
            return Err(super::lisp::native_ice(
                "inliner received an unexpected arity",
            ));
        }
        let certified = self.type_hint_matches(interp, env, &fields[1], expected)?;
        let mut arguments = Vec::with_capacity(arity + 1);
        for mvar in &fields[1..] {
            arguments.push(self.mvar_rvalue(interp, env, unit, mvar)?);
        }
        arguments.push(self.context.int(self.types.bool_, i32::from(certified)));
        Ok(self.context.call(function, &mut arguments))
    }

    fn limple_call(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        unit: &super::lisp::UnitData,
        fields: &[Value],
    ) -> Result<*mut RValueOpaque, LispError> {
        let callee = fields
            .first()
            .ok_or_else(|| super::lisp::native_ice("LIMPLE call has no callee"))?;
        let name = lisp_symbol(callee)?;
        match name {
            "set_internal" => {
                if fields.len() != 3 {
                    return Err(super::lisp::native_ice(
                        "set_internal received an unexpected arity",
                    ));
                }
                let first = self.mvar_rvalue(interp, env, unit, &fields[1])?;
                let second = self.mvar_rvalue(interp, env, unit, &fields[2])?;
                self.runtime_call(
                    "set_internal",
                    &mut [
                        first,
                        second,
                        self.nil_value(),
                        self.context.int(self.types.int, 0),
                    ],
                )
                .map_err(|error| super::lisp::native_ice(&error))
            }
            "1+" | "1-" | "negate" | "car" | "cdr" => {
                let function = match name {
                    "1+" => self.numeric_inliners.as_ref().map(|set| set.add1),
                    "1-" => self.numeric_inliners.as_ref().map(|set| set.sub1),
                    "negate" => self.numeric_inliners.as_ref().map(|set| set.negate),
                    "car" => self.cons_inliners.as_ref().map(|set| set.car),
                    "cdr" => self.cons_inliners.as_ref().map(|set| set.cdr),
                    _ => unreachable!(),
                }
                .ok_or_else(|| super::lisp::native_ice("native inliner is not declared"))?;
                self.call_typed_inliner(
                    interp,
                    env,
                    unit,
                    fields,
                    (
                        function,
                        if matches!(name, "car" | "cdr") {
                            "cons"
                        } else {
                            "fixnum"
                        },
                        1,
                    ),
                )
            }
            "setcar" | "setcdr" => {
                let inliners = self.object_inliners.as_ref().ok_or_else(|| {
                    super::lisp::native_ice("native object inliners are not declared")
                })?;
                let function = if name == "setcar" {
                    inliners.setcar
                } else {
                    inliners.setcdr
                };
                self.call_typed_inliner(interp, env, unit, fields, (function, "cons", 2))
            }
            "consp" | "numberp" | "integerp" => {
                if fields.len() != 2 {
                    return Err(super::lisp::native_ice(
                        "predicate inliner received an unexpected arity",
                    ));
                }
                let value = self.mvar_rvalue(interp, env, unit, &fields[1])?;
                let predicate = match name {
                    "consp" => self.coerce(self.consp(value), self.types.bool_),
                    "numberp" => self
                        .numberp(value)
                        .map_err(|error| super::lisp::native_ice(&error))?,
                    "integerp" => self
                        .integerp(value)
                        .map_err(|error| super::lisp::native_ice(&error))?,
                    _ => unreachable!(),
                };
                let converter = self
                    .object_inliners
                    .as_ref()
                    .ok_or_else(|| {
                        super::lisp::native_ice("native boolean converter is not declared")
                    })?
                    .bool_to_lisp_obj;
                Ok(self.context.call(converter, &mut [predicate]))
            }
            "comp-maybe-gc-or-quit" => {
                let function = self
                    .numeric_inliners
                    .as_ref()
                    .ok_or_else(|| super::lisp::native_ice("native GC inliner is not declared"))?
                    .maybe_gc_or_quit;
                Ok(self.context.call(function, &mut []))
            }
            _ => self.simple_call(interp, env, unit, fields, false),
        }
    }

    fn handler_list_lvalue(&self) -> Result<*mut LValueOpaque, LispError> {
        let globals = self
            .unit_globals()
            .map_err(|error| super::lisp::native_ice(&error))?;
        let thread = self
            .context
            .lvalue_as_rvalue(self.context.dereference(globals.current_thread_ref));
        Ok(self
            .context
            .dereference_field(thread, self.types.thread_handlerlist))
    }

    fn emit_setjmp(&self, buffer: *mut RValueOpaque) -> *mut RValueOpaque {
        let parameter = self
            .context
            .new_param(self.types.void_ptr, &c_string("buf"));
        let function = self.context.new_function(
            FunctionKind::Imported,
            self.types.int,
            &c_string("_setjmp"),
            &mut [parameter],
            false,
        );
        self.context.call(function, &mut [buffer])
    }

    fn push_handler(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        unit: &super::lisp::UnitData,
        arguments: &[Value],
    ) -> Result<(), LispError> {
        if arguments.len() != 4 {
            return Err(super::lisp::native_ice(
                "push-handler received an unexpected arity",
            ));
        }
        let handler_kind = match lisp_symbol(&arguments[0])? {
            "catcher" => 0,
            "condition-case" => 1,
            _ => {
                return Err(super::lisp::native_ice(
                    "incoherent push-handler instruction",
                ));
            }
        };
        let handler_value = self.mvar_rvalue(interp, env, unit, &arguments[1])?;
        let handler = self
            .current_function()
            .map_err(|error| super::lisp::native_ice(&error))?
            .handler;
        let block = self
            .current_block()
            .map_err(|error| super::lisp::native_ice(&error))?;
        let pushed = self
            .runtime_call(
                "push_handler",
                &mut [
                    handler_value,
                    self.context.int(self.types.int, handler_kind),
                ],
            )
            .map_err(|error| super::lisp::native_ice(&error))?;
        self.context.assign(block, handler, pushed);
        let jump_buffer = self.context.lvalue_address(self.context.dereference_field(
            self.context.lvalue_as_rvalue(handler),
            self.types.handler_jmp,
        ));
        let result = self.emit_setjmp(jump_buffer);
        self.conditional_jump(
            block,
            result,
            self.block(&arguments[2])?,
            self.block(&arguments[3])?,
        );
        Ok(())
    }

    fn pop_handler(&self) -> Result<(), LispError> {
        let handler_list = self.handler_list_lvalue()?;
        let next = self
            .context
            .lvalue_as_rvalue(self.context.dereference_field(
                self.context.lvalue_as_rvalue(handler_list),
                self.types.handler_next,
            ));
        let block = self
            .current_block()
            .map_err(|error| super::lisp::native_ice(&error))?;
        self.context.assign(block, handler_list, next);
        Ok(())
    }

    fn fetch_handler(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        destination: &Value,
    ) -> Result<(), LispError> {
        let handler_list = self.handler_list_lvalue()?;
        let handler = self
            .current_function()
            .map_err(|error| super::lisp::native_ice(&error))?
            .handler;
        let block = self
            .current_block()
            .map_err(|error| super::lisp::native_ice(&error))?;
        self.context
            .assign(block, handler, self.context.lvalue_as_rvalue(handler_list));
        let next = self
            .context
            .lvalue_as_rvalue(self.context.dereference_field(
                self.context.lvalue_as_rvalue(handler),
                self.types.handler_next,
            ));
        self.context.assign(block, handler_list, next);
        let value = self
            .context
            .lvalue_as_rvalue(self.context.dereference_field(
                self.context.lvalue_as_rvalue(handler),
                self.types.handler_value,
            ));
        self.assign_mvar(interp, env, destination, value)
    }

    fn emit_instruction(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        unit: &super::lisp::UnitData,
        instruction: &Value,
    ) -> Result<(), LispError> {
        let fields = instruction.to_vec()?;
        let (operation, arguments) = fields
            .split_first()
            .ok_or_else(|| super::lisp::native_ice("empty LIMPLE instruction"))?;
        let operation = lisp_symbol(operation)?;
        let argument = |index: usize| {
            arguments
                .get(index)
                .ok_or_else(|| super::lisp::native_ice("LIMPLE instruction is missing an argument"))
        };
        let block = self
            .current_block()
            .map_err(|error| super::lisp::native_ice(&error))?;

        match operation {
            "jump" => {
                let target = self.block(argument(0)?)?;
                self.context.end_with_jump(block, target);
            }
            "cond-jump" => {
                let left_mvar = argument(0)?;
                let right_mvar = argument(1)?;
                let left = self.mvar_rvalue(interp, env, unit, left_mvar)?;
                let right = self.mvar_rvalue(interp, env, unit, right_mvar)?;
                let then_block = self.block(argument(2)?)?;
                let else_block = self.block(argument(3)?)?;
                let left_is_constant_nil =
                    call_lisp_one(interp, env, "comp-cstr-imm-vld-p", left_mvar.clone())?
                        .is_truthy()
                        && call_lisp_one(interp, env, "comp-cstr-imm", left_mvar.clone())?.is_nil();
                let right_is_constant_nil =
                    call_lisp_one(interp, env, "comp-cstr-imm-vld-p", right_mvar.clone())?
                        .is_truthy()
                        && call_lisp_one(interp, env, "comp-cstr-imm", right_mvar.clone())?
                            .is_nil();
                let test = if left_is_constant_nil || right_is_constant_nil {
                    self.base_eq(left, right)
                } else {
                    self.eq(left, right)
                        .map_err(|error| super::lisp::native_ice(&error))?
                };
                self.conditional_jump(block, test, then_block, else_block);
            }
            "cond-jump-narg-leq" => {
                let count = argument(0)?.as_integer()?;
                let count = i32::try_from(count)
                    .map_err(|_| super::lisp::native_ice("native argument count exceeds c_int"))?;
                let function = self
                    .current_function()
                    .map_err(|error| super::lisp::native_ice(&error))?
                    .function;
                let nargs = self
                    .context
                    .param_as_lvalue(self.context.function_param(function, 0));
                let test = self.context.compare(
                    Comparison::LessThanOrEqual,
                    self.context.lvalue_as_rvalue(nargs),
                    self.context.int(self.types.ptrdiff, count),
                );
                self.conditional_jump(
                    block,
                    test,
                    self.block(argument(1)?)?,
                    self.block(argument(2)?)?,
                );
            }
            "phi" | "assume" => {}
            "push-handler" => self.push_handler(interp, env, unit, arguments)?,
            "pop-handler" => self.pop_handler()?,
            "fetch-handler" => self.fetch_handler(interp, env, argument(0)?)?,
            "call" => {
                let value = self.limple_call(interp, env, unit, arguments)?;
                self.context.evaluate(block, value);
            }
            "callref" => {
                let value = self.call_ref(interp, env, unit, arguments, false)?;
                self.context.evaluate(block, value);
            }
            "direct-call" => {
                let value = self.simple_call(interp, env, unit, arguments, true)?;
                self.context.evaluate(block, value);
            }
            "direct-callref" => {
                let value = self.call_ref(interp, env, unit, arguments, true)?;
                self.context.evaluate(block, value);
            }
            "set" => {
                let destination = argument(0)?;
                let expression = argument(1)?;
                let expression_type = super::lisp::call_c_primitive(
                    interp,
                    env,
                    "cl-type-of",
                    std::slice::from_ref(expression),
                )?;
                let value = if expression_type
                    .as_symbol()
                    .is_ok_and(|name| name == "comp-mvar")
                {
                    self.mvar_rvalue(interp, env, unit, expression)?
                } else {
                    let expression = expression.to_vec()?;
                    let (kind, fields) = expression
                        .split_first()
                        .ok_or_else(|| super::lisp::native_ice("empty LIMPLE set expression"))?;
                    match lisp_symbol(kind)? {
                        "call" => self.limple_call(interp, env, unit, fields)?,
                        "callref" => self.call_ref(interp, env, unit, fields, false)?,
                        "direct-call" => self.simple_call(interp, env, unit, fields, true)?,
                        "direct-callref" => self.call_ref(interp, env, unit, fields, true)?,
                        _ => {
                            return Err(super::lisp::native_ice(
                                "inconsistent LIMPLE set expression",
                            ));
                        }
                    }
                };
                self.assign_mvar(interp, env, destination, value)?;
            }
            "set-par-to-local" => {
                let parameter = usize::try_from(argument(1)?.as_integer()?)
                    .map_err(|_| super::lisp::native_ice("negative native parameter index"))?;
                let function = self
                    .current_function()
                    .map_err(|error| super::lisp::native_ice(&error))?
                    .function;
                let value = self
                    .context
                    .param_as_rvalue(self.context.function_param(function, parameter));
                self.assign_mvar(interp, env, argument(0)?, value)?;
            }
            "set-args-to-local" => {
                let function = self
                    .current_function()
                    .map_err(|error| super::lisp::native_ice(&error))?
                    .function;
                let args = self
                    .context
                    .param_as_lvalue(self.context.function_param(function, 1));
                let value = self.context.lvalue_as_rvalue(
                    self.context
                        .dereference(self.context.lvalue_as_rvalue(args)),
                );
                self.assign_mvar(interp, env, argument(0)?, value)?;
            }
            "set-rest-args-to-local" => {
                let slot = call_lisp_one(interp, env, "comp-mvar-slot", argument(0)?.clone())?
                    .as_integer()?;
                let slot = i32::try_from(slot)
                    .map_err(|_| super::lisp::native_ice("native frame slot exceeds c_int"))?;
                let function = self
                    .current_function()
                    .map_err(|error| super::lisp::native_ice(&error))?
                    .function;
                let nargs = self
                    .context
                    .param_as_lvalue(self.context.function_param(function, 0));
                let args = self
                    .context
                    .param_as_lvalue(self.context.function_param(function, 1));
                let remaining = self.binary(
                    BinaryOp::Minus,
                    self.types.ptrdiff,
                    self.context.lvalue_as_rvalue(nargs),
                    self.context.int(self.types.ptrdiff, slot),
                );
                let value = self
                    .runtime_call(
                        "list",
                        &mut [remaining, self.context.lvalue_as_rvalue(args)],
                    )
                    .map_err(|error| super::lisp::native_ice(&error))?;
                self.assign_mvar(interp, env, argument(0)?, value)?;
            }
            "inc-args" => {
                let function = self
                    .current_function()
                    .map_err(|error| super::lisp::native_ice(&error))?
                    .function;
                let args = self
                    .context
                    .param_as_lvalue(self.context.function_param(function, 1));
                let incremented = self.pointer_arithmetic(
                    self.context.lvalue_as_rvalue(args),
                    self.types.lisp_obj_ptr,
                    std::mem::size_of::<usize>(),
                    self.constants.one,
                );
                self.context.assign(block, args, incremented);
            }
            "setimm" => {
                let relocation = unit.relocation(interp, env, argument(1)?)?;
                let value = self
                    .relocated_value(relocation)
                    .map_err(|error| super::lisp::native_ice(&error))?;
                self.assign_mvar(interp, env, argument(0)?, value)?;
            }
            "comment" => {
                let value = argument(0)?;
                let comment = string_like(value)
                    .map(|string| string.text)
                    .ok_or_else(|| LispError::WrongTypeArgument("stringp".into(), value.clone()))?;
                self.add_comment(&comment)?;
            }
            "return" => {
                let value = self.mvar_rvalue(interp, env, unit, argument(0)?)?;
                self.context.end_with_return(block, value);
            }
            "unreachable" => self.context.end_with_return(block, self.nil_value()),
            _ => {
                return Err(super::lisp::native_ice(&format!(
                    "unsupported LIMPLE operation `{operation}`"
                )));
            }
        }
        Ok(())
    }

    fn taggedp(&self, value: *mut RValueOpaque, tag: i64) -> *mut RValueOpaque {
        let shifted = self.binary(
            BinaryOp::RightShift,
            self.types.emacs_int,
            self.coerce(value, self.types.emacs_int),
            self.context.int(self.types.emacs_int, 0),
        );
        let without_tag = self.binary(
            BinaryOp::Minus,
            self.types.unsigned,
            shifted,
            self.context.int(
                self.types.unsigned,
                i32::try_from(tag).expect("Lisp tag fits int"),
            ),
        );
        let tag_bits = self.binary(
            BinaryOp::BitwiseAnd,
            self.types.unsigned,
            without_tag,
            self.context.int(self.types.unsigned, (1 << GCTYPEBITS) - 1),
        );
        self.context
            .unary(UnaryOp::LogicalNegate, self.types.int, tag_bits)
    }

    fn consp(&self, value: *mut RValueOpaque) -> *mut RValueOpaque {
        self.taggedp(value, LISP_CONS_TAG)
    }

    fn vectorlikep(&self, value: *mut RValueOpaque) -> *mut RValueOpaque {
        self.taggedp(value, LISP_VECTORLIKE_TAG)
    }

    fn floatp(&self, value: *mut RValueOpaque) -> *mut RValueOpaque {
        self.taggedp(value, LISP_FLOAT_TAG)
    }

    fn bare_symbol_p(&self, value: *mut RValueOpaque) -> *mut RValueOpaque {
        self.context
            .cast(self.taggedp(value, LISP_SYMBOL_TAG), self.types.bool_)
    }

    fn symbol_with_pos_p(&self, value: *mut RValueOpaque) -> Result<*mut RValueOpaque, String> {
        let pseudovectorp = self
            .type_inliners
            .as_ref()
            .ok_or_else(|| "native PSEUDOVECTORP inliner is not declared".to_string())?
            .pseudovectorp;
        Ok(self.context.call(
            pseudovectorp,
            &mut [
                value,
                self.context.int(self.types.int, PVEC_SYMBOL_WITH_POS),
            ],
        ))
    }

    fn symbol_with_pos_sym(&self, value: *mut RValueOpaque) -> Result<*mut RValueOpaque, String> {
        let function = self
            .object_inliners
            .as_ref()
            .ok_or_else(|| "native SYMBOL_WITH_POS_SYM inliner is not declared".to_string())?
            .symbol_with_pos_sym;
        Ok(self.context.call(function, &mut [value]))
    }

    fn logical_and(&self, left: *mut RValueOpaque, right: *mut RValueOpaque) -> *mut RValueOpaque {
        self.context
            .binary(BinaryOp::LogicalAnd, self.types.bool_, left, right)
    }

    fn logical_or(&self, left: *mut RValueOpaque, right: *mut RValueOpaque) -> *mut RValueOpaque {
        self.context
            .binary(BinaryOp::LogicalOr, self.types.bool_, left, right)
    }

    /// GNU's `EQ` fast path, including symbol-with-position equivalence.
    /// Keep this as backend code: it is the direct Rust port of `comp.c`'s
    /// JIT graph construction, not an implementation of any Lisp frontend
    /// operation.
    fn eq(
        &self,
        left: *mut RValueOpaque,
        right: *mut RValueOpaque,
    ) -> Result<*mut RValueOpaque, String> {
        let direct = self.context.compare(
            Comparison::Equal,
            self.coerce(left, self.types.emacs_int),
            self.coerce(right, self.types.emacs_int),
        );
        let symbols_with_positions_enabled = self.context.lvalue_as_rvalue(
            self.context
                .dereference(self.unit_globals()?.symbols_with_positions_enabled_ref),
        );
        let left_positioned = self.symbol_with_pos_p(left)?;
        let right_positioned = self.symbol_with_pos_p(right)?;
        let both_positioned = self.logical_and(
            right_positioned,
            self.base_eq(
                self.coerce(self.symbol_with_pos_sym(left)?, self.types.emacs_int),
                self.coerce(self.symbol_with_pos_sym(right)?, self.types.emacs_int),
            ),
        );
        let positioned_and_bare = self.logical_and(
            self.bare_symbol_p(right),
            self.base_eq(
                self.coerce(self.symbol_with_pos_sym(left)?, self.types.emacs_int),
                self.coerce(right, self.types.emacs_int),
            ),
        );
        let left_case = self.logical_and(
            left_positioned,
            self.logical_or(both_positioned, positioned_and_bare),
        );
        let right_case = self.logical_and(
            self.bare_symbol_p(left),
            self.logical_and(
                self.symbol_with_pos_p(right)?,
                self.base_eq(
                    self.coerce(left, self.types.emacs_int),
                    self.coerce(self.symbol_with_pos_sym(right)?, self.types.emacs_int),
                ),
            ),
        );
        Ok(self.logical_or(
            direct,
            self.logical_and(
                symbols_with_positions_enabled,
                self.logical_or(left_case, right_case),
            ),
        ))
    }

    fn bignump(&self, value: *mut RValueOpaque) -> Result<*mut RValueOpaque, String> {
        let pseudovectorp = self
            .type_inliners
            .as_ref()
            .ok_or_else(|| "native PSEUDOVECTORP inliner is not declared".to_string())?
            .pseudovectorp;
        Ok(self.context.call(
            pseudovectorp,
            &mut [value, self.context.int(self.types.int, PVEC_BIGNUM)],
        ))
    }

    fn integerp(&self, value: *mut RValueOpaque) -> Result<*mut RValueOpaque, String> {
        Ok(self.binary(
            BinaryOp::LogicalOr,
            self.types.bool_,
            self.fixnump(value),
            self.bignump(value)?,
        ))
    }

    fn numberp(&self, value: *mut RValueOpaque) -> Result<*mut RValueOpaque, String> {
        Ok(self.binary(
            BinaryOp::LogicalOr,
            self.types.bool_,
            self.integerp(value)?,
            self.floatp(value),
        ))
    }

    fn xuntag(
        &self,
        value: *mut RValueOpaque,
        target: *mut TypeOpaque,
        tag: i64,
    ) -> *mut RValueOpaque {
        let pointer = self.coerce(value, self.types.void_ptr);
        let untagged = self.binary(
            BinaryOp::Minus,
            self.types.uintptr,
            pointer,
            self.context.long(
                self.types.lisp_word_tag,
                std::ffi::c_long::try_from(tag).expect("Lisp tag fits c_long"),
            ),
        );
        self.coerce(untagged, self.context.pointer_type(target))
    }

    fn xcons(&self, value: *mut RValueOpaque) -> *mut RValueOpaque {
        self.xuntag(
            value,
            self.context.struct_type(self.types.lisp_cons),
            LISP_CONS_TAG,
        )
    }

    fn xcar_lvalue(&self, cons: *mut RValueOpaque) -> *mut LValueOpaque {
        let union = self
            .context
            .dereference_field(self.xcons(cons), self.types.lisp_cons_u);
        let body = self
            .context
            .lvalue_access_field(union, self.types.lisp_cons_u_s);
        self.context
            .lvalue_access_field(body, self.types.lisp_cons_car)
    }

    fn xcdr_lvalue(&self, cons: *mut RValueOpaque) -> *mut LValueOpaque {
        let union = self
            .context
            .dereference_field(self.xcons(cons), self.types.lisp_cons_u);
        let body = self
            .context
            .lvalue_access_field(union, self.types.lisp_cons_u_s);
        let cdr_union = self
            .context
            .lvalue_access_field(body, self.types.lisp_cons_cdr_union);
        self.context
            .lvalue_access_field(cdr_union, self.types.lisp_cons_cdr)
    }

    fn xcar(&self, cons: *mut RValueOpaque) -> *mut RValueOpaque {
        let union = self.context.lvalue_as_rvalue(
            self.context
                .dereference_field(self.xcons(cons), self.types.lisp_cons_u),
        );
        let body = self
            .context
            .rvalue_access_field(union, self.types.lisp_cons_u_s);
        self.context
            .rvalue_access_field(body, self.types.lisp_cons_car)
    }

    fn xcdr(&self, cons: *mut RValueOpaque) -> *mut RValueOpaque {
        let union = self.context.lvalue_as_rvalue(
            self.context
                .dereference_field(self.xcons(cons), self.types.lisp_cons_u),
        );
        let body = self
            .context
            .rvalue_access_field(union, self.types.lisp_cons_u_s);
        let cdr_union = self
            .context
            .rvalue_access_field(body, self.types.lisp_cons_cdr_union);
        self.context
            .rvalue_access_field(cdr_union, self.types.lisp_cons_cdr)
    }

    fn base_eq(&self, left: *mut RValueOpaque, right: *mut RValueOpaque) -> *mut RValueOpaque {
        self.context.compare(
            Comparison::Equal,
            self.coerce(left, self.types.emacs_int),
            self.coerce(right, self.types.emacs_int),
        )
    }

    fn nilp(&self, value: *mut RValueOpaque) -> *mut RValueOpaque {
        self.base_eq(value, self.nil_value())
    }

    fn conditional_jump(
        &self,
        block: *mut super::gccjit::BlockOpaque,
        test: *mut RValueOpaque,
        then_block: *mut super::gccjit::BlockOpaque,
        else_block: *mut super::gccjit::BlockOpaque,
    ) {
        if self.context.rvalue_type(test) == self.types.bool_ {
            self.context
                .end_with_conditional(block, test, then_block, else_block);
        } else {
            let negated = self
                .context
                .unary(UnaryOp::LogicalNegate, self.types.bool_, test);
            self.context
                .end_with_conditional(block, negated, else_block, then_block);
        }
    }

    fn runtime_call(
        &self,
        name: &str,
        args: &mut [*mut RValueOpaque],
    ) -> Result<*mut RValueOpaque, String> {
        let runtime = self
            .runtime
            .as_ref()
            .ok_or_else(|| "native compiler runtime imports are not declared".to_string())?;
        let field = runtime.field(name)?;
        let table = self
            .current_function
            .as_ref()
            .map(|function| self.context.lvalue_as_rvalue(function.function_relocations))
            .unwrap_or_else(|| self.context.lvalue_as_rvalue(runtime.global));
        let function = self.context.dereference_field(table, field);
        Ok(self
            .context
            .call_through_pointer(self.context.lvalue_as_rvalue(function), args))
    }

    fn runtime_call_ref(
        &self,
        name: &str,
        argument_count: usize,
        first_argument: *mut LValueOpaque,
    ) -> Result<*mut RValueOpaque, String> {
        self.runtime_call(
            name,
            &mut [
                self.context.int(
                    self.types.ptrdiff,
                    i32::try_from(argument_count)
                        .expect("native call argument count exceeds c_int"),
                ),
                self.context.lvalue_address(first_argument),
            ],
        )
    }

    /// Define GNU's paired CAR/CDR substitutes.  This must be invoked first
    /// in the internal-function sequence after the unit relocation globals.
    fn define_car_cdr(&mut self) -> Result<(), String> {
        if self.cons_inliners.is_some() {
            return Err("native CAR/CDR inliners already declared".to_string());
        }
        let mut functions = Vec::with_capacity(2);
        for (index, name) in ["CAR", "CDR"].into_iter().enumerate() {
            let cons = self.context.new_param(self.types.lisp_obj, &c_string("c"));
            let certified = self
                .context
                .new_param(self.types.bool_, &c_string("cert_cons"));
            let function = self.context.new_function(
                FunctionKind::Internal,
                self.types.lisp_obj,
                &c_string(name),
                &mut [cons, certified],
                false,
            );
            let entry = self.context.new_block(function, &c_string("entry_block"));
            let is_cons = self.context.new_block(function, &c_string("is_cons_b"));
            let not_cons = self.context.new_block(function, &c_string("not_a_cons_b"));
            let cons_value = self.context.param_as_rvalue(cons);
            let condition = self.binary(
                BinaryOp::LogicalOr,
                self.types.bool_,
                self.context.param_as_rvalue(certified),
                self.consp(cons_value),
            );
            self.conditional_jump(entry, condition, is_cons, not_cons);
            self.context.end_with_return(
                is_cons,
                if index == 0 {
                    self.xcar(cons_value)
                } else {
                    self.xcdr(cons_value)
                },
            );

            let is_nil = self.context.new_block(function, &c_string("is_nil_b"));
            let not_nil = self.context.new_block(function, &c_string("not_nil_b"));
            self.conditional_jump(not_cons, self.nilp(cons_value), is_nil, not_nil);
            self.context.end_with_return(is_nil, self.nil_value());
            let listp = self.relocated_value(self.unit_globals()?.core_relocations.listp)?;
            self.context.evaluate(
                not_nil,
                self.runtime_call("wrong_type_argument", &mut [listp, cons_value])?,
            );
            self.context.end_with_return(not_nil, self.nil_value());
            functions.push(function);
        }
        self.cons_inliners = Some(ConsInliners {
            car: functions[0],
            cdr: functions[1],
        });
        Ok(())
    }

    fn define_pseudovectorp(&mut self) -> Result<(), String> {
        if self.type_inliners.is_some() {
            return Err("native type inliners already declared".to_string());
        }
        let object = self.context.new_param(self.types.lisp_obj, &c_string("a"));
        let code = self.context.new_param(self.types.int, &c_string("code"));
        let function = self.context.new_function(
            FunctionKind::Internal,
            self.types.bool_,
            &c_string("PSEUDOVECTORP"),
            &mut [object, code],
            false,
        );
        let entry = self.context.new_block(function, &c_string("entry_block"));
        let return_false = self.context.new_block(function, &c_string("ret_false_b"));
        let call_typep = self
            .context
            .new_block(function, &c_string("call_pseudovector_typep_b"));
        self.conditional_jump(
            entry,
            self.vectorlikep(self.context.param_as_rvalue(object)),
            call_typep,
            return_false,
        );
        self.context.end_with_return(
            return_false,
            self.context.int(self.types.bool_, i32::from(false)),
        );
        let result = self.runtime_call(
            "helper_PSEUDOVECTOR_TYPEP_XUNTAG",
            &mut [
                self.context.param_as_rvalue(object),
                self.context.param_as_rvalue(code),
            ],
        )?;
        self.context.end_with_return(call_typep, result);

        let get_object = self.context.new_param(self.types.lisp_obj, &c_string("a"));
        let get_symbol_with_position = self.context.new_function(
            FunctionKind::Internal,
            self.types.symbol_with_position_ptr,
            &c_string("GET_SYMBOL_WITH_POSITION"),
            &mut [get_object],
            false,
        );
        let get_entry = self
            .context
            .new_block(get_symbol_with_position, &c_string("entry_block"));
        let get_result = self.runtime_call(
            "helper_GET_SYMBOL_WITH_POSITION",
            &mut [self.context.param_as_rvalue(get_object)],
        )?;
        self.context.end_with_return(get_entry, get_result);

        let ok = self.context.new_param(self.types.int, &c_string("ok"));
        let predicate = self
            .context
            .new_param(self.types.lisp_obj, &c_string("predicate"));
        let value = self.context.new_param(self.types.lisp_obj, &c_string("x"));
        let check_type = self.context.new_function(
            FunctionKind::Internal,
            self.types.void,
            &c_string("CHECK_TYPE"),
            &mut [ok, predicate, value],
            false,
        );
        let check_entry = self.context.new_block(check_type, &c_string("entry_block"));
        let ok_block = self.context.new_block(check_type, &c_string("ok_block"));
        let not_ok_block = self
            .context
            .new_block(check_type, &c_string("not_ok_block"));
        self.conditional_jump(
            check_entry,
            self.context.param_as_rvalue(ok),
            ok_block,
            not_ok_block,
        );
        self.context.end_with_void_return(ok_block);
        let error = self.runtime_call(
            "wrong_type_argument",
            &mut [
                self.context.param_as_rvalue(predicate),
                self.context.param_as_rvalue(value),
            ],
        )?;
        self.context.evaluate(not_ok_block, error);
        self.context.end_with_void_return(not_ok_block);

        self.type_inliners = Some(TypeInliners {
            pseudovectorp: function,
            get_symbol_with_position,
            check_type,
        });
        Ok(())
    }

    fn define_object_inliners(&mut self) -> Result<(), String> {
        if self.object_inliners.is_some() {
            return Err("native object inliners already declared".to_string());
        }
        let types = self
            .type_inliners
            .as_ref()
            .ok_or_else(|| "native type inliners are not declared".to_string())?;

        let object = self.context.new_param(self.types.lisp_obj, &c_string("a"));
        let symbol_with_pos_sym = self.context.new_function(
            FunctionKind::Internal,
            self.types.lisp_obj,
            &c_string("SYMBOL_WITH_POS_SYM"),
            &mut [object],
            false,
        );
        let entry = self
            .context
            .new_block(symbol_with_pos_sym, &c_string("entry_block"));
        let is_symbol_with_pos = self.context.call(
            types.pseudovectorp,
            &mut [
                self.context.param_as_rvalue(object),
                self.context.int(self.types.int, PVEC_SYMBOL_WITH_POS),
            ],
        );
        let predicate =
            self.relocated_value(self.unit_globals()?.core_relocations.symbol_with_pos_p)?;
        let check = self.context.call(
            types.check_type,
            &mut [
                self.context.cast(is_symbol_with_pos, self.types.int),
                predicate,
                self.context.param_as_rvalue(object),
            ],
        );
        self.context.evaluate(entry, check);
        let untagged = self.context.call(
            types.get_symbol_with_position,
            &mut [self.context.param_as_rvalue(object)],
        );
        let structure = self
            .context
            .lvalue_as_rvalue(self.context.dereference(untagged));
        let symbol = self
            .context
            .rvalue_access_field(structure, self.types.symbol_with_position_symbol);
        self.context.end_with_return(entry, symbol);

        let impure_object = self
            .context
            .new_param(self.types.lisp_obj, &c_string("obj"));
        let impure_pointer = self
            .context
            .new_param(self.types.void_ptr, &c_string("ptr"));
        let check_impure = self.context.new_function(
            FunctionKind::Internal,
            self.types.void,
            &c_string("CHECK_IMPURE"),
            &mut [impure_object, impure_pointer],
            false,
        );
        let impure_entry = self
            .context
            .new_block(check_impure, &c_string("entry_block"));
        let error_block = self.context.new_block(check_impure, &c_string("err_block"));
        let impure_ok = self.context.new_block(check_impure, &c_string("ok_block"));
        let pure = self.unit_globals()?.pure;
        let offset = self.binary(
            BinaryOp::Minus,
            self.types.uintptr,
            self.context.param_as_rvalue(impure_object),
            pure,
        );
        let is_pure = self.context.compare(
            Comparison::LessThanOrEqual,
            offset,
            self.context.int(
                self.types.uintptr,
                i32::try_from(super::abi::PURESIZE).expect("PURESIZE fits int"),
            ),
        );
        self.conditional_jump(impure_entry, is_pure, error_block, impure_ok);
        self.context.end_with_void_return(impure_ok);
        let pure_error = self.runtime_call(
            "pure_write_error",
            &mut [self.context.param_as_rvalue(impure_object)],
        )?;
        self.context.evaluate(error_block, pure_error);
        self.context.end_with_void_return(error_block);

        let bool_param = self.context.new_param(self.types.bool_, &c_string("x"));
        let bool_to_lisp_obj = self.context.new_function(
            FunctionKind::Internal,
            self.types.lisp_obj,
            &c_string("bool_to_lisp_obj"),
            &mut [bool_param],
            false,
        );
        let bool_entry = self
            .context
            .new_block(bool_to_lisp_obj, &c_string("entry_block"));
        let return_t = self
            .context
            .new_block(bool_to_lisp_obj, &c_string("ret_t_block"));
        let return_nil = self
            .context
            .new_block(bool_to_lisp_obj, &c_string("ret_nil_block"));
        self.conditional_jump(
            bool_entry,
            self.context.param_as_rvalue(bool_param),
            return_t,
            return_nil,
        );
        self.context.end_with_return(
            return_t,
            self.relocated_value(self.unit_globals()?.core_relocations.t)?,
        );
        self.context.end_with_return(return_nil, self.nil_value());

        let mut setters = Vec::with_capacity(2);
        for (index, (name, parameter_name)) in [("setcar", "new_car"), ("setcdr", "new_cdr")]
            .into_iter()
            .enumerate()
        {
            let cell = self
                .context
                .new_param(self.types.lisp_obj, &c_string("cell"));
            let new_element = self
                .context
                .new_param(self.types.lisp_obj, &c_string(parameter_name));
            let certified = self
                .context
                .new_param(self.types.bool_, &c_string("cert_cons"));
            let function = self.context.new_function(
                FunctionKind::Internal,
                self.types.lisp_obj,
                &c_string(name),
                &mut [cell, new_element, certified],
                false,
            );
            let setter_entry = self.context.new_block(function, &c_string("entry_block"));
            let cell_value = self.context.param_as_rvalue(cell);
            let is_cons = self.consp(cell_value);
            let predicate = self.relocated_value(self.unit_globals()?.core_relocations.consp)?;
            let checked = self
                .context
                .call(types.check_type, &mut [is_cons, predicate, cell_value]);
            self.context.evaluate(setter_entry, checked);
            let impure = self
                .context
                .call(check_impure, &mut [cell_value, self.xcons(cell_value)]);
            self.context.evaluate(setter_entry, impure);
            self.context.assign(
                setter_entry,
                if index == 0 {
                    self.xcar_lvalue(cell_value)
                } else {
                    self.xcdr_lvalue(cell_value)
                },
                self.context.param_as_rvalue(new_element),
            );
            self.context
                .end_with_return(setter_entry, self.context.param_as_rvalue(new_element));
            setters.push(function);
        }

        self.object_inliners = Some(ObjectInliners {
            symbol_with_pos_sym,
            bool_to_lisp_obj,
            setcar: setters[0],
            setcdr: setters[1],
        });
        Ok(())
    }

    /// Define GNU `comp.c`'s paired `add1`/`sub1` helpers in the same order.
    fn build_add1_sub1(&self) -> Result<(*mut FunctionOpaque, *mut FunctionOpaque), String> {
        let mut functions = Vec::with_capacity(2);
        for (name, fallback, operation, limit) in [
            ("add1", "1+", BinaryOp::Plus, MOST_POSITIVE_FIXNUM),
            ("sub1", "1-", BinaryOp::Minus, MOST_NEGATIVE_FIXNUM),
        ] {
            let value_param = self.context.new_param(self.types.lisp_obj, &c_string("n"));
            let certified_param = self
                .context
                .new_param(self.types.bool_, &c_string("cert_fixnum"));
            let function = self.context.new_function(
                FunctionKind::Internal,
                self.types.lisp_obj,
                &c_string(name),
                &mut [value_param, certified_param],
                false,
            );
            let entry = self.context.new_block(function, &c_string("entry_block"));
            let inline = self.context.new_block(function, &c_string("inline_block"));
            let fallback_block = self.context.new_block(function, &c_string("fcall_block"));

            let value = self.context.param_as_rvalue(value_param);
            let untagged = self.xfixnum(value);
            let known_fixnum = self.binary(
                BinaryOp::LogicalOr,
                self.types.bool_,
                self.context.param_as_rvalue(certified_param),
                self.fixnump(value),
            );
            let not_at_limit = self.context.compare(
                Comparison::NotEqual,
                untagged,
                self.context.long(self.types.emacs_int, limit),
            );
            let can_inline = self.binary(
                BinaryOp::LogicalAnd,
                self.types.bool_,
                known_fixnum,
                not_at_limit,
            );
            self.conditional_jump(entry, can_inline, inline, fallback_block);

            let result = self.binary(
                operation,
                self.types.emacs_int,
                untagged,
                self.constants.one,
            );
            self.context
                .end_with_return(inline, self.make_fixnum(result));
            let fallback_result = self.runtime_call(fallback, &mut [value])?;
            self.context
                .end_with_return(fallback_block, fallback_result);
            functions.push(function);
        }
        Ok((functions[0], functions[1]))
    }

    fn define_numeric_inliners(&mut self) -> Result<(), String> {
        if self.numeric_inliners.is_some() {
            return Err("native numeric inliners already declared".to_string());
        }
        let (add1, sub1) = self.build_add1_sub1()?;

        let value = self.context.new_param(self.types.lisp_obj, &c_string("n"));
        let certified = self
            .context
            .new_param(self.types.bool_, &c_string("cert_fixnum"));
        let negate = self.context.new_function(
            FunctionKind::Internal,
            self.types.lisp_obj,
            &c_string("negate"),
            &mut [value, certified],
            false,
        );
        let entry = self.context.new_block(negate, &c_string("entry_block"));
        let inline = self.context.new_block(negate, &c_string("inline_block"));
        let fallback = self.context.new_block(negate, &c_string("fcall_block"));
        let value_lvalue = self.context.param_as_lvalue(value);
        let value_rvalue = self.context.lvalue_as_rvalue(value_lvalue);
        let untagged = self.xfixnum(value_rvalue);
        let known_fixnum = self.binary(
            BinaryOp::LogicalOr,
            self.types.bool_,
            self.context.param_as_rvalue(certified),
            self.fixnump(self.context.lvalue_as_rvalue(value_lvalue)),
        );
        let not_minimum = self.context.compare(
            Comparison::NotEqual,
            untagged,
            self.context
                .long(self.types.emacs_int, MOST_NEGATIVE_FIXNUM),
        );
        let can_inline = self.binary(
            BinaryOp::LogicalAnd,
            self.types.bool_,
            known_fixnum,
            not_minimum,
        );
        self.conditional_jump(entry, can_inline, inline, fallback);
        let negated = self
            .context
            .unary(UnaryOp::Minus, self.types.emacs_int, untagged);
        self.context
            .end_with_return(inline, self.make_fixnum(negated));
        let fallback_result = self.runtime_call_ref("-", 1, value_lvalue)?;
        self.context.end_with_return(fallback, fallback_result);

        let quitcounter = self.context.new_global(
            GlobalKind::Internal,
            self.types.unsigned,
            &c_string("quitcounter"),
        );
        let maybe_gc_or_quit = self.context.new_function(
            FunctionKind::Internal,
            self.types.void,
            &c_string("maybe_gc_quit"),
            &mut [],
            false,
        );
        let increment = self
            .context
            .new_block(maybe_gc_or_quit, &c_string("increment_block"));
        let maybe_do_it = self
            .context
            .new_block(maybe_gc_or_quit, &c_string("maybe_do_it_block"));
        let pass = self
            .context
            .new_block(maybe_gc_or_quit, &c_string("pass_block"));
        self.context.assign(
            increment,
            quitcounter,
            self.binary(
                BinaryOp::Plus,
                self.types.unsigned,
                self.context.lvalue_as_rvalue(quitcounter),
                self.context.int(self.types.unsigned, 1),
            ),
        );
        let should_run = self.binary(
            BinaryOp::RightShift,
            self.types.unsigned,
            self.context.lvalue_as_rvalue(quitcounter),
            self.context.int(self.types.unsigned, 9),
        );
        self.conditional_jump(increment, should_run, maybe_do_it, pass);
        self.context.assign(
            maybe_do_it,
            quitcounter,
            self.context.int(self.types.unsigned, 0),
        );
        let maybe_gc = self.runtime_call("maybe_gc", &mut [])?;
        self.context.evaluate(maybe_do_it, maybe_gc);
        let maybe_quit = self.runtime_call("maybe_quit", &mut [])?;
        self.context.evaluate(maybe_do_it, maybe_quit);
        self.context.end_with_void_return(maybe_do_it);
        self.context.end_with_void_return(pass);

        self.numeric_inliners = Some(NumericInliners {
            add1,
            sub1,
            negate,
            maybe_gc_or_quit,
        });
        Ok(())
    }
}

fn function_field(
    context: &Context,
    c_name: &str,
    return_type: *mut TypeOpaque,
    params: &[*mut TypeOpaque],
) -> *mut FieldOpaque {
    let mut params = params.to_vec();
    let function_pointer = context.new_function_pointer_type(return_type, &mut params, false);
    let const_function_pointer = context.const_type(function_pointer);
    context.new_field(const_function_pointer, &c_string(c_name))
}

fn emit_static_object(context: &Context, char_type: *mut TypeOpaque, name: &str, printed: &[u8]) {
    let string_size = printed.len() + 1;
    let mut blob = Vec::with_capacity(std::mem::size_of::<isize>() + string_size);
    let string_size =
        isize::try_from(string_size).expect("native compiler static object exceeds ptrdiff_t");
    blob.extend_from_slice(&string_size.to_ne_bytes());
    blob.extend_from_slice(printed);
    blob.push(0);
    let global = context.new_global(
        GlobalKind::Exported,
        context.new_array_type(char_type, blob.len()),
        &c_string(&format!("{name}_blob")),
    );
    context.initialize_global(global, &blob);
}

fn c_string(value: &str) -> CString {
    CString::new(value).expect("native compiler symbol names cannot contain NUL")
}

fn call_lisp_one(
    interp: &mut Interpreter,
    env: &mut Env,
    name: &str,
    argument: Value,
) -> Result<Value, LispError> {
    super::lisp::call(interp, env, name, &[argument])
}

fn lisp_string(value: Value) -> Result<String, LispError> {
    string_like(&value)
        .map(|string| string.text)
        .ok_or_else(|| LispError::WrongTypeArgument("stringp".into(), value))
}

fn lisp_symbol(value: &Value) -> Result<&str, LispError> {
    value.as_symbol()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn acquires_the_comp_c_backend_context() {
        let compiler = Compiler::acquire().expect("acquire native compiler");
        // The backend reports the version of the libgccjit it actually
        // loaded; the pinned toolchain differs per reference build.
        assert_eq!(
            Compiler::libgccjit_version(),
            Some(super::super::gccjit::api().expect("libgccjit").version())
        );
        assert!(compiler.context.first_error().is_none());
    }
}
