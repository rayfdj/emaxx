//! Typed ownership for the dynamically loaded libgccjit C API.

use libloading::Library;
use std::ffi::{CStr, c_char, c_int, c_long, c_void};
use std::ptr;
use std::sync::OnceLock;

#[repr(C)]
pub(crate) struct ContextOpaque {
    _private: [u8; 0],
}

#[repr(C)]
#[cfg(test)]
pub(crate) struct ResultOpaque {
    _private: [u8; 0],
}

#[repr(C)]
pub(crate) struct TypeOpaque {
    _private: [u8; 0],
}

#[repr(C)]
pub(crate) struct FieldOpaque {
    _private: [u8; 0],
}

#[repr(C)]
pub(crate) struct StructOpaque {
    _private: [u8; 0],
}

#[repr(C)]
pub(crate) struct FunctionOpaque {
    _private: [u8; 0],
}

#[repr(C)]
pub(crate) struct BlockOpaque {
    _private: [u8; 0],
}

#[repr(C)]
pub(crate) struct ParamOpaque {
    _private: [u8; 0],
}

#[repr(C)]
pub(crate) struct RValueOpaque {
    _private: [u8; 0],
}

#[repr(C)]
pub(crate) struct LValueOpaque {
    _private: [u8; 0],
}

type Acquire = unsafe extern "C" fn() -> *mut ContextOpaque;
type Release = unsafe extern "C" fn(*mut ContextOpaque);
type Version = unsafe extern "C" fn() -> c_int;
type GetType = unsafe extern "C" fn(*mut ContextOpaque, c_int) -> *mut TypeOpaque;
type GetIntType = unsafe extern "C" fn(*mut ContextOpaque, c_int, c_int) -> *mut TypeOpaque;
type TypeGetPointer = unsafe extern "C" fn(*mut TypeOpaque) -> *mut TypeOpaque;
type TypeGetConst = unsafe extern "C" fn(*mut TypeOpaque) -> *mut TypeOpaque;
type TypeIsPointer = unsafe extern "C" fn(*mut TypeOpaque) -> *mut TypeOpaque;
type NewOpaqueStruct =
    unsafe extern "C" fn(*mut ContextOpaque, *mut c_void, *const c_char) -> *mut StructOpaque;
type StructAsType = unsafe extern "C" fn(*mut StructOpaque) -> *mut TypeOpaque;
type NewField = unsafe extern "C" fn(
    *mut ContextOpaque,
    *mut c_void,
    *mut TypeOpaque,
    *const c_char,
) -> *mut FieldOpaque;
type NewStructType = unsafe extern "C" fn(
    *mut ContextOpaque,
    *mut c_void,
    *const c_char,
    c_int,
    *mut *mut FieldOpaque,
) -> *mut StructOpaque;
type NewUnionType = unsafe extern "C" fn(
    *mut ContextOpaque,
    *mut c_void,
    *const c_char,
    c_int,
    *mut *mut FieldOpaque,
) -> *mut TypeOpaque;
type NewArrayType = unsafe extern "C" fn(
    *mut ContextOpaque,
    *mut c_void,
    *mut TypeOpaque,
    c_int,
) -> *mut TypeOpaque;
type StructSetFields =
    unsafe extern "C" fn(*mut StructOpaque, *mut c_void, c_int, *mut *mut FieldOpaque);
type NewFunctionPtrType = unsafe extern "C" fn(
    *mut ContextOpaque,
    *mut c_void,
    *mut TypeOpaque,
    c_int,
    *mut *mut TypeOpaque,
    c_int,
) -> *mut TypeOpaque;
type NewParam = unsafe extern "C" fn(
    *mut ContextOpaque,
    *mut c_void,
    *mut TypeOpaque,
    *const c_char,
) -> *mut ParamOpaque;
type NewFunction = unsafe extern "C" fn(
    *mut ContextOpaque,
    *mut c_void,
    c_int,
    *mut TypeOpaque,
    *const c_char,
    c_int,
    *mut *mut ParamOpaque,
    c_int,
) -> *mut FunctionOpaque;
type FunctionGetParam = unsafe extern "C" fn(*mut FunctionOpaque, c_int) -> *mut ParamOpaque;
type NewBlock = unsafe extern "C" fn(*mut FunctionOpaque, *const c_char) -> *mut BlockOpaque;
type ParamAsRValue = unsafe extern "C" fn(*mut ParamOpaque) -> *mut RValueOpaque;
type ParamAsLValue = unsafe extern "C" fn(*mut ParamOpaque) -> *mut LValueOpaque;
type NewGlobal = unsafe extern "C" fn(
    *mut ContextOpaque,
    *mut c_void,
    c_int,
    *mut TypeOpaque,
    *const c_char,
) -> *mut LValueOpaque;
type GlobalSetInitializer =
    unsafe extern "C" fn(*mut LValueOpaque, *const c_void, usize) -> *mut LValueOpaque;
type LValueAsRValue = unsafe extern "C" fn(*mut LValueOpaque) -> *mut RValueOpaque;
type LValueAccessField =
    unsafe extern "C" fn(*mut LValueOpaque, *mut c_void, *mut FieldOpaque) -> *mut LValueOpaque;
type LValueGetAddress = unsafe extern "C" fn(*mut LValueOpaque, *mut c_void) -> *mut RValueOpaque;
type RValueAccessField =
    unsafe extern "C" fn(*mut RValueOpaque, *mut c_void, *mut FieldOpaque) -> *mut RValueOpaque;
type RValueDereference = unsafe extern "C" fn(*mut RValueOpaque, *mut c_void) -> *mut LValueOpaque;
type RValueDereferenceField =
    unsafe extern "C" fn(*mut RValueOpaque, *mut c_void, *mut FieldOpaque) -> *mut LValueOpaque;
type RValueGetType = unsafe extern "C" fn(*mut RValueOpaque) -> *mut TypeOpaque;
type NewArrayAccess = unsafe extern "C" fn(
    *mut ContextOpaque,
    *mut c_void,
    *mut RValueOpaque,
    *mut RValueOpaque,
) -> *mut LValueOpaque;
type NewInt = unsafe extern "C" fn(*mut ContextOpaque, *mut TypeOpaque, c_int) -> *mut RValueOpaque;
type NewLong =
    unsafe extern "C" fn(*mut ContextOpaque, *mut TypeOpaque, c_long) -> *mut RValueOpaque;
type NewPointer =
    unsafe extern "C" fn(*mut ContextOpaque, *mut TypeOpaque, *mut c_void) -> *mut RValueOpaque;
type NewUnaryOp = unsafe extern "C" fn(
    *mut ContextOpaque,
    *mut c_void,
    c_int,
    *mut TypeOpaque,
    *mut RValueOpaque,
) -> *mut RValueOpaque;
type NewBinaryOp = unsafe extern "C" fn(
    *mut ContextOpaque,
    *mut c_void,
    c_int,
    *mut TypeOpaque,
    *mut RValueOpaque,
    *mut RValueOpaque,
) -> *mut RValueOpaque;
type NewComparison = unsafe extern "C" fn(
    *mut ContextOpaque,
    *mut c_void,
    c_int,
    *mut RValueOpaque,
    *mut RValueOpaque,
) -> *mut RValueOpaque;
type NewCast = unsafe extern "C" fn(
    *mut ContextOpaque,
    *mut c_void,
    *mut RValueOpaque,
    *mut TypeOpaque,
) -> *mut RValueOpaque;
type NewCall = unsafe extern "C" fn(
    *mut ContextOpaque,
    *mut c_void,
    *mut FunctionOpaque,
    c_int,
    *mut *mut RValueOpaque,
) -> *mut RValueOpaque;
type NewCallThroughPtr = unsafe extern "C" fn(
    *mut ContextOpaque,
    *mut c_void,
    *mut RValueOpaque,
    c_int,
    *mut *mut RValueOpaque,
) -> *mut RValueOpaque;
type FunctionNewLocal = unsafe extern "C" fn(
    *mut FunctionOpaque,
    *mut c_void,
    *mut TypeOpaque,
    *const c_char,
) -> *mut LValueOpaque;
type BlockAddAssignment =
    unsafe extern "C" fn(*mut BlockOpaque, *mut c_void, *mut LValueOpaque, *mut RValueOpaque);
type BlockAddEval = unsafe extern "C" fn(*mut BlockOpaque, *mut c_void, *mut RValueOpaque);
type BlockAddComment = unsafe extern "C" fn(*mut BlockOpaque, *mut c_void, *const c_char);
type EndWithConditional = unsafe extern "C" fn(
    *mut BlockOpaque,
    *mut c_void,
    *mut RValueOpaque,
    *mut BlockOpaque,
    *mut BlockOpaque,
);
type EndWithJump = unsafe extern "C" fn(*mut BlockOpaque, *mut c_void, *mut BlockOpaque);
type EndWithReturn = unsafe extern "C" fn(*mut BlockOpaque, *mut c_void, *mut RValueOpaque);
type EndWithVoidReturn = unsafe extern "C" fn(*mut BlockOpaque, *mut c_void);
#[cfg(test)]
type Compile = unsafe extern "C" fn(*mut ContextOpaque) -> *mut ResultOpaque;
type CompileToFile = unsafe extern "C" fn(*mut ContextOpaque, c_int, *const c_char);
type SetIntOption = unsafe extern "C" fn(*mut ContextOpaque, c_int, c_int);
type SetBoolOption = unsafe extern "C" fn(*mut ContextOpaque, c_int, c_int);
type AddOption = unsafe extern "C" fn(*mut ContextOpaque, *const c_char);
type DumpToFile = unsafe extern "C" fn(*mut ContextOpaque, *const c_char, c_int);
type DumpReproducer = unsafe extern "C" fn(*mut ContextOpaque, *const c_char);
#[cfg(test)]
type ResultGetCode = unsafe extern "C" fn(*mut ResultOpaque, *const c_char) -> *mut c_void;
#[cfg(test)]
type ResultRelease = unsafe extern "C" fn(*mut ResultOpaque);
type GetFirstError = unsafe extern "C" fn(*mut ContextOpaque) -> *const c_char;

#[derive(Clone, Copy)]
#[repr(i32)]
pub(crate) enum TypeKind {
    Void = 0,
    VoidPtr = 1,
    Bool = 2,
    Char = 3,
    Int = 8,
    UnsignedInt = 9,
    Long = 10,
    UnsignedLong = 11,
    LongLong = 12,
    UnsignedLongLong = 13,
}

#[derive(Clone, Copy)]
#[repr(i32)]
pub(crate) enum FunctionKind {
    Exported = 0,
    Internal = 1,
    Imported = 2,
}

#[derive(Clone, Copy)]
#[repr(i32)]
pub(crate) enum GlobalKind {
    Exported = 0,
    Internal = 1,
}

#[derive(Clone, Copy)]
#[repr(i32)]
pub(crate) enum UnaryOp {
    Minus = 0,
    LogicalNegate = 2,
}

#[derive(Clone, Copy)]
#[repr(i32)]
pub(crate) enum BinaryOp {
    Plus = 0,
    Minus = 1,
    Multiply = 2,
    BitwiseAnd = 5,
    LogicalAnd = 8,
    LogicalOr = 9,
    LeftShift = 10,
    RightShift = 11,
}

#[derive(Clone, Copy)]
#[repr(i32)]
pub(crate) enum Comparison {
    Equal = 0,
    NotEqual = 1,
    LessThanOrEqual = 3,
}

#[derive(Clone, Copy)]
#[repr(i32)]
pub(crate) enum OutputKind {
    DynamicLibrary = 2,
}

#[derive(Clone, Copy)]
#[repr(i32)]
pub(crate) enum BoolOption {
    DebugInfo = 0,
    DumpEverything = 5,
    KeepIntermediates = 7,
}

pub(crate) struct Api {
    _library: Library,
    acquire: Acquire,
    release: Release,
    version_major: Version,
    version_minor: Version,
    version_patchlevel: Version,
    get_type: GetType,
    get_int_type: GetIntType,
    type_get_pointer: TypeGetPointer,
    type_get_const: TypeGetConst,
    type_is_pointer: TypeIsPointer,
    new_opaque_struct: NewOpaqueStruct,
    struct_as_type: StructAsType,
    new_field: NewField,
    new_struct_type: NewStructType,
    new_union_type: NewUnionType,
    new_array_type: NewArrayType,
    struct_set_fields: StructSetFields,
    new_function_ptr_type: NewFunctionPtrType,
    new_param: NewParam,
    new_function: NewFunction,
    function_get_param: FunctionGetParam,
    new_block: NewBlock,
    param_as_rvalue: ParamAsRValue,
    param_as_lvalue: ParamAsLValue,
    new_global: NewGlobal,
    global_set_initializer: GlobalSetInitializer,
    lvalue_as_rvalue: LValueAsRValue,
    lvalue_access_field: LValueAccessField,
    lvalue_get_address: LValueGetAddress,
    rvalue_access_field: RValueAccessField,
    rvalue_dereference: RValueDereference,
    rvalue_dereference_field: RValueDereferenceField,
    rvalue_get_type: RValueGetType,
    new_array_access: NewArrayAccess,
    new_int: NewInt,
    new_long: NewLong,
    new_pointer: NewPointer,
    new_unary_op: NewUnaryOp,
    new_binary_op: NewBinaryOp,
    new_comparison: NewComparison,
    new_cast: NewCast,
    new_bitcast: NewCast,
    new_call: NewCall,
    new_call_through_ptr: NewCallThroughPtr,
    function_new_local: FunctionNewLocal,
    block_add_assignment: BlockAddAssignment,
    block_add_eval: BlockAddEval,
    block_add_comment: BlockAddComment,
    end_with_conditional: EndWithConditional,
    end_with_jump: EndWithJump,
    end_with_return: EndWithReturn,
    end_with_void_return: EndWithVoidReturn,
    #[cfg(test)]
    compile: Compile,
    compile_to_file: CompileToFile,
    set_int_option: SetIntOption,
    set_bool_option: SetBoolOption,
    add_command_line_option: AddOption,
    add_driver_option: AddOption,
    dump_to_file: DumpToFile,
    dump_reproducer: DumpReproducer,
    #[cfg(test)]
    result_get_code: ResultGetCode,
    #[cfg(test)]
    result_release: ResultRelease,
    get_first_error: GetFirstError,
}

impl Api {
    fn load() -> Result<Self, String> {
        let mut errors = Vec::new();
        for candidate in candidates() {
            // SAFETY: Loading libgccjit by its platform library name is the
            // intended API.  Every symbol below is copied while the Library
            // is retained in `Api`, so no pointer outlives its owner.
            let library = match unsafe { Library::new(candidate) } {
                Ok(library) => library,
                Err(error) => {
                    errors.push(format!("{candidate}: {error}"));
                    continue;
                }
            };
            // SAFETY: These names and signatures come directly from the
            // installed libgccjit.h.  A missing symbol rejects this library.
            unsafe {
                macro_rules! symbol {
                    ($name:literal, $type:ty) => {
                        *library
                            .get::<$type>(concat!($name, "\0").as_bytes())
                            .map_err(|error| format!("{candidate}: {}: {error}", $name))?
                    };
                }
                return Ok(Self {
                    acquire: symbol!("gcc_jit_context_acquire", Acquire),
                    release: symbol!("gcc_jit_context_release", Release),
                    version_major: symbol!("gcc_jit_version_major", Version),
                    version_minor: symbol!("gcc_jit_version_minor", Version),
                    version_patchlevel: symbol!("gcc_jit_version_patchlevel", Version),
                    get_type: symbol!("gcc_jit_context_get_type", GetType),
                    get_int_type: symbol!("gcc_jit_context_get_int_type", GetIntType),
                    type_get_pointer: symbol!("gcc_jit_type_get_pointer", TypeGetPointer),
                    type_get_const: symbol!("gcc_jit_type_get_const", TypeGetConst),
                    type_is_pointer: symbol!("gcc_jit_type_is_pointer", TypeIsPointer),
                    new_opaque_struct: symbol!(
                        "gcc_jit_context_new_opaque_struct",
                        NewOpaqueStruct
                    ),
                    struct_as_type: symbol!("gcc_jit_struct_as_type", StructAsType),
                    new_field: symbol!("gcc_jit_context_new_field", NewField),
                    new_struct_type: symbol!("gcc_jit_context_new_struct_type", NewStructType),
                    new_union_type: symbol!("gcc_jit_context_new_union_type", NewUnionType),
                    new_array_type: symbol!("gcc_jit_context_new_array_type", NewArrayType),
                    struct_set_fields: symbol!("gcc_jit_struct_set_fields", StructSetFields),
                    new_function_ptr_type: symbol!(
                        "gcc_jit_context_new_function_ptr_type",
                        NewFunctionPtrType
                    ),
                    new_param: symbol!("gcc_jit_context_new_param", NewParam),
                    new_function: symbol!("gcc_jit_context_new_function", NewFunction),
                    function_get_param: symbol!("gcc_jit_function_get_param", FunctionGetParam),
                    new_block: symbol!("gcc_jit_function_new_block", NewBlock),
                    param_as_rvalue: symbol!("gcc_jit_param_as_rvalue", ParamAsRValue),
                    param_as_lvalue: symbol!("gcc_jit_param_as_lvalue", ParamAsLValue),
                    new_global: symbol!("gcc_jit_context_new_global", NewGlobal),
                    global_set_initializer: symbol!(
                        "gcc_jit_global_set_initializer",
                        GlobalSetInitializer
                    ),
                    lvalue_as_rvalue: symbol!("gcc_jit_lvalue_as_rvalue", LValueAsRValue),
                    lvalue_access_field: symbol!("gcc_jit_lvalue_access_field", LValueAccessField),
                    lvalue_get_address: symbol!("gcc_jit_lvalue_get_address", LValueGetAddress),
                    rvalue_access_field: symbol!("gcc_jit_rvalue_access_field", RValueAccessField),
                    rvalue_dereference: symbol!("gcc_jit_rvalue_dereference", RValueDereference),
                    rvalue_dereference_field: symbol!(
                        "gcc_jit_rvalue_dereference_field",
                        RValueDereferenceField
                    ),
                    rvalue_get_type: symbol!("gcc_jit_rvalue_get_type", RValueGetType),
                    new_array_access: symbol!("gcc_jit_context_new_array_access", NewArrayAccess),
                    new_int: symbol!("gcc_jit_context_new_rvalue_from_int", NewInt),
                    new_long: symbol!("gcc_jit_context_new_rvalue_from_long", NewLong),
                    new_pointer: symbol!("gcc_jit_context_new_rvalue_from_ptr", NewPointer),
                    new_unary_op: symbol!("gcc_jit_context_new_unary_op", NewUnaryOp),
                    new_binary_op: symbol!("gcc_jit_context_new_binary_op", NewBinaryOp),
                    new_comparison: symbol!("gcc_jit_context_new_comparison", NewComparison),
                    new_cast: symbol!("gcc_jit_context_new_cast", NewCast),
                    new_bitcast: symbol!("gcc_jit_context_new_bitcast", NewCast),
                    new_call: symbol!("gcc_jit_context_new_call", NewCall),
                    new_call_through_ptr: symbol!(
                        "gcc_jit_context_new_call_through_ptr",
                        NewCallThroughPtr
                    ),
                    function_new_local: symbol!("gcc_jit_function_new_local", FunctionNewLocal),
                    block_add_assignment: symbol!(
                        "gcc_jit_block_add_assignment",
                        BlockAddAssignment
                    ),
                    block_add_eval: symbol!("gcc_jit_block_add_eval", BlockAddEval),
                    block_add_comment: symbol!("gcc_jit_block_add_comment", BlockAddComment),
                    end_with_conditional: symbol!(
                        "gcc_jit_block_end_with_conditional",
                        EndWithConditional
                    ),
                    end_with_jump: symbol!("gcc_jit_block_end_with_jump", EndWithJump),
                    end_with_return: symbol!("gcc_jit_block_end_with_return", EndWithReturn),
                    end_with_void_return: symbol!(
                        "gcc_jit_block_end_with_void_return",
                        EndWithVoidReturn
                    ),
                    #[cfg(test)]
                    compile: symbol!("gcc_jit_context_compile", Compile),
                    compile_to_file: symbol!("gcc_jit_context_compile_to_file", CompileToFile),
                    set_int_option: symbol!("gcc_jit_context_set_int_option", SetIntOption),
                    set_bool_option: symbol!("gcc_jit_context_set_bool_option", SetBoolOption),
                    add_command_line_option: symbol!(
                        "gcc_jit_context_add_command_line_option",
                        AddOption
                    ),
                    add_driver_option: symbol!("gcc_jit_context_add_driver_option", AddOption),
                    dump_to_file: symbol!("gcc_jit_context_dump_to_file", DumpToFile),
                    dump_reproducer: symbol!(
                        "gcc_jit_context_dump_reproducer_to_file",
                        DumpReproducer
                    ),
                    #[cfg(test)]
                    result_get_code: symbol!("gcc_jit_result_get_code", ResultGetCode),
                    #[cfg(test)]
                    result_release: symbol!("gcc_jit_result_release", ResultRelease),
                    get_first_error: symbol!("gcc_jit_context_get_first_error", GetFirstError),
                    _library: library,
                });
            }
        }
        Err(format!("libgccjit is unavailable ({})", errors.join("; ")))
    }

    pub(crate) fn context(&'static self) -> Result<Context, String> {
        // SAFETY: `self` owns the loaded function and the returned context is
        // uniquely owned by the RAII guard below.
        let raw = unsafe { (self.acquire)() };
        if raw.is_null() {
            Err("libgccjit returned a null compilation context".into())
        } else {
            Ok(Context { api: self, raw })
        }
    }

    pub(crate) fn version(&self) -> (i32, i32, i32) {
        // SAFETY: Version queries take no arguments and have no side effects.
        unsafe {
            (
                (self.version_major)(),
                (self.version_minor)(),
                (self.version_patchlevel)(),
            )
        }
    }
}

fn candidates() -> &'static [&'static str] {
    #[cfg(target_os = "macos")]
    {
        &[
            "/opt/homebrew/opt/libgccjit/lib/gcc/current/libgccjit.0.dylib",
            "/opt/homebrew/lib/gcc/current/libgccjit.dylib",
            "libgccjit.0.dylib",
            "libgccjit.dylib",
        ]
    }
    #[cfg(target_os = "linux")]
    {
        &["libgccjit.so.0", "libgccjit.so"]
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        &[]
    }
}

static API: OnceLock<Result<Api, String>> = OnceLock::new();

pub(crate) fn api() -> Result<&'static Api, String> {
    API.get_or_init(Api::load).as_ref().map_err(Clone::clone)
}

pub(crate) fn available() -> bool {
    api().is_ok()
}

pub(crate) fn version() -> Option<(i32, i32, i32)> {
    api().ok().map(Api::version)
}

pub(crate) struct Context {
    api: &'static Api,
    raw: *mut ContextOpaque,
}

impl Context {
    pub(crate) fn c_type(&self, kind: TypeKind) -> *mut TypeOpaque {
        // SAFETY: The type belongs to this live context.
        unsafe { (self.api.get_type)(self.raw, kind as c_int) }
    }

    pub(crate) fn int_type(&self, bytes: usize, signed: bool) -> *mut TypeOpaque {
        let bytes = c_int::try_from(bytes).expect("libgccjit integer type width exceeds c_int");
        // SAFETY: The returned type belongs to this live context.
        unsafe { (self.api.get_int_type)(self.raw, bytes, c_int::from(signed)) }
    }

    pub(crate) fn pointer_type(&self, kind: *mut TypeOpaque) -> *mut TypeOpaque {
        // SAFETY: KIND belongs to this live context.
        unsafe { (self.api.type_get_pointer)(kind) }
    }

    pub(crate) fn const_type(&self, kind: *mut TypeOpaque) -> *mut TypeOpaque {
        // SAFETY: KIND belongs to this live context.
        unsafe { (self.api.type_get_const)(kind) }
    }

    pub(crate) fn pointed_type(&self, kind: *mut TypeOpaque) -> Option<*mut TypeOpaque> {
        // SAFETY: KIND belongs to this live context.  Reflection returns null
        // exactly when KIND is not a pointer type.
        let pointed = unsafe { (self.api.type_is_pointer)(kind) };
        (!pointed.is_null()).then_some(pointed)
    }

    pub(crate) fn new_opaque_struct(&self, name: &CStr) -> *mut StructOpaque {
        // SAFETY: libgccjit copies NAME and owns the returned declaration.
        unsafe { (self.api.new_opaque_struct)(self.raw, ptr::null_mut(), name.as_ptr()) }
    }

    pub(crate) fn struct_type(&self, structure: *mut StructOpaque) -> *mut TypeOpaque {
        // SAFETY: STRUCTURE belongs to this live context.
        unsafe { (self.api.struct_as_type)(structure) }
    }

    pub(crate) fn new_field(&self, kind: *mut TypeOpaque, name: &CStr) -> *mut FieldOpaque {
        // SAFETY: KIND belongs to this context and libgccjit copies NAME.
        unsafe { (self.api.new_field)(self.raw, ptr::null_mut(), kind, name.as_ptr()) }
    }

    pub(crate) fn new_struct_type(
        &self,
        name: &CStr,
        fields: &mut [*mut FieldOpaque],
    ) -> *mut StructOpaque {
        let count = c_int::try_from(fields.len()).expect("libgccjit struct has too many fields");
        // SAFETY: Every field belongs to this context; libgccjit copies the
        // field array and NAME.
        unsafe {
            (self.api.new_struct_type)(
                self.raw,
                ptr::null_mut(),
                name.as_ptr(),
                count,
                fields.as_mut_ptr(),
            )
        }
    }

    pub(crate) fn new_union_type(
        &self,
        name: &CStr,
        fields: &mut [*mut FieldOpaque],
    ) -> *mut TypeOpaque {
        let count = c_int::try_from(fields.len()).expect("libgccjit union has too many fields");
        // SAFETY: Every field belongs to this context; libgccjit copies the
        // field array and NAME.
        unsafe {
            (self.api.new_union_type)(
                self.raw,
                ptr::null_mut(),
                name.as_ptr(),
                count,
                fields.as_mut_ptr(),
            )
        }
    }

    pub(crate) fn new_array_type(
        &self,
        element_type: *mut TypeOpaque,
        count: usize,
    ) -> *mut TypeOpaque {
        let count = c_int::try_from(count).expect("libgccjit array has too many elements");
        // SAFETY: ELEMENT_TYPE belongs to this context.
        unsafe { (self.api.new_array_type)(self.raw, ptr::null_mut(), element_type, count) }
    }

    pub(crate) fn set_struct_fields(
        &self,
        structure: *mut StructOpaque,
        fields: &mut [*mut FieldOpaque],
    ) {
        let count = c_int::try_from(fields.len()).expect("libgccjit struct has too many fields");
        // SAFETY: STRUCTURE and every field belong to this context; fields
        // are assigned exactly once to an opaque structure.
        unsafe {
            (self.api.struct_set_fields)(structure, ptr::null_mut(), count, fields.as_mut_ptr())
        }
    }

    pub(crate) fn new_function_pointer_type(
        &self,
        return_type: *mut TypeOpaque,
        params: &mut [*mut TypeOpaque],
        variadic: bool,
    ) -> *mut TypeOpaque {
        let count = c_int::try_from(params.len()).expect("libgccjit function has too many params");
        // SAFETY: All types belong to this context and the parameter array is
        // copied by libgccjit.
        unsafe {
            (self.api.new_function_ptr_type)(
                self.raw,
                ptr::null_mut(),
                return_type,
                count,
                params.as_mut_ptr(),
                c_int::from(variadic),
            )
        }
    }

    pub(crate) fn new_param(&self, kind: *mut TypeOpaque, name: &CStr) -> *mut ParamOpaque {
        // SAFETY: The context owns the returned parameter and copies NAME.
        unsafe { (self.api.new_param)(self.raw, ptr::null_mut(), kind, name.as_ptr()) }
    }

    pub(crate) fn new_function(
        &self,
        kind: FunctionKind,
        return_type: *mut TypeOpaque,
        name: &CStr,
        params: &mut [*mut ParamOpaque],
        variadic: bool,
    ) -> *mut FunctionOpaque {
        // SAFETY: All types and parameters belong to this context; libgccjit
        // copies the parameter array.
        unsafe {
            (self.api.new_function)(
                self.raw,
                ptr::null_mut(),
                kind as c_int,
                return_type,
                name.as_ptr(),
                params.len() as c_int,
                params.as_mut_ptr(),
                c_int::from(variadic),
            )
        }
    }

    pub(crate) fn function_param(
        &self,
        function: *mut FunctionOpaque,
        index: usize,
    ) -> *mut ParamOpaque {
        let index = c_int::try_from(index).expect("libgccjit parameter index exceeds c_int");
        // SAFETY: FUNCTION belongs to this context and INDEX names one of its
        // declared parameters.
        unsafe { (self.api.function_get_param)(function, index) }
    }

    pub(crate) fn new_block(&self, function: *mut FunctionOpaque, name: &CStr) -> *mut BlockOpaque {
        // SAFETY: FUNCTION belongs to this context and libgccjit copies NAME.
        unsafe { (self.api.new_block)(function, name.as_ptr()) }
    }

    pub(crate) fn param_as_rvalue(&self, param: *mut ParamOpaque) -> *mut RValueOpaque {
        // SAFETY: PARAM belongs to this context.
        unsafe { (self.api.param_as_rvalue)(param) }
    }

    pub(crate) fn param_as_lvalue(&self, param: *mut ParamOpaque) -> *mut LValueOpaque {
        // SAFETY: PARAM belongs to this context.
        unsafe { (self.api.param_as_lvalue)(param) }
    }

    pub(crate) fn new_global(
        &self,
        kind: GlobalKind,
        value_type: *mut TypeOpaque,
        name: &CStr,
    ) -> *mut LValueOpaque {
        // SAFETY: VALUE_TYPE belongs to this context and libgccjit copies NAME.
        unsafe {
            (self.api.new_global)(
                self.raw,
                ptr::null_mut(),
                kind as c_int,
                value_type,
                name.as_ptr(),
            )
        }
    }

    pub(crate) fn initialize_global(
        &self,
        global: *mut LValueOpaque,
        bytes: &[u8],
    ) -> *mut LValueOpaque {
        // SAFETY: GLOBAL is an integral array global in this context and
        // libgccjit copies exactly BYTES.len() bytes during the call.
        unsafe { (self.api.global_set_initializer)(global, bytes.as_ptr().cast(), bytes.len()) }
    }

    pub(crate) fn lvalue_as_rvalue(&self, value: *mut LValueOpaque) -> *mut RValueOpaque {
        // SAFETY: VALUE belongs to this context.
        unsafe { (self.api.lvalue_as_rvalue)(value) }
    }

    pub(crate) fn lvalue_access_field(
        &self,
        value: *mut LValueOpaque,
        field: *mut FieldOpaque,
    ) -> *mut LValueOpaque {
        // SAFETY: VALUE and FIELD belong to this context and FIELD belongs to
        // VALUE's struct or union type.
        unsafe { (self.api.lvalue_access_field)(value, ptr::null_mut(), field) }
    }

    pub(crate) fn lvalue_address(&self, value: *mut LValueOpaque) -> *mut RValueOpaque {
        // SAFETY: VALUE belongs to this context.
        unsafe { (self.api.lvalue_get_address)(value, ptr::null_mut()) }
    }

    pub(crate) fn rvalue_access_field(
        &self,
        value: *mut RValueOpaque,
        field: *mut FieldOpaque,
    ) -> *mut RValueOpaque {
        // SAFETY: VALUE and FIELD belong to this context and FIELD belongs to
        // VALUE's struct or union type.
        unsafe { (self.api.rvalue_access_field)(value, ptr::null_mut(), field) }
    }

    pub(crate) fn dereference(&self, value: *mut RValueOpaque) -> *mut LValueOpaque {
        // SAFETY: VALUE belongs to this context and has pointer type.
        unsafe { (self.api.rvalue_dereference)(value, ptr::null_mut()) }
    }

    pub(crate) fn dereference_field(
        &self,
        value: *mut RValueOpaque,
        field: *mut FieldOpaque,
    ) -> *mut LValueOpaque {
        // SAFETY: VALUE and FIELD belong to this context and the pointer's
        // pointee is the struct which owns FIELD.
        unsafe { (self.api.rvalue_dereference_field)(value, ptr::null_mut(), field) }
    }

    pub(crate) fn rvalue_type(&self, value: *mut RValueOpaque) -> *mut TypeOpaque {
        // SAFETY: VALUE belongs to this context.
        unsafe { (self.api.rvalue_get_type)(value) }
    }

    pub(crate) fn array_access(
        &self,
        pointer: *mut RValueOpaque,
        index: *mut RValueOpaque,
    ) -> *mut LValueOpaque {
        // SAFETY: POINTER and INDEX belong to this context and have the
        // pointer/integer types required by libgccjit.
        unsafe { (self.api.new_array_access)(self.raw, ptr::null_mut(), pointer, index) }
    }

    pub(crate) fn int(&self, kind: *mut TypeOpaque, value: c_int) -> *mut RValueOpaque {
        // SAFETY: KIND belongs to this context.
        unsafe { (self.api.new_int)(self.raw, kind, value) }
    }

    pub(crate) fn long(&self, kind: *mut TypeOpaque, value: c_long) -> *mut RValueOpaque {
        // SAFETY: KIND belongs to this context.
        unsafe { (self.api.new_long)(self.raw, kind, value) }
    }

    pub(crate) fn pointer(&self, kind: *mut TypeOpaque, value: *mut c_void) -> *mut RValueOpaque {
        // SAFETY: KIND belongs to this context and is a pointer type.  VALUE
        // is embedded as an immediate address by libgccjit.
        unsafe { (self.api.new_pointer)(self.raw, kind, value) }
    }

    pub(crate) fn binary(
        &self,
        operation: BinaryOp,
        kind: *mut TypeOpaque,
        left: *mut RValueOpaque,
        right: *mut RValueOpaque,
    ) -> *mut RValueOpaque {
        // SAFETY: Every operand and type belongs to this context.
        unsafe {
            (self.api.new_binary_op)(
                self.raw,
                ptr::null_mut(),
                operation as c_int,
                kind,
                left,
                right,
            )
        }
    }

    pub(crate) fn unary(
        &self,
        operation: UnaryOp,
        kind: *mut TypeOpaque,
        value: *mut RValueOpaque,
    ) -> *mut RValueOpaque {
        // SAFETY: VALUE and KIND belong to this context.
        unsafe {
            (self.api.new_unary_op)(self.raw, ptr::null_mut(), operation as c_int, kind, value)
        }
    }

    pub(crate) fn compare(
        &self,
        operation: Comparison,
        left: *mut RValueOpaque,
        right: *mut RValueOpaque,
    ) -> *mut RValueOpaque {
        // SAFETY: Both operands belong to this context.
        unsafe {
            (self.api.new_comparison)(self.raw, ptr::null_mut(), operation as c_int, left, right)
        }
    }

    pub(crate) fn cast(
        &self,
        value: *mut RValueOpaque,
        kind: *mut TypeOpaque,
    ) -> *mut RValueOpaque {
        // SAFETY: VALUE and KIND belong to this context.
        unsafe { (self.api.new_cast)(self.raw, ptr::null_mut(), value, kind) }
    }

    pub(crate) fn bitcast(
        &self,
        value: *mut RValueOpaque,
        kind: *mut TypeOpaque,
    ) -> *mut RValueOpaque {
        // SAFETY: VALUE and KIND belong to this context and callers use
        // equally-sized source and destination types.
        unsafe { (self.api.new_bitcast)(self.raw, ptr::null_mut(), value, kind) }
    }

    pub(crate) fn call(
        &self,
        function: *mut FunctionOpaque,
        args: &mut [*mut RValueOpaque],
    ) -> *mut RValueOpaque {
        let count = c_int::try_from(args.len()).expect("libgccjit call has too many arguments");
        // SAFETY: FUNCTION and every argument belong to this context; the
        // argument array is copied.
        unsafe {
            (self.api.new_call)(
                self.raw,
                ptr::null_mut(),
                function,
                count,
                args.as_mut_ptr(),
            )
        }
    }

    pub(crate) fn call_through_pointer(
        &self,
        function: *mut RValueOpaque,
        args: &mut [*mut RValueOpaque],
    ) -> *mut RValueOpaque {
        let count = c_int::try_from(args.len()).expect("libgccjit call has too many arguments");
        // SAFETY: FUNCTION and every argument belong to this context; the
        // function rvalue has a matching pointer-to-function type.
        unsafe {
            (self.api.new_call_through_ptr)(
                self.raw,
                ptr::null_mut(),
                function,
                count,
                args.as_mut_ptr(),
            )
        }
    }

    pub(crate) fn new_local(
        &self,
        function: *mut FunctionOpaque,
        kind: *mut TypeOpaque,
        name: &CStr,
    ) -> *mut LValueOpaque {
        // SAFETY: FUNCTION and KIND belong to this context and libgccjit
        // copies NAME.
        unsafe { (self.api.function_new_local)(function, ptr::null_mut(), kind, name.as_ptr()) }
    }

    pub(crate) fn assign(
        &self,
        block: *mut BlockOpaque,
        destination: *mut LValueOpaque,
        value: *mut RValueOpaque,
    ) {
        // SAFETY: All nodes belong to this context and BLOCK is still open.
        unsafe { (self.api.block_add_assignment)(block, ptr::null_mut(), destination, value) }
    }

    pub(crate) fn evaluate(&self, block: *mut BlockOpaque, value: *mut RValueOpaque) {
        // SAFETY: BLOCK and VALUE belong to this context and BLOCK is open.
        unsafe { (self.api.block_add_eval)(block, ptr::null_mut(), value) }
    }

    pub(crate) fn comment(&self, block: *mut BlockOpaque, text: &CStr) {
        // SAFETY: BLOCK belongs to this context and libgccjit copies TEXT.
        unsafe { (self.api.block_add_comment)(block, ptr::null_mut(), text.as_ptr()) }
    }

    pub(crate) fn end_with_conditional(
        &self,
        block: *mut BlockOpaque,
        condition: *mut RValueOpaque,
        on_true: *mut BlockOpaque,
        on_false: *mut BlockOpaque,
    ) {
        // SAFETY: All nodes belong to this context and this terminates BLOCK.
        unsafe {
            (self.api.end_with_conditional)(block, ptr::null_mut(), condition, on_true, on_false)
        }
    }

    pub(crate) fn end_with_jump(&self, block: *mut BlockOpaque, target: *mut BlockOpaque) {
        // SAFETY: Both blocks belong to the same function and this terminates
        // BLOCK exactly once.
        unsafe { (self.api.end_with_jump)(block, ptr::null_mut(), target) }
    }

    pub(crate) fn end_with_return(&self, block: *mut BlockOpaque, value: *mut RValueOpaque) {
        // SAFETY: BLOCK and VALUE belong to this context and this terminates
        // the block exactly once.
        unsafe { (self.api.end_with_return)(block, ptr::null_mut(), value) }
    }

    pub(crate) fn end_with_void_return(&self, block: *mut BlockOpaque) {
        // SAFETY: BLOCK belongs to this context and this terminates it once.
        unsafe { (self.api.end_with_void_return)(block, ptr::null_mut()) }
    }

    #[cfg(test)]
    pub(crate) fn compile(&self) -> Result<Compiled, String> {
        // SAFETY: The context is complete and remains alive during compile.
        let raw = unsafe { (self.api.compile)(self.raw) };
        if raw.is_null() {
            Err(self
                .first_error()
                .unwrap_or_else(|| "libgccjit compilation failed".into()))
        } else {
            Ok(Compiled { api: self.api, raw })
        }
    }

    pub(crate) fn compile_to_file(&self, kind: OutputKind, path: &CStr) -> Result<(), String> {
        // SAFETY: libgccjit copies/consumes PATH during this call; the context
        // remains live for the complete compilation.
        unsafe { (self.api.compile_to_file)(self.raw, kind as c_int, path.as_ptr()) };
        self.check_error()
    }

    pub(crate) fn set_optimization_level(&self, level: i32) {
        // GCC_JIT_INT_OPTION_OPTIMIZATION_LEVEL is enum value zero.
        // SAFETY: This mutates only this live context before compilation.
        unsafe { (self.api.set_int_option)(self.raw, 0, level.clamp(0, 3)) }
    }

    pub(crate) fn set_debug_info(&self, enabled: bool) {
        self.set_bool_option(BoolOption::DebugInfo, enabled);
    }

    pub(crate) fn set_bool_option(&self, option: BoolOption, enabled: bool) {
        // SAFETY: This mutates only this live context before compilation.
        unsafe { (self.api.set_bool_option)(self.raw, option as c_int, c_int::from(enabled)) }
    }

    pub(crate) fn add_command_line_option(&self, option: &CStr) {
        // SAFETY: libgccjit copies OPTION.
        unsafe { (self.api.add_command_line_option)(self.raw, option.as_ptr()) }
    }

    pub(crate) fn add_driver_option(&self, option: &CStr) {
        // SAFETY: libgccjit copies OPTION.
        unsafe { (self.api.add_driver_option)(self.raw, option.as_ptr()) }
    }

    pub(crate) fn dump_to_file(&self, path: &CStr, update_locations: bool) {
        // SAFETY: libgccjit writes to PATH synchronously and copies any path
        // state it retains.
        unsafe { (self.api.dump_to_file)(self.raw, path.as_ptr(), c_int::from(update_locations)) }
    }

    pub(crate) fn dump_reproducer(&self, path: &CStr) {
        // SAFETY: libgccjit writes PATH synchronously and copies the path.
        unsafe { (self.api.dump_reproducer)(self.raw, path.as_ptr()) }
    }

    fn check_error(&self) -> Result<(), String> {
        match self.first_error() {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    pub(crate) fn first_error(&self) -> Option<String> {
        // SAFETY: The returned string is owned by this live context.
        let error = unsafe { (self.api.get_first_error)(self.raw) };
        (!error.is_null()).then(|| {
            // SAFETY: libgccjit promises a NUL-terminated diagnostic.
            unsafe { CStr::from_ptr(error) }
                .to_string_lossy()
                .into_owned()
        })
    }
}

impl Drop for Context {
    fn drop(&mut self) {
        // SAFETY: This guard uniquely owns the context and releases it once.
        unsafe { (self.api.release)(self.raw) }
    }
}

#[cfg(test)]
pub(crate) struct Compiled {
    api: &'static Api,
    raw: *mut ResultOpaque,
}

#[cfg(test)]
impl Compiled {
    pub(crate) fn code(&self, name: &CStr) -> Result<*mut c_void, String> {
        // SAFETY: The result is live and libgccjit copies/reads NAME only for
        // this call.  The returned code remains owned by this result guard.
        let code = unsafe { (self.api.result_get_code)(self.raw, name.as_ptr()) };
        if code.is_null() {
            Err(format!(
                "compiled function {} was not found",
                name.to_string_lossy()
            ))
        } else {
            Ok(code)
        }
    }
}

#[cfg(test)]
impl Drop for Compiled {
    fn drop(&mut self) {
        // SAFETY: This guard uniquely owns the compiled result.
        unsafe { (self.api.result_release)(self.raw) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;

    #[test]
    fn loads_libgccjit_and_executes_a_smoke_test_function() {
        let api = api().expect("the native compiler test host must provide libgccjit");
        assert_eq!(api.version(), (15, 2, 0));
        let context = api.context().expect("acquire libgccjit context");
        let long = context.c_type(TypeKind::Long);
        let parameter_name = CString::new("value").expect("static name");
        let function_name = CString::new("add_one").expect("static name");
        let block_name = CString::new("entry").expect("static name");
        let parameter = context.new_param(long, &parameter_name);
        let function = context.new_function(
            FunctionKind::Exported,
            long,
            &function_name,
            &mut [parameter],
            false,
        );
        let block = context.new_block(function, &block_name);
        let result = context.binary(
            BinaryOp::Plus,
            long,
            context.param_as_rvalue(parameter),
            context.long(long, 1),
        );
        context.end_with_return(block, result);
        let compiled = context.compile().expect("compile probe");
        let code = compiled.code(&function_name).expect("resolve probe");
        // SAFETY: The emitted function was declared as `long(long)`, which is
        // ABI-compatible with i64 on every supported 64-bit host.
        let function: unsafe extern "C" fn(c_long) -> c_long = unsafe { std::mem::transmute(code) };
        // SAFETY: The result guard owns the code for the duration of the call.
        assert_eq!(unsafe { function(41) }, 42);
    }
}
