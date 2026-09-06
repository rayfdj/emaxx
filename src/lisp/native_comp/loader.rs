//! `.eln` loading and native subroutine ownership.
//!
//! This is the Rust implementation of `comp.c`'s dynamic-loader boundary.
//! The shared object contains only code emitted from GNU's unchanged
//! `comp.el` input; all runtime addresses installed below point into Emaxx's
//! Rust runtime or the loaded compilation unit itself.

use super::backend::{
    COMP_UNIT_SYM, CURRENT_THREAD_RELOC_SYM, Compiler, DATA_RELOC_EPHEMERAL_SYM,
    DATA_RELOC_IMPURE_SYM, DATA_RELOC_SYM, F_SYMBOLS_WITH_POS_ENABLED_RELOC_SYM,
    FUNC_LINK_TABLE_SYM, LINK_TABLE_HASH_SYM, PURE_RELOC_SYM, TEXT_DATA_RELOC_EPHEMERAL_SYM,
    TEXT_DATA_RELOC_IMPURE_SYM, TEXT_DATA_RELOC_SYM, TEXT_FDOC_SYM, TEXT_OPTIM_QLY_SYM,
};
use super::runtime::{NativeCallingConvention, NativeRuntime, NativeWord};
use crate::lisp::eval::{Interpreter, RecordKind};
use crate::lisp::primitives::{
    decode_utf8_bytes, is_vector_value, read_one_form_in_env, string_like, values_equal,
};
use crate::lisp::types::{Env, LispError, Value};
use libloading::Library;
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::ffi::c_void;
use std::path::Path;

const TOP_LEVEL_RUN_SYM: &str = "top_level_run";
const LATE_TOP_LEVEL_RUN_SYM: &str = "late_top_level_run";
#[repr(C)]
struct StaticObjectHeader {
    len: isize,
}

pub(crate) type UnitLibrary = Library;

struct LoadedUnit {
    library: Library,
    record_id: u64,
    /// comp.c sets this when the shared object's saved unit pointer was
    /// already non-nil.  Repeated top-level runs must not recreate anonymous
    /// native lambdas.
    loaded_once: bool,
    /// comp.c's `load_ongoing`: a unit whose top-level code is still running
    /// on this thread must not have its ephemeral relocations rewritten by
    /// a nested load of the same file.
    load_ongoing: bool,
    _data: Value,
    _impure_data: Value,
    _optimization_qualities: Value,
    data_relocations: *mut NativeWord,
    data_relocation_count: usize,
    impure_relocations: *mut NativeWord,
    impure_relocation_count: usize,
}

struct EphemeralRelocations {
    _guard: Value,
    start: *const NativeWord,
    len: usize,
}

/// GNU clears an ELN's saved compilation-unit word before its owning unit is
/// finalized and the dynamic library is closed.  Until first-load validation
/// succeeds, provide the same guarantee on every Rust error return.
struct SavedUnitRollback {
    saved_unit: *mut NativeWord,
    armed: bool,
}

impl SavedUnitRollback {
    fn new(saved_unit: *mut NativeWord) -> Self {
        Self {
            saved_unit,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for SavedUnitRollback {
    fn drop(&mut self) {
        if self.armed {
            unsafe { std::ptr::write(self.saved_unit, 0) };
        }
    }
}

#[derive(Clone, Copy)]
struct NativeFunction {
    target: *const c_void,
    convention: NativeCallingConvention,
    min_args: usize,
    max_args: Option<usize>,
    dynamic: bool,
}

/// The function-pointer and arity portion of a non-dynamic native subr.
/// eval.c:funcall_general sends these through funcall_subr, whereas dynamic
/// native functions follow funcall_lambda and therefore stay on the general
/// evaluator path.
#[derive(Clone, Copy)]
pub(crate) struct DirectNativeFunction {
    pub(crate) target: *const c_void,
    pub(crate) convention: NativeCallingConvention,
    pub(crate) min_args: usize,
    pub(crate) max_args: Option<usize>,
}

#[derive(Default)]
pub(crate) struct NativeRegistry {
    units: Vec<LoadedUnit>,
    functions: HashMap<u64, NativeFunction>,
    function_names: HashMap<u64, Box<str>>,
}

impl NativeRegistry {
    pub(crate) fn is_empty(&self) -> bool {
        self.units.is_empty() && self.functions.is_empty() && self.function_names.is_empty()
    }

    fn unit(&self, record_id: u64) -> Option<&LoadedUnit> {
        self.units.iter().find(|unit| unit.record_id == record_id)
    }

    fn function(&self, record_id: u64) -> Option<NativeFunction> {
        self.functions.get(&record_id).copied()
    }

    pub(crate) fn function_name(&self, record_id: u64) -> Option<&str> {
        self.function_names.get(&record_id).map(AsRef::as_ref)
    }
}

pub(super) struct LoaderState<'a> {
    compiler: &'a RefCell<Option<Compiler>>,
    registry: &'a mut NativeRegistry,
    runtime: &'a mut NativeRuntime,
}

impl<'a> LoaderState<'a> {
    pub(super) fn new(
        compiler: &'a RefCell<Option<Compiler>>,
        registry: &'a mut NativeRegistry,
        runtime: &'a mut NativeRuntime,
    ) -> Self {
        Self {
            compiler,
            registry,
            runtime,
        }
    }
}

thread_local! {
    static ACTIVE_REGISTRY: Cell<*mut NativeRegistry> = const { Cell::new(std::ptr::null_mut()) };
    static ACTIVE_REGISTERED_RUNTIME: Cell<*mut NativeRuntime> =
        const { Cell::new(std::ptr::null_mut()) };
    static ACTIVE_COMPILER: Cell<*const RefCell<Option<Compiler>>> =
        const { Cell::new(std::ptr::null()) };
}

struct RegistryGuard {
    previous_registry: *mut NativeRegistry,
    previous_runtime: *mut NativeRuntime,
    previous_compiler: *const RefCell<Option<Compiler>>,
}

impl Drop for RegistryGuard {
    fn drop(&mut self) {
        ACTIVE_REGISTRY.set(self.previous_registry);
        ACTIVE_REGISTERED_RUNTIME.set(self.previous_runtime);
        ACTIVE_COMPILER.set(self.previous_compiler);
    }
}

pub(super) fn with_native_state<R>(
    compiler: &RefCell<Option<Compiler>>,
    registry: &mut NativeRegistry,
    runtime: &mut NativeRuntime,
    body: impl FnOnce(&mut NativeRuntime) -> R,
) -> R {
    let previous_registry = ACTIVE_REGISTRY.replace(registry);
    let previous_runtime = ACTIVE_REGISTERED_RUNTIME.replace(runtime);
    let previous_compiler = ACTIVE_COMPILER.replace(compiler);
    let _guard = RegistryGuard {
        previous_registry,
        previous_runtime,
        previous_compiler,
    };
    body(runtime)
}

fn with_active_registry<R>(body: impl FnOnce(&mut NativeRegistry) -> R) -> Option<R> {
    ACTIVE_REGISTRY.with(|registry| {
        let registry = registry.get();
        (!registry.is_null()).then(|| {
            // SAFETY: `with_registry` installs the pointer only while its
            // boxed registry remains alive. Native callbacks are synchronous.
            body(unsafe { &mut *registry })
        })
    })
}

fn with_active_registered_runtime<R>(body: impl FnOnce(&mut NativeRuntime) -> R) -> Option<R> {
    ACTIVE_REGISTERED_RUNTIME.with(|runtime| {
        let runtime = runtime.get();
        (!runtime.is_null()).then(|| {
            // SAFETY: `with_registry_and_runtime` installs this pointer only
            // while the owning compiler state remains live.  Native and
            // backend callbacks are synchronous on the Lisp thread.
            body(unsafe { &mut *runtime })
        })
    })
}

pub(super) fn with_active_compiler<R>(
    body: impl FnOnce(&RefCell<Option<Compiler>>) -> R,
) -> Option<R> {
    ACTIVE_COMPILER.with(|compiler| {
        let compiler = compiler.get();
        (!compiler.is_null()).then(|| {
            // SAFETY: `with_native_state` installs a pointer to the compiler
            // cell only while its owning NativeCompilerState is live.  The
            // RefCell enforces the compiler context's non-reentrant mutable
            // access independently of the active runtime and registry.
            body(unsafe { &*compiler })
        })
    })
}

fn inconsistent(file: &Value) -> LispError {
    LispError::SignalValue(Value::list([
        Value::symbol("native-lisp-file-inconsistent"),
        file.clone(),
    ]))
}

fn load_failed(file: &Value, message: impl Into<String>) -> LispError {
    LispError::SignalValue(Value::list([
        Value::symbol("native-lisp-load-failed"),
        file.clone(),
        Value::String(message.into().into()),
    ]))
}

unsafe fn data_symbol<T>(library: &Library, name: &str) -> Result<*mut T, String> {
    let symbol = unsafe { library.get::<*mut T>(name.as_bytes()) }
        .map_err(|error| format!("missing `{name}`: {error}"))?;
    Ok(*symbol)
}

unsafe fn function_symbol(library: &Library, name: &str) -> Result<*const c_void, String> {
    let symbol = unsafe { library.get::<unsafe extern "C" fn()>(name.as_bytes()) }
        .map_err(|error| format!("missing `{name}`: {error}"))?;
    Ok((*symbol as *const ()).cast())
}

unsafe fn read_static_object(
    library: &Library,
    file: &Value,
    name: &str,
    interpreter: &mut Interpreter,
    environment: &mut Env,
) -> Result<Value, LispError> {
    let blob_name = format!("{name}_blob");
    let blob = match unsafe { data_symbol::<StaticObjectHeader>(library, &blob_name) } {
        Ok(blob) => blob,
        Err(_) => {
            let function = unsafe {
                library.get::<unsafe extern "C" fn() -> *mut StaticObjectHeader>(name.as_bytes())
            }
            .map_err(|_| inconsistent(file))?;
            unsafe { function() }
        }
    };
    if blob.is_null() {
        return Err(inconsistent(file));
    }
    let len = unsafe { std::ptr::read_unaligned(std::ptr::addr_of!((*blob).len)) };
    let len = usize::try_from(len).map_err(|_| inconsistent(file))?;
    let bytes = if len == 0 {
        &[]
    } else {
        unsafe {
            std::slice::from_raw_parts(
                blob.cast::<u8>()
                    .add(std::mem::size_of::<StaticObjectHeader>()),
                len,
            )
        }
    };
    // comp.c:load_static_obj passes a freshly allocated Lisp string of this
    // exact blob length (including its terminating NUL) to Fread.  Rust can
    // parse the borrowed bytes directly, but GC timing must still account for
    // the C-owned temporary allocation.
    crate::lisp::types::note_string_allocation(len);
    let text = decode_utf8_bytes(bytes);
    let (value, _) = read_one_form_in_env(interpreter, &text, environment)?;
    let value = interpreter.materialize_read_object_literals(value, environment)?;
    interpreter.intern_symbols_in_value(&value);
    Ok(value)
}

fn vector_values(value: &Value) -> Result<Vec<Value>, LispError> {
    crate::lisp::primitives::vector_items(value)
        .map_err(|_| super::lisp::native_ice("serialized native relocation data is not a vector"))
}

fn comp_unit_relocations_match(
    unit: &LoadedUnit,
    runtime: &mut NativeRuntime,
    interpreter: &Interpreter,
    environment: &Env,
) -> bool {
    let Ok(data_values) = vector_values(&unit._data) else {
        return false;
    };
    if data_values.len() != unit.data_relocation_count {
        return false;
    }
    for (index, expected) in data_values.iter().enumerate() {
        let word = unsafe { std::ptr::read(unit.data_relocations.add(index)) };
        let Ok(actual) = runtime.decode_relocation(word) else {
            return false;
        };
        if !crate::lisp::primitives::values_eq_in_env(interpreter, &actual, expected, environment) {
            return false;
        }
    }

    let Ok(impure_values) = vector_values(&unit._impure_data) else {
        return false;
    };
    if impure_values.len() != unit.impure_relocation_count {
        return false;
    }
    let Some(guard) = interpreter
        .find_record(unit.record_id)
        .and_then(|record| record.slots.get(2))
    else {
        return false;
    };
    for (index, expected) in impure_values.iter().enumerate() {
        let word = unsafe { std::ptr::read(unit.impure_relocations.add(index)) };
        let Ok(actual) = runtime.decode_relocation(word) else {
            return false;
        };
        if actual.as_symbol().ok() == Some("lambda-fixup") {
            return false;
        }
        let native_function = matches!(actual, Value::Record(id)
            if interpreter.find_record(id).is_some_and(|record|
                record.kind == RecordKind::NativeCompiledFunction));
        if native_function {
            let Value::Record(guard_id) = guard else {
                return false;
            };
            if interpreter
                .equal_hash_lookup(*guard_id, &actual, environment)
                .flatten()
                .is_none()
            {
                return false;
            }
        } else if !crate::lisp::primitives::values_eq_in_env(
            interpreter,
            &actual,
            expected,
            environment,
        ) {
            return false;
        }
    }
    true
}

unsafe fn initialize_pointer<T>(
    library: &Library,
    name: &str,
    value: *mut T,
) -> Result<(), String> {
    let destination = unsafe { data_symbol::<*mut T>(library, name) }?;
    if destination.is_null() {
        return Err(format!("null relocation symbol `{name}`"));
    }
    unsafe { std::ptr::write(destination, value) };
    Ok(())
}

unsafe fn fill_relocations(
    library: &Library,
    name: &str,
    runtime: &mut NativeRuntime,
    values: &[Value],
) -> Result<*mut NativeWord, LispError> {
    let destination = unsafe { data_symbol::<NativeWord>(library, name) }
        .map_err(|error| super::lisp::native_ice(&error))?;
    if destination.is_null() && !values.is_empty() {
        return Err(super::lisp::native_ice("null native relocation array"));
    }
    let encoded = runtime.encode_relocations(values)?;
    if !encoded.is_empty() {
        unsafe { std::ptr::copy_nonoverlapping(encoded.as_ptr(), destination, encoded.len()) };
    }
    Ok(destination)
}

/// comp.c:dynlib_open_for_eln.  `file` is the name the unit is known by;
/// `path` is what the dynamic loader opens, which `native-elisp-load` may
/// have renamed to force a fresh handle.
pub(crate) fn open_unit(file: &Value, path: &str) -> Result<Library, LispError> {
    unsafe { Library::new(Path::new(path)) }.map_err(|error| load_failed(file, error.to_string()))
}

/// comp.c:load_comp_unit for an in-process (non-dump) load.
pub(super) fn load(
    state: LoaderState<'_>,
    interpreter: &mut Interpreter,
    environment: &mut Env,
    filename: &Value,
    library: Library,
    candidate_unit: &Value,
    late: bool,
) -> Result<Value, LispError> {
    let LoaderState {
        compiler,
        registry,
        runtime,
    } = state;
    let saved_unit = unsafe { data_symbol::<NativeWord>(&library, COMP_UNIT_SYM) }
        .map_err(|_| inconsistent(filename))?;
    if saved_unit.is_null() {
        return Err(inconsistent(filename));
    }
    let saved_word = unsafe { std::ptr::read(saved_unit) };

    let (record_id, unit, top_level) = if saved_word != 0 {
        // The dynamic loader handed back a unit this session already loaded
        // (dlopen returns the same handle for the same file).  Its static
        // relocations may be live in running frames and are never touched
        // again; only the top-level code runs.
        let unit = runtime.decode_relocation(saved_word)?;
        let Value::Record(record_id) = unit else {
            return Err(inconsistent(filename));
        };
        if registry.unit(record_id).is_none() {
            return Err(inconsistent(filename));
        }
        registry
            .units
            .iter_mut()
            .find(|loaded| loaded.record_id == record_id)
            .expect("unit existence checked above")
            .loaded_once = true;
        let unit_file = interpreter
            .find_record(record_id)
            .and_then(|record| record.slots.first())
            .cloned()
            .unwrap_or_else(|| filename.clone());
        let top_level_name = if late {
            LATE_TOP_LEVEL_RUN_SYM
        } else {
            TOP_LEVEL_RUN_SYM
        };
        let top_level = unsafe { function_symbol(&library, top_level_name) }
            .map_err(|_| inconsistent(&unit_file))?;
        (record_id, unit, top_level)
    } else {
        first_load(
            registry,
            runtime,
            interpreter,
            environment,
            FirstLoadInput {
                library,
                saved_unit,
                candidate_unit,
                late,
            },
        )?
    };

    let index = registry
        .units
        .iter()
        .position(|loaded| loaded.record_id == record_id)
        .expect("unit registered before its top-level code runs");
    let recursive_load = registry.units[index].load_ongoing;
    registry.units[index].load_ongoing = true;
    let unit_file = interpreter
        .find_record(record_id)
        .and_then(|record| record.slots.first())
        .cloned()
        .unwrap_or_else(|| filename.clone());
    let ephemeral = if recursive_load {
        // Another load of this unit is active on the stack and holds the
        // ephemeral data; rewriting it would clobber objects in use.
        Ok(None)
    } else {
        fill_ephemeral_relocations(
            &registry.units[index].library,
            &unit_file,
            runtime,
            interpreter,
            environment,
        )
    };
    let result = match ephemeral {
        Ok(ephemeral) => {
            if let Some(ephemeral) = &ephemeral {
                runtime.push_ephemeral_root_range(ephemeral.start, ephemeral.len);
            }
            let result = with_native_state(compiler, registry, runtime, |runtime| {
                runtime.invoke(
                    interpreter,
                    environment,
                    top_level,
                    NativeCallingConvention::Fixed,
                    std::slice::from_ref(&unit),
                )
            });
            // comp.c keeps data_ephemeral_vec in the load_comp_unit frame
            // until top_level_run returns.  The explicit drop is Rust's
            // counterpart of GNU's post-call volatile self-assignment.
            if let Some(ephemeral) = &ephemeral {
                runtime.pop_ephemeral_root_range(ephemeral.len);
            }
            drop(ephemeral);
            result
        }
        Err(error) => Err(error),
    };
    if result.is_ok() {
        debug_assert!(comp_unit_relocations_match(
            &registry.units[index],
            runtime,
            interpreter,
            environment,
        ));
    }
    if !recursive_load {
        registry.units[index].load_ongoing = false;
    }
    let result = result?;
    // comp.c:register_native_comp_unit.
    let loaded_units = interpreter
        .lookup_var("comp-loaded-comp-units-h", environment)
        .unwrap_or(Value::Nil);
    super::lisp::call_c_primitive(
        interpreter,
        environment,
        "puthash",
        &[unit_file, unit, loaded_units],
    )?;
    Ok(result)
}

/// The `!loaded_once` half of comp.c:load_comp_unit: verify the ABI hash,
/// materialize the unit's static data, and install every runtime pointer
/// and data relocation the generated code addresses.
struct FirstLoadInput<'a> {
    library: Library,
    saved_unit: *mut NativeWord,
    candidate_unit: &'a Value,
    late: bool,
}

fn first_load(
    registry: &mut NativeRegistry,
    runtime: &mut NativeRuntime,
    interpreter: &mut Interpreter,
    environment: &mut Env,
    input: FirstLoadInput<'_>,
) -> Result<(u64, Value, *const c_void), LispError> {
    let FirstLoadInput {
        library,
        saved_unit,
        candidate_unit,
        late,
    } = input;
    let Value::Record(record_id) = candidate_unit else {
        unreachable!("native load candidate is a native compilation unit")
    };
    let record_id = *record_id;
    debug_assert!(
        interpreter
            .find_record(record_id)
            .is_some_and(|record| record.kind == RecordKind::NativeCompUnit)
    );
    let unit = Value::Record(record_id);
    let file = interpreter
        .find_record(record_id)
        .and_then(|record| record.slots.first())
        .cloned()
        .unwrap_or(Value::Nil);
    let unit_word = runtime.encode_relocations(std::slice::from_ref(&unit))?[0];
    unsafe { std::ptr::write(saved_unit, unit_word) };
    let mut saved_unit_rollback = SavedUnitRollback::new(saved_unit);

    let top_level_name = if late {
        LATE_TOP_LEVEL_RUN_SYM
    } else {
        TOP_LEVEL_RUN_SYM
    };
    let top_level =
        unsafe { function_symbol(&library, top_level_name) }.map_err(|_| inconsistent(&file))?;

    // load_comp_unit verifies the complete relocation surface before it
    // allocates the temporary ABI-hash reader string.
    for symbol in [
        CURRENT_THREAD_RELOC_SYM,
        F_SYMBOLS_WITH_POS_ENABLED_RELOC_SYM,
        PURE_RELOC_SYM,
        DATA_RELOC_SYM,
        DATA_RELOC_IMPURE_SYM,
        DATA_RELOC_EPHEMERAL_SYM,
        FUNC_LINK_TABLE_SYM,
    ] {
        let pointer =
            unsafe { data_symbol::<u8>(&library, symbol) }.map_err(|_| inconsistent(&file))?;
        if pointer.is_null() {
            return Err(inconsistent(&file));
        }
    }

    let abi_hash = unsafe {
        read_static_object(
            &library,
            &file,
            LINK_TABLE_HASH_SYM,
            interpreter,
            environment,
        )?
    };
    let expected_hash = interpreter
        .lookup_var("comp-abi-hash", environment)
        .unwrap_or(Value::Nil);
    if !super::lisp::call_c_primitive(
        interpreter,
        environment,
        "string-equal",
        &[abi_hash, expected_hash],
    )?
    .is_truthy()
    {
        return Err(inconsistent(&file));
    }

    unsafe {
        initialize_pointer(
            &library,
            CURRENT_THREAD_RELOC_SYM,
            runtime.current_thread_relocation(),
        )
        .map_err(|_| inconsistent(&file))?;
        initialize_pointer(
            &library,
            F_SYMBOLS_WITH_POS_ENABLED_RELOC_SYM,
            interpreter.symbols_with_positions_relocation(),
        )
        .map_err(|_| inconsistent(&file))?;
        initialize_pointer(&library, PURE_RELOC_SYM, runtime.pure_relocation())
            .map_err(|_| inconsistent(&file))?;
        initialize_pointer(&library, FUNC_LINK_TABLE_SYM, runtime.function_link_table())
            .map_err(|_| inconsistent(&file))?;
    }

    let optimization_qualities = unsafe {
        read_static_object(
            &library,
            &file,
            TEXT_OPTIM_QLY_SYM,
            interpreter,
            environment,
        )?
    };
    let mut data = unsafe {
        read_static_object(
            &library,
            &file,
            TEXT_DATA_RELOC_SYM,
            interpreter,
            environment,
        )?
    };
    let impure_data = unsafe {
        read_static_object(
            &library,
            &file,
            TEXT_DATA_RELOC_IMPURE_SYM,
            interpreter,
            environment,
        )?
    };
    if interpreter
        .lookup_var("purify-flag", environment)
        .is_some_and(|value| value.is_truthy())
    {
        data = crate::lisp::primitives::purecopy_value(interpreter, &data, environment)?;
    }
    let data_values = vector_values(&data)?;
    let impure_values = vector_values(&impure_data)?;
    {
        let record = interpreter
            .find_record_mut(record_id)
            .expect("new native compilation unit remains live");
        record.slots[1] = optimization_qualities.clone();
        record.slots[5] = data.clone();
        record.slots[6] = impure_data.clone();
    }
    let data_relocations =
        unsafe { fill_relocations(&library, DATA_RELOC_SYM, runtime, &data_values)? };
    let impure_relocations =
        unsafe { fill_relocations(&library, DATA_RELOC_IMPURE_SYM, runtime, &impure_values)? };
    runtime.register_permanent_root_range(data_relocations, data_values.len());
    runtime.register_permanent_root_range(impure_relocations, impure_values.len());

    registry.units.push(LoadedUnit {
        library,
        record_id,
        loaded_once: false,
        load_ongoing: false,
        _data: data,
        _impure_data: impure_data,
        _optimization_qualities: optimization_qualities,
        data_relocations,
        data_relocation_count: data_values.len(),
        impure_relocations,
        impure_relocation_count: impure_values.len(),
    });
    runtime.register_permanent_root_range(saved_unit, 1);
    saved_unit_rollback.disarm();
    Ok((record_id, unit, top_level))
}

/// Ephemeral data is read and installed on every non-recursive load; GNU
/// keeps it alive only for the duration of the top-level run.
fn fill_ephemeral_relocations(
    library: &Library,
    filename: &Value,
    runtime: &mut NativeRuntime,
    interpreter: &mut Interpreter,
    environment: &mut Env,
) -> Result<Option<EphemeralRelocations>, LispError> {
    let ephemeral_data = unsafe {
        read_static_object(
            library,
            filename,
            TEXT_DATA_RELOC_EPHEMERAL_SYM,
            interpreter,
            environment,
        )?
    };
    let ephemeral_values = vector_values(&ephemeral_data)?;
    let start = unsafe {
        fill_relocations(
            library,
            DATA_RELOC_EPHEMERAL_SYM,
            runtime,
            &ephemeral_values,
        )?
    };
    Ok(Some(EphemeralRelocations {
        _guard: ephemeral_data,
        start,
        len: ephemeral_values.len(),
    }))
}

/// Load a compilation unit into the native state already executing on this
/// thread.  A unit's top-level function can run arbitrary Lisp, including a
/// `require' that loads another `.eln'.  GNU's `comp.c' has one process-wide
/// loader/runtime state, so that nested unit must join the active registry and
/// heap rather than a temporary default state on `Interpreter'.
pub(crate) fn load_active(
    interpreter: &mut Interpreter,
    environment: &mut Env,
    filename: &Value,
    library: Library,
    candidate_unit: &Value,
    late: bool,
) -> Result<Result<Value, LispError>, Library> {
    if ACTIVE_REGISTRY.with(Cell::get).is_null() {
        return Err(library);
    }
    Ok(with_active_registry(|registry| {
        with_active_compiler(|compiler| {
            super::runtime::with_current_runtime(|runtime| {
                load(
                    LoaderState::new(compiler, registry, runtime),
                    interpreter,
                    environment,
                    filename,
                    library,
                    candidate_unit,
                    late,
                )
            })
            .unwrap_or_else(|| {
                Err(super::lisp::native_ice(
                    "active native registry has no active runtime",
                ))
            })
        })
        .unwrap_or_else(|| {
            Err(super::lisp::native_ice(
                "active native registry has no active compiler state",
            ))
        })
    })
    .expect("active registry checked above"))
}

#[derive(Clone, Copy)]
pub(crate) enum RegistrationKind {
    Lambda,
    Subroutine,
    LateSubroutine,
}

pub(super) fn register_with_state(
    registry: &mut NativeRegistry,
    runtime: &mut NativeRuntime,
    interpreter: &mut Interpreter,
    environment: &mut Env,
    arguments: &[Value],
    kind: RegistrationKind,
) -> Result<Value, LispError> {
    if arguments.len() != 7 {
        return Err(LispError::WrongNumberOfArgs(
            "comp--register-subr".into(),
            arguments.len(),
        ));
    }
    let late_pending_table = if matches!(kind, RegistrationKind::LateSubroutine) {
        let table = interpreter
            .lookup_var("comp-deferred-pending-h", environment)
            .unwrap_or(Value::Nil);
        let pending = super::lisp::call_c_primitive(
            interpreter,
            environment,
            "gethash",
            &[arguments[0].clone(), table.clone(), Value::Nil],
        )?;
        let symbol = arguments[0].as_symbol().map(str::to_owned).or_else(|_| {
            crate::lisp::primitives::symbols_with_pos_enabled(interpreter, environment)
                .then(|| crate::lisp::primitives::symbol_with_pos_parts(interpreter, &arguments[0]))
                .flatten()
                .and_then(|(symbol, _)| symbol.as_symbol().ok().map(str::to_owned))
                .ok_or_else(|| {
                    crate::lisp::primitives::wrong_type_argument("symbolp", arguments[0].clone())
                })
        })?;
        let current = interpreter
            .logical_function_binding(&symbol, &Env::new())
            .unwrap_or(Value::Nil);
        if !values_equal(interpreter, &current, &pending) {
            super::lisp::call_c_primitive(
                interpreter,
                environment,
                "remhash",
                &[arguments[0].clone(), table],
            )?;
            return Ok(Value::Nil);
        }
        Some(table)
    } else {
        None
    };
    let c_name = string_like(&arguments[1])
        .map(|string| string.text)
        .ok_or_else(|| {
            crate::lisp::primitives::wrong_type_argument("stringp", arguments[1].clone())
        })?;
    let dynamic = matches!(arguments[2], Value::Cons(_));
    let (min_args, max_value, lambda_list) = if dynamic {
        (
            arguments[2].car()?.as_integer()?,
            arguments[2].cdr()?,
            arguments[3].clone(),
        )
    } else {
        (arguments[2].as_integer()?, arguments[3].clone(), Value::Nil)
    };
    let min_args = usize::try_from(min_args)
        .map_err(|_| super::lisp::native_ice("negative native minimum arity"))?;
    let (convention, max_args) = match max_value {
        Value::Integer(maximum) => {
            let maximum = usize::try_from(maximum)
                .map_err(|_| super::lisp::native_ice("negative native maximum arity"))?;
            (
                if dynamic || maximum <= 8 {
                    NativeCallingConvention::Fixed
                } else {
                    NativeCallingConvention::Many
                },
                Some(maximum),
            )
        }
        Value::Symbol(ref name) if name == "many" => (
            if dynamic {
                NativeCallingConvention::Fixed
            } else {
                NativeCallingConvention::Many
            },
            None,
        ),
        other => {
            return Err(crate::lisp::primitives::wrong_type_argument(
                "integer-or-many-p",
                other,
            ));
        }
    };
    let Value::Record(unit_record_id) = arguments[6] else {
        return Err(crate::lisp::primitives::wrong_type_argument(
            "native-comp-unit-p",
            arguments[6].clone(),
        ));
    };

    let registered = (|| {
        let unit = registry.unit(unit_record_id).ok_or_else(|| {
            LispError::SignalValue(Value::list([Value::symbol("wrong-register-subr-call")]))
        })?;
        if matches!(kind, RegistrationKind::Lambda) && unit.loaded_once {
            return Ok(Value::Nil);
        }
        let target = unsafe { function_symbol(&unit.library, &c_name) }
            .map_err(|error| super::lisp::native_ice(&error))?;
        let symbol_name = if matches!(kind, RegistrationKind::Lambda) {
            c_name.clone()
        } else {
            arguments[0].as_symbol()?.to_string()
        };
        let rest = arguments[5].to_vec()?;
        let function = interpreter.create_pseudovector(
            RecordKind::NativeCompiledFunction,
            "subr",
            vec![
                // GNU keeps both names as C strings in Lisp_Subr.  Keeping a
                // Lisp string here would invent two GC-visible objects.
                Value::Nil,
                Value::Integer(min_args as i64),
                max_value.clone(),
                Value::Nil,
                arguments[4].clone(),
                rest.first().cloned().unwrap_or(Value::Nil),
                rest.get(1).cloned().unwrap_or(Value::Nil),
                rest.get(2).cloned().unwrap_or(Value::Nil),
                arguments[6].clone(),
                lambda_list,
                if dynamic { Value::T } else { Value::Nil },
            ],
        );
        let Value::Record(function_record_id) = function else {
            unreachable!("native function is a pseudovector")
        };
        registry.functions.insert(
            function_record_id,
            NativeFunction {
                target,
                convention,
                min_args,
                max_args,
                dynamic,
            },
        );
        registry
            .function_names
            .insert(function_record_id, symbol_name.into_boxed_str());

        if matches!(kind, RegistrationKind::Lambda) {
            let (lambda_guard, lambda_name_index) = {
                let unit = interpreter
                    .find_record(unit_record_id)
                    .filter(|record| record.kind == RecordKind::NativeCompUnit)
                    .ok_or_else(|| super::lisp::native_ice("missing native compilation unit"))?;
                let guard = unit.slots.get(2).cloned().ok_or_else(|| {
                    super::lisp::native_ice("native compilation unit has no lambda guard")
                })?;
                let index = unit.slots.get(3).cloned().ok_or_else(|| {
                    super::lisp::native_ice("native compilation unit has no lambda name index")
                })?;
                (guard, index)
            };
            super::lisp::call_c_primitive(
                interpreter,
                environment,
                "puthash",
                &[function.clone(), Value::T, lambda_guard],
            )?;
            let old_index = super::lisp::call_c_primitive(
                interpreter,
                environment,
                "gethash",
                &[arguments[1].clone(), lambda_name_index.clone(), Value::Nil],
            )?;
            if old_index.is_truthy() {
                return Err(super::lisp::native_ice(
                    "duplicate anonymous native function C name",
                ));
            }
            super::lisp::call_c_primitive(
                interpreter,
                environment,
                "puthash",
                &[
                    arguments[1].clone(),
                    arguments[0].clone(),
                    lambda_name_index,
                ],
            )?;
            let relocation = usize::try_from(arguments[0].as_integer()?)
                .map_err(|_| super::lisp::native_ice("negative lambda relocation index"))?;
            let word = runtime.encode_relocations(std::slice::from_ref(&function))?[0];
            let unit = registry
                .unit(unit_record_id)
                .expect("unit checked before registration");
            if relocation >= unit.impure_relocation_count {
                return Err(super::lisp::native_ice(
                    "native lambda relocation index is out of range",
                ));
            }
            unsafe { std::ptr::write(unit.impure_relocations.add(relocation), word) };
        } else {
            interpreter.defalias_value(
                &[arguments[0].clone(), function.clone(), Value::Nil],
                environment,
            )?;
        }
        Ok(function)
    })()?;

    if let Some(table) = late_pending_table {
        super::lisp::call_c_primitive(
            interpreter,
            environment,
            "remhash",
            &[arguments[0].clone(), table],
        )?;
        Ok(Value::Nil)
    } else {
        Ok(registered)
    }
}

pub(crate) fn register_active(
    interpreter: &mut Interpreter,
    environment: &mut Env,
    arguments: &[Value],
    kind: RegistrationKind,
) -> Option<Result<Value, LispError>> {
    if ACTIVE_REGISTRY.with(Cell::get).is_null() {
        return None;
    }
    Some(
        with_active_registry(|registry| {
            super::runtime::with_current_runtime(|runtime| {
                register_with_state(registry, runtime, interpreter, environment, arguments, kind)
            })
            .unwrap_or_else(|| {
                Err(super::lisp::native_ice(
                    "active native registry has no active runtime",
                ))
            })
        })
        .expect("active registry checked above"),
    )
}

pub(crate) fn call_active_function(
    interpreter: &mut Interpreter,
    environment: &mut Env,
    record_id: u64,
    arguments: &[Value],
) -> Option<Result<Value, LispError>> {
    let function = with_active_registry(|registry| registry.function(record_id)).flatten()?;
    Some(call_function_with_runtime(
        function,
        record_id,
        interpreter,
        environment,
        arguments,
    ))
}

pub(crate) fn call_function(
    compiler: &RefCell<Option<Compiler>>,
    registry: &mut NativeRegistry,
    runtime: &mut NativeRuntime,
    interpreter: &mut Interpreter,
    environment: &mut Env,
    record_id: u64,
    arguments: &[Value],
) -> Result<Value, LispError> {
    let function = registry
        .function(record_id)
        .ok_or_else(|| {
            let mut registered = registry.functions.keys().copied().collect::<Vec<_>>();
            registered.sort_unstable();
            super::lisp::native_ice(&format!(
                "native function record {record_id} is not registered; registered records: {registered:?}"
            ))
    })?;
    check_arity(function, arguments.len())?;
    with_native_state(compiler, registry, runtime, |runtime| {
        invoke_function(
            function,
            record_id,
            runtime,
            interpreter,
            environment,
            arguments,
        )
    })
}

fn call_function_with_runtime(
    function: NativeFunction,
    record_id: u64,
    interpreter: &mut Interpreter,
    environment: &mut Env,
    arguments: &[Value],
) -> Result<Value, LispError> {
    check_arity(function, arguments.len())?;
    if let Some(result) = super::runtime::with_current_runtime(|runtime| {
        invoke_function(
            function,
            record_id,
            runtime,
            interpreter,
            environment,
            arguments,
        )
    }) {
        return result;
    }
    with_active_registered_runtime(|runtime| {
        invoke_function(
            function,
            record_id,
            runtime,
            interpreter,
            environment,
            arguments,
        )
    })
    .ok_or_else(|| super::lisp::native_ice("active native function has no runtime"))?
}

fn invoke_function(
    function: NativeFunction,
    record_id: u64,
    runtime: &mut NativeRuntime,
    interpreter: &mut Interpreter,
    environment: &mut Env,
    arguments: &[Value],
) -> Result<Value, LispError> {
    if !function.dynamic {
        return runtime.invoke(
            interpreter,
            environment,
            function.target,
            function.convention,
            arguments,
        );
    }

    // eval.c:funcall_lambda owns the calling convention for Lisp/d native
    // functions.  It binds the recorded lambda list dynamically and enters
    // the generated function through its zero-argument machine entry.
    let record = interpreter
        .find_record(record_id)
        .filter(|record| record.kind == RecordKind::NativeCompiledFunction)
        .ok_or_else(|| super::lisp::native_ice("missing dynamic native function record"))?;
    let lambda_list = record
        .slots
        .get(9)
        .cloned()
        .ok_or_else(|| super::lisp::native_ice("dynamic native function has no lambda list"))?;
    let parameters = lambda_list.to_vec()?;
    let mut argument_index = 0;
    let mut optional = false;
    let mut rest = false;
    let mut previous_rest = false;
    let mut restores = Vec::new();
    let setup = (|| -> Result<(), LispError> {
        for parameter in parameters {
            let parameter = crate::lisp::primitives::symbol_with_pos_parts(interpreter, &parameter)
                .map(|(symbol, _)| symbol)
                .unwrap_or(parameter);
            let name = parameter
                .as_symbol()
                .map_err(|_| invalid_function(record_id))?;
            match name {
                "&rest" => {
                    if rest || previous_rest {
                        return Err(invalid_function(record_id));
                    }
                    rest = true;
                    previous_rest = true;
                }
                "&optional" => {
                    if optional || rest || previous_rest {
                        return Err(invalid_function(record_id));
                    }
                    optional = true;
                }
                _ => {
                    let value = if rest {
                        let value = Value::list(
                            arguments
                                .get(argument_index..)
                                .unwrap_or_default()
                                .iter()
                                .cloned(),
                        );
                        argument_index = arguments.len();
                        value
                    } else if let Some(value) = arguments.get(argument_index) {
                        argument_index += 1;
                        value.clone()
                    } else if optional {
                        Value::Nil
                    } else {
                        return Err(LispError::WrongNumberOfArgs(
                            "native-compiled-function".into(),
                            arguments.len(),
                        ));
                    };
                    restores.push(interpreter.bind_special_dynamic(name, value, environment)?);
                    previous_rest = false;
                }
            }
        }
        if previous_rest {
            return Err(invalid_function(record_id));
        }
        if argument_index < arguments.len() {
            return Err(LispError::WrongNumberOfArgs(
                "native-compiled-function".into(),
                arguments.len(),
            ));
        }
        Ok(())
    })();

    let result = match setup {
        Ok(()) => runtime.invoke(
            interpreter,
            environment,
            function.target,
            NativeCallingConvention::Fixed,
            &[],
        ),
        Err(error) => Err(error),
    };
    let mut restore_error = None;
    for restore in restores.into_iter().rev() {
        if let Err(error) = interpreter.restore_special_dynamic(restore, environment)
            && restore_error.is_none()
        {
            restore_error = Some(error);
        }
    }
    match result {
        Ok(value) => restore_error.map_or(Ok(value), Err),
        Err(error) => Err(error),
    }
}

fn invalid_function(record_id: u64) -> LispError {
    LispError::SignalValue(Value::list([
        Value::symbol("invalid-function"),
        Value::Record(record_id),
    ]))
}

fn check_arity(function: NativeFunction, count: usize) -> Result<(), LispError> {
    if count < function.min_args || function.max_args.is_some_and(|maximum| count > maximum) {
        return Err(LispError::WrongNumberOfArgs(
            "native-compiled-function".into(),
            count,
        ));
    }
    Ok(())
}

pub(crate) fn function_target(
    registry: &NativeRegistry,
    record_id: u64,
) -> Option<(*mut c_void, NativeCallingConvention)> {
    registry
        .function(record_id)
        .map(|function| (function.target.cast_mut(), function.convention))
}

pub(crate) fn active_function_target(
    record_id: u64,
) -> Option<(*mut c_void, NativeCallingConvention)> {
    with_active_registry(|registry| function_target(registry, record_id)).flatten()
}

pub(crate) fn active_function_name(record_id: u64) -> Option<String> {
    with_active_registry(|registry| registry.function_name(record_id).map(str::to_owned)).flatten()
}

/// comp.c:native_function_doc's lazy static-object load.  The returned
/// vector is installed in the compilation unit before its indexed element is
/// read, so every later request reuses the same Lisp object.
pub(crate) fn unit_documentation(
    registry: &NativeRegistry,
    interpreter: &mut Interpreter,
    environment: &mut Env,
    record_id: u64,
) -> Result<Value, LispError> {
    let unit = registry
        .unit(record_id)
        .ok_or_else(|| super::lisp::native_ice("native compilation unit is not loaded"))?;
    let file = interpreter
        .find_record(record_id)
        .filter(|record| record.kind == RecordKind::NativeCompUnit)
        .and_then(|record| record.slots.first())
        .cloned()
        .ok_or_else(|| super::lisp::native_ice("native compilation unit record is missing"))?;
    let docs = unsafe {
        read_static_object(
            &unit.library,
            &file,
            TEXT_FDOC_SYM,
            interpreter,
            environment,
        )?
    };
    if !is_vector_value(&docs) {
        return Err(LispError::SignalValue(Value::list([
            Value::symbol("native-lisp-file-inconsistent"),
            file,
            Value::string("missing documentation vector"),
        ])));
    }
    let record = interpreter
        .find_record_mut(record_id)
        .filter(|record| record.kind == RecordKind::NativeCompUnit)
        .ok_or_else(|| super::lisp::native_ice("native compilation unit record is missing"))?;
    let slot = record.slots.get_mut(4).ok_or_else(|| {
        super::lisp::native_ice("native compilation unit has no documentation slot")
    })?;
    *slot = docs.clone();
    Ok(docs)
}

pub(crate) fn active_unit_documentation(
    interpreter: &mut Interpreter,
    environment: &mut Env,
    record_id: u64,
) -> Option<Result<Value, LispError>> {
    with_active_registry(|registry| {
        unit_documentation(registry, interpreter, environment, record_id)
    })
}

pub(crate) fn active_direct_function(record_id: u64) -> Option<DirectNativeFunction> {
    with_active_registry(|registry| {
        let function = registry.function(record_id)?;
        (!function.dynamic).then_some(DirectNativeFunction {
            target: function.target,
            convention: function.convention,
            min_args: function.min_args,
            max_args: function.max_args,
        })
    })
    .flatten()
}
