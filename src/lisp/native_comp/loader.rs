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
use crate::lisp::primitives::{decode_utf8_bytes, read_one_form_in_env, string_like, values_equal};
use crate::lisp::types::{Env, LispError, Value};
use libloading::Library;
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::ffi::c_void;
use std::path::Path;

const TOP_LEVEL_RUN_SYM: &str = "top_level_run";
const LATE_TOP_LEVEL_RUN_SYM: &str = "late_top_level_run";
const MAX_STATIC_OBJECT_BYTES: usize = 1 << 30;

#[repr(C)]
struct StaticObjectHeader {
    len: isize,
}

pub(crate) type UnitLibrary = Library;

struct LoadedUnit {
    library: Library,
    record_id: u64,
    /// comp.c's `load_ongoing`: a unit whose top-level code is still running
    /// on this thread must not have its ephemeral relocations rewritten by
    /// a nested load of the same file.
    load_ongoing: bool,
    _data: Value,
    _impure_data: Value,
    _optimization_qualities: Value,
    _function_docs: Value,
    impure_relocations: *mut NativeWord,
    impure_relocation_count: usize,
}

struct EphemeralRelocations {
    _guard: Value,
    start: *const NativeWord,
    len: usize,
}

#[derive(Clone, Copy)]
struct NativeFunction {
    target: *const c_void,
    convention: NativeCallingConvention,
    min_args: usize,
    max_args: Option<usize>,
    dynamic: bool,
}

#[derive(Default)]
pub(crate) struct NativeRegistry {
    units: Vec<LoadedUnit>,
    functions: HashMap<u64, NativeFunction>,
}

impl NativeRegistry {
    pub(crate) fn is_empty(&self) -> bool {
        self.units.is_empty() && self.functions.is_empty()
    }

    fn unit(&self, record_id: u64) -> Option<&LoadedUnit> {
        self.units.iter().find(|unit| unit.record_id == record_id)
    }

    fn function(&self, record_id: u64) -> Option<NativeFunction> {
        self.functions.get(&record_id).copied()
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

fn inconsistent(file: &str) -> LispError {
    LispError::SignalValue(Value::list([
        Value::symbol("native-lisp-file-inconsistent"),
        Value::string(file),
    ]))
}

fn load_failed(file: &str, message: impl Into<String>) -> LispError {
    LispError::SignalValue(Value::list([
        Value::symbol("native-lisp-load-failed"),
        Value::string(file),
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
    file: &str,
    name: &str,
    interpreter: &mut Interpreter,
    environment: &mut Env,
) -> Result<Value, LispError> {
    let blob_name = format!("{name}_blob");
    let blob = unsafe { data_symbol::<StaticObjectHeader>(library, &blob_name) }
        .map_err(|_| inconsistent(file))?;
    if blob.is_null() {
        return Err(inconsistent(file));
    }
    let len = unsafe { std::ptr::read_unaligned(std::ptr::addr_of!((*blob).len)) };
    let len = usize::try_from(len).map_err(|_| inconsistent(file))?;
    if len == 0 || len > MAX_STATIC_OBJECT_BYTES {
        return Err(inconsistent(file));
    }
    let bytes = unsafe {
        std::slice::from_raw_parts(
            blob.cast::<u8>()
                .add(std::mem::size_of::<StaticObjectHeader>()),
            len,
        )
    };
    let bytes = bytes.strip_suffix(&[0]).unwrap_or(bytes);
    let text = decode_utf8_bytes(bytes);
    let (value, _) = read_one_form_in_env(interpreter, &text, environment)?;
    let value = interpreter.materialize_read_object_literals(value)?;
    interpreter.intern_symbols_in_value(&value);
    Ok(value)
}

fn vector_values(value: &Value) -> Result<Vec<Value>, LispError> {
    let mut values = value.to_vec()?;
    if !matches!(values.first(), Some(Value::Symbol(name)) if name == "vector-literal") {
        return Err(super::lisp::native_ice(
            "serialized native relocation data is not a vector",
        ));
    }
    values.remove(0);
    Ok(values)
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
pub(crate) fn open_unit(file: &str, path: &str) -> Result<Library, LispError> {
    unsafe { Library::new(Path::new(path)) }.map_err(|error| load_failed(file, error.to_string()))
}

/// comp.c:load_comp_unit for an in-process (non-dump) load.
pub(super) fn load(
    state: LoaderState<'_>,
    interpreter: &mut Interpreter,
    environment: &mut Env,
    filename: &str,
    library: Library,
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
    let top_level_name = if late {
        LATE_TOP_LEVEL_RUN_SYM
    } else {
        TOP_LEVEL_RUN_SYM
    };
    let top_level =
        unsafe { function_symbol(&library, top_level_name) }.map_err(|_| inconsistent(filename))?;

    let (record_id, unit) = if saved_word != 0 {
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
        (record_id, unit)
    } else {
        first_load(
            registry,
            runtime,
            interpreter,
            environment,
            filename,
            library,
            saved_unit,
        )?
    };

    let index = registry
        .units
        .iter()
        .position(|loaded| loaded.record_id == record_id)
        .expect("unit registered before its top-level code runs");
    let recursive_load = registry.units[index].load_ongoing;
    registry.units[index].load_ongoing = true;
    let ephemeral = if recursive_load {
        // Another load of this unit is active on the stack and holds the
        // ephemeral data; rewriting it would clobber objects in use.
        Ok(None)
    } else {
        fill_ephemeral_relocations(
            &registry.units[index].library,
            filename,
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
    if !recursive_load {
        registry.units[index].load_ongoing = false;
    }
    let result = result?;
    // comp.c:register_native_comp_unit.
    let loaded_units = interpreter
        .lookup_var("comp-loaded-comp-units-h", environment)
        .unwrap_or(Value::Nil);
    super::lisp::call(
        interpreter,
        environment,
        "puthash",
        &[Value::string(filename), unit, loaded_units],
    )?;
    Ok(result)
}

/// The `!loaded_once` half of comp.c:load_comp_unit: verify the ABI hash,
/// materialize the unit's static data, and install every runtime pointer
/// and data relocation the generated code addresses.
fn first_load(
    registry: &mut NativeRegistry,
    runtime: &mut NativeRuntime,
    interpreter: &mut Interpreter,
    environment: &mut Env,
    filename: &str,
    library: Library,
    saved_unit: *mut NativeWord,
) -> Result<(u64, Value), LispError> {
    let abi_hash = unsafe {
        read_static_object(
            &library,
            filename,
            LINK_TABLE_HASH_SYM,
            interpreter,
            environment,
        )?
    };
    let expected_hash = interpreter
        .lookup_var("comp-abi-hash", environment)
        .filter(Value::is_truthy)
        .ok_or_else(|| inconsistent(filename))?;
    if !values_equal(interpreter, &abi_hash, &expected_hash) {
        return Err(inconsistent(filename));
    }

    let optimization_qualities = unsafe {
        read_static_object(
            &library,
            filename,
            TEXT_OPTIM_QLY_SYM,
            interpreter,
            environment,
        )?
    };
    let function_docs =
        unsafe { read_static_object(&library, filename, TEXT_FDOC_SYM, interpreter, environment)? };
    let data = unsafe {
        read_static_object(
            &library,
            filename,
            TEXT_DATA_RELOC_SYM,
            interpreter,
            environment,
        )?
    };
    let impure_data = unsafe {
        read_static_object(
            &library,
            filename,
            TEXT_DATA_RELOC_IMPURE_SYM,
            interpreter,
            environment,
        )?
    };
    let data_values = vector_values(&data)?;
    let impure_values = vector_values(&impure_data)?;

    let unit = interpreter.create_pseudovector(
        RecordKind::NativeCompUnit,
        "native-comp-unit",
        vec![Value::string(filename), function_docs.clone()],
    );
    let Value::Record(record_id) = unit else {
        unreachable!("native compilation unit is a pseudovector")
    };
    let unit = Value::Record(record_id);
    let unit_word = runtime.encode_relocations(std::slice::from_ref(&unit))?[0];
    unsafe { std::ptr::write(saved_unit, unit_word) };
    runtime.register_permanent_root_range(saved_unit, 1);
    unsafe {
        initialize_pointer(
            &library,
            CURRENT_THREAD_RELOC_SYM,
            runtime.current_thread_relocation(),
        )
        .map_err(|_| inconsistent(filename))?;
        initialize_pointer(
            &library,
            F_SYMBOLS_WITH_POS_ENABLED_RELOC_SYM,
            runtime.symbols_with_positions_relocation(),
        )
        .map_err(|_| inconsistent(filename))?;
        initialize_pointer(&library, PURE_RELOC_SYM, runtime.pure_relocation())
            .map_err(|_| inconsistent(filename))?;
        initialize_pointer(&library, FUNC_LINK_TABLE_SYM, runtime.function_link_table())
            .map_err(|_| inconsistent(filename))?;
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
        load_ongoing: false,
        _data: data,
        _impure_data: impure_data,
        _optimization_qualities: optimization_qualities,
        _function_docs: function_docs,
        impure_relocations,
        impure_relocation_count: impure_values.len(),
    });
    Ok((record_id, unit))
}

/// Ephemeral data is read and installed on every non-recursive load; GNU
/// keeps it alive only for the duration of the top-level run.
fn fill_ephemeral_relocations(
    library: &Library,
    filename: &str,
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
    filename: &str,
    library: Library,
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

pub(crate) fn register(
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

    let late_pending_table = if matches!(kind, RegistrationKind::LateSubroutine) {
        let table = interpreter
            .lookup_var("comp-deferred-pending-h", environment)
            .unwrap_or(Value::Nil);
        let pending = super::lisp::call(
            interpreter,
            environment,
            "gethash",
            &[arguments[0].clone(), table.clone(), Value::Nil],
        )?;
        let current = arguments[0]
            .as_symbol()
            .ok()
            .and_then(|name| interpreter.lookup_function(name, environment).ok());
        if current
            .as_ref()
            .is_none_or(|current| !values_equal(interpreter, current, &pending))
        {
            super::lisp::call(
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

    let registered = with_active_registry(|registry| {
        let unit = registry
            .unit(unit_record_id)
            .ok_or_else(|| super::lisp::native_ice("unknown native compilation unit"))?;
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
                Value::string(&symbol_name),
                Value::Integer(min_args as i64),
                max_value.clone(),
                Value::string(&c_name),
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

        if matches!(kind, RegistrationKind::Lambda) {
            let relocation = usize::try_from(arguments[0].as_integer()?)
                .map_err(|_| super::lisp::native_ice("negative lambda relocation index"))?;
            let word = super::runtime::with_current_runtime(|runtime| {
                runtime.encode_relocations(std::slice::from_ref(&function))
            })
            .ok_or_else(|| {
                super::lisp::native_ice("native registration outside a native call")
            })??[0];
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
    })
    .ok_or_else(|| super::lisp::native_ice("native registration outside a native load"))??;

    if let Some(table) = late_pending_table {
        super::lisp::call(
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
