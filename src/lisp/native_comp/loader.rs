//! `.eln` loading and native subroutine ownership.
//!
//! This is the Rust implementation of `comp.c`'s dynamic-loader boundary.
//! The shared object contains only code emitted from GNU's unchanged
//! `comp.el` input; all runtime addresses installed below point into Emaxx's
//! Rust runtime or the loaded compilation unit itself.

use super::backend::{
    COMP_UNIT_SYM, CURRENT_THREAD_RELOC_SYM, DATA_RELOC_EPHEMERAL_SYM, DATA_RELOC_IMPURE_SYM,
    DATA_RELOC_SYM, F_SYMBOLS_WITH_POS_ENABLED_RELOC_SYM, FUNC_LINK_TABLE_SYM, LINK_TABLE_HASH_SYM,
    PURE_RELOC_SYM, TEXT_DATA_RELOC_EPHEMERAL_SYM, TEXT_DATA_RELOC_IMPURE_SYM, TEXT_DATA_RELOC_SYM,
    TEXT_FDOC_SYM, TEXT_OPTIM_QLY_SYM,
};
use super::runtime::{NativeCallingConvention, NativeRuntime, NativeWord};
use crate::lisp::eval::{Interpreter, RecordKind};
use crate::lisp::primitives::{decode_utf8_bytes, read_one_form_in_env, string_like, values_equal};
use crate::lisp::types::{Env, LispError, Value};
use libloading::Library;
use std::cell::Cell;
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

struct LoadedUnit {
    _library: Library,
    _file: String,
    record_id: u64,
    _data: Value,
    _impure_data: Value,
    _ephemeral_data: Value,
    _optimization_qualities: Value,
    _function_docs: Value,
    impure_relocations: *mut NativeWord,
    impure_relocation_count: usize,
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

thread_local! {
    static ACTIVE_REGISTRY: Cell<*mut NativeRegistry> = const { Cell::new(std::ptr::null_mut()) };
}

struct RegistryGuard {
    previous: *mut NativeRegistry,
}

impl Drop for RegistryGuard {
    fn drop(&mut self) {
        ACTIVE_REGISTRY.set(self.previous);
    }
}

fn with_registry<R>(registry: &mut NativeRegistry, body: impl FnOnce() -> R) -> R {
    let previous = ACTIVE_REGISTRY.replace(registry);
    let _guard = RegistryGuard { previous };
    body()
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

pub(crate) fn load(
    registry: &mut NativeRegistry,
    runtime: &mut NativeRuntime,
    interpreter: &mut Interpreter,
    environment: &mut Env,
    filename: &str,
    late: bool,
) -> Result<Value, LispError> {
    let path = Path::new(filename);
    let library =
        unsafe { Library::new(path) }.map_err(|error| load_failed(filename, error.to_string()))?;
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
    let ephemeral_data = unsafe {
        read_static_object(
            &library,
            filename,
            TEXT_DATA_RELOC_EPHEMERAL_SYM,
            interpreter,
            environment,
        )?
    };
    let data_values = vector_values(&data)?;
    let impure_values = vector_values(&impure_data)?;
    let ephemeral_values = vector_values(&ephemeral_data)?;

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

    let saved_unit = unsafe { data_symbol::<NativeWord>(&library, COMP_UNIT_SYM) }
        .map_err(|error| super::lisp::native_ice(&error))?;
    if saved_unit.is_null() || unsafe { std::ptr::read(saved_unit) } != 0 {
        return Err(inconsistent(filename));
    }
    unsafe { std::ptr::write(saved_unit, unit_word) };
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
    unsafe { fill_relocations(&library, DATA_RELOC_SYM, runtime, &data_values)? };
    let impure_relocations =
        unsafe { fill_relocations(&library, DATA_RELOC_IMPURE_SYM, runtime, &impure_values)? };
    unsafe {
        fill_relocations(
            &library,
            DATA_RELOC_EPHEMERAL_SYM,
            runtime,
            &ephemeral_values,
        )?
    };
    let top_level_name = if late {
        LATE_TOP_LEVEL_RUN_SYM
    } else {
        TOP_LEVEL_RUN_SYM
    };
    let top_level =
        unsafe { function_symbol(&library, top_level_name) }.map_err(|_| inconsistent(filename))?;

    registry.units.push(LoadedUnit {
        _library: library,
        _file: filename.to_string(),
        record_id,
        _data: data,
        _impure_data: impure_data,
        _ephemeral_data: ephemeral_data,
        _optimization_qualities: optimization_qualities,
        _function_docs: function_docs,
        impure_relocations,
        impure_relocation_count: impure_values.len(),
    });
    with_registry(registry, || {
        runtime.invoke(
            interpreter,
            environment,
            top_level,
            NativeCallingConvention::Fixed,
            std::slice::from_ref(&unit),
        )
    })
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
    late: bool,
) -> Option<Result<Value, LispError>> {
    with_active_registry(|registry| {
        super::runtime::with_current_runtime(|runtime| {
            load(registry, runtime, interpreter, environment, filename, late)
        })
        .unwrap_or_else(|| {
            Err(super::lisp::native_ice(
                "active native registry has no active runtime",
            ))
        })
    })
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
        let target = unsafe { function_symbol(&unit._library, &c_name) }
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
    with_registry(registry, || {
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
    super::runtime::with_current_runtime(|runtime| {
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
