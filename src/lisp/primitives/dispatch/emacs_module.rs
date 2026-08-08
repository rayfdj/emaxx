use super::*;
use libloading::Library;
use std::ffi::{c_int, c_void};

type ModuleInit = unsafe extern "C" fn(*mut c_void) -> c_int;

fn module_condition(name: &str, values: impl IntoIterator<Item = Value>) -> LispError {
    LispError::SignalValue(Value::list(
        std::iter::once(Value::symbol(name)).chain(values),
    ))
}

fn module_load(file: &Value) -> Result<Value, LispError> {
    let path = string_like(file)
        .map(|string| string.text)
        .ok_or_else(|| wrong_type_argument("stringp", file.clone()))?;
    // SAFETY: Opening the caller-selected dynamic library is this primitive's
    // documented boundary.  No foreign code is invoked below.
    let library = unsafe { Library::new(&path) }.map_err(|error| {
        module_condition(
            "module-open-failed",
            [file.clone(), Value::String(error.to_string().into())],
        )
    })?;
    // SAFETY: Only symbol presence is inspected.  The exported data is never
    // dereferenced and the initialization function is deliberately not called.
    unsafe {
        library
            .get::<*mut c_void>(b"plugin_is_GPL_compatible")
            .map_err(|_| module_condition("module-not-gpl-compatible", [file.clone()]))?;
        library
            .get::<ModuleInit>(b"emacs_module_init")
            .map_err(|_| module_condition("missing-module-init-function", [file.clone()]))?;
    }
    Err(LispError::Signal(
        "GNU dynamic module ABI is unavailable in the Rust value backend".into(),
    ))
}

define_dispatch!(
    pub(super) fn call(name: &str, args: &[Value]) -> Result<Value, LispError> {
        match name {
            "module-load" => {
                need_args(name, args, 1)?;
                module_load(&args[0])
            }
        }
    }
);
