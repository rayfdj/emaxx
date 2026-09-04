//! Native Lisp compiler backend.
//!
//! GNU's `comp.el` remains the frontend and owns byte compilation, LIMPLE,
//! optimization, and relocation classification.  This module owns the same
//! backend boundary as GNU's `comp.c`: libgccjit code generation, `.eln`
//! artifacts, relocation, loading, and native subroutine lifetime.

mod abi;
mod backend;
mod gccjit;
// One generated table per supported target.  Each is the pinned GNU
// reference build's C subroutine registration order for that target and
// carries the configuration strings that feed comp.c:hash_native_abi.
#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
#[path = "generated_native_subrs_aarch64_apple_darwin.rs"]
mod generated_native_subrs;
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
#[path = "generated_native_subrs_x86_64_unknown_linux_gnu.rs"]
mod generated_native_subrs;
mod lisp;
mod loader;
mod runtime;
mod state;

pub(crate) use loader::{RegistrationKind, UnitLibrary, open_unit};
pub(crate) use runtime::{
    decode_active_backtrace_arguments, note_lisp_allocation, synchronize_cons_read,
};
pub(crate) use state::NativeCompilerState;

use crate::lisp::eval::Interpreter;
use crate::lisp::types::{Env, LispError, Value};

pub(crate) fn initialize_runtime(interpreter: &mut Interpreter) {
    let subrs = abi::native_subrs();
    let mut signatures = String::new();
    for subr in subrs {
        let maximum = match subr.max_args {
            abi::NativeMaxArgs::Fixed(maximum) => maximum.to_string(),
            abi::NativeMaxArgs::Many => "many".to_string(),
            abi::NativeMaxArgs::Unevalled => "unevalled".to_string(),
        };
        signatures.push_str(subr.name);
        signatures.push('(');
        signatures.push_str(&subr.min_args.to_string());
        signatures.push_str(" . ");
        signatures.push_str(&maximum);
        signatures.push(')');
    }
    let identity = format!(
        "{}{}{}{}{}",
        generated_native_subrs::NATIVE_ABI_VERSION,
        crate::lisp::primitives::emacs_version_value(),
        generated_native_subrs::NATIVE_ABI_SYSTEM_CONFIGURATION,
        generated_native_subrs::NATIVE_ABI_SYSTEM_CONFIGURATION_OPTIONS,
        signatures,
    );
    let abi_hash = format!("{:x}", md5::compute(identity.as_bytes()))[..8].to_string();
    let version_directory = format!(
        "{}-{abi_hash}",
        crate::lisp::primitives::emacs_version_value()
    );
    interpreter.define_special_variable(
        "comp-subr-list",
        Value::list(
            subrs
                .iter()
                .map(|subr| Value::BuiltinFunc(subr.name.into())),
        ),
    );
    interpreter.define_special_variable("comp-abi-hash", Value::string(&abi_hash));
    interpreter
        .define_special_variable("comp-native-version-dir", Value::string(&version_directory));
    for (name, test) in [
        ("comp-deferred-pending-h", "eq"),
        ("comp-eln-to-el-h", "equal"),
        ("comp-installed-trampolines-h", "eql"),
        ("comp-no-native-file-h", "equal"),
        ("comp-loaded-comp-units-h", "equal"),
        ("comp-subr-arities-h", "equal"),
    ] {
        let table = crate::lisp::json::make_hash_table(interpreter, test, Vec::new());
        if name == "comp-loaded-comp-units-h" {
            let Value::Record(id) = &table else {
                unreachable!("native compiler hash tables use hash-table records")
            };
            let record = interpreter
                .find_record_mut(*id)
                .expect("new native compiler hash table record");
            if record.slots.len() < 7 {
                record.slots.resize(7, Value::Nil);
            }
            // comp.c:5926 uses `:weakness value'.  This is observable through
            // hash-table-weakness and controls whether compilation units keep
            // their Lisp values alive.
            record.slots[5] = Value::symbol("value");
        }
        interpreter.define_special_variable(name, table);
    }
    interpreter.define_special_variable(
        "native-comp-eln-load-path",
        Value::list([Value::string("../native-lisp/")]),
    );
    // comp.c leaves this nil.  GNU's unchanged loadup.el enables it at the
    // portable-dump boundary once compiler bootstrap has completed.
    interpreter.define_special_variable("native-comp-enable-subr-trampolines", Value::Nil);
}

pub(crate) fn load(
    interpreter: &mut Interpreter,
    environment: &mut Env,
    filename: &Value,
    library: UnitLibrary,
    candidate_unit: &Value,
    late: bool,
) -> Result<Value, LispError> {
    let library = match loader::load_active(
        interpreter,
        environment,
        filename,
        library,
        candidate_unit,
        late,
    ) {
        Ok(result) => return result,
        Err(library) => library,
    };
    let mut state = std::mem::take(&mut interpreter.native_compiler);
    let result = state.load(
        interpreter,
        environment,
        filename,
        library,
        candidate_unit,
        late,
    );
    interpreter.native_compiler = state;
    result
}

pub(crate) fn call_function(
    interpreter: &mut Interpreter,
    environment: &mut Env,
    record_id: u64,
    arguments: &[Value],
) -> Result<Value, LispError> {
    if let Some(result) =
        loader::call_active_function(interpreter, environment, record_id, arguments)
    {
        return result;
    }
    let mut state = std::mem::take(&mut interpreter.native_compiler);
    let result = state.call_function(interpreter, environment, record_id, arguments);
    interpreter.native_compiler = state;
    result
}

pub(crate) fn function_documentation(
    interpreter: &mut Interpreter,
    environment: &mut Env,
    record_id: u64,
) -> Result<Value, LispError> {
    let (index, unit_id) = {
        let function = interpreter
            .find_record(record_id)
            .filter(|record| record.kind == crate::lisp::eval::RecordKind::NativeCompiledFunction)
            .ok_or_else(|| lisp::native_ice("native documentation requested for a non-function"))?;
        let index = function
            .slots
            .get(5)
            .ok_or_else(|| lisp::native_ice("native function has no documentation index"))?
            .as_integer()
            .and_then(|index| {
                usize::try_from(index)
                    .map_err(|_| lisp::native_ice("negative native documentation index"))
            })?;
        let unit_id = match function.slots.get(8) {
            Some(Value::Record(unit_id)) => *unit_id,
            _ => return Err(lisp::native_ice("native function has no compilation unit")),
        };
        (index, unit_id)
    };
    let docs = interpreter
        .find_record(unit_id)
        .filter(|record| record.kind == crate::lisp::eval::RecordKind::NativeCompUnit)
        .and_then(|record| record.slots.get(4))
        .cloned()
        .ok_or_else(|| lisp::native_ice("native function compilation unit is missing"))?;
    let docs = if docs.is_nil() {
        if let Some(result) = loader::active_unit_documentation(interpreter, environment, unit_id) {
            result?
        } else {
            let state = std::mem::take(&mut interpreter.native_compiler);
            let result = state.unit_documentation(interpreter, environment, unit_id);
            interpreter.native_compiler = state;
            result?
        }
    } else {
        docs
    };
    let mut docs = docs.to_vec()?;
    if !matches!(docs.first(), Some(Value::Symbol(name)) if name == "vector-literal") {
        return Err(lisp::native_ice(
            "native compilation unit documentation is not a vector",
        ));
    }
    docs.remove(0);
    docs.get(index)
        .cloned()
        .ok_or_else(|| lisp::native_ice("native documentation index is out of range"))
}

pub(crate) fn function_name(interpreter: &Interpreter, record_id: u64) -> Option<String> {
    loader::active_function_name(record_id).or_else(|| {
        interpreter
            .native_compiler
            .function_name(record_id)
            .map(str::to_owned)
    })
}

pub(crate) fn register(
    interpreter: &mut Interpreter,
    environment: &mut Env,
    arguments: &[Value],
    kind: RegistrationKind,
) -> Result<Value, LispError> {
    if let Some(result) = loader::register_active(interpreter, environment, arguments, kind) {
        return result;
    }
    let mut state = std::mem::take(&mut interpreter.native_compiler);
    let result = state.register(interpreter, environment, arguments, kind);
    interpreter.native_compiler = state;
    result
}

pub(crate) fn subroutine_index(name: &str) -> Option<usize> {
    abi::native_subrs()
        .iter()
        .position(|subroutine| subroutine.name == name)
}

pub(crate) fn install_trampoline(
    interpreter: &mut Interpreter,
    subroutine_index: usize,
    record_id: u64,
) -> Result<(), LispError> {
    if let Some((target, _convention)) = loader::active_function_target(record_id) {
        return runtime::with_current_runtime(|runtime| {
            runtime.install_trampoline(subroutine_index, target)
        })
        .ok_or_else(|| lisp::native_ice("active native trampoline has no runtime"))?
        .map_err(|error| lisp::native_ice(&error));
    }
    interpreter
        .native_compiler
        .install_trampoline(subroutine_index, record_id)
}

pub(crate) fn call_lisp(
    interpreter: &mut Interpreter,
    environment: &mut Env,
    name: &str,
    arguments: &[Value],
) -> Result<Value, LispError> {
    lisp::call(interpreter, environment, name, arguments)
}

pub(crate) fn call_c_primitive(
    interpreter: &mut Interpreter,
    environment: &mut Env,
    name: &str,
    arguments: &[Value],
) -> Result<Value, LispError> {
    lisp::call_c_primitive(interpreter, environment, name, arguments)
}

pub(crate) fn garbage_collection_finished(
    interpreter: &mut Interpreter,
    live_bytes: usize,
    threshold: i64,
    percentage: Option<f64>,
) {
    if runtime::with_current_runtime(|runtime| {
        runtime.garbage_collection_finished(live_bytes, threshold, percentage);
    })
    .is_some()
    {
        return;
    }
    let mut state = std::mem::take(&mut interpreter.native_compiler);
    state.garbage_collection_finished(live_bytes, threshold, percentage);
    interpreter.native_compiler = state;
}

pub(crate) fn begin_garbage_collection(interpreter: &mut Interpreter) -> Vec<Value> {
    if let Some(roots) = runtime::with_current_runtime(|runtime| runtime.begin_garbage_collection())
    {
        return roots;
    }
    let mut state = std::mem::take(&mut interpreter.native_compiler);
    let roots = state.begin_garbage_collection();
    interpreter.native_compiler = state;
    roots
}
