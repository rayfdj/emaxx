//! Calls from the native backend into GNU's unchanged `comp.el` frontend.
//!
//! The functions in this module deliberately do not know the layout of
//! `comp-ctxt`, `comp-func`, or `comp-mvar` records.  GNU keeps those records
//! and their policy in Lisp, so the Rust port of `comp.c` obtains every value
//! through the same Lisp accessors that the C implementation calls.

use super::abi::native_subrs;
use super::backend::{
    CoreRelocations, FunctionCallingConvention, FunctionDeclaration, HELPER_NAMES, Relocation,
    RelocationArrayKind, SerializedRelocation, UnitInput,
};
use crate::lisp::eval::{Interpreter, SpecialBindingRestore};
use crate::lisp::json;
use crate::lisp::primitives::{encode_internal_multibyte_bytes, render_prin1, string_like};
use crate::lisp::types::{Env, LispError, Value};

pub(crate) struct UnitData {
    speed: i64,
    debug: i64,
    compiler_options: Vec<String>,
    driver_options: Vec<String>,
    reproducer: bool,
    optimization_qualities: Vec<u8>,
    function_docs: Vec<u8>,
    abi_hash: Vec<u8>,
    data: OwnedRelocation,
    impure_data: OwnedRelocation,
    ephemeral_data: OwnedRelocation,
    helper_c_names: [String; HELPER_NAMES.len()],
    subr_c_names: Vec<String>,
    core_relocations: CoreRelocations,
    functions: Vec<FunctionDeclaration>,
    function_values: Vec<Value>,
    default_index: Value,
    impure_index: Value,
    ephemeral_index: Value,
}

struct OwnedRelocation {
    len: usize,
    printed: Vec<u8>,
}

impl UnitData {
    /// Read the current compilation unit through GNU's public Lisp-side
    /// record accessors.  This is the Rust equivalent of the `CALL1I` calls
    /// in `comp.c`; it is not an alternative frontend.
    pub(crate) fn read(interp: &mut Interpreter, env: &mut Env) -> Result<Self, LispError> {
        let context = interp
            .lookup_var("comp-ctxt", env)
            .filter(Value::is_truthy)
            .ok_or_else(|| native_ice("comp-ctxt is nil"))?;

        let speed = call_one(interp, env, "comp-ctxt-speed", context.clone())?.as_integer()?;
        let debug = call_one(interp, env, "comp-ctxt-debug", context.clone())?.as_integer()?;
        let mut compiler_options = string_list(
            interp
                .lookup_var("native-comp-compiler-options", env)
                .unwrap_or(Value::Nil),
        )?;
        compiler_options.extend(string_list(call_one(
            interp,
            env,
            "comp-ctxt-compiler-options",
            context.clone(),
        )?)?);
        let mut driver_options = string_list(
            interp
                .lookup_var("native-comp-driver-options", env)
                .unwrap_or(Value::Nil),
        )?;
        driver_options.extend(string_list(call_one(
            interp,
            env,
            "comp-ctxt-driver-options",
            context.clone(),
        )?)?);
        let reproducer = interp
            .lookup_var("comp-libgccjit-reproducer", env)
            .is_some_and(|value| value.is_truthy());
        let optimization_qualities = print_static(
            interp,
            env,
            &Value::list([
                Value::cons(Value::symbol("native-comp-speed"), Value::Integer(speed)),
                Value::cons(Value::symbol("native-comp-debug"), Value::Integer(debug)),
                Value::cons(
                    Value::symbol("gccjit"),
                    super::state::NativeCompilerState::version()
                        .map(|(major, minor, patch)| {
                            Value::list([
                                Value::Integer(i64::from(major)),
                                Value::Integer(i64::from(minor)),
                                Value::Integer(i64::from(patch)),
                            ])
                        })
                        .unwrap_or(Value::Nil),
                ),
            ]),
        )?;

        let function_docs = call_one(interp, env, "comp-ctxt-function-docs", context.clone())?;
        let function_docs = print_static(interp, env, &function_docs)?;
        let abi_hash = interp
            .lookup_var("comp-abi-hash", env)
            .filter(Value::is_truthy)
            .ok_or_else(|| native_ice("comp-abi-hash is nil"))?;
        let abi_hash = print_static(interp, env, &abi_hash)?;

        let default_container = call_one(interp, env, "comp-ctxt-d-default", context.clone())?;
        let impure_container = call_one(interp, env, "comp-ctxt-d-impure", context.clone())?;
        let ephemeral_container = call_one(interp, env, "comp-ctxt-d-ephemeral", context.clone())?;
        let default_index = call_one(
            interp,
            env,
            "comp-data-container-idx",
            default_container.clone(),
        )?;
        let impure_index = call_one(
            interp,
            env,
            "comp-data-container-idx",
            impure_container.clone(),
        )?;
        let ephemeral_index = call_one(
            interp,
            env,
            "comp-data-container-idx",
            ephemeral_container.clone(),
        )?;

        let core_relocations = CoreRelocations {
            t: find_relocation(
                interp,
                env,
                &Value::T,
                &default_index,
                &impure_index,
                &ephemeral_index,
            )?,
            listp: find_relocation(
                interp,
                env,
                &Value::symbol("listp"),
                &default_index,
                &impure_index,
                &ephemeral_index,
            )?,
            consp: find_relocation(
                interp,
                env,
                &Value::symbol("consp"),
                &default_index,
                &impure_index,
                &ephemeral_index,
            )?,
            symbol_with_pos_p: find_relocation(
                interp,
                env,
                &Value::symbol("symbol-with-pos-p"),
                &default_index,
                &impure_index,
                &ephemeral_index,
            )?,
        };

        let data = read_relocation(interp, env, default_container)?;
        let impure_data = read_relocation(interp, env, impure_container)?;
        let ephemeral_data = read_relocation(interp, env, ephemeral_container)?;

        let mut helper_c_names = std::array::from_fn(|_| String::new());
        for (destination, name) in helper_c_names.iter_mut().zip(HELPER_NAMES) {
            *destination = c_function_name(interp, env, name)?;
        }
        let mut subr_c_names = Vec::with_capacity(native_subrs().len());
        for subr in native_subrs() {
            subr_c_names.push(c_function_name(interp, env, subr.name)?);
        }

        let functions_table = call_one(interp, env, "comp-ctxt-funcs-h", context)?;
        let (_, function_entries) = json::hash_table_entries(interp, &functions_table)
            .ok_or_else(|| native_ice("comp-ctxt-funcs-h returned a non-hash-table"))?;
        let mut functions = Vec::with_capacity(function_entries.len());
        let mut function_values = Vec::with_capacity(function_entries.len());
        for (_, function) in function_entries {
            let c_name =
                string_result(call_one(interp, env, "comp-func-c-name", function.clone())?)?;
            let lexical = call_one(interp, env, "comp-func-l-p", function.clone())?.is_truthy();
            let calling_convention = if !lexical {
                FunctionCallingConvention::NoArgs
            } else {
                let arguments = call_one(interp, env, "comp-func-l-args", function.clone())?;
                if call_one(interp, env, "comp-nargs-p", arguments.clone())?.is_truthy() {
                    FunctionCallingConvention::Nargs
                } else {
                    let max = call_one(interp, env, "comp-args-max", arguments)?.as_integer()?;
                    FunctionCallingConvention::Fixed(
                        usize::try_from(max)
                            .map_err(|_| native_ice("negative lexical maximum arity"))?,
                    )
                }
            };
            functions.push(FunctionDeclaration {
                c_name,
                calling_convention,
            });
            function_values.push(function);
        }

        Ok(Self {
            speed,
            debug,
            compiler_options,
            driver_options,
            reproducer,
            optimization_qualities,
            function_docs,
            abi_hash,
            data,
            impure_data,
            ephemeral_data,
            helper_c_names,
            subr_c_names,
            core_relocations,
            functions,
            function_values,
            default_index,
            impure_index,
            ephemeral_index,
        })
    }

    pub(crate) fn as_input(&self) -> UnitInput<'_> {
        UnitInput {
            debug: self.debug,
            optimization_qualities: &self.optimization_qualities,
            function_docs: &self.function_docs,
            abi_hash: &self.abi_hash,
            data: SerializedRelocation {
                len: self.data.len,
                printed: &self.data.printed,
            },
            impure_data: SerializedRelocation {
                len: self.impure_data.len,
                printed: &self.impure_data.printed,
            },
            ephemeral_data: SerializedRelocation {
                len: self.ephemeral_data.len,
                printed: &self.ephemeral_data.printed,
            },
            helper_c_names: &self.helper_c_names,
            subr_c_names: &self.subr_c_names,
            core_relocations: self.core_relocations,
            functions: &self.functions,
        }
    }

    pub(crate) fn function_values(&self) -> &[Value] {
        &self.function_values
    }

    pub(crate) fn speed(&self) -> i64 {
        self.speed
    }

    pub(crate) fn debug(&self) -> i64 {
        self.debug
    }

    pub(crate) fn compiler_options(&self) -> &[String] {
        &self.compiler_options
    }

    pub(crate) fn driver_options(&self) -> &[String] {
        &self.driver_options
    }

    pub(crate) fn reproducer(&self) -> bool {
        self.reproducer
    }

    pub(crate) fn relocation(
        &self,
        interp: &mut Interpreter,
        env: &mut Env,
        object: &Value,
    ) -> Result<Relocation, LispError> {
        find_relocation(
            interp,
            env,
            object,
            &self.default_index,
            &self.impure_index,
            &self.ephemeral_index,
        )
    }
}

fn string_list(value: Value) -> Result<Vec<String>, LispError> {
    value.to_vec()?.into_iter().map(string_result).collect()
}

fn read_relocation(
    interp: &mut Interpreter,
    env: &mut Env,
    container: Value,
) -> Result<OwnedRelocation, LispError> {
    let index = call_one(interp, env, "comp-data-container-idx", container.clone())?;
    let len = call_one(interp, env, "hash-table-count", index)?.as_integer()?;
    let len = usize::try_from(len).map_err(|_| native_ice("negative relocation array length"))?;
    let objects = call_one(interp, env, "comp-data-container-l", container)?;
    let vector = call_one(interp, env, "vconcat", objects)?;
    Ok(OwnedRelocation {
        len,
        printed: print_static(interp, env, &vector)?,
    })
}

fn find_relocation(
    interp: &mut Interpreter,
    env: &mut Env,
    object: &Value,
    default_index: &Value,
    impure_index: &Value,
    ephemeral_index: &Value,
) -> Result<Relocation, LispError> {
    for (array, index) in [
        (RelocationArrayKind::Default, default_index),
        (RelocationArrayKind::Impure, impure_index),
        (RelocationArrayKind::Ephemeral, ephemeral_index),
    ] {
        let value = call(
            interp,
            env,
            "gethash",
            &[object.clone(), index.clone(), Value::Nil],
        )?;
        if value.is_truthy() {
            let index = value.as_integer()?;
            return Ok(Relocation {
                array,
                index: usize::try_from(index)
                    .map_err(|_| native_ice("negative relocation index"))?,
            });
        }
    }
    Err(native_ice("can't find data in relocation containers"))
}

fn c_function_name(
    interp: &mut Interpreter,
    env: &mut Env,
    name: &str,
) -> Result<String, LispError> {
    let value = call(
        interp,
        env,
        "comp-c-func-name",
        &[Value::symbol(name), Value::string("R")],
    )?;
    string_result(value).map_err(|_| native_ice("comp-c-func-name returned a non-string"))
}

fn string_result(value: Value) -> Result<String, LispError> {
    string_like(&value)
        .map(|string| string.text)
        .ok_or_else(|| LispError::WrongTypeArgument("stringp".into(), value))
}

fn print_static(
    interp: &mut Interpreter,
    env: &mut Env,
    value: &Value,
) -> Result<Vec<u8>, LispError> {
    let mut restores: Vec<SpecialBindingRestore> = Vec::with_capacity(6);
    for (name, binding) in [
        ("print-escape-newlines", Value::T),
        ("print-length", Value::Nil),
        ("print-level", Value::Nil),
        ("print-quoted", Value::T),
        ("print-gensym", Value::T),
        ("print-circle", Value::T),
    ] {
        match interp.bind_special_dynamic(name, binding, env) {
            Ok(restore) => restores.push(restore),
            Err(error) => {
                restore_bindings(interp, env, restores)?;
                return Err(error);
            }
        }
    }
    let rendered = render_prin1(interp, value, env);
    let restore = restore_bindings(interp, env, restores);
    let rendered = rendered?;
    restore?;
    encode_internal_multibyte_bytes(&rendered)
}

fn restore_bindings(
    interp: &mut Interpreter,
    env: &mut Env,
    restores: Vec<SpecialBindingRestore>,
) -> Result<(), LispError> {
    for restore in restores.into_iter().rev() {
        interp.restore_special_dynamic(restore, env)?;
    }
    Ok(())
}

fn call_one(
    interp: &mut Interpreter,
    env: &mut Env,
    name: &str,
    argument: Value,
) -> Result<Value, LispError> {
    call(interp, env, name, &[argument])
}

pub(crate) fn call(
    interp: &mut Interpreter,
    env: &mut Env,
    name: &str,
    arguments: &[Value],
) -> Result<Value, LispError> {
    let function = interp.lookup_function(name, env)?;
    interp.call_function_value(function, Some(name), arguments, env)
}

pub(crate) fn native_ice(message: &str) -> LispError {
    LispError::SignalValue(Value::list([
        Value::symbol("native-ice"),
        Value::string(message),
    ]))
}
