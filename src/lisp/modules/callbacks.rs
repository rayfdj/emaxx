use super::*;
use crate::lisp::types::Kind;
use num_bigint::{BigInt, Sign};
use num_traits::{ToPrimitive, Zero};

const NULL: Handle = std::ptr::null_mut();

pub(super) unsafe extern "C" fn make_global_ref(env: *mut ModuleEnv, value: Handle) -> Handle {
    api(env, NULL, |a| {
        let value = a.value(value);
        let existing = a.interpreter().modules.globals.keys().copied().find(|key| {
            primitives::values_eq_in_env(
                a.interpreter(),
                &a.interpreter().modules.values[key],
                &value,
                &crate::lisp::types::Env::new(),
            )
        });
        let state = &mut a.interpreter_mut().modules;
        if let Some(key) = existing {
            let count = state.globals.get_mut(&key).expect("existing global");
            *count = count
                .checked_add(1)
                .filter(|count| *count <= isize::MAX as usize)
                .ok_or_else(|| condition("overflow-error", []))?;
            Ok(key as Handle)
        } else {
            let handle = state.allocate(value);
            state.globals.insert(handle as usize, 1);
            Ok(handle)
        }
    })
}

pub(super) unsafe extern "C" fn free_global_ref(env: *mut ModuleEnv, handle: Handle) {
    api(env, (), |a| {
        let value = a.value(handle);
        let state = &a.interpreter().modules;
        if state.assertions && !state.globals.contains_key(&(handle as usize)) {
            abort(&format!(
                "Global value was not found in list of {} globals",
                state.globals.len()
            ));
        }
        // GNU accepts an eq local handle here when assertions are disabled.
        let key = state.globals.keys().copied().find(|key| {
            primitives::values_eq_in_env(
                a.interpreter(),
                &state.values[key],
                &value,
                &crate::lisp::types::Env::new(),
            )
        });
        if let Some(key) = key {
            let state = &mut a.interpreter_mut().modules;
            let count = state.globals.get_mut(&key).expect("existing global");
            *count -= 1;
            if *count == 0 {
                state.globals.remove(&key);
                state.release(key);
            }
        }
        Ok(())
    });
}

pub(super) unsafe extern "C" fn non_local_exit_check(env: *mut ModuleEnv) -> c_int {
    Context(unsafe { &*activation(env) }).exit_kind()
}
pub(super) unsafe extern "C" fn non_local_exit_clear(env: *mut ModuleEnv) {
    unsafe { &*activation(env) }.pending.borrow_mut().take();
}
pub(super) unsafe extern "C" fn non_local_exit_get(
    env: *mut ModuleEnv,
    symbol: *mut Handle,
    data: *mut Handle,
) -> c_int {
    let a = Context(unsafe { &*activation(env) });
    if a.pending.borrow().is_some() {
        // SAFETY: the public API requires two writable output pointers.
        unsafe {
            *symbol = a.exit_values.get().0;
            *data = a.exit_values.get().1;
        }
    }
    a.exit_kind()
}
pub(super) unsafe extern "C" fn non_local_exit_signal(
    env: *mut ModuleEnv,
    symbol: Handle,
    data: Handle,
) {
    api(env, (), |a| {
        a.set_pending(LispError::SignalValue(Value::cons(
            a.value(symbol),
            a.value(data),
        )));
        Ok(())
    });
}
pub(super) unsafe extern "C" fn non_local_exit_throw(
    env: *mut ModuleEnv,
    tag: Handle,
    value: Handle,
) {
    api(env, (), |a| {
        a.set_pending(LispError::Throw(a.value(tag), a.value(value)));
        Ok(())
    });
}

pub(super) unsafe extern "C" fn make_function(
    env: *mut ModuleEnv,
    min: isize,
    max: isize,
    function: Function,
    doc: *const c_char,
    data: *mut c_void,
) -> Handle {
    api(env, NULL, |a| {
        let fixnum_max = i64::MAX >> 2;
        if min < 0
            || min as i64 > fixnum_max
            || (max != -2 && (max < min || max as i64 > fixnum_max))
        {
            return Err(condition(
                "invalid-arity",
                [Value::Integer(min as i64), Value::Integer(max as i64)],
            ));
        }
        let documentation = if doc.is_null() {
            Value::Nil
        } else {
            // SAFETY: make_function's DOCSTRING is a NUL-terminated C string.
            utf8_value(unsafe { CStr::from_ptr(doc) }.to_bytes())?
        };
        let value = a.interpreter_mut().create_pseudovector(
            RecordKind::ModuleFunction,
            "module-function",
            vec![documentation, Value::Nil, Value::Nil],
        );
        let Kind::Record(id) = value.kind() else {
            unreachable!()
        };
        a.interpreter_mut().modules.functions.insert(
            id.id,
            ModuleFunction {
                min,
                max,
                function,
                data,
                finalizer: None,
            },
        );
        Ok(a.make(value))
    })
}

pub(super) unsafe extern "C" fn funcall(
    env: *mut ModuleEnv,
    function: Handle,
    count: isize,
    args: *mut Handle,
) -> Handle {
    api(env, NULL, |a| {
        let count = usize::try_from(count).map_err(|_| condition("overflow-error", []))?;
        let function = a.value(function);
        // SAFETY: a module supplies COUNT valid handles (NULL is valid for 0).
        let handles = if count == 0 {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(args, count) }
        };
        let args: Vec<_> = handles.iter().map(|handle| a.value(*handle)).collect();
        // SAFETY: this synchronous call may reenter the API. Its context
        // shares the activation without retaining mutable activation borrows.
        let result = unsafe {
            (&mut *a.interpreter).call_function_value(function, None, &args, &mut *a.environment)
        };
        result.map(|value| a.make(value))
    })
}

pub(super) unsafe extern "C" fn intern(env: *mut ModuleEnv, name: *const c_char) -> Handle {
    api(env, NULL, |a| {
        let name = unsafe { CStr::from_ptr(name) }.to_bytes();
        let value = a.primitive("intern", &[utf8_value(name)?])?;
        Ok(a.make(value))
    })
}
pub(super) unsafe extern "C" fn type_of(env: *mut ModuleEnv, value: Handle) -> Handle {
    api(env, NULL, |a| {
        let result = a.primitive("type-of", &[a.value(value)])?;
        Ok(a.make(result))
    })
}
pub(super) unsafe extern "C" fn is_not_nil(env: *mut ModuleEnv, value: Handle) -> bool {
    api(env, false, |a| Ok(a.value(value).is_truthy()))
}
pub(super) unsafe extern "C" fn eq(env: *mut ModuleEnv, left: Handle, right: Handle) -> bool {
    api(env, false, |a| {
        Ok(primitives::values_eq_in_env(
            a.interpreter(),
            &a.value(left),
            &a.value(right),
            &crate::lisp::types::Env::new(),
        ))
    })
}

fn integer(value: &Value) -> Result<BigInt, LispError> {
    match value.kind() {
        Kind::Integer(n) => Ok(BigInt::from(n)),
        Kind::BigInteger(n) => Ok((n).into()),
        _ => Err(primitives::wrong_type_argument("integerp", *value)),
    }
}
pub(super) unsafe extern "C" fn extract_integer(env: *mut ModuleEnv, handle: Handle) -> i64 {
    api(env, 0, |a| {
        let value = a.value(handle);
        integer(&value)?
            .to_i64()
            .ok_or_else(|| condition("overflow-error", [value]))
    })
}
pub(super) unsafe extern "C" fn make_integer(env: *mut ModuleEnv, n: i64) -> Handle {
    api(env, NULL, |a| {
        Ok(a.make(primitives::normalize_integer_value(n)))
    })
}
pub(super) unsafe extern "C" fn extract_float(env: *mut ModuleEnv, handle: Handle) -> f64 {
    api(env, 0.0, |a| match a.value(handle).kind() {
        Kind::Float(n) => Ok(n.get()),
        value => Err(primitives::wrong_type_argument("floatp", value.value())),
    })
}
pub(super) unsafe extern "C" fn make_float(env: *mut ModuleEnv, n: f64) -> Handle {
    api(env, NULL, |a| Ok(a.make(Value::float(n))))
}

fn utf8_value(bytes: &[u8]) -> Result<Value, LispError> {
    let mut remaining = bytes;
    let mut text = String::new();
    let mut extended = Vec::new();
    let mut characters = 0;
    loop {
        match std::str::from_utf8(remaining) {
            Ok(valid) => {
                text.push_str(valid);
                break;
            }
            Err(error) => {
                let (prefix, suffix) = remaining.split_at(error.valid_up_to());
                let valid = std::str::from_utf8(prefix).expect("validated UTF-8 prefix");
                text.push_str(valid);
                characters += valid.chars().count();
                // GNU's decoder accepts surrogate code points. Preserve
                // them in the string's existing extended-character storage;
                // Rust char intentionally cannot represent them.
                if let [0xed, middle @ 0xa0..=0xbf, last @ 0x80..=0xbf, ..] = suffix {
                    let code = 0xd000 | (u32::from(*middle & 0x3f) << 6) | u32::from(*last & 0x3f);
                    extended.push((characters, code));
                    text.push(char::REPLACEMENT_CHARACTER);
                    characters += 1;
                    remaining = &suffix[3..];
                } else {
                    return Err(primitives::wrong_type_argument(
                        "utf-8-string-p",
                        primitives::make_shared_string_value_with_multibyte(
                            primitives::decode_utf8_bytes(bytes),
                            Vec::new(),
                            true,
                        ),
                    ));
                }
            }
        }
    }
    Ok(primitives::make_shared_string_value_with_extended_chars(
        text,
        Vec::new(),
        true,
        extended,
    ))
}
pub(super) unsafe extern "C" fn copy_string_contents(
    env: *mut ModuleEnv,
    handle: Handle,
    buffer: *mut c_char,
    size: *mut isize,
) -> bool {
    api(env, false, |a| {
        let value = a.value(handle);
        let string = primitives::string_like(&value)
            .ok_or_else(|| primitives::wrong_type_argument("stringp", value))?;
        let bytes = if string.multibyte {
            let mut bytes = Vec::new();
            for code in string.character_codes() {
                if !(0..=0x10ffff).contains(&code) {
                    return Err(primitives::wrong_type_argument("unicode-string-p", value));
                }
                primitives::push_emacs_multibyte_char(&mut bytes, code as u32)?;
            }
            bytes
        } else {
            // GNU returns an unibyte string's bytes unchanged.
            primitives::encode_raw_text_bytes(&string.text)?
        };
        let required =
            isize::try_from(bytes.len() + 1).map_err(|_| condition("overflow-error", []))?;
        if buffer.is_null() {
            unsafe {
                *size = required;
            }
            return Ok(true);
        }
        let capacity = unsafe { *size };
        unsafe {
            *size = required;
        }
        if capacity < required {
            return Err(condition(
                "args-out-of-range",
                [
                    Value::Integer(capacity as i64),
                    Value::Integer(required as i64),
                    primitives::normalize_integer_value(isize::MAX as i64),
                ],
            ));
        }
        // SAFETY: checked capacity; the API supplies this writable buffer.
        unsafe {
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), buffer.cast(), bytes.len());
            *buffer.add(bytes.len()) = 0;
        }
        Ok(true)
    })
}
pub(super) unsafe extern "C" fn make_string(
    env: *mut ModuleEnv,
    text: *const c_char,
    length: isize,
) -> Handle {
    api(env, NULL, |a| {
        let length = string_length(length)?;
        let bytes = if length == 0 {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(text.cast(), length) }
        };
        Ok(a.make(utf8_value(bytes)?))
    })
}
pub(super) unsafe extern "C" fn make_unibyte_string(
    env: *mut ModuleEnv,
    text: *const c_char,
    length: isize,
) -> Handle {
    api(env, NULL, |a| {
        let length = string_length(length)?;
        let bytes = if length == 0 {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(text.cast(), length) }
        };
        Ok(a.make(primitives::bytes_to_shared_unibyte_value(bytes)))
    })
}

fn string_length(length: isize) -> Result<usize, LispError> {
    // STRING_BYTES_BOUND: a string's byte count must fit in a fixnum and
    // leave room for its terminating NUL in pointer arithmetic.
    if length < 0 || length as i64 > (i64::MAX >> 2).min((isize::MAX - 1) as i64) {
        return Err(condition("overflow-error", []));
    }
    Ok(length as usize)
}

fn record_id(
    a: &Context<'_>,
    handle: Handle,
    kind: RecordKind,
    predicate: &str,
) -> Result<u64, LispError> {
    let value = a.value(handle);
    if let Kind::Record(id) = value.kind()
        && a.interpreter()
            .find_record(id)
            .is_some_and(|record| record.kind == kind)
    {
        return Ok(id.id);
    }
    Err(primitives::wrong_type_argument(predicate, value))
}
pub(super) unsafe extern "C" fn make_user_ptr(
    env: *mut ModuleEnv,
    finalizer: Finalizer,
    data: *mut c_void,
) -> Handle {
    api(env, NULL, |a| {
        let value = a.interpreter_mut().create_pseudovector(
            RecordKind::UserPointer,
            "user-ptr",
            Vec::new(),
        );
        let Kind::Record(id) = value.kind() else {
            unreachable!()
        };
        a.interpreter_mut()
            .modules
            .pointers
            .insert(id.id, UserPointer { data, finalizer });
        Ok(a.make(value))
    })
}
pub(super) unsafe extern "C" fn get_user_ptr(env: *mut ModuleEnv, value: Handle) -> *mut c_void {
    api(env, std::ptr::null_mut(), |a| {
        let id = record_id(a, value, RecordKind::UserPointer, "user-ptrp")?;
        Ok(a.interpreter().modules.pointers[&id].data)
    })
}
pub(super) unsafe extern "C" fn set_user_ptr(
    env: *mut ModuleEnv,
    value: Handle,
    data: *mut c_void,
) {
    api(env, (), |a| {
        let id = record_id(a, value, RecordKind::UserPointer, "user-ptrp")?;
        a.interpreter_mut()
            .modules
            .pointers
            .get_mut(&id)
            .expect("user pointer")
            .data = data;
        Ok(())
    });
}
pub(super) unsafe extern "C" fn get_user_finalizer(
    env: *mut ModuleEnv,
    value: Handle,
) -> Finalizer {
    api(env, None, |a| {
        let id = record_id(a, value, RecordKind::UserPointer, "user-ptrp")?;
        Ok(a.interpreter().modules.pointers[&id].finalizer)
    })
}
pub(super) unsafe extern "C" fn set_user_finalizer(
    env: *mut ModuleEnv,
    value: Handle,
    finalizer: Finalizer,
) {
    api(env, (), |a| {
        let id = record_id(a, value, RecordKind::UserPointer, "user-ptrp")?;
        a.interpreter_mut()
            .modules
            .pointers
            .get_mut(&id)
            .expect("user pointer")
            .finalizer = finalizer;
        Ok(())
    });
}
pub(super) unsafe extern "C" fn get_function_finalizer(
    env: *mut ModuleEnv,
    value: Handle,
) -> Finalizer {
    api(env, None, |a| {
        let id = record_id(a, value, RecordKind::ModuleFunction, "module-function-p")?;
        Ok(a.interpreter().modules.functions[&id].finalizer)
    })
}
pub(super) unsafe extern "C" fn set_function_finalizer(
    env: *mut ModuleEnv,
    value: Handle,
    finalizer: Finalizer,
) {
    api(env, (), |a| {
        let id = record_id(a, value, RecordKind::ModuleFunction, "module-function-p")?;
        a.interpreter_mut()
            .modules
            .functions
            .get_mut(&id)
            .expect("module function")
            .finalizer = finalizer;
        Ok(())
    });
}
pub(super) unsafe extern "C" fn make_interactive(
    env: *mut ModuleEnv,
    function: Handle,
    spec: Handle,
) {
    api(env, (), |a| {
        let id = record_id(a, function, RecordKind::ModuleFunction, "module-function-p")?;
        let spec = a.value(spec);
        let form = if spec.is_nil() {
            Value::list([Value::symbol("interactive")])
        } else {
            Value::list([Value::symbol("interactive"), spec])
        };
        a.interpreter_mut()
            .find_record_mut(id)
            .expect("module function")
            .slots[1] = form;
        Ok(())
    });
}

fn vector(a: &Context<'_>, handle: Handle, index: Option<isize>) -> Result<Value, LispError> {
    let value = a.value(handle);
    let Kind::Vector(ref vector) = value.kind() else {
        return Err(primitives::wrong_type_argument("vectorp", value));
    };
    if let Some(index) = index
        && (index < 0 || index as usize >= vector.len())
    {
        return Err(condition(
            "args-out-of-range",
            [
                Value::Integer(index as i64),
                Value::Integer(0),
                Value::Integer(vector.len() as i64 - 1),
            ],
        ));
    }
    Ok(value)
}
pub(super) unsafe extern "C" fn vec_get(
    env: *mut ModuleEnv,
    handle: Handle,
    index: isize,
) -> Handle {
    api(env, NULL, |a| {
        let vector = vector(a, handle, Some(index))?;
        let value = a.primitive("aref", &[vector, Value::Integer(index as i64)])?;
        Ok(a.make(value))
    })
}
pub(super) unsafe extern "C" fn vec_set(
    env: *mut ModuleEnv,
    handle: Handle,
    index: isize,
    value: Handle,
) {
    api(env, (), |a| {
        let vector = vector(a, handle, Some(index))?;
        a.primitive(
            "aset",
            &[vector, Value::Integer(index as i64), a.value(value)],
        )?;
        Ok(())
    });
}
pub(super) unsafe extern "C" fn vec_size(env: *mut ModuleEnv, handle: Handle) -> isize {
    api(env, 0, |a| {
        let Kind::Vector(vector) = (vector(a, handle, None)?).kind() else {
            unreachable!()
        };
        Ok(vector.len() as isize)
    })
}
pub(super) unsafe extern "C" fn should_quit(env: *mut ModuleEnv) -> bool {
    api(env, false, |a| {
        Ok(!a.interpreter().quit_flag_is_nil() && !a.interpreter().inhibit_quit_is_truthy())
    })
}
pub(super) unsafe extern "C" fn process_input(env: *mut ModuleEnv) -> c_int {
    api(env, 1, |a| {
        unsafe {
            (&mut *a.interpreter).maybe_quit(&mut *a.environment)?;
        }
        Ok(0)
    })
}
pub(super) unsafe extern "C" fn extract_time(env: *mut ModuleEnv, value: Handle) -> libc::timespec {
    api(
        env,
        libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        },
        |a| {
            let pair = a.primitive(
                "time-convert",
                &[a.value(value), Value::Integer(1_000_000_000)],
            )?;
            let ticks = integer(&pair.car()?)?;
            let (seconds, nanos) = primitives::floor_div_mod(&ticks, &BigInt::from(1_000_000_000));
            Ok(libc::timespec {
                tv_sec: seconds
                    .to_i128()
                    .and_then(|seconds| libc::time_t::try_from(seconds).ok())
                    .ok_or_else(|| {
                        LispError::Signal("Specified time is not representable".into())
                    })?,
                tv_nsec: nanos.to_i64().expect("nanosecond remainder") as libc::c_long,
            })
        },
    )
}
pub(super) unsafe extern "C" fn make_time(env: *mut ModuleEnv, time: libc::timespec) -> Handle {
    api(env, NULL, |a| {
        let ticks = BigInt::from(time.tv_sec) * 1_000_000_000 + BigInt::from(time.tv_nsec);
        Ok(a.make(Value::cons(
            primitives::normalize_bigint_value(ticks),
            Value::Integer(1_000_000_000),
        )))
    })
}
pub(super) unsafe extern "C" fn extract_big_integer(
    env: *mut ModuleEnv,
    handle: Handle,
    sign: *mut c_int,
    count: *mut isize,
    magnitude: *mut usize,
) -> bool {
    api(env, false, |a| {
        let value = integer(&a.value(handle))?;
        let signum = match value.sign() {
            Sign::Minus => -1,
            Sign::NoSign => 0,
            Sign::Plus => 1,
        };
        if !sign.is_null() {
            unsafe {
                *sign = signum;
            }
        }
        if count.is_null() || value.is_zero() {
            return Ok(true);
        }
        let (_, bytes) = value.to_bytes_le();
        let required = bytes.len().div_ceil(std::mem::size_of::<usize>()) as isize;
        if magnitude.is_null() {
            unsafe {
                *count = required;
            }
            return Ok(true);
        }
        let capacity = unsafe { *count };
        if capacity < required {
            unsafe {
                *count = required;
            }
            return Err(condition(
                "args-out-of-range",
                [
                    Value::Integer(capacity as i64),
                    Value::Integer(required as i64),
                    primitives::normalize_integer_value(
                        (isize::MAX / std::mem::size_of::<usize>() as isize) as i64,
                    ),
                ],
            ));
        }
        for (index, chunk) in bytes.chunks(std::mem::size_of::<usize>()).enumerate() {
            let mut limb = [0; std::mem::size_of::<usize>()];
            limb[..chunk.len()].copy_from_slice(chunk);
            unsafe {
                *magnitude.add(index) = usize::from_le_bytes(limb);
            }
        }
        Ok(true)
    })
}
pub(super) unsafe extern "C" fn make_big_integer(
    env: *mut ModuleEnv,
    sign: c_int,
    count: isize,
    magnitude: *const usize,
) -> Handle {
    api(env, NULL, |a| {
        if sign == 0 {
            return Ok(a.make(Value::Integer(0)));
        }
        let count = usize::try_from(count).map_err(|_| condition("overflow-error", []))?;
        let limbs = if count == 0 {
            &[]
        } else {
            unsafe { std::slice::from_raw_parts(magnitude, count) }
        };
        let bytes: Vec<_> = limbs.iter().flat_map(|limb| limb.to_le_bytes()).collect();
        let value = BigInt::from_bytes_le(if sign < 0 { Sign::Minus } else { Sign::Plus }, &bytes);
        Ok(a.make(primitives::normalize_bigint_value(value)))
    })
}
pub(super) unsafe extern "C" fn open_channel(env: *mut ModuleEnv, process: Handle) -> c_int {
    api(env, -1, |a| {
        let process = a.value(process);
        a.interpreter_mut().open_module_channel(&process)
    })
}
