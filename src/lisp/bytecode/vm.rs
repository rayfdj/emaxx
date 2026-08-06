//! Execution of decoded GNU 30.2 bytecode (exec_byte_code port).
//!
//! Runs a validated [`ByteCodeObject`] against the interpreter: operand
//! stack, argument prologue, dynamic binds with a specpdl-style unwind
//! stack, and catch/condition-case handlers that unwind to an in-function
//! destination exactly like GNU's `pushhandler`/`sys_setjmp` pairs.
//! The dispatch match is exhaustive over the decoded opcode set, so the
//! compiler proves every GNU 30.2 opcode has an execution arm.

use super::super::eval::Interpreter;
use super::super::primitives;
use super::super::types::{Env, LispError, Value};
use super::{ArgSpec, ByteCodeObject, Instr, Op};

/// One specpdl-style entry the VM must undo on Bunbind or error unwind.
enum UnwindEntry {
    Binding(super::super::eval::SpecialBindingRestore),
    /// Bsave_excursion: restore buffer and (marker-tracked) point.
    Excursion {
        buffer_id: u64,
        marker_id: u64,
        saved_pt: usize,
    },
    /// Bsave_current_buffer: restore the current buffer only.
    CurrentBuffer {
        buffer_id: u64,
    },
    /// Bsave_restriction on a wide buffer: re-widen on exit.
    RestrictionWide {
        buffer_id: u64,
    },
    /// Bsave_restriction on a narrowed buffer: marker-tracked bounds.
    Restriction {
        buffer_id: u64,
        beg_id: u64,
        end_id: u64,
        saved_begv: usize,
        saved_zv: usize,
    },
    /// Bunwind_protect: a handler function (24.4+) or list of forms.
    Protect(Value),
}

/// Undo one specpdl-style entry (GNU's do_one_unbind).
fn unwind_one(
    interp: &mut Interpreter,
    entry: UnwindEntry,
    env: &mut Env,
) -> Result<(), LispError> {
    match entry {
        UnwindEntry::Binding(restore) => interp.restore_special_binding(restore, env),
        UnwindEntry::Excursion {
            buffer_id,
            marker_id,
            saved_pt,
        } => {
            if interp.has_buffer_id(buffer_id) {
                let _ = interp.set_current_buffer_id(buffer_id);
                let restore_pt = interp
                    .marker_position(marker_id)
                    .unwrap_or(saved_pt)
                    .clamp(interp.buffer.point_min(), interp.buffer.point_max());
                interp.buffer.goto_char(restore_pt);
            }
            let _ = interp.set_marker(marker_id, None, None);
            Ok(())
        }
        UnwindEntry::CurrentBuffer { buffer_id } => {
            if interp.has_buffer_id(buffer_id) {
                let _ = interp.set_current_buffer_id(buffer_id);
            }
            Ok(())
        }
        UnwindEntry::RestrictionWide { buffer_id } => {
            let final_buffer_id = interp.current_buffer_id();
            if interp.has_buffer_id(buffer_id) {
                if final_buffer_id != buffer_id {
                    let _ = interp.set_current_buffer_id(buffer_id);
                }
                let full_end = interp.buffer.size_total() + 1;
                interp.buffer.restore_restriction(1, full_end);
                if final_buffer_id != buffer_id && interp.has_buffer_id(final_buffer_id) {
                    let _ = interp.set_current_buffer_id(final_buffer_id);
                }
            }
            Ok(())
        }
        UnwindEntry::Restriction {
            buffer_id,
            beg_id,
            end_id,
            saved_begv,
            saved_zv,
        } => {
            let final_buffer_id = interp.current_buffer_id();
            let restore_begv = interp.marker_position(beg_id).unwrap_or(saved_begv);
            let restore_zv = interp.marker_position(end_id).unwrap_or(saved_zv);
            if interp.has_buffer_id(buffer_id) {
                if final_buffer_id != buffer_id {
                    let _ = interp.set_current_buffer_id(buffer_id);
                }
                interp.buffer.restore_restriction(restore_begv, restore_zv);
                if final_buffer_id != buffer_id && interp.has_buffer_id(final_buffer_id) {
                    let _ = interp.set_current_buffer_id(final_buffer_id);
                }
            }
            let _ = interp.set_marker(beg_id, None, None);
            let _ = interp.set_marker(end_id, None, None);
            Ok(())
        }
        UnwindEntry::Protect(handler) => {
            // GNU records bcall0 for functions (24.4+) and prog_ignore
            // for the obsolete forms-list shape.
            let is_function = prim(interp, "functionp", std::slice::from_ref(&handler), env)?;
            if is_function.is_truthy() {
                interp.call_function_value(handler, None, &[], env)?;
            } else if let Ok(forms) = handler.to_vec() {
                for form in &forms {
                    interp.eval(form, env)?;
                }
            }
            Ok(())
        }
    }
}

enum HandlerKind {
    /// Bpushcatch: TAG caught by `eq`.
    Catch(Value),
    /// Bpushconditioncase: the popped conditions clause head.
    ConditionCase(Value),
}

struct Handler {
    kind: HandlerKind,
    dest: usize,
    stack_len: usize,
    unwind_len: usize,
}

/// Materialize a constant still in reader form: record literals
/// (`#[...]` nested closures, `#s(hash-table ...)` jump tables) become
/// live objects; vectors materialize elementwise.  Everything else is
/// already a value.
fn materialize_constant(
    interp: &mut Interpreter,
    constant: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    let Ok(items) = constant.to_vec() else {
        return Ok(constant.clone());
    };
    match items.first() {
        Some(Value::Symbol(marker)) if marker == "emaxx--record-literal" => {
            interp.eval(constant, env)
        }
        Some(Value::Symbol(marker)) if marker == "emaxx--hash-table-literal" => {
            // Same materialization `sf_quote' applies to quoted literals.
            primitives::materialize_read_hash_table_literals(interp, constant)
        }
        // The reader wraps `#s(hash-table ...)' in a quote; GNU constants
        // hold the table itself, so unwrap exactly that artifact.
        Some(Value::Symbol(quote))
            if quote == "quote"
                && items.len() == 2
                && items[1].to_vec().ok().is_some_and(|inner| {
                    matches!(
                        inner.first(),
                        Some(Value::Symbol(marker)) if marker == "emaxx--hash-table-literal"
                    )
                }) =>
        {
            primitives::materialize_read_hash_table_literals(interp, &items[1])
        }
        Some(Value::Symbol(marker)) if marker == "vector-literal" => {
            let mut rebuilt = Vec::with_capacity(items.len());
            rebuilt.push(Value::symbol("vector-literal"));
            for item in &items[1..] {
                rebuilt.push(materialize_constant(interp, item, env)?);
            }
            Ok(Value::list(rebuilt))
        }
        _ => Ok(constant.clone()),
    }
}

/// Call a named Emaxx primitive with stack values (the Bcar/Bplus-style
/// single-instruction function ops).
fn prim(
    interp: &mut Interpreter,
    name: &str,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    primitives::call(interp, name, args, env)
}

/// A byte-code function decoded, validated, and materialized once:
/// instructions, an O(1) byte-offset -> instruction-index table for
/// jumps, and live constants.  Cached per record so repeated calls skip
/// all of that setup (GNU decodes lazily inside its dispatch loop and
/// its closures are already live objects).
pub struct CachedProgram {
    pub argspec: ArgSpec,
    pub instrs: Vec<Instr>,
    pub offset_index: Vec<u32>,
    pub constants: Vec<Value>,
    pub stack_depth: usize,
}

impl CachedProgram {
    #[inline]
    fn instr_at(&self, byte_offset: usize) -> usize {
        self.offset_index[byte_offset] as usize
    }
}

fn build_cached(
    interp: &mut Interpreter,
    object: &ByteCodeObject,
    env: &mut Env,
) -> Result<CachedProgram, LispError> {
    let instrs = super::decode_program(&object.code, object.constants.len())
        .map_err(|error| LispError::Signal(error.to_string()))?;
    let mut offset_index = vec![u32::MAX; object.code.len() + 1];
    for (index, instr) in instrs.iter().enumerate() {
        offset_index[instr.offset] = index as u32;
    }
    let mut constants = Vec::with_capacity(object.constants.len());
    for constant in &object.constants {
        constants.push(materialize_constant(interp, constant, env)?);
    }
    Ok(CachedProgram {
        argspec: object.argspec.clone(),
        instrs,
        offset_index,
        constants,
        stack_depth: object.stack_depth,
    })
}

/// Execute the genuine byte-code function stored in RECORD_ID, decoding
/// and materializing it once and reusing the cached program afterwards.
pub fn execute_record(
    interp: &mut Interpreter,
    record_id: u64,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    // Mutation of a record's slots goes through find_record_mut, which
    // drops the cached program, so a cache hit is always current.  Ids are
    // dense from 1, so id-1 indexes the slot vector directly.
    let index = (record_id as usize).saturating_sub(1);
    if let Some(Some(program)) = interp.bytecode_program_cache.get(index) {
        let program = std::rc::Rc::clone(program);
        return run(interp, &program, args, env);
    }
    let slots = interp
        .find_record(record_id)
        .map(|record| record.slots.clone())
        .ok_or_else(|| LispError::Signal("byte-code record vanished".into()))?;
    let object = super::ByteCodeObject::from_slots(&slots)
        .map_err(|error| LispError::Signal(error.to_string()))?
        .ok_or_else(|| {
            LispError::SignalValue(Value::list([
                Value::Symbol("invalid-function".into()),
                Value::Record(record_id),
            ]))
        })?;
    let program = std::rc::Rc::new(build_cached(interp, &object, env)?);
    if interp.bytecode_program_cache.len() <= index {
        interp.bytecode_program_cache.resize(index + 1, None);
    }
    interp.bytecode_program_cache[index] = Some(std::rc::Rc::clone(&program));
    run(interp, &program, args, env)
}

/// Execute OBJECT with ARGS, returning the value of Breturn.
pub fn execute(
    interp: &mut Interpreter,
    object: &ByteCodeObject,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    let program = build_cached(interp, object, env)?;
    run(interp, &program, args, env)
}

fn run(
    interp: &mut Interpreter,
    object: &CachedProgram,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    // Reuse a pooled operand stack: one heap allocation per nesting
    // level instead of one per call.
    let mut stack = interp.vm_stack_pool.pop().unwrap_or_default();
    stack.clear();
    stack.reserve(object.stack_depth.max(8));
    let result = run_with_stack(interp, object, args, env, &mut stack);
    stack.clear();
    if interp.vm_stack_pool.len() < 256 {
        interp.vm_stack_pool.push(stack);
    }
    result
}

fn run_with_stack(
    interp: &mut Interpreter,
    object: &CachedProgram,
    args: &[Value],
    env: &mut Env,
    stack: &mut Vec<Value>,
) -> Result<Value, LispError> {
    let stack = &mut *stack;
    let mut unwinds: Vec<UnwindEntry> = Vec::new();

    // Argument prologue (exec_byte_code's ARGS_TEMPLATE handling).
    match &object.argspec {
        ArgSpec::Packed {
            mandatory,
            nonrest,
            rest,
        } => {
            let mandatory = *mandatory as usize;
            let nonrest = *nonrest as usize;
            if args.len() < mandatory || (!rest && args.len() > nonrest) {
                return Err(LispError::WrongNumberOfArgs(
                    "byte-code function".into(),
                    args.len(),
                ));
            }
            let pushed = args.len().min(nonrest);
            stack.extend(args[..pushed].iter().cloned());
            if args.len() > nonrest {
                stack.push(Value::list(args[nonrest..].iter().cloned()));
            } else {
                for _ in args.len()..nonrest {
                    stack.push(Value::Nil);
                }
                if *rest {
                    stack.push(Value::Nil);
                }
            }
        }
        // Old-style dynamic bytecode: funcall_lambda specbinds each
        // formal (honoring &optional/&rest) and runs the body with an
        // empty stack; the exit balance below is GNU's unbind_to.
        ArgSpec::Legacy(arglist) => {
            let formals = arglist.to_vec().unwrap_or_default();
            let mut index = 0usize;
            let mut optional = false;
            let mut rest = false;
            for formal in &formals {
                let name = match formal {
                    Value::Symbol(name) => name.clone(),
                    other => {
                        let error = LispError::SignalValue(Value::list([
                            Value::Symbol("invalid-function".into()),
                            other.clone(),
                        ]));
                        while let Some(entry) = unwinds.pop() {
                            unwind_one(interp, entry, env)?;
                        }
                        return Err(error);
                    }
                };
                if name == "&optional" {
                    optional = true;
                    continue;
                }
                if name == "&rest" {
                    rest = true;
                    continue;
                }
                let value = if rest {
                    Value::list(args[index.min(args.len())..].iter().cloned())
                } else if index < args.len() {
                    args[index].clone()
                } else if optional {
                    Value::Nil
                } else {
                    while let Some(entry) = unwinds.pop() {
                        unwind_one(interp, entry, env)?;
                    }
                    return Err(LispError::WrongNumberOfArgs(
                        "byte-code function".into(),
                        args.len(),
                    ));
                };
                if rest {
                    index = args.len();
                } else {
                    index += 1;
                }
                let restore = interp.bind_special_variable(&name, value, env)?;
                unwinds.push(UnwindEntry::Binding(restore));
                if rest {
                    break;
                }
            }
            if !rest && index < args.len() {
                while let Some(entry) = unwinds.pop() {
                    unwind_one(interp, entry, env)?;
                }
                return Err(LispError::WrongNumberOfArgs(
                    "byte-code function".into(),
                    args.len(),
                ));
            }
        }
    }
    let mut handlers: Vec<Handler> = Vec::new();
    let mut pc = 0usize;

    macro_rules! pop {
        () => {
            stack.pop().expect("validated bytecode never underflows")
        };
    }

    let result = 'run: loop {
        let Some(instr) = object.instrs.get(pc) else {
            break Err(LispError::Signal(
                "byte code ran off the end of its program".into(),
            ));
        };
        let Instr { op, .. } = *instr;
        pc += 1;

        // Hot pre-dispatch: the ops below either cannot fail or only take
        // this path when their operands make failure impossible, so they
        // skip the fallible-step closure (and its Result plumbing)
        // entirely.  Anything that falls through runs the full arm below.
        match op {
            Op::StackRef(n) => {
                let value = stack[stack.len() - 1 - n as usize].clone();
                stack.push(value);
                continue;
            }
            Op::StackSet(n) => {
                let value = pop!();
                let slot = stack.len() - 1 - (n as usize - 1);
                stack[slot] = value;
                continue;
            }
            Op::Dup => {
                let top = stack.last().expect("validated bytecode").clone();
                stack.push(top);
                continue;
            }
            Op::Discard => {
                pop!();
                continue;
            }
            Op::Constant(index) | Op::Constant2(index) => {
                stack.push(object.constants[index as usize].clone());
                continue;
            }
            Op::Goto { target } => {
                pc = object.instr_at(target as usize);
                continue;
            }
            Op::GotoIfNil { target } => {
                if pop!().is_nil() {
                    pc = object.instr_at(target as usize);
                }
                continue;
            }
            Op::GotoIfNonNil { target } => {
                if !pop!().is_nil() {
                    pc = object.instr_at(target as usize);
                }
                continue;
            }
            Op::GotoIfNilElsePop { target } => {
                if stack.last().expect("validated bytecode").is_nil() {
                    pc = object.instr_at(target as usize);
                } else {
                    pop!();
                }
                continue;
            }
            Op::GotoIfNonNilElsePop { target } => {
                if !stack.last().expect("validated bytecode").is_nil() {
                    pc = object.instr_at(target as usize);
                } else {
                    pop!();
                }
                continue;
            }
            Op::Return => {
                break 'run Ok(pop!());
            }
            Op::Not => {
                let value = pop!();
                stack.push(if value.is_nil() { Value::T } else { Value::Nil });
                continue;
            }
            Op::Cons => {
                let b = pop!();
                let a = pop!();
                stack.push(Value::cons(a, b));
                continue;
            }
            Op::Eq => {
                let b = pop!();
                let a = pop!();
                let equal = crate::lisp::primitives::values_eq_in_env(interp, &a, &b, env);
                stack.push(if equal { Value::T } else { Value::Nil });
                continue;
            }
            Op::Consp => {
                let a = pop!();
                stack.push(if matches!(a, Value::Cons(..)) {
                    Value::T
                } else {
                    Value::Nil
                });
                continue;
            }
            Op::Plus | Op::Diff | Op::Mult => {
                let len = stack.len();
                if let (Value::Integer(x), Value::Integer(y)) = (&stack[len - 2], &stack[len - 1]) {
                    let fast = match op {
                        Op::Plus => x.checked_add(*y),
                        Op::Diff => x.checked_sub(*y),
                        _ => x.checked_mul(*y),
                    };
                    if let Some(n) = fast {
                        stack.truncate(len - 2);
                        stack.push(Value::Integer(n));
                        continue;
                    }
                }
            }
            Op::Quo | Op::Rem => {
                let len = stack.len();
                if let (Value::Integer(x), Value::Integer(y)) = (&stack[len - 2], &stack[len - 1]) {
                    // checked_div/checked_rem refuse y == 0 and the MIN/-1
                    // overflow, which fall through to the full arithmetic
                    // (and its arith-error).
                    let fast = match op {
                        Op::Quo => x.checked_div(*y),
                        _ => x.checked_rem(*y),
                    };
                    if let Some(n) = fast {
                        stack.truncate(len - 2);
                        stack.push(Value::Integer(n));
                        continue;
                    }
                }
            }
            Op::Eqlsign | Op::Gtr | Op::Lss | Op::Leq | Op::Geq => {
                let len = stack.len();
                if let (Value::Integer(x), Value::Integer(y)) = (&stack[len - 2], &stack[len - 1]) {
                    let holds = match op {
                        Op::Eqlsign => x == y,
                        Op::Gtr => x > y,
                        Op::Lss => x < y,
                        Op::Leq => x <= y,
                        _ => x >= y,
                    };
                    stack.truncate(len - 2);
                    stack.push(if holds { Value::T } else { Value::Nil });
                    continue;
                }
            }
            Op::Add1 | Op::Sub1 | Op::Negate => {
                if let Some(Value::Integer(x)) = stack.last() {
                    let fast = match op {
                        Op::Add1 => x.checked_add(1),
                        Op::Sub1 => x.checked_sub(1),
                        _ => x.checked_neg(),
                    };
                    if let Some(n) = fast {
                        *stack.last_mut().expect("validated bytecode") = Value::Integer(n);
                        continue;
                    }
                }
            }
            Op::Aref => {
                let len = stack.len();
                if let Value::Integer(index) = &stack[len - 1]
                    && *index >= 0
                    && let Some(value) =
                        crate::lisp::primitives::vector_aref_fast(&stack[len - 2], *index as usize)
                {
                    stack.truncate(len - 2);
                    stack.push(value);
                    continue;
                }
            }
            Op::Aset => {
                // Stack: [.. vector index value]; aset returns the value.
                let len = stack.len();
                if let Value::Integer(index) = &stack[len - 2]
                    && *index >= 0
                    && crate::lisp::primitives::vector_aset_fast(
                        &stack[len - 3],
                        *index as usize,
                        &stack[len - 1],
                    )
                    .is_some()
                {
                    let value = pop!();
                    stack.truncate(len - 3);
                    stack.push(value);
                    continue;
                }
            }
            Op::Car | Op::Cdr | Op::CarSafe | Op::CdrSafe => match stack.last() {
                Some(Value::Cons(car, cdr)) => {
                    let value = if matches!(op, Op::Car | Op::CarSafe) {
                        car.borrow().clone()
                    } else {
                        cdr.borrow().clone()
                    };
                    *stack.last_mut().expect("validated bytecode") = value;
                    continue;
                }
                Some(Value::Nil) => continue,
                Some(_) if matches!(op, Op::CarSafe | Op::CdrSafe) => {
                    *stack.last_mut().expect("validated bytecode") = Value::Nil;
                    continue;
                }
                _ => {}
            },
            _ => {}
        }

        // Every fallible operation funnels through here so handler
        // unwinding (GNU's sys_setjmp arm) is applied uniformly.
        let step: Result<(), LispError> = (|| {
            match op {
                Op::StackRef(n) => {
                    let value = stack[stack.len() - 1 - n as usize].clone();
                    stack.push(value);
                }
                Op::StackSet(n) => {
                    let value = pop!();
                    let slot = stack.len() - 1 - (n as usize - 1);
                    // stack-set N stores relative to the *pre-pop* top.
                    stack[slot] = value;
                }
                Op::DiscardN {
                    count,
                    preserve_tos,
                } => {
                    if preserve_tos {
                        let top = pop!();
                        for _ in 0..count {
                            pop!();
                        }
                        stack.push(top);
                    } else {
                        for _ in 0..count {
                            pop!();
                        }
                    }
                }
                Op::Discard => {
                    pop!();
                }
                Op::Dup => {
                    let top = stack.last().expect("validated bytecode").clone();
                    stack.push(top);
                }
                Op::Constant(index) | Op::Constant2(index) => {
                    stack.push(object.constants[index as usize].clone());
                }
                Op::VarRef(index) => {
                    let name = object.constants[index as usize].clone();
                    let value = prim(interp, "symbol-value", &[name], env)?;
                    stack.push(value);
                }
                Op::VarSet(index) => {
                    let name = object.constants[index as usize].clone();
                    let value = pop!();
                    prim(interp, "set", &[name, value], env)?;
                }
                Op::VarBind(index) => {
                    let name = object.constants[index as usize]
                        .as_symbol()
                        .map_err(|_| LispError::Signal("varbind constant must be a symbol".into()))?
                        .to_string();
                    let value = pop!();
                    let restore = interp.bind_special_variable(&name, value, env)?;
                    unwinds.push(UnwindEntry::Binding(restore));
                }
                Op::Unbind(count) => {
                    for _ in 0..count {
                        match unwinds.pop() {
                            Some(entry) => unwind_one(interp, entry, env)?,
                            None => {
                                return Err(LispError::Signal("byte code unbind underflow".into()));
                            }
                        }
                    }
                }
                Op::SaveExcursion => {
                    let buffer_id = interp.current_buffer_id();
                    let saved_pt = interp.buffer.point();
                    let Value::Marker(marker_id) = interp.make_marker() else {
                        unreachable!("make_marker returns a marker")
                    };
                    interp.set_marker(marker_id, Some(saved_pt), Some(buffer_id))?;
                    unwinds.push(UnwindEntry::Excursion {
                        buffer_id,
                        marker_id,
                        saved_pt,
                    });
                }
                Op::SaveCurrentBuffer | Op::SaveCurrentBufferObsolete => {
                    unwinds.push(UnwindEntry::CurrentBuffer {
                        buffer_id: interp.current_buffer_id(),
                    });
                }
                Op::SaveRestriction => {
                    let buffer_id = interp.current_buffer_id();
                    let saved_begv = interp.buffer.point_min();
                    let saved_zv = interp.buffer.point_max();
                    // Mirrors sf_save_restriction: a wide buffer records "no
                    // restriction" (marker-tracking would spuriously
                    // re-narrow after edits at BEGV).
                    if saved_begv == 1 && saved_zv == interp.buffer.size_total() + 1 {
                        unwinds.push(UnwindEntry::RestrictionWide { buffer_id });
                    } else {
                        let Value::Marker(beg_id) = interp.make_marker() else {
                            unreachable!("make_marker returns a marker")
                        };
                        let Value::Marker(end_id) = interp.make_marker() else {
                            unreachable!("make_marker returns a marker")
                        };
                        let _ = interp.set_marker(beg_id, Some(saved_begv), Some(buffer_id));
                        let _ = interp.set_marker(end_id, Some(saved_zv), Some(buffer_id));
                        interp.set_marker_insertion_type(end_id, true);
                        interp.buffer.push_undo_meta(Value::cons(
                            Value::Marker(beg_id),
                            Value::Integer(-(saved_begv as i64)),
                        ));
                        interp.buffer.push_undo_meta(Value::cons(
                            Value::Marker(end_id),
                            Value::Integer(saved_zv as i64),
                        ));
                        unwinds.push(UnwindEntry::Restriction {
                            buffer_id,
                            beg_id,
                            end_id,
                            saved_begv,
                            saved_zv,
                        });
                    }
                }
                Op::UnwindProtect => {
                    let handler = pop!();
                    unwinds.push(UnwindEntry::Protect(handler));
                }
                Op::SaveWindowExcursion => {
                    // Obsolete since 24.1: TOP is a list of body forms;
                    // evaluate them inside a window-configuration save.
                    let body = pop!();
                    let snapshot = interp.snapshot_window_configuration();
                    let mut value = Value::Nil;
                    let result: Result<(), LispError> = (|| {
                        for form in &body.to_vec()? {
                            value = interp.eval(form, env)?;
                        }
                        Ok(())
                    })();
                    let _ = interp.restore_window_configuration(snapshot);
                    result?;
                    stack.push(value);
                }
                Op::Catch => {
                    // Obsolete since 25: TAG below an unevaluated body form.
                    let body = pop!();
                    let tag = pop!();
                    let value = match interp.eval(&body, env) {
                        Err(LispError::Throw(thrown, thrown_value))
                            if prim(interp, "eq", &[tag.clone(), thrown.clone()], env)?
                                .is_truthy() =>
                        {
                            thrown_value
                        }
                        other => other?,
                    };
                    stack.push(value);
                }
                Op::ConditionCase => {
                    // Obsolete since 25: VAR, BODY form, HANDLERS list —
                    // exactly `(condition-case VAR BODY . HANDLERS)`.
                    let handlers = pop!();
                    let body = pop!();
                    let var = pop!();
                    let mut form = vec![Value::symbol("condition-case"), var, body];
                    form.extend(handlers.to_vec()?);
                    let value = interp.eval(&Value::list(form), env)?;
                    stack.push(value);
                }
                Op::TempOutputBufferSetup => {
                    // Obsolete since 24.1: create/erase the temp buffer and
                    // bind standard-output to it (GNU temp_output_buffer_setup
                    // specbinds, so Bunbind pops it).
                    let name = pop!();
                    let buffer = prim(interp, "get-buffer-create", &[name], env)?;
                    let saved = interp.current_buffer_id();
                    let buffer_id = interp.resolve_buffer_id(&buffer)?;
                    let _ = interp.set_current_buffer_id(buffer_id);
                    prim(interp, "erase-buffer", &[], env)?;
                    let _ = interp.set_current_buffer_id(saved);
                    let restore =
                        interp.bind_special_variable("standard-output", buffer.clone(), env)?;
                    unwinds.push(UnwindEntry::Binding(restore));
                    stack.push(buffer);
                }
                Op::TempOutputBufferShow => {
                    // Obsolete since 24.1: show the buffer at TOP, replace it
                    // with the saved value below, pop the standard-output bind.
                    let value = pop!();
                    let buffer = pop!();
                    let show = interp
                        .lookup_var("temp-buffer-show-function", env)
                        .unwrap_or(Value::Nil);
                    if show.is_truthy() {
                        interp.call_function_value(
                            show,
                            None,
                            std::slice::from_ref(&buffer),
                            env,
                        )?;
                    } else {
                        prim(interp, "display-buffer", &[buffer], env)?;
                    }
                    stack.push(value);
                    match unwinds.pop() {
                        Some(entry) => unwind_one(interp, entry, env)?,
                        None => {
                            return Err(LispError::Signal("byte code unbind underflow".into()));
                        }
                    }
                }
                Op::InteractiveP => {
                    // Obsolete since 24.1: GNU call0s the Lisp function.
                    let value = interp.call_function_value(
                        Value::symbol("interactive-p"),
                        Some("interactive-p"),
                        &[],
                        env,
                    )?;
                    stack.push(value);
                }
                // Zero-argument push ops.
                Op::Point
                | Op::PointMax
                | Op::PointMin
                | Op::FollowingChar
                | Op::PrecedingChar
                | Op::CurrentColumn
                | Op::Eolp
                | Op::Eobp
                | Op::Bolp
                | Op::Bobp
                | Op::CurrentBuffer
                | Op::Widen => {
                    let name = match op {
                        Op::Point => "point",
                        Op::PointMax => "point-max",
                        Op::PointMin => "point-min",
                        Op::FollowingChar => "following-char",
                        Op::PrecedingChar => "preceding-char",
                        Op::CurrentColumn => "current-column",
                        Op::Eolp => "eolp",
                        Op::Eobp => "eobp",
                        Op::Bolp => "bolp",
                        Op::Bobp => "bobp",
                        Op::CurrentBuffer => "current-buffer",
                        _ => "widen",
                    };
                    let value = prim(interp, name, &[], env)?;
                    stack.push(value);
                }
                // One-argument buffer/navigation ops (TOP = F(TOP)).
                Op::GotoChar
                | Op::Insert
                | Op::CharAfter
                | Op::SetBuffer
                | Op::ForwardChar
                | Op::ForwardWord
                | Op::ForwardLine
                | Op::CharSyntax
                | Op::EndOfLine
                | Op::MatchBeginning
                | Op::MatchEnd => {
                    let a = pop!();
                    let name = match op {
                        Op::GotoChar => "goto-char",
                        Op::Insert => "insert",
                        Op::CharAfter => "char-after",
                        Op::SetBuffer => "set-buffer",
                        Op::ForwardChar => "forward-char",
                        Op::ForwardWord => "forward-word",
                        Op::ForwardLine => "forward-line",
                        Op::CharSyntax => "char-syntax",
                        Op::EndOfLine => "end-of-line",
                        Op::MatchBeginning => "match-beginning",
                        _ => "match-end",
                    };
                    let value = prim(interp, name, &[a], env)?;
                    stack.push(value);
                }
                Op::IndentTo => {
                    // GNU passes an explicit nil MINIMUM.
                    let column = pop!();
                    let value = prim(interp, "indent-to", &[column, Value::Nil], env)?;
                    stack.push(value);
                }
                // Two-argument region/motion ops.
                Op::SkipCharsForward
                | Op::SkipCharsBackward
                | Op::BufferSubstring
                | Op::DeleteRegion
                | Op::NarrowToRegion => {
                    let b = pop!();
                    let a = pop!();
                    let name = match op {
                        Op::SkipCharsForward => "skip-chars-forward",
                        Op::SkipCharsBackward => "skip-chars-backward",
                        Op::BufferSubstring => "buffer-substring",
                        Op::DeleteRegion => "delete-region",
                        _ => "narrow-to-region",
                    };
                    let value = prim(interp, name, &[a, b], env)?;
                    stack.push(value);
                }
                Op::SetMarker => {
                    let position_buffer = pop!();
                    let position = pop!();
                    let marker = pop!();
                    let value = prim(
                        interp,
                        "set-marker",
                        &[marker, position, position_buffer],
                        env,
                    )?;
                    stack.push(value);
                }
                Op::InsertN(n) => {
                    let items: Vec<Value> = stack.drain(stack.len() - n as usize..).collect();
                    let value = prim(interp, "insert", &items, env)?;
                    stack.push(value);
                }
                Op::Call(argc) => {
                    let argc = argc as usize;
                    let args_start = stack.len() - argc;
                    // Call with the arguments still on the stack (GNU's
                    // exec_byte_code does the same); an error unwind
                    // truncates to the handler's recorded depth anyway.
                    let func = std::mem::replace(&mut stack[args_start - 1], Value::Nil);
                    let value =
                        interp.call_function_value(func, None, &stack[args_start..], env)?;
                    stack.truncate(args_start - 1);
                    stack.push(value);
                }
                Op::Goto { target } => {
                    pc = object.instr_at(target as usize);
                }
                Op::GotoIfNil { target } => {
                    if pop!().is_nil() {
                        pc = object.instr_at(target as usize);
                    }
                }
                Op::GotoIfNonNil { target } => {
                    if !pop!().is_nil() {
                        pc = object.instr_at(target as usize);
                    }
                }
                Op::GotoIfNilElsePop { target } => {
                    if stack.last().expect("validated bytecode").is_nil() {
                        pc = object.instr_at(target as usize);
                    } else {
                        pop!();
                    }
                }
                Op::GotoIfNonNilElsePop { target } => {
                    if !stack.last().expect("validated bytecode").is_nil() {
                        pc = object.instr_at(target as usize);
                    } else {
                        pop!();
                    }
                }
                Op::Return => {
                    return Err(LispError::Throw(
                        Value::Symbol("--emaxx-bytecode-return--".into()),
                        pop!(),
                    ));
                }
                Op::PushCatch { target } => {
                    let tag = pop!();
                    // `throw' consults the interpreter's active-catch
                    // registry before unwinding; a VM catch frame must be
                    // visible there like any `sf_catch' frame.
                    interp.push_active_catch_tag(tag.clone());
                    handlers.push(Handler {
                        kind: HandlerKind::Catch(tag),
                        dest: target as usize,
                        stack_len: stack.len(),
                        unwind_len: unwinds.len(),
                    });
                }
                Op::PushConditionCase { target } => {
                    let conditions = pop!();
                    handlers.push(Handler {
                        kind: HandlerKind::ConditionCase(conditions),
                        dest: target as usize,
                        stack_len: stack.len(),
                        unwind_len: unwinds.len(),
                    });
                }
                Op::PopHandler => {
                    if let Some(handler) = handlers.pop()
                        && matches!(handler.kind, HandlerKind::Catch(_))
                    {
                        interp.pop_active_catch_tag();
                    }
                }
                Op::Switch => {
                    let table = pop!();
                    let value = pop!();
                    let dest = prim(interp, "gethash", &[value, table, Value::Nil], env)?;
                    if let Value::Integer(dest) = dest {
                        pc = object.instr_at(dest as usize);
                    }
                }
                Op::ListN(n) => {
                    let items: Vec<Value> = stack.drain(stack.len() - n as usize..).collect();
                    stack.push(Value::list(items));
                }
                Op::ConcatN(n) => {
                    let items: Vec<Value> = stack.drain(stack.len() - n as usize..).collect();
                    let value = prim(interp, "concat", &items, env)?;
                    stack.push(value);
                }
                Op::List1 | Op::List2 | Op::List3 | Op::List4 => {
                    let n = match op {
                        Op::List1 => 1,
                        Op::List2 => 2,
                        Op::List3 => 3,
                        _ => 4,
                    };
                    let items: Vec<Value> = stack.drain(stack.len() - n..).collect();
                    stack.push(Value::list(items));
                }
                Op::Not => {
                    let value = pop!();
                    stack.push(if value.is_nil() { Value::T } else { Value::Nil });
                }
                // Two-argument ops with inline fast paths.
                Op::Eq => {
                    let b = pop!();
                    let a = pop!();
                    let equal = crate::lisp::primitives::values_eq_in_env(interp, &a, &b, env);
                    stack.push(if equal { Value::T } else { Value::Nil });
                }
                Op::Cons => {
                    let b = pop!();
                    let a = pop!();
                    stack.push(Value::cons(a, b));
                }
                Op::Eqlsign | Op::Gtr | Op::Lss | Op::Leq | Op::Geq => {
                    let b = pop!();
                    let a = pop!();
                    if let (Value::Integer(x), Value::Integer(y)) = (&a, &b) {
                        let holds = match op {
                            Op::Eqlsign => x == y,
                            Op::Gtr => x > y,
                            Op::Lss => x < y,
                            Op::Leq => x <= y,
                            _ => x >= y,
                        };
                        stack.push(if holds { Value::T } else { Value::Nil });
                        return Ok(());
                    }
                    let name = match op {
                        Op::Eqlsign => "=",
                        Op::Gtr => ">",
                        Op::Lss => "<",
                        Op::Leq => "<=",
                        _ => ">=",
                    };
                    let value = prim(interp, name, &[a, b], env)?;
                    stack.push(value);
                }
                Op::Plus | Op::Diff | Op::Mult => {
                    let b = pop!();
                    let a = pop!();
                    if let (Value::Integer(x), Value::Integer(y)) = (&a, &b) {
                        let fast = match op {
                            Op::Plus => x.checked_add(*y),
                            Op::Diff => x.checked_sub(*y),
                            _ => x.checked_mul(*y),
                        };
                        if let Some(n) = fast {
                            stack.push(Value::Integer(n));
                            return Ok(());
                        }
                    }
                    let name = match op {
                        Op::Plus => "+",
                        Op::Diff => "-",
                        _ => "*",
                    };
                    let value = prim(interp, name, &[a, b], env)?;
                    stack.push(value);
                }
                Op::Max | Op::Min => {
                    let b = pop!();
                    let a = pop!();
                    if let (Value::Integer(x), Value::Integer(y)) = (&a, &b) {
                        let n = if matches!(op, Op::Max) {
                            (*x).max(*y)
                        } else {
                            (*x).min(*y)
                        };
                        stack.push(Value::Integer(n));
                        return Ok(());
                    }
                    let name = if matches!(op, Op::Max) { "max" } else { "min" };
                    let value = prim(interp, name, &[a, b], env)?;
                    stack.push(value);
                }
                // Two-argument primitive ops.
                Op::Memq
                | Op::Nth
                | Op::Aref
                | Op::Setcar
                | Op::Setcdr
                | Op::Nthcdr
                | Op::Elt
                | Op::Member
                | Op::Assq
                | Op::Equal
                | Op::Get
                | Op::Quo
                | Op::Rem
                | Op::StringEqlsign
                | Op::StringLss
                | Op::Concat2
                | Op::Set
                | Op::Fset
                | Op::Nconc => {
                    let b = pop!();
                    let a = pop!();
                    let name = match op {
                        Op::Memq => "memq",
                        Op::Nth => "nth",
                        Op::Aref => "aref",
                        Op::Setcar => "setcar",
                        Op::Setcdr => "setcdr",
                        Op::Nthcdr => "nthcdr",
                        Op::Elt => "elt",
                        Op::Member => "member",
                        Op::Assq => "assq",
                        Op::Equal => "equal",
                        Op::Get => "get",
                        Op::Quo => "/",
                        Op::Rem => "%",
                        Op::StringEqlsign => "string-equal",
                        Op::StringLss => "string-lessp",
                        Op::Concat2 => "concat",
                        Op::Set => "set",
                        Op::Fset => "fset",
                        _ => "nconc",
                    };
                    let value = prim(interp, name, &[a, b], env)?;
                    stack.push(value);
                }
                // One-argument ops with inline fast paths.
                Op::Car | Op::Cdr | Op::CarSafe | Op::CdrSafe => {
                    let a = pop!();
                    match (&a, op) {
                        (Value::Cons(car, _), Op::Car | Op::CarSafe) => {
                            let value = car.borrow().clone();
                            stack.push(value);
                        }
                        (Value::Cons(_, cdr), _) => {
                            let value = cdr.borrow().clone();
                            stack.push(value);
                        }
                        (Value::Nil, _) | (_, Op::CarSafe | Op::CdrSafe) => stack.push(Value::Nil),
                        _ => {
                            let name = if matches!(op, Op::Car) { "car" } else { "cdr" };
                            let value = prim(interp, name, &[a], env)?;
                            stack.push(value);
                        }
                    }
                }
                Op::Add1 | Op::Sub1 | Op::Negate => {
                    let a = pop!();
                    if let Value::Integer(x) = &a {
                        let fast = match op {
                            Op::Add1 => x.checked_add(1),
                            Op::Sub1 => x.checked_sub(1),
                            _ => x.checked_neg(),
                        };
                        if let Some(n) = fast {
                            stack.push(Value::Integer(n));
                            return Ok(());
                        }
                    }
                    let name = match op {
                        Op::Add1 => "1+",
                        Op::Sub1 => "1-",
                        _ => "-",
                    };
                    let value = prim(interp, name, &[a], env)?;
                    stack.push(value);
                }
                Op::Consp => {
                    let a = pop!();
                    stack.push(if matches!(a, Value::Cons(..)) {
                        Value::T
                    } else {
                        Value::Nil
                    });
                }
                // One-argument primitive ops.
                Op::Symbolp
                | Op::Stringp
                | Op::Listp
                | Op::Length
                | Op::SymbolValue
                | Op::SymbolFunction
                | Op::Nreverse
                | Op::Numberp
                | Op::Integerp
                | Op::Upcase
                | Op::Downcase => {
                    let a = pop!();
                    let name = match op {
                        Op::Symbolp => "symbolp",
                        Op::Stringp => "stringp",
                        Op::Listp => "listp",
                        Op::Length => "length",
                        Op::SymbolValue => "symbol-value",
                        Op::SymbolFunction => "symbol-function",
                        Op::Nreverse => "nreverse",
                        Op::Numberp => "numberp",
                        Op::Integerp => "integerp",
                        Op::Upcase => "upcase",
                        _ => "downcase",
                    };
                    let value = prim(interp, name, &[a], env)?;
                    stack.push(value);
                }
                // Three-argument primitive ops.
                Op::Aset | Op::Substring | Op::Concat3 => {
                    let c = pop!();
                    let b = pop!();
                    let a = pop!();
                    let name = match op {
                        Op::Aset => "aset",
                        Op::Substring => "substring",
                        _ => "concat",
                    };
                    let value = prim(interp, name, &[a, b, c], env)?;
                    stack.push(value);
                }
                Op::Concat4 => {
                    let d = pop!();
                    let c = pop!();
                    let b = pop!();
                    let a = pop!();
                    let value = prim(interp, "concat", &[a, b, c, d], env)?;
                    stack.push(value);
                }
            }
            Ok(())
        })();

        match step {
            Ok(()) => {}
            Err(LispError::Throw(tag, value)) if matches!(&tag, Value::Symbol(name) if name == "--emaxx-bytecode-return--") =>
            {
                break 'run Ok(value);
            }
            Err(error) => {
                // GNU's handler search: innermost first; Bpushcatch
                // handles `throw' by tag eq, Bpushconditioncase handles
                // signals whose condition list matches the clause head.
                let mut handled = false;
                while let Some(handler) = handlers.pop() {
                    if matches!(handler.kind, HandlerKind::Catch(_)) {
                        interp.pop_active_catch_tag();
                    }
                    let matched_value = match (&handler.kind, &error) {
                        (HandlerKind::Catch(tag), LispError::Throw(thrown, value)) => {
                            let same = prim(interp, "eq", &[tag.clone(), thrown.clone()], env)?;
                            if same.is_truthy() {
                                Some(value.clone())
                            } else {
                                None
                            }
                        }
                        (HandlerKind::ConditionCase(_), LispError::Throw(_, _))
                        | (HandlerKind::ConditionCase(_), LispError::Terminate(_))
                        | (HandlerKind::Catch(_), _) => None,
                        (HandlerKind::ConditionCase(clause), error) => {
                            let condition = error.condition_type();
                            let condition_list = interp.error_condition_names(&condition);
                            if Interpreter::clause_head_matches(clause, &condition, &condition_list)
                            {
                                Some(super::super::eval::error_condition_value(error))
                            } else {
                                None
                            }
                        }
                    };
                    if let Some(value) = matched_value {
                        while unwinds.len() > handler.unwind_len {
                            match unwinds.pop() {
                                Some(entry) => unwind_one(interp, entry, env)?,
                                None => break,
                            }
                        }
                        stack.truncate(handler.stack_len);
                        stack.push(value);
                        pc = object.instr_at(handler.dest);
                        handled = true;
                        break;
                    }
                }
                if !handled {
                    break 'run Err(error);
                }
            }
        }
    };

    // Balance any entries left by an abnormal exit (GNU unbind_to on
    // the frame's specpdl watermark), running unwind-protect handlers
    // and restoring saved buffer state on the way out; catch tags of
    // still-pushed handlers leave the registry with their frame.
    for handler in handlers {
        if matches!(handler.kind, HandlerKind::Catch(_)) {
            interp.pop_active_catch_tag();
        }
    }
    while let Some(entry) = unwinds.pop() {
        unwind_one(interp, entry, env)?;
    }
    result
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::super::tests::fixture_objects;
    use super::*;

    fn run(name: &str, args: &[Value]) -> Result<Value, LispError> {
        let mut interp = Interpreter::new();
        let mut env = Env::new();
        let object = fixture_objects()
            .remove(name)
            .unwrap_or_else(|| panic!("fixture {name} missing"));
        execute(&mut interp, &object, args, &mut env)
    }

    #[test]
    fn executes_arithmetic_fixture() {
        // (defun emaxx-fx-add (a b) (+ a b 1))
        let value = run("emaxx-fx-add", &[Value::Integer(2), Value::Integer(3)]).unwrap();
        assert_eq!(value, Value::Integer(6));
    }

    #[test]
    fn executes_branching_fixture() {
        // (defun emaxx-fx-branch (x) (if (consp x) (car x) (list x 'tagged)))
        let value = run(
            "emaxx-fx-branch",
            &[Value::list([Value::Integer(7), Value::Integer(8)])],
        )
        .unwrap();
        assert_eq!(value, Value::Integer(7));
        let value = run("emaxx-fx-branch", &[Value::Integer(5)]).unwrap();
        assert_eq!(
            format!("{value}"),
            format!(
                "{}",
                Value::list([Value::Integer(5), Value::symbol("tagged")])
            )
        );
    }

    #[test]
    fn executes_loop_fixture() {
        // (defun emaxx-fx-loop (n) (let ((acc nil)) (while (> n 0) (push n acc) (setq n (1- n))) acc))
        let value = run("emaxx-fx-loop", &[Value::Integer(3)]).unwrap();
        assert_eq!(
            format!("{value}"),
            format!(
                "{}",
                Value::list([Value::Integer(1), Value::Integer(2), Value::Integer(3)])
            )
        );
    }

    #[test]
    fn executes_rest_args_fixture() {
        // (defun emaxx-fx-rest (head &rest tail) (cons head (length tail)))
        let value = run(
            "emaxx-fx-rest",
            &[Value::symbol("a"), Value::symbol("b"), Value::symbol("c")],
        )
        .unwrap();
        assert_eq!(format!("{value}"), "(a . 2)");
        let value = run("emaxx-fx-rest", &[Value::symbol("solo")]).unwrap();
        assert_eq!(format!("{value}"), "(solo . 0)");
    }

    #[test]
    fn executes_condition_case_fixture() {
        // (defun emaxx-fx-catch (f) (condition-case err (funcall f) (error (cons 'caught err))))
        let mut interp = Interpreter::new();
        let mut env = Env::new();
        let object = fixture_objects().remove("emaxx-fx-catch").unwrap();
        // Non-signaling: (lambda () 42) via a builtin-friendly stand-in.
        let ok = execute(
            &mut interp,
            &object,
            &[Value::Lambda(
                Vec::new(),
                std::rc::Rc::new(vec![Value::Integer(42)]),
                std::rc::Rc::new(std::cell::RefCell::new(Vec::new())),
            )],
            &mut env,
        )
        .unwrap();
        assert_eq!(ok, Value::Integer(42));
        // Signaling: `car` of a non-list signals wrong-type-argument,
        // which the (error ...) clause catches.
        let caught = execute(
            &mut interp,
            &object,
            &[Value::Lambda(
                Vec::new(),
                std::rc::Rc::new(vec![Value::list([Value::symbol("car"), Value::Integer(9)])]),
                std::rc::Rc::new(std::cell::RefCell::new(Vec::new())),
            )],
            &mut env,
        )
        .unwrap();
        assert!(format!("{caught}").starts_with("(caught wrong-type-argument"));
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod phase_c_tests {
    use super::super::tests::{ORACLE_ELC2, named_objects};
    use super::*;

    fn run2(name: &str, args: &[Value]) -> Result<Value, LispError> {
        let mut interp = Interpreter::new();
        let mut env = Env::new();
        let object = named_objects(ORACLE_ELC2)
            .remove(name)
            .unwrap_or_else(|| panic!("fixture {name} missing"));
        execute(&mut interp, &object, args, &mut env)
    }

    #[test]
    fn executes_buffer_ops_fixture() {
        // (with-temp-buffer (insert "hello world") (goto-char (point-min))
        //   (forward-word 1) (list (point) (buffer-substring ...) (point-max)))
        let value = run2("emaxx-fx2-buffer", &[]).unwrap();
        assert_eq!(format!("{value}"), "(6 \"hello\" 12)");
    }

    #[test]
    fn executes_save_excursion_fixture() {
        // Point restored to 3 after save-excursion moves to point-max.
        let value = run2("emaxx-fx2-excursion", &[]).unwrap();
        assert_eq!(value, Value::Integer(3));
    }

    #[test]
    fn executes_unwind_protect_fixture() {
        // Normal exit: handler still runs (setcdr log 'cleaned).
        let mut interp = Interpreter::new();
        let mut env = Env::new();
        let object = named_objects(ORACLE_ELC2)
            .remove("emaxx-fx2-unwind")
            .unwrap();
        let log = Value::list([Value::symbol("payload"), Value::symbol("untouched")]);
        let value = execute(&mut interp, &object, std::slice::from_ref(&log), &mut env).unwrap();
        assert_eq!(format!("{value}"), "payload");
        assert_eq!(format!("{log}"), "(payload . cleaned)");
    }

    #[test]
    fn executes_unwind_protect_through_throw() {
        // The handler must run while the throw unwinds to the catch.
        let value = run2("emaxx-fx2-unwind-throw", &[Value::symbol("tag")]).unwrap();
        assert_eq!(format!("{value}"), "(cleaned)");
    }

    #[test]
    fn executes_dynamic_binding_fixture() {
        // (let ((emaxx-fx2-dyn value)) (symbol-value 'emaxx-fx2-dyn))
        let mut interp = Interpreter::new();
        let mut env = Env::new();
        // The defvar lives in the fixture file, not the test interp:
        // declare it as GNU load would.
        interp
            .eval(
                &Value::list([
                    Value::symbol("defvar"),
                    Value::symbol("emaxx-fx2-dyn"),
                    Value::list([Value::symbol("quote"), Value::symbol("unset")]),
                ]),
                &mut env,
            )
            .unwrap();
        let object = named_objects(ORACLE_ELC2)
            .remove("emaxx-fx2-dynbind")
            .unwrap();
        let value = execute(&mut interp, &object, &[Value::symbol("bound")], &mut env).unwrap();
        assert_eq!(format!("{value}"), "bound");
        // The binding must be undone after the call.
        let after = interp
            .eval(&Value::symbol("emaxx-fx2-dyn"), &mut env)
            .unwrap();
        assert_eq!(format!("{after}"), "unset");
    }

    #[test]
    fn executes_legacy_dynamic_argspec_fixture() {
        // (defun emaxx-fx3-legacy (a &optional b &rest c)
        //   (list a b (length c) (symbol-value 'a))) — dynamic binding:
        // formals are specbound, so symbol-value sees `a'.
        let mut interp = Interpreter::new();
        let mut env = Env::new();
        let object = super::super::tests::named_objects(super::super::tests::ORACLE_ELC3)
            .remove("emaxx-fx3-legacy")
            .unwrap();
        assert!(matches!(object.argspec, ArgSpec::Legacy(_)));
        let value = execute(
            &mut interp,
            &object,
            &[
                Value::Integer(1),
                Value::Integer(2),
                Value::Integer(3),
                Value::Integer(4),
            ],
            &mut env,
        )
        .unwrap();
        assert_eq!(format!("{value}"), "(1 2 2 1)");
        // Optional/rest defaults, and bindings must not leak out.
        let value = execute(&mut interp, &object, &[Value::Integer(9)], &mut env).unwrap();
        assert_eq!(format!("{value}"), "(9 nil 0 9)");
        assert!(interp.eval(&Value::symbol("a"), &mut env).is_err());
        // Too few arguments signals like GNU.
        assert!(execute(&mut interp, &object, &[], &mut env).is_err());
    }

    #[test]
    fn executes_switch_fixture() {
        for (key, expected) in [
            ("alpha", "1"),
            ("gamma", "3"),
            ("zeta", "6"),
            ("nomatch", "default"),
        ] {
            let value = run2("emaxx-fx2-switch", &[Value::symbol(key)]).unwrap();
            assert_eq!(format!("{value}"), expected, "case {key}");
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod native_surface_tests {
    use super::*;

    /// `make-byte-code` output must execute through `byte-code`-adjacent
    /// dispatch: build (lambda (x) (1+ x)) by hand and funcall it.
    #[test]
    fn make_byte_code_object_is_callable() {
        let mut interp = Interpreter::new();
        let mut env = Env::new();
        // argspec 257: one mandatory arg; code: dup; add1; return.
        let object = prim(
            &mut interp,
            "make-byte-code",
            &[
                Value::Integer(257),
                Value::String("\u{89}\u{54}\u{87}".into()),
                Value::list([Value::symbol("vector-literal")]),
                Value::Integer(3),
            ],
            &mut env,
        )
        .unwrap();
        let value = interp
            .call_function_value(object, None, &[Value::Integer(41)], &mut env)
            .unwrap();
        assert_eq!(value, Value::Integer(42));
    }

    /// GNU's `byte-code' executes a bare program against a constants
    /// vector; `internal-stack-stats' is a nil-returning telemetry stub.
    #[test]
    fn byte_code_primitive_executes_program() {
        let mut interp = Interpreter::new();
        let mut env = Env::new();
        // constant0; constant1; plus; return  with constants [40 2].
        let value = prim(
            &mut interp,
            "byte-code",
            &[
                Value::String("\u{c0}\u{c1}\u{5c}\u{87}".into()),
                Value::list([
                    Value::symbol("vector-literal"),
                    Value::Integer(40),
                    Value::Integer(2),
                ]),
                Value::Integer(4),
            ],
            &mut env,
        )
        .unwrap();
        assert_eq!(value, Value::Integer(42));
        assert_eq!(
            prim(&mut interp, "internal-stack-stats", &[], &mut env).unwrap(),
            Value::Nil
        );
    }
}
