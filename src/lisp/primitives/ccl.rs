use super::*;

const CCL_HEADER_EOF: usize = 1;
const CCL_HEADER_MAIN: usize = 2;
const CCL_CODE_MIN: i64 = -(1 << 27);
const CCL_CODE_MAX: i64 = (1 << 27) - 1;
const CHARSET_UNICODE: i32 = 2;

define_dispatch!(
    pub(super) fn call(
        interp: &mut Interpreter,
        name: &str,
        args: &[Value],
        env: &mut Env,
    ) -> Result<Value, LispError> {
        match name {
            "ccl-program-p" => {
                need_args(name, args, 1)?;
                Ok(if ccl_program_p(interp, &args[0]) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "register-ccl-program" => {
                need_args(name, args, 2)?;
                register_ccl_program(interp, &args[0], &args[1])
            }
            "register-code-conversion-map" => {
                need_args(name, args, 2)?;
                register_code_conversion_map(interp, &args[0], &args[1])
            }
            "ccl-execute" => {
                need_args(name, args, 2)?;
                ccl_execute(interp, &args[0], &args[1], env)
            }
            "ccl-execute-on-string" => {
                need_arg_range(name, args, 3, 5)?;
                ccl_execute_on_string(
                    interp,
                    &args[0],
                    &args[1],
                    &args[2],
                    args.get(3).is_some_and(Value::is_truthy),
                    args.get(4).is_some_and(Value::is_truthy),
                    env,
                )
            }
        }
    }
);

fn make_vector(items: impl IntoIterator<Item = Value>) -> Value {
    Value::list(std::iter::once(Value::Symbol("vector-literal".into())).chain(items))
}

fn ccl_program_p(interp: &Interpreter, object: &Value) -> bool {
    if is_vector_value(object) {
        return matches!(resolve_ccl_program(interp, object), Ok(Some(_)));
    }
    let Ok(name) = object.as_symbol() else {
        return false;
    };
    interp
        .get_symbol_property(name, "ccl-program-idx")
        .and_then(|value| value.as_integer().ok())
        .and_then(|index| usize::try_from(index).ok())
        .is_some_and(|index| index < interp.ccl_programs.len())
}

/// Resolve `(SYMBOL . PROPERTY)' operands emitted by ccl.el.  `None' means
/// the vector is structurally valid but one of those properties is not yet
/// defined; GNU permits that state at registration time and retries it when
/// the program is executed.
fn resolve_ccl_program(
    interp: &Interpreter,
    program: &Value,
) -> Result<Option<Vec<i32>>, LispError> {
    if !is_vector_value(program) {
        return Err(LispError::TypeError("vector".into(), program.type_name()));
    }
    let items = vector_items(program)?;
    if items.len() <= CCL_HEADER_MAIN {
        return Err(LispError::Signal("Invalid CCL program".into()));
    }

    let mut unresolved = false;
    let mut code = Vec::with_capacity(items.len());
    for item in items {
        let value = match item {
            Value::Integer(number) => Some(number),
            Value::BigInteger(_) => item.as_integer().ok(),
            Value::Cons(car, cdr) => {
                let symbol = car.borrow().as_symbol()?.to_string();
                let property = cdr.borrow().as_symbol()?.to_string();
                interp
                    .get_symbol_property(&symbol, &property)
                    .and_then(|value| value.as_integer().ok())
            }
            Value::Nil | Value::T | Value::Symbol(_) => {
                let symbol = item.as_symbol()?.to_string();
                [
                    "translation-table-id",
                    "code-conversion-map-id",
                    "ccl-program-idx",
                ]
                .into_iter()
                .find_map(|property| {
                    interp
                        .get_symbol_property(&symbol, property)
                        .and_then(|value| value.as_integer().ok())
                })
            }
            _ => return Err(LispError::Signal("Invalid CCL program".into())),
        };
        if let Some(value) = value {
            let value = i32::try_from(value)
                .map_err(|_| LispError::Signal("Invalid CCL program".into()))?;
            code.push(value);
        } else {
            unresolved = true;
            code.push(0);
        }
    }

    let magnification = i64::from(code[0]);
    let eof = i64::from(code[CCL_HEADER_EOF]);
    if magnification < 0 || eof < 0 || eof > code.len() as i64 {
        return Err(LispError::Signal("Invalid CCL program".into()));
    }
    Ok((!unresolved).then_some(code))
}

fn registered_program(interp: &Interpreter, program: &Value) -> Result<Value, LispError> {
    if is_vector_value(program) {
        return Ok(program.clone());
    }
    let name = program
        .as_symbol()
        .map_err(|_| LispError::Signal("Invalid CCL program".into()))?;
    let index = interp
        .get_symbol_property(name, "ccl-program-idx")
        .and_then(|value| value.as_integer().ok())
        .and_then(|value| usize::try_from(value).ok())
        .ok_or_else(|| LispError::Signal("Invalid CCL program".into()))?;
    interp
        .ccl_programs
        .get(index)
        .and_then(Option::as_ref)
        .and_then(|(_, program)| (!program.is_nil()).then_some(program.clone()))
        .ok_or_else(|| LispError::Signal("Invalid CCL program".into()))
}

fn executable_program(interp: &Interpreter, program: &Value) -> Result<Vec<i32>, LispError> {
    let program = registered_program(interp, program)?;
    resolve_ccl_program(interp, &program)?
        .ok_or_else(|| LispError::Signal("Invalid CCL program".into()))
}

fn register_ccl_program(
    interp: &mut Interpreter,
    name: &Value,
    program: &Value,
) -> Result<Value, LispError> {
    let name = name.as_symbol()?.to_string();
    if !program.is_nil() {
        // Registration accepts unresolved symbolic operands, but never an
        // invalid vector.  This mirrors resolve_symbol_ccl_program's t/nil
        // distinction in GNU ccl.c.
        let _ = resolve_ccl_program(interp, program)?;
    }

    let index = interp
        .ccl_programs
        .iter()
        .position(|slot| {
            slot.as_ref()
                .is_some_and(|(slot_name, _)| slot_name == &name)
        })
        .or_else(|| interp.ccl_programs.iter().position(Option::is_none))
        .unwrap_or_else(|| {
            interp.ccl_programs.push(None);
            interp.ccl_programs.len() - 1
        });
    interp.ccl_programs[index] = Some((name.clone(), program.clone()));
    interp.put_symbol_property(&name, "ccl-program-idx", Value::Integer(index as i64));
    Ok(Value::Integer(index as i64))
}

fn register_code_conversion_map(
    interp: &mut Interpreter,
    symbol: &Value,
    map: &Value,
) -> Result<Value, LispError> {
    let symbol = symbol.as_symbol()?.to_string();
    if !is_vector_value(map) {
        return Err(LispError::TypeError("vector".into(), map.type_name()));
    }
    let table = interp
        .default_value("code-conversion-map-vector")
        .ok_or_else(|| LispError::Void("code-conversion-map-vector".into()))?;
    if !is_vector_value(&table) {
        return Err(LispError::Signal(
            "Invalid code-conversion-map-vector".into(),
        ));
    }
    let slots = vector_items(&table)?;
    let mut index = None;
    for (candidate, slot) in slots.iter().enumerate() {
        match slot {
            Value::Cons(car, _) if car.borrow().as_symbol().ok() == Some(symbol.as_str()) => {
                index = Some(candidate);
                break;
            }
            Value::Cons(_, _) => {}
            _ => {
                index = Some(candidate);
                break;
            }
        }
    }
    let index = index.unwrap_or(slots.len());
    let entry = Value::cons(Value::Symbol(symbol.clone()), map.clone());
    if index < slots.len() {
        aset_vector_value(&table, index, entry)?;
    } else {
        let mut extended = slots;
        extended.push(entry);
        interp.set_global_binding("code-conversion-map-vector", make_vector(extended));
    }
    interp.put_symbol_property(&symbol, "code-conversion-map", map.clone());
    interp.put_symbol_property(
        &symbol,
        "code-conversion-map-id",
        Value::Integer(index as i64),
    );
    Ok(Value::Integer(index as i64))
}

fn initial_registers(value: &Value, length: usize) -> Result<[i32; 8], LispError> {
    if !is_vector_value(value) {
        return Err(LispError::TypeError("vector".into(), value.type_name()));
    }
    let items = vector_items(value)?;
    if items.len() != length {
        return Err(LispError::Signal(format!(
            "Length of vector is not {length}"
        )));
    }
    let mut registers = [0; 8];
    for (target, source) in registers.iter_mut().zip(items.iter()) {
        if let Ok(number) = source.as_integer()
            && let Ok(number) = i32::try_from(number)
        {
            *target = number;
        }
    }
    Ok(registers)
}

fn write_registers(vector: &Value, registers: &[i32; 8]) -> Result<(), LispError> {
    for (index, value) in registers.iter().enumerate() {
        aset_vector_value(vector, index, Value::Integer(i64::from(*value)))?;
    }
    Ok(())
}

fn ccl_execute(
    interp: &mut Interpreter,
    program: &Value,
    registers: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    let code = executable_program(interp, program)?;
    let initial = initial_registers(registers, 8)?;
    let mut machine = CclMachine::new(code, initial, Vec::new(), true, false);
    machine.run(interp, env)?;
    write_registers(registers, &machine.registers)?;
    Ok(Value::Nil)
}

fn ccl_execute_on_string(
    interp: &mut Interpreter,
    program: &Value,
    status: &Value,
    source: &Value,
    continue_at_eof: bool,
    unibyte: bool,
    env: &mut Env,
) -> Result<Value, LispError> {
    let code = executable_program(interp, program)?;
    let initial = initial_registers(status, 9)?;
    let status_items = vector_items(status)?;
    let start_pc = status_items
        .get(8)
        .and_then(|value| value.as_integer().ok())
        .and_then(|value| usize::try_from(value).ok())
        .filter(|pc| CCL_HEADER_MAIN < *pc && *pc < code.len())
        .unwrap_or(CCL_HEADER_MAIN);
    let string = string_like(source)
        .ok_or_else(|| LispError::TypeError("string".into(), source.type_name()))?;
    let input = if string.multibyte {
        string.text.chars().map(|ch| ch as i32).collect()
    } else {
        internal_text_bytes(&string.text, false)?
            .into_iter()
            .map(i32::from)
            .collect()
    };
    let mut machine = CclMachine::new(code, initial, input, !continue_at_eof, true);
    machine.pc = start_pc;
    machine.run(interp, env)?;
    write_registers(status, &machine.registers)?;
    aset_vector_value(status, 8, Value::Integer(machine.pc as i64))?;

    if unibyte {
        let bytes = machine
            .output
            .into_iter()
            .map(|value| value as u8)
            .collect::<Vec<_>>();
        Ok(bytes_to_shared_unibyte_value(&bytes))
    } else {
        let text = machine
            .output
            .into_iter()
            .map(|value| char::from_u32(value as u32).unwrap_or('\u{FFFD}'))
            .collect();
        Ok(Value::String(text))
    }
}

struct CclMachine {
    code: Vec<i32>,
    registers: [i32; 8],
    pc: usize,
    eof_pc: usize,
    input: Vec<i32>,
    input_index: usize,
    output: Vec<i32>,
    final_input: bool,
    io_enabled: bool,
    suspended: bool,
}

impl CclMachine {
    fn new(
        code: Vec<i32>,
        registers: [i32; 8],
        input: Vec<i32>,
        final_input: bool,
        io_enabled: bool,
    ) -> Self {
        let eof_pc = code[CCL_HEADER_EOF] as usize;
        Self {
            code,
            registers,
            pc: CCL_HEADER_MAIN,
            eof_pc,
            input,
            input_index: 0,
            output: Vec::new(),
            final_input,
            io_enabled,
            suspended: false,
        }
    }

    fn error(&self, pc: usize) -> LispError {
        LispError::Signal(format!("Error in CCL program at {pc}th code"))
    }

    fn word(&self, pc: usize) -> Result<i32, LispError> {
        self.code.get(pc).copied().ok_or_else(|| self.error(pc))
    }

    fn next_word(&mut self) -> Result<i32, LispError> {
        let word = self.word(self.pc)?;
        self.pc += 1;
        Ok(word)
    }

    fn jump(&mut self, offset: i32, at: usize) -> Result<(), LispError> {
        self.pc =
            usize::try_from(self.pc as i64 + i64::from(offset)).map_err(|_| self.error(at))?;
        if self.pc >= self.code.len() {
            return Err(self.error(at));
        }
        Ok(())
    }

    fn read_into(&mut self, register: usize, instruction_pc: usize) -> Result<bool, LispError> {
        if !self.io_enabled {
            return Err(self.error(instruction_pc));
        }
        if let Some(value) = self.input.get(self.input_index).copied() {
            self.input_index += 1;
            self.registers[register] = value;
            return Ok(true);
        }
        if self.final_input {
            self.registers[register] = -1;
            self.pc = self.eof_pc;
        } else {
            self.pc = instruction_pc;
            self.suspended = true;
        }
        Ok(false)
    }

    fn write(&mut self, value: i32, instruction_pc: usize) -> Result<(), LispError> {
        if !self.io_enabled {
            return Err(self.error(instruction_pc));
        }
        self.output.push(value);
        Ok(())
    }

    fn arithmetic(&mut self, op: i32, left: i32, right: i32) -> Result<i32, LispError> {
        let value = match op {
            0x00 => left.wrapping_add(right),
            0x01 => left.wrapping_sub(right),
            0x02 => left.wrapping_mul(right),
            0x03 if right != 0 => left.checked_div(right).unwrap_or(left),
            0x04 if right != 0 => left.checked_rem(right).unwrap_or(0),
            0x05 => left & right,
            0x06 => left | right,
            0x07 => left ^ right,
            0x08 if right >= 0 => {
                if right < i32::BITS as i32 {
                    (left as u32).wrapping_shl(right as u32) as i32
                } else {
                    0
                }
            }
            0x09 if right >= 0 => left >> right.min(i32::BITS as i32 - 1),
            0x0A => ((left as u32).wrapping_shl(8) as i32) | right,
            0x0B => {
                self.registers[7] = left & 0xFF;
                left >> 8
            }
            0x0C if right != 0 => {
                self.registers[7] = left.checked_rem(right).unwrap_or(0);
                left.checked_div(right).unwrap_or(left.wrapping_neg())
            }
            0x10 => i32::from(left < right),
            0x11 => i32::from(left > right),
            0x12 => i32::from(left == right),
            0x13 => i32::from(left <= right),
            0x14 => i32::from(left >= right),
            0x15 => i32::from(left != right),
            _ => return Err(self.error(self.pc.saturating_sub(1))),
        };
        Ok(value)
    }

    fn write_packed_string(&mut self, length: usize, start: usize) -> Result<(), LispError> {
        if !self.io_enabled {
            return Err(self.error(start.saturating_sub(1)));
        }
        let multibyte = self.word(start)? & 0x0100_0000 != 0;
        if multibyte {
            for offset in 0..length {
                self.output.push(self.word(start + offset)? & 0x00FF_FFFF);
            }
        } else {
            for offset in 0..length {
                let word = self.word(start + offset / 3)? as u32;
                let shift = (2 - offset % 3) * 8;
                self.output.push(((word >> shift) & 0xFF) as i32);
            }
        }
        Ok(())
    }

    fn lookup_integer(
        &mut self,
        interp: &mut Interpreter,
        table_id: usize,
        key_register: usize,
        value_register: usize,
        env: &mut Env,
    ) -> Result<(), LispError> {
        let tables = interp
            .default_value("translation-hash-table-vector")
            .ok_or_else(|| self.error(self.pc.saturating_sub(1)))?;
        let slot = vector_slot_value(&tables, table_id)?;
        let Value::Cons(_, table) = slot else {
            return Err(self.error(self.pc.saturating_sub(1)));
        };
        let table = table.borrow().clone();
        let Some((test, entries)) = json::hash_table_entries(interp, &table) else {
            return Err(self.error(self.pc.saturating_sub(1)));
        };
        let key = Value::Integer(i64::from(self.registers[key_register]));
        for (candidate, value) in entries {
            if hash_table_key_matches(interp, &table, &test, &candidate, &key, env)? {
                let value = i32::try_from(value.as_integer()?)
                    .map_err(|_| self.error(self.pc.saturating_sub(1)))?;
                self.registers[key_register] = CHARSET_UNICODE;
                self.registers[value_register] = value;
                self.registers[7] = 1;
                return Ok(());
            }
        }
        self.registers[7] = 0;
        Ok(())
    }

    fn run(&mut self, interp: &mut Interpreter, env: &mut Env) -> Result<(), LispError> {
        let mut steps = 0usize;
        while !self.suspended {
            steps += 1;
            if steps > 10_000_000 {
                return Err(self.error(self.pc));
            }
            let instruction_pc = self.pc;
            let code = self.next_word()?;
            if !(CCL_CODE_MIN..=CCL_CODE_MAX).contains(&i64::from(code)) {
                return Err(self.error(instruction_pc));
            }
            let command = code & 0x1F;
            let field1 = code >> 8;
            let rrr = ((code & 0xFF) >> 5) as usize;
            let rrr_upper = (field1 & 7) as usize;
            let rrr_third = ((field1 >> 3) & 7) as usize;

            match command {
                0x00 => self.registers[rrr] = self.registers[rrr_upper],
                0x01 => self.registers[rrr] = field1,
                0x02 => self.registers[rrr] = self.next_word()?,
                0x03 => {
                    let index = self.registers[rrr_upper];
                    let length = field1 >> 3;
                    if 0 <= index && index < length {
                        self.registers[rrr] = self.word(self.pc + index as usize)?;
                    }
                    self.pc += length as usize;
                }
                0x04 => self.jump(field1, instruction_pc)?,
                0x05 => {
                    if self.registers[rrr] == 0 {
                        self.jump(field1, instruction_pc)?;
                    }
                }
                0x06 => {
                    self.write(self.registers[rrr], instruction_pc)?;
                    self.jump(field1, instruction_pc)?;
                }
                0x07 => {
                    self.write(self.registers[rrr], instruction_pc)?;
                    self.pc += 1;
                    if !self.read_into(rrr, instruction_pc)? {
                        continue;
                    }
                    self.jump(field1 - 1, instruction_pc)?;
                }
                0x08 => {
                    self.write(self.word(self.pc)?, instruction_pc)?;
                    self.jump(field1, instruction_pc)?;
                }
                0x09 => {
                    self.write(self.word(self.pc)?, instruction_pc)?;
                    self.pc += 1;
                    if !self.read_into(rrr, instruction_pc)? {
                        continue;
                    }
                    self.jump(field1 - 1, instruction_pc)?;
                }
                0x0A => {
                    let length = usize::try_from(self.next_word()?)
                        .map_err(|_| self.error(instruction_pc))?;
                    self.write_packed_string(length, self.pc)?;
                    self.jump(field1 - 1, instruction_pc)?;
                }
                0x0B => {
                    let index = self.registers[rrr];
                    let length = self.word(self.pc)?;
                    if 0 <= index && index < length {
                        self.write(self.word(self.pc + 1 + index as usize)?, instruction_pc)?;
                    }
                    self.pc += length as usize + 2;
                    if !self.read_into(rrr, instruction_pc)? {
                        continue;
                    }
                    self.jump(field1 - (length + 2), instruction_pc)?;
                }
                0x0C => {
                    if !self.read_into(rrr, instruction_pc)? {
                        continue;
                    }
                    self.jump(field1, instruction_pc)?;
                }
                0x0D | 0x10 => {
                    if command == 0x10 && !self.read_into(rrr, instruction_pc)? {
                        continue;
                    }
                    let index = if 0 <= self.registers[rrr] && self.registers[rrr] < field1 {
                        self.registers[rrr] as usize
                    } else {
                        field1 as usize
                    };
                    let offset = self.word(self.pc + index)?;
                    self.jump(offset, instruction_pc)?;
                }
                0x0E => {
                    let mut remaining = field1;
                    let mut register = rrr;
                    loop {
                        if !self.read_into(register, instruction_pc)? {
                            break;
                        }
                        if remaining == 0 {
                            break;
                        }
                        let next = self.next_word()?;
                        remaining = next >> 8;
                        register = ((next & 0xFF) >> 5) as usize;
                    }
                }
                0x0F => {
                    let op = field1 >> 6;
                    let right = self.next_word()?;
                    let value = self.arithmetic(op, self.registers[rrr_upper], right)?;
                    self.registers[7] = value;
                    self.write(value, instruction_pc)?;
                }
                0x11 => {
                    let mut remaining = field1;
                    let mut register = rrr;
                    loop {
                        self.write(self.registers[register], instruction_pc)?;
                        if remaining == 0 {
                            break;
                        }
                        let next = self.next_word()?;
                        remaining = next >> 8;
                        register = ((next & 0xFF) >> 5) as usize;
                    }
                }
                0x12 => {
                    let op = field1 >> 6;
                    let value =
                        self.arithmetic(op, self.registers[rrr_upper], self.registers[rrr_third])?;
                    self.registers[7] = value;
                    self.write(value, instruction_pc)?;
                }
                0x13 => return Err(self.error(instruction_pc)),
                0x14 => {
                    if rrr == 0 {
                        self.write(field1, instruction_pc)?;
                    } else {
                        let length =
                            usize::try_from(field1).map_err(|_| self.error(instruction_pc))?;
                        self.write_packed_string(length, self.pc)?;
                        self.pc += length.div_ceil(3);
                    }
                }
                0x15 => {
                    let index = self.registers[rrr];
                    if 0 <= index && index < field1 {
                        self.write(self.word(self.pc + index as usize)?, instruction_pc)?;
                    }
                    self.pc += field1 as usize;
                }
                0x16 => {
                    self.pc = instruction_pc;
                    return Ok(());
                }
                0x17 | 0x18 => {
                    let right = if command == 0x17 {
                        self.next_word()?
                    } else {
                        self.registers[rrr_upper]
                    };
                    let op = field1 >> 6;
                    self.registers[rrr] = self.arithmetic(op, self.registers[rrr], right)?;
                }
                0x19 | 0x1A => {
                    let right = if command == 0x19 {
                        self.next_word()?
                    } else {
                        self.registers[rrr_third]
                    };
                    let op = field1 >> 6;
                    self.registers[rrr] = self.arithmetic(op, self.registers[rrr_upper], right)?;
                }
                0x1B..=0x1E => {
                    if matches!(command, 0x1D | 0x1E) && !self.read_into(rrr, instruction_pc)? {
                        continue;
                    }
                    let left = self.registers[rrr];
                    let jump_pc = self.pc as i64 + i64::from(field1);
                    let op = self.next_word()?;
                    let right = if matches!(command, 0x1B | 0x1D) {
                        self.next_word()?
                    } else {
                        let register = usize::try_from(self.next_word()?)
                            .map_err(|_| self.error(instruction_pc))?;
                        *self
                            .registers
                            .get(register)
                            .ok_or_else(|| self.error(instruction_pc))?
                    };
                    self.registers[7] = self.arithmetic(op, left, right)?;
                    if self.registers[7] == 0 {
                        self.pc =
                            usize::try_from(jump_pc).map_err(|_| self.error(instruction_pc))?;
                    }
                }
                0x1F => {
                    let extension = field1 >> 6;
                    match extension {
                        0x13 => {
                            let table_id = usize::try_from(self.next_word()?)
                                .map_err(|_| self.error(instruction_pc))?;
                            self.lookup_integer(interp, table_id, rrr_upper, rrr, env)?;
                        }
                        _ => return Err(self.error(instruction_pc)),
                    }
                }
                _ => return Err(self.error(instruction_pc)),
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vector(items: impl IntoIterator<Item = Value>) -> Value {
        make_vector(items)
    }

    #[test]
    fn pgg_crc24_program_updates_status_like_gnu_ccl() {
        let mut interp = Interpreter::new();
        let program = vector(
            [
                1, 30, 14, 114744, 114775, 0, 161, 131127, 1, 148217, 15, 82167, 1, 1848, 131159,
                1, 1595, 5, 256, 114743, 390, 114775, 19707, 1467, 16, 7, 183, 1, -5628, -7164, 22,
            ]
            .into_iter()
            .map(Value::Integer),
        );
        let status = vector(
            [Value::Nil, Value::Integer(183), Value::Integer(1230)]
                .into_iter()
                .chain(std::iter::repeat_n(Value::Nil, 6)),
        );
        let result = ccl_execute_on_string(
            &mut interp,
            &program,
            &status,
            &Value::String("foo".into()),
            false,
            false,
            &mut Vec::new(),
        )
        .expect("execute GNU's pgg CRC24 program");

        assert_eq!(result, Value::String(String::new()));
        let values = vector_items(&status).expect("status vector");
        assert_eq!(
            values,
            vec![
                Value::Integer(-1),
                Value::Integer(79),
                Value::Integer(687_194_709),
                Value::Integer(0),
                Value::Integer(0),
                Value::Integer(7),
                Value::Integer(0),
                Value::Integer(0),
                Value::Integer(30),
            ]
        );
    }

    #[test]
    fn symbolic_hash_operand_resolves_when_the_table_is_defined() {
        let mut interp = Interpreter::new();
        let table = json::make_hash_table(
            &mut interp,
            "eq",
            vec![(Value::Integer(17), Value::Integer(16))],
        );
        interp.set_global_binding(
            "translation-hash-table-vector",
            vector([Value::cons(Value::Symbol("table".into()), table)]),
        );
        interp.put_symbol_property("table", "translation-hash-table-id", Value::Integer(0));
        let program = vector([
            Value::Integer(2),
            Value::Integer(4),
            Value::Integer(311_359),
            Value::cons(
                Value::Symbol("table".into()),
                Value::Symbol("translation-hash-table-id".into()),
            ),
            Value::Integer(22),
        ]);
        let registers = vector(
            [Value::Integer(17)]
                .into_iter()
                .chain(std::iter::repeat_n(Value::Integer(0), 7)),
        );

        ccl_execute(&mut interp, &program, &registers, &mut Vec::new())
            .expect("execute lookup-integer");
        assert_eq!(
            vector_items(&registers).expect("register vector"),
            vec![
                Value::Integer(2),
                Value::Integer(16),
                Value::Integer(0),
                Value::Integer(0),
                Value::Integer(0),
                Value::Integer(0),
                Value::Integer(0),
                Value::Integer(1),
            ]
        );
    }
}
