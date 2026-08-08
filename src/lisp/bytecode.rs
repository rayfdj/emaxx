//! GNU Emacs 30.2 bytecode decoding and validation.
//!
//! Ports the opcode set and operand encodings of GNU `src/bytecode.c`
//! (`BYTE_CODES`, `exec_byte_code`) so genuine `.elc` byte-code-function
//! objects can be decoded, validated, and — in a later phase — executed.
//! This phase covers decoding only: recognizing genuine objects, walking
//! their opcode stream, and rejecting unsupported or corrupt bytecode
//! with precise errors instead of misexecuting it (issue #10).

pub mod vm;

use super::types::Value;

/// Why a byte-code object or its opcode stream was rejected.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ByteCodeError {
    /// A byte value GNU 30.2 leaves undefined (its exec loop signals
    /// "Invalid byte opcode"), at this offset in the code string.
    UnknownOpcode { offset: usize, byte: u8 },
    /// The code string ended inside an instruction's operand bytes.
    TruncatedOperand { offset: usize, byte: u8 },
    /// A constants-vector reference is out of range.
    ConstantOutOfRange {
        offset: usize,
        index: usize,
        constants_len: usize,
    },
    /// A jump destination lies outside the code string.
    JumpOutOfRange {
        offset: usize,
        target: usize,
        code_len: usize,
    },
    /// A jump destination lands inside another instruction; the GNU
    /// compiler only ever emits instruction-boundary targets, so this
    /// indicates corruption.
    JumpIntoInstruction { offset: usize, target: usize },
    /// The object's slots do not form a genuine GNU byte-code function
    /// (wrong types or too few slots).
    MalformedObject(String),
    /// The code string contains a char above U+00FF, so it cannot be a
    /// unibyte opcode string.
    NonUnibyteCode { char_index: usize },
}

impl std::fmt::Display for ByteCodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ByteCodeError::UnknownOpcode { offset, byte } => {
                write!(f, "invalid byte opcode: op={byte}, ptr={offset}")
            }
            ByteCodeError::TruncatedOperand { offset, byte } => {
                write!(f, "truncated operand for opcode {byte} at {offset}")
            }
            ByteCodeError::ConstantOutOfRange {
                offset,
                index,
                constants_len,
            } => write!(
                f,
                "constant index {index} out of range (vector length {constants_len}) at {offset}"
            ),
            ByteCodeError::JumpOutOfRange {
                offset,
                target,
                code_len,
            } => write!(
                f,
                "jump target {target} out of range (code length {code_len}) at {offset}"
            ),
            ByteCodeError::JumpIntoInstruction { offset, target } => {
                write!(
                    f,
                    "jump target {target} not an instruction boundary at {offset}"
                )
            }
            ByteCodeError::MalformedObject(detail) => {
                write!(f, "malformed byte-code object: {detail}")
            }
            ByteCodeError::NonUnibyteCode { char_index } => {
                write!(f, "byte-code string is not unibyte at char {char_index}")
            }
        }
    }
}

/// A decoded instruction: the semantic operation with its operand
/// resolved from immediate bits or trailing bytes.  Families that GNU
/// spreads over eight opcode values (`Bvarref`..`Bvarref7`) collapse to
/// one variant carrying the resolved operand.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Op {
    /// Push the stack slot OPERAND below the top (Bstack_ref1..7).
    StackRef(u16),
    /// Push the dynamic value of constants[OPERAND] (Bvarref family).
    VarRef(u16),
    /// Set the dynamic value of constants[OPERAND] from popped TOS.
    VarSet(u16),
    /// Dynamically bind constants[OPERAND] to popped TOS.
    VarBind(u16),
    /// Call with OPERAND argument values (function below them on stack).
    Call(u16),
    /// Unbind OPERAND special bindings.
    Unbind(u16),
    /// Pop one handler (Bpophandler).
    PopHandler,
    /// Push a condition-case handler jumping to TARGET on signal.
    PushConditionCase {
        target: u16,
    },
    /// Push a catch handler jumping to TARGET on throw.
    PushCatch {
        target: u16,
    },
    Nth,
    Symbolp,
    Consp,
    Stringp,
    Listp,
    Eq,
    Memq,
    Not,
    Car,
    Cdr,
    Cons,
    List1,
    List2,
    List3,
    List4,
    Length,
    Aref,
    Aset,
    SymbolValue,
    SymbolFunction,
    Set,
    Fset,
    Get,
    Substring,
    Concat2,
    Concat3,
    Concat4,
    Sub1,
    Add1,
    Eqlsign,
    Gtr,
    Lss,
    Leq,
    Geq,
    Diff,
    Negate,
    Plus,
    Max,
    Min,
    Mult,
    Point,
    /// Bsave_current_buffer_OBSOLETE (executes identically in GNU 30).
    SaveCurrentBufferObsolete,
    GotoChar,
    Insert,
    PointMax,
    PointMin,
    CharAfter,
    FollowingChar,
    PrecedingChar,
    CurrentColumn,
    IndentTo,
    Eolp,
    Eobp,
    Bolp,
    Bobp,
    CurrentBuffer,
    SetBuffer,
    SaveCurrentBuffer,
    /// Binteractive_p (obsolete since 24.1 but still executed).
    InteractiveP,
    ForwardChar,
    ForwardWord,
    SkipCharsForward,
    SkipCharsBackward,
    ForwardLine,
    CharSyntax,
    BufferSubstring,
    DeleteRegion,
    NarrowToRegion,
    Widen,
    EndOfLine,
    /// Push constants[OPERAND] (Bconstant2, two-byte index).
    Constant2(u16),
    Goto {
        target: u16,
    },
    GotoIfNil {
        target: u16,
    },
    GotoIfNonNil {
        target: u16,
    },
    GotoIfNilElsePop {
        target: u16,
    },
    GotoIfNonNilElsePop {
        target: u16,
    },
    Return,
    Discard,
    Dup,
    SaveExcursion,
    /// Bsave_window_excursion (obsolete since 24.1 but still executed).
    SaveWindowExcursion,
    SaveRestriction,
    /// Bcatch (obsolete since 25 but still executed).
    Catch,
    UnwindProtect,
    /// Bcondition_case (obsolete since 25 but still executed).
    ConditionCase,
    /// Btemp_output_buffer_setup (obsolete since 24.1).
    TempOutputBufferSetup,
    /// Btemp_output_buffer_show (obsolete since 24.1).
    TempOutputBufferShow,
    SetMarker,
    MatchBeginning,
    MatchEnd,
    Upcase,
    Downcase,
    StringEqlsign,
    StringLss,
    Equal,
    Nthcdr,
    Elt,
    Member,
    Assq,
    Nreverse,
    Setcar,
    Setcdr,
    CarSafe,
    CdrSafe,
    Nconc,
    Quo,
    Rem,
    Numberp,
    Integerp,
    /// Pop OPERAND-1 extra values and push (list OPERAND-values) (BlistN).
    ListN(u8),
    /// Concat of OPERAND values (BconcatN).
    ConcatN(u8),
    /// Insert of OPERAND values (BinsertN).
    InsertN(u8),
    /// Store popped TOS into the slot OPERAND below the (new) top.
    StackSet(u16),
    /// Discard OPERAND values; when PRESERVE_TOS, keep TOS on top of the
    /// remaining stack (BdiscardN with bit 0x80).
    DiscardN {
        count: u8,
        preserve_tos: bool,
    },
    /// Jump via the hash-table jump table on TOS (Bswitch).
    Switch,
    /// Push constants[OPERAND] (Bconstant, index packed in the opcode).
    Constant(u16),
}

/// A decoded instruction and where it sits in the code string.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Instr {
    /// Byte offset of the opcode within the code string.
    pub offset: usize,
    /// Total encoded length (opcode plus operand bytes).
    pub len: usize,
    pub op: Op,
}

// GNU bytecode.c opcode values (octal, as in BYTE_CODES).
const B_STACK_REF: u8 = 0o000;
const B_VARREF: u8 = 0o010;
const B_VARSET: u8 = 0o020;
const B_VARBIND: u8 = 0o030;
const B_CALL: u8 = 0o040;
const B_UNBIND: u8 = 0o050;
const B_POPHANDLER: u8 = 0o060;
const B_PUSHCONDITIONCASE: u8 = 0o061;
const B_PUSHCATCH: u8 = 0o062;
const B_NTH: u8 = 0o070;
const B_CONSTANT2: u8 = 0o201;
const B_GOTO: u8 = 0o202;
const B_RETURN: u8 = 0o207;
const B_LISTN: u8 = 0o257;
const B_CONCATN: u8 = 0o260;
const B_INSERTN: u8 = 0o261;
const B_STACK_SET: u8 = 0o262;
const B_STACK_SET2: u8 = 0o263;
const B_DISCARDN: u8 = 0o266;
const B_SWITCH: u8 = 0o267;
const B_CONSTANT: u8 = 0o300;

/// Operand reader over the code string (GNU's FETCH/FETCH2), carrying
/// the instruction context so truncation errors are precise.
struct OperandReader<'a> {
    code: &'a [u8],
    cursor: usize,
    offset: usize,
    byte: u8,
}

impl OperandReader<'_> {
    fn fetch(&mut self) -> Result<u16, ByteCodeError> {
        let value = *self
            .code
            .get(self.cursor)
            .ok_or(ByteCodeError::TruncatedOperand {
                offset: self.offset,
                byte: self.byte,
            })?;
        self.cursor += 1;
        Ok(value as u16)
    }

    fn fetch2(&mut self) -> Result<u16, ByteCodeError> {
        let low = self.fetch()?;
        let high = self.fetch()?;
        Ok(low | (high << 8))
    }

    /// The six operand-in-opcode families share one layout: +0..+5 pack
    /// the operand into the opcode, +6 takes one trailing byte, +7 two.
    fn family_operand(&mut self, base: u8) -> Result<u16, ByteCodeError> {
        match self.byte - base {
            immediate @ 0..=5 => Ok(immediate as u16),
            6 => self.fetch(),
            _ => self.fetch2(),
        }
    }
}

/// Decode one instruction starting at `offset`.  `code` is the whole
/// string so jump validation can happen later against full bounds.
fn decode_instr(code: &[u8], offset: usize) -> Result<Instr, ByteCodeError> {
    let byte = code[offset];
    let mut reader = OperandReader {
        code,
        cursor: offset + 1,
        offset,
        byte,
    };

    let op = match byte {
        // Bstack_ref+0 is deliberately unimplemented in GNU (its slot is
        // the CASE_ABORT arm): the compiler emits Bdup instead.
        B_STACK_REF => return Err(ByteCodeError::UnknownOpcode { offset, byte }),
        0o001..=0o007 => Op::StackRef(reader.family_operand(B_STACK_REF)?),
        0o010..=0o017 => Op::VarRef(reader.family_operand(B_VARREF)?),
        0o020..=0o027 => Op::VarSet(reader.family_operand(B_VARSET)?),
        0o030..=0o037 => Op::VarBind(reader.family_operand(B_VARBIND)?),
        0o040..=0o047 => Op::Call(reader.family_operand(B_CALL)?),
        0o050..=0o057 => Op::Unbind(reader.family_operand(B_UNBIND)?),
        B_POPHANDLER => Op::PopHandler,
        B_PUSHCONDITIONCASE => Op::PushConditionCase {
            target: reader.fetch2()?,
        },
        B_PUSHCATCH => Op::PushCatch {
            target: reader.fetch2()?,
        },
        B_NTH => Op::Nth,
        0o071 => Op::Symbolp,
        0o072 => Op::Consp,
        0o073 => Op::Stringp,
        0o074 => Op::Listp,
        0o075 => Op::Eq,
        0o076 => Op::Memq,
        0o077 => Op::Not,
        0o100 => Op::Car,
        0o101 => Op::Cdr,
        0o102 => Op::Cons,
        0o103 => Op::List1,
        0o104 => Op::List2,
        0o105 => Op::List3,
        0o106 => Op::List4,
        0o107 => Op::Length,
        0o110 => Op::Aref,
        0o111 => Op::Aset,
        0o112 => Op::SymbolValue,
        0o113 => Op::SymbolFunction,
        0o114 => Op::Set,
        0o115 => Op::Fset,
        0o116 => Op::Get,
        0o117 => Op::Substring,
        0o120 => Op::Concat2,
        0o121 => Op::Concat3,
        0o122 => Op::Concat4,
        0o123 => Op::Sub1,
        0o124 => Op::Add1,
        0o125 => Op::Eqlsign,
        0o126 => Op::Gtr,
        0o127 => Op::Lss,
        0o130 => Op::Leq,
        0o131 => Op::Geq,
        0o132 => Op::Diff,
        0o133 => Op::Negate,
        0o134 => Op::Plus,
        0o135 => Op::Max,
        0o136 => Op::Min,
        0o137 => Op::Mult,
        0o140 => Op::Point,
        0o141 => Op::SaveCurrentBufferObsolete,
        0o142 => Op::GotoChar,
        0o143 => Op::Insert,
        0o144 => Op::PointMax,
        0o145 => Op::PointMin,
        0o146 => Op::CharAfter,
        0o147 => Op::FollowingChar,
        0o150 => Op::PrecedingChar,
        0o151 => Op::CurrentColumn,
        0o152 => Op::IndentTo,
        0o154 => Op::Eolp,
        0o155 => Op::Eobp,
        0o156 => Op::Bolp,
        0o157 => Op::Bobp,
        0o160 => Op::CurrentBuffer,
        0o161 => Op::SetBuffer,
        0o162 => Op::SaveCurrentBuffer,
        0o164 => Op::InteractiveP,
        0o165 => Op::ForwardChar,
        0o166 => Op::ForwardWord,
        0o167 => Op::SkipCharsForward,
        0o170 => Op::SkipCharsBackward,
        0o171 => Op::ForwardLine,
        0o172 => Op::CharSyntax,
        0o173 => Op::BufferSubstring,
        0o174 => Op::DeleteRegion,
        0o175 => Op::NarrowToRegion,
        0o176 => Op::Widen,
        0o177 => Op::EndOfLine,
        B_CONSTANT2 => Op::Constant2(reader.fetch2()?),
        B_GOTO => Op::Goto {
            target: reader.fetch2()?,
        },
        0o203 => Op::GotoIfNil {
            target: reader.fetch2()?,
        },
        0o204 => Op::GotoIfNonNil {
            target: reader.fetch2()?,
        },
        0o205 => Op::GotoIfNilElsePop {
            target: reader.fetch2()?,
        },
        0o206 => Op::GotoIfNonNilElsePop {
            target: reader.fetch2()?,
        },
        B_RETURN => Op::Return,
        0o210 => Op::Discard,
        0o211 => Op::Dup,
        0o212 => Op::SaveExcursion,
        0o213 => Op::SaveWindowExcursion,
        0o214 => Op::SaveRestriction,
        0o215 => Op::Catch,
        0o216 => Op::UnwindProtect,
        0o217 => Op::ConditionCase,
        0o220 => Op::TempOutputBufferSetup,
        0o221 => Op::TempOutputBufferShow,
        0o223 => Op::SetMarker,
        0o224 => Op::MatchBeginning,
        0o225 => Op::MatchEnd,
        0o226 => Op::Upcase,
        0o227 => Op::Downcase,
        0o230 => Op::StringEqlsign,
        0o231 => Op::StringLss,
        0o232 => Op::Equal,
        0o233 => Op::Nthcdr,
        0o234 => Op::Elt,
        0o235 => Op::Member,
        0o236 => Op::Assq,
        0o237 => Op::Nreverse,
        0o240 => Op::Setcar,
        0o241 => Op::Setcdr,
        0o242 => Op::CarSafe,
        0o243 => Op::CdrSafe,
        0o244 => Op::Nconc,
        0o245 => Op::Quo,
        0o246 => Op::Rem,
        0o247 => Op::Numberp,
        0o250 => Op::Integerp,
        B_LISTN => Op::ListN(reader.fetch()? as u8),
        B_CONCATN => Op::ConcatN(reader.fetch()? as u8),
        B_INSERTN => Op::InsertN(reader.fetch()? as u8),
        B_STACK_SET => Op::StackSet(reader.fetch()?),
        B_STACK_SET2 => Op::StackSet(reader.fetch2()?),
        B_DISCARDN => {
            let raw = reader.fetch()? as u8;
            Op::DiscardN {
                count: raw & 0x7F,
                preserve_tos: raw & 0x80 != 0,
            }
        }
        B_SWITCH => Op::Switch,
        B_CONSTANT.. => Op::Constant((byte - B_CONSTANT) as u16),
        // Undefined byte values: 0o063-0o067, 0o153, 0o163, 0o200,
        // 0o222, 0o251-0o256, 0o264, 0o265, 0o270-0o277.  GNU's exec
        // loop signals "Invalid byte opcode" for all of them.
        _ => return Err(ByteCodeError::UnknownOpcode { offset, byte }),
    };
    Ok(Instr {
        offset,
        len: reader.cursor - offset,
        op,
    })
}

/// Decode and validate a whole opcode stream against its constants
/// vector: every opcode known, no truncated operands, constant indices
/// in range, jump targets at instruction boundaries within the code.
pub fn decode_program(code: &[u8], constants_len: usize) -> Result<Vec<Instr>, ByteCodeError> {
    let mut instrs = Vec::new();
    let mut offset = 0;
    while offset < code.len() {
        let instr = decode_instr(code, offset)?;
        offset += instr.len;
        instrs.push(instr);
    }

    let boundaries: std::collections::HashSet<usize> =
        instrs.iter().map(|instr| instr.offset).collect();
    let check_jump = |offset: usize, target: u16| -> Result<(), ByteCodeError> {
        let target = target as usize;
        if target >= code.len() {
            return Err(ByteCodeError::JumpOutOfRange {
                offset,
                target,
                code_len: code.len(),
            });
        }
        if !boundaries.contains(&target) {
            return Err(ByteCodeError::JumpIntoInstruction { offset, target });
        }
        Ok(())
    };
    let check_constant = |offset: usize, index: u16| -> Result<(), ByteCodeError> {
        let index = index as usize;
        if index >= constants_len {
            return Err(ByteCodeError::ConstantOutOfRange {
                offset,
                index,
                constants_len,
            });
        }
        Ok(())
    };

    for instr in &instrs {
        match instr.op {
            Op::VarRef(index) | Op::VarSet(index) | Op::VarBind(index) => {
                check_constant(instr.offset, index)?;
            }
            Op::Constant(index) | Op::Constant2(index) => check_constant(instr.offset, index)?,
            Op::Goto { target }
            | Op::GotoIfNil { target }
            | Op::GotoIfNonNil { target }
            | Op::GotoIfNilElsePop { target }
            | Op::GotoIfNonNilElsePop { target }
            | Op::PushCatch { target }
            | Op::PushConditionCase { target } => check_jump(instr.offset, target)?,
            _ => {}
        }
    }
    Ok(instrs)
}

/// The argument contract of a byte-code function.
#[derive(Clone, Debug, PartialEq)]
pub enum ArgSpec {
    /// Lexical-binding packed integer: bits 0..6 = minimum arguments,
    /// bit 7 = &rest present, bits 8..14 = maximum non-rest arguments
    /// (exec_byte_code's ARGS_TEMPLATE).
    Packed {
        mandatory: u8,
        nonrest: u8,
        rest: bool,
    },
    /// Old-style dynamic-binding argument list; arguments are
    /// dynamically bound by the VM prologue rather than pushed.
    Legacy(Value),
}

impl ArgSpec {
    fn from_value(value: &Value) -> Result<ArgSpec, ByteCodeError> {
        match value {
            Value::Integer(packed) => {
                let packed = *packed;
                if !(0..=0x7FFF).contains(&packed) {
                    return Err(ByteCodeError::MalformedObject(format!(
                        "argument template {packed} out of range"
                    )));
                }
                Ok(ArgSpec::Packed {
                    mandatory: (packed & 0x7F) as u8,
                    nonrest: ((packed >> 8) & 0x7F) as u8,
                    rest: packed & 0x80 != 0,
                })
            }
            Value::Nil | Value::Cons(_) => Ok(ArgSpec::Legacy(value.clone())),
            other => Err(ByteCodeError::MalformedObject(format!(
                "argument spec must be an integer or list, got {}",
                other.type_name()
            ))),
        }
    }
}

/// A validated genuine GNU byte-code function: the CLOSURE_* slots of
/// GNU's `Lisp_Closure` (lisp.h) with the opcode string decoded.
#[derive(Clone, Debug)]
pub struct ByteCodeObject {
    pub argspec: ArgSpec,
    pub code: Vec<u8>,
    pub constants: Vec<Value>,
    pub stack_depth: usize,
    pub doc: Option<Value>,
    pub interactive: Option<Value>,
}

// The reader stores raw high bytes of unibyte strings (`\200`-style
// escapes) as private-use chars U+E000+byte (reader.rs
// RAW_BYTE_REGEX_BASE); decoding must map them back.
const RAW_BYTE_BASE: u32 = 0xE000;

/// Extract the unibyte bytes of a code string.  Chars are either plain
/// U+0000..U+00FF or the reader's U+E000+byte raw-byte encoding;
/// anything else cannot be an opcode byte.
fn unibyte_bytes(text: &str) -> Result<Vec<u8>, ByteCodeError> {
    let mut bytes = Vec::with_capacity(text.len());
    for (char_index, ch) in text.chars().enumerate() {
        let code_point = u32::from(ch);
        if code_point <= 0xFF {
            bytes.push(code_point as u8);
        } else if (RAW_BYTE_BASE..=RAW_BYTE_BASE + 0xFF).contains(&code_point) {
            bytes.push((code_point - RAW_BYTE_BASE) as u8);
        } else {
            return Err(ByteCodeError::NonUnibyteCode { char_index });
        }
    }
    Ok(bytes)
}

fn string_text(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::StringObject(state) => Some(state.borrow().text.clone()),
        _ => None,
    }
}

/// Whether record slots look like a GENUINE GNU byte-code function
/// (argspec, opcode string, constants vector, stack depth) rather than
/// Emaxx's byte-compile facade (an executable lambda in slot 0).
pub fn slots_are_genuine_bytecode(slots: &[Value]) -> bool {
    slots.len() >= 4
        && matches!(slots[0], Value::Integer(_) | Value::Nil | Value::Cons(_))
        && string_text(&slots[1]).is_some()
        && vector_items(&slots[2]).is_some()
        && matches!(slots[3], Value::Integer(_))
}

fn vector_items(value: &Value) -> Option<Vec<Value>> {
    let items = value.to_vec().ok()?;
    match items.split_first() {
        Some((Value::Symbol(marker), rest)) if marker == "vector-literal" => Some(rest.to_vec()),
        _ => None,
    }
}

impl ByteCodeObject {
    /// Parse and validate a genuine byte-code function from record
    /// slots ([argspec, code, constants, depth, doc?, interactive?]).
    /// Returns `Ok(None)` for Emaxx facade objects, which are executed
    /// through their embedded lambda instead.
    pub fn from_slots(slots: &[Value]) -> Result<Option<ByteCodeObject>, ByteCodeError> {
        if !slots_are_genuine_bytecode(slots) {
            return Ok(None);
        }
        let argspec = ArgSpec::from_value(&slots[0])?;
        let code_text = string_text(&slots[1])
            .ok_or_else(|| ByteCodeError::MalformedObject("code slot is not a string".into()))?;
        let code = unibyte_bytes(&code_text)?;
        let constants = vector_items(&slots[2]).ok_or_else(|| {
            ByteCodeError::MalformedObject("constants slot is not a vector".into())
        })?;
        let Value::Integer(depth) = slots[3] else {
            return Err(ByteCodeError::MalformedObject(
                "stack depth slot is not an integer".into(),
            ));
        };
        if depth < 0 {
            return Err(ByteCodeError::MalformedObject(format!(
                "negative stack depth {depth}"
            )));
        }
        decode_program(&code, constants.len())?;
        Ok(Some(ByteCodeObject {
            argspec,
            code,
            constants,
            stack_depth: depth as usize,
            doc: slots.get(4).cloned(),
            interactive: slots.get(5).cloned(),
        }))
    }
}

/// How an `.elc` payload should be treated by the loader.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ElcKind {
    /// No ELC header: ordinary Lisp source.
    PlainLisp,
    /// Emaxx's own textual compiled stub (`;ELC` + newline, no
    /// version byte): readable macro-expanded Lisp.
    EmaxxStub,
    /// GNU `byte-compile-insert-header` versioned header
    /// (`;ELC<0x1e>`); the forms may still be ordinary readable Lisp
    /// (headered plain files) or contain genuine `#[...]` bytecode —
    /// distinguishing those requires reading the forms.
    GnuVersioned,
}

/// Classify raw `.elc` file bytes by header alone.
pub fn classify_elc_header(bytes: &[u8]) -> ElcKind {
    if bytes.starts_with(b";ELC\x1e") {
        ElcKind::GnuVersioned
    } else if bytes.starts_with(b";ELC") {
        ElcKind::EmaxxStub
    } else {
        ElcKind::PlainLisp
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
pub(crate) mod tests {
    use super::*;

    #[test]
    fn decodes_operand_in_opcode_families() {
        // varref3; constant0; return
        let code = [0o013, 0o300, 0o207];
        let instrs = decode_program(&code, 4).unwrap();
        assert_eq!(
            instrs.iter().map(|instr| instr.op).collect::<Vec<_>>(),
            vec![Op::VarRef(3), Op::Constant(0), Op::Return]
        );
        assert_eq!(instrs[1].offset, 1);
        assert_eq!(instrs[2].offset, 2);
    }

    #[test]
    fn decodes_one_and_two_byte_operands() {
        // varref6 200; varref7 0x0102; return
        let code = [0o016, 200, 0o017, 0x02, 0x01, 0o207];
        let instrs = decode_program(&code, 300).unwrap();
        assert_eq!(instrs[0].op, Op::VarRef(200));
        assert_eq!(instrs[0].len, 2);
        assert_eq!(instrs[1].op, Op::VarRef(0x0102));
        assert_eq!(instrs[1].len, 3);
    }

    #[test]
    fn rejects_undefined_opcodes() {
        for byte in [
            0o000u8, 0o063, 0o067, 0o153, 0o163, 0o200, 0o222, 0o251, 0o256, 0o264, 0o265, 0o270,
            0o277,
        ] {
            let err = decode_program(&[byte], 0).unwrap_err();
            assert_eq!(
                err,
                ByteCodeError::UnknownOpcode { offset: 0, byte },
                "byte {byte:o}"
            );
        }
    }

    #[test]
    fn rejects_truncated_operands() {
        // goto needs two operand bytes; only one present.
        let err = decode_program(&[0o202, 0x00], 0).unwrap_err();
        assert_eq!(
            err,
            ByteCodeError::TruncatedOperand {
                offset: 0,
                byte: 0o202
            }
        );
    }

    #[test]
    fn rejects_constant_out_of_range() {
        // constant2 index 7 with a 3-entry vector.
        let err = decode_program(&[0o201, 7, 0, 0o207], 3).unwrap_err();
        assert_eq!(
            err,
            ByteCodeError::ConstantOutOfRange {
                offset: 0,
                index: 7,
                constants_len: 3
            }
        );
    }

    #[test]
    fn rejects_jump_out_of_range_and_mid_instruction() {
        // goto 99 in a 4-byte program.
        let err = decode_program(&[0o202, 99, 0, 0o207], 0).unwrap_err();
        assert!(matches!(
            err,
            ByteCodeError::JumpOutOfRange { target: 99, .. }
        ));
        // goto 1 targets its own operand byte.
        let err = decode_program(&[0o202, 1, 0, 0o207], 0).unwrap_err();
        assert!(matches!(
            err,
            ByteCodeError::JumpIntoInstruction { target: 1, .. }
        ));
        // goto 3 (the return) is a valid boundary.
        decode_program(&[0o202, 3, 0, 0o207], 0).unwrap();
    }

    #[test]
    fn decodes_discardn_preserve_tos_flag() {
        let instrs = decode_program(&[0o266, 0x83, 0o207], 0).unwrap();
        assert_eq!(
            instrs[0].op,
            Op::DiscardN {
                count: 3,
                preserve_tos: true
            }
        );
        let instrs = decode_program(&[0o266, 0x03, 0o207], 0).unwrap();
        assert_eq!(
            instrs[0].op,
            Op::DiscardN {
                count: 3,
                preserve_tos: false
            }
        );
    }

    #[test]
    fn decodes_handler_targets() {
        // pushcatch -> offset 4 (the return); pophandler; return
        let code = [0o062, 4, 0, 0o060, 0o207];
        let instrs = decode_program(&code, 0).unwrap();
        assert_eq!(instrs[0].op, Op::PushCatch { target: 4 });
        assert_eq!(instrs[1].op, Op::PopHandler);
    }

    #[test]
    fn argspec_unpacks_gnu_template() {
        // (a b &optional c &rest d) = mandatory 2, nonrest 3, rest.
        let spec = ArgSpec::from_value(&Value::Integer(0x0382)).unwrap();
        assert_eq!(
            spec,
            ArgSpec::Packed {
                mandatory: 2,
                nonrest: 3,
                rest: true
            }
        );
    }

    #[test]
    fn classifies_elc_headers() {
        assert_eq!(
            classify_elc_header(b";ELC\x1e\x17\x00\x00\x00"),
            ElcKind::GnuVersioned
        );
        assert_eq!(classify_elc_header(b";ELC\n(progn)"), ElcKind::EmaxxStub);
        assert_eq!(
            classify_elc_header(b";; plain -*- lexical-binding: t -*-"),
            ElcKind::PlainLisp
        );
    }

    /// Genuine `.elc` produced by the pinned GNU Emacs 30.2 oracle from
    /// trivial source (arithmetic, branch, condition-case, while loop,
    /// &rest) — every embedded `#[...]` object must decode cleanly.
    const ORACLE_ELC: &str = include_str!("bytecode/fixture-oracle-30.2.elc");

    fn collect_byte_code_slot_lists(value: &Value, found: &mut Vec<Vec<Value>>) {
        let Ok(items) = value.to_vec() else { return };
        if let [Value::Symbol(marker), kind, slots @ ..] = items.as_slice()
            && marker == super::super::reader::RECORD_LITERAL_SYMBOL
            && kind.to_vec().is_ok_and(|kind_items| {
                matches!(
                    kind_items.as_slice(),
                    [Value::Symbol(quote), Value::Symbol(name)]
                        if quote == "quote" && name == "byte-code-function"
                )
            })
        {
            // Undo the reader's slot-eval wrapping: `(quote X)` -> X.
            let unwrapped = slots
                .iter()
                .map(|slot| match slot.to_vec().ok().as_deref() {
                    Some([Value::Symbol(quote), inner]) if quote == "quote" => inner.clone(),
                    _ => slot.clone(),
                })
                .collect();
            found.push(unwrapped);
            return;
        }
        for item in items {
            collect_byte_code_slot_lists(&item, found);
        }
    }

    /// Second fixture set: buffer ops, save-excursion, unwind-protect,
    /// catch/throw, dynamic binding, and a Bswitch jump table.
    pub(crate) const ORACLE_ELC2: &str = include_str!("bytecode/fixture-oracle2-30.2.elc");

    /// Parse an oracle fixture into named byte-code objects
    /// ((defalias 'NAME #[...]) pairs), shared with the VM tests.
    pub(crate) fn named_objects(elc: &str) -> std::collections::HashMap<String, ByteCodeObject> {
        let source = super::super::preprocess_lazy_doc_source(
            std::path::Path::new("fixture-oracle-30.2.elc"),
            elc,
            false,
        );
        let forms = super::super::reader::Reader::new(&source)
            .read_all()
            .unwrap();
        let mut objects = std::collections::HashMap::new();
        for form in &forms {
            let Ok(items) = form.to_vec() else { continue };
            let [Value::Symbol(head), name_form, object_form] = items.as_slice() else {
                continue;
            };
            if head != "defalias" {
                continue;
            }
            let Ok(name_items) = name_form.to_vec() else {
                continue;
            };
            let [Value::Symbol(quote), Value::Symbol(name)] = name_items.as_slice() else {
                continue;
            };
            if quote != "quote" {
                continue;
            }
            let mut slot_lists = Vec::new();
            collect_byte_code_slot_lists(object_form, &mut slot_lists);
            if let Some(slots) = slot_lists.first()
                && let Ok(Some(object)) = ByteCodeObject::from_slots(slots)
            {
                objects.insert(name.clone(), object);
            }
        }
        objects
    }

    pub(crate) fn fixture_objects() -> std::collections::HashMap<String, ByteCodeObject> {
        named_objects(ORACLE_ELC)
    }

    /// Third fixture: dynamic-binding (legacy list argspec) bytecode.
    pub(crate) const ORACLE_ELC3: &str = include_str!("bytecode/fixture-oracle3-30.2.elc");

    #[test]
    fn loader_executes_genuine_elc_without_sibling_source() {
        // End to end: a genuine GNU `.elc` with NO sibling `.el` loads
        // through load_file_strict, its defaliases resolve to byte-code
        // records, and calls execute on the VM.
        let dir =
            std::env::temp_dir().join(format!("emaxx-bytecode-loader-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let elc = dir.join("fixture-src2.elc");
        std::fs::write(&elc, ORACLE_ELC2).unwrap();
        let mut interp = super::super::eval::Interpreter::new();
        super::super::load_file_strict(&mut interp, &elc).unwrap();
        let mut env = super::super::types::Env::new();
        let value = interp
            .eval(&Value::list([Value::symbol("emaxx-fx2-buffer")]), &mut env)
            .unwrap();
        assert_eq!(format!("{value}"), "(6 \"hello\" 12)");
        let value = interp
            .eval(
                &Value::list([
                    Value::symbol("emaxx-fx2-switch"),
                    Value::list([Value::symbol("quote"), Value::symbol("gamma")]),
                ]),
                &mut env,
            )
            .unwrap();
        assert_eq!(format!("{value}"), "3");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn oracle_elc_fixture_decodes_cleanly() {
        assert_eq!(
            classify_elc_header(ORACLE_ELC.as_bytes()),
            ElcKind::GnuVersioned
        );
        // Same preprocessing the loader applies before reading `.elc`
        // text: `#@N` lazy docs and `#$` current-file references.
        let source = super::super::preprocess_lazy_doc_source(
            std::path::Path::new("fixture-oracle-30.2.elc"),
            ORACLE_ELC,
            false,
        );
        let forms = super::super::reader::Reader::new(&source)
            .read_all()
            .unwrap();
        let mut slot_lists = Vec::new();
        for form in &forms {
            collect_byte_code_slot_lists(form, &mut slot_lists);
        }
        assert_eq!(slot_lists.len(), 5, "five compiled fixture functions");
        for slots in &slot_lists {
            let object = ByteCodeObject::from_slots(slots)
                .expect("oracle bytecode must validate")
                .expect("oracle objects are genuine bytecode");
            assert!(matches!(object.argspec, ArgSpec::Packed { .. }));
            assert!(object.stack_depth > 0);
            let instrs = decode_program(&object.code, object.constants.len()).unwrap();
            assert!(matches!(
                instrs.last().map(|instr| instr.op),
                Some(Op::Return)
            ));
        }
        // The &rest fixture must carry the rest flag.
        let rest_specs: Vec<bool> = slot_lists
            .iter()
            .filter_map(|slots| match ArgSpec::from_value(&slots[0]) {
                Ok(ArgSpec::Packed { rest, .. }) => Some(rest),
                _ => None,
            })
            .collect();
        assert!(rest_specs.contains(&true));
    }

    #[test]
    fn from_slots_ignores_emaxx_facade_objects() {
        // Emaxx facade: slot 0 is an executable lambda, not an argspec.
        let slots = [
            Value::Lambda(
                Vec::new().into(),
                std::rc::Rc::new(Vec::new()),
                std::rc::Rc::new(std::cell::RefCell::new(Vec::new())),
            ),
            Value::Nil,
            Value::Nil,
            Value::Nil,
        ];
        assert!(ByteCodeObject::from_slots(&slots).unwrap().is_none());
    }

    #[test]
    fn from_slots_parses_genuine_object() {
        // #[257 "\211\207" [] 3] — identity: stack-ref1; return... using
        // real GNU compiler output shape: argspec 257 = 1 mandatory, 1 max.
        let slots = [
            Value::Integer(257),
            Value::String("\u{89}\u{87}".into()),
            Value::list([Value::symbol("vector-literal")]),
            Value::Integer(3),
        ];
        let object = ByteCodeObject::from_slots(&slots).unwrap().unwrap();
        assert_eq!(
            object.argspec,
            ArgSpec::Packed {
                mandatory: 1,
                nonrest: 1,
                rest: false
            }
        );
        assert_eq!(object.code, vec![0o211, 0o207]);
        assert_eq!(object.stack_depth, 3);
    }

    #[test]
    fn from_slots_rejects_corrupt_code() {
        // Genuine shape but with an undefined opcode byte.
        let slots = [
            Value::Integer(0),
            Value::String("\u{53}".into()), // 0o123 is Sub1 — fine; use 0o153
            Value::list([Value::symbol("vector-literal")]),
            Value::Integer(2),
        ];
        assert!(ByteCodeObject::from_slots(&slots).unwrap().is_some());
        let slots = [
            Value::Integer(0),
            Value::String("\u{6B}".into()), // 0o153: undefined in GNU 30.2
            Value::list([Value::symbol("vector-literal")]),
            Value::Integer(2),
        ];
        assert_eq!(
            ByteCodeObject::from_slots(&slots).unwrap_err(),
            ByteCodeError::UnknownOpcode {
                offset: 0,
                byte: 0o153
            }
        );
    }
}
