//! fns.c:concat_to_string copies strings in their actual internal encoding.

use super::{Kind, LispError, StringError, StringObjectRef, StringProperties, Value};
use super::{StringAllocation, character_code, encode_character, string_overflow, validate_size};
use std::ptr;

fn visit_sequence(
    value: Value,
    mut visit: impl FnMut(Value) -> Result<(), LispError>,
) -> Result<(), LispError> {
    match value.kind() {
        Kind::Vector(vector) => {
            for item in vector.slots() {
                visit(item)?;
            }
            Ok(())
        }
        Kind::Nil | Kind::Cons(_) => value.visit_list_elements(visit),
        _ => Err(LispError::WrongTypeArgument("sequencep".into(), value)),
    }
}

struct Output {
    data: *mut u8,
    length: usize,
    filled: usize,
}

impl Output {
    fn extend(&mut self, bytes: &[u8]) {
        assert!(bytes.len() <= self.length - self.filled);
        if !bytes.is_empty() {
            // SAFETY: the constructor owns this fresh allocation, and the
            // checked range is disjoint from the input string/encoded word.
            unsafe {
                ptr::copy_nonoverlapping(bytes.as_ptr(), self.data.add(self.filled), bytes.len())
            };
        }
        self.filled += bytes.len();
    }

    fn character(&mut self, code: u32, multibyte: bool) {
        if multibyte {
            let (encoded, width) = encode_character(code).expect("validated character");
            self.extend(&encoded[..width]);
        } else {
            self.extend(&[code as u8]);
        }
    }
}

/// composite.c:make_composition_value_copy: each `composition' value of
/// the plists of LIST (text_property_list's, so the source string's own
/// plists) replaced by a copy of its first cons.
fn make_composition_value_copy(list: Value) {
    let mut list = list;
    while let Some(cell) = list.cons_ref() {
        let plist = cell
            .car
            .get()
            .cons_ref()
            .and_then(|item| item.cdr.get().cons_ref())
            .and_then(|rest| rest.cdr.get().cons_ref())
            .map_or(Value::Nil, |rest| rest.car.get());
        crate::intervals::copy_composition_value(plist);
        list = cell.cdr.get();
    }
}

impl StringObjectRef {
    pub(crate) fn concatenate(args: &[Value]) -> Result<Self, StringError> {
        let mut characters = 0usize;
        let mut nbytes = 0usize;
        let mut multibyte = false;
        // Validate before allocating the output. A list's full spine is
        // checked before its characters, as Flength does in concat_to_string.
        for &arg in args {
            let (count, bytes, needs_multibyte) = match arg.kind() {
                Kind::StringObject(state) => {
                    let state = state.borrow();
                    (state.len(), state.bytes().len(), state.is_multibyte())
                }
                _ => {
                    if matches!(arg.kind(), Kind::Cons(_)) {
                        arg.visit_list_elements(|_| Ok(()))?;
                    }
                    let mut count = 0usize;
                    let mut bytes = 0usize;
                    let mut flag = false;
                    visit_sequence(arg, |value| {
                        let code = character_code(value)?;
                        count += 1;
                        bytes = bytes
                            .checked_add(encode_character(code)?.1)
                            .ok_or_else(string_overflow)?;
                        flag |= (128..0x3fff80).contains(&code);
                        Ok(())
                    })?;
                    (count, bytes, flag)
                }
            };
            characters = characters.checked_add(count).ok_or_else(string_overflow)?;
            nbytes = nbytes.checked_add(bytes).ok_or_else(string_overflow)?;
            multibyte |= needs_multibyte;
        }
        if multibyte {
            // str_to_multibyte adds one byte for each non-ASCII unibyte octet.
            for &arg in args {
                let extra = match arg.kind() {
                    Kind::StringObject(state) => {
                        let state = state.borrow();
                        if state.is_multibyte() {
                            0
                        } else {
                            state.bytes().iter().filter(|byte| **byte >= 128).count()
                        }
                    }
                    _ => 0,
                };
                nbytes = nbytes.checked_add(extra).ok_or_else(string_overflow)?;
            }
        } else {
            nbytes = characters;
        }

        validate_size(nbytes)?;
        // Each string argument with intervals, and where it goes.
        let mut textprops: Vec<(Value, usize)> = Vec::new();
        let result = Self::from_data(
            nbytes,
            characters,
            multibyte,
            StringAllocation::Ordinary,
            |data| {
                let mut bytes = Output {
                    data,
                    length: nbytes,
                    filled: 0,
                };
                let mut offset = 0usize;
                for &arg in args {
                    match arg.kind() {
                        Kind::StringObject(state) => {
                            let state = state.borrow();
                            if state.props.has_intervals() {
                                textprops.push((arg, offset));
                            }
                            if state.is_multibyte() == multibyte {
                                bytes.extend(state.bytes());
                            } else {
                                for &byte in state.bytes() {
                                    let code = if byte < 128 {
                                        u32::from(byte)
                                    } else {
                                        0x3fff00 + u32::from(byte)
                                    };
                                    bytes.character(code, true);
                                }
                            }
                            offset += state.len();
                        }
                        _ => visit_sequence(arg, |value| {
                            bytes.character(
                                character_code(value).expect("validated character"),
                                multibyte,
                            );
                            offset += 1;
                            Ok(())
                        })
                        .expect("validated sequence"),
                    }
                }
                assert_eq!(bytes.filled, nbytes);
                debug_assert_eq!(offset, characters);
                StringProperties::default()
            },
        )?;
        // fns.c:concat_to_string: each argument's text_property_list added
        // to the result where the argument went, a composition value
        // copied where the previous argument with properties ended there.
        if !textprops.is_empty() {
            let mut last_to_end = None;
            let mut props = StringProperties::default();
            for (arg, to) in textprops {
                let Kind::StringObject(state) = arg.kind() else {
                    unreachable!("a string argument");
                };
                let (list, len) = {
                    let state = state.borrow();
                    let list = match state.props.intervals() {
                        Some(intervals) => crate::intervals::text_property_list(
                            intervals,
                            0,
                            state.len(),
                            Value::Nil,
                        )?,
                        None => Value::Nil,
                    };
                    (list, state.len())
                };
                if last_to_end == Some(to) {
                    make_composition_value_copy(list);
                }
                crate::intervals::add_string_text_properties_from_list(
                    &mut props, characters, list, to,
                )?;
                last_to_end = Some(to + len);
            }
            result.borrow_mut().props = props;
        }
        Ok(result)
    }
}
