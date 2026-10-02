//! fns.c:concat_to_string copies strings in their actual internal encoding.

use super::{Kind, LispError, StringError, StringObjectRef, StringPropertySpan, Value};
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

fn copy_properties(
    target: &mut Vec<StringPropertySpan>,
    source: &[StringPropertySpan],
    offset: usize,
) {
    target.extend(source.iter().map(|span| StringPropertySpan {
        start: offset + span.start,
        end: offset + span.end,
        // add_text_properties_from_list reverses each copied plist.
        props: span.props.iter().rev().cloned().collect(),
    }));
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
        Self::from_data(
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
                let mut props = Vec::new();
                let mut offset = 0usize;
                for &arg in args {
                    match arg.kind() {
                        Kind::StringObject(state) => {
                            let state = state.borrow();
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
                            copy_properties(&mut props, &state.props, offset);
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
                props.retain(|span| span.start < span.end && !span.props.is_empty());
                props.sort_by_key(|span| (span.start, span.end));
                let mut merged: Vec<StringPropertySpan> = Vec::new();
                for span in props {
                    if let Some(last) = merged.last_mut()
                        && last.end == span.start
                        && crate::buffer::text_property_plists_eq(&last.props, &span.props)
                    {
                        last.end = span.end;
                    } else {
                        merged.push(span);
                    }
                }
                merged
            },
        )
    }
}
