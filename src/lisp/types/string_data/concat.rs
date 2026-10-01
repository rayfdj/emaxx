//! fns.c:concat_to_string copies strings in their actual internal encoding.

use super::{Kind, LispError, SharedStringState, StringPropertySpan, Value};
use super::{allocate_bytes, character_code, encode_character, string_overflow};

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

fn extend_encoded(bytes: &mut Vec<u8>, code: u32, multibyte: bool) -> Result<(), LispError> {
    if multibyte {
        let (encoded, width) = encode_character(code)?;
        bytes.extend_from_slice(&encoded[..width]);
    } else {
        // An unibyte result contains only ASCII and byte8 sequence values.
        bytes.push(code as u8);
    }
    Ok(())
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

impl SharedStringState {
    pub(crate) fn concatenate(args: &[Value]) -> Result<Self, LispError> {
        let mut characters = 0usize;
        let mut nbytes = 0usize;
        let mut multibyte = false;
        // Validate before allocating the output. A list's full spine is
        // checked before its characters, as Flength does in concat_to_string.
        for &arg in args {
            let (count, bytes, needs_multibyte) = match arg.kind() {
                Kind::StringObject(state) => {
                    let state = state.borrow();
                    (state.characters, state.bytes.len(), state.multibyte)
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
                        if state.multibyte {
                            0
                        } else {
                            state.bytes.iter().filter(|byte| **byte >= 128).count()
                        }
                    }
                    _ => 0,
                };
                nbytes = nbytes.checked_add(extra).ok_or_else(string_overflow)?;
            }
        } else {
            nbytes = characters;
        }

        let mut bytes = allocate_bytes(nbytes)?;
        let mut props = Vec::new();
        let mut offset = 0usize;
        for &arg in args {
            match arg.kind() {
                Kind::StringObject(state) => {
                    let state = state.borrow();
                    if state.multibyte == multibyte {
                        bytes.extend_from_slice(&state.bytes);
                    } else {
                        for &byte in &state.bytes {
                            let code = if byte < 128 {
                                u32::from(byte)
                            } else {
                                0x3fff00 + u32::from(byte)
                            };
                            extend_encoded(&mut bytes, code, true)?;
                        }
                    }
                    copy_properties(&mut props, &state.props, offset);
                    offset += state.characters;
                }
                _ => visit_sequence(arg, |value| {
                    extend_encoded(&mut bytes, character_code(value)?, multibyte)?;
                    offset += 1;
                    Ok(())
                })?,
            }
        }
        debug_assert_eq!(bytes.len(), nbytes);
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
        Ok(Self {
            bytes,
            characters,
            multibyte,
            props: merged,
        })
    }
}
