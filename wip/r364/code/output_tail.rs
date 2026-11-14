// print/output.rs: reference code of the first implementation.

    /// One strout of OCTETS, each counted a character.
    pub(crate) fn push_octets(&mut self, octets: &[u8]) {
        let start = self.bytes.len();
        for &byte in octets {
            let code = if byte < 128 {
                u32::from(byte)
            } else {
                0x3fff00 + u32::from(byte)
            };
            let (bytes, count) = encode_character(code).expect("valid C-string byte");
            self.bytes.extend_from_slice(&bytes[..count]);
        }
        self.mark_strout(start, true);
    }

    pub(crate) fn push_c_function_byte(&mut self, byte: u8) {
        let argument = byte as std::ffi::c_char as i32 as u32;
        if argument != u32::from(byte) {
            self.function_codes.push((self.bytes.len(), argument));
        }
        self.push_code(u32::from(byte));
    }

    /// Whether a strout of a C string's octets that are not all ASCII is
    /// among the output.
    pub(crate) fn has_octets(&self) -> bool {
        self.strouts
            .iter()
            .any(|strout| strout.octets && !self.bytes[strout.start..strout.end].is_ascii())
    }

    /// Whether strout was called for this output, if only with nothing.
    pub(crate) fn strout_called(&self) -> bool {
        self.strout_called
    }

    /// print.c:print_unwind onto print_buffer holding this text: SAVED's
    /// first SCHARS bytes copied over its start, its length kept.  (A
    /// copy that ends inside a character leaves bytes that are no
    /// character: each stays as the raw byte it is.)
    pub(crate) fn overwritten_by(mut self, saved: &Self) -> Self {
        let count = saved.restored_len().min(self.bytes.len());
        if count == 0 {
            return self;
        }
        self.bytes[..count].copy_from_slice(&saved.bytes[..count]);
        self.strouts.clear();
        self.function_codes.clear();
        if self.bytes.is_ascii() {
            self.multibyte = false;
            return self;
        }
        self.bytes = canonical_text(&self.bytes);
        self.multibyte = self.multibyte || saved.multibyte;
        self
    }

    /// How many of this text's bytes print_unwind copies back when it is
    /// the text set aside: SCHARS of the string print_prepare made of it.
    pub(crate) fn restored_len(&self) -> usize {
        self.codes().count().min(self.bytes.len())
    }

    /// The text MEMORY, bytes of print_buffer, holds: MULTIBYTE as the
    /// text they were written as.  (Bytes that are no character, where
    /// one print's text ends inside another's character, each stay as the
    /// raw byte they are.)
    pub(crate) fn from_memory(memory: &[u8], multibyte: bool) -> Self {
        if memory.is_ascii() {
            return Self {
                bytes: memory.to_vec(),
                ..Self::default()
            };
        }
        Self {
            bytes: canonical_text(memory),
            multibyte,
            ..Self::default()
        }
    }

/// BYTES as text: what decodes stays, any other byte becomes its raw-byte
/// character.
fn canonical_text(bytes: &[u8]) -> Vec<u8> {
    let mut canonical = Vec::with_capacity(bytes.len());
    let mut rest = bytes;
    while !rest.is_empty() {
        match decode_character(rest) {
            Ok((_, width)) if width <= rest.len() => {
                canonical.extend_from_slice(&rest[..width]);
                rest = &rest[width..];
            }
            _ => {
                let (bytes, count) = encode_character(0x3fff00 + u32::from(rest[0]))
                    .expect("a raw byte is a character");
                canonical.extend_from_slice(&bytes[..count]);
                rest = &rest[1..];
            }
        }
    }
    canonical
}
