//! gnulib md5.c: the MD5 message digest of RFC 1321, as Emacs links it
//! for `md5', `secure-hash' and the native compiler's file names.  Whole
//! 64-byte blocks are processed in place from the input; only a partial
//! block is copied into the context's buffer.

/// md5.h struct md5_ctx: the four chaining words, the 64-bit byte count
/// and the bytes of a block not yet processed (up to 128, as
/// md5_process_bytes concatenates a leftover with new input).
pub(crate) struct Md5Ctx {
    a: u32,
    b: u32,
    c: u32,
    d: u32,
    total: [u32; 2],
    buflen: usize,
    buffer: [u8; 128],
}

/// The bytes that pad a message to the next 64-byte boundary (RFC 1321,
/// 3.1: Step 1).
const FILLBUF: [u8; 64] = {
    let mut fill = [0; 64];
    fill[0] = 0x80;
    fill
};

impl Md5Ctx {
    /// md5_init_ctx (RFC 1321, 3.3: Step 3).
    pub(crate) fn new() -> Self {
        Md5Ctx {
            a: 0x6745_2301,
            b: 0xefcd_ab89,
            c: 0x98ba_dcfe,
            d: 0x1032_5476,
            total: [0, 0],
            buflen: 0,
            buffer: [0; 128],
        }
    }

    /// md5_read_ctx: the digest, little-endian.
    fn read(&self) -> [u8; 16] {
        let mut digest = [0; 16];
        digest[0..4].copy_from_slice(&self.a.to_le_bytes());
        digest[4..8].copy_from_slice(&self.b.to_le_bytes());
        digest[8..12].copy_from_slice(&self.c.to_le_bytes());
        digest[12..16].copy_from_slice(&self.d.to_le_bytes());
        digest
    }

    /// md5_finish_ctx: the leftover bytes, the padding and the bit count
    /// processed, then the digest.
    pub(crate) fn finish(mut self) -> [u8; 16] {
        let bytes = self.buflen;
        let size = if bytes < 56 { 64 } else { 128 };
        // Now count remaining bytes.
        let (low, carry) = self.total[0].overflowing_add(bytes as u32);
        self.total[0] = low;
        self.total[1] = self.total[1].wrapping_add(u32::from(carry));
        // The 64-bit message length in bits at the end of the buffer.
        self.buffer[size - 8..size - 4].copy_from_slice(&(self.total[0] << 3).to_le_bytes());
        self.buffer[size - 4..size]
            .copy_from_slice(&((self.total[1] << 3) | (self.total[0] >> 29)).to_le_bytes());
        self.buffer[bytes..size - 8].copy_from_slice(&FILLBUF[..size - 8 - bytes]);
        let buffer = self.buffer;
        self.process_block(&buffer[..size]);
        self.read()
    }

    /// md5_process_bytes: complete blocks of BUFFER processed where they
    /// lie, a leftover kept for the next call.
    pub(crate) fn process_bytes(&mut self, mut buffer: &[u8]) {
        // When we already have some bits in our internal buffer
        // concatenate both inputs first.
        if self.buflen != 0 {
            let left_over = self.buflen;
            let add = (128 - left_over).min(buffer.len());
            self.buffer[left_over..left_over + add].copy_from_slice(&buffer[..add]);
            self.buflen += add;
            if self.buflen > 64 {
                let whole = self.buflen & !63;
                let blocks = self.buffer;
                self.process_block(&blocks[..whole]);
                self.buflen &= 63;
                let start = (left_over + add) & !63;
                self.buffer.copy_within(start..start + self.buflen, 0);
            }
            buffer = &buffer[add..];
        }
        // Process available complete blocks.
        if buffer.len() >= 64 {
            let whole = buffer.len() & !63;
            self.process_block(&buffer[..whole]);
            buffer = &buffer[whole..];
        }
        // Move remaining bytes in internal buffer.
        if !buffer.is_empty() {
            let mut left_over = self.buflen;
            self.buffer[left_over..left_over + buffer.len()].copy_from_slice(buffer);
            left_over += buffer.len();
            if left_over >= 64 {
                let block = self.buffer;
                self.process_block(&block[..64]);
                left_over -= 64;
                self.buffer.copy_within(64..64 + left_over, 0);
            }
            self.buflen = left_over;
        }
    }

    /// md5_process_block: BUFFER's 64-byte blocks folded into the
    /// chaining words, its length into the byte count.
    fn process_block(&mut self, buffer: &[u8]) {
        debug_assert!(buffer.len().is_multiple_of(64));
        let len = buffer.len();
        let lolen = len as u32;
        // RFC 1321 allows a message of up to 2^64 bits; this counts bytes
        // in a double word.
        let (low, carry) = self.total[0].overflowing_add(lolen);
        self.total[0] = low;
        self.total[1] = self.total[1]
            .wrapping_add(((len as u64) >> 32) as u32)
            .wrapping_add(u32::from(carry));

        let (mut a, mut b, mut c, mut d) = (self.a, self.b, self.c, self.d);
        for block in buffer.as_chunks::<64>().0 {
            let mut x = [0u32; 16];
            for (word, bytes) in x.iter_mut().zip(block.as_chunks::<4>().0) {
                *word = u32::from_le_bytes(*bytes);
            }
            let (a_save, b_save, c_save, d_save) = (a, b, c, d);

            // The four functions of RFC 1321, the first as in Colin
            // Plumb's implementation.
            macro_rules! ff {
                ($b:expr, $c:expr, $d:expr) => {
                    $d ^ ($b & ($c ^ $d))
                };
            }
            macro_rules! fg {
                ($b:expr, $c:expr, $d:expr) => {
                    ff!($d, $b, $c)
                };
            }
            macro_rules! fh {
                ($b:expr, $c:expr, $d:expr) => {
                    $b ^ $c ^ $d
                };
            }
            macro_rules! fi {
                ($b:expr, $c:expr, $d:expr) => {
                    $c ^ ($b | !$d)
                };
            }
            macro_rules! op {
                ($f:ident, $a:ident, $b:ident, $c:ident, $d:ident, $k:expr, $s:expr, $t:expr) => {
                    $a = $a
                        .wrapping_add($f!($b, $c, $d))
                        .wrapping_add(x[$k])
                        .wrapping_add($t)
                        .rotate_left($s)
                        .wrapping_add($b);
                };
            }

            // Round 1.
            op!(ff, a, b, c, d, 0, 7, 0xd76a_a478);
            op!(ff, d, a, b, c, 1, 12, 0xe8c7_b756);
            op!(ff, c, d, a, b, 2, 17, 0x2420_70db);
            op!(ff, b, c, d, a, 3, 22, 0xc1bd_ceee);
            op!(ff, a, b, c, d, 4, 7, 0xf57c_0faf);
            op!(ff, d, a, b, c, 5, 12, 0x4787_c62a);
            op!(ff, c, d, a, b, 6, 17, 0xa830_4613);
            op!(ff, b, c, d, a, 7, 22, 0xfd46_9501);
            op!(ff, a, b, c, d, 8, 7, 0x6980_98d8);
            op!(ff, d, a, b, c, 9, 12, 0x8b44_f7af);
            op!(ff, c, d, a, b, 10, 17, 0xffff_5bb1);
            op!(ff, b, c, d, a, 11, 22, 0x895c_d7be);
            op!(ff, a, b, c, d, 12, 7, 0x6b90_1122);
            op!(ff, d, a, b, c, 13, 12, 0xfd98_7193);
            op!(ff, c, d, a, b, 14, 17, 0xa679_438e);
            op!(ff, b, c, d, a, 15, 22, 0x49b4_0821);

            // Round 2.
            op!(fg, a, b, c, d, 1, 5, 0xf61e_2562);
            op!(fg, d, a, b, c, 6, 9, 0xc040_b340);
            op!(fg, c, d, a, b, 11, 14, 0x265e_5a51);
            op!(fg, b, c, d, a, 0, 20, 0xe9b6_c7aa);
            op!(fg, a, b, c, d, 5, 5, 0xd62f_105d);
            op!(fg, d, a, b, c, 10, 9, 0x0244_1453);
            op!(fg, c, d, a, b, 15, 14, 0xd8a1_e681);
            op!(fg, b, c, d, a, 4, 20, 0xe7d3_fbc8);
            op!(fg, a, b, c, d, 9, 5, 0x21e1_cde6);
            op!(fg, d, a, b, c, 14, 9, 0xc337_07d6);
            op!(fg, c, d, a, b, 3, 14, 0xf4d5_0d87);
            op!(fg, b, c, d, a, 8, 20, 0x455a_14ed);
            op!(fg, a, b, c, d, 13, 5, 0xa9e3_e905);
            op!(fg, d, a, b, c, 2, 9, 0xfcef_a3f8);
            op!(fg, c, d, a, b, 7, 14, 0x676f_02d9);
            op!(fg, b, c, d, a, 12, 20, 0x8d2a_4c8a);

            // Round 3.
            op!(fh, a, b, c, d, 5, 4, 0xfffa_3942);
            op!(fh, d, a, b, c, 8, 11, 0x8771_f681);
            op!(fh, c, d, a, b, 11, 16, 0x6d9d_6122);
            op!(fh, b, c, d, a, 14, 23, 0xfde5_380c);
            op!(fh, a, b, c, d, 1, 4, 0xa4be_ea44);
            op!(fh, d, a, b, c, 4, 11, 0x4bde_cfa9);
            op!(fh, c, d, a, b, 7, 16, 0xf6bb_4b60);
            op!(fh, b, c, d, a, 10, 23, 0xbebf_bc70);
            op!(fh, a, b, c, d, 13, 4, 0x289b_7ec6);
            op!(fh, d, a, b, c, 0, 11, 0xeaa1_27fa);
            op!(fh, c, d, a, b, 3, 16, 0xd4ef_3085);
            op!(fh, b, c, d, a, 6, 23, 0x0488_1d05);
            op!(fh, a, b, c, d, 9, 4, 0xd9d4_d039);
            op!(fh, d, a, b, c, 12, 11, 0xe6db_99e5);
            op!(fh, c, d, a, b, 15, 16, 0x1fa2_7cf8);
            op!(fh, b, c, d, a, 2, 23, 0xc4ac_5665);

            // Round 4.
            op!(fi, a, b, c, d, 0, 6, 0xf429_2244);
            op!(fi, d, a, b, c, 7, 10, 0x432a_ff97);
            op!(fi, c, d, a, b, 14, 15, 0xab94_23a7);
            op!(fi, b, c, d, a, 5, 21, 0xfc93_a039);
            op!(fi, a, b, c, d, 12, 6, 0x655b_59c3);
            op!(fi, d, a, b, c, 3, 10, 0x8f0c_cc92);
            op!(fi, c, d, a, b, 10, 15, 0xffef_f47d);
            op!(fi, b, c, d, a, 1, 21, 0x8584_5dd1);
            op!(fi, a, b, c, d, 8, 6, 0x6fa8_7e4f);
            op!(fi, d, a, b, c, 15, 10, 0xfe2c_e6e0);
            op!(fi, c, d, a, b, 6, 15, 0xa301_4314);
            op!(fi, b, c, d, a, 13, 21, 0x4e08_11a1);
            op!(fi, a, b, c, d, 4, 6, 0xf753_7e82);
            op!(fi, d, a, b, c, 11, 10, 0xbd3a_f235);
            op!(fi, c, d, a, b, 2, 15, 0x2ad7_d2bb);
            op!(fi, b, c, d, a, 9, 21, 0xeb86_d391);

            // Add the starting values of the context.
            a = a.wrapping_add(a_save);
            b = b.wrapping_add(b_save);
            c = c.wrapping_add(c_save);
            d = d.wrapping_add(d_save);
        }
        self.a = a;
        self.b = b;
        self.c = c;
        self.d = d;
    }
}

/// md5_buffer: the digest of BUFFER.
pub(crate) fn md5_buffer(buffer: &[u8]) -> [u8; 16] {
    let mut ctx = Md5Ctx::new();
    ctx.process_bytes(buffer);
    ctx.finish()
}

/// md5-stream.c's BLOCKSIZE: the bytes md5_stream reads at a time.
const BLOCKSIZE: usize = 32768;

/// md5-stream.c:md5_stream: the digest of what STREAM yields, read a
/// block at a time into one buffer, never the whole of it at once.
pub(crate) fn md5_stream(stream: &mut impl std::io::Read) -> std::io::Result<[u8; 16]> {
    let mut ctx = Md5Ctx::new();
    let mut buffer = vec![0; BLOCKSIZE];
    loop {
        match stream.read(&mut buffer) {
            Ok(0) => return Ok(ctx.finish()),
            Ok(count) => ctx.process_bytes(&buffer[..count]),
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
}

/// The digest's 32 lowercase hexadecimal digits, as hexbuf_digest
/// writes them.
pub(crate) fn hex_digest(digest: [u8; 16]) -> String {
    let mut hex = String::with_capacity(32);
    for byte in digest {
        hex.push(char::from(b"0123456789abcdef"[usize::from(byte >> 4)]));
        hex.push(char::from(b"0123456789abcdef"[usize::from(byte & 15)]));
    }
    hex
}

/// `hex_digest' of BUFFER's digest.
pub(crate) fn md5_hex(buffer: &[u8]) -> String {
    hex_digest(md5_buffer(buffer))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn digests_match_rfc_1321_and_split_input() {
        // RFC 1321, A.5's test suite.
        for (input, digest) in [
            ("", "d41d8cd98f00b204e9800998ecf8427e"),
            ("a", "0cc175b9c0f1b6a831c399e269772661"),
            ("abc", "900150983cd24fb0d6963f7d28e17f72"),
            ("message digest", "f96b697d7cb7938d525a2f31aaf161d0"),
            (
                "abcdefghijklmnopqrstuvwxyz",
                "c3fcd3d76192e4007dfb496cca67e13b",
            ),
            (
                "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789",
                "d174ab98d277d9f5a5611c2c9f419d9f",
            ),
            (
                "12345678901234567890123456789012345678901234567890123456789012345678901234567890",
                "57edf4a22be3c955ac49da2e2107b67a",
            ),
        ] {
            assert_eq!(md5_hex(input.as_bytes()), digest, "{input:?}");
        }
        // A message fed in pieces of every size digests as one buffer.
        let message: Vec<u8> = (0..1000u32).map(|i| (i * 7 + 3) as u8).collect();
        let whole = md5_buffer(&message);
        for piece in [1, 3, 55, 56, 63, 64, 65, 127, 128, 129, 300] {
            let mut ctx = Md5Ctx::new();
            for chunk in message.chunks(piece) {
                ctx.process_bytes(chunk);
            }
            assert_eq!(ctx.finish(), whole, "pieces of {piece}");
        }
    }
}
