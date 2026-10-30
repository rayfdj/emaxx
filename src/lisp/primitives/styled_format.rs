//! editfns.c styled_format, the body of `format' and `format-message',
//! with the C library printf conversions of numbers it hands its
//! integer and floating-point specs to, and character.c
//! lisp_string_width, by which it measures a string argument.

use super::*;
use crate::lisp::types::Kind;
use smallvec::SmallVec;

/// Whether float.h's long double is wider than double where GNU Emacs is
/// built for: on x86 and AArch64 GNU/Linux and on x86 macOS, not on
/// AArch64 macOS, whose long double is double.
const LONG_DOUBLE_WIDER: bool = !cfg!(all(target_os = "macos", target_arch = "aarch64"));

/// styled_format's USEFUL_PRECISION_MAX, 1 - LDBL_MIN_EXP: the largest
/// precision a conversion can give a nonzero digit.
const USEFUL_PRECISION_MAX: isize = if LONG_DOUBLE_WIDER { 16382 } else { 1022 };

/// styled_format's max_bufsize, STRING_BYTES_BOUND + 1.
const MAX_BUFSIZE: isize = 1 << 61;

/// What a printf conversion writes, held inline when short.
type Printed = SmallVec<[u8; 32]>;

/// The byte buffers the printf conversions below write to: the result
/// itself, where styled_format's sprintf writes, or a short text.
trait PrintBuffer: std::ops::DerefMut<Target = [u8]> {
    fn push(&mut self, byte: u8);
    fn extend_from_slice(&mut self, bytes: &[u8]);
    fn truncate(&mut self, len: usize);
}

impl PrintBuffer for Vec<u8> {
    fn push(&mut self, byte: u8) {
        Vec::push(self, byte);
    }
    fn extend_from_slice(&mut self, bytes: &[u8]) {
        Vec::extend_from_slice(self, bytes);
    }
    fn truncate(&mut self, len: usize) {
        Vec::truncate(self, len);
    }
}

impl PrintBuffer for Printed {
    fn push(&mut self, byte: u8) {
        SmallVec::push(self, byte);
    }
    fn extend_from_slice(&mut self, bytes: &[u8]) {
        SmallVec::extend_from_slice(self, bytes);
    }
    fn truncate(&mut self, len: usize) {
        SmallVec::truncate(self, len);
    }
}

/// N zeros or spaces (BYTE) onto OUT.
fn push_repeated(out: &mut impl PrintBuffer, byte: u8, n: usize) {
    for _ in 0..n {
        out.push(byte);
    }
}

/// alloc.c string_overflow.
fn string_overflow() -> LispError {
    LispError::Signal("Maximum string size exceeded".into())
}

/// editfns.c str2num: the decimal number at FORMAT[AT..], PTRDIFF_MAX
/// when it overflows, and the index after its digits.
fn str2num(format: &[u8], mut at: usize) -> (isize, usize) {
    let mut n: isize = 0;
    while let Some(&digit) = format.get(at).filter(|byte| byte.is_ascii_digit()) {
        n = n
            .checked_mul(10)
            .and_then(|n| n.checked_add(isize::from(digit - b'0')))
            .unwrap_or(isize::MAX);
        at += 1;
    }
    (n, at)
}

/// character.h CHAR_HEAD_P.
fn char_head_p(byte: u8) -> bool {
    byte & 0xC0 != 0x80
}

/// character.h BYTE8_STRING: the raw byte BYTE in a multibyte text.
fn push_byte8(buf: &mut Vec<u8>, byte: u8) {
    buf.push(0xC0 | ((byte >> 6) & 1));
    buf.push(0x80 | (byte & 0x3F));
}

/// A unibyte result's bytes as the multibyte text they become, which is
/// what styled_format's retry with MULTIBYTE set would have written.
fn upgrade_to_multibyte(buf: &mut Vec<u8>) {
    if buf.is_ascii() {
        return;
    }
    let mut multibyte = Vec::with_capacity(buf.len() * 2);
    for &byte in buf.iter() {
        if byte < 0x80 {
            multibyte.push(byte);
        } else {
            push_byte8(&mut multibyte, byte);
        }
    }
    *buf = multibyte;
}

/// character.c copy_text: NBYTES of a text of multibyteness
/// FROM_MULTIBYTE into a result of multibyteness TO_MULTIBYTE.
fn copy_text(buf: &mut Vec<u8>, bytes: &[u8], from_multibyte: bool, to_multibyte: bool) {
    if from_multibyte == to_multibyte {
        buf.extend_from_slice(bytes);
    } else if to_multibyte {
        for &byte in bytes {
            if byte < 0x80 {
                buf.push(byte);
            } else {
                push_byte8(buf, byte);
            }
        }
    } else {
        let mut at = 0;
        while at < bytes.len() {
            let (code, width) = crate::lisp::types::string_data::decode_character(&bytes[at..])
                .unwrap_or((u32::from(bytes[at]), 1));
            buf.push(if code < 0x80 {
                code as u8
            } else {
                (code & 0xFF) as u8
            });
            at += width;
        }
    }
}

/// Room for ADDITIONAL more bytes, or alloc.c memory_full's signal, for
/// the large requests a field width or precision makes.
#[inline]
fn reserve(
    interp: &Interpreter,
    env: &Env,
    buf: &mut Vec<u8>,
    additional: usize,
) -> Result<(), LispError> {
    if additional < 1 << 16 {
        return Ok(());
    }
    reserve_large(interp, env, buf, additional)
}

#[inline(never)]
fn reserve_large(
    interp: &Interpreter,
    env: &Env,
    buf: &mut Vec<u8>,
    additional: usize,
) -> Result<(), LispError> {
    buf.try_reserve(additional).map_err(|_| {
        string_storage_error(
            interp,
            env,
            crate::lisp::types::string_data::StringError::AllocationFailed,
        )
    })
}

/// The columns, characters and bytes lisp_string_width reports.
pub(crate) struct StringWidth {
    pub(crate) width: usize,
    pub(crate) chars: usize,
    pub(crate) bytes: usize,
}

/// lisp_string_width's walk over BYTES, read as multibyte text when
/// MULTIBYTE, from character FROM to TO: COMPOSITION answers the end and
/// width of a static composition at a character index, and every other
/// character takes char_width's columns under the display table DP.
#[allow(clippy::too_many_arguments)]
fn string_width_walk(
    interp: &mut Interpreter,
    env: &Env,
    dp: Option<crate::lisp::types::CharTableRef>,
    bytes: &[u8],
    multibyte: bool,
    from: usize,
    to: usize,
    precision: isize,
    mut composition: impl FnMut(&mut Interpreter, usize) -> Result<Option<(usize, usize)>, LispError>,
) -> Result<StringWidth, LispError> {
    let char_bytes = |at: usize| {
        if multibyte {
            crate::lisp::types::string_data::decode_character(&bytes[at..])
                .unwrap_or((u32::from(bytes[at]), 1))
        } else {
            (u32::from(bytes[at]), 1)
        }
    };
    let mut i = 0;
    let mut i_byte = 0;
    while i < from && i_byte < bytes.len() {
        i_byte += char_bytes(i_byte).1;
        i += 1;
    }
    let from_byte = i_byte;
    let mut width = 0usize;
    while i < to && i_byte < bytes.len() {
        let (chars, nbytes, thiswidth) = match composition(interp, i)? {
            Some((end, thiswidth)) => {
                let mut end_byte = i_byte;
                for _ in i..end {
                    if end_byte >= bytes.len() {
                        break;
                    }
                    end_byte += char_bytes(end_byte).1;
                }
                (end - i, end_byte - i_byte, thiswidth)
            }
            None => {
                let (c, nbytes) = char_bytes(i_byte);
                (1, nbytes, display_char_width(interp, c, dp, env))
            }
        };
        if 0 < precision && precision - (width as isize) < thiswidth as isize {
            return Ok(StringWidth {
                width,
                chars: i - from,
                bytes: i_byte - from_byte,
            });
        }
        width = width.checked_add(thiswidth).ok_or_else(string_overflow)?;
        i += chars;
        i_byte += nbytes;
    }
    Ok(StringWidth {
        width,
        chars: i - from,
        bytes: i_byte - from_byte,
    })
}

/// character.c lisp_string_width, as batch runs it (AUTO_COMP finds no
/// window frame): the columns STRING's characters FROM..TO take, a valid
/// static `composition' its registered width and any other character
/// char_width's under the buffer's display table.  A string whose bytes
/// are its characters is read bytewise.  With a positive PRECISION the
/// walk stops before the character that would take it past PRECISION
/// columns, and CHARS and BYTES count what it took.
pub(crate) fn lisp_string_width(
    interp: &mut Interpreter,
    env: &Env,
    string: &Value,
    from: usize,
    to: usize,
    precision: isize,
) -> Result<StringWidth, LispError> {
    let Kind::StringObject(state) = string.kind() else {
        return Err(LispError::WrongTypeArgument("stringp".into(), *string));
    };
    let dp = buffer_display_table(interp, env);
    if !state.borrow().props.has_intervals() {
        let state = state.borrow();
        let multibyte = state.len() < state.bytes().len();
        return string_width_walk(
            interp,
            env,
            dp,
            state.bytes(),
            multibyte,
            from,
            to,
            precision,
            |_, _| Ok(None),
        );
    }
    // Registering a composition can rewrite the string's property, so the
    // walk reads a copy of its bytes.
    let (bytes, multibyte) = {
        let state = state.borrow();
        (state.bytes().to_vec(), state.len() < state.bytes().len())
    };
    let compositions = StringCompositions::of(interp, string)?;
    string_width_walk(
        interp,
        env,
        dp,
        &bytes,
        multibyte,
        from,
        to,
        precision,
        |interp, at| compositions.at(interp, string, at),
    )
}

/// glibc printf's integer conversions as styled_format's convspec asks
/// for them: "%[+][ ].*jd" of a signed value (NEGATIVE with MAGNITUDE),
/// and "%[#].*j[oxX]" of an unsigned one, on which the + and space flags
/// have no effect.  At least PREC digits (one for a negative PREC), and
/// none for zero at precision zero.
#[allow(clippy::too_many_arguments)]
fn sprintf_integer(
    out: &mut impl PrintBuffer,
    conversion: u8,
    negative: bool,
    magnitude: u64,
    prec: isize,
    plus: bool,
    space: bool,
    sharp: bool,
) {
    let mut digits = [0u8; 64];
    let mut at = digits.len();
    let alphabet: &[u8; 16] = if conversion == b'X' {
        b"0123456789ABCDEF"
    } else {
        b"0123456789abcdef"
    };
    let radix: u64 = match conversion {
        b'o' => 8,
        b'x' | b'X' => 16,
        _ => 10,
    };
    let mut n = magnitude;
    // A constant divisor in each arm.
    match radix {
        10 => {
            while n != 0 {
                at -= 1;
                digits[at] = alphabet[(n % 10) as usize];
                n /= 10;
            }
        }
        16 => {
            while n != 0 {
                at -= 1;
                digits[at] = alphabet[(n % 16) as usize];
                n /= 16;
            }
        }
        _ => {
            while n != 0 {
                at -= 1;
                digits[at] = alphabet[(n % 8) as usize];
                n /= 8;
            }
        }
    }
    let digits = &digits[at..];
    let mut zeros = usize::try_from(if prec < 0 { 1 } else { prec })
        .unwrap_or(0)
        .saturating_sub(digits.len());
    if conversion == b'd' {
        if negative {
            out.push(b'-');
        } else if plus {
            out.push(b'+');
        } else if space {
            out.push(b' ');
        }
    } else if sharp {
        if radix == 16 && magnitude != 0 {
            out.extend_from_slice(if conversion == b'X' { b"0X" } else { b"0x" });
        } else if radix == 8 && zeros == 0 && digits.first() != Some(&b'0') {
            // The precision grows until the first digit is a zero.
            zeros = 1;
        }
    }
    push_repeated(out, b'0', zeros);
    out.extend_from_slice(digits);
}

/// The sign glibc printf writes before a floating-point conversion: a
/// minus for a negative sign bit (of a zero or a NaN too), else the +
/// or space flag's.
fn push_float_sign(out: &mut impl PrintBuffer, negative: bool, plus: bool, space: bool) {
    if negative {
        out.push(b'-');
    } else if plus {
        out.push(b'+');
    } else if space {
        out.push(b' ');
    }
}

/// %e's exponent as glibc writes it: a sign and at least two digits.
fn push_exponent(out: &mut impl PrintBuffer, exponent: i32) {
    out.push(b'e');
    out.push(if exponent < 0 { b'-' } else { b'+' });
    let magnitude = exponent.unsigned_abs();
    if magnitude < 10 {
        out.push(b'0');
    }
    out.extend_from_slice(magnitude.to_string().as_bytes());
}

/// glibc printf's %g after its %e or %f: unless SHARP, the fraction's
/// trailing zeros and then a bare decimal point go.  START is where the
/// number begins (after any sign).
fn strip_g_zeros(out: &mut impl PrintBuffer, start: usize, sharp: bool) {
    if sharp {
        return;
    }
    let exponent_at = out[start..]
        .iter()
        .position(|&byte| byte == b'e')
        .map_or(out.len(), |at| start + at);
    if !out[start..exponent_at].contains(&b'.') {
        return;
    }
    let mut end = exponent_at;
    while out[end - 1] == b'0' {
        end -= 1;
    }
    if out[end - 1] == b'.' {
        end -= 1;
    }
    let exponent: SmallVec<[u8; 8]> = SmallVec::from_slice(&out[exponent_at..]);
    out.truncate(end);
    out.extend_from_slice(&exponent);
}

/// glibc printf's %e, %f and %g of the double X, as styled_format's
/// convspec "%[+][ ][#].*[efg]" asks for them; a negative PREC is the
/// default six.  Rust's exact formatting rounds as glibc does, half to
/// even on the exact binary value.
#[allow(clippy::too_many_arguments)]
fn sprintf_double(
    out: &mut impl PrintBuffer,
    conversion: u8,
    x: f64,
    prec: isize,
    plus: bool,
    space: bool,
    sharp: bool,
) {
    push_float_sign(out, x.is_sign_negative(), plus, space);
    if x.is_nan() {
        out.extend_from_slice(b"nan");
        return;
    }
    if x.is_infinite() {
        out.extend_from_slice(b"inf");
        return;
    }
    let x = x.abs();
    let prec = if prec < 0 { 6 } else { prec as usize };
    let start = out.len();
    match conversion {
        b'f' => push_fixed(out, x, prec, sharp),
        b'e' => push_scientific(out, x, prec, sharp),
        _ => {
            let p = prec.max(1);
            let exponent = if x == 0.0 {
                0
            } else {
                scientific_parts(x, p - 1).1
            };
            if (p as i64) > i64::from(exponent) && exponent >= -4 {
                push_fixed(out, x, (p as i64 - 1 - i64::from(exponent)) as usize, sharp);
            } else {
                push_scientific(out, x, p - 1, sharp);
            }
            strip_g_zeros(out, start, sharp);
        }
    }
}

/// %f's digits of the nonnegative X.
fn push_fixed(out: &mut impl PrintBuffer, x: f64, prec: usize, sharp: bool) {
    use std::io::Write as _;
    let mut text = Vec::new();
    let _ = write!(text, "{x:.prec$}");
    out.extend_from_slice(&text);
    if prec == 0 && sharp {
        out.push(b'.');
    }
}

/// The mantissa digits and decimal exponent of the nonnegative X rounded
/// to PREC + 1 significant digits.
fn scientific_parts(x: f64, prec: usize) -> (String, i32) {
    let text = format!("{x:.prec$e}");
    let (mantissa, exponent) = text.split_once('e').expect("Rust's exponent form");
    (
        mantissa.to_owned(),
        exponent.parse().expect("Rust's decimal exponent"),
    )
}

/// %e's text of the nonnegative X.
fn push_scientific(out: &mut impl PrintBuffer, x: f64, prec: usize, sharp: bool) {
    let (mantissa, exponent) = scientific_parts(x, prec);
    out.extend_from_slice(mantissa.as_bytes());
    if prec == 0 && sharp {
        out.push(b'.');
    }
    push_exponent(out, exponent);
}

/// DIGITS (the decimal digits of a positive integer) rounded to KEEP
/// significant digits, half to even, and whether the rounding carried
/// into a new leading digit.
fn round_decimal(digits: &[u8], keep: usize) -> (Vec<u8>, bool) {
    if keep >= digits.len() {
        let mut kept = digits.to_vec();
        kept.resize(keep, b'0');
        return (kept, false);
    }
    let mut kept = digits[..keep].to_vec();
    let first = digits[keep];
    let rest_nonzero = digits[keep + 1..].iter().any(|&digit| digit != b'0');
    let odd = kept.last().is_some_and(|&digit| (digit - b'0') % 2 == 1);
    let round_up = first > b'5' || (first == b'5' && (rest_nonzero || odd));
    if !round_up {
        return (kept, false);
    }
    for digit in kept.iter_mut().rev() {
        if *digit == b'9' {
            *digit = b'0';
        } else {
            *digit += 1;
            return (kept, false);
        }
    }
    kept.insert(0, b'1');
    kept.pop();
    (kept, true)
}

/// glibc printf's %Le, %Lf and %Lg of an integer a long double holds
/// exactly (NEGATIVE with MAGNITUDE): styled_format formats an integer as
/// a long double when the double nearest it differs from it.
#[allow(clippy::too_many_arguments)]
fn sprintf_exact_integer<B: PrintBuffer>(
    out: &mut B,
    conversion: u8,
    negative: bool,
    magnitude: u64,
    prec: isize,
    plus: bool,
    space: bool,
    sharp: bool,
) {
    push_float_sign(out, negative, plus, space);
    let digits = magnitude.to_string().into_bytes();
    let prec = if prec < 0 { 6 } else { prec as usize };
    let start = out.len();
    let fixed = |out: &mut B, prec: usize| {
        out.extend_from_slice(&digits);
        if prec > 0 || sharp {
            out.push(b'.');
        }
        push_repeated(out, b'0', prec);
    };
    let scientific = |out: &mut B, prec: usize| {
        let (mantissa, carried) = round_decimal(&digits, prec + 1);
        let exponent = digits.len() as i32 - 1 + i32::from(carried);
        out.push(mantissa[0]);
        if prec > 0 || sharp {
            out.push(b'.');
        }
        out.extend_from_slice(&mantissa[1..]);
        push_exponent(out, exponent);
    };
    match conversion {
        b'f' => fixed(out, prec),
        b'e' => scientific(out, prec),
        _ => {
            let p = prec.max(1);
            let carried = round_decimal(&digits, p).1;
            let exponent = digits.len() - 1 + usize::from(carried);
            if p > exponent {
                fixed(out, p - 1 - exponent);
            } else {
                scientific(out, p - 1);
            }
            strip_g_zeros(out, start, sharp);
        }
    }
}

/// An integer as intmax_t or uintmax_t holds it, when one does
/// (integer_to_intmax, integer_to_uintmax): its sign and magnitude.
fn integer_to_machine(value: &Value) -> Option<(bool, u64)> {
    match value.kind() {
        Kind::Integer(n) => Some((n < 0, n.unsigned_abs())),
        Kind::BigInteger(n) => {
            if let Some(n) = n.to_i64() {
                Some((n < 0, n.unsigned_abs()))
            } else {
                n.to_u64().map(|n| (false, n))
            }
        }
        _ => None,
    }
}

/// bignum.c double_to_integer: the truncated D, which must be finite.
fn double_to_integer(d: f64) -> Result<BigInt, LispError> {
    if !d.is_finite() {
        return Err(LispError::SignalValue(Value::list([Value::symbol(
            "overflow-error",
        )])));
    }
    bigint_from_truncated_float(d)
}

/// The SYMBOL_NAME of ARG when C's SYMBOLP admits it.
fn c_symbol_name(interp: &Interpreter, arg: &Value, env: &Env) -> Option<Value> {
    if !matches!(
        arg.kind(),
        Kind::Nil | Kind::T | Kind::Symbol(_) | Kind::SymbolWithPos(_)
    ) {
        return None;
    }
    checked_symbol_identity(interp, arg, env)
        .ok()
        .map(|symbol| symbol.lisp_name())
}

/// The text a %s substitutes: a Lisp string (the argument, a symbol's
/// name, `prin1-to-string''s output or `char-to-string''s), or the
/// printed digits of a number, which are the unibyte ASCII string
/// `prin1-to-string' would make.
enum SpecText {
    String(Value),
    Number(Printed),
}

/// A range of a string's text properties, as text_property_list gives
/// it: its start, end and the interval's own plist.
type PropertyRange = (usize, usize, Value);

/// What styled_format records of a conversion: where its output went in
/// the result, the argument it read, and where it began in the format.
struct FormatSpec {
    argument: Value,
    start: usize,
    end: usize,
    fbeg: usize,
    intervals: bool,
}

/// styled_format's "Invalid format operation" error for the conversion
/// whose first byte is FORMAT[AT].
fn invalid_format_operation(format: &[u8], at: usize, multibyte_format: bool) -> LispError {
    let byte = format[at];
    let message = if multibyte_format {
        let code = crate::lisp::types::string_data::decode_character(&format[at..])
            .map_or(u32::from(byte), |(code, _)| code);
        let mut text = String::from("Invalid format operation %");
        push_character_code(&mut text, &mut Vec::new(), 0, code);
        text
    } else if byte <= 127 {
        format!("Invalid format operation %{}", char::from(byte))
    } else {
        format!("Invalid format operation char #o{byte:03o}")
    };
    LispError::Signal(message)
}

/// editfns.c styled_format: ARGS[0], the format, with each of its
/// conversion specifications replaced by its argument's text; MESSAGE
/// is `format-message', which also requotes the format's grave accents
/// and apostrophes in `text-quoting-style'.
pub(crate) fn styled_format(
    interp: &mut Interpreter,
    env: &mut Env,
    args: &[Value],
    message: bool,
) -> Result<Value, LispError> {
    let Kind::StringObject(format_string) = args[0].kind() else {
        return Err(LispError::WrongTypeArgument("stringp".into(), args[0]));
    };
    // The format is read from a copy, since a conversion can run Lisp.
    let (format, multibyte_format, format_chars, fmt_props) = {
        let state = format_string.borrow();
        (
            SmallVec::<[u8; 64]>::from_slice(state.bytes()),
            state.is_multibyte(),
            state.len(),
            state.props.has_intervals(),
        )
    };
    let nargs = args.len() as isize;

    // The result is multibyte if any of the inputs is (a converted
    // argument or a requoted character can make it so later).
    let mut multibyte = multibyte_format
        || args[1..]
            .iter()
            .any(|arg| string_is_multibyte(arg) == Some(true));
    let quoting_style = if message {
        effective_text_quoting_style(interp, env)
    } else {
        ""
    };

    let mut buf: Vec<u8> = Vec::with_capacity(format.len() + 16);
    let mut nchars = 0usize;
    let mut specs: SmallVec<[FormatSpec; 4]> = SmallVec::new();
    let mut arg_intervals = false;
    // Whether each character of the format was not copied to the result,
    // kept for the format's text properties.
    let mut discarded: Vec<bool> = if fmt_props {
        vec![false; format_chars]
    } else {
        Vec::new()
    };
    // Whether the result is not the format itself.
    let mut new_result = false;
    // The number of the last argument substituted.
    let mut n: isize = 0;
    let mut at = 0usize;
    // The index of the character at AT.
    let mut fpos = 0usize;

    while at < format.len() {
        let format0 = at;
        let fpos0 = fpos;
        let format_char = format[at];
        at += 1;

        if format_char != b'%' {
            fpos += 1;
            if (format_char == b'`' || format_char == b'\'') && quoting_style == "curve" {
                if !multibyte {
                    upgrade_to_multibyte(&mut buf);
                    multibyte = true;
                }
                buf.extend_from_slice(if format_char == b'`' {
                    "\u{2018}".as_bytes()
                } else {
                    "\u{2019}".as_bytes()
                });
                new_result = true;
            } else if format_char == b'`' && quoting_style == "straight" {
                buf.push(b'\'');
                new_result = true;
            } else if multibyte_format {
                // A whole multibyte character.
                while at < format.len() && !char_head_p(format[at]) {
                    at += 1;
                }
                buf.extend_from_slice(&format[format0..at]);
            } else if multibyte && format_char >= 0x80 {
                push_byte8(&mut buf, format_char);
                new_result = true;
            } else {
                buf.push(format_char);
            }
            nchars += 1;
            continue;
        }

        // '%' [field-number] [flags] [field-width] [precision] format
        if format.get(at).is_some_and(u8::is_ascii_digit) {
            let (num, num_end) = str2num(&format, at);
            if format.get(num_end) == Some(&b'$') {
                n = num - 1;
                at = num_end + 1;
            }
        }
        let mut minus_flag = false;
        let mut plus_flag = false;
        let mut space_flag = false;
        let mut sharp_flag = false;
        let mut zero_flag = false;
        while let Some(&flag) = format.get(at) {
            match flag {
                b'-' => minus_flag = true,
                b'+' => plus_flag = true,
                b' ' => space_flag = true,
                b'#' => sharp_flag = true,
                b'0' => zero_flag = true,
                _ => break,
            }
            at += 1;
        }
        // Ignore flags when sprintf ignores them.
        space_flag &= !plus_flag;
        zero_flag &= !minus_flag;

        let (field_width, mut num_end) = str2num(&format, at);
        if MAX_BUFSIZE <= field_width {
            return Err(string_overflow());
        }
        let precision_given = format.get(num_end) == Some(&b'.');
        let precision = if precision_given {
            let (precision, end) = str2num(&format, num_end + 1);
            num_end = end;
            precision
        } else {
            isize::MAX
        };
        at = num_end;
        if at == format.len() {
            return Err(LispError::Signal(
                "Format string ends in middle of format specifier".into(),
            ));
        }
        let mut conversion = format[at];
        at += 1;
        // The specification's characters, all ASCII but an invalid
        // conversion's, which signals below.
        let spec_chars = at - format0;
        if fmt_props {
            let marked = spec_chars - usize::from(conversion == b'%');
            discarded[fpos0..(fpos0 + marked).min(format_chars)].fill(true);
        }
        fpos += spec_chars;
        if conversion == b'%' {
            new_result = true;
            buf.push(b'%');
            nchars += 1;
            continue;
        }

        n += 1;
        if n >= nargs {
            return Err(LispError::Signal(
                "Not enough arguments for format string".into(),
            ));
        }
        let mut arg = args[n as usize];
        let mut text = None;

        // For 'S', prin1 the argument, and then treat like 's'.  For 's',
        // princ any argument that is not a string or symbol.
        if conversion == b'S'
            || (conversion == b's'
                && !matches!(arg.kind(), Kind::StringObject(_))
                && c_symbol_name(interp, &arg, env).is_none())
        {
            text = Some(spec_print(interp, env, &arg, conversion == b's')?);
            conversion = b's';
        } else if conversion == b'c' {
            if let Kind::Integer(code) = arg.kind()
                && !(0..0x80).contains(&code)
            {
                arg = Value::StringObject(
                    StringObjectRef::from_characters(&[arg])
                        .map_err(|error| string_storage_error(interp, env, error))?,
                );
                text = Some(SpecText::String(arg));
                conversion = b's';
            }
            zero_flag = false;
        }
        if text.is_none()
            && let Some(name) = c_symbol_name(interp, &arg, env)
        {
            arg = name;
            if string_is_multibyte(&name) == Some(true) && !multibyte {
                // GNU formats again from the start, multibyte, and reads the
                // name it kept for this spec: no longer the argument, it
                // turns a %c into a %s.
                upgrade_to_multibyte(&mut buf);
                multibyte = true;
                if conversion == b'c' {
                    conversion = b's';
                }
            }
            if conversion == b's' {
                text = Some(SpecText::String(name));
            }
        }
        if let Some(SpecText::String(string)) = &text
            && string_is_multibyte(string) == Some(true)
            && !multibyte
        {
            upgrade_to_multibyte(&mut buf);
            multibyte = true;
        }

        if conversion == b's' {
            let text = match text {
                Some(text) => text,
                None => SpecText::String(arg),
            };
            if at == format.len() && at == 2 && !fmt_props {
                return match text {
                    SpecText::String(string) => Ok(string),
                    SpecText::Number(digits) => Ok(Value::StringObject(
                        StringObjectRef::from_storage(digits.to_vec(), digits.len(), false)
                            .map_err(|error| string_storage_error(interp, env, error))?,
                    )),
                };
            }
            let (argument, intervals) = match &text {
                SpecText::String(string) => (
                    *string,
                    matches!(string.kind(), Kind::StringObject(state) if state.borrow().props.has_intervals()),
                ),
                SpecText::Number(_) => (Value::Nil, false),
            };

            // lisp_string_width ignores a precision of 0, but GNU libc
            // functions print 0 characters when the precision is 0.
            let prec: isize = if precision_given { precision } else { -1 };
            let (width, nchars_string, nbytes) = if prec == 0 {
                (0, 0, 0)
            } else {
                let (whole_chars, whole_bytes) = match &text {
                    SpecText::String(string) => {
                        let Kind::StringObject(state) = string.kind() else {
                            unreachable!("a %s text is a string")
                        };
                        let state = state.borrow();
                        (state.len(), state.bytes().len())
                    }
                    SpecText::Number(digits) => (digits.len(), digits.len()),
                };
                // The width only pads to a field width or stops at a
                // precision; without either nothing reads it.
                if prec < 0 && field_width == 0 {
                    (0, whole_chars, whole_bytes)
                } else {
                    let measured = match &text {
                        SpecText::String(string) => {
                            lisp_string_width(interp, env, string, 0, whole_chars, prec)?
                        }
                        SpecText::Number(digits) => {
                            let dp = buffer_display_table(interp, env);
                            string_width_walk(
                                interp,
                                env,
                                dp,
                                digits,
                                false,
                                0,
                                digits.len(),
                                prec,
                                |_, _| Ok(None),
                            )?
                        }
                    };
                    if prec < 0 {
                        (measured.width, whole_chars, whole_bytes)
                    } else {
                        (measured.width, measured.chars, measured.bytes)
                    }
                }
            };

            let padding = (field_width as usize).saturating_sub(width);
            if MAX_BUFSIZE - padding as isize <= nbytes as isize {
                return Err(string_overflow());
            }
            reserve(interp, env, &mut buf, padding + nbytes * 2)?;
            // With properties in the format, the spec's span takes in the
            // padding on its left.
            let mut start = nchars;
            if !minus_flag {
                push_repeated(&mut buf, b' ', padding);
                nchars += padding;
            }
            if !fmt_props {
                start = nchars;
            }
            match &text {
                SpecText::String(string) => {
                    let Kind::StringObject(state) = string.kind() else {
                        unreachable!("a %s text is a string")
                    };
                    let state = state.borrow();
                    copy_text(
                        &mut buf,
                        &state.bytes()[..nbytes],
                        state.is_multibyte(),
                        multibyte,
                    );
                }
                SpecText::Number(digits) => buf.extend_from_slice(&digits[..nbytes]),
            }
            nchars += nchars_string;
            if minus_flag {
                push_repeated(&mut buf, b' ', padding);
                nchars += padding;
            }
            arg_intervals |= intervals;
            specs.push(FormatSpec {
                argument,
                start,
                end: nchars,
                fbeg: fpos0,
                intervals,
            });
            new_result = true;
            continue;
        }

        if !matches!(
            conversion,
            b'c' | b'd' | b'e' | b'f' | b'g' | b'i' | b'o' | b'x' | b'X'
        ) {
            return Err(invalid_format_operation(&format, at - 1, multibyte_format));
        }
        if !(matches!(arg.kind(), Kind::Integer(_))
            || (matches!(arg.kind(), Kind::BigInteger(_) | Kind::Float(_)) && conversion != b'c'))
        {
            return Err(LispError::Signal(
                "Format specifier doesn't match argument type".into(),
            ));
        }

        if conversion == b'd' || conversion == b'i' {
            sharp_flag = false;
        }
        let float_conversion = matches!(conversion, b'e' | b'f' | b'g');
        if !(float_conversion || conversion == b'c') {
            zero_flag &= !precision_given;
        }
        let mut prec: isize = if precision_given {
            precision.min(USEFUL_PRECISION_MAX)
        } else {
            -1
        };
        // Characters to be inserted after spaces and before leading zeros:
        // a bignum's sign flag and base prefix.
        let mut prefix = [0u8; 3];
        let mut prefixlen = 0usize;
        // What sprintf writes, at the result's end as styled_format's
        // sprintf writes into its buffer: SPRINTF_BYTES bytes from P_START.
        // A byte read past them is the NUL sprintf ends with, but for %c at
        // precision zero, where it is the character.
        let p_start = buf.len();
        let p = &mut buf;
        let mut sprintf_bytes: usize;
        let mut bignum: Option<BigInt> = None;

        if float_conversion {
            let (darg, exact) = match arg.kind() {
                Kind::Float(f) => (f.get(), None),
                _ if LONG_DOUBLE_WIDER => match integer_to_machine(&arg) {
                    Some((negative, magnitude)) => {
                        let value = if negative {
                            -i128::from(magnitude)
                        } else {
                            i128::from(magnitude)
                        };
                        let darg = value as f64;
                        // A long double holds the integer the double does not.
                        (
                            darg,
                            (darg as i128 != value).then_some((negative, magnitude)),
                        )
                    }
                    None => (numeric_to_f64(interp, &arg)?, None),
                },
                Kind::Integer(value) => (value as f64, None),
                _ => (numeric_to_f64(interp, &arg)?, None),
            };
            match exact {
                Some((negative, magnitude)) => sprintf_exact_integer(
                    p, conversion, negative, magnitude, prec, plus_flag, space_flag, sharp_flag,
                ),
                None => {
                    sprintf_double(p, conversion, darg, prec, plus_flag, space_flag, sharp_flag)
                }
            }
            sprintf_bytes = p.len() - p_start;
        } else if conversion == b'c' {
            // Don't use sprintf here, as it might mishandle prec.
            let Kind::Integer(code) = arg.kind() else {
                unreachable!("%c of a fixnum, checked above")
            };
            p.push(code as u8);
            sprintf_bytes = usize::from(prec != 0);
        } else if let Kind::BigInteger(n) = arg.kind() {
            let n: BigInt = n.into();
            bignum = Some(n);
            sprintf_bytes = 0;
        } else if conversion == b'd' || conversion == b'i' {
            match arg.kind() {
                Kind::Integer(x) => {
                    sprintf_integer(
                        p,
                        b'd',
                        x < 0,
                        x.unsigned_abs(),
                        prec,
                        plus_flag,
                        space_flag,
                        false,
                    );
                    sprintf_bytes = p.len() - p_start;
                }
                _ => {
                    let Kind::Float(f) = arg.kind() else {
                        unreachable!("%d of a fixnum, bignum or float")
                    };
                    // Truncate and then convert -0 to 0, to be more
                    // consistent with %x etc.; see Bug#31938.
                    let mut x = f.get().trunc();
                    if x == 0.0 {
                        x = 0.0;
                    }
                    sprintf_double(p, b'f', x, 0, plus_flag, space_flag, false);
                    sprintf_bytes = p.len() - p_start;
                    let signedp = !p[p_start].is_ascii_digit();
                    prec = precision.min((sprintf_bytes - usize::from(signedp)) as isize);
                }
            }
        } else {
            let unsigned = match arg.kind() {
                Kind::Integer(i) => {
                    let binary_as_unsigned = interp
                        .forwarded_c_value("binary-as-unsigned", env)
                        .is_some_and(|value| value.is_truthy());
                    if binary_as_unsigned {
                        // XUFIXNUM: the fixnum's bits as an unsigned value.
                        Some((false, (i as u64) & ((1u64 << 62) - 1)))
                    } else {
                        Some((i < 0, i.unsigned_abs()))
                    }
                }
                _ => {
                    let Kind::Float(f) = arg.kind() else {
                        unreachable!("%x of a fixnum, bignum or float")
                    };
                    let d = f.get();
                    let abs_d = d.abs();
                    if abs_d < u64::MAX as f64 + 1.0 {
                        Some((d <= -1.0, abs_d as u64))
                    } else {
                        bignum = Some(double_to_integer(d)?);
                        None
                    }
                }
            };
            sprintf_bytes = 0;
            if let Some((negative, x)) = unsigned {
                let signedp = negative || plus_flag || space_flag;
                if signedp {
                    p.push(if negative {
                        b'-'
                    } else if plus_flag {
                        b'+'
                    } else {
                        b' '
                    });
                }
                sprintf_integer(p, conversion, false, x, prec, false, false, sharp_flag);
                sprintf_bytes = p.len() - p_start;
            }
        }
        if let Some(n) = bignum {
            let base = match conversion {
                b'd' | b'i' => 10,
                b'o' => 8,
                _ => 16,
            };
            let mut digits = n.to_str_radix(base);
            if conversion == b'X' {
                digits.make_ascii_uppercase();
            }
            p.truncate(p_start);
            p.extend_from_slice(digits.as_bytes());
            sprintf_bytes = digits.len();
            let negative = digits.starts_with('-');
            prec = precision.min((sprintf_bytes - prefixlen) as isize);
            prefix[prefixlen] = if plus_flag { b'+' } else { b' ' };
            prefixlen += usize::from((plus_flag || space_flag) && !negative);
            prefix[prefixlen] = b'0';
            prefix[prefixlen + 1] = conversion;
            prefixlen += if sharp_flag && base == 16 { 2 } else { 0 };
        }

        // Now the length of the formatted item is known, except it omits
        // padding and excess precision.  Deal with excess precision first.
        let printed = &buf[p_start..];
        let p_at = |i: usize| printed.get(i).copied().unwrap_or(0);
        let mut excess_precision: isize = if precision_given { precision - prec } else { 0 };
        let mut trailing_zeros: isize = 0;
        if excess_precision != 0 && float_conversion {
            if !p_at(sprintf_bytes.wrapping_sub(1)).is_ascii_digit()
                || (conversion == b'g' && !(sharp_flag && printed[..sprintf_bytes].contains(&b'.')))
            {
                excess_precision = 0;
            }
            trailing_zeros = excess_precision;
        }
        let mut leading_zeros = excess_precision - trailing_zeros;

        // The total bytes needed for this item, including excess precision
        // and padding.
        let numwidth = ((prefixlen + sprintf_bytes) as isize)
            .checked_add(excess_precision)
            .unwrap_or(isize::MAX);
        let mut padding = if numwidth < field_width {
            field_width - numwidth
        } else {
            0
        };
        if MAX_BUFSIZE - ((prefixlen + sprintf_bytes) as isize) <= excess_precision
            || MAX_BUFSIZE - padding <= numwidth
        {
            return Err(string_overflow());
        }

        let signedp = matches!(p_at(0), b'-' | b'+' | b' ');
        let mut beglen = usize::from(signedp)
            + if p_at(usize::from(signedp)) == b'0'
                && matches!(p_at(usize::from(signedp) + 1), b'x' | b'X')
            {
                2
            } else {
                0
            };
        // A sign GNU reads from %c's unwritten byte at precision zero is
        // not output (GNU then moves a negative length).
        beglen = beglen.min(sprintf_bytes);
        if zero_flag && p_at(beglen).is_ascii_hexdigit() {
            leading_zeros += padding;
            padding = 0;
        }
        if leading_zeros == 0 && sharp_flag && conversion == b'o' && p_at(beglen) != b'0' {
            leading_zeros += 1;
            padding -= isize::from(padding != 0);
        }
        let mut endlen = 0;
        if trailing_zeros != 0
            && (conversion == b'e' || conversion == b'g')
            && let Some(e) = printed[..sprintf_bytes]
                .iter()
                .position(|&byte| byte == b'e')
        {
            endlen = sprintf_bytes - e;
        }
        let midlen = sprintf_bytes - beglen - endlen;
        let leading_padding = if minus_flag { 0 } else { padding };
        let trailing_padding = padding - leading_padding;
        let incr = padding as usize
            + leading_zeros as usize
            + prefixlen
            + sprintf_bytes
            + trailing_zeros as usize;
        if incr == sprintf_bytes {
            // Nothing to insert: the output is what sprintf wrote.
            buf.truncate(p_start + sprintf_bytes);
        } else {
            // Insert padding and excess-precision zeros: LEADING_PADDING
            // spaces, BEGLEN bytes of the output, the prefix, LEADING_ZEROS
            // zeros, MIDLEN bytes of the output, TRAILING_ZEROS zeros,
            // ENDLEN bytes of the output, TRAILING_PADDING spaces.
            let printed: Printed = SmallVec::from_slice(&buf[p_start..p_start + sprintf_bytes]);
            buf.truncate(p_start);
            reserve(interp, env, &mut buf, incr)?;
            push_repeated(&mut buf, b' ', leading_padding as usize);
            buf.extend_from_slice(&printed[..beglen]);
            buf.extend_from_slice(&prefix[..prefixlen]);
            push_repeated(&mut buf, b'0', leading_zeros as usize);
            buf.extend_from_slice(&printed[beglen..beglen + midlen]);
            push_repeated(&mut buf, b'0', trailing_zeros as usize);
            buf.extend_from_slice(&printed[sprintf_bytes - endlen..]);
            push_repeated(&mut buf, b' ', trailing_padding as usize);
        }
        specs.push(FormatSpec {
            argument: arg,
            start: nchars,
            end: nchars + incr,
            fbeg: fpos0,
            intervals: false,
        });
        nchars += incr;
        new_result = true;
    }

    if !new_result {
        return Ok(args[0]);
    }
    let val = Value::StringObject(
        StringObjectRef::from_storage(buf, nchars, multibyte)
            .map_err(|error| string_storage_error(interp, env, error))?,
    );
    if fmt_props || arg_intervals {
        add_format_properties(&val, &args[0], &discarded, &specs)?;
    }
    Ok(val)
}

/// The text a %s or %S of a non-string ARG substitutes, as
/// `prin1-to-string' writes it (NOESCAPE for %s): a number's digits
/// directly, which print.c prints with no Lisp and no object.
fn spec_print(
    interp: &mut Interpreter,
    env: &mut Env,
    arg: &Value,
    noescape: bool,
) -> Result<SpecText, LispError> {
    match arg.kind() {
        Kind::Integer(n)
            if !interp
                .symbol_value_key(cached_symbol!("print-integers-as-characters"))
                .is_some_and(|value| value.is_truthy()) =>
        {
            let mut digits: Printed = SmallVec::new();
            sprintf_integer(
                &mut digits,
                b'd',
                n < 0,
                n.unsigned_abs(),
                -1,
                false,
                false,
                false,
            );
            Ok(SpecText::Number(digits))
        }
        Kind::Float(f) => Ok(SpecText::Number(SmallVec::from_slice(
            float_to_string(interp, f.get()).as_bytes(),
        ))),
        _ => Ok(SpecText::String(prin1_to_string(
            interp, env, arg, noescape, None,
        )?)),
    }
}

/// The end of styled_format: the format's text properties onto the
/// result VAL, each range moved to where its characters went (a range
/// over a specification's first character takes in all its output), and
/// then each string argument's properties over its output, a range that
/// reached the argument's end stretched to the output's.
fn add_format_properties(
    val: &Value,
    format: &Value,
    discarded: &[bool],
    specs: &[FormatSpec],
) -> Result<(), LispError> {
    // text_property_list of the format, in increasing order.
    let format_props: Vec<PropertyRange> = match format.kind() {
        Kind::StringObject(state) => property_ranges(&state.borrow())?,
        _ => Vec::new(),
    };
    if !format_props.is_empty() {
        let mut position = 0usize;
        let mut translated = 0usize;
        let mut fieldn = 0usize;
        let mut advance = |to: usize, position: &mut usize, translated: &mut usize| {
            while *position < to {
                if !discarded.get(*position).copied().unwrap_or(false) {
                    *translated += 1;
                } else if fieldn < specs.len()
                    && *position >= specs[fieldn].fbeg
                    && *translated == specs[fieldn].start
                {
                    *translated += specs[fieldn].end - specs[fieldn].start;
                    fieldn += 1;
                }
                *position += 1;
            }
        };
        let mut moved = Vec::with_capacity(format_props.len());
        for (start, end, plist) in format_props {
            advance(start, &mut position, &mut translated);
            let new_start = translated;
            advance(end, &mut position, &mut translated);
            moved.push((new_start, translated, plist));
        }
        for (start, end, plist) in moved {
            add_string_properties(val, start, end, plist)?;
        }
    }

    for (i, spec) in specs.iter().enumerate() {
        if !spec.intervals {
            continue;
        }
        let Kind::StringObject(state) = spec.argument.kind() else {
            continue;
        };
        let len = state.borrow().len();
        let new_len = spec.end - spec.start;
        // text_property_list's order, decreasing.
        let mut props: Vec<PropertyRange> = property_ranges(&state.borrow())?;
        props.reverse();
        // textprop.c extend_property_ranges.
        props.retain_mut(|(start, end, _)| {
            if *start >= new_len {
                return false;
            }
            if (*end == len && *end != new_len) || *end > new_len {
                *end = new_len;
            }
            true
        });
        // If successive arguments have properties, be sure that the value
        // of the `composition' property be the copy: in the argument's own
        // plists, which text_property_list gives.
        if 1 < i && specs[i - 1].end != 0 {
            for (_, _, plist) in &props {
                crate::intervals::copy_composition_value(*plist);
            }
        }
        for (start, end, plist) in props {
            add_string_properties(val, spec.start + start, spec.start + end, plist)?;
        }
    }
    Ok(())
}

/// textprop.c:text_property_list of a whole string, in increasing order:
/// each interval with a plist, the plist its own.
fn property_ranges(
    state: &crate::lisp::types::SharedStringState,
) -> Result<Vec<PropertyRange>, LispError> {
    let Some(intervals) = state.props.intervals() else {
        return Ok(Vec::new());
    };
    Ok(intervals
        .iter()
        .filter(|(_, _, plist)| !plist.is_nil())
        .collect())
}

/// print.c float_to_string: DATA as the printer writes it.  A
/// `float-output-format' of the form "%.N[efg]" (N at most DBL_DIG digits,
/// or none, and zero only for %f) is printf's format for it; any other
/// value prints the fewest digits that read back as DATA.  Unless the
/// format is "%.0f", the text then reads as a float: where it shows
/// neither a fraction nor an exponent, a decimal point and digit follow.
pub(crate) fn float_to_string(interp: &Interpreter, data: f64) -> String {
    if !data.is_finite() {
        return crate::lisp::types::format_float(data);
    }
    let spec = interp
        .symbol_value_key(cached_symbol!("float-output-format"))
        .and_then(|format| match format.kind() {
            Kind::StringObject(state) => float_output_spec(state.borrow().bytes()),
            _ => None,
        });
    let Some((conversion, width)) = spec else {
        return crate::lisp::types::format_float(data);
    };
    let mut text: Printed = SmallVec::new();
    // "%.e" is precision zero.
    sprintf_double(
        &mut text,
        conversion,
        data,
        width.max(0),
        false,
        false,
        false,
    );
    if width != 0 {
        let at = text
            .iter()
            .position(|&byte| !byte.is_ascii_digit() && byte != b'-')
            .unwrap_or(text.len());
        if text.get(at) == Some(&b'.') && at + 1 == text.len() {
            text.push(b'0');
        } else if at == text.len() {
            text.extend_from_slice(b".0");
        }
    }
    String::from_utf8(text.to_vec()).expect("printf writes ASCII")
}

/// float_to_string's check of `float-output-format''s bytes FORMAT: the
/// conversion and the digits' count (-1 for none) of a valid "%.N[efg]",
/// read as C reads it, up to a NUL.
fn float_output_spec(format: &[u8]) -> Option<(u8, isize)> {
    let at = |i: usize| format.get(i).copied().unwrap_or(0);
    if at(0) != b'%' || at(1) != b'.' {
        return None;
    }
    let mut cp = 2;
    let mut width: isize = -1;
    if at(cp).is_ascii_digit() {
        width = 0;
        while at(cp).is_ascii_digit() {
            width = width * 10 + isize::from(at(cp) - b'0');
            cp += 1;
            if 15 < width {
                return None;
            }
        }
        // A precision of zero is valid only for %f.
        if width == 0 && at(cp) != b'f' {
            return None;
        }
    }
    let conversion = at(cp);
    if !matches!(conversion, b'e' | b'f' | b'g') || at(cp + 1) != 0 {
        return None;
    }
    Some((conversion, width))
}
