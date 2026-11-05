//! image.c's image specifications: valid_image_p over the image types
//! of the contracted X build, each type's keyword table checked as
//! parse_image_spec checks it.

use super::*;
use crate::lisp::types::Kind;

/// image.c's image_value_type.
#[derive(Clone, Copy)]
enum ImageValueType {
    DontCheck,
    String,
    StringOrNil,
    Symbol,
    PositiveInteger,
    NonNegativeIntegerOrPair,
    Ascent,
    NonNegativeInteger,
    Integer,
}

use ImageValueType::*;

/// An image type's keyword: its name, its value type and whether a spec
/// must give it.
type ImageKeyword = (&'static str, ImageValueType, bool);

const XBM_FORMAT: &[ImageKeyword] = &[
    (":type", Symbol, true),
    (":file", String, false),
    (":width", PositiveInteger, false),
    (":height", PositiveInteger, false),
    (":stride", PositiveInteger, false),
    (":data", DontCheck, false),
    (":foreground", StringOrNil, false),
    (":background", StringOrNil, false),
    (":ascent", Ascent, false),
    (":margin", NonNegativeIntegerOrPair, false),
    (":relief", Integer, false),
    (":conversion", DontCheck, false),
    (":heuristic-mask", DontCheck, false),
    (":mask", DontCheck, false),
    (":data-width", PositiveInteger, false),
    (":data-height", PositiveInteger, false),
];

const XPM_FORMAT: &[ImageKeyword] = &[
    (":type", Symbol, true),
    (":file", String, false),
    (":data", String, false),
    (":ascent", Ascent, false),
    (":margin", NonNegativeIntegerOrPair, false),
    (":relief", Integer, false),
    (":conversion", DontCheck, false),
    (":heuristic-mask", DontCheck, false),
    (":mask", DontCheck, false),
    (":color-symbols", DontCheck, false),
    (":background", StringOrNil, false),
];

const PBM_FORMAT: &[ImageKeyword] = &[
    (":type", Symbol, true),
    (":file", String, false),
    (":data", String, false),
    (":ascent", Ascent, false),
    (":margin", NonNegativeIntegerOrPair, false),
    (":relief", Integer, false),
    (":conversion", DontCheck, false),
    (":heuristic-mask", DontCheck, false),
    (":mask", DontCheck, false),
    (":foreground", StringOrNil, false),
    (":background", StringOrNil, false),
];

const PNG_FORMAT: &[ImageKeyword] = &[
    (":type", Symbol, true),
    (":data", String, false),
    (":file", String, false),
    (":ascent", Ascent, false),
    (":margin", NonNegativeIntegerOrPair, false),
    (":relief", Integer, false),
    (":conversion", DontCheck, false),
    (":heuristic-mask", DontCheck, false),
    (":mask", DontCheck, false),
    (":background", StringOrNil, false),
];

const JPEG_FORMAT: &[ImageKeyword] = &[
    (":type", Symbol, true),
    (":data", String, false),
    (":file", String, false),
    (":ascent", Ascent, false),
    (":margin", NonNegativeIntegerOrPair, false),
    (":relief", Integer, false),
    (":conversions", DontCheck, false),
    (":heuristic-mask", DontCheck, false),
    (":mask", DontCheck, false),
    (":background", StringOrNil, false),
];

const TIFF_FORMAT: &[ImageKeyword] = &[
    (":type", Symbol, true),
    (":data", String, false),
    (":file", String, false),
    (":ascent", Ascent, false),
    (":margin", NonNegativeIntegerOrPair, false),
    (":relief", Integer, false),
    (":conversions", DontCheck, false),
    (":heuristic-mask", DontCheck, false),
    (":mask", DontCheck, false),
    (":background", StringOrNil, false),
    (":index", NonNegativeInteger, false),
];

/// gif_format and webp_format.
const GIF_FORMAT: &[ImageKeyword] = &[
    (":type", Symbol, true),
    (":data", String, false),
    (":file", String, false),
    (":ascent", Ascent, false),
    (":margin", NonNegativeIntegerOrPair, false),
    (":relief", Integer, false),
    (":conversion", DontCheck, false),
    (":heuristic-mask", DontCheck, false),
    (":mask", DontCheck, false),
    (":index", NonNegativeInteger, false),
    (":background", StringOrNil, false),
];

const SVG_FORMAT: &[ImageKeyword] = &[
    (":type", Symbol, true),
    (":data", String, false),
    (":file", String, false),
    (":base-uri", String, false),
    (":css", String, false),
    (":ascent", Ascent, false),
    (":margin", NonNegativeIntegerOrPair, false),
    (":relief", Integer, false),
    (":conversion", DontCheck, false),
    (":heuristic-mask", DontCheck, false),
    (":mask", DontCheck, false),
    (":foreground", StringOrNil, false),
    (":background", StringOrNil, false),
];

/// image.c's image_types of the X build (librsvg, libpng, giflib,
/// libtiff, libjpeg, libXpm and libwebp), in their order.
const IMAGE_TYPES: &[(&str, &[ImageKeyword])] = &[
    ("svg", SVG_FORMAT),
    ("png", PNG_FORMAT),
    ("gif", GIF_FORMAT),
    ("tiff", TIFF_FORMAT),
    ("jpeg", JPEG_FORMAT),
    ("xpm", XPM_FORMAT),
    ("webp", GIF_FORMAT),
    ("xbm", XBM_FORMAT),
    ("pbm", PBM_FORMAT),
];

/// IMAGEP: a list headed by `image'.
fn imagep(object: &Value) -> bool {
    object.cons_values().is_some_and(
        |(head, _)| matches!(head.kind(), Kind::Symbol(symbol) if symbol.is_named("image")),
    )
}

fn symbol_is(value: &Value, name: &str) -> bool {
    matches!(value.kind(), Kind::Symbol(symbol) if symbol.is_named(name))
}

/// RANGED_FIXNUMP (LO, VALUE, HI).
fn ranged_fixnum(value: &Value, lo: i64, hi: i64) -> bool {
    matches!(value.kind(), Kind::Integer(number) if (lo..=hi).contains(&number))
}

/// The values parse_image_spec records for each keyword it recognizes.
type ParsedKeywords = Vec<Option<Value>>;

/// image.c:parse_image_spec: SPEC's property list walked a key and its
/// value at a time (a circular or dotted one is no spec), each keyword
/// of KEYWORDS given once with a value of its type, an unknown keyword
/// let pass, `:type' TYPE and every mandatory keyword given.
fn parse_image_spec(
    spec: &Value,
    keywords: &[ImageKeyword],
    type_name: &str,
) -> Option<ParsedKeywords> {
    if !imagep(spec) {
        return None;
    }
    let mut values: ParsedKeywords = vec![None; keywords.len()];
    let (_, mut plist) = spec.cons_values()?;
    // FOR_EACH_TAIL_SAFE's cycle check, Brent's teleporting tortoise.
    let mut tortoise = plist;
    let mut steps = 0usize;
    let mut power = 2usize;
    while let Some((key, rest)) = plist.cons_values() {
        // The first element of a pair must be a symbol, and a value
        // must follow.
        if !matches!(key.kind(), Kind::Symbol(_) | Kind::Nil | Kind::T) {
            return None;
        }
        let (value, tail) = rest.cons_values()?;
        let name = match key.kind() {
            Kind::Nil => "nil",
            Kind::T => "t",
            _ => key.symbol_str().unwrap_or(""),
        };
        if let Some(index) = keywords.iter().position(|(keyword, _, _)| *keyword == name) {
            if values[index].is_some() {
                return None;
            }
            values[index] = Some(value);
            let valid = match keywords[index].1 {
                String => value.is_string(),
                StringOrNil => value.is_string() || value.is_nil(),
                Symbol => matches!(value.kind(), Kind::Symbol(_) | Kind::Nil | Kind::T),
                PositiveInteger => ranged_fixnum(&value, 1, i32::MAX.into()),
                NonNegativeIntegerOrPair => {
                    ranged_fixnum(&value, 0, i32::MAX.into())
                        || value.cons_values().is_some_and(|(car, cdr)| {
                            ranged_fixnum(&car, 0, i32::MAX.into())
                                && ranged_fixnum(&cdr, 0, i32::MAX.into())
                        })
                }
                Ascent => symbol_is(&value, "center") || ranged_fixnum(&value, 0, 100),
                NonNegativeInteger => matches!(value.kind(), Kind::Integer(number) if number >= 0),
                Integer => ranged_fixnum(&value, i32::MIN.into(), i32::MAX.into()),
                DontCheck => true,
            };
            if !valid {
                return None;
            }
            if name == ":type" && !symbol_is(&value, type_name) {
                return None;
            }
        }
        if tail.is_nil() {
            // Every mandatory keyword must be there.
            return keywords
                .iter()
                .zip(&values)
                .all(|((_, _, mandatory), value)| !mandatory || value.is_some())
                .then_some(values);
        }
        plist = tail;
        steps += 1;
        if steps == power {
            tortoise = plist;
            steps = 0;
            power *= 2;
        } else if plist.eq_value(tortoise) {
            return None;
        }
    }
    None
}

/// The bytes image.c scans of string DATA (SSDATA): its ASCII
/// characters as they are, any other character as bytes none of the
/// scanners' tokens are.
fn scanned_bytes(data: &crate::lisp::primitives::strings::StringLike) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(data.text.len());
    for code in data.character_codes_iter() {
        if (0..0x80).contains(&code) {
            bytes.push(code as u8);
        } else if data.multibyte {
            let len = crate::lisp::primitives::strings::emacs_multibyte_char_len(code as u32)
                .unwrap_or(2);
            bytes.extend(std::iter::repeat_n(0x80, len));
        } else {
            bytes.push(0x80);
        }
    }
    bytes
}

const XBM_TK_IDENT: i32 = 256;
const XBM_TK_NUMBER: i32 = 257;
const XBM_TK_OVERFLOW: i32 = 258;
const BUFSIZ: usize = 8192;

/// image.c:xbm_scan over BYTES from *POS: the next token, an identifier
/// left in IDENT and a number in VALUE.  The string's terminating NUL
/// stops a comment.
fn xbm_scan(bytes: &[u8], pos: &mut usize, ident: &mut Vec<u8>, value: &mut i32) -> i32 {
    let end = bytes.len();
    let at = |index: usize| bytes.get(index).copied().unwrap_or(0);
    // c_isspace.
    let space = |c: u8| matches!(c, b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r');
    let mut c: u8 = 0;
    loop {
        // Skip white space; the last byte is never a token.
        while *pos < end {
            c = bytes[*pos];
            *pos += 1;
            if !space(c) {
                break;
            }
        }
        if *pos >= end {
            return 0;
        }
        if c.is_ascii_digit() {
            let mut number: i32 = 0;
            let mut overflow = false;
            if c == b'0' && *pos < end {
                c = bytes[*pos];
                *pos += 1;
                if c == b'x' || c == b'X' {
                    while *pos < end {
                        c = bytes[*pos];
                        *pos += 1;
                        let Some(digit) = (c as char).to_digit(16) else {
                            break;
                        };
                        let (product, carry) = number.overflowing_mul(16);
                        overflow |= carry;
                        number = product.wrapping_add(digit as i32);
                    }
                } else if (b'0'..=b'7').contains(&c) {
                    number = i32::from(c - b'0');
                    while *pos < end {
                        c = bytes[*pos];
                        *pos += 1;
                        if !(b'0'..=b'7').contains(&c) {
                            break;
                        }
                        let (product, carry) = number.overflowing_mul(8);
                        overflow |= carry;
                        number = product.wrapping_add(i32::from(c - b'0'));
                    }
                }
            } else {
                number = i32::from(c - b'0');
                while *pos < end {
                    c = bytes[*pos];
                    *pos += 1;
                    if !c.is_ascii_digit() {
                        break;
                    }
                    let (product, carry) = number.overflowing_mul(10);
                    let (sum, carry_add) = product.overflowing_add(i32::from(c - b'0'));
                    overflow |= carry | carry_add;
                    number = sum;
                }
            }
            if *pos < end {
                *pos -= 1;
            }
            *value = number;
            return if overflow {
                XBM_TK_OVERFLOW
            } else {
                XBM_TK_NUMBER
            };
        }
        if c == b'\'' {
            // A character literal: only hex escapes.
            let mut number: i32 = 0;
            let mut overflow = false;
            if *pos == end {
                return 0;
            }
            c = bytes[*pos];
            *pos += 1;
            if c != b'\\' || *pos == end {
                return 0;
            }
            c = bytes[*pos];
            *pos += 1;
            if c == b'x' {
                while *pos < end {
                    c = bytes[*pos];
                    *pos += 1;
                    if c == b'\'' {
                        *value = number;
                        return if overflow {
                            XBM_TK_OVERFLOW
                        } else {
                            XBM_TK_NUMBER
                        };
                    }
                    let Some(digit) = (c as char).to_digit(16) else {
                        return 0;
                    };
                    let (product, carry) = number.overflowing_mul(16);
                    overflow |= carry;
                    number = product.wrapping_add(digit as i32);
                }
            }
            return 0;
        }
        if c.is_ascii_alphabetic() || c == b'_' {
            ident.clear();
            ident.push(c);
            while *pos < end && ident.len() < BUFSIZ {
                c = bytes[*pos];
                *pos += 1;
                if !(c.is_ascii_alphanumeric() || c == b'_') {
                    break;
                }
                ident.push(c);
            }
            if *pos < end {
                *pos -= 1;
            }
            return XBM_TK_IDENT;
        }
        if c == b'/' && at(*pos) == b'*' {
            // A C-style comment.
            *pos += 1;
            while at(*pos) != 0 && (at(*pos) != b'*' || at(*pos + 1) != b'/') {
                *pos += 1;
            }
            if at(*pos) != 0 {
                *pos += 2;
                continue;
            }
        }
        return i32::from(c);
    }
}

/// image.c:check_image_size with no frame: a positive size within
/// `max-image-size', in pixels or as a scale of a 1024-pixel frame.
fn check_image_size(interp: &Interpreter, env: &Env, width: i32, height: i32) -> bool {
    if width <= 0 || height <= 0 {
        return false;
    }
    match interp
        .lookup_var_key(cached_symbol!("max-image-size"), env)
        .map(|value| value.kind())
    {
        Some(Kind::Integer(limit)) => i64::from(width) <= limit && i64::from(height) <= limit,
        Some(Kind::Float(scale)) => {
            f64::from(width) <= scale.get() * 1024.0 && f64::from(height) <= scale.get() * 1024.0
        }
        _ => true,
    }
}

/// image.c:xbm_file_p: whether DATA is the text of an XBM file, as far
/// as xbm_read_bitmap_data reads it without its bits: `#define's of
/// numbers, those ending in `width' and `height' giving a size
/// check_image_size accepts.
fn xbm_file_p(interp: &Interpreter, env: &Env, data: &Value) -> bool {
    let Some(text) = string_like(data).filter(|_| data.is_string()) else {
        return false;
    };
    let bytes = scanned_bytes(&text);
    let mut pos = 0;
    let mut ident = Vec::new();
    let mut value = 0;
    let (mut width, mut height) = (-1, -1);
    let mut token = xbm_scan(&bytes, &mut pos, &mut ident, &mut value);
    while token == i32::from(b'#') {
        token = xbm_scan(&bytes, &mut pos, &mut ident, &mut value);
        if token != XBM_TK_IDENT || ident != b"define" {
            return false;
        }
        token = xbm_scan(&bytes, &mut pos, &mut ident, &mut value);
        if token != XBM_TK_IDENT {
            return false;
        }
        token = xbm_scan(&bytes, &mut pos, &mut ident, &mut value);
        if token == XBM_TK_NUMBER {
            // IDENT still holds the macro's name.
            let suffix = match ident.iter().rposition(|&byte| byte == b'_') {
                Some(index) => &ident[index + 1..],
                None => &ident[..],
            };
            if suffix == b"width" {
                width = value;
            } else if suffix == b"height" {
                height = value;
            }
        }
        if token != XBM_TK_NUMBER {
            return false;
        }
        token = xbm_scan(&bytes, &mut pos, &mut ident, &mut value);
    }
    check_image_size(interp, env, width, height)
}

/// image.c:xbm_image_p: a file, the text of one, or bits as a vector of
/// strings or bool-vectors, a string or a bool-vector, enough of them
/// for its `:data-width', `:data-height' and `:stride'.
fn xbm_image_p(interp: &Interpreter, env: &Env, object: &Value) -> bool {
    // XBM_FILE, XBM_STRIDE, XBM_DATA, XBM_DATA_WIDTH and
    // XBM_DATA_HEIGHT: the bits' size is given by `:data-width' and
    // `:data-height', not the image's `:width' and `:height'.
    const FILE: usize = 1;
    const STRIDE: usize = 4;
    const DATA: usize = 5;
    const WIDTH: usize = 14;
    const HEIGHT: usize = 15;
    let Some(kw) = parse_image_spec(object, XBM_FORMAT, "xbm") else {
        return false;
    };
    if kw[FILE].is_some() {
        return kw[DATA].is_none();
    }
    if let Some(data) = kw[DATA]
        && xbm_file_p(interp, env, &data)
    {
        return true;
    }
    let (Some(width), Some(height), Some(data)) = (kw[WIDTH], kw[HEIGHT], kw[DATA]) else {
        return false;
    };
    let width = width.as_integer().unwrap_or(0);
    let height = height.as_integer().unwrap_or(0);
    let stride = kw[STRIDE]
        .and_then(|stride| stride.as_integer().ok())
        .unwrap_or(width);
    let chars =
        |value: &Value| string_like(value).map_or(0, |text| text.text.chars().count() as i64);
    let bool_len = |value: &Value| match value.kind() {
        Kind::BoolVector(vector) => Some(vector.len() as i64),
        _ => None,
    };
    match data.kind() {
        Kind::Vector(vector) => {
            let rows: Vec<Value> = vector.slots().collect();
            if (rows.len() as i64) < height {
                return false;
            }
            // Each row a string or bool-vector long enough for a line.
            rows.iter().take(height.max(0) as usize).all(|row| {
                if row.is_string() {
                    chars(row) >= stride / 8
                } else if let Some(len) = bool_len(row) {
                    len >= width
                } else {
                    false
                }
            })
        }
        _ if data.is_string() => chars(&data) >= stride / 8 * height,
        Kind::BoolVector(vector) => {
            if height > 1 && stride != (width + 7) / 8 * 8 {
                return false;
            }
            (vector.len() as i64) / height >= stride
        }
        _ => false,
    }
}

/// image.c:xpm_valid_color_symbols_p: a list of string pairs.
fn xpm_valid_color_symbols_p(mut symbols: Value) -> bool {
    while let Some((symbol, rest)) = symbols.cons_values() {
        if !symbol
            .cons_values()
            .is_some_and(|(name, color)| name.is_string() && color.is_string())
        {
            break;
        }
        symbols = rest;
    }
    symbols.is_nil()
}

/// image.c:valid_image_p: an `image' list whose `:type' names an image
/// type its spec is valid for.
pub(crate) fn valid_image_p(interp: &Interpreter, object: &Value, env: &Env) -> bool {
    if !imagep(object) {
        return false;
    }
    let Some((_, mut tail)) = object.cons_values() else {
        return false;
    };
    let mut tortoise = tail;
    let (mut steps, mut power) = (0usize, 2usize);
    while let Some((key, rest)) = tail.cons_values() {
        if symbol_is(&key, ":type") {
            let Some((type_value, _)) = rest.cons_values() else {
                return false;
            };
            let Some(&(type_name, format)) = IMAGE_TYPES
                .iter()
                .find(|(name, _)| symbol_is(&type_value, name))
            else {
                return false;
            };
            return match type_name {
                "xbm" => xbm_image_p(interp, env, object),
                "xpm" => parse_image_spec(object, format, type_name).is_some_and(|kw| {
                    // :file or :data, and no :color-symbols or a valid one.
                    usize::from(kw[1].is_some()) + usize::from(kw[2].is_some()) == 1
                        && kw[9].is_none_or(xpm_valid_color_symbols_p)
                }),
                _ => parse_image_spec(object, format, type_name).is_some_and(|kw| {
                    let file = format.iter().position(|(name, _, _)| *name == ":file");
                    let data = format.iter().position(|(name, _, _)| *name == ":data");
                    let given = |index: Option<usize>| {
                        usize::from(index.is_some_and(|index| kw[index].is_some()))
                    };
                    given(file) + given(data) == 1
                }),
            };
        }
        let Some((_, next)) = rest.cons_values() else {
            return false;
        };
        tail = next;
        steps += 1;
        if steps == power {
            tortoise = tail;
            steps = 0;
            power *= 2;
        } else if tail.eq_value(tortoise) {
            return false;
        }
    }
    false
}
