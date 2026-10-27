//! gnulib's nstrftime (strftime.c's __strftime_internal), as GNU Emacs
//! builds it: no tzname fallback (HAVE_TZNAME is unset, so %Z is the
//! struct's tm_zone), tm_gmtoff for %z, and the locale's items (%a, %b,
//! %c, %p, %x, %X, %r and the E and O alternatives) from the C
//! library's strftime, whose C-locale answers are this same code's.

use super::tz::{TimeZone, Tm, mktime_z};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Pad {
    /// The default: pad with 0 unless the format says otherwise.
    Zero,
    /// '0': always pad with 0.
    AlwaysZero,
    /// '+': always output a sign.
    Sign,
    /// '_': pad with spaces.
    Space,
    /// '-': do not pad.
    None,
}

const WEEKDAY_NAMES: [&str; 7] = [
    "Sunday",
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
];
const MONTH_NAMES: [&str; 12] = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
];

fn isleap(year: i32) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

/// strftime.c:iso_week_days: the days from the first day of the first
/// ISO week of this year to YDAY with week day WDAY.
fn iso_week_days(yday: i32, wday: i32) -> i32 {
    // Add enough to the first operand of % to make it nonnegative.
    let big_enough_multiple_of_7 = (366 / 7 + 2) * 7;
    yday - (yday - wday + 4 + big_enough_multiple_of_7) % 7 + 4 - 1
}

/// The output and its width, as __strftime_internal's add and cpy
/// macros keep them.
struct Out<'a> {
    buf: &'a mut Vec<u8>,
}

impl Out<'_> {
    /// width_add: N bytes from F, after the padding WIDTH asks for.
    fn add(&mut self, width: i32, pad: Pad, bytes: &[u8]) {
        let w = if pad == Pad::None || width < 0 {
            0
        } else {
            width as usize
        };
        if bytes.len() < w {
            let fill = if matches!(pad, Pad::AlwaysZero | Pad::Sign) {
                b'0'
            } else {
                b' '
            };
            self.buf.extend(std::iter::repeat_n(fill, w - bytes.len()));
        }
        self.buf.extend_from_slice(bytes);
    }

    /// width_cpy: as add, with the case conversion asked for.
    fn cpy(&mut self, width: i32, pad: Pad, bytes: &[u8], to_lowcase: bool, to_uppcase: bool) {
        if to_lowcase {
            self.add(width, pad, &bytes.to_ascii_lowercase());
        } else if to_uppcase {
            self.add(width, pad, &bytes.to_ascii_uppercase());
        } else {
            self.add(width, pad, bytes);
        }
    }
}

/// nstrftime (FORMAT, TP, TZ, NS), as timefns.c:emacs_nmemftime calls it:
/// each NUL-terminated piece of FORMAT separately, the NULs kept.
pub(crate) fn nmemftime(format: &[u8], tp: &Tm, tz: &TimeZone, ns: i32) -> Vec<u8> {
    let mut buf = Vec::new();
    for (index, piece) in format.split(|&b| b == 0).enumerate() {
        if index > 0 {
            buf.push(0);
        }
        strftime_internal(&mut buf, piece, tp, false, Pad::Zero, -1, tz, ns);
    }
    buf
}

/// The C library's strftime of the single conversion "%MODIFIER CHAR"
/// (strftime.c's underlying_strftime).
fn underlying_strftime(format_char: u8, modifier: u8, tp: &Tm) -> Vec<u8> {
    let mut format = vec![b'%'];
    if modifier != 0 {
        format.push(modifier);
    }
    format.push(format_char);
    let mut buf = Vec::new();
    glibc_strftime(&mut buf, &format, tp);
    buf
}

/// strftime.c:__strftime_internal.
#[allow(clippy::too_many_arguments)]
fn strftime_internal(
    buf: &mut Vec<u8>,
    format: &[u8],
    tp: &Tm,
    upcase: bool,
    yr_spec: Pad,
    mut width: i32,
    tz: &TimeZone,
    ns: i32,
) {
    let mut out = Out { buf };
    let hour12 = match tp.hour {
        h if h > 12 => h - 12,
        0 => 12,
        h => h,
    };
    let zone: &[u8] = tp.zone.as_deref().map_or(b"", str::as_bytes);
    let mut f = 0usize;
    while f < format.len() {
        let mut pad = Pad::Zero;
        let mut to_lowcase = false;
        let mut to_uppcase = upcase;
        let mut change_case = false;
        if format[f] != b'%' {
            out.add(width, pad, &format[f..=f]);
            width = -1;
            f += 1;
            continue;
        }
        let percent = f;
        // Flags that modify the format.
        loop {
            f += 1;
            match format.get(f) {
                Some(b'_') => pad = Pad::Space,
                Some(b'-') => pad = Pad::None,
                Some(b'+') => pad = Pad::Sign,
                Some(b'0') => pad = Pad::AlwaysZero,
                Some(b'^') => to_uppcase = true,
                Some(b'#') => change_case = true,
                _ => break,
            }
        }
        if format.get(f).is_some_and(u8::is_ascii_digit) {
            width = 0;
            while let Some(&digit) = format.get(f).filter(|d| d.is_ascii_digit()) {
                width = width
                    .checked_mul(10)
                    .and_then(|w| w.checked_add(i32::from(digit - b'0')))
                    .unwrap_or(i32::MAX);
                f += 1;
            }
        }
        let modifier = match format.get(f) {
            Some(&m @ (b'E' | b'O')) => {
                f += 1;
                m
            }
            _ => 0,
        };
        let format_char = format.get(f).copied().unwrap_or(0);

        // The numeric conversions' state.
        let mut digits = 0i32;
        let mut negative_number = false;
        let mut u_number_value: u32 = 0;
        let mut always_output_a_sign = false;
        let mut tz_colon_mask = 0u32;

        enum Step {
            Done,
            BadFormat,
            Underlying(u8),
            Subformat(&'static [u8], i32),
            Number,
            NumberSpacepad,
            SignedNumber,
            Yearish,
            TzOffset,
            Digits(Vec<u8>),
        }

        let step = match format_char {
            b'%' => {
                if f - 1 != percent {
                    // bad_percent: output what precedes this '%', which
                    // then starts a conversion of its own.
                    f -= 1;
                    Step::BadFormat
                } else {
                    out.add(width, pad, b"%");
                    Step::Done
                }
            }
            b'a' | b'A' => {
                if modifier != 0 {
                    Step::BadFormat
                } else {
                    if change_case {
                        to_uppcase = true;
                        to_lowcase = false;
                    }
                    Step::Underlying(format_char)
                }
            }
            b'b' | b'h' | b'B' => {
                if change_case {
                    to_uppcase = true;
                    to_lowcase = false;
                }
                if modifier == b'E' {
                    Step::BadFormat
                } else {
                    Step::Underlying(format_char)
                }
            }
            b'c' => {
                if modifier == b'O' {
                    Step::BadFormat
                } else {
                    Step::Underlying(format_char)
                }
            }
            b'C' => {
                if modifier == b'E' {
                    Step::Underlying(format_char)
                } else {
                    let negative_year = tp.year < -1900;
                    let zero_thru_1899 = !negative_year & (tp.year < 0);
                    let century = (tp.year - 99 * i32::from(zero_thru_1899)) / 100 + 1900 / 100;
                    digits = 2;
                    negative_number = negative_year;
                    u_number_value = century as u32;
                    Step::Yearish
                }
            }
            b'x' => {
                if modifier == b'O' {
                    Step::BadFormat
                } else {
                    Step::Underlying(format_char)
                }
            }
            b'D' => {
                if modifier != 0 {
                    Step::BadFormat
                } else {
                    Step::Subformat(b"%m/%d/%y", -1)
                }
            }
            b'd' | b'e' if modifier == b'E' => Step::BadFormat,
            b'd' => {
                digits = 2;
                u_number_value = tp.mday as u32;
                negative_number = tp.mday < 0;
                Step::Number
            }
            b'e' => {
                digits = 2;
                u_number_value = tp.mday as u32;
                negative_number = tp.mday < 0;
                Step::NumberSpacepad
            }
            b'F' => {
                if modifier != 0 {
                    Step::BadFormat
                } else {
                    let subwidth = if pad == Pad::Zero && width < 0 {
                        pad = Pad::Sign;
                        4
                    } else {
                        (width - 6).max(0)
                    };
                    Step::Subformat(b"%Y-%m-%d", subwidth)
                }
            }
            b'H' | b'I' | b'k' | b'l' | b'j' | b'M' | b'm' | b'S' | b'U' | b'W' | b'w'
                if modifier == b'E' =>
            {
                Step::BadFormat
            }
            b'H' | b'I' | b'M' | b'S' | b'U' | b'W' | b'w' | b'u' => {
                let (d, value) = match format_char {
                    b'H' => (2, tp.hour),
                    b'I' => (2, hour12),
                    b'M' => (2, tp.min),
                    b'S' => (2, tp.sec),
                    b'U' => (2, (tp.yday - tp.wday + 7) / 7),
                    b'W' => (2, (tp.yday - (tp.wday - 1 + 7) % 7 + 7) / 7),
                    b'w' => (1, tp.wday),
                    _ => (1, (tp.wday - 1 + 7) % 7 + 1),
                };
                digits = d;
                u_number_value = value as u32;
                negative_number = value < 0;
                Step::Number
            }
            b'k' | b'l' => {
                let value = if format_char == b'k' { tp.hour } else { hour12 };
                digits = 2;
                u_number_value = value as u32;
                negative_number = value < 0;
                Step::NumberSpacepad
            }
            b'j' => {
                digits = 3;
                negative_number = tp.yday < -1;
                u_number_value = (tp.yday as u32).wrapping_add(1);
                Step::SignedNumber
            }
            b'm' => {
                digits = 2;
                negative_number = tp.mon < -1;
                u_number_value = (tp.mon as u32).wrapping_add(1);
                Step::SignedNumber
            }
            b'N' => {
                if modifier == b'E' {
                    Step::BadFormat
                } else {
                    let mut n = ns;
                    let ns_digits = 9;
                    if width <= 0 {
                        width = ns_digits;
                    }
                    let mut ndigs = ns_digits;
                    while width < ndigs || (1 < ndigs && n % 10 == 0) {
                        ndigs -= 1;
                        n /= 10;
                    }
                    let mut digits_buf = vec![b'0'; ndigs as usize];
                    for j in (0..ndigs as usize).rev() {
                        digits_buf[j] = (n % 10) as u8 + b'0';
                        n /= 10;
                    }
                    if pad == Pad::Zero {
                        pad = Pad::AlwaysZero;
                    }
                    out.cpy(0, pad, &digits_buf, to_lowcase, to_uppcase);
                    out.add(width - ndigs, pad, b"");
                    Step::Done
                }
            }
            b'n' => {
                out.add(width, pad, b"\n");
                Step::Done
            }
            b'P' | b'p' => {
                if format_char == b'P' {
                    to_lowcase = true;
                }
                if change_case {
                    to_uppcase = false;
                    to_lowcase = true;
                }
                Step::Underlying(b'p')
            }
            b'q' => {
                digits = 1;
                negative_number = false;
                u_number_value = (((tp.mon * 11) >> 5) + 1) as u32;
                Step::SignedNumber
            }
            b'R' => Step::Subformat(b"%H:%M", -1),
            b'r' => Step::Underlying(format_char),
            b's' => {
                let mut ltm = tp.clone();
                ltm.yday = -1;
                match mktime_z(tz, &mut ltm) {
                    Ok(t) if ltm.yday >= 0 => {
                        negative_number = t < 0;
                        digits = 1;
                        always_output_a_sign = false;
                        Step::Digits(t.unsigned_abs().to_string().into_bytes())
                    }
                    // EOVERFLOW: nstrftime fails.
                    _ => Step::Done,
                }
            }
            b'X' => {
                if modifier == b'O' {
                    Step::BadFormat
                } else {
                    Step::Underlying(format_char)
                }
            }
            b'T' => Step::Subformat(b"%H:%M:%S", -1),
            b't' => {
                out.add(width, pad, b"\t");
                Step::Done
            }
            b'V' | b'g' | b'G' => {
                if modifier == b'E' {
                    Step::BadFormat
                } else {
                    let year = tp.year
                        + if tp.year < 0 {
                            1900 % 400
                        } else {
                            1900 % 400 - 400
                        };
                    let mut year_adjust = 0;
                    let mut days = iso_week_days(tp.yday, tp.wday);
                    if days < 0 {
                        // This ISO week belongs to the previous year.
                        year_adjust = -1;
                        days =
                            iso_week_days(tp.yday + (365 + i32::from(isleap(year - 1))), tp.wday);
                    } else {
                        let d = iso_week_days(tp.yday - (365 + i32::from(isleap(year))), tp.wday);
                        if 0 <= d {
                            // This ISO week belongs to the next year.
                            year_adjust = 1;
                            days = d;
                        }
                    }
                    match format_char {
                        b'g' => {
                            let yy = (tp.year % 100 + year_adjust) % 100;
                            digits = 2;
                            negative_number = false;
                            u_number_value = if 0 <= yy {
                                yy
                            } else if tp.year < -1900 - year_adjust {
                                -yy
                            } else {
                                yy + 100
                            } as u32;
                            Step::Yearish
                        }
                        b'G' => {
                            digits = 4;
                            negative_number = tp.year < -1900 - year_adjust;
                            u_number_value = (tp.year as u32)
                                .wrapping_add(1900)
                                .wrapping_add(year_adjust as u32);
                            Step::Yearish
                        }
                        _ => {
                            digits = 2;
                            let value = days / 7 + 1;
                            u_number_value = value as u32;
                            negative_number = value < 0;
                            Step::Number
                        }
                    }
                }
            }
            b'Y' => {
                if modifier == b'E' {
                    Step::Underlying(format_char)
                } else if modifier == b'O' {
                    Step::BadFormat
                } else {
                    digits = 4;
                    negative_number = tp.year < -1900;
                    u_number_value = (tp.year as u32).wrapping_add(1900);
                    Step::Yearish
                }
            }
            b'y' => {
                if modifier == b'E' {
                    Step::Underlying(format_char)
                } else {
                    let mut yy = tp.year % 100;
                    if yy < 0 {
                        yy = if tp.year < -1900 { -yy } else { yy + 100 };
                    }
                    digits = 2;
                    negative_number = false;
                    u_number_value = yy as u32;
                    Step::Yearish
                }
            }
            b'Z' => {
                if change_case {
                    to_uppcase = false;
                    to_lowcase = true;
                }
                out.cpy(width, pad, zone, to_lowcase, to_uppcase);
                Step::Done
            }
            b':' | b'z' => {
                // :, ::, and ::: are valid only just before 'z'.
                let mut colons = 0usize;
                if format_char == b':' {
                    colons = 1;
                    while format.get(f + colons) == Some(&b':') {
                        colons += 1;
                    }
                }
                if format_char == b':' && format.get(f + colons) != Some(&b'z') {
                    Step::BadFormat
                } else {
                    f += colons;
                    if tp.isdst < 0 {
                        Step::Done
                    } else {
                        let diff = tp.gmtoff as i32;
                        negative_number = diff < 0 || (diff == 0 && zone.first() == Some(&b'-'));
                        let hour_diff = diff / 60 / 60;
                        let min_diff = diff / 60 % 60;
                        let sec_diff = diff % 60;
                        let hh_mm = |digits: &mut i32, mask: &mut u32, value: &mut u32| {
                            *digits = 6;
                            *mask = 0o4;
                            *value = (hour_diff * 100 + min_diff) as u32;
                        };
                        let hh_mm_ss = |digits: &mut i32, mask: &mut u32, value: &mut u32| {
                            *digits = 9;
                            *mask = 0o24;
                            *value = (hour_diff * 10000 + min_diff * 100 + sec_diff) as u32;
                        };
                        match colons {
                            0 => {
                                digits = 5;
                                tz_colon_mask = 0;
                                u_number_value = (hour_diff * 100 + min_diff) as u32;
                                Step::TzOffset
                            }
                            1 => {
                                hh_mm(&mut digits, &mut tz_colon_mask, &mut u_number_value);
                                Step::TzOffset
                            }
                            2 => {
                                hh_mm_ss(&mut digits, &mut tz_colon_mask, &mut u_number_value);
                                Step::TzOffset
                            }
                            3 => {
                                if sec_diff != 0 {
                                    hh_mm_ss(&mut digits, &mut tz_colon_mask, &mut u_number_value);
                                } else if min_diff != 0 {
                                    hh_mm(&mut digits, &mut tz_colon_mask, &mut u_number_value);
                                } else {
                                    digits = 3;
                                    tz_colon_mask = 0;
                                    u_number_value = hour_diff as u32;
                                }
                                Step::TzOffset
                            }
                            _ => Step::BadFormat,
                        }
                    }
                }
            }
            // GNU extension: % at the end of the format.
            0 => {
                f -= 1;
                Step::BadFormat
            }
            _ => Step::BadFormat,
        };

        // The numeric formats, as the labels after do_number.
        let digits_step = match step {
            Step::Done => None,
            Step::BadFormat => {
                // Unknown format: output it, including the '%'.
                let end = (f + 1).min(format.len());
                out.cpy(width, pad, &format[percent..end], to_lowcase, to_uppcase);
                None
            }
            Step::Underlying(char) => {
                let bytes = underlying_strftime(char, modifier, tp);
                out.cpy(width, pad, &bytes, to_lowcase, to_uppcase);
                None
            }
            Step::Subformat(subfmt, subwidth) => {
                let mut sub = Vec::new();
                strftime_internal(&mut sub, subfmt, tp, to_uppcase, pad, subwidth, tz, ns);
                out.add(width, pad, &sub);
                None
            }
            Step::Digits(bytes) => Some(bytes),
            Step::TzOffset
            | Step::Yearish
            | Step::Number
            | Step::NumberSpacepad
            | Step::SignedNumber => {
                match step {
                    Step::TzOffset => always_output_a_sign = true,
                    Step::Yearish => {
                        if pad == Pad::Zero {
                            pad = yr_spec;
                        }
                        always_output_a_sign = pad == Pad::Sign
                            && ((if digits == 2 { 99 } else { 9999 }) < u_number_value
                                || digits < width);
                        tz_colon_mask = 0;
                    }
                    _ => {
                        if matches!(step, Step::NumberSpacepad) && pad == Pad::Zero {
                            pad = Pad::Space;
                        }
                        always_output_a_sign = false;
                        tz_colon_mask = 0;
                    }
                }
                if modifier == b'O' && !negative_number {
                    let bytes = underlying_strftime(format_char, modifier, tp);
                    out.cpy(width, pad, &bytes, to_lowcase, to_uppcase);
                    None
                } else {
                    let mut value = if negative_number {
                        u_number_value.wrapping_neg()
                    } else {
                        u_number_value
                    };
                    let mut rev = Vec::new();
                    loop {
                        if tz_colon_mask & 1 != 0 {
                            rev.push(b':');
                        }
                        tz_colon_mask >>= 1;
                        rev.push((value % 10) as u8 + b'0');
                        value /= 10;
                        if value == 0 && tz_colon_mask == 0 {
                            break;
                        }
                    }
                    rev.reverse();
                    Some(rev)
                }
            }
        };
        if let Some(number) = digits_step {
            // do_number_sign_and_padding.
            if pad == Pad::Zero {
                pad = Pad::AlwaysZero;
            }
            if width < 0 {
                width = digits;
            }
            let sign_char = if negative_number {
                Some(b'-')
            } else if always_output_a_sign {
                Some(b'+')
            } else {
                None
            };
            let numlen = number.len() as i32;
            let shortage = width - i32::from(sign_char.is_some()) - numlen;
            let padding = if pad == Pad::None || shortage <= 0 {
                0
            } else {
                shortage
            };
            if let Some(sign) = sign_char {
                if pad == Pad::Space {
                    out.buf.extend(std::iter::repeat_n(b' ', padding as usize));
                    width -= padding;
                }
                out.add(0, pad, &[sign]);
                width -= 1;
            }
            out.cpy(width, pad, &number, to_lowcase, to_uppcase);
        }
        width = -1;
        f += 1;
    }
}

/// glibc 2.39's strftime_l.c:__strftime_internal in the C locale: the
/// C library's strftime that gnulib's copy calls for the locale's items.
/// Its numbers are not gnulib's: %Y and %G are at least one digit, %C,
/// %Y and %G add 1900 in an int, and %y and %g are mod 100. The C locale
/// has no eras, so its YR_SPEC (an era year's padding) has no use.
fn glibc_strftime(buf: &mut Vec<u8>, format: &[u8], tp: &Tm) {
    let hour12 = match tp.hour {
        h if h > 12 => h - 12,
        0 => 12,
        h => h,
    };
    let zone: &[u8] = tp.zone.as_deref().map_or(b"", str::as_bytes);
    // The add macro: N bytes after WIDTH's padding, zeros for '0'.
    let add = |buf: &mut Vec<u8>, width: i32, pad: u8, bytes: &[u8]| {
        let delta = width - bytes.len() as i32;
        if delta > 0 {
            let fill = if pad == b'0' { b'0' } else { b' ' };
            buf.extend(std::iter::repeat_n(fill, delta as usize));
        }
        buf.extend_from_slice(bytes);
    };
    let cpy = |buf: &mut Vec<u8>, width: i32, pad: u8, bytes: &[u8], lower: bool, upper: bool| {
        if lower {
            add(buf, width, pad, &bytes.to_ascii_lowercase());
        } else if upper {
            add(buf, width, pad, &bytes.to_ascii_uppercase());
        } else {
            add(buf, width, pad, bytes);
        }
    };
    let mut f = 0usize;
    while f < format.len() {
        let mut pad: u8 = 0;
        let mut width: i32 = -1;
        let mut to_lowcase = false;
        let mut to_uppcase = false;
        let mut change_case = false;
        if format[f] != b'%' {
            add(buf, width, pad, &format[f..=f]);
            f += 1;
            continue;
        }
        let percent = f;
        loop {
            f += 1;
            match format.get(f) {
                Some(&c @ (b'_' | b'-' | b'0')) => pad = c,
                Some(b'^') => to_uppcase = true,
                Some(b'#') => change_case = true,
                _ => break,
            }
        }
        if format.get(f).is_some_and(u8::is_ascii_digit) {
            width = 0;
            while let Some(&digit) = format.get(f).filter(|d| d.is_ascii_digit()) {
                width = width
                    .checked_mul(10)
                    .and_then(|w| w.checked_add(i32::from(digit - b'0')))
                    .unwrap_or(i32::MAX);
                f += 1;
            }
        }
        let modifier = match format.get(f) {
            Some(&m @ (b'E' | b'O')) => {
                f += 1;
                m
            }
            _ => 0,
        };
        let format_char = format.get(f).copied().unwrap_or(0);
        // DO_NUMBER and DO_NUMBER_SPACEPAD: Some((digits, value, spacepad)).
        let mut number: Option<(i32, i32, bool)> = None;
        let mut subfmt: Option<&'static [u8]> = None;
        let mut bad = false;
        let week_names = |abbrev: bool| -> &'static str {
            WEEKDAY_NAMES
                .get(tp.wday as usize)
                .filter(|_| (0..=6).contains(&tp.wday))
                .map_or("?", |name| if abbrev { &name[..3] } else { name })
        };
        let month_names = |abbrev: bool| -> &'static str {
            MONTH_NAMES
                .get(tp.mon as usize)
                .filter(|_| (0..=11).contains(&tp.mon))
                .map_or("?", |name| if abbrev { &name[..3] } else { name })
        };
        match format_char {
            b'%' => {
                if modifier != 0 {
                    bad = true;
                } else {
                    add(buf, width, pad, b"%");
                }
            }
            b'a' | b'A' => {
                if modifier != 0 {
                    bad = true;
                } else {
                    if change_case {
                        to_uppcase = true;
                        to_lowcase = false;
                    }
                    let name = week_names(format_char == b'a');
                    cpy(buf, width, pad, name.as_bytes(), to_lowcase, to_uppcase);
                }
            }
            b'b' | b'h' | b'B' => {
                if format_char != b'B' && change_case {
                    to_uppcase = true;
                    to_lowcase = false;
                }
                if modifier == b'E' {
                    bad = true;
                } else {
                    if format_char == b'B' && change_case {
                        to_uppcase = true;
                        to_lowcase = false;
                    }
                    // The C locale's alternative month names are its names.
                    let name = month_names(format_char != b'B');
                    cpy(buf, width, pad, name.as_bytes(), to_lowcase, to_uppcase);
                }
            }
            b'c' | b'x' | b'X' if modifier == b'O' => bad = true,
            b'c' => subfmt = Some(b"%a %b %e %H:%M:%S %Y"),
            b'x' => subfmt = Some(b"%m/%d/%y"),
            b'X' => subfmt = Some(b"%H:%M:%S"),
            b'D' => {
                if modifier != 0 {
                    bad = true;
                } else {
                    subfmt = Some(b"%m/%d/%y");
                }
            }
            b'F' => {
                if modifier != 0 {
                    bad = true;
                } else {
                    subfmt = Some(b"%Y-%m-%d");
                }
            }
            b'R' => subfmt = Some(b"%H:%M"),
            b'r' => subfmt = Some(b"%I:%M:%S %p"),
            b'T' => subfmt = Some(b"%H:%M:%S"),
            b'C' => {
                let year = tp.year.wrapping_add(1900);
                number = Some((1, year / 100 - i32::from(year % 100 < 0), false));
            }
            b'd' | b'e' | b'H' | b'I' | b'k' | b'l' | b'j' | b'M' | b'm' | b'S' | b'U' | b'V'
            | b'g' | b'G' | b'W' | b'w'
                if modifier == b'E' =>
            {
                bad = true
            }
            b'd' => number = Some((2, tp.mday, false)),
            b'e' => number = Some((2, tp.mday, true)),
            b'H' => number = Some((2, tp.hour, false)),
            b'I' => number = Some((2, hour12, false)),
            b'k' => number = Some((2, tp.hour, true)),
            b'l' => number = Some((2, hour12, true)),
            b'j' => number = Some((3, 1 + tp.yday, false)),
            b'M' => number = Some((2, tp.min, false)),
            b'm' => number = Some((2, tp.mon + 1, false)),
            b'S' => number = Some((2, tp.sec, false)),
            b'u' => number = Some((1, (tp.wday - 1 + 7) % 7 + 1, false)),
            b'U' => number = Some((2, (tp.yday - tp.wday + 7) / 7, false)),
            b'W' => number = Some((2, (tp.yday - (tp.wday - 1 + 7) % 7 + 7) / 7, false)),
            b'w' => number = Some((1, tp.wday, false)),
            b'V' | b'g' | b'G' => {
                let mut year = tp.year.wrapping_add(1900);
                let mut days = iso_week_days(tp.yday, tp.wday);
                if days < 0 {
                    // This ISO week belongs to the previous year.
                    year = year.wrapping_sub(1);
                    days = iso_week_days(tp.yday + (365 + i32::from(isleap(year))), tp.wday);
                } else {
                    let d = iso_week_days(tp.yday - (365 + i32::from(isleap(year))), tp.wday);
                    if 0 <= d {
                        // This ISO week belongs to the next year.
                        year = year.wrapping_add(1);
                        days = d;
                    }
                }
                number = Some(match format_char {
                    b'g' => (2, (year % 100 + 100) % 100, false),
                    b'G' => (1, year, false),
                    _ => (2, days / 7 + 1, false),
                });
            }
            b'Y' => {
                if modifier == b'O' {
                    bad = true;
                } else {
                    number = Some((1, tp.year.wrapping_add(1900), false));
                }
            }
            // The C locale has no era for 'E'.
            b'y' => number = Some((2, (tp.year % 100 + 100) % 100, false)),
            b'n' => add(buf, width, pad, b"\n"),
            b't' => add(buf, width, pad, b"\t"),
            b'P' | b'p' => {
                if format_char == b'P' {
                    to_lowcase = true;
                }
                if change_case {
                    to_uppcase = false;
                    to_lowcase = true;
                }
                let ampm: &[u8] = if tp.hour > 11 { b"PM" } else { b"AM" };
                cpy(buf, width, pad, ampm, to_lowcase, to_uppcase);
            }
            b's' => {
                let mut ltm = tp.clone();
                if let Ok(t) = mktime_z(&TimeZone::Tz(None), &mut ltm) {
                    let mut digits = t.unsigned_abs().to_string().into_bytes();
                    if t < 0 {
                        digits.insert(0, b'-');
                    }
                    cpy(buf, width, pad, &digits, to_lowcase, to_uppcase);
                }
            }
            b'Z' => {
                if change_case {
                    to_uppcase = false;
                    to_lowcase = true;
                }
                cpy(buf, width, pad, zone, to_lowcase, to_uppcase);
            }
            b'z' => {
                if tp.isdst >= 0 {
                    let mut diff = tp.gmtoff as i32;
                    if diff < 0 {
                        add(buf, width, pad, b"-");
                        diff = -diff;
                    } else {
                        add(buf, width, pad, b"+");
                    }
                    diff /= 60;
                    number = Some((4, (diff / 60) * 100 + diff % 60, false));
                }
            }
            0 => {
                // GNU extension: % at the end of the format.
                f -= 1;
                bad = true;
            }
            _ => bad = true,
        }
        if bad {
            // Unknown format: output it, including the '%'.
            let end = (f + 1).min(format.len());
            cpy(
                buf,
                width,
                pad,
                &format[percent..end],
                to_lowcase,
                to_uppcase,
            );
        } else if let Some(subfmt) = subfmt {
            let mut sub = Vec::new();
            glibc_strftime(&mut sub, subfmt, tp);
            let start = buf.len();
            add(buf, width, pad, &sub);
            if to_uppcase {
                buf[start..].make_ascii_uppercase();
            }
        } else if let Some((d, number_value, spacepad)) = number {
            let digits = d.max(width);
            if spacepad && pad != b'0' && pad != b'-' {
                pad = b'_';
            }
            // The C locale has no alternative digits for 'O'.
            let negative_number = number_value < 0;
            let mut text = number_value.unsigned_abs().to_string().into_bytes();
            if negative_number {
                text.insert(0, b'-');
            }
            if pad != b'-' {
                let padding = digits - text.len() as i32;
                if padding > 0 {
                    if pad == b'_' {
                        buf.extend(std::iter::repeat_n(b' ', padding as usize));
                        width = if width > padding { width - padding } else { 0 };
                    } else {
                        if negative_number {
                            text.remove(0);
                            buf.push(b'-');
                        }
                        buf.extend(std::iter::repeat_n(b'0', padding as usize));
                        width = 0;
                    }
                }
            }
            cpy(buf, width, pad, &text, to_lowcase, to_uppcase);
        }
        f += 1;
    }
}
