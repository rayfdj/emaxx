//! The C library time zone layer timefns.c stands on: glibc's tzset.c
//! (the TZ string), tzfile.c (TZif data files), offtime.c and mktime.c,
//! and gnulib's time_rz.c (`timezone_t', localtime_rz and mktime_z).
//! GNU Emacs hands every zone to these as a TZ string, so a zone here is
//! that string (or its absence, the wall clock) and the answers are the
//! ones glibc gives with TZ set to it.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::atomic::{AtomicI64, Ordering};

/// struct tm, with glibc's tm_gmtoff and tm_zone.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Tm {
    pub(crate) sec: i32,
    pub(crate) min: i32,
    pub(crate) hour: i32,
    pub(crate) mday: i32,
    pub(crate) mon: i32,
    /// Years since 1900.
    pub(crate) year: i32,
    pub(crate) wday: i32,
    pub(crate) yday: i32,
    pub(crate) isdst: i32,
    pub(crate) gmtoff: i64,
    pub(crate) zone: Option<Rc<str>>,
}

/// gnulib's timezone_t as timefns.c makes them: the null zone (utc_tz,
/// which time_rz hands to gmtime_r and timegm), or a TZ setting, None
/// standing for TZ unset (the wall clock, tzalloc (NULL)).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum TimeZone {
    Utc,
    Tz(Option<Rc<str>>),
}

/// The EOVERFLOW a conversion fails with (time_error's "Specified time
/// is not representable").
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TimeOverflow;

const TZDEFAULT: &str = "/etc/localtime";
const TZDEFRULES: &str = "posixrules";
const DEFAULT_TZDIR: &str = "/usr/share/zoneinfo";
const SECS_PER_HOUR: i64 = 60 * 60;
const SECS_PER_DAY: i64 = SECS_PER_HOUR * 24;

/// mktime.c's __mon_yday: the days before each month (0-12).
const MON_YDAY: [[i64; 13]; 2] = [
    [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365],
    [0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366],
];

fn isleap(year: i64) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

/// offtime.c:__offtime: T at OFFSET seconds east of UTC, broken down
/// (the zone fields untouched); None (EOVERFLOW) when the year does not
/// fit an int.
fn offtime(t: i64, offset: i64, tp: &mut Tm) -> bool {
    let mut days = t / SECS_PER_DAY;
    let mut rem = t % SECS_PER_DAY + offset;
    while rem < 0 {
        rem += SECS_PER_DAY;
        days -= 1;
    }
    while rem >= SECS_PER_DAY {
        rem -= SECS_PER_DAY;
        days += 1;
    }
    let hour = rem / SECS_PER_HOUR;
    rem %= SECS_PER_HOUR;
    let min = rem / 60;
    let sec = rem % 60;
    // January 1, 1970 was a Thursday.
    let mut wday = (4 + days) % 7;
    if wday < 0 {
        wday += 7;
    }
    let div = |a: i64, b: i64| a / b - i64::from(a % b < 0);
    let leaps_thru_end_of = |y: i64| div(y, 4) - div(y, 100) + div(y, 400);
    let mut y: i64 = 1970;
    while days < 0 || days >= if isleap(y) { 366 } else { 365 } {
        // Guess a corrected year, assuming 365 days per year.
        let yg = y + days / 365 - i64::from(days % 365 < 0);
        days -= (yg - y) * 365 + leaps_thru_end_of(yg - 1) - leaps_thru_end_of(y - 1);
        y = yg;
    }
    let Ok(year) = i32::try_from(y - 1900) else {
        return false;
    };
    let ip = &MON_YDAY[usize::from(isleap(y))];
    let mut mon = 11;
    while days < ip[mon] {
        mon -= 1;
    }
    tp.hour = hour as i32;
    tp.min = min as i32;
    tp.sec = sec as i32;
    tp.wday = wday as i32;
    tp.year = year;
    tp.yday = days as i32;
    tp.mon = mon as i32;
    tp.mday = (days - ip[mon]) as i32 + 1;
    true
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum RuleKind {
    /// n: day of year, 0 to 365.
    #[default]
    J0,
    /// Jn: Julian day, 1 to 365, without February 29th.
    J1,
    /// Mm.n.d.
    M,
}

/// tzset.c's tz_rule, without its cache of the change time.
#[derive(Clone, Debug, Default)]
struct TzRule {
    name: Rc<str>,
    kind: RuleKind,
    m: u16,
    n: u16,
    d: u16,
    secs: i32,
    offset: i32,
}

impl TzRule {
    /// tzset.c:compute_change: when in YEAR the change to this rule
    /// happens.
    fn change(&self, year: i64) -> i64 {
        // First set T to January 1st, 0:00:00 GMT in YEAR.
        let mut t = if year > 1970 {
            ((year - 1970) * 365 + ((year - 1) / 4 - 1970 / 4) - ((year - 1) / 100 - 1970 / 100)
                + ((year - 1) / 400 - 1970 / 400))
                * SECS_PER_DAY
        } else {
            0
        };
        match self.kind {
            RuleKind::J1 => {
                t += (i64::from(self.d) - 1) * SECS_PER_DAY;
                if self.d >= 60 && isleap(year) {
                    t += SECS_PER_DAY;
                }
            }
            RuleKind::J0 => t += i64::from(self.d) * SECS_PER_DAY,
            RuleKind::M => {
                let myday = &MON_YDAY[usize::from(isleap(year))];
                let m = usize::from(self.m);
                t += myday[m - 1] * SECS_PER_DAY;
                // Zeller's Congruence: the day of the week of the month's
                // first day.
                let year = year as i32;
                let m1 = (i32::from(self.m) + 9) % 12 + 1;
                let yy0 = if self.m <= 2 { year - 1 } else { year };
                let yy1 = yy0 / 100;
                let yy2 = yy0 % 100;
                let mut dow = ((26 * m1 - 2) / 10 + 1 + yy2 + yy2 / 4 + yy1 / 4 - 2 * yy1) % 7;
                if dow < 0 {
                    dow += 7;
                }
                let mut d = i32::from(self.d) - dow;
                if d < 0 {
                    d += 7;
                }
                for _ in 1..self.n {
                    if i64::from(d + 7) >= myday[m] - myday[m - 1] {
                        break;
                    }
                    d += 7;
                }
                t += i64::from(d) * SECS_PER_DAY;
            }
        }
        t - i64::from(self.offset) + i64::from(self.secs)
    }
}

/// tzset.c's tz_rules: standard time, then daylight saving time.
#[derive(Clone, Debug, Default)]
struct PosixRules([TzRule; 2]);

impl PosixRules {
    /// tzset.c:__tz_compute: whether TIMER (whose UTC year is YEAR) is in
    /// daylight saving time.
    fn isdst(&self, timer: i64, year: i64) -> bool {
        let standard = self.0[0].change(year);
        let daylight = self.0[1].change(year);
        // The southern hemisphere's daylight saving time ends in the next
        // year.
        if standard > daylight {
            timer < daylight || timer >= standard
        } else {
            timer >= standard && timer < daylight
        }
    }
}

/// sscanf's "%hu" on BYTES at *POS: an unsigned short (wrapping as
/// strtoul's value is truncated), or None if no digit is there.
fn scan_hu(bytes: &[u8], pos: &mut usize) -> Option<u16> {
    let mut index = *pos;
    while index < bytes.len() && bytes[index].is_ascii_whitespace() {
        index += 1;
    }
    let mut negative = false;
    if index < bytes.len() && matches!(bytes[index], b'+' | b'-') {
        negative = bytes[index] == b'-';
        index += 1;
    }
    let start = index;
    let mut value: u64 = 0;
    while index < bytes.len() && bytes[index].is_ascii_digit() {
        value = value
            .saturating_mul(10)
            .saturating_add(u64::from(bytes[index] - b'0'));
        index += 1;
    }
    if index == start {
        return None;
    }
    *pos = index;
    // strtoul's value (ULONG_MAX past its range), stored as an unsigned
    // short.
    let value = if negative {
        value.wrapping_neg()
    } else {
        value
    };
    Some(value as u16)
}

/// sscanf (s, "%hu%n:%hu%n:%hu%n", ...): up to three colon-separated
/// numbers, each defaulting as given, and the bytes consumed by the
/// last number read (0 if none).
fn scan_hms(bytes: &[u8], defaults: (u16, u16, u16)) -> (u16, u16, u16, usize, bool) {
    let (mut mm, mut ss) = (defaults.1, defaults.2);
    let mut pos = 0;
    let Some(hh) = scan_hu(bytes, &mut pos) else {
        return (defaults.0, mm, ss, 0, false);
    };
    let mut consumed = pos;
    if bytes.get(pos) == Some(&b':') {
        let mut next = pos + 1;
        if let Some(m) = scan_hu(bytes, &mut next) {
            mm = m;
            pos = next;
            consumed = pos;
            if bytes.get(pos) == Some(&b':') {
                let mut next = pos + 1;
                if let Some(s) = scan_hu(bytes, &mut next) {
                    ss = s;
                    consumed = next;
                }
            }
        }
    }
    (hh, mm, ss, consumed, true)
}

fn compute_offset(ss: u16, mm: u16, hh: u16) -> i32 {
    i32::from(ss.min(59)) + i32::from(mm.min(59)) * 60 + i32::from(hh.min(24)) * 60 * 60
}

/// tzset.c:parse_tzname.
fn parse_tzname(tz: &[u8], pos: &mut usize) -> Option<Rc<str>> {
    let start = *pos;
    let mut p = start;
    while p < tz.len() && tz[p].is_ascii_alphabetic() {
        p += 1;
    }
    let (name_start, mut len) = (start, p - start);
    let name = if len < 3 {
        p = start;
        if tz.get(p) != Some(&b'<') {
            return None;
        }
        p += 1;
        let quoted = p;
        while p < tz.len() && (tz[p].is_ascii_alphanumeric() || matches!(tz[p], b'+' | b'-')) {
            p += 1;
        }
        len = p - quoted;
        if tz.get(p) != Some(&b'>') || len < 3 {
            return None;
        }
        p += 1;
        &tz[quoted..quoted + len]
    } else {
        &tz[name_start..name_start + len]
    };
    *pos = p;
    Some(String::from_utf8_lossy(name).into())
}

/// tzset.c:parse_offset.
fn parse_offset(tz: &[u8], pos: &mut usize, rules: &mut PosixRules, whichrule: usize) -> bool {
    let mut p = *pos;
    if whichrule == 0
        && (p >= tz.len() || !(matches!(tz[p], b'+' | b'-') || tz[p].is_ascii_digit()))
    {
        return false;
    }
    let sign = if p < tz.len() && matches!(tz[p], b'+' | b'-') {
        let sign = if tz[p] == b'-' { 1 } else { -1 };
        p += 1;
        sign
    } else {
        -1
    };
    *pos = p;
    let (hh, mm, ss, consumed, parsed) = scan_hms(&tz[p..], (0, 0, 0));
    if parsed {
        rules.0[whichrule].offset = sign * compute_offset(ss, mm, hh);
    } else if whichrule == 0 {
        // Standard time defaults to offset zero.
        rules.0[0].offset = 0;
        return false;
    } else {
        // DST defaults to one hour later than standard time.
        rules.0[1].offset = rules.0[0].offset + 60 * 60;
    }
    *pos = p + consumed;
    true
}

/// strtoul (s, &end, 10) for parse_rule's day numbers.
fn scan_ulong(bytes: &[u8], pos: usize) -> Option<(u64, usize)> {
    let mut index = pos;
    while index < bytes.len() && bytes[index].is_ascii_whitespace() {
        index += 1;
    }
    let mut negative = false;
    if index < bytes.len() && matches!(bytes[index], b'+' | b'-') {
        negative = bytes[index] == b'-';
        index += 1;
    }
    let start = index;
    let mut value: u64 = 0;
    while index < bytes.len() && bytes[index].is_ascii_digit() {
        value = value
            .saturating_mul(10)
            .saturating_add(u64::from(bytes[index] - b'0'));
        index += 1;
    }
    if index == start {
        return None;
    }
    Some((
        if negative {
            value.wrapping_neg()
        } else {
            value
        },
        index,
    ))
}

/// tzset.c:parse_rule.
fn parse_rule(tz: &[u8], pos: &mut usize, rules: &mut PosixRules, whichrule: usize) -> bool {
    let mut p = *pos;
    // Ignore a comma, as early POSIX.1 printings had one.
    if tz.get(p) == Some(&b',') {
        p += 1;
    }
    let tzr = &mut rules.0[whichrule];
    match tz.get(p) {
        Some(&c) if c == b'J' || c.is_ascii_digit() => {
            tzr.kind = if c == b'J' {
                RuleKind::J1
            } else {
                RuleKind::J0
            };
            if tzr.kind == RuleKind::J1 {
                p += 1;
                if !tz.get(p).is_some_and(u8::is_ascii_digit) {
                    return false;
                }
            }
            let Some((d, end)) = scan_ulong(tz, p) else {
                return false;
            };
            if d > 365 || (tzr.kind == RuleKind::J1 && d == 0) {
                return false;
            }
            tzr.d = d as u16;
            p = end;
        }
        Some(b'M') => {
            tzr.kind = RuleKind::M;
            // sscanf (tz, "M%hu.%hu.%hu%n", ...) != 3
            let mut q = p + 1;
            let Some(m) = scan_hu(tz, &mut q) else {
                return false;
            };
            if tz.get(q) != Some(&b'.') {
                return false;
            }
            q += 1;
            let Some(n) = scan_hu(tz, &mut q) else {
                return false;
            };
            if tz.get(q) != Some(&b'.') {
                return false;
            }
            q += 1;
            let Some(d) = scan_hu(tz, &mut q) else {
                return false;
            };
            if !(1..=12).contains(&m) || !(1..=5).contains(&n) || d > 6 {
                return false;
            }
            (tzr.m, tzr.n, tzr.d) = (m, n, d);
            p = q;
        }
        None => {
            // The U.S. rules of the Energy Policy Act of 2005,
            // "M3.2.0,M11.1.0".
            tzr.kind = RuleKind::M;
            (tzr.m, tzr.n, tzr.d) = if whichrule == 0 {
                (3, 2, 0)
            } else {
                (11, 1, 0)
            };
        }
        Some(_) => return false,
    }
    match tz.get(p) {
        Some(b'/') => {
            p += 1;
            if p >= tz.len() {
                return false;
            }
            let negative = tz[p] == b'-';
            if negative {
                p += 1;
            }
            // Default to 2:00 AM.
            let (hh, mm, ss, consumed, _) = scan_hms(&tz[p..], (2, 0, 0));
            p += consumed;
            tzr.secs = (if negative { -1 } else { 1 })
                * (i32::from(hh) * 60 * 60 + i32::from(mm) * 60 + i32::from(ss));
        }
        None | Some(b',') => tzr.secs = 2 * 60 * 60,
        Some(_) => return false,
    }
    *pos = p;
    true
}

/// TZif's struct ttinfo.
#[derive(Clone, Copy, Debug, Default)]
struct TtInfo {
    offset: i32,
    isdst: bool,
    idx: usize,
    isstd: bool,
    isgmt: bool,
}

#[derive(Clone, Copy, Debug)]
struct Leap {
    transition: i64,
    change: i64,
}

/// A TZif file as tzfile.c reads it.
#[derive(Clone, Debug, Default)]
struct TzFile {
    transitions: Vec<i64>,
    type_idxs: Vec<u8>,
    types: Vec<TtInfo>,
    zone_names: Vec<u8>,
    leaps: Vec<Leap>,
    /// The POSIX TZ string after the 64-bit data, for times past the last
    /// transition.
    tzspec: Option<Rc<PosixRules>>,
}

impl TzFile {
    fn abbreviation(&self, idx: usize) -> Rc<str> {
        let bytes = self.zone_names.get(idx..).unwrap_or(&[]);
        let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
        String::from_utf8_lossy(&bytes[..end]).into()
    }
}

/// A TZ setting as glibc holds it once tzset has run.
#[derive(Clone, Debug)]
enum GlibcZone {
    File(Rc<TzFile>),
    Rules(Rc<PosixRules>),
}

fn decode32(bytes: &[u8]) -> i64 {
    i64::from(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

fn decode64(bytes: &[u8]) -> i64 {
    i64::from_be_bytes(bytes[..8].try_into().unwrap_or([0; 8]))
}

/// tzfile.c:__tzfile_read's parse of a TZif file's bytes.
fn parse_tzfile(data: &[u8]) -> Option<TzFile> {
    let mut pos = 0usize;
    let mut trans_width = 4usize;
    let take = |pos: &mut usize, n: usize| -> Option<&[u8]> {
        let slice = data.get(*pos..pos.checked_add(n)?)?;
        *pos += n;
        Some(slice)
    };
    loop {
        let head = take(&mut pos, 44)?;
        if &head[..4] != b"TZif" {
            return None;
        }
        let version = head[4];
        let num_isgmt = decode32(&head[20..24]) as u32 as usize;
        let num_isstd = decode32(&head[24..28]) as u32 as usize;
        let num_leaps = decode32(&head[28..32]) as u32 as usize;
        let num_transitions = decode32(&head[32..36]) as u32 as usize;
        let num_types = decode32(&head[36..40]) as u32 as usize;
        let chars = decode32(&head[40..44]) as u32 as usize;
        if num_isstd > num_types || num_isgmt > num_types {
            return None;
        }
        if trans_width == 4 && version != 0 {
            // Use the 8-byte data after the second header.
            trans_width = 8;
            let to_skip = num_transitions * (4 + 1)
                + num_types * 6
                + chars
                + num_leaps * 8
                + num_isstd
                + num_isgmt;
            pos = pos.checked_add(to_skip)?;
            if pos > data.len() {
                return None;
            }
            continue;
        }
        let tzspec_len = if trans_width == 8 {
            let rem = data.len() - pos;
            let fixed = num_transitions * (8 + 1) + num_types * 6 + chars;
            let mut len = rem.checked_sub(fixed)?;
            len = len.checked_sub(num_leaps.checked_mul(12)?)?;
            len = len.checked_sub(num_isstd)?;
            if len == 0 || len - 1 < num_isgmt {
                return None;
            }
            len -= num_isgmt + 1;
            if len == 0 {
                return None;
            }
            len
        } else {
            0
        };
        let transitions_bytes = take(&mut pos, num_transitions * trans_width)?;
        let type_idxs = take(&mut pos, num_transitions)?.to_vec();
        if type_idxs.iter().any(|&t| usize::from(t) >= num_types) {
            return None;
        }
        let transitions = transitions_bytes
            .chunks(trans_width)
            .map(|chunk| {
                if trans_width == 4 {
                    decode32(chunk)
                } else {
                    decode64(chunk)
                }
            })
            .collect();
        let mut types = Vec::with_capacity(num_types);
        for _ in 0..num_types {
            let entry = take(&mut pos, 6)?;
            if entry[4] > 1 || usize::from(entry[5]) > chars {
                return None;
            }
            types.push(TtInfo {
                offset: decode32(&entry[..4]) as i32,
                isdst: entry[4] == 1,
                idx: usize::from(entry[5]),
                isstd: false,
                isgmt: false,
            });
        }
        let zone_names = take(&mut pos, chars)?.to_vec();
        let mut leaps = Vec::with_capacity(num_leaps);
        for _ in 0..num_leaps {
            let transition = take(&mut pos, trans_width)?;
            let transition = if trans_width == 4 {
                decode32(transition)
            } else {
                decode64(transition)
            };
            let change = decode32(take(&mut pos, 4)?);
            leaps.push(Leap { transition, change });
        }
        for info in types.iter_mut().take(num_isstd) {
            info.isstd = take(&mut pos, 1)?[0] != 0;
        }
        for info in types.iter_mut().take(num_isgmt) {
            info.isgmt = take(&mut pos, 1)?[0] != 0;
        }
        let mut tzspec = None;
        if trans_width == 8 {
            // Skip over the newline first.
            if data.get(pos) == Some(&b'\n') && pos + tzspec_len <= data.len() {
                let spec = &data[pos + 1..pos + tzspec_len];
                // Don't use an empty TZ string.
                let spec = &spec[..spec.iter().position(|&b| b == 0).unwrap_or(spec.len())];
                if !spec.is_empty() {
                    tzspec = Some(Rc::new(parse_tz_rules_only(spec)));
                }
            }
        }
        return Some(TzFile {
            transitions,
            type_idxs,
            types,
            zone_names,
            leaps,
            tzspec,
        });
    }
}

/// The file tzfile.c reads for FILE: absolute, or under TZDIR.
fn tzfile_path(file: &str) -> std::path::PathBuf {
    if file.starts_with('/') {
        return file.into();
    }
    let tzdir = std::env::var_os("TZDIR")
        .filter(|dir| !dir.is_empty())
        .unwrap_or_else(|| DEFAULT_TZDIR.into());
    let mut path = std::path::PathBuf::from(tzdir);
    // asprintf ("%s/%s", tzdir, file).
    path.push(file);
    path
}

/// Files already read, by path, with the identity tzfile.c checks before
/// reading a file again.
type TzFileCache = HashMap<std::path::PathBuf, (FileIdentity, Option<Rc<TzFile>>)>;

thread_local! {
    static TZFILES: RefCell<TzFileCache> = RefCell::new(HashMap::new());
    /// tzset's result for each TZ setting.
    static ZONES: RefCell<HashMap<Option<Rc<str>>, GlibcZone>> = RefCell::new(HashMap::new());
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct FileIdentity {
    dev: u64,
    ino: u64,
    mtime: i64,
    size: u64,
}

fn file_identity(path: &std::path::Path) -> Option<FileIdentity> {
    use std::os::unix::fs::MetadataExt;
    let metadata = std::fs::metadata(path).ok()?;
    Some(FileIdentity {
        dev: metadata.dev(),
        ino: metadata.ino(),
        mtime: metadata.mtime(),
        size: metadata.size(),
    })
}

/// tzfile.c:__tzfile_read: FILE's data, or None (no data file).
fn tzfile_read(file: &str) -> Option<Rc<TzFile>> {
    if file.is_empty() {
        return None;
    }
    let path = tzfile_path(file);
    let identity = file_identity(&path)?;
    if !std::fs::metadata(&path).is_ok_and(|metadata| metadata.is_file()) {
        return None;
    }
    if let Some(cached) = TZFILES.with(|files| {
        files
            .borrow()
            .get(&path)
            .filter(|(seen, _)| *seen == identity)
            .map(|(_, parsed)| parsed.clone())
    }) {
        return cached;
    }
    let parsed = std::fs::read(&path)
        .ok()
        .and_then(|data| parse_tzfile(&data))
        .map(Rc::new);
    TZFILES.with(|files| files.borrow_mut().insert(path, (identity, parsed.clone())));
    parsed
}

/// tzfile.c:__tzfile_default: the TZDEFRULES file's transitions moved to
/// the given offsets, with the given names for its two kinds of time.
fn tzfile_default(std: &Rc<str>, dst: &Rc<str>, stdoff: i32, dstoff: i32) -> Option<TzFile> {
    let rules = tzfile_read(TZDEFRULES)?;
    if rules.types.len() < 2 {
        return None;
    }
    let mut file = (*rules).clone();
    // tzfile.c's rule_stdoff and rule_dstoff as __tzfile_read leaves them.
    let (rule_stdoff, rule_dstoff) = tzfile_rule_offsets(&rules);
    let mut isdst = false;
    for i in 0..file.transitions.len() {
        let trans_type = rules.types[usize::from(rules.type_idxs[i])];
        file.type_idxs[i] = u8::from(trans_type.isdst);
        if trans_type.isgmt {
            // The transition time is in GMT.
        } else if isdst && !trans_type.isstd {
            file.transitions[i] += i64::from(dstoff) - rule_dstoff;
        } else {
            file.transitions[i] += i64::from(stdoff) - rule_stdoff;
        }
        isdst = trans_type.isdst;
    }
    let mut names = std.as_bytes().to_vec();
    names.push(0);
    let dst_idx = names.len();
    names.extend_from_slice(dst.as_bytes());
    names.push(0);
    file.zone_names = names;
    file.types.truncate(2);
    file.types[0] = TtInfo {
        idx: 0,
        offset: stdoff,
        isdst: false,
        ..file.types[0]
    };
    file.types[1] = TtInfo {
        idx: dst_idx,
        offset: dstoff,
        isdst: true,
        ..file.types[1]
    };
    Some(file)
}

/// The rule_stdoff and rule_dstoff __tzfile_read computes: the last
/// standard time offset transitioned to, and (rule_dstoff is never set
/// there, so it keeps its initial zero or what a previous
/// __tzfile_default left) zero.
fn tzfile_rule_offsets(file: &TzFile) -> (i64, i64) {
    if file.transitions.is_empty() {
        let offset = file.types.first().map_or(0, |info| i64::from(info.offset));
        return (offset, offset);
    }
    let stdoff = file
        .type_idxs
        .iter()
        .rev()
        .map(|&t| file.types[usize::from(t)])
        .find(|info| !info.isdst)
        .map_or(0, |info| i64::from(info.offset));
    (stdoff, 0)
}

/// __tzset_parse_tz for a TZif file's own TZ string: the rules, without
/// the TZDEFRULES fallback a string with a daylight name and no rule
/// would try (zic never writes one).
fn parse_tz_rules_only(tz: &[u8]) -> PosixRules {
    match tzset_parse_tz(tz, false) {
        GlibcZone::Rules(rules) => (*rules).clone(),
        GlibcZone::File(_) => PosixRules::default(),
    }
}

/// tzset.c:__tzset_parse_tz.  TRY_DEFAULT_RULES consults TZDEFRULES for
/// a daylight name with no rule.
fn tzset_parse_tz(tz: &[u8], try_default_rules: bool) -> GlibcZone {
    // Reset to unnamed UTC.
    let empty: Rc<str> = Rc::from("");
    let mut rules = PosixRules::default();
    rules.0[0].name = empty.clone();
    rules.0[1].name = empty;
    let mut pos = 0usize;
    if let Some(name) = parse_tzname(tz, &mut pos) {
        rules.0[0].name = name;
        if parse_offset(tz, &mut pos, &mut rules, 0) {
            if pos < tz.len() {
                if let Some(name) = parse_tzname(tz, &mut pos) {
                    rules.0[1].name = name;
                    parse_offset(tz, &mut pos, &mut rules, 1);
                    let rest = &tz[pos..];
                    if try_default_rules && (rest.is_empty() || rest == b",") {
                        // There is no rule: see if there is a default rule
                        // file.
                        if let Some(file) = tzfile_default(
                            &rules.0[0].name,
                            &rules.0[1].name,
                            rules.0[0].offset,
                            rules.0[1].offset,
                        ) {
                            return GlibcZone::File(Rc::new(file));
                        }
                    }
                }
                // Figure out the standard <-> DST rules.
                if parse_rule(tz, &mut pos, &mut rules, 0) {
                    parse_rule(tz, &mut pos, &mut rules, 1);
                }
            } else {
                // There is no DST.
                rules.0[1].name = rules.0[0].name.clone();
                rules.0[1].offset = rules.0[0].offset;
            }
        }
    }
    GlibcZone::Rules(Rc::new(rules))
}

/// tzset.c:tzset_internal for the TZ value TZ (None: unset).
fn tzset(tz: Option<&str>) -> GlibcZone {
    let mut tz = tz;
    if tz == Some("") {
        // The empty string means UTC, explicitly.
        tz = Some("Universal");
    }
    // A leading colon means "implementation defined syntax": try a data
    // file, and if none exists parse the 1003.1 syntax.
    if let Some(stripped) = tz.and_then(|tz| tz.strip_prefix(':')) {
        tz = Some(stripped);
    }
    let unset = tz.is_none();
    let tz = tz.unwrap_or(TZDEFAULT);
    if let Some(file) = tzfile_read(tz) {
        return GlibcZone::File(file);
    }
    // No data file found.  Default to UTC if nothing was specified.
    if unset || tz.is_empty() || tz == TZDEFAULT {
        let utc: Rc<str> = Rc::from("UTC");
        let mut rules = PosixRules::default();
        rules.0[0].name = utc.clone();
        rules.0[1].name = utc;
        return GlibcZone::Rules(Rc::new(rules));
    }
    tzset_parse_tz(tz.as_bytes(), true)
}

fn glibc_zone(tz: &Option<Rc<str>>) -> GlibcZone {
    if let Some(zone) = ZONES.with(|zones| zones.borrow().get(tz).cloned()) {
        // A data file is read again when it changed, as __tzfile_read's
        // check of the file's identity on each tzset does.
        let stale = matches!(&zone, GlibcZone::File(file) if {
            let name = tz.as_deref().unwrap_or(TZDEFAULT);
            let name = name.strip_prefix(':').unwrap_or(name);
            let name = if name.is_empty() { "Universal" } else { name };
            !tzfile_read(name).is_some_and(|fresh| Rc::ptr_eq(&fresh, file))
        });
        if !stale {
            return zone;
        }
    }
    let zone = tzset(tz.as_deref());
    ZONES.with(|zones| zones.borrow_mut().insert(tz.clone(), zone.clone()));
    zone
}

/// The zone fields tzfile.c:__tzfile_compute gives TIMER, and its leap
/// correction and leap hits.
fn tzfile_compute(file: &TzFile, timer: i64, tp: &mut Tm) -> (i64, i32) {
    let types = &file.types;
    let transitions = &file.transitions;
    let info = if transitions.is_empty() || timer < transitions[0] {
        // Before any transition: the first standard time type, or the
        // first type if all are daylight saving time.
        let i = types.iter().position(|info| !info.isdst).unwrap_or(0);
        types.get(i).copied()
    } else if timer >= transitions[transitions.len() - 1]
        && let Some(rules) = file.tzspec.as_deref()
    {
        let mut utc = Tm::default();
        if offtime(timer, 0, &mut utc) {
            let isdst = rules.isdst(timer, i64::from(utc.year) + 1900);
            let rule = &rules.0[usize::from(isdst)];
            tp.isdst = i32::from(isdst);
            tp.zone = Some(rule.name.clone());
            tp.gmtoff = i64::from(rule.offset);
            return leap_correction(file, timer);
        }
        types
            .get(usize::from(file.type_idxs[transitions.len() - 1]))
            .copied()
    } else {
        // The type of the last transition at or before TIMER.
        let i = transitions.partition_point(|&t| t <= timer);
        types.get(usize::from(file.type_idxs[i - 1])).copied()
    };
    if let Some(info) = info {
        tp.isdst = i32::from(info.isdst);
        tp.zone = Some(file.abbreviation(info.idx));
        tp.gmtoff = i64::from(info.offset);
    }
    leap_correction(file, timer)
}

/// __tzfile_compute's leap second correction for TIMER and the number
/// of leap seconds TIMER is on.
fn leap_correction(file: &TzFile, timer: i64) -> (i64, i32) {
    let leaps = &file.leaps;
    let Some(i) = leaps.iter().rposition(|leap| timer >= leap.transition) else {
        return (0, 0);
    };
    let correct = leaps[i].change;
    let mut hit = 0;
    if timer == leaps[i].transition
        && ((i == 0 && leaps[i].change > 0) || (i > 0 && leaps[i].change > leaps[i - 1].change))
    {
        hit = 1;
        let mut i = i;
        while i > 0
            && leaps[i].transition == leaps[i - 1].transition + 1
            && leaps[i].change == leaps[i - 1].change + 1
        {
            hit += 1;
            i -= 1;
        }
    }
    (correct, hit)
}

/// glibc's localtime_r with TZ set to TZ: tzset.c:__tz_convert.
fn glibc_localtime(tz: &Option<Rc<str>>, timer: i64) -> Result<Tm, TimeOverflow> {
    let mut tm = Tm::default();
    let (correction, hit) = match glibc_zone(tz) {
        GlibcZone::File(file) => tzfile_compute(&file, timer, &mut tm),
        GlibcZone::Rules(rules) => {
            if !offtime(timer, 0, &mut tm) {
                return Err(TimeOverflow);
            }
            let isdst = rules.isdst(timer, i64::from(tm.year) + 1900);
            let rule = &rules.0[usize::from(isdst)];
            tm.isdst = i32::from(isdst);
            tm.zone = Some(rule.name.clone());
            tm.gmtoff = i64::from(rule.offset);
            (0, 0)
        }
    };
    if !offtime(timer, tm.gmtoff - correction, &mut tm) {
        return Err(TimeOverflow);
    }
    tm.sec += hit;
    Ok(tm)
}

/// gmtime_r.
fn gmtime(timer: i64) -> Result<Tm, TimeOverflow> {
    let mut tm = Tm {
        zone: Some(Rc::from("GMT")),
        ..Tm::default()
    };
    if !offtime(timer, 0, &mut tm) {
        return Err(TimeOverflow);
    }
    Ok(tm)
}

/// time_rz.c:localtime_rz.
pub(crate) fn localtime_rz(tz: &TimeZone, timer: i64) -> Result<Tm, TimeOverflow> {
    match tz {
        TimeZone::Utc => gmtime(timer),
        TimeZone::Tz(name) => glibc_localtime(name, timer),
    }
}

/// mktime.c's leapyear: whether YEAR + 1900 is a leap year.
fn leapyear(year: i64) -> bool {
    (year & 3) == 0 && (year % 100 != 0 || ((year / 100) & 3) == (-(1900 / 100) & 3))
}

/// mktime.c:ydhms_diff.
#[allow(clippy::too_many_arguments)]
fn ydhms_diff(
    year1: i64,
    yday1: i64,
    hour1: i32,
    min1: i32,
    sec1: i32,
    year0: i32,
    yday0: i32,
    hour0: i32,
    min0: i32,
    sec0: i32,
) -> i64 {
    let a4 = (year1 >> 2) + (1900 >> 2) - i64::from(year1 & 3 == 0);
    let b4 = (i64::from(year0) >> 2) + (1900 >> 2) - i64::from(year0 & 3 == 0);
    let a100 = (a4 + i64::from(a4 < 0)) / 25 - i64::from(a4 < 0);
    let b100 = (b4 + i64::from(b4 < 0)) / 25 - i64::from(b4 < 0);
    let a400 = a100 >> 2;
    let b400 = b100 >> 2;
    let intervening_leap_days = (a4 - b4) - (a100 - b100) + (a400 - b400);
    let years = year1 - i64::from(year0);
    let days = 365 * years + yday1 - i64::from(yday0) + intervening_leap_days;
    let hours = 24 * days + i64::from(hour1) - i64::from(hour0);
    let minutes = 60 * hours + i64::from(min1) - i64::from(min0);
    60 * minutes + i64::from(sec1) - i64::from(sec0)
}

fn tm_diff(year: i64, yday: i64, hour: i32, min: i32, sec: i32, tp: &Tm) -> i64 {
    ydhms_diff(
        year, yday, hour, min, sec, tp.year, tp.yday, tp.hour, tp.min, tp.sec,
    )
}

fn long_int_avg(a: i64, b: i64) -> i64 {
    (a >> 1) + (b >> 1) + ((a | b) & 1)
}

/// mktime.c:ranged_convert, every i64 being a time_t.
fn ranged_convert(
    convert: &dyn Fn(i64) -> Result<Tm, TimeOverflow>,
    t: &mut i64,
) -> Result<Tm, TimeOverflow> {
    if let Ok(tm) = convert(*t) {
        return Ok(tm);
    }
    let mut bad = *t;
    let mut ok = 0i64;
    let mut oktm = None;
    // Narrow the range between BAD and OK until they differ by 1.
    loop {
        let mid = long_int_avg(ok, bad);
        if mid == ok || mid == bad {
            break;
        }
        match convert(mid) {
            Ok(tm) => {
                ok = mid;
                oktm = Some(tm);
            }
            Err(TimeOverflow) => bad = mid,
        }
    }
    let tm = oktm.ok_or(TimeOverflow)?;
    *t = ok;
    Ok(tm)
}

fn isdst_differ(a: i32, b: i32) -> bool {
    ((a == 0) != (b == 0)) && 0 <= a && 0 <= b
}

/// mktime.c:__mktime_internal: TP's time, inverting CONVERT, with *OFFSET
/// the guess carried from the previous call; TP normalized on success.
fn mktime_internal(
    tp: &mut Tm,
    convert: &dyn Fn(i64) -> Result<Tm, TimeOverflow>,
    offset: &AtomicI64,
) -> Result<i64, TimeOverflow> {
    let mut remaining_probes = 6;
    let mut sec = tp.sec;
    let (min, hour, mday, mon) = (tp.min, tp.hour, tp.mday, tp.mon);
    let year_requested = tp.year;
    let isdst = tp.isdst;
    // 1 if the previous probe was DST.
    let mut dst2 = false;
    // Ensure that mon is in range, and set year accordingly.
    let mon_remainder = mon % 12;
    let negative_mon_remainder = mon_remainder < 0;
    let mon_years = mon / 12 - i32::from(negative_mon_remainder);
    let year = i64::from(year_requested) + i64::from(mon_years);
    // The day of the year, which need not be in range.
    let mon_yday = MON_YDAY[usize::from(leapyear(year))]
        [(mon_remainder + 12 * i32::from(negative_mon_remainder)) as usize]
        - 1;
    let yday = mon_yday + i64::from(mday);
    let off = offset.load(Ordering::Relaxed);
    let sec_requested = sec;
    // Out-of-range seconds are handled specially, since ydhms_diff
    // assumes every minute has 60 seconds.
    sec = sec.clamp(0, 59);
    let negative_offset_guess = 0i64.wrapping_sub(off) as i32;
    let t0 = ydhms_diff(
        year,
        yday,
        hour,
        min,
        sec,
        1970 - 1900,
        0,
        0,
        0,
        negative_offset_guess,
    );
    let (mut t, mut t1, mut t2) = (t0, t0, t0);
    let mut tm;
    // Repeatedly use the error to improve the guess.
    let offset_found = loop {
        tm = ranged_convert(convert, &mut t)?;
        let dt = tm_diff(year, yday, hour, min, sec, &tm);
        if dt == 0 {
            break false;
        }
        if t == t1
            && t != t2
            && (tm.isdst < 0
                || if isdst < 0 {
                    i32::from(dst2) <= i32::from(tm.isdst != 0)
                } else {
                    (isdst != 0) != (tm.isdst != 0)
                })
        {
            // Oscillating between two values: the requested time is in a
            // spring-forward gap of size DT.  Return a time DT away from
            // the requested one, preferring one whose tm_isdst differs
            // from the requested value.
            break true;
        }
        remaining_probes -= 1;
        if remaining_probes == 0 {
            return Err(TimeOverflow);
        }
        t1 = t2;
        t2 = t;
        t = t.wrapping_add(dt);
        dst2 = tm.isdst != 0;
    };
    if !offset_found && isdst_differ(isdst, tm.isdst) {
        // tm.tm_isdst has the wrong value: look for a neighboring time
        // with the right value, and use its UTC offset.
        let dst_difference = i64::from(isdst == 0) - i64::from(tm.isdst == 0);
        let stride: i64 = 601_200;
        let duration_max: i64 = 457_243_209;
        let delta_bound = duration_max / 2 + stride;
        let mut found = false;
        let mut delta = stride;
        'probe: while delta < delta_bound {
            for direction in [-1i64, 1] {
                if let Some(mut ot) = t.checked_add(delta * direction) {
                    let otm = ranged_convert(convert, &mut ot)?;
                    if !isdst_differ(isdst, otm.isdst) {
                        // Extrapolate back to the desired time.
                        let gt = ot + tm_diff(year, yday, hour, min, sec, &otm);
                        if let Ok(converted) = convert(gt) {
                            tm = converted;
                            t = gt;
                            found = true;
                            break 'probe;
                        }
                    }
                }
            }
            delta += stride;
        }
        if !found {
            // No unusual DST offset nearby: assume one-hour DST.
            t += 60 * 60 * dst_difference;
            tm = convert(t)?;
        }
    }
    offset.store(
        t.wrapping_sub(t0)
            .wrapping_sub(i64::from(negative_offset_guess)),
        Ordering::Relaxed,
    );
    if sec_requested != tm.sec {
        // Reflect the requested tm_sec, not the normalized value, and
        // repair any false match due to a leap second.
        let sec_adjustment =
            i64::from(sec == 0 && tm.sec == 60) - i64::from(sec) + i64::from(sec_requested);
        t = t.checked_add(sec_adjustment).ok_or(TimeOverflow)?;
        tm = convert(t)?;
    }
    *tp = tm;
    Ok(t)
}

/// mktime.c's static localtime_offset and timegm.c's gmtime_offset.
static LOCALTIME_OFFSET: AtomicI64 = AtomicI64::new(0);
static GMTIME_OFFSET: AtomicI64 = AtomicI64::new(0);

/// time_rz.c:mktime_z: TM's time in TZ, TM normalized on success.
pub(crate) fn mktime_z(tz: &TimeZone, tm: &mut Tm) -> Result<i64, TimeOverflow> {
    match tz {
        TimeZone::Utc => {
            // timegm.c: "tmp->tm_isdst = 0".
            tm.isdst = 0;
            mktime_internal(tm, &gmtime, &GMTIME_OFFSET)
        }
        TimeZone::Tz(name) => {
            let mut tm_1 = Tm {
                yday: -1,
                ..tm.clone()
            };
            let t = mktime_internal(
                &mut tm_1,
                &|timer| glibc_localtime(name, timer),
                &LOCALTIME_OFFSET,
            )?;
            if tm_1.yday < 0 {
                return Err(TimeOverflow);
            }
            *tm = tm_1;
            Ok(t)
        }
    }
}
