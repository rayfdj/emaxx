//! Native-code ABI shared by generated `.eln` files and the Rust runtime.
//!
//! The order of the live runtime subroutine table is part of the `.eln` ABI:
//! generated code addresses a primitive through its byte offset in that
//! table.  The table is built from Emaxx's native primitive registrations,
//! at the same C/Rust boundary where GNU's `defsubr` builds it.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NativeMaxArgs {
    Fixed(u16),
    Many,
    Unevalled,
}

/// A C primitive is a statically allocated GNU `Lisp_Subr`, not an
/// interned name or a per-interpreter native handle. The prefix is the
/// configured 64-bit `lisp.h` layout, including HAVE_NATIVE_COMP fields.
///
/// The Rust extension stores a string view of the same name bytes and
/// the Rust dispatch metadata. Neither is another mutable Lisp payload.
#[repr(C, align(8))]
pub(crate) struct NativeSubr {
    header: usize,
    pub(crate) function: *const std::ffi::c_void,
    pub(crate) min_args: u16,
    max_args_word: i16,
    symbol_name: *const std::ffi::c_char,
    intspec: *const std::ffi::c_char,
    command_modes: usize,
    doc: isize,
    native_comp_u: usize,
    native_c_name: *const std::ffi::c_char,
    lambda_list: usize,
    native_type: usize,
    pub(crate) name: &'static str,
    index: u32,
    facts: std::sync::OnceLock<crate::lisp::primitives::NameFacts>,
}

// lisp.h:struct Lisp_Subr on both supported HAVE_NATIVE_COMP targets.
const _: () = {
    assert!(std::mem::offset_of!(NativeSubr, header) == 0);
    assert!(std::mem::offset_of!(NativeSubr, function) == 8);
    assert!(std::mem::offset_of!(NativeSubr, min_args) == 16);
    assert!(std::mem::offset_of!(NativeSubr, max_args_word) == 18);
    assert!(std::mem::offset_of!(NativeSubr, symbol_name) == 24);
    assert!(std::mem::offset_of!(NativeSubr, intspec) == 32);
    assert!(std::mem::offset_of!(NativeSubr, command_modes) == 40);
    assert!(std::mem::offset_of!(NativeSubr, doc) == 48);
    assert!(std::mem::offset_of!(NativeSubr, native_comp_u) == 56);
    assert!(std::mem::offset_of!(NativeSubr, native_c_name) == 64);
    assert!(std::mem::offset_of!(NativeSubr, lambda_list) == 72);
    assert!(std::mem::offset_of!(NativeSubr, native_type) == 80);
    assert!(std::mem::offset_of!(NativeSubr, name) == 88);
};

// SAFETY: the generated table owns immutable static objects. Its pointers
// name static string bytes and code, never a collector-owned allocation.
// The Lisp-valued prefix fields are the immutable nil word. Dispatch
// metadata contains only immutable descriptors and function pointers and
// is initialized through OnceLock. Invoking a function still requires the
// ordinary runtime ownership boundary; sharing a descriptor grants no
// access to another thread's interpreter.
unsafe impl Send for NativeSubr {}
// SAFETY: as above, all shared prefix data is immutable and initialization
// of the dispatch extension is synchronized by OnceLock.
unsafe impl Sync for NativeSubr {}

impl NativeSubr {
    pub(crate) const fn new(
        name: &'static std::ffi::CStr,
        min_args: u16,
        max_args: NativeMaxArgs,
        function: *const std::ffi::c_void,
        index: u32,
    ) -> Self {
        assert!(min_args <= i16::MAX as u16, "GNU subr minimum fits a short");
        if let NativeMaxArgs::Fixed(count) = max_args {
            assert!(count <= i16::MAX as u16, "GNU subr maximum fits a short");
        }
        let name_text = match std::str::from_utf8(name.to_bytes()) {
            Ok(text) => text,
            Err(_) => panic!("GNU subr names must be valid UTF-8"),
        };
        Self {
            header: (1_usize << 62) | (18 << 24),
            function,
            min_args,
            max_args_word: match max_args {
                NativeMaxArgs::Fixed(count) => count as i16,
                NativeMaxArgs::Many => -2,
                NativeMaxArgs::Unevalled => -1,
            },
            symbol_name: name.as_ptr(),
            intspec: std::ptr::null(),
            command_modes: 0,
            doc: 0,
            native_comp_u: 0,
            native_c_name: std::ptr::null(),
            lambda_list: 0,
            native_type: 0,
            name: name_text,
            index,
            facts: std::sync::OnceLock::new(),
        }
    }

    pub(crate) fn max_args(&self) -> NativeMaxArgs {
        match self.max_args_word {
            -2 => NativeMaxArgs::Many,
            -1 => NativeMaxArgs::Unevalled,
            count => NativeMaxArgs::Fixed(count as u16),
        }
    }

    pub(crate) fn facts(&'static self) -> crate::lisp::primitives::NameFacts {
        *self
            .facts
            .get_or_init(|| crate::lisp::primitives::compute_name_facts(self.name))
    }
}

/// The address of a registered, immutable C primitive. Ordinary calls
/// carry this reference, so resolving a subr never reconstructs its name
/// or consults the native runtime's object-handle maps.
#[derive(Clone, Copy)]
pub struct BuiltinRef(&'static NativeSubr);

impl BuiltinRef {
    pub(crate) fn from_subr(subr: &'static NativeSubr) -> Self {
        Self(subr)
    }

    /// # Safety
    /// ADDRESS names a registered static NativeSubr, for the process lifetime.
    pub(crate) unsafe fn from_raw(address: usize) -> Self {
        Self(unsafe { &*(address as *const NativeSubr) })
    }

    pub(crate) fn identity_ptr(self) -> usize {
        std::ptr::from_ref(self.0) as usize
    }

    pub(crate) fn id(self) -> u32 {
        self.0.index
    }

    pub fn as_str(self) -> &'static str {
        self.0.name
    }

    pub(crate) fn descriptor(self) -> &'static NativeSubr {
        self.0
    }

    pub(crate) fn facts(self) -> crate::lisp::primitives::NameFacts {
        self.0.facts()
    }

    pub(crate) fn arity_value(self) -> crate::lisp::types::Value {
        use crate::lisp::types::Value;
        let maximum = match self.0.max_args() {
            NativeMaxArgs::Fixed(count) => Value::Integer(i64::from(count)),
            NativeMaxArgs::Many => Value::symbol("many"),
            NativeMaxArgs::Unevalled => Value::symbol("unevalled"),
        };
        Value::cons(Value::Integer(i64::from(self.0.min_args)), maximum)
    }
}

impl std::ops::Deref for BuiltinRef {
    type Target = str;

    fn deref(&self) -> &str {
        self.0.name
    }
}

impl std::fmt::Display for BuiltinRef {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.name.fmt(formatter)
    }
}

impl std::fmt::Debug for BuiltinRef {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("BuiltinRef")
            .field(&self.0.name)
            .finish()
    }
}

impl PartialEq for BuiltinRef {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self.0, other.0)
    }
}

impl Eq for BuiltinRef {}

impl PartialEq<str> for BuiltinRef {
    fn eq(&self, other: &str) -> bool {
        self.0.name == other
    }
}

impl PartialEq<&str> for BuiltinRef {
    fn eq(&self, other: &&str) -> bool {
        self.0.name == *other
    }
}

impl From<&str> for BuiltinRef {
    fn from(name: &str) -> Self {
        find_builtin(name).expect("a builtin value names a registered GNU C subr")
    }
}

impl From<String> for BuiltinRef {
    fn from(name: String) -> Self {
        Self::from(name.as_str())
    }
}

impl From<crate::lisp::types::SymbolName> for BuiltinRef {
    fn from(name: crate::lisp::types::SymbolName) -> Self {
        Self::from(name.as_str())
    }
}

pub(crate) fn find_builtin(name: &str) -> Option<BuiltinRef> {
    native_subrs()
        .iter()
        .find(|subr| subr.name == name)
        .map(BuiltinRef)
}

/// Checked external-word decoding: validate the allocation start without
/// dereferencing arbitrary pointers or inventing an identity registry.
pub(crate) fn builtin_at_address(address: usize) -> Option<BuiltinRef> {
    let subrs = native_subrs();
    let offset = address.checked_sub(subrs.as_ptr() as usize)?;
    let size = std::mem::size_of::<NativeSubr>();
    if !offset.is_multiple_of(size) {
        return None;
    }
    subrs.get(offset / size).map(BuiltinRef)
}

pub(crate) fn native_subrs() -> &'static [NativeSubr] {
    &super::generated_native_subrs::NATIVE_SUBRS
}

pub(crate) const LISP_CONS_SIZE: usize = 2 * std::mem::size_of::<usize>();

// Layout facts generated code observes about GNU's `sys_jmp_buf`,
// `struct handler`, and `struct thread_state`.  `comp.c` treats the jmp
// buffer as opaque bytes, but its size fixes the handler layout embedded in
// every `.eln`, so each supported target carries values measured from the
// pinned GNU reference build for that target (`sizeof`/`offsetof` against
// its configured `lisp.h` and `thread.h`), never derived by hand.
//
// `struct handler` and `struct thread_state` change with the reference
// build's configuration: for example `HAVE_X_WINDOWS` appends an
// `x_error_handler_depth` field to the handler.  The numbers below therefore
// belong to the same pinned configuration as the generated subroutine table.
#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
pub(crate) const SYS_JMP_BUF_SIZE: usize = 192;
#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
pub(crate) const HANDLER_VALUE_OFFSET: usize = 24;
#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
pub(crate) const HANDLER_NEXT_OFFSET: usize = 32;
#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
pub(crate) const HANDLER_JMP_OFFSET: usize = 60;
#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
pub(crate) const HANDLER_SIZE: usize = 304;
// The exact trailing pad passed by the pinned Darwin/arm64 comp.o is 408
// bytes: 96 bytes before m_handlerlist + one pointer + 408 = 512.  The total
// is part of libgccjit's type graph even though generated code accesses only
// m_handlerlist, so an approximate opaque tail would change the artifact.
#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
pub(crate) const THREAD_STATE_SIZE: usize = 512;
// puresize.h PURESIZE, which comp.c's PURE_P check compares an object's
// distance from `pure' against: BASE_PURESIZE (3400000 plus the
// configuration's SYSTEM_PURESIZE_EXTRA, 200000 on Darwin/NS) times
// PURESIZE_RATIO 10/6 on 64-bit hosts, times 12/10 only under
// ENABLE_CHECKING.  The immediate lands in every `.eln', so it is measured
// from the pinned reference build's headers like the layout constants.
#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
pub(crate) const PURESIZE: i64 = 6_000_000;

// glibc's x86-64 `jmp_buf` is 200 bytes; the pinned X11/GTK reference build
// defines `HAVE_X_WINDOWS`, so `struct handler` ends with
// `x_error_handler_depth` and pads to 304 bytes, and `struct thread_state`
// is 520 bytes with `m_handlerlist` at 96.
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
pub(crate) const SYS_JMP_BUF_SIZE: usize = 200;
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
pub(crate) const HANDLER_VALUE_OFFSET: usize = 24;
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
pub(crate) const HANDLER_NEXT_OFFSET: usize = 32;
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
pub(crate) const HANDLER_JMP_OFFSET: usize = 64;
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
pub(crate) const HANDLER_SIZE: usize = 304;
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
pub(crate) const THREAD_STATE_SIZE: usize = 520;
// The pinned Linux reference build has no SYSTEM_PURESIZE_EXTRA and no
// ENABLE_CHECKING: 3400000 * 10 / 6 = 5666666 (measured from its
// puresize.h with its own compiler flags; the Darwin 6000000 produced a
// three-byte `cmp' immediate difference in every Linux `.eln' with a
// PURE_P check, comp-tests.el's `setcar' first).
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
pub(crate) const PURESIZE: i64 = 5_666_666;

#[cfg(not(any(
    all(target_os = "macos", target_arch = "aarch64"),
    all(target_os = "linux", target_arch = "x86_64")
)))]
compile_error!(
    "native compilation needs measured GNU ABI layout constants and a generated subroutine table for this target"
);

// `m_handlerlist` follows the vector header, eight Lisp_Object fields, two
// stack pointers, and the catch-list pointer in GNU's `struct thread_state`
// on every supported 64-bit target.
pub(crate) const THREAD_HANDLERLIST_OFFSET: usize = 96;

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    // The pinned reference build's `(length comp-subr-list)` for each
    // supported target.
    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    const PINNED_SUBR_COUNT: usize = 1_445;
    // The pinned Linux oracle (--with-x-toolkit=no, no dbus) registers
    // 1455 subroutines; the gtk3/cairo/dbus build the table first came
    // from had 1467.
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    const PINNED_SUBR_COUNT: usize = 1_455;

    #[test]
    fn generated_native_subr_table_has_the_pinned_gnu_registration_order() {
        let subrs = native_subrs();
        assert_eq!(subrs.len(), PINNED_SUBR_COUNT);
        assert_eq!(subrs[0].name, "json-parse-buffer");
        let add1 = subrs
            .iter()
            .position(|subr| subr.name == "1+")
            .expect("data.c registers 1+");
        assert_eq!(
            subrs
                .last()
                .expect("native subroutine table is nonempty")
                .name,
            "internal-make-lisp-face"
        );
        assert_eq!(subrs[add1].min_args, 1);
        assert_eq!(subrs[add1].max_args(), NativeMaxArgs::Fixed(1));

        let unique = subrs.iter().map(|subr| subr.name).collect::<HashSet<_>>();
        assert_eq!(unique.len(), subrs.len());
    }
}
