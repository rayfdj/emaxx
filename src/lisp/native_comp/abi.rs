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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct NativeSubr {
    pub(crate) name: &'static str,
    pub(crate) min_args: u16,
    pub(crate) max_args: NativeMaxArgs,
}

pub(crate) fn native_subrs() -> &'static [NativeSubr] {
    super::generated_native_subrs::NATIVE_SUBRS
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
pub(crate) const HANDLER_SIZE: usize = 288;
// The exact trailing pad passed by the pinned Darwin/arm64 comp.o is 408
// bytes: 96 bytes before m_handlerlist + one pointer + 408 = 512.  The total
// is part of libgccjit's type graph even though generated code accesses only
// m_handlerlist, so an approximate opaque tail would change the artifact.
#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
pub(crate) const THREAD_STATE_SIZE: usize = 512;

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

/// The actual constants used by both the backend and runtime, exposed to
/// the independent configured-C-header audit (not a second expected table).
pub(crate) fn native_layout_contract() -> [(&'static str, usize); 8] {
    [
        ("LISP_CONS_SIZE", LISP_CONS_SIZE),
        ("SYS_JMP_BUF_SIZE", SYS_JMP_BUF_SIZE),
        ("HANDLER_VALUE_OFFSET", HANDLER_VALUE_OFFSET),
        ("HANDLER_NEXT_OFFSET", HANDLER_NEXT_OFFSET),
        ("HANDLER_JMP_OFFSET", HANDLER_JMP_OFFSET),
        ("HANDLER_SIZE", HANDLER_SIZE),
        ("THREAD_HANDLERLIST_OFFSET", THREAD_HANDLERLIST_OFFSET),
        ("THREAD_STATE_SIZE", THREAD_STATE_SIZE),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    // The pinned reference build's `(length comp-subr-list)` for each
    // supported target.
    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    const PINNED_SUBR_COUNT: usize = 1_445;
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    const PINNED_SUBR_COUNT: usize = 1_467;

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
        assert_eq!(subrs[add1].max_args, NativeMaxArgs::Fixed(1));

        let unique = subrs.iter().map(|subr| subr.name).collect::<HashSet<_>>();
        assert_eq!(unique.len(), subrs.len());
    }
}
