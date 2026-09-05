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
        assert_eq!(subrs[add1].max_args, NativeMaxArgs::Fixed(1));

        let unique = subrs.iter().map(|subr| subr.name).collect::<HashSet<_>>();
        assert_eq!(unique.len(), subrs.len());
    }
}
