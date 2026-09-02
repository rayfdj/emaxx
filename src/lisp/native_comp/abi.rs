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

// These are the platform C ABI sizes used by GNU's `sys_jmp_buf` typedef.
// `comp.c` deliberately treats the buffer as opaque bytes but its exact size
// determines the handler layout embedded in generated code.
#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
pub(crate) const SYS_JMP_BUF_SIZE: usize = 192;
#[cfg(all(target_os = "macos", target_arch = "x86_64"))]
pub(crate) const SYS_JMP_BUF_SIZE: usize = 148;
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
pub(crate) const SYS_JMP_BUF_SIZE: usize = 200;
#[cfg(all(target_os = "linux", target_arch = "aarch64"))]
pub(crate) const SYS_JMP_BUF_SIZE: usize = 312;

#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
pub(crate) const HANDLER_VALUE_OFFSET: usize = 24;
#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
pub(crate) const HANDLER_NEXT_OFFSET: usize = 32;
#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
pub(crate) const HANDLER_JMP_OFFSET: usize = 60;
#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
pub(crate) const HANDLER_SIZE: usize = 304;

#[cfg(all(target_os = "macos", target_arch = "x86_64"))]
pub(crate) const HANDLER_VALUE_OFFSET: usize = 24;
#[cfg(all(target_os = "macos", target_arch = "x86_64"))]
pub(crate) const HANDLER_NEXT_OFFSET: usize = 32;
#[cfg(all(target_os = "macos", target_arch = "x86_64"))]
pub(crate) const HANDLER_JMP_OFFSET: usize = 60;
#[cfg(all(target_os = "macos", target_arch = "x86_64"))]
pub(crate) const HANDLER_SIZE: usize = 256;

#[cfg(any(
    all(target_os = "linux", target_arch = "x86_64"),
    all(target_os = "linux", target_arch = "aarch64")
))]
pub(crate) const HANDLER_VALUE_OFFSET: usize = 24;
#[cfg(any(
    all(target_os = "linux", target_arch = "x86_64"),
    all(target_os = "linux", target_arch = "aarch64")
))]
pub(crate) const HANDLER_NEXT_OFFSET: usize = 32;
#[cfg(any(
    all(target_os = "linux", target_arch = "x86_64"),
    all(target_os = "linux", target_arch = "aarch64")
))]
pub(crate) const HANDLER_JMP_OFFSET: usize = 64;
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
pub(crate) const HANDLER_SIZE: usize = 312;
#[cfg(all(target_os = "linux", target_arch = "aarch64"))]
pub(crate) const HANDLER_SIZE: usize = 424;

// `m_handlerlist` follows the vector header, eight Lisp_Object fields, two
// stack pointers, and the catch-list pointer in GNU's `struct thread_state`.
pub(crate) const THREAD_HANDLERLIST_OFFSET: usize = 96;

// The exact trailing pad passed by the pinned Darwin/arm64 comp.o is 408
// bytes: 96 bytes before m_handlerlist + one pointer + 408 = 512.  The total
// is part of libgccjit's type graph even though generated code accesses only
// m_handlerlist, so an approximate opaque tail would change the artifact.
#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
pub(crate) const THREAD_STATE_SIZE: usize = 512;

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn generated_native_subr_table_has_the_pinned_gnu_registration_order() {
        let subrs = native_subrs();
        assert_eq!(subrs.len(), 1_445);
        assert_eq!(subrs[0].name, "json-parse-buffer");
        assert_eq!(subrs[1_256].name, "1+");
        assert_eq!(
            subrs
                .last()
                .expect("native subroutine table is nonempty")
                .name,
            "internal-make-lisp-face"
        );
        assert_eq!(subrs[1_256].min_args, 1);
        assert_eq!(subrs[1_256].max_args, NativeMaxArgs::Fixed(1));

        let unique = subrs.iter().map(|subr| subr.name).collect::<HashSet<_>>();
        assert_eq!(unique.len(), subrs.len());
    }
}
