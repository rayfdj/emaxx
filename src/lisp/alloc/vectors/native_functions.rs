//! comp.c:make_subr and lisp.h:Lisp_Subr. Native functions own their
//! callable fields, C names and five traced Lisp fields in one allocation.

use super::{VectorTag, Vectorlike, VectorlikeRef};
use crate::lisp::types::Value;
use std::cell::Cell;
use std::ffi::{CStr, CString, c_char, c_void};

/// The GNU payload after its one-word vector header, including HAVE_NATIVE_COMP.
#[repr(C)]
#[derive(Debug)]
pub struct NativeFunctionState {
    target: Cell<*const c_void>,
    min_args: Cell<i16>,
    max_args: Cell<i16>,
    /// The argument counts a call can pass in registers alone, a bit each
    /// (`register_counts'): kept in Lisp_Subr's padding after the arity,
    /// which no C or generated code reads.
    register_counts: Cell<u8>,
    /// Whether the two names are the image's bytes (pdumper's subrs name
    /// their C strings in the dump), not this object's allocations.
    image_names: Cell<bool>,
    symbol_name: Cell<*mut c_char>,
    interactive: Cell<Value>,
    command_modes: Cell<Value>,
    doc: Cell<isize>,
    unit: Cell<Value>,
    c_name: Cell<*mut c_char>,
    lambda_list: Cell<Value>,
    native_type: Cell<Value>,
}

pub type NativeFunctionRef = VectorlikeRef<NativeFunctionState>;

/// Construction data, consumed once; no signature or name registry survives it.
pub(crate) struct NativeFunctionSpec {
    pub(crate) target: *const c_void,
    pub(crate) min_args: i16,
    pub(crate) max_args: i16,
    pub(crate) name: CString,
    pub(crate) c_name: CString,
    pub(crate) doc: isize,
    pub(crate) fields: [Value; 5],
}

const _: () = {
    assert!(size_of::<NativeFunctionState>() == 80);
    assert!(std::mem::offset_of!(NativeFunctionState, target) == 0);
    assert!(std::mem::offset_of!(NativeFunctionState, min_args) == 8);
    assert!(std::mem::offset_of!(NativeFunctionState, max_args) == 10);
    assert!(std::mem::offset_of!(NativeFunctionState, register_counts) == 12);
    assert!(std::mem::offset_of!(NativeFunctionState, image_names) == 13);
    assert!(std::mem::offset_of!(NativeFunctionState, symbol_name) == 16);
    assert!(std::mem::offset_of!(NativeFunctionState, interactive) == 24);
    assert!(std::mem::offset_of!(NativeFunctionState, command_modes) == 32);
    assert!(std::mem::offset_of!(NativeFunctionState, doc) == 40);
    assert!(std::mem::offset_of!(NativeFunctionState, unit) == 48);
    assert!(std::mem::offset_of!(NativeFunctionState, c_name) == 56);
    assert!(std::mem::offset_of!(NativeFunctionState, lambda_list) == 64);
    assert!(std::mem::offset_of!(NativeFunctionState, native_type) == 72);
};

impl Vectorlike for NativeFunctionState {
    const TAG: VectorTag = VectorTag::Subr;
    // The Lisp fields are interspersed with C data. alloc.c marks them explicitly.
}

impl NativeFunctionRef {
    pub(crate) fn new(spec: NativeFunctionSpec) -> Self {
        crate::lisp::native_comp::note_lisp_allocation(88);
        let function = Self::allocate(NativeFunctionState {
            target: Cell::new(spec.target),
            min_args: Cell::new(spec.min_args),
            max_args: Cell::new(spec.max_args),
            register_counts: Cell::new(0),
            image_names: Cell::new(false),
            symbol_name: Cell::new(spec.name.into_raw()),
            interactive: Cell::new(spec.fields[0]),
            command_modes: Cell::new(spec.fields[1]),
            doc: Cell::new(spec.doc),
            unit: Cell::new(spec.fields[2]),
            c_name: Cell::new(spec.c_name.into_raw()),
            lambda_list: Cell::new(spec.fields[3]),
            native_type: Cell::new(spec.fields[4]),
        });
        function.refresh_register_counts();
        function
    }

    /// A record the image loader fills: no target and no names yet.
    pub(crate) fn blank() -> Self {
        crate::lisp::native_comp::note_lisp_allocation(88);
        let function = Self::allocate(NativeFunctionState {
            target: Cell::new(std::ptr::null()),
            min_args: Cell::new(0),
            max_args: Cell::new(0),
            register_counts: Cell::new(0),
            image_names: Cell::new(false),
            symbol_name: Cell::new(std::ptr::null_mut()),
            interactive: Cell::new(Value::Nil),
            command_modes: Cell::new(Value::Nil),
            doc: Cell::new(0),
            unit: Cell::new(Value::Nil),
            c_name: Cell::new(std::ptr::null_mut()),
            lambda_list: Cell::new(Value::Nil),
            native_type: Cell::new(Value::Nil),
        });
        function.refresh_register_counts();
        function
    }
}

impl NativeFunctionState {
    #[inline]
    pub(crate) fn target(&self) -> *const c_void {
        self.target.get()
    }
    pub(crate) fn set_target(&self, target: *const c_void) {
        self.target.set(target);
        self.refresh_register_counts();
    }
    #[inline]
    pub(crate) fn min_args(&self) -> i16 {
        self.min_args.get()
    }
    #[inline]
    pub(crate) fn max_args_word(&self) -> i16 {
        self.max_args.get()
    }
    pub(crate) fn set_arity(&self, minimum: i16, maximum: i16) {
        self.min_args.set(minimum);
        self.max_args.set(maximum);
        self.refresh_register_counts();
    }

    /// Bit N set when a call with N arguments may go straight to the
    /// target in registers: a target, lexical binding (no lambda list to
    /// bind), and N within an arity of at most six.  The callers' arity
    /// tests in one: `call_native_nested' and `call_fixed_function'.
    #[inline(always)]
    pub(crate) fn register_counts(&self) -> u8 {
        self.register_counts.get()
    }

    fn refresh_register_counts(&self) {
        let minimum = self.min_args.get();
        let maximum = self.max_args.get();
        let mut counts = 0u8;
        // MANY and UNEVALLED (negative) take no register call, and a
        // negative minimum, which no fixed arity has, admits no count.
        if !self.target.get().is_null()
            && !self.is_dynamic()
            && (0..=6).contains(&maximum)
            && minimum >= 0
        {
            for count in minimum..=maximum {
                counts |= 1 << count;
            }
        }
        self.register_counts.set(counts);
    }
    pub(crate) fn doc_index(&self) -> isize {
        self.doc.get()
    }
    pub(crate) fn set_doc_index(&self, doc: isize) {
        self.doc.set(doc);
    }
    pub(crate) fn name(&self) -> &str {
        name_text(self.symbol_name.get())
    }
    pub(crate) fn c_name(&self) -> &str {
        name_text(self.c_name.get())
    }
    /// The C name with its NUL, as dlsym takes it.
    pub(crate) fn c_name_c_str(&self) -> &CStr {
        let name = self.c_name.get();
        if name.is_null() {
            return c"";
        }
        // SAFETY: as `name_text'.
        unsafe { CStr::from_ptr(name) }
    }
    /// Name a loaded record by the image's own NUL-terminated bytes, as
    /// pdumper.c leaves a dumped subr's `symbol_name' and `native_c_name'
    /// pointing into the dump.
    ///
    /// # Safety
    /// NAME and C_NAME are UTF-8 C strings without interior NULs that live
    /// as long as the process and never change.
    pub(crate) unsafe fn set_image_names(&mut self, name: *const c_char, c_name: *const c_char) {
        // SAFETY: exclusive access: no borrowed view of the old names
        // outlives this.
        unsafe { self.release_names() };
        self.symbol_name.set(name.cast_mut());
        self.c_name.set(c_name.cast_mut());
        self.image_names.set(true);
    }

    /// # Safety
    /// The names are not used again until replaced.
    unsafe fn release_names(&self) {
        if !self.image_names.replace(false) {
            // SAFETY: owned names are null or came from CString::into_raw
            // exactly once.
            unsafe {
                release_name(self.symbol_name.replace(std::ptr::null_mut()));
                release_name(self.c_name.replace(std::ptr::null_mut()));
            }
        }
    }
    #[inline]
    pub(crate) fn unit(&self) -> Value {
        self.unit.get()
    }
    #[inline]
    pub(crate) fn lambda_list(&self) -> Value {
        self.lambda_list.get()
    }
    #[inline]
    pub(crate) fn is_dynamic(&self) -> bool {
        self.lambda_list.get().is_truthy()
    }
    pub(crate) fn interactive(&self) -> Value {
        self.interactive.get()
    }
    pub(crate) fn command_modes(&self) -> Value {
        self.command_modes.get()
    }
    pub(crate) fn native_type(&self) -> Value {
        self.native_type.get()
    }
    pub(crate) fn arity_value(&self) -> Value {
        Value::cons(
            Value::Integer(i64::from(self.min_args())),
            if self.max_args_word() < 0 {
                Value::symbol("many")
            } else {
                Value::Integer(i64::from(self.max_args_word()))
            },
        )
    }
    pub(crate) fn fields(&self) -> [Value; 5] {
        [
            self.interactive.get(),
            self.command_modes.get(),
            self.unit.get(),
            self.lambda_list.get(),
            self.native_type.get(),
        ]
    }
    pub(crate) fn set_fields(&self, fields: [Value; 5]) {
        self.interactive.set(fields[0]);
        self.command_modes.set(fields[1]);
        self.unit.set(fields[2]);
        self.lambda_list.set(fields[3]);
        self.native_type.set(fields[4]);
        self.refresh_register_counts();
    }
}

impl Drop for NativeFunctionState {
    fn drop(&mut self) {
        // alloc.c:cleanup_vector releases both C names when the subr dies.
        // SAFETY: the object is gone after this; image names are not
        // released.
        unsafe { self.release_names() };
    }
}

/// A name's text: empty while a loaded record is still unnamed.
fn name_text<'a>(name: *mut c_char) -> &'a str {
    if name.is_null() {
        return "";
    }
    // SAFETY: a live object's NUL-terminated name, a CString made from a
    // Rust string or the image's bytes, which `set_image_names' requires
    // be UTF-8 (the loader checks them); no native path changes names.
    let bytes = unsafe { CStr::from_ptr(name) }.to_bytes();
    debug_assert!(std::str::from_utf8(bytes).is_ok(), "native name UTF-8");
    unsafe { std::str::from_utf8_unchecked(bytes) }
}

/// # Safety
/// NAME is null or came from CString::into_raw, and is released once.
unsafe fn release_name(name: *mut c_char) {
    if !name.is_null() {
        drop(unsafe { CString::from_raw(name) });
    }
}
