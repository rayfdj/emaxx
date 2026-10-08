//! comp.h:Lisp_Native_Comp_Unit: seven traced Lisp words and the actual
//! relocation pointer, load flags and dynamic-library handle.

use super::{VectorHeader, VectorTag, Vectorlike, VectorlikeRef};
use crate::lisp::types::Value;
use libloading::Library;
use std::cell::Cell;
use std::ffi::c_void;
use std::mem::ManuallyDrop;

#[repr(C)]
#[derive(Debug)]
pub struct NativeUnitState {
    fields: [Cell<Value>; 7],
    impure_relocations: Cell<*mut usize>,
    pub(crate) loaded_once: Cell<bool>,
    pub(crate) load_ongoing: Cell<bool>,
    handle: Cell<*mut c_void>,
}

pub type NativeUnitRef = VectorlikeRef<NativeUnitState>;

const _: () = {
    assert!(size_of::<NativeUnitState>() == 80);
    assert!(std::mem::offset_of!(NativeUnitState, fields) == 0);
    assert!(std::mem::offset_of!(NativeUnitState, impure_relocations) == 56);
    assert!(std::mem::offset_of!(NativeUnitState, loaded_once) == 64);
    assert!(std::mem::offset_of!(NativeUnitState, load_ongoing) == 65);
    assert!(std::mem::offset_of!(NativeUnitState, handle) == 72);
};

impl Vectorlike for NativeUnitState {
    const TAG: VectorTag = VectorTag::NativeCompUnit;
    const LISP_SLOTS: usize = 7;
}

impl NativeUnitRef {
    pub(crate) fn new() -> Self {
        crate::lisp::native_comp::note_lisp_allocation(88);
        Self::allocate(NativeUnitState {
            fields: [const { Cell::new(Value::Nil) }; 7],
            impure_relocations: Cell::new(std::ptr::null_mut()),
            loaded_once: Cell::new(false),
            load_ongoing: Cell::new(false),
            handle: Cell::new(std::ptr::null_mut()),
        })
    }
}

impl NativeUnitState {
    pub(crate) fn field(&self, index: usize) -> Value {
        self.fields[index].get()
    }
    pub(crate) fn set_field(&self, index: usize, value: Value) {
        self.fields[index].set(value);
    }
    pub(crate) fn fields(&self) -> [Value; 7] {
        std::array::from_fn(|index| self.field(index))
    }
    pub(crate) fn set_fields(&self, fields: [Value; 7]) {
        for (slot, value) in self.fields.iter().zip(fields) {
            slot.set(value);
        }
    }
    pub(crate) fn is_loaded(&self) -> bool {
        !self.handle.get().is_null()
    }
    pub(crate) fn install_library(&self, library: Library, impure_relocations: *mut usize) {
        assert!(!self.is_loaded(), "one owned dlopen handle per native unit");
        self.impure_relocations.set(impure_relocations);
        self.handle
            .set(libloading::os::unix::Library::from(library).into_raw());
    }
    /// A non-owning libloading view. The rooted unit, not this temporary
    /// wrapper, owns the dlopen reference. No callback can change its handle.
    pub(crate) fn library(&self) -> Option<ManuallyDrop<Library>> {
        let handle = self.handle.get();
        if handle.is_null() {
            None
        } else {
            // SAFETY: install_library consumed a live dlopen reference; the
            // collector closes it only after this unit is unreachable. The
            // ManuallyDrop view must never close the unit's reference.
            Some(ManuallyDrop::new(unsafe {
                libloading::os::unix::Library::from_raw(handle).into()
            }))
        }
    }
    pub(crate) fn impure_relocations(&self) -> *mut usize {
        self.impure_relocations.get()
    }
    pub(crate) fn impure_relocation_count(&self) -> usize {
        match self.field(6).kind() {
            crate::lisp::types::Kind::Vector(vector) => vector.len(),
            _ => 0,
        }
    }
}

impl NativeUnitState {
    fn unload(&self) {
        let handle = self.handle.replace(std::ptr::null_mut());
        if handle.is_null() {
            return;
        }
        // comp.c:unload_comp_unit. The GC calls this after tracing proves
        // no live function, loader frame or Lisp root reaches the unit.
        // Host-runtime teardown also calls it before dropping relocation
        // cells, just as the previous owning Library's destructor did.
        unsafe {
            let library = libloading::os::unix::Library::from_raw(handle);
            if let Ok(saved) = library.get::<*mut usize>(b"comp_unit\0") {
                let header = (std::ptr::from_ref(self) as *const u8)
                    .sub(size_of::<VectorHeader>())
                    .cast_mut()
                    .cast::<VectorHeader>();
                let word = Value::NativeCompUnit(NativeUnitRef::from_raw(header)).word();
                if !saved.is_null() && std::ptr::read(*saved) == word {
                    std::ptr::write(*saved, Value::Nil.word());
                }
            }
            drop(library);
        }
    }

    /// Emaxx can tear down an editor without ending its host process. Close
    /// the units whose actual ELN relocation points into that editor's native
    /// runtime before its stable thread cell is freed. No ownership id or
    /// weak handle registry is needed, and ordinary calls pay no lookup.
    pub(crate) fn belongs_to_runtime(&self, thread_cell: *const c_void) -> bool {
        let Some(library) = self.library() else {
            return false;
        };
        // SAFETY: this is the exact pointer field installed by comp.c's
        // first-load path. Inspection does not run Lisp or collect.
        unsafe {
            library
                .get::<*mut *mut c_void>(b"current_thread_reloc\0")
                .is_ok_and(|reloc| {
                    !reloc.is_null()
                        && std::ptr::eq(std::ptr::read(*reloc).cast_const(), thread_cell)
                })
        }
    }

    pub(crate) fn close_for_runtime(&self, thread_cell: *mut c_void) {
        if self.belongs_to_runtime(thread_cell) {
            self.unload();
        }
    }
}

impl Drop for NativeUnitState {
    fn drop(&mut self) {
        self.unload();
    }
}
