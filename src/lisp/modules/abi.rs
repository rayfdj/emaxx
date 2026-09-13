//! The append-only public layout from GNU Emacs 30's emacs-module.h.
//! This is the foreign ABI, independent of Emaxx's internal Value layout.
use super::*;

pub(super) type Handle = *mut c_void;
pub(super) type Finalizer = Option<unsafe extern "C" fn(*mut c_void)>;
pub(super) type Function =
    unsafe extern "C" fn(*mut ModuleEnv, isize, *mut Handle, *mut c_void) -> Handle;

#[repr(C)]
pub(super) struct Runtime {
    pub size: isize,
    pub private_members: *mut ModuleEnv,
    pub get_environment: unsafe extern "C" fn(*mut Runtime) -> *mut ModuleEnv,
}

macro_rules! environment {
    ($($name:ident ($($arg:ty),*) -> $ret:ty;)+) => {
        #[repr(C)]
        pub(super) struct ModuleEnv {
            pub size: isize,
            pub private_members: *mut Activation,
            $(pub $name: unsafe extern "C" fn(*mut ModuleEnv, $($arg),*) -> $ret,)+
        }
        impl ModuleEnv {
            pub fn new() -> Self {
                Self {
                    size: std::mem::size_of::<Self>() as isize,
                    private_members: std::ptr::null_mut(),
                    $($name,)+
                }
            }
        }
    };
}

environment! {
    make_global_ref(Handle) -> Handle;
    free_global_ref(Handle) -> ();
    non_local_exit_check() -> c_int;
    non_local_exit_clear() -> ();
    non_local_exit_get(*mut Handle, *mut Handle) -> c_int;
    non_local_exit_signal(Handle, Handle) -> ();
    non_local_exit_throw(Handle, Handle) -> ();
    make_function(isize, isize, Function, *const c_char, *mut c_void) -> Handle;
    funcall(Handle, isize, *mut Handle) -> Handle;
    intern(*const c_char) -> Handle;
    type_of(Handle) -> Handle;
    is_not_nil(Handle) -> bool;
    eq(Handle, Handle) -> bool;
    extract_integer(Handle) -> i64;
    make_integer(i64) -> Handle;
    extract_float(Handle) -> f64;
    make_float(f64) -> Handle;
    copy_string_contents(Handle, *mut c_char, *mut isize) -> bool;
    make_string(*const c_char, isize) -> Handle;
    make_user_ptr(Finalizer, *mut c_void) -> Handle;
    get_user_ptr(Handle) -> *mut c_void;
    set_user_ptr(Handle, *mut c_void) -> ();
    get_user_finalizer(Handle) -> Finalizer;
    set_user_finalizer(Handle, Finalizer) -> ();
    vec_get(Handle, isize) -> Handle;
    vec_set(Handle, isize, Handle) -> ();
    vec_size(Handle) -> isize;
    should_quit() -> bool;
    process_input() -> c_int;
    extract_time(Handle) -> libc::timespec;
    make_time(libc::timespec) -> Handle;
    extract_big_integer(Handle, *mut c_int, *mut isize, *mut usize) -> bool;
    make_big_integer(c_int, isize, *const usize) -> Handle;
    get_function_finalizer(Handle) -> Finalizer;
    set_function_finalizer(Handle, Finalizer) -> ();
    open_channel(Handle) -> c_int;
    make_interactive(Handle, Handle) -> ();
    make_unibyte_string(*const c_char, isize) -> Handle;
}
