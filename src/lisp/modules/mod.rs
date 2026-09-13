//! GNU's public module API over Rust-owned Lisp values.
//!
//! Each foreign call has an activation and scoped value handles. Global
//! references retain their values independently; functions and user pointers
//! use the interpreter's typed pseudovector arena and its reachability pass.
//! No Rust representation is exposed through emacs_value.
mod abi;
mod callbacks;
#[cfg(test)]
mod tests;

use super::eval::{Interpreter, MarkedIds, RecordKind};
use super::primitives;
use super::types::{Env, LispError, Value};
use abi::*;
use callbacks::*;
use libloading::Library;
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::ffi::{CStr, c_char, c_int, c_void};
use std::rc::Rc;

#[derive(Clone, Copy, Debug)]
pub(crate) struct ModuleFunction {
    pub min: isize,
    pub max: isize,
    function: Function,
    data: *mut c_void,
    finalizer: Finalizer,
}

#[derive(Clone, Copy, Debug)]
struct UserPointer {
    data: *mut c_void,
    finalizer: Finalizer,
}

#[derive(Default)]
pub(crate) struct ModuleState {
    pub assertions: bool,
    pub functions: HashMap<u64, ModuleFunction>,
    pointers: HashMap<u64, UserPointer>,
    // Boxes keep the opaque addresses stable when these maps grow.
    values: HashMap<usize, Box<Value>>,
    globals: HashMap<usize, usize>,
    // These allocations retain their addresses, not just their contents:
    // assertion mode must distinguish expired handles from later objects.
    #[allow(clippy::vec_box)]
    retired_values: Vec<Box<Value>>,
    #[allow(clippy::vec_box)]
    retired_envs: Vec<Box<ModuleEnv>>,
    #[allow(clippy::vec_box)]
    retired_runtimes: Vec<Box<Runtime>>,
    libraries: Vec<Rc<Library>>,
}

impl std::fmt::Debug for ModuleState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ModuleState")
            .field("functions", &self.functions.len())
            .finish_non_exhaustive()
    }
}

impl Clone for ModuleState {
    fn clone(&self) -> Self {
        // Interpreter templates precede module loading. A live foreign
        // activation or global handle cannot be copied into another runtime.
        assert!(
            self.libraries.is_empty(),
            "cannot clone a live module runtime"
        );
        Self {
            assertions: self.assertions,
            ..Self::default()
        }
    }
}

impl ModuleState {
    pub fn roots(&self) -> impl Iterator<Item = &Value> {
        self.values.values().map(Box::as_ref)
    }

    pub fn global_root(&self) -> Option<&Value> {
        self.globals
            .keys()
            .next()
            .map(|key| self.values[key].as_ref())
    }

    fn allocate(&mut self, value: Value) -> Handle {
        let mut value = Box::new(value);
        let handle = (&mut *value as *mut Value).cast();
        self.values.insert(handle as usize, value);
        handle
    }

    fn release(&mut self, handle: usize) {
        if let Some(mut value) = self.values.remove(&handle)
            && self.assertions
        {
            // Assertion mode never reuses a handle address, as required
            // to distinguish a stale local from a new live value.
            *value = Value::Nil;
            self.retired_values.push(value);
        }
    }

    pub fn collect(&mut self, live: &MarkedIds) {
        let mut finalizers = Vec::new();
        self.functions.retain(|id, function| {
            if live.contains(id) {
                return true;
            }
            if let Some(finalizer) = function.finalizer {
                finalizers.push((finalizer, function.data));
            }
            false
        });
        self.pointers.retain(|id, pointer| {
            if live.contains(id) {
                return true;
            }
            if let Some(finalizer) = pointer.finalizer {
                finalizers.push((finalizer, pointer.data));
            }
            false
        });
        MODULE_GC.with(|flag| {
            let previous = flag.replace(true);
            for (finalizer, data) in finalizers {
                // SAFETY: the supplying library is retained by this state;
                // the module owns DATA and its finalizer's implementation.
                unsafe { finalizer(data) };
            }
            flag.set(previous);
        });
    }
}

thread_local! {
    // Register before entering foreign code, remove on return. Looking up
    // before dereferencing also rejects callbacks from a foreign OS thread.
    static ACTIVE_ENVS: RefCell<HashMap<usize, *const Activation>> = RefCell::new(HashMap::new());
    static ACTIVE_RUNTIMES: RefCell<HashMap<usize, *mut ModuleEnv>> = RefCell::new(HashMap::new());
    static MODULE_GC: Cell<bool> = const { Cell::new(false) };
}

struct Activation {
    interpreter: *mut Interpreter,
    environment: *mut Env,
    public: Box<ModuleEnv>,
    locals: RefCell<Vec<usize>>,
    pending: RefCell<Option<LispError>>,
    exit_values: Cell<(Handle, Handle)>,
}

impl Activation {
    fn new(interpreter: &mut Interpreter, environment: &mut Env) -> Rc<Self> {
        let activation = Rc::new(Self {
            interpreter,
            environment,
            public: Box::new(ModuleEnv::new()),
            locals: RefCell::new(Vec::new()),
            pending: RefCell::new(None),
            exit_values: Cell::new((std::ptr::null_mut(), std::ptr::null_mut())),
        });
        ACTIVE_ENVS.with_borrow_mut(|envs| {
            envs.insert(activation.public_ptr() as usize, Rc::as_ptr(&activation));
        });
        activation
    }

    fn public_ptr(&self) -> *mut ModuleEnv {
        // Public API tables are read-only to the module, despite the C API
        // accepting a non-const pointer. All mutable state is Rust-owned.
        (&*self.public as *const ModuleEnv).cast_mut()
    }
}

/// Each API invocation has its own context. A module may call Lisp which
/// reenters the module using the same environment: only the activation is
/// shared, and none of its RefCell borrows cross a Lisp or foreign call.
struct Context<'a>(&'a Activation);

impl std::ops::Deref for Context<'_> {
    type Target = Activation;

    fn deref(&self) -> &Activation {
        self.0
    }
}

impl Context<'_> {
    fn interpreter(&self) -> &Interpreter {
        // SAFETY: the activation cannot outlive its caller's interpreter.
        unsafe { &*self.interpreter }
    }

    fn interpreter_mut(&mut self) -> &mut Interpreter {
        // SAFETY: callbacks run synchronously on the interpreter thread.
        // No state-map borrow is kept while calling Lisp or foreign code.
        unsafe { &mut *self.interpreter }
    }

    fn value(&self, handle: Handle) -> Value {
        self.interpreter()
            .modules
            .values
            .get(&(handle as usize))
            .map(|value| (**value).clone())
            .unwrap_or_else(|| {
                let count = self.interpreter().modules.values.len();
                let environments = ACTIVE_ENVS.with_borrow(HashMap::len);
                abort(&format!(
                    "Emacs value not found in {count} values of {environments} environments"
                ))
            })
    }

    fn make(&mut self, value: Value) -> Handle {
        let handle = self.interpreter_mut().modules.allocate(value);
        self.locals.borrow_mut().push(handle as usize);
        handle
    }

    fn primitive(&mut self, name: &str, args: &[Value]) -> Result<Value, LispError> {
        // SAFETY: both pointers are the current activation's exclusive call
        // context. They are only borrowed during this synchronous operation.
        unsafe { primitives::call(&mut *self.interpreter, name, args, &mut *self.environment) }
    }

    fn set_pending(&mut self, error: LispError) {
        if self.pending.borrow().is_some() {
            return;
        }
        let (symbol, data) = match &error {
            LispError::Throw(tag, value) => (tag.clone(), value.clone()),
            _ => {
                let condition = super::eval::error_condition_value(&error);
                (
                    condition.car().unwrap_or(Value::Nil),
                    condition.cdr().unwrap_or(Value::Nil),
                )
            }
        };
        let exit_values = (self.make(symbol), self.make(data));
        self.exit_values.set(exit_values);
        *self.pending.borrow_mut() = Some(error);
    }

    fn exit_kind(&self) -> c_int {
        match *self.pending.borrow() {
            None => 0,
            Some(LispError::Throw(_, _)) => 2,
            Some(_) => 1,
        }
    }

    fn finish(&mut self, result: Handle) -> Result<Value, LispError> {
        // GNU checks quits before the module's saved signal/throw.
        unsafe {
            (&mut *self.interpreter).maybe_quit(&mut *self.environment)?;
        }
        self.propagate_exit()?;
        Ok(self.value(result))
    }

    fn propagate_exit(&mut self) -> Result<(), LispError> {
        let pending = self.pending.borrow_mut().take();
        match pending {
            Some(LispError::Throw(tag, value)) => {
                // GNU rethrows through Fthrow after leaving the foreign
                // callback's catch-all boundary, including no-catch checking.
                unsafe {
                    (&mut *self.interpreter).throw_value(tag, value, &mut *self.environment)?;
                }
                Ok(())
            }
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

impl Drop for Activation {
    fn drop(&mut self) {
        let public = self.public_ptr() as usize;
        ACTIVE_ENVS.with_borrow_mut(|envs| {
            envs.remove(&public);
        });
        let locals = self.locals.take();
        // SAFETY: all foreign calls have returned before this activation
        // is dropped; the caller's interpreter is still live.
        let modules = &mut unsafe { &mut *self.interpreter }.modules;
        for handle in locals {
            modules.release(handle);
        }
        if modules.assertions {
            let mut public = std::mem::replace(&mut self.public, Box::new(ModuleEnv::new()));
            public.private_members = std::ptr::null_mut();
            modules.retired_envs.push(public);
        }
    }
}

fn abort(message: &str) -> ! {
    eprintln!("Emacs module assertion: {message}");
    std::process::abort()
}

fn activation(env: *mut ModuleEnv) -> *const Activation {
    if MODULE_GC.get() {
        abort("Module function called during garbage collection");
    }
    ACTIVE_ENVS
        .with_borrow(|envs| envs.get(&(env as usize)).copied())
        .unwrap_or_else(|| {
            abort("Module function called outside a live environment on the current Lisp thread")
        })
}

fn api<T: Copy>(
    env: *mut ModuleEnv,
    default: T,
    body: impl FnOnce(&mut Context<'_>) -> Result<T, LispError>,
) -> T {
    let activation = activation(env);
    // SAFETY: checked against the thread-local live activation registry.
    let mut context = Context(unsafe { &*activation });
    if context.pending.borrow().is_some() {
        return default;
    }
    // Like GNU's CATCHER_ALL, this handler belongs to the current Lisp
    // thread. A module may suspend it by calling Lisp's thread-yield.
    let handlers = context.interpreter_mut().push_module_handler();
    // A Rust panic must never unwind through the public C ABI.
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| body(&mut context)));
    context.interpreter_mut().pop_handler_bindings(handlers);
    match result {
        Ok(Ok(value)) => value,
        Ok(Err(error)) => {
            context.set_pending(error);
            default
        }
        Err(_) => abort("Rust panic in module API callback"),
    }
}

fn condition(name: &str, values: impl IntoIterator<Item = Value>) -> LispError {
    LispError::SignalValue(Value::list(
        std::iter::once(Value::symbol(name)).chain(values),
    ))
}

unsafe extern "C" fn get_environment(runtime: *mut Runtime) -> *mut ModuleEnv {
    ACTIVE_RUNTIMES
        .with_borrow(|runtimes| runtimes.get(&(runtime as usize)).copied())
        .unwrap_or_else(|| abort("Runtime pointer is not live"))
}

pub(crate) fn load(
    interpreter: &mut Interpreter,
    file: &Value,
    environment: &mut Env,
) -> Result<Value, LispError> {
    let path = primitives::string_like(file)
        .ok_or_else(|| primitives::wrong_type_argument("stringp", file.clone()))?
        .text;
    // dynlib.c uses global symbols for modules, unlike native Lisp units.
    // SAFETY: module-load explicitly loads caller-selected native code.
    #[cfg(unix)]
    let library = unsafe {
        libloading::os::unix::Library::open(Some(&path), libc::RTLD_LAZY | libc::RTLD_GLOBAL)
            .map(Library::from)
    };
    #[cfg(not(unix))]
    let library = unsafe { Library::new(&path) };
    let library = Rc::new(library.map_err(|error| {
        condition(
            "module-open-failed",
            [file.clone(), Value::string(&error.to_string())],
        )
    })?);
    // Fmodule_load retains every opened library, even when a later GPL,
    // entry-point or initialization check signals. Its symbols stay visible.
    interpreter.modules.libraries.push(Rc::clone(&library));
    // SAFETY: symbol types are the documented emacs-module.h declarations.
    let init = unsafe {
        library
            .get::<*mut c_void>(b"plugin_is_GPL_compatible")
            .map_err(|_| condition("module-not-gpl-compatible", [file.clone()]))?;
        *library
            .get::<unsafe extern "C" fn(*mut Runtime) -> c_int>(b"emacs_module_init")
            .map_err(|_| condition("missing-module-init-function", [file.clone()]))?
    };
    let activation = Activation::new(interpreter, environment);
    let mut runtime = Box::new(Runtime {
        size: std::mem::size_of::<Runtime>() as isize,
        private_members: activation.public_ptr(),
        get_environment,
    });
    let runtime_ptr = &mut *runtime as *mut Runtime;
    ACTIVE_RUNTIMES.with_borrow_mut(|runtimes| {
        runtimes.insert(runtime_ptr as usize, activation.public_ptr());
    });
    // SAFETY: the public runtime/environment and every handle remain live
    // throughout initialization. The foreign library implements its ABI.
    let status = unsafe { init(runtime_ptr) };
    ACTIVE_RUNTIMES.with_borrow_mut(|runtimes| {
        runtimes.remove(&(runtime_ptr as usize));
    });
    let mut context = Context(&activation);
    if context.interpreter().modules.assertions {
        context
            .interpreter_mut()
            .modules
            .retired_runtimes
            .push(runtime);
    }
    // SAFETY: foreign initialization has returned. Borrow the caller through
    // the activation's pointers, which are also used by synchronous callbacks.
    unsafe {
        (&mut *context.interpreter).maybe_quit(&mut *context.environment)?;
    }
    if status != 0 {
        return Err(condition(
            "module-init-failed",
            [file.clone(), Value::Integer(status as i64)],
        ));
    }
    context.propagate_exit()?;
    Ok(Value::T)
}

pub(crate) fn call(
    interpreter: &mut Interpreter,
    environment: &mut Env,
    id: u64,
    args: &[Value],
) -> Result<Value, LispError> {
    let function = *interpreter
        .modules
        .functions
        .get(&id)
        .ok_or_else(|| condition("invalid-function", [Value::Record(id)]))?;
    if args.len() < function.min as usize
        || (function.max >= 0 && args.len() > function.max as usize)
    {
        return Err(condition(
            "wrong-number-of-arguments",
            [Value::Record(id), Value::Integer(args.len() as i64)],
        ));
    }
    let activation = Activation::new(interpreter, environment);
    // GNU's live C call frame keeps the function reachable. A named Lisp
    // backtrace alone cannot do that after its symbol is rebound, so root
    // the function with this activation until foreign execution returns.
    Context(&activation).make(Value::Record(id));
    let mut handles: Vec<_> = args
        .iter()
        .map(|arg| Context(&activation).make(arg.clone()))
        .collect();
    let arguments = if handles.is_empty() {
        std::ptr::null_mut()
    } else {
        handles.as_mut_ptr()
    };
    // SAFETY: the library is retained; its function and data were registered
    // by make_function. The activation roots every argument during callbacks.
    let result = unsafe {
        (function.function)(
            activation.public_ptr(),
            args.len() as isize,
            arguments,
            function.data,
        )
    };
    Context(&activation).finish(result)
}

pub(crate) fn print_function(interpreter: &Interpreter, id: u64) -> String {
    let Some(function) = interpreter.modules.functions.get(&id) else {
        return "#<module function>".into();
    };
    let address = function.function as *const () as *const c_void;
    let mut description = format!("at {address:p}");
    let mut library = None;
    #[cfg(unix)]
    {
        let mut info = std::mem::MaybeUninit::<libc::Dl_info>::uninit();
        // SAFETY: dladdr inspects a live function address and initializes INFO.
        if unsafe { libc::dladdr(address, info.as_mut_ptr()) } != 0 {
            let info = unsafe { info.assume_init() };
            // GNU's dynlib_addr publishes both names only when both exist.
            // ELF libraries often have a file name but no dynamic symbol
            // for a static module function; print only its address then.
            if !info.dli_sname.is_null() && !info.dli_fname.is_null() {
                description = unsafe { CStr::from_ptr(info.dli_sname) }
                    .to_string_lossy()
                    .into_owned();
                library = Some(
                    unsafe { CStr::from_ptr(info.dli_fname) }
                        .to_string_lossy()
                        .into_owned(),
                );
            }
        }
    }
    if !function.data.is_null() {
        description.push_str(&format!(" with data {:p}", function.data));
    }
    if let Some(library) = library {
        description.push_str(&format!(" from {library}"));
    }
    format!("#<module function {description}>")
}

pub(crate) fn print_user_pointer(interpreter: &Interpreter, id: u64) -> String {
    let pointer = interpreter
        .modules
        .pointers
        .get(&id)
        .expect("live user pointer");
    let finalizer = pointer
        .finalizer
        .map_or(std::ptr::null(), |f| f as *const () as *const c_void);
    let mut output = [0 as c_char; 128];
    // GNU print.c uses the host's %p spelling, including its null-pointer
    // spelling. The fixed format and two pointer arguments fit this buffer.
    unsafe {
        libc::snprintf(
            output.as_mut_ptr(),
            output.len(),
            c"#<user-ptr ptr=%p finalizer=%p>".as_ptr(),
            pointer.data,
            finalizer,
        );
        CStr::from_ptr(output.as_ptr())
            .to_string_lossy()
            .into_owned()
    }
}
