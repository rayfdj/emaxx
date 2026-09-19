use super::*;

#[test]
#[cfg(unix)]
fn module_libraries_keep_global_symbols_after_rejected_load() {
    let directory = std::env::temp_dir().join(format!(
        "emaxx-module-symbols-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("current time")
            .as_nanos()
    ));
    std::fs::create_dir(&directory).expect("module fixture directory");
    let mut libraries = Vec::new();
    for (name, source) in [
        ("rejected", "int emaxx_module_rejected_symbol;"),
        (
            "valid",
            "int plugin_is_GPL_compatible; int emaxx_module_valid_symbol;\n\
             int emacs_module_init(void *runtime) { (void) runtime; return 0; }",
        ),
        (
            "consumer",
            "#define _GNU_SOURCE 1\n#include <dlfcn.h>\nint plugin_is_GPL_compatible;\n\
             int emacs_module_init(void *runtime) { (void) runtime;\n\
               return dlsym(RTLD_DEFAULT, \"emaxx_module_rejected_symbol\") &&\n\
                      dlsym(RTLD_DEFAULT, \"emaxx_module_valid_symbol\") ? 0 : 17; }",
        ),
    ] {
        let source_path = directory.join(format!("{name}.c"));
        std::fs::write(&source_path, source).expect("module fixture source");
        let library = directory.join(format!(
            "{name}.{}",
            if cfg!(target_os = "macos") {
                "dylib"
            } else {
                "so"
            }
        ));
        let mut compiler =
            std::process::Command::new(std::env::var_os("CC").unwrap_or_else(|| "cc".into()));
        compiler.env_clear().envs(
            super::super::eval::initial_process_environment()
                .iter()
                .cloned(),
        );
        compiler.args(if cfg!(target_os = "macos") {
            &["-dynamiclib"][..]
        } else {
            &["-shared", "-fPIC"][..]
        });
        assert!(
            compiler
                .arg(&source_path)
                .arg("-o")
                .arg(&library)
                .status()
                .expect("compile module fixture")
                .success()
        );
        libraries.push(Value::string(&library.to_string_lossy()));
    }
    let mut interpreter = Interpreter::new();
    let mut environment = Env::new();
    let error =
        load(&mut interpreter, &libraries[0], &mut environment).expect_err("missing GPL marker");
    assert_eq!(
        super::super::eval::error_condition_value(&error)
            .car()
            .expect("condition symbol"),
        Value::symbol("module-not-gpl-compatible")
    );
    assert_eq!(
        load(&mut interpreter, &libraries[1], &mut environment).expect("valid provider"),
        Value::T
    );
    // dynlib.c opens modules with RTLD_GLOBAL. Fmodule_load does not close
    // an opened library when its GPL or entry-point checks fail either.
    assert_eq!(
        load(&mut interpreter, &libraries[2], &mut environment)
            .expect("both providers remain globally visible"),
        Value::T
    );
    drop(interpreter);
    std::fs::remove_dir_all(directory).expect("remove module fixtures");
}

#[test]
fn module_time_conversion_floors_and_checks_host_range() {
    let mut interpreter = Interpreter::new();
    let mut environment = Env::new();
    let activation = Activation::new(&mut interpreter, &mut environment);
    let env = activation.public_ptr();
    let mut context = Context(&activation);
    // GNU lisp_time_argument floors sub-nanosecond fractions and reports
    // an unrepresentable time through time_overflow, not overflow-error.
    unsafe {
        let value = context.make(Value::cons(Value::Integer(-1), Value::Integer(3)));
        let time = extract_time(env, value);
        assert_eq!((time.tv_sec, time.tv_nsec), (-1, 666_666_666));
        assert_eq!(non_local_exit_check(env), 0);
        for seconds in [libc::time_t::MIN, libc::time_t::MAX] {
            let value = context.make(primitives::normalize_bigint_value(seconds.into()));
            let time = extract_time(env, value);
            assert_eq!((time.tv_sec, time.tv_nsec), (seconds, 0));
            assert_eq!(non_local_exit_check(env), 0);
        }
        for seconds in [
            num_bigint::BigInt::from(libc::time_t::MIN) - 1,
            num_bigint::BigInt::from(libc::time_t::MAX) + 1,
        ] {
            let value = context.make(primitives::normalize_bigint_value(seconds));
            let time = extract_time(env, value);
            assert_eq!((time.tv_sec, time.tv_nsec), (0, 0));
            let mut symbol = std::ptr::null_mut();
            let mut data = std::ptr::null_mut();
            assert_eq!(non_local_exit_get(env, &mut symbol, &mut data), 1);
            assert_eq!(context.value(symbol), Value::symbol("error"));
            assert_eq!(
                context.value(data),
                Value::list([Value::string("Specified time is not representable")])
            );
            non_local_exit_clear(env);
        }
    }
}

#[test]
fn module_strings_preserve_bytes_unicode_and_pending_errors() {
    let mut interpreter = Interpreter::new();
    let mut environment = Env::new();
    let activation = Activation::new(&mut interpreter, &mut environment);
    let env = activation.public_ptr();
    // SAFETY: these calls use live environments and exactly sized buffers.
    unsafe {
        let bytes = [0xff_u8, 0, b'a'];
        let string = make_unibyte_string(env, bytes.as_ptr().cast(), 3);
        let mut required = std::mem::MaybeUninit::<isize>::uninit();
        assert!(copy_string_contents(
            env,
            string,
            std::ptr::null_mut(),
            required.as_mut_ptr()
        ));
        let mut size = required.assume_init();
        assert_eq!(size, 4);
        let mut copied = [0_u8; 4];
        assert!(copy_string_contents(
            env,
            string,
            copied.as_mut_ptr().cast(),
            &mut size
        ));
        assert_eq!(copied, [0xff, 0, b'a', 0]);

        let surrogate = [0xed_u8, 0xa0, 0x80];
        let string = make_string(env, surrogate.as_ptr().cast(), 3);
        assert_eq!(non_local_exit_check(env), 0);
        size = 4;
        assert!(copy_string_contents(
            env,
            string,
            copied.as_mut_ptr().cast(),
            &mut size
        ));
        assert_eq!(copied, [0xed, 0xa0, 0x80, 0]);

        assert!(make_string(env, bytes.as_ptr().cast(), 1).is_null());
        assert_eq!(non_local_exit_check(env), 1);
        assert!(
            make_integer(env, 7).is_null(),
            "pending errors inhibit API operations"
        );
        let mut symbol = std::ptr::null_mut();
        let mut data = std::ptr::null_mut();
        assert_eq!(non_local_exit_get(env, &mut symbol, &mut data), 1);
        assert_eq!(
            Context(&activation).value(symbol),
            Value::symbol("wrong-type-argument")
        );
        non_local_exit_clear(env);
        assert_eq!(extract_integer(env, make_integer(env, 7)), 7);
    }
}

unsafe extern "C" fn use_outer_environment(
    _env: *mut ModuleEnv,
    _nargs: isize,
    _args: *mut Handle,
    data: *mut c_void,
) -> Handle {
    // GNU permits any still-live environment, including an outer call's.
    unsafe { intern(data.cast(), c"module-reentered".as_ptr()) }
}

unsafe extern "C" fn reenter(
    env: *mut ModuleEnv,
    _nargs: isize,
    args: *mut Handle,
    data: *mut c_void,
) -> Handle {
    unsafe {
        let mut inner = make_function(
            env,
            0,
            0,
            use_outer_environment,
            std::ptr::null(),
            env.cast(),
        );
        let result = funcall(env, *args, 1, &mut inner);
        if (&*data.cast::<Cell<bool>>()).get() {
            intern(env, c"finalized-too-early".as_ptr())
        } else {
            result
        }
    }
}

unsafe extern "C" fn mark_finalized(data: *mut c_void) {
    unsafe { (&*data.cast::<Cell<bool>>()).set(true) };
}

#[test]
fn module_environment_supports_reentry_and_gc_through_lisp() {
    let mut interpreter = Interpreter::new();
    let mut environment = Env::new();
    let finalized = Cell::new(false);
    // The function object is made and called out of this frame, and the
    // frames below are clobbered before the collection: the stack is
    // scanned conservatively, and a `Value' left in a live frame would
    // keep the record.
    #[inline(never)]
    fn call_through_lisp(
        interpreter: &mut Interpreter,
        environment: &mut Env,
        finalized: &Cell<bool>,
    ) -> Value {
        let outer = {
            let activation = Activation::new(interpreter, environment);
            // SAFETY: the function and environment live for the entire call.
            let function = unsafe {
                make_function(
                    activation.public_ptr(),
                    1,
                    1,
                    reenter,
                    std::ptr::null(),
                    (finalized as *const Cell<bool>).cast_mut().cast(),
                )
            };
            unsafe {
                set_function_finalizer(activation.public_ptr(), function, Some(mark_finalized))
            };
            Context(&activation).value(function)
        };
        let form = super::super::reader::Reader::new(
            "(function (lambda (inner) (garbage-collect) (funcall inner)))",
        )
        .read()
        .expect("reentrant callback should parse")
        .expect("reentrant callback should contain a form");
        let callback = interpreter
            .eval(&form, environment)
            .expect("reentrant callback should evaluate");
        let result = interpreter
            // A named call's backtrace holds its symbol. Like an unbound or
            // redefined symbol, it no longer keeps this function object alive.
            .call_function_value(outer, Some("module-reentry-test"), &[callback], environment)
            .expect("nested module invocation should survive collection");
        assert!(
            interpreter.modules.values.is_empty(),
            "local handles expire on return"
        );
        assert!(!finalized.get(), "an executing function must remain live");
        result
    }
    let result = call_through_lisp(&mut interpreter, &mut environment, &finalized);
    assert_eq!(result, Value::symbol("module-reentered"));
    crate::lisp::alloc::clobber_stack();
    primitives::call(&mut interpreter, "garbage-collect", &[], &mut environment)
        .expect("collection after the foreign call");
    assert!(
        finalized.get(),
        "the returned function can now be finalized"
    );
}
