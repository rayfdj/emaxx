use super::*;

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
    let outer = {
        let activation = Activation::new(&mut interpreter, &mut environment);
        // SAFETY: the function and environment live for the entire call.
        let function = unsafe {
            make_function(
                activation.public_ptr(),
                1,
                1,
                reenter,
                std::ptr::null(),
                (&finalized as *const Cell<bool>).cast_mut().cast(),
            )
        };
        unsafe { set_function_finalizer(activation.public_ptr(), function, Some(mark_finalized)) };
        Context(&activation).value(function)
    };
    let form = super::super::reader::Reader::new(
        "(function (lambda (inner) (garbage-collect) (funcall inner)))",
    )
    .read()
    .expect("reentrant callback should parse")
    .expect("reentrant callback should contain a form");
    let callback = interpreter
        .eval(&form, &mut environment)
        .expect("reentrant callback should evaluate");
    let result = interpreter
        // A named call's backtrace holds its symbol. Like an unbound or
        // redefined symbol, it no longer keeps this function object alive.
        .call_function_value(
            outer,
            Some("module-reentry-test"),
            &[callback],
            &mut environment,
        )
        .expect("nested module invocation should survive collection");
    assert_eq!(result, Value::symbol("module-reentered"));
    assert!(
        interpreter.modules.values.is_empty(),
        "local handles expire on return"
    );
    assert!(!finalized.get(), "an executing function must remain live");
    primitives::call(&mut interpreter, "garbage-collect", &[], &mut environment)
        .expect("collection after the foreign call");
    assert!(
        finalized.get(),
        "the returned function can now be finalized"
    );
}
