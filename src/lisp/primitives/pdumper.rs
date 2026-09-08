//! pdumper.c: the portable dumper's Lisp entry point.
//!
//! D07 covers `Fdump_emacs_portable' from its first check to the point
//! where GNU opens the output file: the batch, main-thread and
//! other-thread refusals in source order, the unchanged Lisp
//! `load--fixup-all-elns', the collection loop that repeats while
//! finalizers ran, the dynamic `command-line-processed' binding, the
//! filename check and expansion, and the three variables
//! `dump_unwind_cleanup' restores.  The writer itself (D08 onward) starts
//! at `write_dump'.

use super::*;

pub(crate) fn dump_emacs_portable(
    interp: &mut Interpreter,
    env: &mut crate::lisp::types::Env,
    filename: &Value,
    track_referrers: Option<&Value>,
) -> Result<Value, LispError> {
    // eassert (initialized): a primitive only runs in an initialized image.
    if !interp
        .lookup_var("noninteractive", env)
        .is_some_and(|value| value.is_truthy())
    {
        return Err(LispError::Signal(
            "Dumping Emacs currently works only in batch mode.  If you'd like it \
             to work interactively, please consider contributing a patch to Emacs."
                .into(),
        ));
    }
    // will_dump_with_unexec_p: false without HAVE_UNEXEC, which this
    // configuration does not define.
    if !interp.current_thread_is_main() {
        return Err(LispError::Signal(
            "This function can be called only in the main thread".into(),
        ));
    }
    // !NILP (XCDR (Fall_threads ())): the main thread is always listed.
    if interp.live_threads().len() > 1 {
        return Err(LispError::Signal(
            "No other Lisp threads can be running when this function is called".into(),
        ));
    }
    // HAVE_NATIVE_COMP: CALLN (Ffuncall, intern_c_string ("load--fixup-all-elns")).
    super::hooks_overlays::call_named_function(interp, "load--fixup-all-elns", &[], env)?;
    // check_pure_size reports a pure-space overflow; Emaxx has no pure space.
    // "Clear out any detritus in memory."
    loop {
        interp.finalizers_run = 0;
        crate::lisp::native_comp::garbage_collect_now(interp, env)?;
        if interp.finalizers_run == 0 {
            break;
        }
    }
    // specbind (Qcommand_line_processed, Qnil), unbound with the specpdl on
    // every exit.
    let binding = interp.bind_special_dynamic("command-line-processed", Value::Nil, env)?;
    let result = dump_with_context(interp, env, filename, track_referrers);
    let unbound = interp.restore_special_dynamic(binding, env);
    let value = result?;
    unbound?;
    Ok(value)
}

fn dump_with_context(
    interp: &mut Interpreter,
    env: &mut crate::lisp::types::Env,
    filename: &Value,
    track_referrers: Option<&Value>,
) -> Result<Value, LispError> {
    let Some(name) = string_like(filename).map(|string| string.text) else {
        return Err(LispError::WrongTypeArgument(
            "stringp".into(),
            filename.clone(),
        ));
    };
    let filename = super::system::expand_file_name_runtime(interp, env, &name, None)?;
    // ENCODE_FILE: the host receives the expanded name as the same UTF-8
    // bytes every other file primitive hands it.
    // record_unwind_protect_ptr (dump_unwind_cleanup, ctx); block_input ():
    // Emaxx has no asynchronous input handler to block.
    let context = DumpContext::enter(interp);
    let result = write_dump(
        interp,
        env,
        &filename,
        track_referrers.is_some_and(|value| value.is_truthy()),
    );
    context.leave(interp);
    result
}

/// The C variables `Fdump_emacs_portable' clears for the duration of the
/// dump and `dump_unwind_cleanup' puts back: direct writes to the
/// forwarded cells, not `set_internal', so no watcher runs.
struct DumpContext {
    old_purify_flag: Value,
    old_post_gc_hook: Value,
    old_process_environment: Value,
}

impl DumpContext {
    fn enter(interp: &mut Interpreter) -> Self {
        let mut take = |name: &str| {
            let old = interp.default_value(name).unwrap_or(Value::Nil);
            interp.set_global_binding(name, Value::Nil);
            old
        };
        let old_purify_flag = take("purify-flag");
        // "Make sure various weird things are less likely to happen."
        let old_post_gc_hook = take("post-gc-hook");
        // "Reset process-environment -- this is for when they re-dump a
        // pdump-restored emacs, since set_initial_environment wants always
        // to cons it from scratch."
        let old_process_environment = take("process-environment");
        Self {
            old_purify_flag,
            old_post_gc_hook,
            old_process_environment,
        }
    }

    fn leave(self, interp: &mut Interpreter) {
        interp.set_global_binding("purify-flag", self.old_purify_flag);
        interp.set_global_binding("post-gc-hook", self.old_post_gc_hook);
        interp.set_global_binding("process-environment", self.old_process_environment);
    }
}

/// From `emacs_open (SSDATA (filename), O_RDWR | O_TRUNC | O_CREAT, 0666)'
/// on: the header, the object queues, the sections and the relocation
/// tables (D08 and later).  Until that writer exists, no file is opened,
/// so no incomplete or lookalike image is ever left behind.
fn write_dump(
    _interp: &mut Interpreter,
    _env: &mut crate::lisp::types::Env,
    _filename: &str,
    _track_referrers: bool,
) -> Result<Value, LispError> {
    Err(LispError::Signal(PORTABLE_DUMPER_UNAVAILABLE.into()))
}
