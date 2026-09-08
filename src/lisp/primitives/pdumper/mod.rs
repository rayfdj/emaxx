//! pdumper.c: the portable dumper.
//!
//! `dump_emacs_portable' is Fdump_emacs_portable: the batch, main-thread
//! and other-thread refusals in source order, the unchanged Lisp
//! `load--fixup-all-elns', the collection loop that repeats while
//! finalizers ran, the dynamic `command-line-processed' binding, the
//! filename check and expansion, the three variables `dump_unwind_cleanup'
//! restores (D07), then the file open and the image itself: header, hot
//! section from the roots, discardable and cold sections, fixups,
//! relocation tables and the completed header (D08, `write_image').
//! `context' is the dump context and the per-object writers, `image' the
//! file layout, `load' the validation and reconstruction side.

pub(crate) mod context;
pub(crate) mod image;
/// pdumper_load's validation and reconstruction: exercised by the D08
/// round-trip controls until the process-level restore (D12) wires it
/// into startup.
#[cfg(test)]
pub(crate) mod load;
#[cfg(test)]
mod tests;

use super::*;
use context::{DumpContext, DumpError};
use image::*;

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
    let context = DumpContextVariables::enter(interp);
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
struct DumpContextVariables {
    old_purify_flag: Value,
    old_post_gc_hook: Value,
    old_process_environment: Value,
}

impl DumpContextVariables {
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
/// on.  The image is built in memory and written once at the end, as
/// GNU's is; a failure before that leaves the truncated, empty file, as
/// GNU's does.  The startup reconstruction hands off before the open.
fn write_dump(
    interp: &mut Interpreter,
    env: &mut crate::lisp::types::Env,
    filename: &str,
    track_referrers: bool,
) -> Result<Value, LispError> {
    if interp.image_reconstruction_handoff {
        return Err(LispError::Signal(PORTABLE_DUMPER_UNAVAILABLE.into()));
    }
    let mut file = {
        use std::os::unix::fs::OpenOptionsExt;
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o666)
            .open(filename)
    }
    .map_err(|error| {
        LispError::SignalValue(super::file_io::file_operation_error_value(
            "Opening dump output",
            &error,
            filename,
        ))
    })?;
    let mut ctx = DumpContext::new(track_referrers, interp.main_thread_record_id());
    let summary = match write_image(&mut ctx, interp, RootSource::Interpreter) {
        Ok(summary) => summary,
        Err(DumpError::Unsupported(unsupported)) => {
            ctx.print_paths_to_root(interp, env, &unsupported.object);
            return Err(LispError::Signal(format!(
                "unsupported object type in dump: {}",
                unsupported.message
            )));
        }
        Err(DumpError::Lisp(error)) => return Err(error),
    };
    if file.write_all(ctx.buffer()).is_err() {
        return Err(LispError::SignalValue(super::file_io::file_error_value(
            "Could not write to dump file",
            filename,
        )));
    }
    eprint!("{}", summary.report());
    // unblock_input (); return unbind_to (count, Qnil).
    Ok(Value::Nil)
}

/// Where the roots come from: the interpreter (Fdump_emacs_portable) or
/// an explicit list (the round-trip controls).
pub(crate) enum RootSource {
    Interpreter,
    #[cfg(test)]
    Explicit(Vec<(RootSlot, Value)>),
}

/// The byte and relocation counts Fdump_emacs_portable reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DumpSummary {
    pub(crate) header_bytes: u32,
    pub(crate) hot_bytes: u32,
    pub(crate) discardable_bytes: u32,
    pub(crate) cold_bytes: u32,
    pub(crate) hot_relocations: u32,
    pub(crate) discardable_relocations: u32,
}

impl DumpSummary {
    pub(crate) fn report(&self) -> String {
        format!(
            "Dump complete\nByte counts: header={} hot={} discardable={} cold={}\n\
             Reloc counts: hot={} discardable={}\n",
            self.header_bytes,
            self.hot_bytes,
            self.discardable_bytes,
            self.cold_bytes,
            self.hot_relocations,
            self.discardable_relocations
        )
    }
}

/// Fdump_emacs_portable from the header write to the completed header:
/// the same sequence of section starts, drains, fixups and tables.
pub(crate) fn write_image(
    ctx: &mut DumpContext,
    interp: &Interpreter,
    roots: RootSource,
) -> Result<DumpSummary, DumpError> {
    let header_start = ctx.offset();
    eprintln!("Dumping fingerprint: {}", hex(executable_fingerprint()));
    ctx.write_header()?;
    let header_end = ctx.offset();

    let hot_start = ctx.offset();
    // "Start the dump process by processing the static roots and queuing
    // up the objects to which they refer."
    match roots {
        RootSource::Interpreter => ctx.dump_roots(interp)?,
        #[cfg(test)]
        RootSource::Explicit(list) => ctx.dump_explicit_roots(interp, &list)?,
    }
    // dump_charset_table, the finalizer list heads, the remembered data
    // and dump_metadata_for_pdumper join the image with their object
    // kinds (D11, D13).

    // "Dump until while we keep finding objects to dump."  Deferred hash
    // tables are D10; there is nothing else to drain in between.
    loop {
        ctx.drain_normal_queue(interp)?;
        if ctx.queue_is_empty() {
            break;
        }
    }
    ctx.header.hash_list = 0;

    ctx.sort_copied_objects();
    // dump_hot_parts_of_discardable_objects: the built-in symbols' hot
    // parts.  Emaxx's symbols are heap objects already written above,
    // except `nil' and `t', whose cells are written here.
    ctx.dump_builtin_symbol_roots(interp)?;

    let hot_end = ctx.offset();
    ctx.header.discardable_start = hot_end;

    ctx.drain_copied_objects(interp)?;
    assert!(ctx.queue_is_empty());

    let discardable_end = ctx.offset();
    ctx.align_output(64 * 1024)?;
    ctx.header.cold_start = ctx.offset();

    ctx.drain_cold_data(interp)?;
    // dump_drain_user_remembered_data_cold: D13.

    // "After this point, the dump file contains no data that can be part
    // of the Lisp heap."
    ctx.set_end_heap();

    ctx.do_fixups()?;

    for phase in 0..RELOC_NUM_PHASES {
        ctx.emit_dump_relocs(phase)?;
    }
    let hot_relocations = ctx.number_hot_relocations;
    ctx.number_hot_relocations = 0;
    let discardable_relocations = ctx.number_discardable_relocations;
    ctx.number_discardable_relocations = 0;
    ctx.emit_object_starts()?;
    ctx.emit_emacs_relocs()?;

    let cold_end = ctx.offset();
    ctx.assert_drained();

    // "Dump is complete.  Go back to the header and write the magic
    // indicating that the dump is complete and can be loaded."
    ctx.mark_complete();
    ctx.seek_to_start();
    ctx.write_header()?;

    Ok(DumpSummary {
        header_bytes: header_end - header_start,
        hot_bytes: hot_end - hot_start,
        discardable_bytes: discardable_end - ctx.header.discardable_start,
        cold_bytes: cold_end - ctx.header.cold_start,
        hot_relocations,
        discardable_relocations,
    })
}
