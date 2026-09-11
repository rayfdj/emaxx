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
/// pdumper_load's validation and object reconstruction.
pub(crate) mod load;
#[cfg(test)]
mod tests;

use super::*;
use crate::lisp::eval::PdumperLoadRecord;
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
    if !dump_messages_suppressed() {
        eprint!("{}", summary.report());
    }
    // unblock_input (); return unbind_to (count, Qnil).
    Ok(Value::Nil)
}

/// The harness's fixture dump and load run inside processes whose
/// stderr the tests compare byte for byte with GNU's, so their
/// fingerprint and byte-count lines (GNU prints them from a build's
/// dump and a failed load, never from a session) are held back while
/// the guard lives.  `dump-emacs-portable' called from Lisp prints as
/// pdumper.c does.
static DUMP_MESSAGES_SUPPRESSED: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

pub(crate) fn dump_messages_suppressed() -> bool {
    DUMP_MESSAGES_SUPPRESSED.load(std::sync::atomic::Ordering::Relaxed) != 0
}

pub(crate) struct QuietDumpMessages;

impl QuietDumpMessages {
    pub(crate) fn hold() -> Self {
        DUMP_MESSAGES_SUPPRESSED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Self
    }
}

impl Drop for QuietDumpMessages {
    fn drop(&mut self) {
        DUMP_MESSAGES_SUPPRESSED.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }
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
    if !dump_messages_suppressed() {
        eprintln!("Dumping fingerprint: {}", hex(executable_fingerprint()));
    }
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

    // "Dump until while we keep finding objects to dump.  We add new
    // objects to the queue by side effect during dumping.  We accumulate
    // some types of objects in special lists to get more locality for
    // these object types at runtime."
    loop {
        ctx.drain_deferred_hash_tables(interp)?;
        ctx.drain_normal_queue(interp)?;
        if ctx.queue_is_empty() && ctx.deferred_hash_tables_is_empty() {
            break;
        }
    }
    ctx.header.hash_list = ctx.dump_hash_table_list(interp)?;
    // "dump_hash_table_list just adds a new vector to the dump but all its
    // content should already have been in the dump."
    assert!(ctx.queue_is_empty() && ctx.deferred_hash_tables_is_empty());

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

/// pdumper.c:pdumper_load_result, for the process-level loader.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum PdumperLoadError {
    /// PDUMPER_LOAD_FILE_NOT_FOUND: ENOENT or ENOTDIR on open.
    FileNotFound,
    /// PDUMPER_LOAD_BAD_FILE_TYPE: too small, or not a dump's magic.
    BadFileType,
    /// PDUMPER_LOAD_FAILED_DUMP: the incomplete marker is still set.
    FailedDump,
    /// PDUMPER_LOAD_VERSION_MISMATCH: another build's fingerprint.
    VersionMismatch,
    /// PDUMPER_LOAD_ERROR (+ errno) and the reconstruction failures.
    Error(String),
}

impl PdumperLoadError {
    /// emacs.c:dump_error_to_string (a reconstruction failure carries
    /// its own message where GNU says "generic error" or strerror).
    pub(crate) fn reason(&self) -> String {
        match self {
            Self::FileNotFound => "could not open file".into(),
            Self::BadFileType => "not a dump file".into(),
            Self::FailedDump => "dump file is result of failed dump attempt".into(),
            Self::VersionMismatch => "not built for this Emacs executable".into(),
            Self::Error(message) => message.clone(),
        }
    }
}

/// pdumper.c:pdumper_load: refuse a second load, open and validate the
/// file exactly as GNU does, then the point of no return: rebuild the
/// objects, install the symbols and static roots, record the load.
pub(crate) fn pdumper_load(
    path: &std::path::Path,
    interp: &mut Interpreter,
) -> Result<PdumperLoadRecord, PdumperLoadError> {
    // eassert (!dump_loaded_p ()): "We can load only one dump."
    if interp.dump_loaded_p() {
        return Err(PdumperLoadError::Error("a dump is already loaded".into()));
    }
    let started = std::time::Instant::now();
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) => {
            return Err(match error.kind() {
                std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory => {
                    PdumperLoadError::FileNotFound
                }
                _ => PdumperLoadError::Error(error.to_string()),
            });
        }
    };
    if bytes.len() < HEADER_LEN {
        return Err(PdumperLoadError::BadFileType);
    }
    let image = match load::load_image(&bytes, interp) {
        Ok(image) => image,
        Err(load::LoadError::BadFileType) => return Err(PdumperLoadError::BadFileType),
        Err(load::LoadError::FailedDump) => return Err(PdumperLoadError::FailedDump),
        Err(load::LoadError::VersionMismatch) => return Err(PdumperLoadError::VersionMismatch),
        Err(load::LoadError::Error(message)) => return Err(PdumperLoadError::Error(message)),
    };
    let record = PdumperLoadRecord {
        filename: path.to_string_lossy().into_owned(),
        load_time: started.elapsed(),
        dump_size: bytes.len() as u64,
    };
    interp
        .install_image(&image, record.clone())
        .map_err(PdumperLoadError::Error)?;
    // pdumper_set_emacs_execdir, then LATE_RELOCS (the native compilation
    // units) and VERY_LATE_RELOCS (the native functions), after the Emacs
    // relocations gave the process its variables (`comp-abi-hash',
    // `native-comp-eln-load-path', `comp-loaded-comp-units-h').
    crate::lisp::native_comp::load_dumped_code(
        interp,
        &image.native_units,
        &image.native_functions,
    )
    .map_err(|error| PdumperLoadError::Error(error.to_string()))?;
    Ok(record)
}

/// emacs.c:load_pdump for the startup path.  An explicit `--dump-file'
/// is loaded, and any failure -- a missing file included -- is the fatal
/// "could not load dump file".  Otherwise the executable's own
/// `<name>.pdmp' beside it is tried: missing means the process is
/// temacs and builds its state itself (`Ok(None)'), any other failure is
/// fatal.  GNU's further candidate, `PATH_EXEC/emacs-VERSION.pdmp' (the
/// installed image), has no Emaxx installation layout yet.
pub(crate) fn load_pdump_at_startup(
    interp: &mut Interpreter,
    dump_file: Option<&std::path::Path>,
) -> Option<PdumperLoadRecord> {
    // term.c:fatal: "emacs: " and the message on stderr, exit 1.
    let fatal = |candidate: &std::path::Path, error: PdumperLoadError| -> ! {
        eprintln!(
            "emacs: could not load dump file \"{}\": {}",
            candidate.display(),
            error.reason()
        );
        std::process::exit(1)
    };
    if let Some(named) = dump_file {
        return match pdumper_load(named, interp) {
            Ok(record) => Some(record),
            Err(error) => fatal(named, error),
        };
    }
    let exe = std::env::current_exe().ok()?;
    let mut sibling = exe.clone();
    sibling.set_file_name(format!(
        "{}.pdmp",
        exe.file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_default()
    ));
    match pdumper_load(&sibling, interp) {
        Ok(record) => Some(record),
        Err(PdumperLoadError::FileNotFound) => None,
        Err(error) => fatal(&sibling, error),
    }
}
