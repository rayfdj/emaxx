//! Process state owned by GNU `comp.c`'s Rust replacement.

use super::{
    backend::Compiler,
    gccjit,
    lisp::UnitData,
    loader::{self, NativeRegistry},
    runtime::NativeRuntime,
};
use crate::lisp::eval::Interpreter;
use crate::lisp::types::{Env, LispError};
use std::cell::RefCell;
use std::ffi::c_void;

/// The single compiler context that GNU keeps in its static `comp_t`.
///
/// Interpreter templates may be cloned only while no libgccjit context is
/// live.  A context contains raw pointers into one libgccjit arena and cannot
/// be duplicated.
#[derive(Default)]
pub(crate) struct NativeCompilerState {
    // The compiler can be borrowed by a suspended backend frame. Preserve
    // that single RefCell (and its re-entry check) across execution owners.
    pub(super) compiler: loader::SharedCompiler,
    pub(super) runtime: NativeRuntime,
    pub(super) registry: Box<NativeRegistry>,
}

impl Clone for NativeCompilerState {
    fn clone(&self) -> Self {
        assert!(
            self.compiler.borrow().is_none()
                && self.runtime.is_pristine()
                && self.registry.is_empty(),
            "cannot clone an interpreter with live native compiler or runtime state"
        );
        Self::default()
    }
}

impl NativeCompilerState {
    pub(crate) fn available() -> bool {
        gccjit::available()
    }

    pub(crate) fn begin_garbage_collection(&mut self) -> Vec<crate::lisp::types::Value> {
        self.runtime.begin_garbage_collection()
    }

    pub(crate) fn garbage_collection_finished(
        &mut self,
        live_bytes: usize,
        threshold: i64,
        percentage: Option<f64>,
    ) {
        self.runtime
            .garbage_collection_finished(live_bytes, threshold, percentage);
    }

    pub(crate) fn version() -> Option<(i32, i32, i32)> {
        gccjit::version()
    }

    pub(crate) fn acquire(&mut self) -> Result<(), String> {
        Self::acquire_cell(&self.compiler)
    }

    fn acquire_cell(compiler: &RefCell<Option<Compiler>>) -> Result<(), String> {
        let mut compiler = compiler
            .try_borrow_mut()
            .map_err(|_| "compiler context already taken".to_string())?;
        if compiler.is_some() {
            return Err("compiler context already taken".to_string());
        }
        *compiler = Some(Compiler::acquire()?);
        Ok(())
    }

    pub(crate) fn release(&mut self) {
        Self::release_cell(&self.compiler);
    }

    fn release_cell(compiler: &RefCell<Option<Compiler>>) {
        *compiler.borrow_mut() = None;
    }

    pub(crate) fn acquire_active() -> Option<Result<(), String>> {
        loader::with_active_compiler(|compiler| Self::acquire_cell(compiler))
    }

    pub(crate) fn release_active() -> Option<()> {
        loader::with_active_compiler(|compiler| Self::release_cell(compiler))
    }

    #[cfg(test)]
    pub(crate) fn is_acquired(&self) -> bool {
        self.compiler.borrow().is_some()
    }

    pub(crate) fn load(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        filename: &crate::lisp::types::Value,
        library: loader::UnitLibrary,
        candidate_unit: &crate::lisp::types::Value,
        late: bool,
    ) -> Result<crate::lisp::types::Value, LispError> {
        loader::load(
            loader::LoaderState::new(&self.compiler, &mut self.registry, &mut self.runtime),
            interp,
            env,
            filename,
            library,
            candidate_unit,
            late,
        )
    }

    pub(crate) fn call_function(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        record_id: u64,
        arguments: &[crate::lisp::types::Value],
    ) -> Result<crate::lisp::types::Value, LispError> {
        loader::call_function(
            &self.compiler,
            &mut self.registry,
            &mut self.runtime,
            interp,
            env,
            record_id,
            arguments,
        )
    }

    pub(crate) fn register(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        arguments: &[crate::lisp::types::Value],
        kind: loader::RegistrationKind,
    ) -> Result<crate::lisp::types::Value, LispError> {
        loader::register_with_state(
            &mut self.registry,
            &mut self.runtime,
            interp,
            env,
            arguments,
            kind,
        )
    }

    pub(crate) fn function_name(&self, record_id: u64) -> Option<&str> {
        self.registry.function_name(record_id)
    }

    pub(crate) fn unit_documentation(
        &self,
        interp: &mut Interpreter,
        env: &mut Env,
        record_id: u64,
    ) -> Result<crate::lisp::types::Value, LispError> {
        loader::unit_documentation(&self.registry, interp, env, record_id)
    }

    pub(crate) fn install_trampoline(
        &mut self,
        subroutine_index: usize,
        record_id: u64,
    ) -> Result<(), LispError> {
        let (target, _convention): (*mut c_void, _) =
            loader::function_target(&self.registry, record_id).ok_or_else(|| {
                super::lisp::native_ice("trampoline is not a registered native function")
            })?;
        self.runtime
            .install_trampoline(subroutine_index, target)
            .map_err(|error| super::lisp::native_ice(&error))
    }

    pub(crate) fn compile_current_unit(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        output_filename: &str,
    ) -> Result<String, LispError> {
        let Self {
            runtime, registry, ..
        } = self;
        // comp.c calls Lisp accessors while its global compiler context is
        // live.  Those accessors can themselves already be native compiled,
        // so the process-wide native function registry and runtime must stay
        // reachable throughout the backend call just as they do in GNU.
        loader::with_native_state(&self.compiler, registry, runtime, |_| {
            Self::compile_current_unit_with(&self.compiler, interp, env, output_filename)
        })
    }

    pub(crate) fn compile_current_unit_active(
        interp: &mut Interpreter,
        env: &mut Env,
        output_filename: &str,
    ) -> Option<Result<String, LispError>> {
        loader::with_active_compiler(|compiler| {
            Self::compile_current_unit_with(compiler, interp, env, output_filename)
        })
    }

    fn compile_current_unit_with(
        compiler: &RefCell<Option<Compiler>>,
        interp: &mut Interpreter,
        env: &mut Env,
        output_filename: &str,
    ) -> Result<String, LispError> {
        let mut compiler = compiler
            .try_borrow_mut()
            .map_err(|_| super::lisp::native_ice("compiler context already in use"))?;
        let compiler = compiler
            .as_mut()
            .ok_or_else(|| super::lisp::native_ice("compiler context is not initialized"))?;
        let unit = UnitData::read(interp, env)?;
        compiler
            .configure_unit(unit.speed(), unit.debug(), output_filename)
            .map_err(|error| super::lisp::native_ice(&error))?;
        compiler
            .begin_unit(&unit.as_input())
            .map_err(|error| super::lisp::native_ice(&error))?;
        compiler.emit_functions(interp, env, &unit)?;
        compiler
            .prepare_output(&unit, output_filename)
            .map_err(|error| super::lisp::native_ice(&error))?;

        let character_count = output_filename.chars().count();
        if character_count < 4 {
            return Err(LispError::Signal("Args out of range".into()));
        }
        let base = output_filename
            .chars()
            .take(character_count - 4)
            .collect::<String>();
        let temporary = super::lisp::call(
            interp,
            env,
            "make-temp-file",
            &[
                crate::lisp::types::Value::string(&base),
                crate::lisp::types::Value::Nil,
                crate::lisp::types::Value::string(".eln.tmp"),
                crate::lisp::types::Value::Nil,
            ],
        )?;
        let temporary = crate::lisp::primitives::string_like(&temporary)
            .map(|string| string.text)
            .ok_or_else(|| super::lisp::native_ice("make-temp-file returned a non-string"))?;
        compiler
            .compile_to_file(&temporary)
            .map_err(|error| super::lisp::native_ice(&error))?;
        Ok(temporary)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compiler_owners_share_the_context_and_preserve_live_borrow_exclusion() {
        let mut first = NativeCompilerState::default();
        let mut second = NativeCompilerState {
            compiler: first.compiler.clone(),
            ..NativeCompilerState::default()
        };
        first
            .acquire()
            .expect("acquire the shared compiler context");
        assert!(second.is_acquired());
        assert_eq!(
            second.acquire(),
            Err("compiler context already taken".into())
        );
        second.release();
        assert!(!first.is_acquired(), "release is visible to every owner");
        let borrowed = first.compiler.borrow_mut();
        assert_eq!(
            second.acquire(),
            Err("compiler context already taken".into())
        );
        drop(borrowed);
        second
            .acquire()
            .expect("context can be acquired after borrow ends");
        first.release();
        assert!(!second.is_acquired());
    }

    #[test]
    fn comp_c_context_is_unique_and_release_is_idempotent() {
        let mut state = NativeCompilerState::default();
        state.acquire().expect("acquire native compiler context");
        assert!(state.is_acquired());
        assert_eq!(
            state.acquire(),
            Err("compiler context already taken".into())
        );
        state.release();
        state.release();
        assert!(!state.is_acquired());
    }
}
