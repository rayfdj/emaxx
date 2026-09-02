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
use std::ffi::c_void;

/// The single compiler context that GNU keeps in its static `comp_t`.
///
/// Interpreter templates may be cloned only while no libgccjit context is
/// live.  A context contains raw pointers into one libgccjit arena and cannot
/// be duplicated.
#[derive(Default)]
pub(crate) struct NativeCompilerState {
    compiler: Option<Compiler>,
    runtime: NativeRuntime,
    registry: Box<NativeRegistry>,
}

impl Clone for NativeCompilerState {
    fn clone(&self) -> Self {
        assert!(
            self.compiler.is_none() && self.runtime.is_pristine() && self.registry.is_empty(),
            "cannot clone an interpreter with live native compiler or runtime state"
        );
        Self::default()
    }
}

impl NativeCompilerState {
    pub(crate) fn available() -> bool {
        gccjit::available()
    }

    pub(crate) fn version() -> Option<(i32, i32, i32)> {
        gccjit::version()
    }

    pub(crate) fn acquire(&mut self) -> Result<(), String> {
        if self.compiler.is_some() {
            return Err("compiler context already taken".to_string());
        }
        self.compiler = Some(Compiler::acquire()?);
        Ok(())
    }

    pub(crate) fn release(&mut self) {
        self.compiler = None;
    }

    #[cfg(test)]
    pub(crate) fn is_acquired(&self) -> bool {
        self.compiler.is_some()
    }

    pub(crate) fn load(
        &mut self,
        interp: &mut Interpreter,
        env: &mut Env,
        filename: &str,
        library: loader::UnitLibrary,
        late: bool,
    ) -> Result<crate::lisp::types::Value, LispError> {
        loader::load(
            &mut self.registry,
            &mut self.runtime,
            interp,
            env,
            filename,
            library,
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
            &mut self.registry,
            &mut self.runtime,
            interp,
            env,
            record_id,
            arguments,
        )
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
        let unit = UnitData::read(interp, env)?;
        let compiler = self
            .compiler
            .as_mut()
            .ok_or_else(|| super::lisp::native_ice("compiler context is not initialized"))?;
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
