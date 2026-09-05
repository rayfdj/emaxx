//! C-owned library-path initialization. GNU startup.el owns expansion of
//! subdirs.el and command-line -L arguments; neither belongs in this module.

use crate::lisp::eval::Interpreter;
use crate::lisp::primitives;
use crate::lisp::types::{LispError, Value};

fn c(interpreter: &mut Interpreter, name: &str, args: &[Value]) -> Result<Value, LispError> {
    primitives::call(interpreter, name, args, &mut Vec::new())
}

/// emacs.c:decode_env_path. Empty components share the single dot object,
/// or denote insertion of the defaults when EMPTY is true.
fn decode_env_path(
    interpreter: &mut Interpreter,
    path: &[u8],
    empty: bool,
) -> Result<Value, LispError> {
    let empty_element = if empty {
        Value::Nil
    } else {
        Value::string(".")
    };
    let separator = if cfg!(windows) { b';' } else { b':' };
    let mut result = Value::Nil;
    for bytes in path.split(|byte| *byte == separator) {
        let mut element = if bytes.is_empty() {
            empty_element.clone()
        } else {
            primitives::bytes_to_shared_unibyte_value(bytes)
        };
        if !element.is_nil() {
            let mut handler = c(
                interpreter,
                "find-file-name-handler",
                &[element.clone(), Value::T],
            )?;
            if c(interpreter, "symbolp", std::slice::from_ref(&handler))?.is_truthy()
                && c(
                    interpreter,
                    "get",
                    &[handler.clone(), Value::symbol("safe-magic")],
                )?
                .is_truthy()
            {
                handler = Value::Nil;
            }
            if !handler.is_nil() {
                element = c(interpreter, "concat", &[Value::string("/:"), element])?;
            }
        }
        result = Value::cons(element, result);
    }
    c(interpreter, "nreverse", &[result])
}

fn add_directory(
    interpreter: &mut Interpreter,
    path: Value,
    directory: Value,
) -> Result<Value, LispError> {
    if c(interpreter, "member", &[directory.clone(), path.clone()])?.is_nil() {
        Ok(Value::cons(directory, path))
    } else {
        Ok(path)
    }
}

fn expand(
    interpreter: &mut Interpreter,
    name: &str,
    directory: &Value,
) -> Result<Value, LispError> {
    c(
        interpreter,
        "expand-file-name",
        &[Value::string(name), directory.clone()],
    )
}

/// lread.c:load_path_default. The installed constants below are the
/// /usr/local configure defaults in both pinned oracle build contracts
/// (epaths.h PATH_LOADSEARCH and PATH_SITELOADSEARCH), not a discovered or
/// oracle-probed list of Lisp subdirectories.
fn default_load_path(
    interpreter: &mut Interpreter,
    dump_path: &Value,
    will_dump: bool,
    no_site_lisp: bool,
) -> Result<Value, LispError> {
    if will_dump {
        return Ok(dump_path.clone());
    }
    let installed = format!(
        "/usr/local/share/emacs/{}/lisp",
        primitives::emacs_version_value()
    );
    let mut path = decode_env_path(interpreter, installed.as_bytes(), false)?;
    let installation = interpreter.symbol_value_cell("installation-directory")?;
    if !installation.is_nil() {
        let directory = expand(interpreter, "lisp", &installation)?;
        if c(
            interpreter,
            "file-accessible-directory-p",
            std::slice::from_ref(&directory),
        )?
        .is_truthy()
        {
            if c(interpreter, "member", &[directory.clone(), path.clone()])?.is_nil() {
                path = Value::list([directory]);
            }
        } else {
            path = c(interpreter, "nconc", &[path, dump_path.clone()])?;
        }
        if !no_site_lisp {
            let site = expand(interpreter, "site-lisp", &installation)?;
            if c(
                interpreter,
                "file-accessible-directory-p",
                std::slice::from_ref(&site),
            )?
            .is_truthy()
            {
                path = add_directory(interpreter, path, site)?;
            }
        }
        let source = interpreter.symbol_value_cell("source-directory")?;
        if c(
            interpreter,
            "equal",
            &[installation.clone(), source.clone()],
        )?
        .is_nil()
        {
            let makefile = expand(interpreter, "src/Makefile", &installation)?;
            let exists = c(interpreter, "file-exists-p", &[makefile])?;
            let template = expand(interpreter, "src/Makefile.in", &installation)?;
            let has_template = c(interpreter, "file-exists-p", &[template])?;
            if exists.is_truthy() && has_template.is_nil() {
                let directory = expand(interpreter, "lisp", &source)?;
                path = add_directory(interpreter, path, directory)?;
                if !no_site_lisp {
                    let site = expand(interpreter, "site-lisp", &source)?;
                    if c(
                        interpreter,
                        "file-accessible-directory-p",
                        std::slice::from_ref(&site),
                    )?
                    .is_truthy()
                    {
                        path = add_directory(interpreter, path, site)?;
                    }
                }
            }
        }
    }
    Ok(path)
}

fn check_load_path(
    interpreter: &mut Interpreter,
    path: &Value,
    initialized: bool,
) -> Result<(), LispError> {
    for directory in path.to_vec()? {
        if !directory.is_string() {
            continue;
        }
        let checked = c(
            interpreter,
            "directory-file-name",
            std::slice::from_ref(&directory),
        )?;
        // lread.c calls the internal fileio.c helper here, not the Lisp
        // primitive: no expansion, file coding conversion, or handler call.
        let bytes =
            primitives::encode_internal_multibyte_bytes(&primitives::string_text(&checked)?)?;
        if let Err(error) = primitives::accessible_directory(&bytes) {
            let reason = primitives::errno_text(&error);
            let text = format!(
                "Warning: Lisp directory '{}': {reason}\n",
                primitives::string_text(&directory)?
            );
            eprint!("{text}");
            if initialized {
                interpreter.append_message_capture(&text, false, &mut Vec::new());
            }
        }
    }
    Ok(())
}

fn add_site_load_path(
    interpreter: &mut Interpreter,
    path: Value,
    no_site_lisp: bool,
) -> Result<Value, LispError> {
    if no_site_lisp {
        return Ok(path);
    }
    let sites = format!(
        "/usr/local/share/emacs/{}/site-lisp:/usr/local/share/emacs/site-lisp",
        primitives::emacs_version_value()
    );
    let sites = decode_env_path(interpreter, sites.as_bytes(), false)?;
    c(interpreter, "nconc", &[sites, path])
}

/// lread.c:init_lread, after reconstructed persistent state and before the
/// new process evaluates GNU's stored top-level form.
pub(crate) fn initialize_load_path(
    interpreter: &mut Interpreter,
    dump_path: Value,
    will_dump: bool,
    no_site_lisp: bool,
) -> Result<(), LispError> {
    let environment = (!will_dump)
        .then(|| std::env::var_os("EMACSLOADPATH"))
        .flatten();
    let path = if let Some(environment) = environment {
        let environment = decode_env_path(interpreter, environment.as_encoded_bytes(), true)?;
        check_load_path(interpreter, &environment, true)?;
        if c(interpreter, "memq", &[Value::Nil, environment.clone()])?.is_nil() {
            environment
        } else {
            let defaults = default_load_path(interpreter, &dump_path, false, no_site_lisp)?;
            check_load_path(interpreter, &defaults, true)?;
            let defaults = add_site_load_path(interpreter, defaults, no_site_lisp)?;
            let mut result = Value::Nil;
            for element in environment.to_vec()? {
                let tail = if element.is_nil() {
                    defaults.clone()
                } else {
                    Value::list([element])
                };
                result = c(interpreter, "append", &[result, tail])?;
            }
            result
        }
    } else {
        let defaults = default_load_path(interpreter, &dump_path, will_dump, no_site_lisp)?;
        check_load_path(interpreter, &defaults, !will_dump)?;
        add_site_load_path(interpreter, defaults, will_dump || no_site_lisp)?
    };
    interpreter.set_load_path_value(path);
    for name in ["values", "load-file-name", "load-true-file-name"] {
        interpreter.set_symbol_value_cell(name, Value::Nil);
    }
    interpreter.set_symbol_value_cell("standard-input", Value::T);
    interpreter.set_current_load_file(None);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(unix)]
    fn startup_directory_check_uses_search_permission_and_preserves_errno() {
        use std::os::unix::fs::PermissionsExt;
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let root =
            std::env::temp_dir().join(format!("directory-access-{}-{stamp}", std::process::id()));
        std::fs::create_dir(&root).expect("create this test's directory");
        let regular = root.join("regular");
        std::fs::write(&regular, b"").expect("create ordinary file control");
        let regular_result =
            primitives::accessible_directory(regular.as_os_str().as_encoded_bytes());
        let missing =
            primitives::accessible_directory(root.join("missing").as_os_str().as_encoded_bytes());
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o100))
            .expect("make directory searchable only");
        let searchable = primitives::accessible_directory(root.as_os_str().as_encoded_bytes());
        let mut interpreter = Interpreter::new();
        let public = c(
            &mut interpreter,
            "file-accessible-directory-p",
            &[Value::String(root.display().to_string().into())],
        );
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
            .expect("restore test directory permissions");
        std::fs::remove_dir_all(&root).expect("remove only the test-owned directory");
        searchable.expect("search permission is sufficient without read permission");
        assert_eq!(public.expect("public accessible-directory query"), Value::T);
        assert_eq!(
            regular_result
                .expect_err("a regular file is not a directory")
                .raw_os_error(),
            Some(libc::ENOTDIR)
        );
        assert_eq!(
            missing.expect_err("missing directory").raw_os_error(),
            Some(libc::ENOENT)
        );
        assert_eq!(
            primitives::accessible_directory(b"")
                .expect_err("empty path")
                .raw_os_error(),
            Some(libc::ENOENT)
        );
        primitives::accessible_directory(b"/").expect("root remains root");
        primitives::accessible_directory(b"//").expect("preserve a double-slash root");
    }

    #[test]
    fn decode_env_path_preserves_empty_components_and_shared_dot() {
        let mut interpreter = Interpreter::new();
        let dots = decode_env_path(&mut interpreter, b":a::", false).expect("decode dot entries");
        let entries = dots.to_vec().expect("proper path");
        assert_eq!(
            entries,
            [
                Value::string("."),
                Value::string("a"),
                Value::string("."),
                Value::string(".")
            ]
        );
        assert!(primitives::values_eq_in_env(
            &interpreter,
            &entries[0],
            &entries[2],
            &Vec::new()
        ));
        let nils =
            decode_env_path(&mut interpreter, b":a::", true).expect("decode default insertions");
        assert_eq!(
            nils,
            Value::list([Value::Nil, Value::string("a"), Value::Nil, Value::Nil])
        );
    }

    #[test]
    fn decode_env_path_quotes_only_unsafe_file_handlers() {
        let mut interpreter = Interpreter::new();
        let handler = Value::symbol("identity");
        interpreter.set_global_binding(
            "file-name-handler-alist",
            Value::list([Value::cons(Value::string("remote"), handler.clone())]),
        );
        let quoted =
            decode_env_path(&mut interpreter, b"remote", false).expect("quote unsafe handler");
        assert_eq!(quoted, Value::list([Value::string("/:remote")]));
        c(
            &mut interpreter,
            "put",
            &[handler, Value::symbol("safe-magic"), Value::T],
        )
        .expect("mark safe handler");
        let unquoted =
            decode_env_path(&mut interpreter, b"remote", false).expect("retain safe path");
        assert_eq!(unquoted, Value::list([Value::string("remote")]));
    }

    #[test]
    fn decode_env_path_retains_the_unibyte_environment_bytes() {
        let mut interpreter = Interpreter::new();
        let path =
            decode_env_path(&mut interpreter, "é".as_bytes(), false).expect("decode UTF-8 bytes");
        let element = path.car().expect("one path element");
        assert_eq!(
            c(&mut interpreter, "length", std::slice::from_ref(&element)).expect("byte characters"),
            Value::Integer(2)
        );
        assert_eq!(
            c(
                &mut interpreter,
                "string-bytes",
                std::slice::from_ref(&element)
            )
            .expect("raw byte count"),
            Value::Integer(2)
        );
        assert_eq!(
            c(&mut interpreter, "multibyte-string-p", &[element]).expect("unibyte flag"),
            Value::Nil
        );
    }

    #[test]
    fn initialize_load_path_uses_gnu_empty_entry_and_dump_phase_rules() {
        let _environment = crate::compat::lock_boot_environment_for_write();
        let previous = std::env::var_os("EMACSLOADPATH");
        let root = crate::compat::project_root()
            .join("../emacs/lisp")
            .canonicalize()
            .expect("GNU Lisp source root");
        let root_value = Value::String(root.display().to_string().into());
        let dump_path = Value::list([root_value.clone()]);
        // SAFETY: bootstrap environment write lock excludes other bootstraps.
        unsafe {
            std::env::set_var("EMACSLOADPATH", format!(":{}:", root.display()));
        }
        let mut interpreter = Interpreter::new();
        let initialized = initialize_load_path(&mut interpreter, dump_path.clone(), false, true);
        let session = interpreter.symbol_value_cell("load-path");
        let dumping = initialize_load_path(&mut interpreter, dump_path.clone(), true, false);
        let dump = interpreter.symbol_value_cell("load-path");
        // SAFETY: restore the process environment under the same lock.
        unsafe {
            match previous {
                Some(value) => std::env::set_var("EMACSLOADPATH", value),
                None => std::env::remove_var("EMACSLOADPATH"),
            }
        }
        initialized.expect("initialize live-session path");
        let entries = session
            .expect("session path bound")
            .to_vec()
            .expect("proper session list");
        assert_eq!(
            entries,
            [root_value.clone(), root_value.clone(), root_value]
        );
        assert!(
            primitives::values_eq_in_env(&interpreter, &entries[0], &entries[2], &Vec::new()),
            "each default insertion retains the original directory string"
        );
        dumping.expect("initialize dump-build path");
        assert_eq!(
            dump.expect("dump path bound"),
            dump_path,
            "dumping ignores EMACSLOADPATH and all site-lisp additions"
        );
    }

    #[test]
    fn default_load_path_follows_uninstalled_and_out_of_tree_rules() {
        let stamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let root =
            std::env::temp_dir().join(format!("load-path-contract-{}-{stamp}", std::process::id()));
        let build = root.join("build");
        let source = root.join("source");
        for directory in [
            build.join("src"),
            build.join("lisp"),
            build.join("site-lisp"),
            source.join("lisp"),
            source.join("site-lisp"),
        ] {
            std::fs::create_dir_all(directory).expect("create path contract directories");
        }
        std::fs::write(build.join("src/Makefile"), b"").expect("mark an out-of-tree build");
        let directory = |path: &std::path::Path| Value::String(path.display().to_string().into());
        let mut interpreter = Interpreter::new();
        interpreter.set_global_binding("installation-directory", directory(&build));
        interpreter.set_global_binding("source-directory", directory(&source));
        let dump = Value::list([directory(&source.join("lisp"))]);
        let without_site = default_load_path(&mut interpreter, &dump, false, true);
        let with_site = default_load_path(&mut interpreter, &dump, false, false);
        std::fs::write(build.join("src/Makefile.in"), b"").expect("mark a moved source tree");
        let moved_source = default_load_path(&mut interpreter, &dump, false, true);
        std::fs::remove_dir_all(&root).expect("remove only this test's directory tree");
        assert_eq!(
            without_site.expect("out-of-tree default path"),
            Value::list([
                directory(&source.join("lisp")),
                directory(&build.join("lisp"))
            ])
        );
        assert_eq!(
            with_site.expect("out-of-tree site directories"),
            Value::list([
                directory(&source.join("site-lisp")),
                directory(&source.join("lisp")),
                directory(&build.join("site-lisp")),
                directory(&build.join("lisp"))
            ])
        );
        assert_eq!(
            moved_source.expect("moved source tree sanity check"),
            Value::list([directory(&build.join("lisp"))])
        );
    }
}
