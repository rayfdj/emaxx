use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{SubjectLock, acquire_subject_lock, sha256_file, write_json};

const EGLOT: &str = "test/lisp/progmodes/eglot-tests.el";
const MODULE: &str = "test/src/emacs-module-tests.el";

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct Evidence {
    tools: BTreeMap<String, Tool>,
    inputs_sha256: BTreeMap<String, String>,
    module_sha256: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Tool {
    executable: PathBuf,
    sha256: String,
    version: String,
}

#[derive(Default)]
pub(super) struct Prerequisites {
    pub evidence: Evidence,
    tool_directory: Option<PathBuf>,
    module: Option<PathBuf>,
    outputs: BTreeMap<PathBuf, String>,
    source: PathBuf,
    _module_lock: Option<SubjectLock>,
}

pub(super) struct PreparedRunner {
    pub binary: PathBuf,
    pub root: Option<PathBuf>,
    pub inputs: BTreeMap<PathBuf, String>,
    _temporary_directory: Option<super::RunnerTempDirectory>,
}

impl PreparedRunner {
    pub fn verify(&self) -> Result<(), String> {
        verify_hashes(&self.inputs)
    }
}

impl Prerequisites {
    pub fn prepare(source: &Path, files: &[PathBuf], artifact: &Path) -> Result<Self, String> {
        let mut prepared = Self {
            source: source.into(),
            ..Self::default()
        };
        if files.iter().any(|file| file.ends_with(EGLOT)) {
            prepared.prepare_language_servers(artifact)?;
        }
        if files.iter().any(|file| file.ends_with(MODULE)) {
            prepared.prepare_module(artifact)?;
        }
        write_json(
            &artifact.join("prerequisites.json"),
            &prepared.evidence,
            "shared prerequisites",
        )?;
        Ok(prepared)
    }

    pub fn configure(&self, command: &mut Command) -> Result<(), String> {
        if let Some(directory) = &self.tool_directory {
            let mut paths = vec![directory.clone()];
            paths.extend(env::split_paths(&env::var_os("PATH").unwrap_or_default()));
            command.env(
                "PATH",
                env::join_paths(paths)
                    .map_err(|error| format!("prepare shared tool PATH: {error}"))?,
            );
        }
        Ok(())
    }

    pub fn verify(&self) -> Result<(), String> {
        for tool in self.evidence.tools.values() {
            verify_hashes(&BTreeMap::from([(
                tool.executable.clone(),
                tool.sha256.clone(),
            )]))?;
        }
        for (path, expected) in &self.evidence.inputs_sha256 {
            if sha256_file(Path::new(path))? != *expected {
                return Err(format!(
                    "prerequisite input changed during execution: {path}"
                ));
            }
        }
        verify_hashes(&self.outputs)
    }

    fn record_input(&mut self, path: &Path) -> Result<(), String> {
        self.evidence
            .inputs_sha256
            .insert(path.display().to_string(), sha256_file(path)?);
        Ok(())
    }

    fn record_tool(&mut self, name: &str, executable: PathBuf) -> Result<PathBuf, String> {
        let version = checked_output(Command::new(&executable).arg("--version"))?;
        self.evidence.tools.insert(
            name.into(),
            Tool {
                sha256: sha256_file(&executable)?,
                executable: executable.clone(),
                version: version.trim().into(),
            },
        );
        Ok(executable)
    }

    fn prepare_language_servers(&mut self, artifact: &Path) -> Result<(), String> {
        // GNU's pinned Rust fixture waits for rustAnalyzer/Indexing. The
        // toolchain is a prerequisite shared by both editors, not an editor
        // restriction or a replacement for real server execution.
        let rust_bin = if let Some(directory) = env::var_os("EMAXX_COMPAT_RUST_BIN") {
            fs::canonicalize(&directory)
                .map_err(|error| format!("resolve Rust toolchain: {error}"))?
        } else {
            let rustc = checked_output(Command::new("rustup").args(["which", "--toolchain", "1.75.0", "rustc"]))
                .map_err(|error| format!("Eglot requires Rust 1.75 with rust-analyzer and rust-src; install with `rustup toolchain install 1.75.0 --profile minimal --component rust-analyzer --component rust-src`: {error}"))?;
            Path::new(rustc.trim())
                .parent()
                .ok_or("rustup returned a path without a parent")?
                .to_path_buf()
        };
        let directory = artifact.join("tools");
        fs::create_dir_all(&directory)
            .map_err(|error| format!("create {}: {error}", directory.display()))?;
        for name in ["cargo", "rustc", "rustdoc", "rust-analyzer"] {
            let executable = fs::canonicalize(rust_bin.join(name))
                .map_err(|error| format!("missing Rust 1.75 component {name}: {error}"))?;
            let executable = self.record_tool(name, executable)?;
            link(&executable, &directory.join(name))?;
            self.outputs
                .insert(directory.join(name), sha256_file(&executable)?);
        }
        if !self.evidence.tools["rustc"]
            .version
            .starts_with("rustc 1.75.0 ")
        {
            return Err("the pinned GNU Eglot fixture requires Rust 1.75.0".into());
        }
        let sysroot = rust_bin.parent().ok_or("Rust toolchain has no sysroot")?;
        if !sysroot.join("lib/rustlib/src/rust/library").is_dir() {
            return Err("Eglot requires rust-src for Rust 1.75.0".into());
        }
        // The analyzer reads source and compiled sysroot libraries as well
        // as its executable. Bind those actual inputs to resume provenance.
        for part in ["lib", "libexec"] {
            let root = sysroot.join(part);
            if root.exists() {
                for entry in walkdir::WalkDir::new(root).follow_links(true) {
                    let entry = entry.map_err(|error| format!("inspect Rust sysroot: {error}"))?;
                    if entry.file_type().is_file() {
                        self.record_input(entry.path())?;
                    }
                }
            }
        }
        let clangd = resolve_program(
            &env::var_os("EMAXX_COMPAT_CLANGD").unwrap_or_else(|| "clangd".into()),
        )?;
        // /usr/bin/clangd is an Xcode selector, not the language server.
        #[cfg(target_os = "macos")]
        let clangd = if clangd == Path::new("/usr/bin/clangd") {
            PathBuf::from(checked_output(Command::new("xcrun").args(["--find", "clangd"]))?.trim())
        } else {
            clangd
        };
        let clangd = self.record_tool("clangd", clangd)?;
        let launcher = directory.join("clangd");
        write_launcher(&launcher, &clangd, &["--offset-encoding=utf-16"])?;
        self.outputs
            .insert(launcher.clone(), sha256_file(&launcher)?);
        self.tool_directory = Some(directory);
        Ok(())
    }

    fn prepare_module(&mut self, artifact: &Path) -> Result<(), String> {
        let makefile_path = self.source.join("test/Makefile");
        let makefile = fs::read_to_string(&makefile_path)
            .map_err(|error| format!("read configured GNU test Makefile: {error}"))?;
        let compiler = shlex::split(make_variable(&makefile, "CC")?)
            .ok_or("invalid configured GNU CC command")?;
        let compiler = compiler.first().ok_or("configured GNU CC is empty")?;
        self.record_tool("module-compiler", resolve_program(compiler.as_ref())?)?;
        self.record_tool("make", resolve_program("make".as_ref())?)?;
        for relative in [
            MODULE,
            "test/src/emacs-module-resources/mod-test.c",
            "test/Makefile",
            "src/emacs-module.h",
            "src/config.h",
            "lib/mini-gmp.c",
            "lib/mini-gmp.h",
        ] {
            self.record_input(&self.source.join(relative))?;
        }
        let key = format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&self.evidence).map_err(|error| error.to_string())?)
        );
        // Stable build paths make compiler output reproducible for resume.
        // GNU make owns dependency checking; the completed library is always
        // hashed and separately copied into each editor's test installation.
        let build = super::compat::project_root()
            .join("target/compat-fixtures")
            .join(key);
        let fixture = build.join("test/src/emacs-module-resources");
        fs::create_dir_all(&fixture)
            .map_err(|error| format!("create module build directory: {error}"))?;
        fs::create_dir_all(build.join("src"))
            .map_err(|error| format!("create module include directory: {error}"))?;
        self._module_lock = Some(acquire_subject_lock(&build)?);
        for name in ["emacs-module.h", "config.h"] {
            fs::copy(
                self.source.join("src").join(name),
                build.join("src").join(name),
            )
            .map_err(|error| format!("copy GNU module header {name}: {error}"))?;
        }
        fs::copy(
            self.source
                .join("test/src/emacs-module-resources/mod-test.c"),
            fixture.join("mod-test.c"),
        )
        .map_err(|error| format!("copy unchanged GNU module source: {error}"))?;
        let suffix = make_variable(&makefile, "SO")?;
        if ![".dylib", ".so"].contains(&suffix) {
            return Err(format!("unsupported configured module suffix {suffix}"));
        }
        let mut make = Command::new(&self.evidence.tools["make"].executable);
        make.current_dir(build.join("test"))
            .arg("-f")
            .arg(&makefile_path)
            .arg(format!("srcdir={}/test", self.source.display()))
            .arg(format!("src/emacs-module-resources/mod-test{suffix}"))
            .env_remove("MAKEFLAGS")
            .env_remove("MFLAGS");
        let output = make
            .output()
            .map_err(|error| format!("build GNU module fixture: {error}"))?;
        let log = format!(
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        fs::write(artifact.join("module-build.log"), &log)
            .map_err(|error| format!("write module build log: {error}"))?;
        if !output.status.success() {
            return Err(format!("GNU module fixture build failed: {log}"));
        }
        let module = fixture.join(format!("mod-test{suffix}"));
        let hash = sha256_file(&module)?;
        self.evidence.module_sha256 = Some(hash.clone());
        self.outputs.insert(module.clone(), hash);
        self.module = Some(module);
        Ok(())
    }

    pub fn runner(
        &self,
        binary: &Path,
        runner: &str,
        relative: &str,
    ) -> Result<PreparedRunner, String> {
        if relative != MODULE {
            return Ok(PreparedRunner {
                binary: binary.into(),
                root: None,
                inputs: BTreeMap::new(),
                _temporary_directory: None,
            });
        }
        let module = self
            .module
            .as_ref()
            .ok_or("module fixture was not prepared")?;
        // Keep installed paths short as well as temporary paths: GNU's
        // help output fills the real module filename before the upstream
        // test abbreviates it. Long artifact paths cause genuine wrapping.
        let temporary_directory = super::make_runner_temp_directory(runner)?;
        let root = temporary_directory.path.clone();
        let binary =
            fs::canonicalize(binary).map_err(|error| format!("resolve runner binary: {error}"))?;
        let source = fs::canonicalize(&self.source)
            .map_err(|error| format!("resolve GNU source: {error}"))?;
        let bin_directory = binary.parent().ok_or("runner binary has no parent")?;
        let ancestor = common_ancestor(bin_directory, &source)?;
        let installed_bin = root.join(
            bin_directory
                .strip_prefix(&ancestor)
                .map_err(|error| error.to_string())?,
        );
        fs::create_dir_all(&installed_bin)
            .map_err(|error| format!("create installed executable directory: {error}"))?;
        let fixture = installed_bin.join("../test/src/emacs-module-resources");
        fs::create_dir_all(&fixture)
            .map_err(|error| format!("create installed module directory: {error}"))?;
        // A portable dump records native units relative to its executable
        // and installation. Preserve that relationship even when the editor
        // binary lives outside the GNU library tree (as Emaxx's Cargo binary
        // does). Moving just the executable would invalidate those paths.
        if source == ancestor {
            for name in ["lisp", "native-lisp", "etc", "lib-src"] {
                if source.join(name).exists() {
                    link(&source.join(name), &root.join(name))?;
                }
            }
        } else {
            let installed_source = root.join(
                source
                    .strip_prefix(&ancestor)
                    .map_err(|error| error.to_string())?,
            );
            fs::create_dir_all(
                installed_source
                    .parent()
                    .ok_or("installed source has no parent")?,
            )
            .map_err(|error| format!("create installed library parent: {error}"))?;
            link(&source, &installed_source)?;
        }
        let executable = installed_bin.join(binary.file_name().ok_or("binary has no filename")?);
        let mut inputs = BTreeMap::new();
        copy_verified(&binary, &executable, &mut inputs)?;
        copy_verified(&dump_path(&binary), &dump_path(&executable), &mut inputs)?;
        copy_verified(
            module,
            &fixture.join(module.file_name().ok_or("module has no filename")?),
            &mut inputs,
        )?;
        Ok(PreparedRunner {
            binary: executable,
            root: Some(root),
            inputs,
            _temporary_directory: Some(temporary_directory),
        })
    }
}

fn common_ancestor(left: &Path, right: &Path) -> Result<PathBuf, String> {
    left.ancestors()
        .find(|parent| right.starts_with(parent))
        .map(Path::to_path_buf)
        .ok_or_else(|| {
            format!(
                "runner and libraries have no common filesystem root: {} and {}",
                left.display(),
                right.display()
            )
        })
}

fn copy_verified(
    source: &Path,
    destination: &Path,
    inputs: &mut BTreeMap<PathBuf, String>,
) -> Result<(), String> {
    let expected = sha256_file(source)?;
    fs::copy(source, destination).map_err(|error| {
        format!(
            "copy {} to {}: {error}",
            source.display(),
            destination.display()
        )
    })?;
    if sha256_file(destination)? != expected {
        return Err(format!("copied input changed: {}", destination.display()));
    }
    inputs.insert(destination.into(), expected);
    Ok(())
}

fn dump_path(binary: &Path) -> PathBuf {
    let mut name = binary.as_os_str().to_os_string();
    name.push(".pdmp");
    PathBuf::from(name)
}

fn verify_hashes(files: &BTreeMap<PathBuf, String>) -> Result<(), String> {
    for (path, expected) in files {
        if sha256_file(path)? != *expected {
            return Err(format!("prepared input changed: {}", path.display()));
        }
    }
    Ok(())
}

fn make_variable<'a>(makefile: &'a str, name: &str) -> Result<&'a str, String> {
    makefile
        .lines()
        .find_map(|line| line.strip_prefix(&format!("{name} = ")))
        .map(str::trim)
        .ok_or_else(|| format!("configured GNU test Makefile has no {name}"))
}

fn checked_output(command: &mut Command) -> Result<String, String> {
    let output = command
        .output()
        .map_err(|error| format!("run prerequisite {:?}: {error}", command.get_program()))?;
    if !output.status.success() {
        return Err(format!(
            "prerequisite {:?} failed: {}",
            command.get_program(),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    String::from_utf8(output.stdout)
        .map_err(|error| format!("prerequisite output is not UTF-8: {error}"))
}

fn resolve_program(program: &std::ffi::OsStr) -> Result<PathBuf, String> {
    let name = Path::new(program);
    let candidates = if name.components().count() > 1 {
        vec![name.into()]
    } else {
        env::split_paths(&env::var_os("PATH").unwrap_or_default())
            .map(|directory| directory.join(name))
            .collect()
    };
    for candidate in candidates {
        let executable = candidate.is_file();
        #[cfg(unix)]
        let executable = executable && {
            use std::os::unix::fs::PermissionsExt;
            fs::metadata(&candidate)
                .is_ok_and(|metadata| metadata.permissions().mode() & 0o111 != 0)
        };
        if executable {
            return fs::canonicalize(&candidate)
                .map_err(|error| format!("resolve {}: {error}", candidate.display()));
        }
    }
    Err(format!(
        "missing required prerequisite executable {}",
        name.display()
    ))
}

#[cfg(unix)]
fn link(source: &Path, destination: &Path) -> Result<(), String> {
    std::os::unix::fs::symlink(source, destination).map_err(|error| {
        format!(
            "link {} to {}: {error}",
            destination.display(),
            source.display()
        )
    })
}

#[cfg(not(unix))]
fn link(_source: &Path, _destination: &Path) -> Result<(), String> {
    Err("ordinary Eglot/module prerequisites currently require a Unix host".into())
}

fn write_launcher(path: &Path, program: &Path, arguments: &[&str]) -> Result<(), String> {
    let command = std::iter::once(
        program
            .to_str()
            .ok_or("language server path is not UTF-8")?,
    )
    .chain(arguments.iter().copied())
    .map(|word| {
        shlex::try_quote(word)
            .map(|word| word.into_owned())
            .map_err(|error| error.to_string())
    })
    .collect::<Result<Vec<_>, _>>()?
    .join(" ");
    fs::write(path, format!("#!/bin/sh\nexec {command} \"$@\"\n"))
        .map_err(|error| format!("write language server launcher: {error}"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o755))
            .map_err(|error| format!("make launcher executable: {error}"))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_prerequisites_fail_instead_of_selecting_fewer_tests() {
        assert!(resolve_program("/definitely-missing-emaxx-audit-server".as_ref()).is_err());
        assert!(make_variable("CC = cc\n", "SO").is_err());
    }

    #[cfg(unix)]
    #[test]
    fn staged_installation_preserves_native_paths_and_gnu_module_lookup() {
        let root = super::super::unique_temp_path("module-layout-control").unwrap();
        let source = root.join("emacs");
        fs::create_dir_all(source.join("src")).unwrap();
        fs::create_dir_all(source.join("native-lisp")).unwrap();
        fs::write(source.join("native-lisp/unit.eln"), "native library").unwrap();
        let module = root.join("mod-test.so");
        fs::write(&module, "compiled module").unwrap();
        let prerequisites = Prerequisites {
            source: source.clone(),
            module: Some(module),
            ..Prerequisites::default()
        };
        for (runner, original, native_relative) in [
            (
                "oracle",
                source.join("src/emacs"),
                "../native-lisp/unit.eln",
            ),
            (
                "emaxx",
                root.join("emaxx/target/gate/emaxx"),
                "../../../emacs/native-lisp/unit.eln",
            ),
        ] {
            fs::create_dir_all(original.parent().unwrap()).unwrap();
            fs::write(&original, "editor executable").unwrap();
            fs::write(dump_path(&original), "portable image").unwrap();
            let prepared = prerequisites.runner(&original, runner, MODULE).unwrap();
            let directory = prepared.binary.parent().unwrap();
            assert_eq!(
                fs::read_to_string(directory.join(native_relative)).unwrap(),
                "native library"
            );
            assert_eq!(
                fs::read_to_string(
                    directory.join("../test/src/emacs-module-resources/mod-test.so")
                )
                .unwrap(),
                "compiled module"
            );
            prepared.verify().unwrap();
        }
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn launcher_preserves_arguments_and_does_not_interpret_path_metacharacters() {
        use std::os::unix::fs::PermissionsExt;
        let root = super::super::unique_temp_path("launcher-control").unwrap();
        fs::create_dir_all(&root).unwrap();
        let executable = root.join("server ' $(false)");
        fs::write(&executable, "#!/bin/sh\nprintf '%s\\n' \"$@\"\n").unwrap();
        fs::set_permissions(&executable, fs::Permissions::from_mode(0o755)).unwrap();
        let launcher = root.join("clangd");
        write_launcher(&launcher, &executable, &["--offset-encoding=utf-16"]).unwrap();
        let output =
            checked_output(Command::new(&launcher).args(["argument with spaces", "$(false)"]))
                .unwrap();
        assert_eq!(
            output,
            "--offset-encoding=utf-16\nargument with spaces\n$(false)\n"
        );
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn changed_prepared_executable_or_symlink_is_rejected() {
        let root = super::super::unique_temp_path("input-control").unwrap();
        fs::create_dir_all(&root).unwrap();
        let first = root.join("first");
        let second = root.join("second");
        let executable = root.join("tool");
        fs::write(&first, "first executable").unwrap();
        fs::write(&second, "other executable").unwrap();
        link(&first, &executable).unwrap();
        let expected = BTreeMap::from([(executable.clone(), sha256_file(&executable).unwrap())]);
        verify_hashes(&expected).unwrap();
        fs::remove_file(&executable).unwrap();
        link(&second, &executable).unwrap();
        assert!(verify_hashes(&expected).is_err());
        fs::remove_dir_all(root).unwrap();
    }
}
