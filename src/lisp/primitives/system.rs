use super::*;

pub(crate) fn json_parse_options(args: &[Value]) -> Result<JsonParseOptions, LispError> {
    let mut options = JsonParseOptions {
        object_type: JsonObjectType::HashTable,
        array_type: JsonArrayType::Vector,
        null_object: Value::Symbol(":null".into()),
        false_object: Value::Symbol(":false".into()),
    };
    let mut index = 0usize;
    while index + 1 < args.len() {
        let key = args[index].as_symbol()?;
        let value = args[index + 1].clone();
        match key {
            ":object-type" => {
                options.object_type = match &value {
                    Value::Symbol(symbol) if symbol == "hash-table" => JsonObjectType::HashTable,
                    Value::Symbol(symbol) if symbol == "alist" => JsonObjectType::Alist,
                    Value::Symbol(symbol) if symbol == "plist" => JsonObjectType::Plist,
                    other => {
                        return Err(LispError::TypeError("symbol".into(), other.type_name()));
                    }
                };
            }
            ":array-type" => {
                options.array_type = match &value {
                    Value::Symbol(symbol) if symbol == "vector" => JsonArrayType::Vector,
                    Value::Symbol(symbol) if symbol == "list" => JsonArrayType::List,
                    other => {
                        return Err(LispError::TypeError("symbol".into(), other.type_name()));
                    }
                };
            }
            ":null-object" => options.null_object = value,
            ":false-object" => options.false_object = value,
            _ => {
                return Err(LispError::TypeError("json-option".into(), key.into()));
            }
        }
        index += 2;
    }
    Ok(options)
}

pub(crate) fn json_serialize_options(args: &[Value]) -> Result<(Value, Value), LispError> {
    let mut null_object = Value::Symbol(":null".into());
    let mut false_object = Value::Symbol(":false".into());
    let mut index = 0usize;
    while index + 1 < args.len() {
        let key = args[index].as_symbol()?;
        let value = args[index + 1].clone();
        match key {
            ":null-object" => null_object = value,
            ":false-object" => false_object = value,
            _ => {
                return Err(LispError::TypeError("json-option".into(), key.into()));
            }
        }
        index += 2;
    }
    Ok((null_object, false_object))
}

pub(crate) fn current_group_id() -> Result<u32, LispError> {
    #[cfg(unix)]
    {
        // SAFETY: getegid has no preconditions and does not dereference memory.
        Ok(unsafe { libc::getegid() } as u32)
    }
    #[cfg(not(unix))]
    {
        let output = Command::new("id")
            .arg("-g")
            .output()
            .map_err(|error| LispError::Signal(error.to_string()))?;
        if !output.status.success() {
            return Err(LispError::Signal("Failed to determine current gid".into()));
        }
        let value = String::from_utf8_lossy(&output.stdout);
        value
            .trim()
            .parse::<u32>()
            .map_err(|error| LispError::Signal(error.to_string()))
    }
}

pub(crate) fn current_user_id() -> Result<u32, LispError> {
    #[cfg(unix)]
    {
        // SAFETY: geteuid has no preconditions and does not dereference memory.
        Ok(unsafe { libc::geteuid() } as u32)
    }
    #[cfg(not(unix))]
    {
        let output = Command::new("id")
            .arg("-u")
            .output()
            .map_err(|error| LispError::Signal(error.to_string()))?;
        if !output.status.success() {
            return Err(LispError::Signal("Failed to determine current uid".into()));
        }
        let value = String::from_utf8_lossy(&output.stdout);
        value
            .trim()
            .parse::<u32>()
            .map_err(|error| LispError::Signal(error.to_string()))
    }
}

pub(crate) fn current_real_group_id() -> Result<u32, LispError> {
    #[cfg(unix)]
    {
        // SAFETY: getgid has no preconditions and does not dereference memory.
        Ok(unsafe { libc::getgid() } as u32)
    }
    #[cfg(not(unix))]
    {
        current_group_id()
    }
}

pub(crate) fn current_real_user_id() -> Result<u32, LispError> {
    #[cfg(unix)]
    {
        // SAFETY: getuid has no preconditions and does not dereference memory.
        Ok(unsafe { libc::getuid() } as u32)
    }
    #[cfg(not(unix))]
    {
        current_user_id()
    }
}

#[cfg(unix)]
fn user_name_from_uid(uid: u32) -> Option<String> {
    let mut passwd = std::mem::MaybeUninit::<libc::passwd>::uninit();
    let mut result = std::ptr::null_mut();
    // getpwuid_r uses caller-owned scratch storage and no process-global
    // passwd cursor, so concurrent interpreters cannot disturb this lookup.
    let mut scratch = vec![0_i8; 16 * 1024];
    // SAFETY: all pointers refer to appropriately sized caller-owned storage
    // for the duration of this reentrant libc call.
    if unsafe {
        libc::getpwuid_r(
            uid as libc::uid_t,
            passwd.as_mut_ptr(),
            scratch.as_mut_ptr(),
            scratch.len(),
            &mut result,
        )
    } != 0
        || result.is_null()
    {
        return None;
    }
    // SAFETY: successful getpwuid_r initialized passwd, whose name points
    // into scratch until this function returns.
    let name = unsafe { std::ffi::CStr::from_ptr((*passwd.as_ptr()).pw_name) };
    Some(name.to_string_lossy().into_owned())
}

pub(crate) fn group_name_from_gid(gid: i64) -> Result<Option<String>, LispError> {
    if cfg!(target_os = "macos") {
        let output = Command::new("dscacheutil")
            .args(["-q", "group", "-a", "gid", &gid.to_string()])
            .output();
        if let Ok(output) = output
            && output.status.success()
        {
            let text = String::from_utf8_lossy(&output.stdout);
            for line in text.lines() {
                if let Some(name) = line.strip_prefix("name:") {
                    return Ok(Some(name.trim().to_string()));
                }
            }
        }
    }

    let output = Command::new("getent")
        .args(["group", &gid.to_string()])
        .output();
    if let Ok(output) = output
        && output.status.success()
    {
        let text = String::from_utf8_lossy(&output.stdout);
        if let Some(name) = text.split(':').next()
            && !name.is_empty()
        {
            return Ok(Some(name.to_string()));
        }
    }

    if let Ok(groups) = std::fs::read_to_string("/etc/group") {
        for line in groups.lines() {
            let mut parts = line.split(':');
            let Some(name) = parts.next() else { continue };
            let _ = parts.next();
            let Some(entry_gid) = parts.next() else {
                continue;
            };
            if entry_gid.parse::<i64>().ok() == Some(gid) {
                return Ok(Some(name.to_string()));
            }
        }
    }

    Ok(None)
}

pub(crate) fn find_executable(name: &str) -> Option<String> {
    if name.contains(std::path::MAIN_SEPARATOR) && std::path::Path::new(name).exists() {
        return Some(name.to_string());
    }
    let path = std::env::var_os("PATH")?;
    for entry in std::env::split_paths(&path) {
        let candidate = entry.join(name);
        if candidate.exists() {
            return Some(candidate.display().to_string());
        }
    }
    None
}

pub(crate) fn default_directory() -> String {
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    path_to_directory_string(&cwd)
}

pub(crate) fn default_system_configuration() -> String {
    let machine = uname_value("-m").unwrap_or_else(|| std::env::consts::ARCH.to_string());
    match std::env::consts::OS {
        "macos" => {
            let release = uname_value("-r").unwrap_or_else(|| "0".into());
            format!("{machine}-apple-darwin{release}")
        }
        "linux" => format!("{machine}-unknown-linux-gnu"),
        "freebsd" => format!("{machine}-unknown-freebsd"),
        "windows" => format!("{machine}-pc-windows-msvc"),
        os => format!("{machine}-{os}"),
    }
}

pub(crate) fn uname_value(flag: &str) -> Option<String> {
    let output = Command::new("uname").arg(flag).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let value = String::from_utf8(output.stdout).ok()?;
    let trimmed = value.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

pub(crate) fn compat_repo_root_from_test_directory(test_directory: &str) -> Option<PathBuf> {
    PathBuf::from(test_directory)
        .parent()
        .map(Path::to_path_buf)
}

pub(crate) fn compat_data_directory() -> Option<String> {
    std::env::var("EMACS_TEST_DIRECTORY")
        .ok()
        .and_then(|test_directory| compat_repo_root_from_test_directory(&test_directory))
        .map(|repo_root| path_to_directory_string(&repo_root.join("etc")))
}

pub(crate) fn compat_installation_directory() -> Option<String> {
    std::env::var("EMACS_TEST_DIRECTORY")
        .ok()
        .and_then(|test_directory| compat_repo_root_from_test_directory(&test_directory))
        .map(|repo_root| path_to_directory_string(&repo_root))
}

pub(crate) fn compat_emacsclient_path_from_test_directory(test_directory: &str) -> Option<PathBuf> {
    let repo_root = compat_repo_root_from_test_directory(test_directory)?;
    let candidate = repo_root.join("lib-src").join("emacsclient");
    candidate.exists().then_some(candidate)
}

pub(crate) fn compat_emacsclient_program_name() -> Option<String> {
    std::env::var("EMACS_TEST_DIRECTORY")
        .ok()
        .and_then(|test_directory| compat_emacsclient_path_from_test_directory(&test_directory))
        .map(|path| path.display().to_string())
}

pub(crate) fn current_invocation_name() -> Option<String> {
    current_invocation_path()
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
}

pub(crate) fn current_invocation_directory() -> Option<String> {
    current_invocation_path()
        .parent()
        .map(path_to_directory_string)
}

pub(crate) fn current_invocation_path() -> PathBuf {
    std::env::current_exe().unwrap_or_else(|_| PathBuf::from("emaxx"))
}

pub(crate) fn command_line_args_value() -> Value {
    let invocation = current_invocation_path().display().to_string();
    let mut args = std::env::args_os()
        .map(|value| Value::String(value.to_string_lossy().into_owned()))
        .collect::<Vec<_>>();
    if let Some(first) = args.first_mut() {
        *first = Value::String(invocation);
    } else {
        args.push(Value::String(invocation));
    }
    Value::list(args)
}

pub(crate) fn emacs_pid_value() -> i64 {
    i64::from(std::process::id())
}

fn process_inventory() -> sysinfo::System {
    let mut system = sysinfo::System::new();
    system.refresh_processes_specifics(
        sysinfo::ProcessesToUpdate::All,
        true,
        sysinfo::ProcessRefreshKind::everything().without_tasks(),
    );
    system
}

pub(crate) fn list_system_processes_value() -> Value {
    let system = process_inventory();
    let mut pids = system
        .processes()
        .keys()
        .map(|pid| i64::from(pid.as_u32()))
        .collect::<Vec<_>>();
    pids.sort_unstable();
    Value::list(pids.into_iter().map(Value::Integer))
}

fn old_style_process_time(ticks: u64, ticks_per_second: u64) -> Value {
    let exact = exact_time_value(BigInt::from(ticks), BigInt::from(ticks_per_second))
        .expect("positive process time resolution");
    exact_time_to_old_style(&exact).expect("process times fit GNU's old-style time representation")
}

fn process_state_code(status: sysinfo::ProcessStatus) -> &'static str {
    use sysinfo::ProcessStatus;

    match status {
        ProcessStatus::Idle => "I",
        ProcessStatus::Run => "R",
        ProcessStatus::Sleep | ProcessStatus::Suspended => "S",
        ProcessStatus::Stop | ProcessStatus::Tracing => "T",
        ProcessStatus::Zombie => "Z",
        ProcessStatus::Dead => "X",
        ProcessStatus::Wakekill | ProcessStatus::Waking => "W",
        ProcessStatus::Parked => "P",
        ProcessStatus::LockBlocked | ProcessStatus::UninterruptibleDiskSleep => "D",
        ProcessStatus::Unknown(_) => "?",
    }
}

pub(crate) fn process_attributes_value(pid: i64) -> Value {
    let Ok(pid) = u32::try_from(pid) else {
        return Value::Nil;
    };
    let system = process_inventory();
    let Some(process) = system.process(sysinfo::Pid::from_u32(pid)) else {
        return Value::Nil;
    };
    let mut attributes = Vec::new();
    let mut push = |name: &str, value: Value| {
        attributes.push(Value::cons(Value::symbol(name), value));
    };

    #[cfg(unix)]
    {
        if let Some(uid) = process.effective_user_id().or_else(|| process.user_id()) {
            let uid = **uid;
            push("euid", Value::Integer(i64::from(uid)));
            if let Some(user) = user_name_from_uid(uid) {
                push("user", Value::String(user));
            }
        }
        if let Some(gid) = process.effective_group_id().or_else(|| process.group_id()) {
            push("egid", Value::Integer(i64::from(*gid)));
            if let Ok(Some(group)) = group_name_from_gid(i64::from(*gid)) {
                push("group", Value::String(group));
            }
        }
    }

    push(
        "comm",
        Value::String(process.name().to_string_lossy().into_owned()),
    );
    push(
        "state",
        Value::String(process_state_code(process.status()).into()),
    );
    if let Some(parent) = process.parent() {
        push("ppid", Value::Integer(i64::from(parent.as_u32())));
    }
    #[cfg(unix)]
    {
        let raw_pid = pid as libc::pid_t;
        // SAFETY: getpgid and getsid accept an integer process id and do not
        // dereference caller-owned memory.
        let process_group = unsafe { libc::getpgid(raw_pid) };
        if process_group >= 0 {
            push("pgrp", Value::Integer(i64::from(process_group)));
        }
        // SAFETY: same contract as getpgid above.
        let session = unsafe { libc::getsid(raw_pid) };
        if session >= 0 {
            push("sess", Value::Integer(i64::from(session)));
        }
    }
    if let Some(tasks) = process.tasks() {
        push("thcount", Value::Integer(tasks.len() as i64));
    }
    push("start", old_style_process_time(process.start_time(), 1));
    push(
        "vsize",
        Value::Integer((process.virtual_memory() / 1024) as i64),
    );
    push("rss", Value::Integer((process.memory() / 1024) as i64));
    push("etime", old_style_process_time(process.run_time(), 1));
    let accumulated_millis = process.accumulated_cpu_time();
    push("time", old_style_process_time(accumulated_millis, 1_000));
    let command = process
        .cmd()
        .iter()
        .map(|argument| argument.to_string_lossy())
        .collect::<Vec<_>>()
        .join(" ");
    if !command.is_empty() {
        push("args", Value::String(command));
    }
    Value::list(attributes)
}

pub(crate) fn expand_file_name(path: &str, base: Option<&str>) -> String {
    let home = std::env::var("HOME").ok();
    expand_file_name_with_home(path, base, home.as_deref())
}

fn expand_file_name_with_home(path: &str, base: Option<&str>, home: Option<&str>) -> String {
    let preserve_directory_syntax = path.ends_with(std::path::MAIN_SEPARATOR);
    let expanded = expand_home_prefix_with_home(path, home);
    let candidate = PathBuf::from(expanded);
    let absolute = if candidate.is_absolute() {
        candidate
    } else {
        let base_dir = base
            .map(|base| PathBuf::from(expand_home_prefix_with_home(base, home)))
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| PathBuf::from(default_directory()));
        if base_dir.is_absolute() {
            base_dir.join(candidate)
        } else {
            std::env::current_dir()
                .unwrap_or_else(|_| PathBuf::from("."))
                .join(base_dir)
                .join(candidate)
        }
    };
    if preserve_directory_syntax {
        path_to_directory_string(&absolute)
    } else {
        normalize_path(&absolute).display().to_string()
    }
}

pub(crate) fn expand_file_name_runtime(
    interp: &mut Interpreter,
    env: &mut Env,
    path: &str,
    base: Option<&str>,
) -> Result<String, LispError> {
    validate_file_name(path)?;
    if let Some(base) = base {
        validate_file_name(base)?;
    }
    let handler =
        if let Some(handler) = find_file_name_handler(interp, env, path, "expand-file-name")? {
            Some(handler)
        } else if let Some(base) = base {
            find_file_name_handler(interp, env, base, "expand-file-name")?
        } else {
            None
        };
    if let Some(handler) = handler {
        let function = match handler {
            Value::Symbol(symbol) => interp.lookup_function(&symbol, env)?,
            other => other,
        };
        let handled = call_function_value(
            interp,
            &function,
            &[
                Value::Symbol("expand-file-name".into()),
                Value::String(path.to_string()),
                base.map(|value| Value::String(value.to_string()))
                    .unwrap_or(Value::Nil),
            ],
            env,
        )?;
        return string_text(&handled);
    }
    Ok(expand_file_name_in_env(interp, env, path, base))
}

pub(crate) fn resolve_file_name_in_env(interp: &Interpreter, env: &Env, path: &str) -> String {
    // `/:` quotes a local name so Lisp file-name handlers are bypassed.  Keep
    // the marker in lexical file-name operations, but remove it at the one
    // boundary where a name becomes a host path.
    if let Some(unquoted) = unquote_local_file_name(path) {
        return expand_file_name_in_env(interp, env, &unquoted, None);
    }
    if let Some(remote) = parse_remote_file_name(path) {
        return resolved_remote_localname_in_env(interp, env, &remote);
    }
    if Path::new(path).is_absolute() {
        return path.to_string();
    }
    let base = interp
        .lookup_var("default-directory", env)
        .and_then(|value| string_like(&value).map(|string| string.text));
    let base = base
        .as_deref()
        .and_then(|base| unquote_local_file_name(base).or_else(|| Some(base.to_string())));
    let expanded = expand_file_name_in_env(interp, env, path, base.as_deref());
    unquote_local_file_name(&expanded).unwrap_or(expanded)
}

/// Return the canonical name recorded in `buffer-file-truename'.
///
/// Visiting APIs keep the spelling requested by Lisp in `buffer-file-name',
/// while this companion value resolves host aliases such as macOS's
/// `/var' -> `/private/var' symlink.  If the final component does not exist,
/// retain the expanded name, matching `file-truename' at our current host
/// abstraction boundary.
pub(crate) fn canonical_file_name(path: &str) -> String {
    fs::canonicalize(path)
        .map(|path| path.to_string_lossy().into_owned())
        .unwrap_or_else(|_| path.to_string())
}

pub(crate) fn unquote_local_file_name(path: &str) -> Option<String> {
    path.strip_prefix("/:")
        .map(|rest| if rest.is_empty() { "/" } else { rest }.to_string())
}

pub(crate) fn expand_file_name_in_env(
    interp: &Interpreter,
    env: &Env,
    path: &str,
    base: Option<&str>,
) -> String {
    let home = lisp_environment_string(interp, env, "HOME");
    expand_file_name_with_home(path, base, home.as_deref())
}

pub(crate) fn resolved_remote_localname_in_env(
    interp: &Interpreter,
    env: &Env,
    remote: &RemoteFileNameParts,
) -> String {
    if remote.method == "mock" {
        let home = lisp_environment_string(interp, env, "HOME");
        expand_home_prefix_with_home(&remote.localname, home.as_deref())
    } else {
        remote.localname.clone()
    }
}

fn lisp_environment_string(interp: &Interpreter, env: &Env, variable: &str) -> Option<String> {
    let environment = interp.lookup_var("process-environment", env)?;
    getenv_in_environment(variable, &environment, false)
        .ok()
        .flatten()
        .and_then(|value| string_like(&value).map(|string| string.text))
}

pub(crate) fn substitute_in_file_name(path: &str) -> String {
    let mut result = String::new();
    let chars: Vec<char> = path.chars().collect();
    let mut index = 0usize;
    while index < chars.len() {
        if chars[index] != '$' {
            result.push(chars[index]);
            index += 1;
            continue;
        }
        if index + 1 < chars.len() && chars[index + 1] == '$' {
            result.push('$');
            index += 2;
            continue;
        }
        if index + 1 < chars.len() && chars[index + 1] == '{' {
            let mut end = index + 2;
            while end < chars.len() && chars[end] != '}' {
                end += 1;
            }
            if end < chars.len() && chars[end] == '}' {
                let name: String = chars[index + 2..end].iter().collect();
                result.push_str(&std::env::var(&name).unwrap_or_default());
                index = end + 1;
                continue;
            }
        }
        let mut end = index + 1;
        while end < chars.len() && (chars[end].is_ascii_alphanumeric() || chars[end] == '_') {
            end += 1;
        }
        if end == index + 1 {
            result.push('$');
            index += 1;
            continue;
        }
        let name: String = chars[index + 1..end].iter().collect();
        result.push_str(&std::env::var(&name).unwrap_or_default());
        index = end;
    }
    result
}

pub(crate) fn expand_home_prefix(path: &str) -> String {
    let home = std::env::var("HOME").ok();
    expand_home_prefix_with_home(path, home.as_deref())
}

fn expand_home_prefix_with_home(path: &str, home: Option<&str>) -> String {
    if path == "~" {
        return home.unwrap_or(path).to_string();
    }
    if let Some(suffix) = path.strip_prefix("~/")
        && let Some(home) = home
    {
        return PathBuf::from(home).join(suffix).display().to_string();
    }
    if let Some(rest) = path.strip_prefix('~') {
        let (user, suffix) = rest
            .split_once('/')
            .map(|(user, suffix)| (user, Some(suffix)))
            .unwrap_or((rest, None));
        if user_exists(user)
            && let Some(home) = home
        {
            return suffix.map_or_else(
                || home.to_string(),
                |suffix| PathBuf::from(home).join(suffix).display().to_string(),
            );
        }
    }
    path.to_string()
}

pub(crate) fn current_user_login_name() -> Option<String> {
    std::env::var("LOGNAME")
        .ok()
        .filter(|value| !value.is_empty())
        .or_else(|| std::env::var("USER").ok().filter(|value| !value.is_empty()))
}

pub(crate) fn system_name_value() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .filter(|value| !value.is_empty())
        .or_else(|| {
            std::env::var("COMPUTERNAME")
                .ok()
                .filter(|value| !value.is_empty())
        })
        .or_else(|| {
            std::process::Command::new("hostname")
                .output()
                .ok()
                .filter(|output| output.status.success())
                .and_then(|output| String::from_utf8(output.stdout).ok())
                .map(|name| name.trim().to_string())
                .filter(|name| !name.is_empty())
        })
        .unwrap_or_else(|| "localhost".into())
}

pub(crate) fn current_user_full_name() -> Option<String> {
    std::env::var("EMAXX_USER_FULL_NAME")
        .ok()
        .filter(|value| !value.is_empty())
        .or_else(current_user_login_name)
}

pub(crate) fn emacs_version_value() -> String {
    std::env::var("EMAXX_EMACS_VERSION")
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_string())
}

pub(crate) fn emacs_major_version_value() -> i64 {
    parse_version_components(&emacs_version_value())
        .unwrap_or_default()
        .first()
        .copied()
        .unwrap_or(0)
}

pub(crate) fn emacs_minor_version_value() -> i64 {
    parse_version_components(&emacs_version_value())
        .unwrap_or_default()
        .get(1)
        .copied()
        .unwrap_or(0)
}

pub(crate) fn system_configuration() -> String {
    if let Ok(value) = std::env::var("EMAXX_SYSTEM_CONFIGURATION")
        && !value.is_empty()
    {
        return value;
    }
    SYSTEM_CONFIGURATION
        .get_or_init(default_system_configuration)
        .clone()
}

pub(crate) fn emacs_version_description() -> String {
    format!(
        "GNU Emacs {} ({})",
        emacs_version_value(),
        system_configuration()
    )
}

pub(crate) fn user_exists(name: &str) -> bool {
    current_user_login_name().is_some_and(|login| login == name)
}

pub(crate) fn user_full_name(name: Option<&str>) -> Option<String> {
    match name {
        None | Some("") => current_user_full_name(),
        Some(name) if user_exists(name) => current_user_full_name(),
        _ => None,
    }
}

pub(crate) fn file_name_directory(path: &str) -> Option<String> {
    path.rfind('/').map(|index| path[..=index].to_string())
}

pub(crate) fn file_relative_name(file: &str, directory: &str) -> String {
    let file_path = normalize_path(Path::new(file));
    let directory_path = normalize_path(Path::new(directory));
    let file_components = file_path.components().collect::<Vec<_>>();
    let directory_components = directory_path.components().collect::<Vec<_>>();
    if file_components.first() != directory_components.first() {
        return file_path.display().to_string();
    }

    let mut shared = 0usize;
    while shared < file_components.len()
        && shared < directory_components.len()
        && file_components[shared] == directory_components[shared]
    {
        shared += 1;
    }

    let mut relative = PathBuf::new();
    for _ in shared..directory_components.len() {
        relative.push("..");
    }
    for component in &file_components[shared..] {
        relative.push(component.as_os_str());
    }
    if relative.as_os_str().is_empty() {
        ".".into()
    } else {
        relative.display().to_string()
    }
}

pub(crate) fn file_name_nondirectory(path: &str) -> String {
    path.rsplit('/').next().unwrap_or(path).to_string()
}

pub(crate) fn file_name_sans_extension(path: &str) -> String {
    let directory = file_name_directory(path).unwrap_or_default();
    let name = file_name_nondirectory(path);
    if let Some(index) = name.rfind('.')
        && index > 0
    {
        return format!("{directory}{}", &name[..index]);
    }
    path.to_string()
}

pub(crate) fn file_name_extension(path: &str, with_period: bool) -> Option<String> {
    let name = file_name_nondirectory(path);
    let index = name.rfind('.')?;
    if index == 0 {
        return None;
    }
    Some(if with_period {
        name[index..].to_string()
    } else {
        name[index + 1..].to_string()
    })
}

pub(crate) fn file_name_as_directory(path: &str) -> String {
    if path.is_empty() {
        "./".into()
    } else if path.ends_with('/') {
        path.to_string()
    } else {
        format!("{path}/")
    }
}

pub(crate) fn directory_file_name(path: &str) -> String {
    if path.is_empty() {
        return String::new();
    }
    if path.chars().all(|ch| ch == '/') {
        return if path.len() == 2 {
            "//".into()
        } else {
            "/".into()
        };
    }
    path.trim_end_matches('/').to_string()
}

pub(crate) fn dired_buffer_name(directory: &str) -> String {
    if directory.contains('*') {
        Path::new(directory)
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or(directory)
            .to_string()
    } else {
        format!("{}{}", directory_file_name(directory), "/")
    }
}

pub(crate) fn dired_listing_for_directory(directory: &str) -> Result<String, LispError> {
    if directory.contains('*') {
        return dired_listing_for_wildcard(directory);
    }

    let mut entries = fs::read_dir(directory)
        .map_err(|error| {
            LispError::SignalValue(file_error_with_detail_value(
                "Opening directory",
                &error.to_string(),
                directory,
            ))
        })?
        .map(|entry| {
            entry
                .map_err(|error| {
                    LispError::SignalValue(file_error_with_detail_value(
                        "Reading directory",
                        &error.to_string(),
                        directory,
                    ))
                })
                .map(|entry| {
                    let name = entry.file_name().to_string_lossy().into_owned();
                    let metadata = entry.metadata().ok();
                    (name, metadata)
                })
        })
        .collect::<Result<Vec<_>, LispError>>()?;
    entries.sort_by(|left, right| left.0.cmp(&right.0));

    let mut listing = String::new();
    listing.push_str(&file_name_as_directory(directory));
    listing.push_str(":\n");
    listing.push_str(&dired_listing_line(
        ".",
        fs::metadata(directory).ok().as_ref(),
    ));
    let parent_metadata = Path::new(directory)
        .parent()
        .and_then(|parent| parent.metadata().ok());
    listing.push_str(&dired_listing_line("..", parent_metadata.as_ref()));
    for (entry, metadata) in entries {
        listing.push_str(&dired_listing_line(&entry, metadata.as_ref()));
    }
    Ok(listing)
}

pub(crate) fn dired_base_directory(directory: &str) -> String {
    if !directory.contains('*') {
        return directory.to_string();
    }

    let path = Path::new(directory);
    let mut base = if path.is_absolute() {
        PathBuf::from(std::path::MAIN_SEPARATOR.to_string())
    } else {
        PathBuf::from(".")
    };
    for component in path.components() {
        match component {
            Component::RootDir | Component::Prefix(_) | Component::CurDir => {}
            Component::ParentDir => base.push(".."),
            Component::Normal(segment) => {
                if segment.to_string_lossy().contains('*') {
                    break;
                }
                base.push(segment);
            }
        }
    }
    base.to_string_lossy().into_owned()
}

fn dired_listing_for_wildcard(directory: &str) -> Result<String, LispError> {
    let base_directory = dired_base_directory(directory);
    let matches = expand_simple_wildcard_paths(directory)?;
    let base_path = Path::new(&base_directory);
    let wildcard = Path::new(directory)
        .strip_prefix(base_path)
        .ok()
        .map(|path| path.to_string_lossy().into_owned())
        .unwrap_or_else(|| directory.to_string());

    let mut listing = String::new();
    listing.push_str("  ");
    listing.push_str(&file_name_as_directory(&base_directory));
    listing.push_str(":\n");
    listing.push_str("  wildcard ");
    listing.push_str(&wildcard);
    listing.push('\n');
    for path in matches {
        let metadata = fs::metadata(&path).ok();
        let display_name = Path::new(&path)
            .strip_prefix(base_path)
            .ok()
            .map(|path| path.to_string_lossy().into_owned())
            .unwrap_or(path);
        listing.push_str(&dired_listing_line(&display_name, metadata.as_ref()));
    }
    Ok(listing)
}

fn dired_listing_line(name: &str, metadata: Option<&fs::Metadata>) -> String {
    let file_type = if metadata.is_some_and(|metadata| metadata.is_dir()) {
        'd'
    } else {
        '-'
    };
    let size = metadata.map(fs::Metadata::len).unwrap_or(0);
    format!("{file_type}rw-r--r-- 1 user group {size:>8} 2026-01-01 00:00 {name}\n")
}

pub(crate) fn expand_simple_wildcard_paths(pattern: &str) -> Result<Vec<String>, LispError> {
    if !pattern.contains('*') {
        return Ok(vec![pattern.to_string()]);
    }

    let path = Path::new(pattern);
    let mut roots = if path.is_absolute() {
        vec![PathBuf::from(std::path::MAIN_SEPARATOR.to_string())]
    } else {
        vec![PathBuf::from(".")]
    };

    for component in path.components() {
        match component {
            Component::RootDir | Component::Prefix(_) | Component::CurDir => {}
            Component::ParentDir => {
                for root in &mut roots {
                    root.push("..");
                }
            }
            Component::Normal(segment) => {
                let segment = segment.to_string_lossy();
                if segment.contains('*') {
                    let mut expanded = Vec::new();
                    for root in &roots {
                        let entries = fs::read_dir(root)
                            .map_err(|error| LispError::Signal(error.to_string()))?;
                        for entry in entries {
                            let entry =
                                entry.map_err(|error| LispError::Signal(error.to_string()))?;
                            let name = entry.file_name();
                            let name = name.to_string_lossy();
                            if wildcard_match(&segment, &name) {
                                expanded.push(entry.path());
                            }
                        }
                    }
                    roots = expanded;
                } else {
                    for root in &mut roots {
                        root.push(segment.as_ref());
                    }
                }
            }
        }
    }

    roots.sort();
    if roots.is_empty() {
        return Err(LispError::Signal(format!("No match: {pattern}")));
    }
    Ok(roots
        .into_iter()
        .map(|path| path.to_string_lossy().into_owned())
        .collect())
}

fn wildcard_match(pattern: &str, text: &str) -> bool {
    let parts = pattern.split('*').collect::<Vec<_>>();
    let mut rest = text;

    if let Some(first) = parts.first()
        && !first.is_empty()
    {
        let Some(stripped) = rest.strip_prefix(first) else {
            return false;
        };
        rest = stripped;
    }

    for part in parts.iter().skip(1).take(parts.len().saturating_sub(2)) {
        if part.is_empty() {
            continue;
        }
        let Some(index) = rest.find(part) else {
            return false;
        };
        rest = &rest[index + part.len()..];
    }

    if let Some(last) = parts.last()
        && !last.is_empty()
        && !rest.ends_with(last)
    {
        return false;
    }

    true
}

pub(crate) fn initialize_dired_buffer(
    interp: &mut Interpreter,
    buffer_name: &str,
    directory: &str,
) -> Result<(), LispError> {
    let listing = dired_listing_for_directory(directory)?;
    let base_directory = dired_base_directory(directory);
    interp.buffer = crate::buffer::Buffer::from_text(buffer_name, &listing);
    interp.buffer.goto_char(interp.buffer.point_max());
    interp.buffer.set_unmodified();
    interp
        .buffer
        .set_visited_file_modtime(file_modtime(&base_directory)?);
    let buffer_id = interp.current_buffer_id();
    interp.set_buffer_local_value(buffer_id, "major-mode", Value::Symbol("dired-mode".into()));
    interp.set_buffer_local_value(buffer_id, "mode-name", Value::String("Dired".into()));
    interp.set_buffer_local_value(buffer_id, "buffer-read-only", Value::T);
    interp.set_buffer_local_value(
        buffer_id,
        "dired-directory",
        Value::String(if directory.contains('*') {
            directory.to_string()
        } else {
            file_name_as_directory(directory)
        }),
    );
    let expanded_directory = file_name_as_directory(&expand_file_name(&base_directory, None));
    interp.set_buffer_local_value(
        buffer_id,
        "dired-subdir-alist",
        Value::list([Value::cons(
            Value::String(expanded_directory.clone()),
            Value::Integer(1),
        )]),
    );
    interp.set_buffer_local_value(
        buffer_id,
        "default-directory",
        Value::String(file_name_as_directory(&base_directory)),
    );
    interp.set_buffer_local_value(
        buffer_id,
        "revert-buffer-function",
        Value::Symbol("emaxx-dired-revert".into()),
    );
    interp.set_buffer_local_value(
        buffer_id,
        "buffer-stale-function",
        Value::Symbol("dired-buffer-stale-p".into()),
    );
    interp.set_buffer_local_value(buffer_id, "buffer-auto-revert-by-notification", Value::Nil);
    let buffer_value = Value::Buffer(buffer_id, buffer_name.to_string());
    let existing = interp
        .lookup_var("dired-buffers", &Vec::new())
        .and_then(|value| value.to_vec().ok())
        .unwrap_or_default();
    let mut entries = Vec::new();
    entries.push(Value::cons(
        Value::String(expanded_directory.clone()),
        buffer_value.clone(),
    ));
    for entry in existing {
        let Some((dir, buffer)) = entry.cons_values() else {
            continue;
        };
        if matches!(&dir, Value::String(existing_dir) if existing_dir == &expanded_directory)
            || matches!(buffer, Value::Buffer(existing_id, _) if existing_id == buffer_id)
        {
            continue;
        }
        entries.push(Value::cons(dir, buffer));
    }
    interp.set_symbol_value_cell("dired-buffers", Value::list(entries));
    Ok(())
}

pub(crate) fn goto_dired_listing_entry(interp: &mut Interpreter, name: &str) -> bool {
    let mut pos = 1;
    for line in interp.buffer.full_buffer_string().split_inclusive('\n') {
        let line_without_newline = line.trim_end_matches('\n');
        if let Some(prefix) = line_without_newline.strip_suffix(name)
            && prefix.ends_with(' ')
        {
            let target = pos + line_without_newline.chars().count() - name.chars().count();
            interp.buffer.goto_char(target);
            return true;
        }
        pos += line.chars().count();
    }
    false
}

pub(crate) fn refresh_current_dired_buffer_for_path(
    interp: &mut Interpreter,
    changed_path: &str,
    env: &mut Env,
) -> Result<(), LispError> {
    let buffer_id = interp.current_buffer_id();
    let Some(directory) = interp
        .buffer_local_value(buffer_id, "dired-directory")
        .and_then(|value| string_like(&value).map(|string| string.text))
    else {
        return Ok(());
    };
    let directory_path = Path::new(&directory);
    let changed = Path::new(changed_path);
    // A directory listing changes when the directory itself or one of its
    // immediate entries changes.  Descendant mutations belong to a child
    // directory's listing; refreshing an ancestor here makes Dired react to
    // events that GNU's file-notification backend never delivers to it.
    if changed != directory_path && changed.parent() != Some(directory_path) {
        return Ok(());
    }
    let buffer_name = interp.buffer.name.clone();
    initialize_dired_buffer(interp, &buffer_name, &directory)?;
    let target_text = changed.to_string_lossy().into_owned();
    let _ = call_named_function(
        interp,
        "dired-goto-file",
        &[Value::String(target_text)],
        env,
    )?;
    Ok(())
}

pub(crate) fn directory_name_p(path: &str) -> bool {
    path.ends_with('/')
}

pub(crate) fn file_name_absolute_p(path: &str) -> bool {
    if path.starts_with('/') {
        return true;
    }
    if path == "~" || path.starts_with("~/") {
        return true;
    }
    if let Some(rest) = path.strip_prefix('~') {
        let user = rest.split('/').next().unwrap_or_default();
        return user_exists(user);
    }
    false
}

pub(crate) fn file_name_concat(parts: &[String]) -> String {
    let mut iter = parts.iter().filter(|part| !part.is_empty());
    let Some(first) = iter.next() else {
        return String::new();
    };
    let mut result = first.clone();
    for part in iter {
        if result.is_empty() {
            result = part.clone();
        } else if result.ends_with('/') {
            result.push_str(part.trim_start_matches('/'));
        } else {
            result.push('/');
            result.push_str(part.trim_start_matches('/'));
        }
    }
    result
}

pub(crate) fn validate_file_name(path: &str) -> Result<(), LispError> {
    if path.contains('\0') {
        Err(LispError::TypeError("string".into(), path.to_string()))
    } else {
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MockFileNameHandlerOperation {
    Custom,
    LocalPathArguments(&'static [usize]),
}

/// Classify the operations implemented by Emaxx's in-process transport for
/// ERT's `/mock:' Tramp method.  Keep handler advertisement and handler
/// execution on this single table: if an operation is absent, real Tramp may
/// legitimately handle it; if it is present, it must never leak into a shell
/// connection merely because `tramp.el' has populated
/// `file-name-handler-alist'.
pub(crate) fn mock_file_name_handler_operation(
    operation: &str,
) -> Option<MockFileNameHandlerOperation> {
    use MockFileNameHandlerOperation::{Custom, LocalPathArguments};

    match operation {
        "abbreviate-file-name"
        | "exec-path"
        | "expand-file-name"
        | "file-group-gid"
        | "file-local-copy"
        | "file-remote-p"
        | "file-truename"
        | "file-user-uid"
        | "make-process"
        | "start-file-process" => Some(Custom),
        "delete-directory"
        | "delete-file"
        | "file-accessible-directory-p"
        | "file-directory-p"
        | "file-executable-p"
        | "file-exists-p"
        | "file-readable-p"
        | "file-regular-p"
        | "file-writable-p" => Some(LocalPathArguments(&[0])),
        "write-region" => Some(LocalPathArguments(&[2, 5])),
        _ => None,
    }
}

pub(crate) fn find_file_name_handler(
    interp: &Interpreter,
    env: &Env,
    file: &str,
    operation: &str,
) -> Result<Option<Value>, LispError> {
    if mock_file_name_handler_operation(operation).is_some()
        && parse_remote_file_name(file).is_some_and(|remote| remote.method == "mock")
    {
        return Ok(Some(Value::Symbol("emaxx-mock-file-name-handler".into())));
    }
    let handlers = interp
        .lookup_var("file-name-handler-alist", env)
        .unwrap_or(Value::Nil);
    let entries = handlers.to_vec()?;
    let operation = Value::Symbol(operation.to_string());
    let inhibited = if interp
        .lookup_var("inhibit-file-name-operation", env)
        .as_ref()
        == Some(&operation)
    {
        interp
            .lookup_var("inhibit-file-name-handlers", env)
            .unwrap_or(Value::Nil)
            .to_vec()?
    } else {
        Vec::new()
    };
    let mut regexp_env = env.clone();
    regexp_env.push(vec![("case-fold-search".into(), Value::Nil)]);
    let mut best = None;
    let mut result = None;

    for entry in entries {
        let Value::Cons(pattern, handler) = entry else {
            continue;
        };
        let pattern = pattern.borrow().clone();
        let handler = handler.borrow().clone();
        let Some(pattern) = string_like(&pattern) else {
            continue;
        };
        if let Value::Symbol(symbol) = &handler
            && let Some(operations) = interp.get_symbol_property(symbol, "operations")
            && !operations.is_nil()
            && !operations.to_vec()?.contains(&operation)
        {
            continue;
        }
        let regexp = regexp::compile_elisp_regex(interp, &pattern, &regexp_env, "", true)?;
        let Some(captures) = regexp
            .captures(file)
            .map_err(|error| LispError::Signal(error.to_string()))?
        else {
            continue;
        };
        let position = captures
            .get(0)
            .expect("a successful regexp match has group zero")
            .start();
        if best.is_none_or(|current| position > current) && !inhibited.contains(&handler) {
            best = Some(position);
            result = Some(handler);
        }
    }
    Ok(result)
}

/// Give Lisp file-name handlers the same choke point that GNU's individual
/// file primitives provide.  The operation table deliberately contains only
/// arguments that are file names; arbitrary string arguments must never
/// acquire file-name-handler semantics.
pub(crate) fn dispatch_file_name_handler(
    interp: &mut Interpreter,
    env: &mut Env,
    operation: &str,
    args: &[Value],
) -> Result<Option<Value>, LispError> {
    let indices: &[usize] = match operation {
        "add-name-to-file"
        | "copy-directory"
        | "copy-file"
        | "file-equal-p"
        | "file-in-directory-p"
        | "file-name-all-completions"
        | "file-name-completion"
        | "file-newer-than-file-p"
        | "make-symbolic-link"
        | "rename-file" => &[0, 1],
        "write-region" => &[2, 5],
        "start-file-process" => &[2],
        "access-file"
        | "abbreviate-file-name"
        | "byte-compiler-base-file-name"
        | "delete-directory"
        | "delete-file"
        | "diff-latest-backup-file"
        | "directory-file-name"
        | "directory-files"
        | "directory-files-and-attributes"
        | "dired-compress-file"
        | "dired-uncache"
        | "file-accessible-directory-p"
        | "file-acl"
        | "file-attributes"
        | "file-directory-p"
        | "file-executable-p"
        | "file-exists-p"
        | "file-locked-p"
        | "file-local-copy"
        | "file-modes"
        | "file-name-as-directory"
        | "file-name-case-insensitive-p"
        | "file-name-directory"
        | "file-name-nondirectory"
        | "file-name-sans-versions"
        | "file-ownership-preserved-p"
        | "file-readable-p"
        | "file-regular-p"
        | "file-remote-p"
        | "file-selinux-context"
        | "file-symlink-p"
        | "file-system-info"
        | "file-truename"
        | "file-writable-p"
        | "find-backup-file-name"
        | "get-file-buffer"
        | "insert-directory"
        | "insert-file-contents"
        | "load"
        | "lock-file"
        | "make-directory"
        | "make-nearby-temp-file"
        | "make-temp-file"
        | "process-file"
        | "set-file-acl"
        | "set-file-modes"
        | "set-file-selinux-context"
        | "set-file-times"
        | "substitute-in-file-name"
        | "unhandled-file-name-directory"
        | "unlock-file"
        | "vc-registered" => &[0],
        _ => &[],
    };

    let mut candidates = indices
        .iter()
        .filter_map(|index| args.get(*index))
        .filter_map(string_like)
        .map(|string| string.text)
        .collect::<Vec<_>>();

    if operation == "make-process" {
        for pair in args.chunks_exact(2) {
            if pair[0] == Value::Symbol(":command".into())
                && let Ok(command) = pair[1].to_vec()
                && let Some(program) = command.first().and_then(string_like)
            {
                candidates.push(program.text);
                break;
            }
        }
    }

    let uses_implicit_default_directory = if indices.is_empty() {
        matches!(
            operation,
            "make-auto-save-file-name"
                | "make-process"
                | "memory-info"
                | "set-visited-file-modtime"
                | "shell-command"
                | "temporary-file-directory"
                | "verify-visited-file-modtime"
        )
    } else {
        !matches!(
            operation,
            "abbreviate-file-name"
                | "directory-file-name"
                | "file-name-as-directory"
                | "file-name-directory"
                | "file-name-nondirectory"
                | "file-name-sans-versions"
                | "file-remote-p"
                | "substitute-in-file-name"
                | "unhandled-file-name-directory"
        ) && candidates.iter().any(|file| !file_name_absolute_p(file))
    };
    if uses_implicit_default_directory
        && let Some(directory) = interp.lookup_var("default-directory", env)
        && let Some(directory) = string_like(&directory)
    {
        candidates.push(directory.text);
    }

    if matches!(
        operation,
        "make-auto-save-file-name" | "set-visited-file-modtime" | "verify-visited-file-modtime"
    ) {
        let buffer_id = if operation == "verify-visited-file-modtime" {
            args.first()
                .filter(|buffer| buffer.is_truthy())
                .and_then(|buffer| interp.resolve_buffer_id(buffer).ok())
                .unwrap_or_else(|| interp.current_buffer_id())
        } else {
            interp.current_buffer_id()
        };
        if let Some(file) = interp
            .get_buffer_by_id(buffer_id)
            .and_then(|buffer| buffer.file.clone())
        {
            candidates.push(file);
        }
    }

    for file in candidates {
        let Some(handler) = find_file_name_handler(interp, env, &file, operation)? else {
            continue;
        };
        let (function, original_name) = match handler {
            Value::Symbol(symbol) => {
                let function = interp.lookup_function(&symbol, env)?;
                (function, Some(symbol))
            }
            function => (function, None),
        };
        let mut handler_args = std::iter::once(Value::Symbol(operation.into()))
            .chain(args.iter().cloned())
            .collect::<Vec<_>>();
        if operation == "verify-visited-file-modtime" && args.is_empty() {
            handler_args.push(Value::Nil);
        }
        let result =
            interp.call_function_value(function, original_name.as_deref(), &handler_args, env)?;
        if operation == "insert-file-contents" {
            let inserted = result
                .to_vec()
                .ok()
                .and_then(|items| items.get(1).cloned())
                .and_then(|value| value.as_integer().ok())
                .and_then(|value| usize::try_from(value).ok())
                .unwrap_or(0);
            finish_insert_file_contents(interp, env, inserted, &args[1..])?;
        }
        if operation == "write-region"
            && let Some(visit) = args.get(4)
            && (matches!(visit, Value::T) || string_like(visit).is_some())
        {
            // GNU's native write-region retains responsibility for the
            // VISIT postconditions even when a Lisp file-name handler writes
            // the bytes.  Handlers such as jka-compr update the visited
            // modtime, then rely on this outer primitive boundary to record
            // the visited name and mark the source buffer saved.
            let visited_name = if matches!(visit, Value::T) {
                args.get(2)
                    .and_then(string_like)
                    .map(|name| name.text)
                    .ok_or_else(|| LispError::TypeError("string".into(), "non-string".into()))?
            } else {
                string_like(visit)
                    .map(|name| name.text)
                    .ok_or_else(|| LispError::TypeError("string".into(), visit.type_name()))?
            };
            interp.buffer.file = Some(expand_file_name_runtime(interp, env, &visited_name, None)?);
            interp.buffer.set_unmodified();
        }
        return Ok(Some(result));
    }
    Ok(None)
}

pub(crate) fn ert_resource_directory(interp: &Interpreter) -> Option<String> {
    let testfile = interp
        .current_load_file()
        .or(interp.ert_test_source_file.as_deref())
        .or(interp.buffer.file.as_deref())?;
    Some(ert_resource_directory_for(testfile))
}

pub(crate) fn ert_resource_directory_for(testfile: &str) -> String {
    let expanded = PathBuf::from(expand_file_name(testfile, None));
    let sibling_resources = expanded
        .parent()
        .map(|parent| parent.join("resources"))
        .filter(|path| path.is_dir());
    let resource_dir = sibling_resources.unwrap_or_else(|| {
        let rendered = expanded.display().to_string();
        let trimmed = rendered
            .strip_suffix(".el")
            .map(|path| {
                path.strip_suffix("-tests")
                    .or_else(|| path.strip_suffix("-test"))
                    .unwrap_or(path)
            })
            .unwrap_or(rendered.as_str());
        PathBuf::from(format!("{trimmed}-resources"))
    });
    path_to_directory_string(&resource_dir)
}

pub(crate) fn apple_gcc_version_match(output: &str) -> Option<usize> {
    output
        .find("Apple LLVM")
        .or_else(|| output.find("Apple Clang"))
        .or_else(|| output.find("Apple clang"))
        .or_else(|| output.find("Xcode.app"))
}

pub(crate) fn directory_files(
    interp: &Interpreter,
    directory: &str,
    full: bool,
    matcher: Option<&Value>,
    nosort: bool,
    count: Option<usize>,
    env: &Env,
) -> Result<Value, LispError> {
    let directory = resolve_file_name_in_env(interp, env, directory);
    validate_file_name(&directory)?;
    let mut entries = vec![".".to_string(), "..".to_string()];
    let iter = fs::read_dir(&directory).map_err(|error| LispError::Signal(error.to_string()))?;
    for entry in iter {
        let entry = entry.map_err(|error| LispError::Signal(error.to_string()))?;
        entries.push(entry.file_name().to_string_lossy().into_owned());
    }
    if let Some(matcher) = matcher {
        let pattern = string_like(matcher)
            .ok_or_else(|| LispError::TypeError("string".into(), matcher.type_name()))?;
        regexp::validate_elisp_regex(&pattern.text)?;
        let regex = regexp::compile_elisp_regex(interp, &pattern, env, "", true)?;
        let mut filtered = Vec::new();
        for entry in entries {
            if regex
                .is_match(&entry)
                .map_err(|error| LispError::Signal(error.to_string()))?
            {
                filtered.push(entry);
            }
        }
        entries = filtered;
    }
    if !nosort {
        entries.sort();
    }
    if let Some(count) = count {
        entries.truncate(count);
    }
    Ok(Value::list(entries.into_iter().map(|entry| {
        let text = if full {
            Path::new(&directory)
                .join(&entry)
                .to_string_lossy()
                .into_owned()
        } else {
            entry
        };
        let multibyte = text
            .chars()
            .any(|ch| !is_raw_byte_regex_char(ch) && (ch as u32) > 0x7f);
        make_shared_string_value_with_multibyte(text, Vec::new(), multibyte)
    })))
}

pub(crate) fn charset_for_char(code: u32) -> &'static str {
    if code <= 0x7f {
        "ascii"
    } else if (RAW_BYTE_REGEX_BASE..=RAW_BYTE_REGEX_BASE + 0xff).contains(&code)
        || (RAW_BYTE8_BASE..=RAW_BYTE8_BASE + 0xff).contains(&code)
    {
        "eight-bit"
    } else {
        "unicode"
    }
}

pub(crate) fn default_charset_plist(name: &str, interp: &Interpreter) -> Option<Value> {
    match interp.charset_canonical_name(name)?.as_str() {
        "ascii" => Some(Value::list([
            Value::Symbol(":short-name".into()),
            Value::String("ASCII".into()),
        ])),
        "unicode" => Some(Value::list([
            Value::Symbol(":short-name".into()),
            Value::String("Unicode".into()),
        ])),
        "eight-bit" => Some(Value::list([
            Value::Symbol(":short-name".into()),
            Value::String("Eight-bit".into()),
        ])),
        _ => None,
    }
}

pub(crate) fn charsets_for_text(text: &str, interp: &Interpreter) -> Vec<Value> {
    let mut names = Vec::new();
    if text.chars().any(|ch| (ch as u32) <= 0x7f) {
        names.push("ascii".to_string());
    }
    if text.chars().any(|ch| (ch as u32) > 0x7f) {
        names.push("unicode".to_string());
    }
    if names.is_empty() {
        names.push("ascii".to_string());
    }
    names.sort_by_key(|name| interp.charset_priority_rank(name));
    names.dedup();
    names.into_iter().map(Value::Symbol).collect()
}

pub(crate) fn charset_max_codepoint(name: &str) -> i64 {
    match name {
        "ascii" => 0x7f,
        _ => 0x10ffff,
    }
}

pub(crate) fn charset_ranges_for(
    charset: &str,
    from: i64,
    to: i64,
    interp: &Interpreter,
) -> Result<Vec<(i64, i64)>, LispError> {
    let canonical = interp
        .charset_canonical_name(charset)
        .ok_or_else(|| LispError::Void(charset.to_string()))?;
    let (lower, upper) = if from <= to { (from, to) } else { (to, from) };
    let range = match canonical.as_str() {
        "ascii" => {
            let start = lower.max(0);
            let end = upper.min(0x7f);
            if start <= end {
                Some((start, end))
            } else {
                None
            }
        }
        "unicode" => {
            let start = lower.max(0);
            let end = upper.min(0x10ffff);
            if start <= end {
                Some((start, end))
            } else {
                None
            }
        }
        _ => None,
    };
    Ok(range.into_iter().collect())
}

pub(crate) fn normalize_path(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            other => normalized.push(other.as_os_str()),
        }
    }
    if normalized.as_os_str().is_empty() {
        PathBuf::from(".")
    } else {
        normalized
    }
}

pub(crate) fn path_to_directory_string(path: &Path) -> String {
    let mut rendered = normalize_path(path).display().to_string();
    if !rendered.ends_with(std::path::MAIN_SEPARATOR) {
        rendered.push(std::path::MAIN_SEPARATOR);
    }
    rendered
}

pub(crate) fn file_readable_p(path: &str) -> bool {
    fs::File::open(path).is_ok()
}

pub(crate) fn file_writable_p(path: &str) -> bool {
    let candidate = Path::new(path);
    if candidate.exists() {
        if let Ok(metadata) = fs::metadata(candidate)
            && metadata.is_dir()
        {
            return directory_allows_create(candidate);
        }
        return fs::OpenOptions::new().write(true).open(candidate).is_ok();
    }
    let Some(parent) = candidate.parent() else {
        return false;
    };
    directory_allows_create(parent)
}

fn directory_allows_create(directory: &Path) -> bool {
    let directory = if directory.as_os_str().is_empty() {
        Path::new(".")
    } else {
        directory
    };
    if !fs::metadata(directory)
        .map(|metadata| metadata.is_dir())
        .unwrap_or(false)
    {
        return false;
    }
    let pid = std::process::id();
    for attempt in 0..8u8 {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0);
        let probe = directory.join(format!(".emaxx-writable-probe-{pid}-{stamp:x}-{attempt:x}"));
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&probe)
        {
            Ok(_) => {
                let _ = fs::remove_file(probe);
                return true;
            }
            Err(error) if error.kind() == ErrorKind::AlreadyExists => continue,
            Err(_) => return false,
        }
    }
    false
}

pub(crate) fn file_executable_p(path: &str) -> bool {
    let Ok(metadata) = fs::metadata(path) else {
        return false;
    };
    if !metadata.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        metadata.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        true
    }
}
