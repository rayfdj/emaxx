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
#[derive(Clone, Copy)]
enum UserAccountQuery<'a> {
    Uid(u32),
    Name(&'a std::ffi::CStr),
}

#[cfg(unix)]
struct UserAccount {
    login: String,
    full_name: Option<String>,
}

#[cfg(unix)]
fn user_account(query: UserAccountQuery<'_>) -> Option<UserAccount> {
    let mut scratch_len = 16 * 1024;
    loop {
        let mut passwd = std::mem::MaybeUninit::<libc::passwd>::uninit();
        let mut result = std::ptr::null_mut();
        let mut scratch = vec![0_i8; scratch_len];
        // getpwuid_r/getpwnam_r use caller-owned storage, so concurrent
        // interpreters cannot invalidate the returned account fields while
        // this lookup copies them.
        // SAFETY: every pointer refers to appropriately sized caller-owned
        // storage for the duration of the reentrant libc call.
        let status = unsafe {
            match query {
                UserAccountQuery::Uid(uid) => libc::getpwuid_r(
                    uid as libc::uid_t,
                    passwd.as_mut_ptr(),
                    scratch.as_mut_ptr(),
                    scratch.len(),
                    &mut result,
                ),
                UserAccountQuery::Name(name) => libc::getpwnam_r(
                    name.as_ptr(),
                    passwd.as_mut_ptr(),
                    scratch.as_mut_ptr(),
                    scratch.len(),
                    &mut result,
                ),
            }
        };
        if status == 0 {
            if result.is_null() {
                return None;
            }
            // SAFETY: a successful lookup initialized RESULT and its strings
            // point into SCRATCH until this iteration ends.
            let account = unsafe { &*result };
            if account.pw_name.is_null() {
                return None;
            }
            // SAFETY: libc passwd string fields are NUL-terminated when
            // present, and the backing storage remains alive in this scope.
            let login = unsafe { std::ffi::CStr::from_ptr(account.pw_name) }
                .to_string_lossy()
                .into_owned();
            let full_name = if account.pw_gecos.is_null() {
                None
            } else {
                // GNU ignores the comma-separated non-name GECOS fields.
                let raw = unsafe { std::ffi::CStr::from_ptr(account.pw_gecos) }.to_string_lossy();
                let mut full = raw.split(',').next().unwrap_or_default().to_string();
                // Systems configured with AMPERSAND_FULL_NAME substitute the
                // login at the first ampersand and capitalize its first byte.
                if let Some(index) = full.find('&') {
                    let mut expanded_login = login.clone();
                    if let Some(first) = expanded_login.get_mut(..1) {
                        first.make_ascii_uppercase();
                    }
                    full.replace_range(index..=index, &expanded_login);
                }
                Some(full)
            };
            return Some(UserAccount { login, full_name });
        }
        if status != libc::ERANGE || scratch_len >= 1024 * 1024 {
            return None;
        }
        scratch_len *= 2;
    }
}

#[cfg(unix)]
pub(crate) fn user_name_from_uid(uid: u32) -> Option<String> {
    user_account(UserAccountQuery::Uid(uid)).map(|account| account.login)
}

#[cfg(not(unix))]
pub(crate) fn user_name_from_uid(uid: u32) -> Option<String> {
    (current_user_id().ok() == Some(uid))
        .then(current_user_login_name)
        .flatten()
}

#[cfg(unix)]
pub(crate) fn user_full_name_from_uid(uid: u32) -> Option<String> {
    user_account(UserAccountQuery::Uid(uid)).and_then(|account| account.full_name)
}

#[cfg(unix)]
pub(crate) fn user_full_name_from_login(login: &str) -> Option<String> {
    let login = std::ffi::CString::new(login).ok()?;
    user_account(UserAccountQuery::Name(&login)).and_then(|account| account.full_name)
}

#[cfg(not(unix))]
pub(crate) fn user_full_name_from_uid(_uid: u32) -> Option<String> {
    None
}

#[cfg(not(unix))]
pub(crate) fn user_full_name_from_login(login: &str) -> Option<String> {
    (current_user_login_name().as_deref() == Some(login)).then(|| {
        std::env::var("EMAXX_USER_FULL_NAME")
            .or_else(|_| std::env::var("NAME"))
            .unwrap_or_else(|_| login.to_string())
    })
}

/// Decode the legacy unsigned-ID representation accepted by GNU's
/// CONS_TO_INTEGER boundary: an integer, an integral float, or the obsolete
/// high/low cons form.  User and group primitives share this one contract.
pub(crate) fn legacy_unsigned_id(value: &Value) -> Result<u32, LispError> {
    fn integer_part(value: &Value) -> Option<u64> {
        match value {
            Value::Integer(value) => u64::try_from(*value).ok(),
            Value::BigInteger(value) => value.to_u64(),
            _ => None,
        }
    }

    let decoded = match value {
        Value::Integer(_) | Value::BigInteger(_) => integer_part(value),
        Value::Float(value)
            if value.is_finite()
                && *value >= 0.0
                && *value <= f64::from(u32::MAX)
                && value.fract() == 0.0 =>
        {
            Some(*value as u64)
        }
        Value::Cons(_) => (|| {
            let (high, rest) = value.cons_values().expect("matched cons");
            let high = integer_part(&high)?;
            if let Some((middle, low)) = rest.cons_values()
                && high <= u64::from(u32::MAX) >> 40
                && integer_part(&middle).is_some_and(|part| part < 1 << 24)
                && integer_part(&low).is_some_and(|part| part < 1 << 16)
            {
                Some((high << 40) | (integer_part(&middle)? << 16) | integer_part(&low)?)
            } else {
                let low = rest.cons_values().map_or(rest, |(car, _)| car);
                (high <= u64::from(u32::MAX) >> 16)
                    .then(|| integer_part(&low).filter(|part| *part < 1 << 16))
                    .flatten()
                    .map(|low| (high << 16) | low)
            }
        })(),
        _ => None,
    };
    decoded
        .and_then(|value| u32::try_from(value).ok())
        .ok_or_else(|| {
            LispError::Signal("Not an in-range integer, integral float, or cons of integers".into())
        })
}

#[cfg(unix)]
pub(crate) fn group_name_from_gid(gid: i64) -> Result<Option<String>, LispError> {
    let Ok(gid) = libc::gid_t::try_from(gid) else {
        return Ok(None);
    };
    let mut scratch_len = 16 * 1024;
    loop {
        let mut group = std::mem::MaybeUninit::<libc::group>::uninit();
        let mut result = std::ptr::null_mut();
        let mut scratch = vec![0_i8; scratch_len];
        // SAFETY: all pointers refer to appropriately sized caller-owned
        // storage for the duration of this reentrant libc lookup.
        let status = unsafe {
            libc::getgrgid_r(
                gid,
                group.as_mut_ptr(),
                scratch.as_mut_ptr(),
                scratch.len(),
                &mut result,
            )
        };
        if status == 0 {
            if result.is_null() {
                return Ok(None);
            }
            // SAFETY: successful getgrgid_r initialized GROUP, whose name
            // points into SCRATCH until this iteration returns.
            let name = unsafe { std::ffi::CStr::from_ptr((*group.as_ptr()).gr_name) };
            return Ok(Some(name.to_string_lossy().into_owned()));
        }
        if status != libc::ERANGE || scratch_len >= 1024 * 1024 {
            return Ok(None);
        }
        scratch_len *= 2;
    }
}

#[cfg(not(unix))]
pub(crate) fn group_name_from_gid(_gid: i64) -> Result<Option<String>, LispError> {
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

/// Return GNU Emacs's `system-type' spelling for a Rust target OS.
///
/// GNU derives this value in configure.ac rather than exposing the platform's
/// raw name. Keep that translation here so native startup values and compact
/// file-less fallbacks cannot acquire independent platform policies.
pub(crate) fn gnu_system_type_for_target(target_os: &str) -> &str {
    match target_os {
        "macos" | "ios" => "darwin",
        "linux" => "gnu/linux",
        "freebsd" | "netbsd" | "openbsd" | "dragonfly" => "berkeley-unix",
        "windows" => "windows-nt",
        "solaris" | "illumos" => "usg-unix-v",
        "hurd" => "gnu",
        other => other,
    }
}

pub(crate) fn gnu_system_type() -> &'static str {
    gnu_system_type_for_target(std::env::consts::OS)
}

#[cfg(test)]
pub(crate) fn gnu_default_makefile_mode_for_system_type(system_type: &str) -> &'static str {
    match system_type {
        "darwin" | "berkeley-unix" => "makefile-bsdmake-mode",
        _ => "makefile-gmake-mode",
    }
}

#[cfg(test)]
pub(crate) fn gnu_default_makefile_mode() -> &'static str {
    gnu_default_makefile_mode_for_system_type(gnu_system_type())
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
        .map(|value| Value::String(value.to_string_lossy().into_owned().into()))
        .collect::<Vec<_>>();
    if let Some(first) = args.first_mut() {
        *first = Value::String(invocation.into());
    } else {
        args.push(Value::String(invocation.into()));
    }
    Value::list(args)
}

pub(crate) fn emacs_pid_value() -> i64 {
    i64::from(std::process::id())
}

fn process_inventory(processes: sysinfo::ProcessesToUpdate<'_>) -> sysinfo::System {
    let mut system = sysinfo::System::new();
    system.refresh_processes_specifics(
        processes,
        true,
        sysinfo::ProcessRefreshKind::everything().without_tasks(),
    );
    system
}

pub(crate) fn list_system_processes_value() -> Value {
    #[cfg(target_os = "macos")]
    if !darwin_process_inventory_available() {
        return Value::Nil;
    }
    let system = process_inventory(sysinfo::ProcessesToUpdate::All);
    let mut pids = system
        .processes()
        .keys()
        .map(|pid| i64::from(pid.as_u32()))
        .collect::<Vec<_>>();
    pids.sort_unstable();
    Value::list(pids.into_iter().map(Value::Integer))
}

#[cfg(target_os = "macos")]
fn darwin_process_inventory_available() -> bool {
    let mut mib = [libc::CTL_KERN, libc::KERN_PROC, libc::KERN_PROC_ALL];
    let mut len = 0;
    // SAFETY: this is GNU Emacs's read-only KERN_PROC_ALL size probe.  The
    // kernel writes only `len`; both data pointers are null.
    unsafe {
        libc::sysctl(
            mib.as_mut_ptr(),
            mib.len() as libc::c_uint,
            std::ptr::null_mut(),
            &mut len,
            std::ptr::null_mut(),
            0,
        ) == 0
            && len > 0
    }
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
    let pid = sysinfo::Pid::from_u32(pid);
    // `process-attributes' asks about one PID.  Refreshing every process here
    // made Proced's normal list-then-attribute traversal accidentally
    // quadratic in the host process count.  Keep each answer fresh, as GNU
    // does, while asking sysinfo to materialize only the requested process.
    let system = process_inventory(sysinfo::ProcessesToUpdate::Some(&[pid]));
    let Some(process) = system.process(pid) else {
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
                push("user", Value::String(user.into()));
            }
        }
        if let Some(gid) = process.effective_group_id().or_else(|| process.group_id()) {
            push("egid", Value::Integer(i64::from(*gid)));
            if let Ok(Some(group)) = group_name_from_gid(i64::from(*gid)) {
                push("group", Value::String(group.into()));
            }
        }
    }

    push(
        "comm",
        Value::String(process.name().to_string_lossy().into_owned().into()),
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
        let raw_pid = pid.as_u32() as libc::pid_t;
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
        push("args", Value::String(command.into()));
    }
    Value::list(attributes)
}

#[cfg(test)]
mod process_inventory_tests {
    use super::*;

    #[test]
    fn one_pid_refresh_does_not_materialize_the_full_process_table() {
        let pid = sysinfo::Pid::from_u32(std::process::id());
        let system = process_inventory(sysinfo::ProcessesToUpdate::Some(&[pid]));

        assert!(
            system.process(pid).is_some(),
            "current process is inspectable"
        );
        assert_eq!(
            system.processes().len(),
            1,
            "single-PID refresh must not become an all-process snapshot"
        );
    }

    #[test]
    fn rust_targets_map_to_gnu_system_types_and_makefile_policy() {
        for (target, expected) in [
            ("macos", "darwin"),
            ("linux", "gnu/linux"),
            ("freebsd", "berkeley-unix"),
            ("netbsd", "berkeley-unix"),
            ("openbsd", "berkeley-unix"),
            ("dragonfly", "berkeley-unix"),
            ("windows", "windows-nt"),
            ("solaris", "usg-unix-v"),
            ("hurd", "gnu"),
            ("android", "android"),
        ] {
            assert_eq!(gnu_system_type_for_target(target), expected, "{target}");
        }
        for (system_type, expected) in [
            ("darwin", "makefile-bsdmake-mode"),
            ("berkeley-unix", "makefile-bsdmake-mode"),
            ("gnu/linux", "makefile-gmake-mode"),
            ("windows-nt", "makefile-gmake-mode"),
            ("android", "makefile-gmake-mode"),
        ] {
            assert_eq!(
                gnu_default_makefile_mode_for_system_type(system_type),
                expected,
                "{system_type}"
            );
        }
    }
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
    // GNU resolves an explicitly relative DEFAULT-DIRECTORY against the
    // buffer's default-directory before it chooses a file-name handler.  In
    // particular, `("..", "./")' must retain a remote current directory.
    let mut resolved_base = base.map(str::to_string);
    if let Some(relative_base) = base.filter(|base| !file_name_absolute_p(base))
        && let Some(default_directory) = interp
            .lookup_var("default-directory", env)
            .and_then(|value| string_like(&value).map(|string| string.text))
        && default_directory != relative_base
    {
        resolved_base = Some(expand_file_name_runtime(
            interp,
            env,
            relative_base,
            Some(&default_directory),
        )?);
    }
    let base = resolved_base.as_deref();
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
                Value::String(path.to_string().into()),
                base.map(|value| Value::String(value.to_string().into()))
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
    let preserve_directory_syntax = path.ends_with(std::path::MAIN_SEPARATOR);
    let mut candidate = normalize_path(Path::new(path));

    // `std::fs::canonicalize' gives up as soon as the final target is
    // missing.  GNU file-truename instead resolves every existing symlinked
    // prefix and then preserves the unresolved suffix.  Iterate rather than
    // recurse so long chains and cycles have a fixed, explicit hop bound.
    for _ in 0..64 {
        let components = candidate
            .components()
            .map(|component| component.as_os_str().to_owned())
            .collect::<Vec<_>>();
        let mut prefix = PathBuf::new();
        let mut replacement = None;
        for (index, component) in components.iter().enumerate() {
            prefix.push(component);
            let Ok(metadata) = fs::symlink_metadata(&prefix) else {
                continue;
            };
            if !metadata.file_type().is_symlink() {
                continue;
            }
            let Ok(target) = fs::read_link(&prefix) else {
                continue;
            };
            let mut resolved = if target.is_absolute() {
                target
            } else {
                prefix
                    .parent()
                    .map(|parent| parent.join(&target))
                    .unwrap_or(target)
            };
            resolved.extend(components.iter().skip(index + 1));
            replacement = Some(normalize_path(&resolved));
            break;
        }
        let Some(resolved) = replacement else {
            let rendered = candidate.to_string_lossy().into_owned();
            return if preserve_directory_syntax {
                path_to_directory_string(&candidate)
            } else {
                rendered
            };
        };
        candidate = resolved;
    }

    if preserve_directory_syntax {
        path_to_directory_string(&candidate)
    } else {
        candidate.to_string_lossy().into_owned()
    }
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
        let localname =
            unquote_local_file_name(&remote.localname).unwrap_or_else(|| remote.localname.clone());
        expand_home_prefix_with_home(&localname, home.as_deref())
    } else {
        remote.localname.clone()
    }
}

pub(crate) fn lisp_environment_string(
    interp: &Interpreter,
    env: &Env,
    variable: &str,
) -> Option<String> {
    let environment = interp.lookup_var("process-environment", env)?;
    getenv_in_environment(variable, &environment, false)
        .ok()
        .flatten()
        .and_then(|value| string_like(&value).map(|string| string.text))
}

fn substitute_in_file_name_with(
    path: &str,
    mut environment_value: impl FnMut(&str) -> Option<String>,
) -> String {
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
                if let Some(value) = environment_value(&name) {
                    result.push_str(&value);
                } else {
                    result.push_str("${");
                    result.push_str(&name);
                    result.push('}');
                }
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
        if let Some(value) = environment_value(&name) {
            result.push_str(&value);
        } else {
            result.push('$');
            result.push_str(&name);
        }
        index = end;
    }
    // Each doubled slash discards everything before its second slash.  This
    // is GNU's long-standing local-name rule and is also the primitive tail
    // used by Tramp after it has separated a remote prefix.
    if let Some(index) = result.rfind("//") {
        result[index + 1..].to_string()
    } else {
        result
    }
}

#[cfg(test)]
pub(crate) fn substitute_in_file_name(path: &str) -> String {
    substitute_in_file_name_with(path, |name| std::env::var(name).ok())
}

pub(crate) fn substitute_in_file_name_in_env(
    interp: &Interpreter,
    env: &Env,
    path: &str,
) -> String {
    substitute_in_file_name_with(path, |name| lisp_environment_string(interp, env, name))
}

pub(crate) fn expand_home_prefix_with_home(path: &str, home: Option<&str>) -> String {
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
    let configured = std::env::var("LOGNAME")
        .ok()
        .filter(|value| !value.is_empty())
        .or_else(|| std::env::var("USER").ok().filter(|value| !value.is_empty()));
    #[cfg(unix)]
    {
        configured.or_else(|| current_user_id().ok().and_then(user_name_from_uid))
    }
    #[cfg(not(unix))]
    {
        configured.or_else(|| {
            std::env::var("USERNAME")
                .ok()
                .filter(|value| !value.is_empty())
        })
    }
}

pub(crate) fn current_real_user_login_name() -> Option<String> {
    #[cfg(unix)]
    {
        current_real_user_id().ok().and_then(user_name_from_uid)
    }
    #[cfg(not(unix))]
    {
        current_user_login_name()
    }
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
        // GNU resolves the host identity in-process with gethostname (or the
        // platform equivalent).  `sysinfo` provides that same cross-platform
        // boundary without spawning `hostname` on every Lisp call.
        .or_else(sysinfo::System::host_name)
        .unwrap_or_else(|| "localhost".into())
}

pub(crate) fn current_user_full_name() -> Option<String> {
    std::env::var("EMAXX_USER_FULL_NAME")
        .ok()
        .filter(|value| !value.is_empty())
        .or_else(|| std::env::var("NAME").ok().filter(|value| !value.is_empty()))
        .or_else(|| {
            current_user_login_name()
                .as_deref()
                .and_then(user_full_name_from_login)
        })
        .or_else(|| current_user_id().ok().and_then(user_full_name_from_uid))
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

pub(crate) fn user_exists(name: &str) -> bool {
    #[cfg(unix)]
    {
        std::ffi::CString::new(name)
            .ok()
            .and_then(|name| user_account(UserAccountQuery::Name(&name)))
            .is_some()
    }
    #[cfg(not(unix))]
    {
        current_user_login_name().is_some_and(|login| login == name)
    }
}

pub(crate) fn user_full_name(name: Option<&str>) -> Option<String> {
    match name {
        None | Some("") => current_user_full_name(),
        Some(name) => user_full_name_from_login(name),
    }
}

pub(crate) fn file_name_directory(path: &str) -> Option<String> {
    path.rfind('/').map(|index| path[..=index].to_string())
}

pub(crate) fn file_name_nondirectory(path: &str) -> String {
    path.rsplit('/').next().unwrap_or(path).to_string()
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

pub(crate) fn find_file_name_handler(
    interp: &mut Interpreter,
    env: &Env,
    file: &str,
    operation: &str,
) -> Result<Option<Value>, LispError> {
    let handlers = interp
        .lookup_var("file-name-handler-alist", env)
        .unwrap_or(Value::Nil);
    let operation = Value::Symbol(operation.to_string().into());
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
    let cache_key = (file.to_string(), operation.as_symbol()?.to_string());
    let cons_epoch = crate::lisp::types::cons_mutation_epoch();
    let definition_generation = interp.current_definition_generation();
    let handler_alist_id = handlers.cons_id();
    let cached_matches = interp
        .file_name_handler_match_cache
        .get(&cache_key)
        .filter(|entry| {
            entry.cons_epoch == cons_epoch
                && entry.definition_generation == definition_generation
                && entry.handler_alist.cons_id() == handler_alist_id
                && entry.pattern_snapshots.iter().all(|(pattern, snapshot)| {
                    string_like(pattern).is_some_and(|pattern| pattern.text == *snapshot)
                })
        })
        .map(|entry| entry.matches.clone());
    let matches = if let Some(matches) = cached_matches {
        matches
    } else {
        #[cfg(test)]
        FILE_NAME_HANDLER_SCAN_COUNT.with(|count| count.set(count.get() + 1));
        let entries = handlers.to_vec()?;
        let mut regexp_env = env.clone();
        regexp_env.push(vec![("case-fold-search".into(), Value::Nil)].into());
        let mut cacheable = handler_alist_id.is_some();
        let mut pattern_snapshots = Vec::new();
        let mut matches = Vec::new();
        for entry in entries {
            let Some((pattern, handler)) = (entry).cons_cells() else {
                continue;
            };
            let pattern = pattern.borrow().clone();
            let handler = handler.borrow().clone();
            let Some(pattern_text) = string_like(&pattern) else {
                continue;
            };
            cacheable &= !regexp::pattern_depends_on_syntax_table(&pattern_text.text);
            pattern_snapshots.push((pattern, pattern_text.text.clone()));
            if let Value::Symbol(symbol) = &handler
                && let Some(operations) = interp.get_symbol_property(symbol, "operations")
                && !operations.is_nil()
                && !operations.to_vec()?.contains(&operation)
            {
                continue;
            }
            let regexp = regexp::compile_elisp_regex(interp, &pattern_text, &regexp_env, "", true)?;
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
            matches.push((position, handler));
        }
        if cacheable {
            if interp.file_name_handler_match_cache.len() >= 4096 {
                interp.file_name_handler_match_cache.clear();
            }
            interp.file_name_handler_match_cache.insert(
                cache_key,
                crate::lisp::eval::FileNameHandlerMatchCacheEntry {
                    handler_alist: handlers,
                    // Regexp compilation may lazily initialize Lisp-visible
                    // tables.  Stamp the derived result after that work so
                    // the entry is not born stale.
                    cons_epoch: crate::lisp::types::cons_mutation_epoch(),
                    definition_generation: interp.current_definition_generation(),
                    pattern_snapshots,
                    matches: matches.clone(),
                },
            );
        }
        matches
    };
    let mut best = None;
    let mut result = None;
    for (position, handler) in matches {
        if best.is_none_or(|current| position > current) && !inhibited.contains(&handler) {
            best = Some(position);
            result = Some(handler);
        }
    }
    Ok(result)
}

#[cfg(test)]
thread_local! {
    static FILE_NAME_HANDLER_SCAN_COUNT: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn reset_file_name_handler_scan_count() {
    FILE_NAME_HANDLER_SCAN_COUNT.with(|count| count.set(0));
}

#[cfg(test)]
pub(crate) fn file_name_handler_scan_count() -> usize {
    FILE_NAME_HANDLER_SCAN_COUNT.with(std::cell::Cell::get)
}

#[derive(Clone, Copy)]
enum DefaultDirectorySource {
    Never,
    RelativeArgument,
    Always,
}

#[derive(Clone, Copy)]
enum BufferFileSource {
    Current,
    ArgumentOrCurrent,
}

/// One authoritative description of the file-name arguments and implicit
/// paths exposed by a primitive to Lisp file-name handlers.
#[derive(Clone, Copy)]
pub(crate) struct FileNameHandlerOperation {
    string_indices: &'static [usize],
    default_directory: DefaultDirectorySource,
    buffer_file: Option<BufferFileSource>,
    process_command: bool,
}

impl FileNameHandlerOperation {
    const fn new(
        string_indices: &'static [usize],
        default_directory: DefaultDirectorySource,
    ) -> Self {
        Self {
            string_indices,
            default_directory,
            buffer_file: None,
            process_command: false,
        }
    }

    const fn with_buffer_file(mut self, buffer_file: BufferFileSource) -> Self {
        self.buffer_file = Some(buffer_file);
        self
    }

    const fn with_process_command(mut self) -> Self {
        self.process_command = true;
        self
    }
}

/// Give Lisp file-name handlers the same choke point that GNU's individual
/// file primitives provide.  Each operation occurs exactly once here, so its
/// advertisement, file arguments, and implicit path sources cannot drift.
pub(crate) fn file_name_handler_operation(operation: &str) -> Option<FileNameHandlerOperation> {
    use DefaultDirectorySource::{Always, Never, RelativeArgument};

    let specification = match operation {
        "add-name-to-file"
        | "copy-directory"
        | "copy-file"
        | "file-equal-p"
        | "file-in-directory-p"
        | "file-name-all-completions"
        | "file-name-completion"
        | "file-newer-than-file-p"
        | "rename-file" => FileNameHandlerOperation::new(&[0, 1], RelativeArgument),
        // The symlink target is stored data, not a file to access.  GNU's
        // Fmake_symbolic_link consults handlers only for LINKNAME; when that
        // handler runs it still receives TARGET unchanged as an argument.
        "make-symbolic-link" => FileNameHandlerOperation::new(&[1], RelativeArgument),
        "write-region" => FileNameHandlerOperation::new(&[2, 5], RelativeArgument),
        "start-file-process" => FileNameHandlerOperation::new(&[2], Always),
        "process-file" => FileNameHandlerOperation::new(&[0], Always),
        "abbreviate-file-name"
        | "byte-compiler-base-file-name"
        | "directory-file-name"
        | "file-name-as-directory"
        | "file-name-directory"
        | "file-name-nondirectory"
        | "file-name-sans-versions"
        | "file-remote-p"
        | "substitute-in-file-name"
        | "unhandled-file-name-directory" => FileNameHandlerOperation::new(&[0], Never),
        "access-file"
        | "delete-directory"
        | "delete-file"
        | "diff-latest-backup-file"
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
        | "file-name-case-insensitive-p"
        | "file-ownership-preserved-p"
        | "file-readable-p"
        | "file-regular-p"
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
        | "set-file-acl"
        | "set-file-modes"
        | "set-file-selinux-context"
        | "set-file-times"
        | "unlock-file"
        | "vc-registered" => FileNameHandlerOperation::new(&[0], RelativeArgument),
        "memory-info" | "shell-command" | "temporary-file-directory" => {
            FileNameHandlerOperation::new(&[], Always)
        }
        "make-auto-save-file-name" | "set-visited-file-modtime" => {
            FileNameHandlerOperation::new(&[], Always).with_buffer_file(BufferFileSource::Current)
        }
        "verify-visited-file-modtime" => FileNameHandlerOperation::new(&[], Always)
            .with_buffer_file(BufferFileSource::ArgumentOrCurrent),
        "make-process" => FileNameHandlerOperation::new(&[], Always).with_process_command(),
        _ => return None,
    };
    Some(specification)
}

pub(crate) fn dispatch_file_name_handler(
    interp: &mut Interpreter,
    env: &mut Env,
    operation: &str,
    specification: FileNameHandlerOperation,
    args: &[Value],
) -> Result<Option<Value>, LispError> {
    let mut candidates = specification
        .string_indices
        .iter()
        .filter_map(|index| args.get(*index))
        .filter_map(string_like)
        .map(|string| string.text)
        .collect::<Vec<_>>();

    if specification.process_command {
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

    let uses_implicit_default_directory = match specification.default_directory {
        DefaultDirectorySource::Never => false,
        DefaultDirectorySource::RelativeArgument => {
            candidates.iter().any(|file| !file_name_absolute_p(file))
        }
        DefaultDirectorySource::Always => true,
    };
    if uses_implicit_default_directory
        && let Some(directory) = interp.lookup_var("default-directory", env)
        && let Some(directory) = string_like(&directory)
    {
        candidates.push(directory.text);
    }

    if let Some(buffer_file) = specification.buffer_file {
        let buffer_id = match buffer_file {
            BufferFileSource::Current => interp.current_buffer_id(),
            BufferFileSource::ArgumentOrCurrent => args
                .first()
                .filter(|buffer| buffer.is_truthy())
                .and_then(|buffer| interp.resolve_buffer_id(buffer).ok())
                .unwrap_or_else(|| interp.current_buffer_id()),
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
        let result = interp.call_function_value(
            function,
            original_name.as_ref().map(|name| name.as_str()),
            &handler_args,
            env,
        )?;
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
    let iter = fs::read_dir(&directory)
        .map_err(|error| file_operation_error("Opening directory", &error, &directory))?;
    for entry in iter {
        let entry =
            entry.map_err(|error| file_operation_error("Reading directory", &error, &directory))?;
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
    names
        .into_iter()
        .map(|value| Value::Symbol(value.into()))
        .collect()
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
        let probe = directory.join(format!(".writable-probe-{pid}-{stamp:x}-{attempt:x}"));
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
