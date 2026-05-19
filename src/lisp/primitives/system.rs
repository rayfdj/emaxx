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

pub(crate) fn current_user_id() -> Result<u32, LispError> {
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

pub(crate) fn compat_invocation_path_from_test_directory(test_directory: &str) -> Option<PathBuf> {
    let repo_root = compat_repo_root_from_test_directory(test_directory)?;
    let candidate = repo_root.join("src").join("emacs");
    candidate.exists().then_some(candidate)
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
    if let Ok(test_directory) = std::env::var("EMACS_TEST_DIRECTORY")
        && let Some(path) = compat_invocation_path_from_test_directory(&test_directory)
    {
        return path;
    }
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

pub(crate) fn process_attributes_value(pid: i64) -> Value {
    if pid <= 0 || pid != emacs_pid_value() {
        return Value::Nil;
    }
    Value::list([Value::cons(
        Value::Symbol("comm".into()),
        Value::String(current_invocation_name().unwrap_or_else(|| "emaxx".into())),
    )])
}

pub(crate) fn expand_file_name(path: &str, base: Option<&str>) -> String {
    let preserve_directory_syntax = path.ends_with(std::path::MAIN_SEPARATOR);
    let expanded = expand_home_prefix(path);
    let candidate = PathBuf::from(expanded);
    let absolute = if candidate.is_absolute() {
        candidate
    } else {
        let base_dir = base
            .map(PathBuf::from)
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
    if let Some(handler) = find_file_name_handler(interp, env, path) {
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
    Ok(expand_file_name(path, base))
}

pub(crate) fn resolve_file_name_in_env(interp: &Interpreter, env: &Env, path: &str) -> String {
    if Path::new(path).is_absolute() {
        return path.to_string();
    }
    let base = interp
        .lookup_var("default-directory", env)
        .and_then(|value| string_like(&value).map(|string| string.text));
    expand_file_name(path, base.as_deref())
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
    if path == "~" {
        return std::env::var("HOME").unwrap_or_else(|_| path.to_string());
    }
    if let Some(suffix) = path.strip_prefix("~/")
        && let Ok(home) = std::env::var("HOME")
    {
        return PathBuf::from(home).join(suffix).display().to_string();
    }
    if let Some(rest) = path.strip_prefix('~') {
        let (user, suffix) = rest
            .split_once('/')
            .map(|(user, suffix)| (user, Some(suffix)))
            .unwrap_or((rest, None));
        if user_exists(user)
            && let Ok(home) = std::env::var("HOME")
        {
            return suffix.map_or(home.clone(), |suffix| {
                PathBuf::from(home).join(suffix).display().to_string()
            });
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

pub(crate) fn dired_listing_for_directory(directory: &str) -> Result<String, LispError> {
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
                .map(|entry| entry.file_name().to_string_lossy().into_owned())
        })
        .collect::<Result<Vec<String>, LispError>>()?;
    entries.sort();

    let mut listing = String::new();
    listing.push_str(&file_name_as_directory(directory));
    listing.push_str(":\n");
    listing.push_str(".\n..\n");
    for entry in entries {
        listing.push_str(&entry);
        listing.push('\n');
    }
    Ok(listing)
}

pub(crate) fn initialize_dired_buffer(
    interp: &mut Interpreter,
    buffer_name: &str,
    directory: &str,
) -> Result<(), LispError> {
    let listing = dired_listing_for_directory(directory)?;
    interp.buffer = crate::buffer::Buffer::from_text(buffer_name, &listing);
    interp.buffer.set_unmodified();
    interp
        .buffer
        .set_visited_file_modtime(file_modtime(directory)?);
    let buffer_id = interp.current_buffer_id();
    interp.set_buffer_local_value(buffer_id, "major-mode", Value::Symbol("dired-mode".into()));
    interp.set_buffer_local_value(buffer_id, "mode-name", Value::String("Dired".into()));
    interp.set_buffer_local_value(buffer_id, "buffer-read-only", Value::T);
    interp.set_buffer_local_value(
        buffer_id,
        "dired-directory",
        Value::String(file_name_as_directory(directory)),
    );
    interp.set_buffer_local_value(
        buffer_id,
        "revert-buffer-function",
        Value::Symbol("dired-revert".into()),
    );
    interp.set_buffer_local_value(
        buffer_id,
        "buffer-stale-function",
        Value::Symbol("dired-buffer-stale-p".into()),
    );
    interp.set_buffer_local_value(buffer_id, "buffer-auto-revert-by-notification", Value::Nil);
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

pub(crate) fn find_file_name_handler(interp: &Interpreter, env: &Env, file: &str) -> Option<Value> {
    let handlers = interp.lookup_var("file-name-handler-alist", env)?;
    let entries = handlers.to_vec().ok()?;
    for entry in entries {
        let Value::Cons(pattern, handler) = entry else {
            continue;
        };
        let pattern = pattern.borrow().clone();
        let handler = handler.borrow().clone();
        let Ok(pattern) = string_text(&pattern) else {
            continue;
        };
        let Ok(regex) = Regex::new(&pattern) else {
            continue;
        };
        if regex.is_match(file) {
            return Some(handler);
        }
    }
    None
}

pub(crate) fn ert_resource_directory(interp: &Interpreter) -> Option<String> {
    let testfile = interp
        .current_load_file()
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
    validate_file_name(directory)?;
    let mut entries = vec![".".to_string(), "..".to_string()];
    let iter = fs::read_dir(directory).map_err(|error| LispError::Signal(error.to_string()))?;
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
        Value::String(if full {
            Path::new(directory)
                .join(&entry)
                .to_string_lossy()
                .into_owned()
        } else {
            entry
        })
    })))
}

pub(crate) fn charset_for_char(code: u32) -> &'static str {
    if code <= 0x7f {
        "ascii"
    } else if (RAW_BYTE_REGEX_BASE..=RAW_BYTE_REGEX_BASE + 0xff).contains(&code) {
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
        return fs::OpenOptions::new().write(true).open(candidate).is_ok();
    }
    candidate
        .parent()
        .and_then(|parent| fs::metadata(parent).ok())
        .map(|metadata| metadata.is_dir() && !metadata.permissions().readonly())
        .unwrap_or(false)
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
