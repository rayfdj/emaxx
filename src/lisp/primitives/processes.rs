use super::*;

pub(crate) fn run_external_process(
    interp: &Interpreter,
    program: &str,
    argv: &[String],
    input: Option<&[u8]>,
    env: &Env,
) -> Result<std::process::Output, LispError> {
    let mut command = Command::new(program);
    command.args(argv);
    configure_external_command(interp, env, &mut command);
    command.stdin(if input.is_some() {
        Stdio::piped()
    } else {
        Stdio::null()
    });
    command.stdout(Stdio::piped());
    command.stderr(Stdio::piped());
    apply_process_environment(interp, env, &mut command);
    let mut child = command
        .spawn()
        .map_err(|error| LispError::SignalValue(file_error_value(&error.to_string(), program)))?;
    if let Some(stdin_data) = input
        && let Some(mut stdin) = child.stdin.take()
    {
        stdin
            .write_all(stdin_data)
            .map_err(|error| LispError::Signal(error.to_string()))?;
    }
    child
        .wait_with_output()
        .map_err(|error| LispError::Signal(error.to_string()))
}

pub(crate) fn configure_external_command(interp: &Interpreter, env: &Env, command: &mut Command) {
    if let Some(default_directory) = interp
        .lookup_var("default-directory", env)
        .and_then(|value| string_like(&value).map(|string| string.text))
        .filter(|directory| !directory.is_empty())
    {
        command.current_dir(default_directory);
    }
    apply_process_environment(interp, env, command);
}

pub(crate) fn spawn_persistent_process(
    interp: &Interpreter,
    program: &str,
    argv: &[String],
    env: &Env,
) -> Result<Child, LispError> {
    let mut command = Command::new(program);
    command.args(argv);
    configure_external_command(interp, env, &mut command);
    command.stdin(Stdio::piped());
    command.stdout(Stdio::piped());
    command.stderr(Stdio::piped());
    let child = command
        .spawn()
        .map_err(|error| LispError::Signal(error.to_string()))?;
    #[cfg(unix)]
    {
        if let Some(stdout) = child.stdout.as_ref() {
            set_nonblocking(stdout)?;
        }
        if let Some(stderr) = child.stderr.as_ref() {
            set_nonblocking(stderr)?;
        }
    }
    Ok(child)
}

#[cfg(unix)]
pub(crate) fn set_nonblocking<T: AsRawFd>(stream: &T) -> Result<(), LispError> {
    let fd = stream.as_raw_fd();
    // SAFETY: `fd` is an open file descriptor we own for the lifetime of this call.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags < 0 {
        return Err(LispError::Signal(
            std::io::Error::last_os_error().to_string(),
        ));
    }
    // SAFETY: `fd` is still valid here, and we only add the O_NONBLOCK flag.
    let result = unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) };
    if result < 0 {
        return Err(LispError::Signal(
            std::io::Error::last_os_error().to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn process_buffer_target(
    interp: &mut Interpreter,
    value: &Value,
) -> Result<Option<u64>, LispError> {
    if value.is_nil() {
        return Ok(None);
    }
    if let Some(buffer) = string_like(value) {
        return Ok(Some(
            interp
                .find_buffer(&buffer.text)
                .map(|(id, _)| id)
                .unwrap_or_else(|| interp.create_buffer(&buffer.text).0),
        ));
    }
    if matches!(value, Value::Buffer(_, _)) {
        return Ok(Some(interp.resolve_buffer_id(value)?));
    }
    Err(LispError::TypeError(
        "buffer-or-name".into(),
        value.type_name(),
    ))
}

pub(crate) fn process_command_parts(value: &Value) -> Result<(String, Vec<String>), LispError> {
    let items = value.to_vec()?;
    let Some((program, argv)) = items.split_first() else {
        return Err(LispError::Signal(
            "Process command must not be empty".into(),
        ));
    };
    Ok((
        string_text(program)?,
        argv.iter()
            .map(string_text)
            .collect::<Result<Vec<_>, _>>()?,
    ))
}

pub(crate) fn process_coding_pair(value: &Value) -> Result<(Value, Value), LispError> {
    if let Some((decoding, encoding)) = value.cons_values() {
        return Ok((decoding, encoding));
    }
    let items = value.to_vec()?;
    if items.len() == 2 {
        return Ok((items[0].clone(), items[1].clone()));
    }
    Err(LispError::TypeError("cons".into(), value.type_name()))
}

type MakeProcessArgs = (
    Option<u64>,
    Option<String>,
    Vec<String>,
    Option<Value>,
    Option<(Value, Value)>,
    Option<String>,
);

pub(crate) fn parse_make_process_args(
    interp: &mut Interpreter,
    args: &[Value],
) -> Result<MakeProcessArgs, LispError> {
    if !args.len().is_multiple_of(2) {
        return Err(LispError::WrongNumberOfArgs(
            "make-process".into(),
            args.len(),
        ));
    }
    let mut buffer_id = None;
    let mut program = None;
    let mut argv = Vec::new();
    let mut filter = None;
    let mut coding = None;
    let mut name = None;

    for pair in args.chunks_exact(2) {
        let key = pair[0].as_symbol()?;
        let value = &pair[1];
        match key {
            ":name" => name = Some(string_text(value)?),
            ":buffer" => buffer_id = process_buffer_target(interp, value)?,
            ":command" => {
                let (parsed_program, parsed_argv) = process_command_parts(value)?;
                program = Some(parsed_program);
                argv = parsed_argv;
            }
            ":filter" => filter = (!value.is_nil()).then(|| value.clone()),
            ":coding" => coding = Some(process_coding_pair(value)?),
            _ => {}
        }
    }

    Ok((buffer_id, program, argv, filter, coding, name))
}

pub(crate) fn deliver_process_output(
    interp: &mut Interpreter,
    process_id: u64,
    output: &str,
    env: &mut Env,
) -> Result<(), LispError> {
    if output.is_empty() {
        return Ok(());
    }

    // The process buffer may have been killed before straggler output is
    // delivered (epg-reset kills it right after completion); GNU discards
    // such output rather than erroring.
    let target_buffer_id = interp
        .process_buffer_id(process_id)
        .filter(|buffer_id| interp.get_buffer_by_id(*buffer_id).is_some());
    if let Some(filter) = interp.process_filter(process_id) {
        let saved_buffer_id = interp.current_buffer_id();
        let switched = target_buffer_id.is_some_and(|buffer_id| buffer_id != saved_buffer_id);
        if let Some(buffer_id) = target_buffer_id
            && switched
        {
            interp.switch_to_buffer_id(buffer_id)?;
        }
        let result = call_function_value(
            interp,
            &filter,
            &[Value::Record(process_id), Value::String(output.to_string())],
            env,
        );
        if switched {
            interp.switch_to_buffer_id(saved_buffer_id)?;
        }
        result?;
        return Ok(());
    }

    let Some(buffer_id) = target_buffer_id else {
        return Ok(());
    };
    let Some(mark_id) = interp.process_mark_id(process_id) else {
        return Ok(());
    };
    let saved_buffer_id = interp.current_buffer_id();
    let switched = buffer_id != saved_buffer_id;
    if switched {
        interp.switch_to_buffer_id(buffer_id)?;
    }
    let insert_at = interp
        .marker_position(mark_id)
        .unwrap_or_else(|| interp.current_buffer().point_max());
    interp.buffer.goto_char(insert_at);
    interp.insert_current_buffer(output);
    let new_pos = interp.buffer.point();
    let result = interp.set_marker(mark_id, Some(new_pos), Some(buffer_id));
    if switched {
        interp.switch_to_buffer_id(saved_buffer_id)?;
    }
    result
}

pub(crate) fn apply_process_environment(interp: &Interpreter, env: &Env, command: &mut Command) {
    let Some(process_environment) = interp.lookup_var("process-environment", env) else {
        return;
    };
    let Ok(entries) = process_environment_entries(&process_environment) else {
        return;
    };
    command.env_clear();
    for entry in entries {
        if let Some((name, value)) = entry.split_once('=') {
            command.env(name, value);
        }
    }
}

pub(crate) fn process_environment_entries(value: &Value) -> Result<Vec<String>, LispError> {
    value
        .to_vec()?
        .into_iter()
        .map(|item| string_text(&item))
        .collect()
}

pub(crate) fn process_environment_from_entries(entries: &[String]) -> Value {
    Value::list(entries.iter().cloned().map(Value::String))
}

pub(crate) fn setenv_in_environment_entries(
    entries: &mut Vec<String>,
    variable: &str,
    value: Option<&str>,
    keep_empty: bool,
) {
    let prefix = format!("{variable}=");
    if let Some(index) = entries
        .iter()
        .position(|entry| entry == variable || entry.starts_with(&prefix))
    {
        match value {
            Some(value) => entries[index] = format!("{variable}={value}"),
            None if keep_empty => entries[index] = variable.to_string(),
            None => {
                entries.remove(index);
            }
        }
        return;
    }

    if let Some(value) = value {
        entries.insert(0, format!("{variable}={value}"));
    } else if keep_empty {
        entries.insert(0, variable.to_string());
    }
}

pub(crate) fn getenv_in_environment(
    variable: &str,
    environment: &Value,
    negative_entry_is_truthy: bool,
) -> Result<Option<Value>, LispError> {
    let prefix = format!("{variable}=");
    for entry in process_environment_entries(environment)? {
        if let Some(value) = entry.strip_prefix(&prefix) {
            return Ok(Some(Value::String(value.to_string())));
        }
        if entry == variable {
            return Ok(Some(if negative_entry_is_truthy {
                Value::T
            } else {
                Value::Nil
            }));
        }
    }
    Ok(None)
}

pub(crate) fn append_process_bytes_to_buffer(
    interp: &mut Interpreter,
    destination: &Value,
    bytes: &[u8],
) -> Result<(), LispError> {
    if bytes.is_empty() {
        return Ok(());
    }
    let target_id = match destination {
        Value::T => interp.current_buffer_id(),
        Value::Buffer(_, _) => interp.resolve_buffer_id(destination)?,
        Value::String(name) => interp
            .find_buffer(name)
            .map(|(id, _)| id)
            .unwrap_or_else(|| interp.create_buffer(name).0),
        _ => {
            return Err(LispError::TypeError(
                "buffer-or-name".into(),
                destination.type_name(),
            ));
        }
    };
    let original_id = interp.current_buffer_id();
    if target_id != original_id {
        interp.switch_to_buffer_id(target_id)?;
    }
    interp.insert_current_buffer(&decode_raw_text_bytes(bytes));
    if target_id != original_id {
        interp.switch_to_buffer_id(original_id)?;
    }
    Ok(())
}

pub(crate) fn write_process_bytes_to_file(
    path: &str,
    bytes: &[u8],
    append: bool,
) -> Result<(), LispError> {
    let mut options = fs::OpenOptions::new();
    options.write(true).create(true);
    if append {
        options.append(true);
    } else {
        options.truncate(true);
    }
    let mut file = options
        .open(path)
        .map_err(|error| LispError::Signal(error.to_string()))?;
    file.write_all(bytes)
        .map_err(|error| LispError::Signal(error.to_string()))
}

pub(crate) fn write_process_output(
    interp: &mut Interpreter,
    destination: &Value,
    stdout: &[u8],
    stderr: &[u8],
) -> Result<(), LispError> {
    if destination.is_nil() {
        return Ok(());
    }
    if let Ok(items) = destination.to_vec()
        && items.len() == 2
    {
        if items[0] == Value::Symbol(":file".into()) {
            let path = string_text(&items[1])?;
            if !stdout.is_empty() {
                write_process_bytes_to_file(&path, stdout, false)?;
            }
            return Ok(());
        }
        append_process_bytes_to_buffer(interp, &items[0], stdout)?;
        if !stderr.is_empty() {
            if items[1] == Value::T {
                append_process_bytes_to_buffer(interp, &items[0], stderr)?;
            } else if !items[1].is_nil() {
                let path = string_text(&items[1])?;
                write_process_bytes_to_file(&path, stderr, false)?;
            }
        }
        return Ok(());
    }
    if let Some((stdout_destination, stderr_destination)) = destination.cons_values() {
        append_process_bytes_to_buffer(interp, &stdout_destination, stdout)?;
        if !stderr.is_empty() {
            if stderr_destination == Value::T {
                append_process_bytes_to_buffer(interp, &stdout_destination, stderr)?;
            } else if !stderr_destination.is_nil() {
                let path = string_text(&stderr_destination)?;
                write_process_bytes_to_file(&path, stderr, false)?;
            }
        }
        return Ok(());
    }
    append_process_bytes_to_buffer(interp, destination, stdout)?;
    append_process_bytes_to_buffer(interp, destination, stderr)?;
    Ok(())
}

// ── Native url-retrieve (GNU url.el / url-http.el are blocked: their
// make-network-process transport has no emaxx equivalent) ──

/// Fetch URL (http only) and return the raw response bytes
/// (status line + headers + body).
pub(crate) fn http_fetch_raw(url: &str) -> Result<Vec<u8>, String> {
    let rest = url
        .strip_prefix("http://")
        .ok_or_else(|| format!("Unsupported URL: {url}"))?;
    let (hostport, path) = match rest.find('/') {
        Some(index) => (&rest[..index], &rest[index..]),
        None => (rest, "/"),
    };
    let (host, port) = match hostport.rsplit_once(':') {
        Some((host, port)) => (
            host,
            port.parse::<u16>().map_err(|error| error.to_string())?,
        ),
        None => (hostport, 80),
    };
    use std::io::{Read, Write};
    let mut stream = std::net::TcpStream::connect((host, port))
        .map_err(|error| format!("{host}:{port} {error}"))?;
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(30)))
        .map_err(|error| error.to_string())?;
    write!(
        stream,
        "GET {path} HTTP/1.0\r\nHost: {hostport}\r\nConnection: close\r\n\r\n"
    )
    .map_err(|error| error.to_string())?;
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .map_err(|error| error.to_string())?;
    Ok(response)
}

/// Register an async retrieval: a worker thread fetches the response and
/// the wait loops (accept-process-output, sit-for) deliver it.
pub(crate) fn start_url_retrieval(
    interp: &mut Interpreter,
    env: &mut Env,
    url: &str,
    callback: Value,
    cbargs: Vec<Value>,
) -> Result<Value, LispError> {
    let buffer = call(
        interp,
        "generate-new-buffer",
        &[Value::String(format!(" *http {url}*"))],
        env,
    )?;
    let buffer_id = interp.resolve_buffer_id(&buffer)?;
    let (sender, receiver) = std::sync::mpsc::channel();
    let url_owned = url.to_string();
    std::thread::spawn(move || {
        let _ = sender.send(http_fetch_raw(&url_owned));
    });
    interp
        .pending_url_retrievals
        .push(crate::lisp::eval::PendingUrlRetrieval {
            buffer_id,
            url: url.to_string(),
            callback,
            cbargs,
            receiver,
        });
    Ok(buffer)
}

/// Deliver completed retrievals: fill the response buffer and run the
/// callback there (GNU url-retrieve semantics). Returns true if any fired.
pub(crate) fn run_pending_url_retrievals(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<bool, LispError> {
    let mut ready = Vec::new();
    let mut index = 0;
    while index < interp.pending_url_retrievals.len() {
        match interp.pending_url_retrievals[index].receiver.try_recv() {
            Ok(result) => {
                let pending = interp.pending_url_retrievals.remove(index);
                ready.push((pending, result));
            }
            Err(std::sync::mpsc::TryRecvError::Empty) => index += 1,
            Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                let pending = interp.pending_url_retrievals.remove(index);
                ready.push((pending, Err("connection aborted".into())));
            }
        }
    }
    let fired = !ready.is_empty();
    for (pending, result) in ready {
        let saved_buffer = interp.current_buffer_id();
        interp.switch_to_buffer_id(pending.buffer_id)?;
        let status = match result {
            Ok(bytes) => {
                // Bytes map to chars 0..255 so header parsing stays exact.
                let text: String = bytes.iter().map(|byte| char::from(*byte)).collect();
                interp.insert_current_buffer(&text);
                interp.buffer.goto_char(interp.buffer.point_max());
                // GNU url-http flags non-2xx responses in the callback
                // status plist as (:error (error http CODE)).
                let code = text
                    .lines()
                    .next()
                    .and_then(|line| line.split_whitespace().nth(1))
                    .and_then(|code| code.parse::<i64>().ok());
                match code {
                    Some(code) if !(200..300).contains(&code) => Value::list([
                        Value::Symbol(":error".into()),
                        Value::list([
                            Value::Symbol("error".into()),
                            Value::Symbol("http".into()),
                            Value::Integer(code),
                        ]),
                    ]),
                    _ => Value::Nil,
                }
            }
            Err(message) => Value::list([
                Value::Symbol(":error".into()),
                Value::list([
                    Value::Symbol("error".into()),
                    Value::String(format!("{}: {}", pending.url, message)),
                ]),
            ]),
        };
        let mut call_args = vec![status];
        call_args.extend(pending.cbargs.clone());
        let outcome = call_function_value(interp, &pending.callback, &call_args, env);
        let _ = interp.switch_to_buffer_id(saved_buffer);
        outcome?;
    }
    Ok(fired)
}

/// Drain live external process pipes into their buffers/filters.
/// Returns true if any output was delivered.
pub(crate) fn pump_external_process_output(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<bool, LispError> {
    let ids = interp.live_external_process_ids();
    let mut delivered = false;
    for process_id in ids {
        let (stdout, stderr) = interp.poll_process_output(process_id)?;
        if stdout.is_empty() && stderr.is_empty() {
            continue;
        }
        let mut output = String::from_utf8_lossy(&stdout).into_owned();
        output.push_str(&String::from_utf8_lossy(&stderr));
        deliver_process_output(interp, process_id, &output, env)?;
        delivered = true;
    }
    Ok(delivered)
}

// ── Network processes (GNU process.c make-network-process) ──

/// GNU conv_sockaddr_to_lisp: an inet address as [a b c d port] (ipv4)
/// or [w0 .. w7 port] (ipv6).
fn sockaddr_vector(addr: std::net::SocketAddr) -> Value {
    let mut items = vec![Value::symbol("vector-literal")];
    match addr.ip() {
        std::net::IpAddr::V4(ip) => {
            items.extend(
                ip.octets()
                    .iter()
                    .map(|octet| Value::Integer(*octet as i64)),
            );
        }
        std::net::IpAddr::V6(ip) => {
            items.extend(
                ip.segments()
                    .iter()
                    .map(|segment| Value::Integer(*segment as i64)),
            );
        }
    }
    items.push(Value::Integer(addr.port() as i64));
    Value::list(items)
}

/// plist_get on a proper-list plist value by keyword name.
pub(crate) fn contact_plist_get(plist: &Value, key: &str) -> Value {
    let Ok(items) = plist.to_vec() else {
        return Value::Nil;
    };
    let mut index = 0;
    while index + 1 < items.len() {
        if items[index].as_symbol().is_ok_and(|name| name == key) {
            return items[index + 1].clone();
        }
        index += 2;
    }
    Value::Nil
}

/// plist_put on a flat Vec of plist items: replace KEY's value in place,
/// or append the pair (GNU plist_put appends missing keys at the end).
fn plist_items_put(items: &mut Vec<Value>, key: &str, value: Value) {
    let mut index = 0;
    while index + 1 < items.len() {
        if items[index].as_symbol().is_ok_and(|name| name == key) {
            items[index + 1] = value;
            return;
        }
        index += 2;
    }
    items.push(Value::symbol(key));
    items.push(value);
}

/// GNU Fdelete_process on a network process: set status to (exit 0) and
/// run status_notify SYNCHRONOUSLY, so the process's own sentinel sees
/// "deleted\n" before delete-process returns.  A process whose death was
/// already notified is gone from the process list by then (status_notify
/// removes it under delete-exited-processes), so the sentinel never
/// fires twice; emaxx mirrors that by only notifying while the network
/// runtime is still attached.  Sentinel errors are demoted to a message
/// unless debug-on-error asks for the debugger (exec_sentinel).
pub(crate) fn delete_process_notifying(
    interp: &mut Interpreter,
    process_id: u64,
    env: &mut Env,
) -> Result<(), LispError> {
    let was_network = interp.is_network_process(process_id);
    interp.delete_process(process_id)?;
    if !was_network {
        return Ok(());
    }
    match run_process_sentinel(interp, process_id, "deleted\n", env) {
        Ok(()) => Ok(()),
        Err(error @ LispError::Throw(_, _)) => Err(error),
        Err(error) => {
            if interp
                .lookup_var("debug-on-error", env)
                .is_some_and(|value| value.is_truthy())
            {
                return Err(error);
            }
            let _ = crate::lisp::primitives::call(
                interp,
                "message",
                &[
                    Value::String("error in process sentinel: %S".into()),
                    crate::lisp::eval::error_condition_value(&error),
                ],
                env,
            );
            Ok(())
        }
    }
}

/// Create a network server (`:server t`) or client stream.  emaxx models
/// the subset erc-d exercises: a local/ipv4 TCP server with :filter,
/// :sentinel and :log, plus TCP client streams.
pub(crate) fn make_network_process(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    if !args.len().is_multiple_of(2) {
        return Err(LispError::WrongNumberOfArgs(
            "make-network-process".into(),
            args.len(),
        ));
    }
    let mut name = String::from("emaxx-network");
    let mut buffer_id = None;
    let mut filter = None;
    let mut sentinel = None;
    let mut log = None;
    let mut plist = Value::Nil;
    let mut is_server = false;
    let mut family_local = false;
    let mut family_ipv4 = false;
    let mut nowait = false;
    let mut host: Option<String> = None;
    let mut service: Option<i64> = None;
    let mut service_path: Option<String> = None;

    for pair in args.chunks_exact(2) {
        let key = pair[0].as_symbol()?;
        let value = &pair[1];
        match key {
            ":name" => name = string_text(value)?,
            ":buffer" => buffer_id = process_buffer_target(interp, value)?,
            ":filter" => filter = (!value.is_nil()).then(|| value.clone()),
            ":sentinel" => sentinel = (!value.is_nil()).then(|| value.clone()),
            ":log" => log = (!value.is_nil()).then(|| value.clone()),
            ":plist" => plist = value.clone(),
            ":server" => is_server = value.is_truthy(),
            ":nowait" => nowait = value.is_truthy(),
            ":family" => {
                family_local = matches!(value, Value::Symbol(symbol) if symbol == "local");
                family_ipv4 = matches!(value, Value::Symbol(symbol) if symbol == "ipv4");
            }
            ":host" => {
                host = match value {
                    Value::Nil => None,
                    Value::Symbol(symbol) if symbol == "local" => Some("127.0.0.1".into()),
                    _ => Some(string_text(value)?),
                }
            }
            ":service" => {
                service = match value {
                    // `:service t' asks the OS to pick a free port.
                    Value::T => Some(0),
                    Value::Integer(port) => Some(*port),
                    Value::String(_) | Value::StringObject(_) => {
                        let text = string_text(value)?;
                        // `:family local' names a socket file, not a port.
                        service_path = Some(text.clone());
                        text.parse::<i64>().ok()
                    }
                    _ => None,
                }
            }
            _ => {}
        }
    }

    let name = interp.unique_process_name(&name);
    // GNU keeps the original keyword plist as the process's contact info
    // (p->childp), updating :service and appending the resolved :local /
    // :remote address vectors after the socket is set up.
    let mut contact_items: Vec<Value> = args.to_vec();

    if family_local {
        // `:family local' — a unix domain socket named by :service.  GNU
        // conv_sockaddr_to_lisp yields the PATH string for :local/:remote
        // (a client's own end is unnamed, hence "").
        let path = service_path.ok_or_else(|| {
            LispError::Signal("make-network-process: local family needs a :service path".into())
        })?;
        if is_server {
            let listener = std::os::unix::net::UnixListener::bind(&path)
                .map_err(|error| LispError::Signal(format!("make-network-process: {error}")))?;
            listener
                .set_nonblocking(true)
                .map_err(|error| LispError::Signal(error.to_string()))?;
            plist_items_put(&mut contact_items, ":local", Value::String(path.clone()));
            return interp.create_network_process(
                &name,
                buffer_id,
                filter,
                sentinel,
                log,
                plist,
                crate::lisp::eval::NetworkRuntime::UnixListener(listener),
                Some(path),
                None,
                None,
                None,
                Value::list(contact_items),
            );
        }
        let stream = std::os::unix::net::UnixStream::connect(&path)
            .map_err(|error| LispError::Signal(format!("make-network-process: {error}")))?;
        stream
            .set_nonblocking(true)
            .map_err(|error| LispError::Signal(error.to_string()))?;
        plist_items_put(&mut contact_items, ":remote", Value::String(path.clone()));
        plist_items_put(&mut contact_items, ":local", Value::String(String::new()));
        let process = interp.create_network_process(
            &name,
            buffer_id,
            filter,
            sentinel,
            log,
            plist,
            crate::lisp::eval::NetworkRuntime::UnixStream(stream),
            Some(path),
            None,
            None,
            None,
            Value::list(contact_items),
        )?;
        let process_id = interp.resolve_process_id(&process)?;
        if nowait {
            interp.mark_network_process_connecting(process_id);
        } else {
            run_process_sentinel(interp, process_id, "open\n", env)?;
        }
        return Ok(process);
    }

    if is_server {
        let bind_host = host.clone().unwrap_or_else(|| "127.0.0.1".into());
        let bind_port = service.unwrap_or(0) as u16;
        let listener = if family_ipv4 {
            let address =
                std::net::ToSocketAddrs::to_socket_addrs(&(bind_host.as_str(), bind_port))
                    .map_err(|error| LispError::Signal(format!("make-network-process: {error}")))?
                    .find(std::net::SocketAddr::is_ipv4)
                    .ok_or_else(|| {
                        LispError::Signal(format!(
                            "make-network-process: no IPv4 address for {bind_host}"
                        ))
                    })?;
            std::net::TcpListener::bind(address)
        } else {
            std::net::TcpListener::bind((bind_host.as_str(), bind_port))
        }
        .map_err(|error| LispError::Signal(format!("make-network-process: {error}")))?;
        listener
            .set_nonblocking(true)
            .map_err(|error| LispError::Signal(error.to_string()))?;
        let local_addr = listener.local_addr().ok();
        let bound_port = local_addr.map(|addr| addr.port() as i64).or(service);
        if let Some(port) = bound_port {
            plist_items_put(&mut contact_items, ":service", Value::Integer(port));
        }
        if let Some(addr) = local_addr {
            plist_items_put(&mut contact_items, ":local", sockaddr_vector(addr));
        }
        interp.create_network_process(
            &name,
            buffer_id,
            filter,
            sentinel,
            log,
            plist,
            crate::lisp::eval::NetworkRuntime::Listener(listener),
            host,
            bound_port,
            None,
            None,
            Value::list(contact_items),
        )
    } else {
        let connect_host = host.clone().unwrap_or_else(|| "127.0.0.1".into());
        let stream =
            std::net::TcpStream::connect((connect_host.as_str(), service.unwrap_or(0) as u16))
                .map_err(|error| LispError::Signal(format!("make-network-process: {error}")))?;
        stream
            .set_nonblocking(true)
            .map_err(|error| LispError::Signal(error.to_string()))?;
        if let Ok(addr) = stream.peer_addr() {
            plist_items_put(&mut contact_items, ":remote", sockaddr_vector(addr));
        }
        if let Ok(addr) = stream.local_addr() {
            plist_items_put(&mut contact_items, ":local", sockaddr_vector(addr));
        }
        let process = interp.create_network_process(
            &name,
            buffer_id,
            filter,
            sentinel,
            log,
            plist,
            crate::lisp::eval::NetworkRuntime::Stream(stream),
            host,
            service,
            None,
            None,
            Value::list(contact_items),
        )?;
        // GNU runs the sentinel with "open\n" once a client connects.
        let process_id = interp.resolve_process_id(&process)?;
        if nowait {
            interp.mark_network_process_connecting(process_id);
        } else {
            run_process_sentinel(interp, process_id, "open\n", env)?;
        }
        Ok(process)
    }
}

fn run_process_sentinel(
    interp: &mut Interpreter,
    process_id: u64,
    event: &str,
    env: &mut Env,
) -> Result<(), LispError> {
    if let Some(sentinel) = interp.process_sentinel(process_id) {
        call_function_value(
            interp,
            &sentinel,
            &[Value::Record(process_id), Value::String(event.to_string())],
            env,
        )?;
    }
    Ok(())
}

fn run_process_log(
    interp: &mut Interpreter,
    server_id: u64,
    client_id: u64,
    message: &str,
    env: &mut Env,
) -> Result<(), LispError> {
    if let Some(log) = interp.process_log_function(server_id) {
        call_function_value(
            interp,
            &log,
            &[
                Value::Record(server_id),
                Value::Record(client_id),
                Value::String(message.to_string()),
            ],
            env,
        )?;
    }
    Ok(())
}

/// GNU wait_reading_process_output: wait up to TOTAL, handling process
/// output as it arrives and firing timers as they come due, instead of
/// sleeping blind.  While progress is being made the loop re-pumps
/// immediately, so an in-process client/server exchange completes at full
/// speed rather than one round-trip per wait call.  With RETURN_ON_DELIVERY
/// (accept-process-output) the wait ends as soon as output was handled.
/// Returns whether any process output was delivered.
pub(crate) fn wait_pumping_processes(
    interp: &mut Interpreter,
    env: &mut Env,
    total: std::time::Duration,
    return_on_delivery: bool,
) -> Result<bool, LispError> {
    let deadline = std::time::Instant::now() + total;
    let mut delivered = false;
    loop {
        let mut progressed = pump_external_process_output(interp, env)?;
        progressed |= pump_network_processes(interp, env)?;
        progressed |= run_pending_url_retrievals(interp, env)?;
        delivered |= progressed;
        interp.drive_threads(env, true)?;
        if return_on_delivery && delivered {
            break;
        }
        let now = std::time::Instant::now();
        if now >= deadline {
            break;
        }
        if progressed {
            continue;
        }
        // Idle: nap until the deadline or the next due timer, polling at
        // most every 10ms so freshly arriving input is picked up promptly.
        let mut nap = deadline - now;
        if let Some(due) = interp.next_timer_due() {
            nap = nap.min(due.saturating_duration_since(now));
        }
        nap = nap
            .min(std::time::Duration::from_millis(10))
            .max(std::time::Duration::from_millis(1));
        std::thread::sleep(nap);
    }
    Ok(delivered)
}

/// Accept pending server connections and deliver stream input/closure to
/// filters and sentinels.  Returns true if anything was processed.
pub(crate) fn pump_network_processes(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<bool, LispError> {
    let mut progressed = false;

    // `:nowait t' exposes a freshly created client as `connect' until the
    // event loop reports completion.  The OS connection is already usable
    // in this compatibility runtime, but deferring the `open' transition
    // lets callers install their sentinel before it runs, like GNU Emacs.
    for process_id in interp.open_connecting_network_processes() {
        progressed = true;
        run_process_sentinel(interp, process_id, "open\n", env)?;
    }

    // Accept new connections on every server listener.
    for server_id in interp.network_listener_ids() {
        loop {
            let Some((child_runtime, peer_addr)) = interp.accept_network_connection(server_id)?
            else {
                break;
            };
            progressed = true;
            interp.network_connect_counter += 1;
            let connect_number = interp.network_connect_counter;
            // GNU server_accept_connection gives each child a fresh copy of
            // the server's plist (Fcopy_sequence), so the child's later
            // `process-put' (plist-put mutates value cells in place) never
            // clobbers the server's own properties.
            let server_plist = interp.process_plist_value(server_id).unwrap_or(Value::Nil);
            let server_plist = match server_plist.to_vec() {
                Ok(items) => Value::list(items),
                Err(_) => server_plist,
            };
            // The child's contact info is the server's with :server nil,
            // :host/:service naming the peer, and :remote its address.
            // Unix peers are unnamed: :host t, :remote "", :service and
            // :local keep the server's socket path.
            let mut contact_items = interp
                .process_contact_plist(server_id)
                .unwrap_or(Value::Nil)
                .to_vec()
                .unwrap_or_default();
            plist_items_put(&mut contact_items, ":server", Value::Nil);
            let remote = match peer_addr {
                Some(peer_addr) => {
                    plist_items_put(
                        &mut contact_items,
                        ":host",
                        Value::String(peer_addr.ip().to_string()),
                    );
                    plist_items_put(
                        &mut contact_items,
                        ":service",
                        Value::Integer(peer_addr.port() as i64),
                    );
                    if let crate::lisp::eval::NetworkRuntime::Stream(stream) = &child_runtime
                        && let Ok(addr) = stream.local_addr()
                    {
                        plist_items_put(&mut contact_items, ":local", sockaddr_vector(addr));
                    }
                    plist_items_put(&mut contact_items, ":remote", sockaddr_vector(peer_addr));
                    peer_addr.to_string()
                }
                None => {
                    plist_items_put(&mut contact_items, ":host", Value::T);
                    plist_items_put(&mut contact_items, ":remote", Value::String(String::new()));
                    String::new()
                }
            };
            let server_filter = interp.process_filter(server_id);
            let server_sentinel = interp.process_sentinel(server_id);
            let server_log = interp.process_log_function(server_id);
            let server_buffer = interp.process_buffer_id(server_id);
            let base_name = interp.process_name(server_id).unwrap_or_default();
            // GNU names inet children "NAME <HOST:PORT>" (note the space)
            // and unnamed-peer children "NAME <N>" from connect_counter.
            let child_name = if peer_addr.is_some() {
                interp.unique_process_name(&format!("{base_name} <{remote}>"))
            } else {
                interp.unique_process_name(&format!("{base_name} <{connect_number}>"))
            };
            let child = interp.create_network_process(
                &child_name,
                server_buffer,
                server_filter,
                server_sentinel,
                server_log,
                server_plist,
                child_runtime,
                None,
                None,
                Some(remote.clone()),
                Some(server_id),
                Value::list(contact_items),
            )?;
            let child_id = interp.resolve_process_id(&child)?;
            run_process_log(
                interp,
                server_id,
                child_id,
                &format!("accept from {remote}"),
                env,
            )?;
            run_process_sentinel(interp, child_id, &format!("open from {remote}\n"), env)?;
        }
    }

    // Deliver input / closure on every open stream.
    for stream_id in interp.network_stream_ids() {
        let (bytes, closed) = interp.poll_network_stream(stream_id)?;
        if !bytes.is_empty() {
            progressed = true;
            let output = crate::lisp::primitives::coding::bytes_to_shared_unibyte_value(&bytes);
            let text = string_text(&output)?;
            deliver_process_output(interp, stream_id, &text, env)?;
        }
        if closed {
            progressed = true;
            run_process_sentinel(interp, stream_id, "connection broken by remote peer\n", env)?;
        }
    }

    Ok(progressed)
}
