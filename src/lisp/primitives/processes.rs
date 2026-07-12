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

    for pair in args.chunks_exact(2) {
        let key = pair[0].as_symbol()?;
        let value = &pair[1];
        match key {
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

    Ok((buffer_id, program, argv, filter, coding))
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
    let mut host: Option<String> = None;
    let mut service: Option<i64> = None;

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
                        string_text(value)?.parse::<i64>().ok()
                    }
                    _ => None,
                }
            }
            _ => {}
        }
    }

    let name = interp.unique_process_name(&name);

    if is_server {
        let bind_host = host.clone().unwrap_or_else(|| "127.0.0.1".into());
        let listener =
            std::net::TcpListener::bind((bind_host.as_str(), service.unwrap_or(0) as u16))
                .map_err(|error| LispError::Signal(format!("make-network-process: {error}")))?;
        listener
            .set_nonblocking(true)
            .map_err(|error| LispError::Signal(error.to_string()))?;
        let bound_port = listener
            .local_addr()
            .ok()
            .map(|addr| addr.port() as i64)
            .or(service);
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
        )
    } else {
        let connect_host = host.clone().unwrap_or_else(|| "127.0.0.1".into());
        let stream =
            std::net::TcpStream::connect((connect_host.as_str(), service.unwrap_or(0) as u16))
                .map_err(|error| LispError::Signal(format!("make-network-process: {error}")))?;
        stream
            .set_nonblocking(true)
            .map_err(|error| LispError::Signal(error.to_string()))?;
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
        )?;
        // GNU runs the sentinel with "open\n" once a client connects.
        let process_id = interp.resolve_process_id(&process)?;
        run_process_sentinel(interp, process_id, "open\n", env)?;
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

/// Accept pending server connections and deliver stream input/closure to
/// filters and sentinels.  Returns true if anything was processed.
pub(crate) fn pump_network_processes(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<bool, LispError> {
    let mut progressed = false;

    // Accept new connections on every server listener.
    for server_id in interp.network_listener_ids() {
        loop {
            let Some((stream, remote)) = interp.accept_network_connection(server_id)? else {
                break;
            };
            progressed = true;
            // GNU server_accept_connection gives each child a fresh copy of
            // the server's plist (Fcopy_sequence), so the child's later
            // `process-put' (plist-put mutates value cells in place) never
            // clobbers the server's own properties.
            let server_plist = interp.process_plist_value(server_id).unwrap_or(Value::Nil);
            let server_plist = match server_plist.to_vec() {
                Ok(items) => Value::list(items),
                Err(_) => server_plist,
            };
            let server_filter = interp.process_filter(server_id);
            let server_sentinel = interp.process_sentinel(server_id);
            let server_log = interp.process_log_function(server_id);
            let server_buffer = interp.process_buffer_id(server_id);
            let base_name = interp.process_name(server_id).unwrap_or_default();
            let child_name = interp.unique_process_name(&format!("{base_name}<{remote}>"));
            let child = interp.create_network_process(
                &child_name,
                server_buffer,
                server_filter,
                server_sentinel,
                server_log,
                server_plist,
                crate::lisp::eval::NetworkRuntime::Stream(stream),
                None,
                None,
                Some(remote.clone()),
                Some(server_id),
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
