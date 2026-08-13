use super::*;

pub(crate) fn run_external_process(
    interp: &Interpreter,
    program: &str,
    argv: &[String],
    input: Option<&[u8]>,
    env: &Env,
) -> Result<std::process::Output, LispError> {
    #[cfg(test)]
    crate::test_support::mark_process_test();

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
        // `Command::current_dir` is a host boundary.  A file-name handler may
        // retain Lisp's logical remote directory while its transport runs the
        // actual program locally, so resolve it through the same path policy
        // as every other host filesystem operation.
        command.current_dir(resolve_file_name_in_env(interp, env, &default_directory));
    }
    apply_process_environment(interp, env, command);
}

pub(crate) fn spawn_persistent_process(
    interp: &Interpreter,
    program: &str,
    argv: &[String],
    env: &Env,
    connection_type: Option<&Value>,
    separate_stderr: bool,
) -> Result<RunningProcess, LispError> {
    #[cfg(test)]
    crate::test_support::mark_process_test();

    let mut command = Command::new(program);
    command.args(argv);
    configure_external_command(interp, env, &mut command);

    let default_pty = interp
        .lookup_var("process-connection-type", env)
        .is_some_and(|value| value.is_truthy());
    let (input_pty, output_pty) = process_connection_pty_modes(connection_type, default_pty)?;
    let mut pty_input = None;
    let mut pty_output = None;
    let mut pty_slave_guard = None;

    #[cfg(unix)]
    {
        if input_pty && output_pty {
            let (master, slave) = open_emacs_pty()?;
            pty_slave_guard = Some(
                slave
                    .try_clone()
                    .map_err(|error| LispError::Signal(error.to_string()))?,
            );
            command.stdin(Stdio::from(
                slave
                    .try_clone()
                    .map_err(|error| LispError::Signal(error.to_string()))?,
            ));
            command.stdout(Stdio::from(
                slave
                    .try_clone()
                    .map_err(|error| LispError::Signal(error.to_string()))?,
            ));
            if separate_stderr {
                command.stderr(Stdio::piped());
            } else {
                command.stderr(Stdio::from(slave));
            }
            pty_input = Some(
                master
                    .try_clone()
                    .map_err(|error| LispError::Signal(error.to_string()))?,
            );
            pty_output = Some(master);
        } else if input_pty {
            let (master, slave) = open_emacs_pty()?;
            command.stdin(Stdio::from(slave));
            command.stdout(Stdio::piped());
            command.stderr(Stdio::piped());
            pty_input = Some(master);
        } else {
            command.stdin(Stdio::piped());
            if output_pty {
                let (master, slave) = open_emacs_pty()?;
                pty_slave_guard = Some(
                    slave
                        .try_clone()
                        .map_err(|error| LispError::Signal(error.to_string()))?,
                );
                command.stdout(Stdio::from(
                    slave
                        .try_clone()
                        .map_err(|error| LispError::Signal(error.to_string()))?,
                ));
                if separate_stderr {
                    command.stderr(Stdio::piped());
                } else {
                    command.stderr(Stdio::from(slave));
                }
                pty_output = Some(master);
            } else {
                command.stdout(Stdio::piped());
                command.stderr(Stdio::piped());
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (input_pty, output_pty, separate_stderr);
        command.stdin(Stdio::piped());
        command.stdout(Stdio::piped());
        command.stderr(Stdio::piped());
    }

    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;

        // GNU gives every asynchronous child its own process group.  PTY
        // children also lead a fresh session and acquire stdin's slave as
        // their controlling terminal.  Signals then target the child job,
        // including descendants, instead of Emaxx's own process group.
        // SAFETY: setsid/setpgid/ioctl are async-signal-safe child-side
        // operations; fd 0 is already the PTY slave for input_pty.
        unsafe {
            command.pre_exec(move || {
                if input_pty || output_pty {
                    if libc::setsid() < 0 {
                        return Err(std::io::Error::last_os_error());
                    }
                } else if libc::setpgid(0, 0) < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                if input_pty && libc::ioctl(libc::STDIN_FILENO, libc::TIOCSCTTY.into(), 0) < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
    }

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
        if let Some(output) = pty_output.as_ref() {
            set_nonblocking(output)?;
        }
    }
    Ok(RunningProcess {
        child,
        pty_input,
        pty_output,
        pty_slave_guard,
    })
}

fn process_connection_pty_modes(
    connection_type: Option<&Value>,
    default_pty: bool,
) -> Result<(bool, bool), LispError> {
    let Some(connection_type) = connection_type else {
        return Ok((default_pty, default_pty));
    };
    if let Some((input, output)) = connection_type.cons_values() {
        return Ok((
            process_connection_endpoint_uses_pty(&input, default_pty)?,
            process_connection_endpoint_uses_pty(&output, default_pty)?,
        ));
    }
    let pty = process_connection_endpoint_uses_pty(connection_type, default_pty)?;
    Ok((pty, pty))
}

fn process_connection_endpoint_uses_pty(
    value: &Value,
    default_pty: bool,
) -> Result<bool, LispError> {
    match value {
        Value::Nil => Ok(default_pty),
        Value::Symbol(name) if name == "pty" => Ok(true),
        Value::Symbol(name) if name == "pipe" => Ok(false),
        _ => Err(LispError::Signal(format!(
            "Unknown connection type: {value}"
        ))),
    }
}

#[cfg(unix)]
fn open_emacs_pty() -> Result<(fs::File, fs::File), LispError> {
    let mut master = -1;
    let mut slave = -1;
    // SAFETY: `openpty' initializes both file descriptors; the optional
    // name/termios/winsize pointers are allowed to be null.
    if unsafe {
        libc::openpty(
            &mut master,
            &mut slave,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
    } != 0
    {
        return Err(LispError::Signal(
            std::io::Error::last_os_error().to_string(),
        ));
    }
    // SAFETY: ownership of the fresh descriptors is transferred exactly once.
    let master = unsafe { fs::File::from_raw_fd(master) };
    // SAFETY: ownership of the fresh descriptor is transferred exactly once.
    let slave = unsafe { fs::File::from_raw_fd(slave) };
    for fd in [master.as_raw_fd(), slave.as_raw_fd()] {
        // SAFETY: both descriptors are live and owned by the File values.
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
        if flags < 0 || unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) } < 0 {
            return Err(LispError::Signal(
                std::io::Error::last_os_error().to_string(),
            ));
        }
    }

    let mut attributes = std::mem::MaybeUninit::<libc::termios>::uninit();
    // SAFETY: `slave' is a live terminal fd and `tcgetattr' initializes the
    // termios value on success.
    if unsafe { libc::tcgetattr(slave.as_raw_fd(), attributes.as_mut_ptr()) } == 0 {
        // SAFETY: the successful `tcgetattr' initialized `attributes'.
        let mut attributes = unsafe { attributes.assume_init() };
        // GNU leaves canonical input and EOF processing enabled, while
        // disabling echo and CRLF expansion because the editor owns those
        // presentation concerns.  In particular, canonical VEOF is what
        // makes `process-send-eof' meaningful for PTY subprocesses.
        attributes.c_oflag |= libc::OPOST;
        attributes.c_oflag &= !libc::ONLCR;
        attributes.c_lflag &= !libc::ECHO;
        attributes.c_lflag |= libc::ISIG | libc::ICANON;
        attributes.c_iflag &= !libc::ISTRIP;
        attributes.c_cflag = (attributes.c_cflag & !libc::CSIZE) | libc::CS8;
        attributes.c_cc[libc::VEOF] = 4;
        // SAFETY: `tcsetattr' operates on initialized termios storage and
        // the same live terminal fd.
        if unsafe { libc::tcsetattr(slave.as_raw_fd(), libc::TCSANOW, &attributes) } < 0 {
            return Err(LispError::Signal(
                std::io::Error::last_os_error().to_string(),
            ));
        }
    }
    Ok((master, slave))
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
    if matches!(value, Value::Buffer(_)) {
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

fn process_creation_coding_systems(
    interp: &Interpreter,
    env: &Env,
    coding: &Value,
) -> (Value, Value) {
    if coding.is_nil() {
        (
            interp
                .lookup_var("coding-system-for-read", env)
                .unwrap_or(Value::Nil),
            interp
                .lookup_var("coding-system-for-write", env)
                .unwrap_or(Value::Nil),
        )
    } else {
        coding
            .cons_values()
            .unwrap_or_else(|| (coding.clone(), coding.clone()))
    }
}

pub(crate) struct MakeProcessArgs {
    pub(crate) buffer_id: Option<u64>,
    pub(crate) program: Option<String>,
    pub(crate) argv: Vec<String>,
    pub(crate) filter: Option<Value>,
    pub(crate) sentinel: Option<Value>,
    pub(crate) coding: Option<(Value, Value)>,
    pub(crate) name: Option<String>,
    pub(crate) stderr_process_id: Option<u64>,
    pub(crate) file_handler: bool,
    pub(crate) connection_type: Option<Value>,
}

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
    let mut sentinel = None;
    let mut coding = None;
    let mut name = None;
    let mut stderr_process_id = None;
    let mut file_handler = false;
    let mut connection_type = None;

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
            ":sentinel" => sentinel = (!value.is_nil()).then(|| value.clone()),
            ":coding" => coding = Some(process_coding_pair(value)?),
            ":stderr" if !value.is_nil() => {
                stderr_process_id = Some(interp.resolve_process_id(value)?);
            }
            ":file-handler" => file_handler = value.is_truthy(),
            ":connection-type" => connection_type = Some(value.clone()),
            _ => {}
        }
    }

    Ok(MakeProcessArgs {
        buffer_id,
        program,
        argv,
        filter,
        sentinel,
        coding,
        name,
        stderr_process_id,
        file_handler,
        connection_type,
    })
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
    interp.note_process_output_delivery(process_id);

    // The process buffer may have been killed before straggler output is
    // delivered (epg-reset kills it right after completion); GNU discards
    // such output rather than erroring.
    let target_buffer_id = interp
        .process_buffer_id(process_id)
        .filter(|buffer_id| interp.get_buffer_by_id(*buffer_id).is_some());
    if let Some(filter) = interp.process_filter(process_id) {
        // GNU uses t as a flow-control sentinel: the descriptor is removed
        // from the read set until a real/default filter is installed again.
        // The pump normally avoids reaching this branch, but retaining the
        // guard makes direct delivery equally safe.
        if filter == Value::T {
            return Ok(());
        }
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
            &[
                Value::Record(process_id),
                Value::String(output.to_string().into()),
            ],
            env,
        );
        if switched {
            interp.switch_to_buffer_id(saved_buffer_id)?;
        }
        result?;
        return Ok(());
    }

    internal_default_process_filter(interp, process_id, output)
}

/// GNU `internal-default-process-filter'.  Insert at the process mark before
/// other markers, preserve the buffer's logical point, and discard output
/// when the process has no live buffer.
pub(crate) fn internal_default_process_filter(
    interp: &mut Interpreter,
    process_id: u64,
    output: &str,
) -> Result<(), LispError> {
    let target_buffer_id = interp
        .process_buffer_id(process_id)
        .filter(|buffer_id| interp.get_buffer_by_id(*buffer_id).is_some());
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
    let old_point = interp.buffer.point();
    let insert_at = interp
        .marker_position(mark_id)
        .unwrap_or_else(|| interp.current_buffer().point_max());
    interp.buffer.goto_char(insert_at);
    interp.insert_current_buffer_before_markers(output);
    let new_pos = interp.buffer.point();
    let result = interp.set_marker(mark_id, Some(new_pos), Some(buffer_id));
    let inserted_chars = output.chars().count();
    let restored_point = if old_point >= insert_at {
        old_point.saturating_add(inserted_chars)
    } else {
        old_point
    };
    interp.buffer.goto_char(restored_point);
    if switched {
        interp.switch_to_buffer_id(saved_buffer_id)?;
    }
    result
}

/// GNU `internal-default-process-sentinel'.  Non-running state changes are
/// appended at the process mark without letting a process callback retarget
/// the caller's current buffer or point.
pub(crate) fn internal_default_process_sentinel(
    interp: &mut Interpreter,
    process_id: u64,
    message: &str,
) -> Result<(), LispError> {
    if matches!(
        interp.process_status_value(process_id),
        Some(Value::Symbol(status)) if matches!(status.as_str(), "run" | "open")
    ) {
        return Ok(());
    }
    let Some(buffer_id) = interp
        .process_buffer_id(process_id)
        .filter(|buffer_id| interp.get_buffer_by_id(*buffer_id).is_some())
    else {
        return Ok(());
    };
    let Some(mark_id) = interp.process_mark_id(process_id) else {
        return Ok(());
    };
    let process_name = interp.process_name(process_id).unwrap_or_default();
    let text = format!("\nProcess {process_name} {message}");
    let saved_buffer_id = interp.current_buffer_id();
    let switched = buffer_id != saved_buffer_id;
    if switched {
        interp.switch_to_buffer_id(buffer_id)?;
    }
    let old_point = interp.buffer.point();
    let insert_at = interp
        .marker_position(mark_id)
        .unwrap_or_else(|| interp.current_buffer().point_max());
    interp.buffer.goto_char(insert_at);
    interp.insert_current_buffer(&text);
    let new_pos = interp.buffer.point();
    let result = interp.set_marker(mark_id, Some(new_pos), Some(buffer_id));
    let inserted_chars = text.chars().count();
    let restored_point = if old_point >= insert_at {
        old_point.saturating_add(inserted_chars)
    } else {
        old_point
    };
    interp.buffer.goto_char(restored_point);
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
    // Lisp environment lists are first-match-wins (`getenv-internal').
    // Apply them back-to-front so duplicate names have the same precedence
    // when materialized in the host command environment.
    for entry in entries.into_iter().rev() {
        if let Some((name, value)) = entry.split_once('=') {
            command.env(name, value);
        }
    }
}

pub(crate) fn process_environment_entries(value: &Value) -> Result<Vec<String>, LispError> {
    let environment = match value.cons_values() {
        Some((Value::Symbol(symbol), entries)) if symbol == "environment" => entries,
        _ => value.clone(),
    };
    environment
        .to_vec()?
        .into_iter()
        .map(|item| string_text(&item))
        .collect()
}

pub(crate) fn process_environment_from_entries(entries: &[String]) -> Value {
    Value::list(
        entries
            .iter()
            .cloned()
            .map(|value| Value::String(value.into())),
    )
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

pub(crate) fn updated_process_environment(
    environment: &Value,
    variable: &str,
    value: Option<&str>,
    keep_empty: bool,
) -> Result<Value, LispError> {
    let wrapped = matches!(
        environment.cons_values(),
        Some((Value::Symbol(ref symbol), _)) if symbol == "environment"
    );
    let mut entries = process_environment_entries(environment)?;
    setenv_in_environment_entries(&mut entries, variable, value, keep_empty);
    let updated = process_environment_from_entries(&entries);
    Ok(if wrapped {
        Value::cons(Value::Symbol("environment".into()), updated)
    } else {
        updated
    })
}

pub(crate) fn getenv_in_environment(
    variable: &str,
    environment: &Value,
    negative_entry_is_truthy: bool,
) -> Result<Option<Value>, LispError> {
    let prefix = format!("{variable}=");
    for entry in process_environment_entries(environment)? {
        if let Some(value) = entry.strip_prefix(&prefix) {
            return Ok(Some(Value::String(value.to_string().into())));
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
    operation: &str,
    operation_args: &[Value],
    env: &mut Env,
) -> Result<(), LispError> {
    if bytes.is_empty() {
        return Ok(());
    }
    let target_id = match destination {
        Value::T => interp.current_buffer_id(),
        Value::Buffer(_) => interp.resolve_buffer_id(destination)?,
        value => {
            let Some(name) = string_like(value) else {
                return Err(LispError::TypeError(
                    "buffer-or-name".into(),
                    destination.type_name(),
                ));
            };
            interp
                .find_buffer(&name.text)
                .map(|(id, _)| id)
                .unwrap_or_else(|| interp.create_buffer(&name.text).0)
        }
    };
    let original_id = interp.current_buffer_id();
    if target_id != original_id {
        interp.switch_to_buffer_id(target_id)?;
    }
    let mut coding = interp
        .lookup_var("coding-system-for-read", env)
        .filter(|value| !value.is_nil())
        .map(|value| checked_coding_symbol(interp, &value))
        .transpose()?;
    if coding.is_none() {
        let mut lookup_args = Vec::with_capacity(operation_args.len() + 1);
        lookup_args.push(Value::Symbol(operation.into()));
        lookup_args.extend_from_slice(operation_args);
        let operation_coding = find_operation_coding_system_value(interp, &lookup_args, env)?;
        coding = operation_coding
            .cons_values()
            .and_then(|(decoding, _)| (!decoding.is_nil()).then_some(decoding))
            .map(|value| checked_coding_symbol(interp, &value))
            .transpose()?;
    }
    if coding.is_none() {
        coding = interp
            .lookup_var("default-process-coding-system", env)
            .and_then(|value| value.cons_values())
            .and_then(|(decoding, _)| (!decoding.is_nil()).then_some(decoding))
            .map(|value| checked_coding_symbol(interp, &value))
            .transpose()?;
    }
    let mut coding = coding.unwrap_or_else(|| "undecided".into());
    if !interp.buffer.is_multibyte() {
        coding = coding_variant_name(
            interp,
            "raw-text",
            interp.coding_system_eol_type_value(&coding),
        );
    }
    let (text, coding_used) =
        if interp.coding_system_kind_name(&coding).as_deref() == Some("undecided") {
            let (detected, normalized) = auto_detect_coding(interp, bytes);
            (decode_text_bytes(interp, &normalized, &detected)?, detected)
        } else {
            let actual_eol = detect_eol_type(bytes);
            let normalized = decode_bytes_with_explicit_eol(
                bytes,
                interp
                    .coding_system_eol_type_value(&coding)
                    .unwrap_or(actual_eol),
            );
            let canonical = interp
                .coding_system_canonical_name(&coding)
                .unwrap_or(coding);
            let base = interp
                .coding_system_base_name(&canonical)
                .unwrap_or_else(|| canonical.clone());
            let coding_used = if interp.coding_system_eol_type_value(&canonical).is_some() {
                canonical
            } else {
                coding_variant_name(interp, &base, Some(actual_eol))
            };
            let source = if interp.coding_system_kind_name(&coding_used).as_deref()
                == Some("utf-8-with-signature")
            {
                strip_utf8_bom(&normalized).1
            } else {
                &normalized
            };
            (
                decode_text_bytes(interp, source, &coding_used)?,
                coding_used,
            )
        };
    interp.insert_current_buffer(&text);
    interp.set_variable(
        "last-coding-system-used",
        Value::Symbol(coding_used.into()),
        env,
    );
    if target_id != original_id {
        interp.switch_to_buffer_id(original_id)?;
    }
    Ok(())
}

/// Deliver the two child streams to their GNU process objects.  A `:stderr'
/// pipe has its own filter/buffer; folding stderr into the primary process
/// loses that observable routing during synchronous send/EOF drains.
pub(crate) fn deliver_process_streams(
    interp: &mut Interpreter,
    process_id: u64,
    stdout: &[u8],
    stderr: &[u8],
    env: &mut Env,
) -> Result<bool, LispError> {
    let mut delivered = false;
    if !stdout.is_empty() {
        let output = String::from_utf8_lossy(stdout).into_owned();
        deliver_process_output(interp, process_id, &output, env)?;
        delivered = true;
    }
    if !stderr.is_empty() {
        let stderr_process_id = interp.process_stderr(process_id).unwrap_or(process_id);
        let output = String::from_utf8_lossy(stderr).into_owned();
        deliver_process_output(interp, stderr_process_id, &output, env)?;
        delivered = true;
    }
    Ok(delivered)
}

pub(crate) fn write_process_bytes_to_file(
    path: &str,
    bytes: &[u8],
    append: bool,
) -> Result<(), LispError> {
    let path = unquote_local_file_name(path).unwrap_or_else(|| path.to_string());
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
    operation: &str,
    operation_args: &[Value],
    env: &mut Env,
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
        append_process_bytes_to_buffer(interp, &items[0], stdout, operation, operation_args, env)?;
        if !stderr.is_empty() {
            if items[1] == Value::T {
                append_process_bytes_to_buffer(
                    interp,
                    &items[0],
                    stderr,
                    operation,
                    operation_args,
                    env,
                )?;
            } else if !items[1].is_nil() {
                let path = string_text(&items[1])?;
                write_process_bytes_to_file(&path, stderr, false)?;
            }
        }
        return Ok(());
    }
    if let Some((stdout_destination, stderr_destination)) = destination.cons_values() {
        append_process_bytes_to_buffer(
            interp,
            &stdout_destination,
            stdout,
            operation,
            operation_args,
            env,
        )?;
        if !stderr.is_empty() {
            if stderr_destination == Value::T {
                append_process_bytes_to_buffer(
                    interp,
                    &stdout_destination,
                    stderr,
                    operation,
                    operation_args,
                    env,
                )?;
            } else if !stderr_destination.is_nil() {
                let path = string_text(&stderr_destination)?;
                write_process_bytes_to_file(&path, stderr, false)?;
            }
        }
        return Ok(());
    }
    append_process_bytes_to_buffer(interp, destination, stdout, operation, operation_args, env)?;
    append_process_bytes_to_buffer(interp, destination, stderr, operation, operation_args, env)?;
    Ok(())
}

/// Drain live external process pipes into their buffers/filters.
/// Returns true if any output was delivered.
pub(crate) fn pump_external_process_output(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<bool, LispError> {
    let ids = interp.live_external_process_ids();
    let mut progressed = false;
    for process_id in ids {
        if interp.process_output_paused(process_id) {
            // Status changes still arrive while output is held.  GNU removes
            // only the read descriptor, not SIGCHLD/status observation.
            interp.refresh_process_id(process_id)?;
            continue;
        }
        let (stdout, stderr) = interp.poll_process_output(process_id)?;
        progressed |= deliver_process_streams(interp, process_id, &stdout, &stderr, env)?;
    }
    for (process_id, event) in interp.take_pending_subprocess_exit_events() {
        run_process_sentinel(interp, process_id, &event, env)?;
        progressed = true;
    }
    Ok(progressed)
}

// ── Network processes (GNU process.c make-network-process) ──

/// GNU conv_sockaddr_to_lisp: an inet address as [a b c d port] (ipv4)
/// or [w0 .. w7 port] (ipv6).
pub(crate) fn sockaddr_vector(addr: std::net::SocketAddr) -> Value {
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

pub(crate) fn socket_addr_from_value(value: &Value) -> Option<std::net::SocketAddr> {
    let mut items = value.to_vec().ok()?;
    if matches!(items.first(), Some(Value::Symbol(tag)) if tag == "vector-literal") {
        items.remove(0);
    }
    let integers = items
        .iter()
        .map(Value::as_integer)
        .collect::<Result<Vec<_>, _>>()
        .ok()?;
    match integers.as_slice() {
        [a, b, c, d, port] => Some(std::net::SocketAddr::from((
            [
                u8::try_from(*a).ok()?,
                u8::try_from(*b).ok()?,
                u8::try_from(*c).ok()?,
                u8::try_from(*d).ok()?,
            ],
            u16::try_from(*port).ok()?,
        ))),
        [a, b, c, d, e, f, g, h, port] => Some(std::net::SocketAddr::from((
            [
                u16::try_from(*a).ok()?,
                u16::try_from(*b).ok()?,
                u16::try_from(*c).ok()?,
                u16::try_from(*d).ok()?,
                u16::try_from(*e).ok()?,
                u16::try_from(*f).ok()?,
                u16::try_from(*g).ok()?,
                u16::try_from(*h).ok()?,
            ],
            u16::try_from(*port).ok()?,
        ))),
        _ => None,
    }
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

/// GNU Fdelete_process runs status notification synchronously for every
/// process kind.  Marking the sentinel notified before this call prevents
/// the ordinary event pump from delivering the same terminal transition a
/// second time.  Sentinel errors are demoted to a message unless
/// debug-on-error asks for the debugger (exec_sentinel).
pub(crate) fn delete_process_notifying(
    interp: &mut Interpreter,
    process_id: u64,
    env: &mut Env,
) -> Result<(), LispError> {
    let (event, notify_sentinel) = interp.delete_process(process_id)?;
    if !notify_sentinel {
        return Ok(());
    }
    match run_process_sentinel(interp, process_id, event, env) {
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

fn network_io_error_detail(error: &std::io::Error) -> String {
    let rendered = error.to_string();
    match rendered.split_once(" (os error") {
        Some((detail, _)) => detail.into(),
        None => rendered,
    }
}

pub(super) fn network_server_error(error: &std::io::Error) -> LispError {
    LispError::SignalValue(Value::list([
        Value::symbol("file-error"),
        Value::string("Cannot bind server socket"),
        Value::string(&network_io_error_detail(error)),
    ]))
}

pub(super) fn network_client_error(error: &std::io::Error, args: &[Value]) -> LispError {
    let condition = if error.kind() == std::io::ErrorKind::NotFound {
        "file-missing"
    } else {
        "file-error"
    };
    let mut data = vec![
        Value::symbol(condition),
        Value::string("make client process failed"),
        Value::string(&network_io_error_detail(error)),
    ];
    data.extend_from_slice(args);
    LispError::SignalValue(Value::list(data))
}

fn activate_network_client(
    interp: &mut Interpreter,
    process: Value,
    nowait: bool,
    tls_parameters: &Value,
    env: &mut Env,
) -> Result<Value, LispError> {
    let process_id = interp.resolve_process_id(&process)?;
    if !tls_parameters.is_nil() {
        interp.set_process_gnutls_boot_parameters(process_id, tls_parameters.clone());
    }
    if nowait {
        interp.mark_network_process_connecting(process_id);
        return Ok(process);
    }
    if !tls_parameters.is_nil() {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            match progress_async_gnutls(interp, process_id)? {
                AsyncGnuTlsProgress::NotRequested | AsyncGnuTlsProgress::Ready => break,
                AsyncGnuTlsProgress::Pending if std::time::Instant::now() < deadline => {
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
                AsyncGnuTlsProgress::Pending => {
                    return Err(LispError::Signal("GnuTLS handshake timed out".into()));
                }
                AsyncGnuTlsProgress::Failed(error) => {
                    return Err(LispError::Signal(format!(
                        "GnuTLS negotiation failed: {error:?}"
                    )));
                }
            }
        }
    }
    run_process_sentinel(interp, process_id, "open\n", env)?;
    Ok(process)
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
    let mut datagram = false;
    let mut family_local = false;
    let mut family_ipv4 = false;
    let mut family_ipv6 = false;
    let mut host_local = false;
    let mut nowait = false;
    let mut tls_parameters = Value::Nil;
    let mut coding = Value::Nil;
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
            ":type" => match value {
                Value::Nil => datagram = false,
                Value::Symbol(kind) if kind == "datagram" => datagram = true,
                _ => return Err(LispError::Signal("Unsupported connection type".into())),
            },
            ":nowait" => nowait = value.is_truthy(),
            ":coding" => coding = value.clone(),
            ":tls-parameters" => {
                value
                    .to_vec()
                    .map_err(|_| wrong_type_argument("listp", value.clone()))?;
                tls_parameters = value.clone();
            }
            ":family" => {
                family_local = matches!(value, Value::Symbol(symbol) if symbol == "local");
                family_ipv4 = matches!(value, Value::Symbol(symbol) if symbol == "ipv4");
                family_ipv6 = matches!(value, Value::Symbol(symbol) if symbol == "ipv6");
            }
            ":host" => {
                host = match value {
                    Value::Nil => None,
                    Value::Symbol(symbol) if symbol == "local" => {
                        host_local = true;
                        None
                    }
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

    let inherit_coding_system = buffer_id.is_some()
        && interp
            .lookup_var("inherit-process-coding-system", env)
            .is_some_and(|value| value.is_truthy());
    if host_local {
        host = Some(if family_ipv6 { "::1" } else { "127.0.0.1" }.into());
    }
    let (decoding, encoding) = process_creation_coding_systems(interp, env, &coding);
    let name = interp.unique_process_name(&name);
    // GNU keeps the original keyword plist as the process's contact info
    // (p->childp), updating :service and appending the resolved :local /
    // :remote address vectors after the socket is set up.
    let mut contact_items: Vec<Value> = args.to_vec();

    if datagram {
        if family_local {
            return Err(LispError::Signal(
                "Datagram local sockets are not supported".into(),
            ));
        }
        let address_host = host
            .clone()
            .unwrap_or_else(|| if family_ipv6 { "::1" } else { "127.0.0.1" }.into());
        let address_port = service.unwrap_or(0) as u16;
        let address =
            std::net::ToSocketAddrs::to_socket_addrs(&(address_host.as_str(), address_port))
                .map_err(|error| {
                    if is_server {
                        network_server_error(&error)
                    } else {
                        network_client_error(&error, args)
                    }
                })?
                .find(|address| {
                    (!family_ipv4 || address.is_ipv4()) && (!family_ipv6 || address.is_ipv6())
                })
                .ok_or_else(|| {
                    LispError::Signal(format!(
                        "make-network-process: no matching address for {address_host}"
                    ))
                })?;
        let socket = if is_server {
            std::net::UdpSocket::bind(address)
        } else {
            let local = match address {
                std::net::SocketAddr::V4(_) => std::net::SocketAddr::from(([0, 0, 0, 0], 0)),
                std::net::SocketAddr::V6(_) => {
                    std::net::SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 0], 0))
                }
            };
            std::net::UdpSocket::bind(local)
        }
        .map_err(|error| {
            if is_server {
                network_server_error(&error)
            } else {
                network_client_error(&error, args)
            }
        })?;
        socket.set_nonblocking(true).map_err(|error| {
            if is_server {
                network_server_error(&error)
            } else {
                network_client_error(&error, args)
            }
        })?;
        let local_addr = socket.local_addr().ok();
        let remote = (!is_server).then_some(address);
        if let Some(local_addr) = local_addr {
            if is_server {
                plist_items_put(
                    &mut contact_items,
                    ":service",
                    Value::Integer(local_addr.port().into()),
                );
                plist_items_put(&mut contact_items, ":local", sockaddr_vector(local_addr));
            } else {
                let unspecified = match local_addr {
                    std::net::SocketAddr::V4(_) => std::net::SocketAddr::from(([0, 0, 0, 0], 0)),
                    std::net::SocketAddr::V6(_) => {
                        std::net::SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 0], 0))
                    }
                };
                plist_items_put(&mut contact_items, ":local", sockaddr_vector(unspecified));
            }
        }
        if let Some(remote) = remote {
            plist_items_put(&mut contact_items, ":remote", sockaddr_vector(remote));
        }
        let process = interp.create_network_process(
            &name,
            buffer_id,
            inherit_coding_system,
            filter,
            sentinel,
            log,
            plist,
            crate::lisp::eval::NetworkRuntime::Datagram { socket, remote },
            host,
            service,
            None,
            None,
            Value::list(contact_items),
            decoding,
            encoding,
        )?;
        if !is_server {
            let process_id = interp.resolve_process_id(&process)?;
            run_process_sentinel(interp, process_id, "open\n", env)?;
        }
        return Ok(process);
    }

    if family_local {
        // `:family local' — a unix domain socket named by :service.  GNU
        // conv_sockaddr_to_lisp yields the PATH string for :local/:remote
        // (a client's own end is unnamed, hence "").
        let path = service_path.ok_or_else(|| {
            LispError::Signal("make-network-process: local family needs a :service path".into())
        })?;
        let sun_path_capacity = std::mem::size_of::<libc::sockaddr_un>()
            - std::mem::offset_of!(libc::sockaddr_un, sun_path);
        if path.len() >= sun_path_capacity {
            return Err(LispError::Signal("Service name too long".into()));
        }
        if is_server {
            let listener = std::os::unix::net::UnixListener::bind(&path)
                .map_err(|error| network_server_error(&error))?;
            listener
                .set_nonblocking(true)
                .map_err(|error| network_server_error(&error))?;
            plist_items_put(
                &mut contact_items,
                ":local",
                Value::String(path.clone().into()),
            );
            return interp.create_network_process(
                &name,
                buffer_id,
                inherit_coding_system,
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
                decoding,
                encoding,
            );
        }
        let stream = std::os::unix::net::UnixStream::connect(&path)
            .map_err(|error| network_client_error(&error, args))?;
        stream
            .set_nonblocking(true)
            .map_err(|error| network_client_error(&error, args))?;
        plist_items_put(
            &mut contact_items,
            ":remote",
            Value::String(path.clone().into()),
        );
        plist_items_put(
            &mut contact_items,
            ":local",
            Value::String(String::new().into()),
        );
        let process = interp.create_network_process(
            &name,
            buffer_id,
            inherit_coding_system,
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
            decoding,
            encoding,
        )?;
        return activate_network_client(interp, process, nowait, &tls_parameters, env);
    }

    if is_server {
        let bind_host = host
            .clone()
            .unwrap_or_else(|| if family_ipv6 { "::1" } else { "127.0.0.1" }.into());
        let bind_port = service.unwrap_or(0) as u16;
        let listener = if family_ipv4 || family_ipv6 {
            let address =
                std::net::ToSocketAddrs::to_socket_addrs(&(bind_host.as_str(), bind_port))
                    .map_err(|error| network_server_error(&error))?
                    .find(|address| {
                        (!family_ipv4 || address.is_ipv4()) && (!family_ipv6 || address.is_ipv6())
                    })
                    .ok_or_else(|| {
                        LispError::Signal(format!(
                            "make-network-process: no requested-family address for {bind_host}"
                        ))
                    })?;
            std::net::TcpListener::bind(address)
        } else {
            std::net::TcpListener::bind((bind_host.as_str(), bind_port))
        }
        .map_err(|error| network_server_error(&error))?;
        listener
            .set_nonblocking(true)
            .map_err(|error| network_server_error(&error))?;
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
            inherit_coding_system,
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
            decoding,
            encoding,
        )
    } else {
        let connect_host = host
            .clone()
            .unwrap_or_else(|| if family_ipv6 { "::1" } else { "127.0.0.1" }.into());
        let connect_port = service.unwrap_or(0) as u16;
        let addresses =
            std::net::ToSocketAddrs::to_socket_addrs(&(connect_host.as_str(), connect_port))
                .map_err(|error| network_client_error(&error, args))?
                .filter(|address| {
                    (!family_ipv4 || address.is_ipv4()) && (!family_ipv6 || address.is_ipv6())
                })
                .collect::<Vec<_>>();
        if addresses.is_empty() {
            return Err(LispError::Signal(format!(
                "make-network-process: no requested-family address for {connect_host}"
            )));
        }
        // GNU iterates getaddrinfo results until one connection succeeds.
        // Passing the full filtered slice gives Rust's socket layer the same
        // fallback behavior (notably localhost resolving to IPv6 before an
        // IPv4-only listener) without duplicating connection error policy.
        let stream = std::net::TcpStream::connect(addresses.as_slice())
            .map_err(|error| network_client_error(&error, args))?;
        stream
            .set_nonblocking(true)
            .map_err(|error| network_client_error(&error, args))?;
        if let Ok(addr) = stream.peer_addr() {
            plist_items_put(&mut contact_items, ":remote", sockaddr_vector(addr));
        }
        if let Ok(addr) = stream.local_addr() {
            plist_items_put(&mut contact_items, ":local", sockaddr_vector(addr));
        }
        let process = interp.create_network_process(
            &name,
            buffer_id,
            inherit_coding_system,
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
            decoding,
            encoding,
        )?;
        // GNU runs the sentinel with "open\n" only after any requested TLS
        // negotiation has completed.
        activate_network_client(interp, process, nowait, &tls_parameters, env)
    }
}

fn plist_member_value(items: &[Value], key: &str) -> Option<Value> {
    items.chunks_exact(2).find_map(|pair| {
        pair[0]
            .as_symbol()
            .is_ok_and(|candidate| candidate == key)
            .then(|| pair[1].clone())
    })
}

fn serial_data_bits(value: &Value) -> Result<serialport::DataBits, LispError> {
    match value {
        Value::Nil | Value::Integer(8) => Ok(serialport::DataBits::Eight),
        Value::Integer(7) => Ok(serialport::DataBits::Seven),
        _ => Err(LispError::Signal(
            ":bytesize must be nil (8), 7, or 8".into(),
        )),
    }
}

fn serial_parity(value: &Value) -> Result<serialport::Parity, LispError> {
    match value {
        Value::Nil => Ok(serialport::Parity::None),
        Value::Symbol(symbol) if symbol == "even" => Ok(serialport::Parity::Even),
        Value::Symbol(symbol) if symbol == "odd" => Ok(serialport::Parity::Odd),
        _ => Err(LispError::Signal(
            ":parity must be nil (no parity), `even', or `odd'".into(),
        )),
    }
}

fn serial_stop_bits(value: &Value) -> Result<serialport::StopBits, LispError> {
    match value {
        Value::Nil | Value::Integer(1) => Ok(serialport::StopBits::One),
        Value::Integer(2) => Ok(serialport::StopBits::Two),
        _ => Err(LispError::Signal(
            ":stopbits must be nil (1 stopbit), 1, or 2".into(),
        )),
    }
}

fn serial_flow_control(value: &Value) -> Result<serialport::FlowControl, LispError> {
    match value {
        Value::Nil => Ok(serialport::FlowControl::None),
        Value::Symbol(symbol) if symbol == "hw" => Ok(serialport::FlowControl::Hardware),
        Value::Symbol(symbol) if symbol == "sw" => Ok(serialport::FlowControl::Software),
        _ => Err(LispError::Signal(
            ":flowcontrol must be nil (no flowcontrol), `hw', or `sw'".into(),
        )),
    }
}

struct SerialConfiguration {
    speed: u32,
    data_bits: serialport::DataBits,
    parity: serialport::Parity,
    stop_bits: serialport::StopBits,
    flow_control: serialport::FlowControl,
}

fn serial_configuration(
    updates: &[Value],
    stored_contact: &Value,
) -> Result<(SerialConfiguration, Value), LispError> {
    let mut contact_items = stored_contact.to_vec()?;
    let setting = |key: &str, default: Value| {
        let value = plist_member_value(updates, key)
            .unwrap_or_else(|| contact_plist_get(stored_contact, key));
        if value.is_nil() { default } else { value }
    };
    let speed_value = plist_member_value(updates, ":speed")
        .unwrap_or_else(|| contact_plist_get(stored_contact, ":speed"));
    let speed = u32::try_from(speed_value.as_integer()?)
        .map_err(|_| LispError::Signal("Unsupported speed".into()))?;
    let bytesize = setting(":bytesize", Value::Integer(8));
    let parity = plist_member_value(updates, ":parity")
        .unwrap_or_else(|| contact_plist_get(stored_contact, ":parity"));
    let stopbits = setting(":stopbits", Value::Integer(1));
    let flowcontrol = plist_member_value(updates, ":flowcontrol")
        .unwrap_or_else(|| contact_plist_get(stored_contact, ":flowcontrol"));
    let config = SerialConfiguration {
        speed,
        data_bits: serial_data_bits(&bytesize)?,
        parity: serial_parity(&parity)?,
        stop_bits: serial_stop_bits(&stopbits)?,
        flow_control: serial_flow_control(&flowcontrol)?,
    };
    plist_items_put(&mut contact_items, ":speed", Value::Integer(speed.into()));
    plist_items_put(&mut contact_items, ":bytesize", bytesize.clone());
    plist_items_put(&mut contact_items, ":parity", parity.clone());
    plist_items_put(&mut contact_items, ":stopbits", stopbits.clone());
    plist_items_put(&mut contact_items, ":flowcontrol", flowcontrol.clone());
    let parity_summary = match parity {
        Value::Symbol(symbol) if symbol == "even" => 'E',
        Value::Symbol(symbol) if symbol == "odd" => 'O',
        _ => 'N',
    };
    plist_items_put(
        &mut contact_items,
        ":summary",
        Value::String(
            format!(
                "{}{parity_summary}{}",
                bytesize.as_integer()?,
                stopbits.as_integer()?
            )
            .into(),
        ),
    );
    Ok((config, Value::list(contact_items)))
}

fn open_serial_port(path: &str) -> Result<serialport::TTYPort, LispError> {
    #[cfg(unix)]
    {
        use std::os::fd::IntoRawFd;
        use std::os::unix::fs::OpenOptionsExt;

        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_NOCTTY | libc::O_NONBLOCK)
            .open(path)
            .map_err(|error| LispError::Signal(error.to_string()))?;
        // SAFETY: ownership of the descriptor is transferred from File to
        // TTYPort exactly once.  This path intentionally does not touch
        // termios: GNU's documented :speed nil contract preserves it.
        let mut port = unsafe { serialport::TTYPort::from_raw_fd(file.into_raw_fd()) };
        serialport::SerialPort::set_timeout(&mut port, Duration::ZERO)
            .map_err(|error| LispError::Signal(error.to_string()))?;
        Ok(port)
    }

    #[cfg(not(unix))]
    Err(LispError::Signal(
        "Serial ports are unsupported on this platform".into(),
    ))
}

#[cfg(unix)]
fn serial_speed(speed: u32) -> libc::speed_t {
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    {
        speed as libc::speed_t
    }
    #[cfg(not(any(target_os = "macos", target_os = "ios")))]
    {
        match speed {
            0 => libc::B0,
            50 => libc::B50,
            75 => libc::B75,
            110 => libc::B110,
            134 => libc::B134,
            150 => libc::B150,
            200 => libc::B200,
            300 => libc::B300,
            600 => libc::B600,
            1200 => libc::B1200,
            1800 => libc::B1800,
            2400 => libc::B2400,
            4800 => libc::B4800,
            9600 => libc::B9600,
            19200 => libc::B19200,
            38400 => libc::B38400,
            57600 => libc::B57600,
            115200 => libc::B115200,
            230400 => libc::B230400,
            _ => speed as libc::speed_t,
        }
    }
}

#[cfg(unix)]
fn configure_serial_descriptor(
    fd: std::os::fd::RawFd,
    configuration: &SerialConfiguration,
) -> Result<(), LispError> {
    let mut attributes = std::mem::MaybeUninit::<libc::termios>::uninit();
    // SAFETY: FD belongs to the live serial runtime and tcgetattr initializes
    // the termios structure on success.
    if unsafe { libc::tcgetattr(fd, attributes.as_mut_ptr()) } != 0 {
        return Err(LispError::Signal(
            std::io::Error::last_os_error().to_string(),
        ));
    }
    // SAFETY: tcgetattr succeeded above.
    let mut attributes = unsafe { attributes.assume_init() };
    // SAFETY: attributes is initialized and exclusively borrowed.
    unsafe { libc::cfmakeraw(&mut attributes) };
    attributes.c_cflag |= libc::CLOCAL | libc::CREAD;
    // SAFETY: cfsetspeed only mutates the initialized termios structure.
    if unsafe { libc::cfsetspeed(&mut attributes, serial_speed(configuration.speed)) } != 0 {
        return Err(LispError::Signal(
            std::io::Error::last_os_error().to_string(),
        ));
    }
    attributes.c_cflag &= !libc::CSIZE;
    attributes.c_cflag |= match configuration.data_bits {
        serialport::DataBits::Seven => libc::CS7,
        serialport::DataBits::Eight => libc::CS8,
        _ => unreachable!("serial parser accepts only seven or eight data bits"),
    };
    attributes.c_cflag &= !(libc::PARENB | libc::PARODD);
    attributes.c_iflag &= !(libc::IGNPAR | libc::INPCK);
    match configuration.parity {
        serialport::Parity::None => {}
        serialport::Parity::Even => {
            attributes.c_cflag |= libc::PARENB;
            attributes.c_iflag |= libc::IGNPAR | libc::INPCK;
        }
        serialport::Parity::Odd => {
            attributes.c_cflag |= libc::PARENB | libc::PARODD;
            attributes.c_iflag |= libc::IGNPAR | libc::INPCK;
        }
    }
    attributes.c_cflag &= !libc::CSTOPB;
    if configuration.stop_bits == serialport::StopBits::Two {
        attributes.c_cflag |= libc::CSTOPB;
    }
    attributes.c_cflag &= !libc::CRTSCTS;
    attributes.c_iflag &= !(libc::IXON | libc::IXOFF);
    match configuration.flow_control {
        serialport::FlowControl::None => {}
        serialport::FlowControl::Hardware => attributes.c_cflag |= libc::CRTSCTS,
        serialport::FlowControl::Software => attributes.c_iflag |= libc::IXON | libc::IXOFF,
    }
    // SAFETY: FD remains live and tcsetattr reads the initialized structure.
    if unsafe { libc::tcsetattr(fd, libc::TCSANOW, &attributes) } != 0 {
        return Err(LispError::Signal(
            std::io::Error::last_os_error().to_string(),
        ));
    }
    Ok(())
}

fn serial_process_designator(interp: &mut Interpreter, args: &[Value]) -> Result<u64, LispError> {
    let requested = [":process", ":name", ":buffer", ":port"]
        .into_iter()
        .find_map(|key| plist_member_value(args, key).filter(Value::is_truthy));
    let process = match requested.as_ref() {
        None => interp.process_value_for_buffer(interp.current_buffer_id()),
        Some(process @ Value::Record(_)) => Some(process.clone()),
        Some(value) if string_like(value).is_some() => {
            let text = string_text(value)?;
            interp
                .find_process_id_by_name(&text)
                .map(Value::Record)
                .or_else(|| {
                    interp
                        .resolve_buffer_id(value)
                        .ok()
                        .and_then(|buffer_id| interp.process_value_for_buffer(buffer_id))
                })
        }
        Some(value) => interp
            .resolve_buffer_id(value)
            .ok()
            .and_then(|buffer_id| interp.process_value_for_buffer(buffer_id)),
    }
    .ok_or_else(|| wrong_type_argument("processp", requested.unwrap_or(Value::Nil)))?;
    let process_id = interp.resolve_process_id(&process)?;
    if !interp.is_serial_process(process_id) {
        return Err(LispError::Signal("Not a serial process".into()));
    }
    Ok(process_id)
}

pub(crate) fn serial_process_configure(
    interp: &mut Interpreter,
    args: &[Value],
) -> Result<Value, LispError> {
    let process_id = serial_process_designator(interp, args)?;
    let stored_contact = interp
        .process_contact_plist(process_id)
        .unwrap_or(Value::Nil);
    if contact_plist_get(&stored_contact, ":speed").is_nil() {
        return Ok(Value::Nil);
    }
    let (configuration, contact) = serial_configuration(args, &stored_contact)?;
    #[cfg(unix)]
    configure_serial_descriptor(interp.serial_process_fd(process_id)?, &configuration)?;
    interp.set_process_contact_plist(process_id, contact)?;
    Ok(Value::Nil)
}

pub(crate) fn make_serial_process(
    interp: &mut Interpreter,
    args: &[Value],
    env: &mut Env,
) -> Result<Value, LispError> {
    if args.is_empty() {
        return Ok(Value::Nil);
    }
    let port_value = plist_member_value(args, ":port")
        .filter(Value::is_truthy)
        .ok_or_else(|| LispError::Signal("No port specified".into()))?;
    let port_name = string_text(&port_value)?;
    let Some(speed_value) = plist_member_value(args, ":speed") else {
        return Err(LispError::Signal(":speed not specified".into()));
    };
    let speed = if speed_value.is_nil() {
        None
    } else {
        Some(
            u32::try_from(speed_value.as_integer()?)
                .map_err(|_| LispError::Signal("Unsupported speed".into()))?,
        )
    };
    let serial = open_serial_port(&port_name)?;
    let contact = Value::list(args.iter().cloned());
    let contact = if speed.is_some() {
        let (configuration, contact) = serial_configuration(args, &contact)?;
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            configure_serial_descriptor(serial.as_raw_fd(), &configuration)?;
        }
        contact
    } else {
        contact
    };
    let name = plist_member_value(args, ":name")
        .filter(Value::is_truthy)
        .map(|value| string_text(&value))
        .transpose()?
        .unwrap_or_else(|| port_name.clone());
    let buffer = plist_member_value(args, ":buffer")
        .filter(Value::is_truthy)
        .unwrap_or_else(|| Value::String(name.clone().into()));
    let buffer_id =
        process_buffer_target(interp, &buffer)?.expect("a serial process always has a buffer");
    let filter = plist_member_value(args, ":filter").filter(Value::is_truthy);
    let sentinel = plist_member_value(args, ":sentinel").filter(Value::is_truthy);
    let plist = plist_member_value(args, ":plist").unwrap_or(Value::Nil);
    let coding = plist_member_value(args, ":coding").unwrap_or(Value::Nil);
    let (decoding, encoding) = process_creation_coding_systems(interp, env, &coding);
    let noquery = plist_member_value(args, ":noquery").is_some_and(|value| value.is_truthy());
    let stopped = plist_member_value(args, ":stop").is_some_and(|value| value.is_truthy());
    let process = interp.create_serial_process(
        &name,
        buffer_id,
        filter,
        sentinel,
        plist,
        crate::lisp::eval::SerialRuntime { port: serial },
        contact,
        decoding,
        encoding,
        !noquery,
        stopped,
    )?;
    let process_id = interp.resolve_process_id(&process)?;
    let inherit = coding.is_nil()
        && interp
            .lookup_var("inherit-process-coding-system", env)
            .is_some_and(|value| value.is_truthy());
    interp.set_process_inherit_coding_system_flag(process_id, inherit)?;
    Ok(process)
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
            &[
                Value::Record(process_id),
                Value::String(event.to_string().into()),
            ],
            env,
        )?;
    } else {
        internal_default_process_sentinel(interp, process_id, event)?;
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
                Value::String(message.to_string().into()),
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
    total: Option<std::time::Duration>,
    return_on_delivery: bool,
    target_process_id: Option<u64>,
) -> Result<bool, LispError> {
    let deadline = total.map(|total| std::time::Instant::now() + total);
    let mut delivered = false;
    let target_start =
        target_process_id.and_then(|process_id| interp.process_output_delivery_count(process_id));
    loop {
        let mut progressed = pump_external_process_output(interp, env)?;
        progressed |= pump_connection_processes(interp, env)?;
        delivered |= progressed;
        interp.drive_threads(env, true)?;
        let requested_process_delivered = target_process_id.is_some_and(|process_id| {
            interp.process_output_delivery_count(process_id) != target_start
        });
        if return_on_delivery
            && if target_process_id.is_some() {
                requested_process_delivered
            } else {
                delivered
            }
        {
            break;
        }
        let now = std::time::Instant::now();
        if deadline.is_some_and(|deadline| now >= deadline) {
            // Timer/thread callbacks above can consume the remainder of the
            // deadline while the requested child becomes readable.  Drain
            // readiness once more before reporting a timeout; otherwise the
            // bytes are left for the next caller, producing a deterministic
            // one-wait lag under load.
            let mut final_progress = pump_external_process_output(interp, env)?;
            final_progress |= pump_connection_processes(interp, env)?;
            delivered |= final_progress;
            break;
        }
        // With no explicit timeout GNU waits for PROCESS, but returns once a
        // subprocess has exited even if it produced no bytes.  Pending bytes
        // keep the id live until the pump above has delivered them.
        if deadline.is_none()
            && target_process_id.is_some_and(|process_id| {
                !interp.live_external_process_ids().contains(&process_id)
                    && !matches!(
                        interp.process_status_value(process_id),
                        Some(Value::Symbol(status))
                            if matches!(status.as_str(), "run" | "open" | "connect" | "listen")
                    )
            })
        {
            break;
        }
        if progressed {
            continue;
        }
        // Idle: nap until the deadline or the next due timer, polling at
        // most every 10ms so freshly arriving input is picked up promptly.
        let mut nap = deadline
            .map(|deadline| deadline.saturating_duration_since(now))
            .unwrap_or(std::time::Duration::from_millis(10));
        if let Some(due) = interp.next_timer_due() {
            nap = nap.min(due.saturating_duration_since(now));
        }
        nap = nap
            .min(std::time::Duration::from_millis(10))
            .max(std::time::Duration::from_millis(1));
        std::thread::sleep(nap);
    }
    let result = if let Some(process_id) = target_process_id {
        interp.process_output_delivery_count(process_id) != target_start
    } else {
        delivered
    };
    Ok(result)
}

/// Accept pending server connections and deliver stream input/closure to
/// filters and sentinels.  Returns true if anything was processed.
pub(crate) fn pump_connection_processes(
    interp: &mut Interpreter,
    env: &mut Env,
) -> Result<bool, LispError> {
    let mut progressed = false;

    // `:nowait t' exposes a freshly created client as `connect' until the
    // event loop reports completion.  The OS connection is already usable
    // in this compatibility runtime, but deferring the `open' transition
    // lets callers install their sentinel before it runs, like GNU Emacs.
    for process_id in interp.connecting_network_processes() {
        progressed = true;
        match progress_async_gnutls(interp, process_id)? {
            AsyncGnuTlsProgress::NotRequested | AsyncGnuTlsProgress::Ready => {
                interp.mark_network_process_open(process_id);
                run_process_sentinel(interp, process_id, "open\n", env)?;
            }
            AsyncGnuTlsProgress::Pending => {}
            AsyncGnuTlsProgress::Failed(error) => {
                interp.mark_network_process_failed(process_id);
                run_process_sentinel(
                    interp,
                    process_id,
                    &format!("TLS negotiation failed: {error:?}\n"),
                    env,
                )?;
            }
        }
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
                        Value::String(peer_addr.ip().to_string().into()),
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
                    plist_items_put(
                        &mut contact_items,
                        ":remote",
                        Value::String(String::new().into()),
                    );
                    String::new()
                }
            };
            let server_filter = interp.process_filter(server_id);
            let server_sentinel = interp.process_sentinel(server_id);
            let server_log = interp.process_log_function(server_id);
            let server_buffer = interp.process_buffer_id(server_id);
            let (child_decoding, child_encoding) = interp
                .process_coding_system(server_id)?
                .cons_values()
                .unwrap_or((Value::Nil, Value::Nil));
            let child_inherit_coding_system = server_buffer.is_some()
                && interp
                    .process_inherit_coding_system_flag(server_id)
                    .unwrap_or(false);
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
                child_inherit_coding_system,
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
                child_decoding,
                child_encoding,
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

    // Deliver input / closure on every open network or serial stream.
    for stream_id in interp.connection_stream_ids() {
        if interp.process_output_paused(stream_id) {
            continue;
        }
        let (bytes, closed) = interp.poll_connection_stream(stream_id)?;
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
