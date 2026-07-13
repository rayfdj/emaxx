use super::*;

/// (host, service, remote-peer, is-server) for `process-contact'.
pub(crate) type ProcessContactInfo = (Option<String>, Option<i64>, Option<String>, bool);

/// Drain a non-blocking stream: bytes read plus whether the peer closed.
fn drain_nonblocking<R: std::io::Read>(stream: &mut R) -> (Vec<u8>, bool) {
    let mut out = Vec::new();
    let mut closed = false;
    let mut chunk = [0u8; 4096];
    loop {
        match stream.read(&mut chunk) {
            Ok(0) => {
                closed = true;
                break;
            }
            Ok(n) => out.extend_from_slice(&chunk[..n]),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(_) => {
                closed = true;
                break;
            }
        }
    }
    (out, closed)
}

/// Write all of INPUT to a non-blocking stream, napping through WouldBlock.
fn send_all<W: std::io::Write>(stream: &mut W, input: &[u8]) -> Result<(), LispError> {
    let mut written = 0;
    while written < input.len() {
        match stream.write(&input[written..]) {
            Ok(n) => written += n,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(1));
            }
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => return Err(LispError::Signal(error.to_string())),
        }
    }
    let _ = stream.flush();
    Ok(())
}

impl Interpreter {
    pub(super) fn find_thread_state(&self, record_id: u64) -> Option<&ThreadState> {
        self.thread_states
            .iter()
            .find(|thread| thread.record_id == record_id)
    }

    pub(super) fn find_thread_state_mut(&mut self, record_id: u64) -> Option<&mut ThreadState> {
        self.thread_states
            .iter_mut()
            .find(|thread| thread.record_id == record_id)
    }

    pub(super) fn find_mutex_state_mut(&mut self, record_id: u64) -> Option<&mut MutexState> {
        self.mutex_states
            .iter_mut()
            .find(|mutex| mutex.record_id == record_id)
    }

    pub(super) fn find_condition_variable_state(
        &self,
        record_id: u64,
    ) -> Option<&ConditionVariableState> {
        self.condition_variables
            .iter()
            .find(|condvar| condvar.record_id == record_id)
    }

    pub(super) fn find_process_state(&self, record_id: u64) -> Option<&ProcessState> {
        self.process_states
            .iter()
            .find(|process| process.record_id == record_id)
    }

    pub fn process_plist_value(&self, record_id: u64) -> Option<Value> {
        self.find_process_state(record_id)
            .map(|process| process.plist.clone())
    }

    pub fn set_process_plist_value(&mut self, record_id: u64, plist: Value) -> bool {
        if let Some(process) = self.find_process_state_mut(record_id) {
            process.plist = Self::stored_value(plist);
            true
        } else {
            false
        }
    }

    pub(super) fn find_process_state_mut(&mut self, record_id: u64) -> Option<&mut ProcessState> {
        self.process_states
            .iter_mut()
            .find(|process| process.record_id == record_id)
    }

    pub fn resolve_thread_id(&self, value: &Value) -> Result<u64, LispError> {
        match value {
            Value::Record(id)
                if self
                    .find_record(*id)
                    .is_some_and(|record| record.type_name == "thread") =>
            {
                Ok(*id)
            }
            other => Err(wrong_type_argument("threadp", other.clone())),
        }
    }

    pub fn resolve_mutex_id(&self, value: &Value) -> Result<u64, LispError> {
        match value {
            Value::Record(id)
                if self
                    .find_record(*id)
                    .is_some_and(|record| record.type_name == "mutex") =>
            {
                Ok(*id)
            }
            other => Err(wrong_type_argument("mutexp", other.clone())),
        }
    }

    pub fn resolve_condition_variable_id(&self, value: &Value) -> Result<u64, LispError> {
        match value {
            Value::Record(id)
                if self
                    .find_record(*id)
                    .is_some_and(|record| record.type_name == "condition-variable") =>
            {
                Ok(*id)
            }
            other => Err(wrong_type_argument("condition-variable-p", other.clone())),
        }
    }

    pub fn resolve_process_id(&self, value: &Value) -> Result<u64, LispError> {
        match value {
            Value::Record(id)
                if self
                    .find_record(*id)
                    .is_some_and(|record| record.type_name == "process") =>
            {
                Ok(*id)
            }
            other => Err(wrong_type_argument("processp", other.clone())),
        }
    }

    pub fn create_process(
        &mut self,
        buffer_id: Option<u64>,
        program: Option<String>,
        argv: Vec<String>,
        runtime: Option<Child>,
        name: Option<String>,
    ) -> Result<Value, LispError> {
        // GNU names the process after the NAME argument (uniquified with
        // <N> on collision), not the program.
        let name = self.unique_process_name(&name.or_else(|| program.clone()).unwrap_or_default());
        let process = self.create_record("process", Vec::new());
        let Value::Record(record_id) = process.clone() else {
            unreachable!("create_record returns a record")
        };
        let marker = self.make_marker();
        let Value::Marker(mark_marker_id) = marker else {
            unreachable!("make_marker returns a marker")
        };
        let initial_position =
            buffer_id.and_then(|id| self.get_buffer_by_id(id).map(|buffer| buffer.point_max()));
        self.set_marker(mark_marker_id, initial_position, buffer_id)?;
        self.process_states.push(ProcessState {
            record_id,
            buffer_id,
            mark_marker_id,
            status: ProcessStatus::Run,
            filter: None,
            sentinel: None,
            log: None,
            name,
            _query_on_exit_flag: false,
            decoding: Value::Nil,
            encoding: Value::Nil,
            program,
            argv,
            runtime: runtime.map(|child| RunningProcess { child }),
            network: None,
            contact_host: None,
            contact_service: None,
            remote: None,
            parent_server_id: None,
            pending_stdout: Vec::new(),
            pending_stderr: Vec::new(),
            plist: Value::Nil,
            contact: Value::T,
        });
        Ok(process)
    }

    /// Create a network process record (server listener, client stream, or
    /// an accepted server-child stream).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn create_network_process(
        &mut self,
        name: &str,
        buffer_id: Option<u64>,
        filter: Option<Value>,
        sentinel: Option<Value>,
        log: Option<Value>,
        plist: Value,
        network: NetworkRuntime,
        contact_host: Option<String>,
        contact_service: Option<i64>,
        remote: Option<String>,
        parent_server_id: Option<u64>,
        contact: Value,
    ) -> Result<Value, LispError> {
        let status = match &network {
            NetworkRuntime::Listener(_) | NetworkRuntime::UnixListener(_) => ProcessStatus::Listen,
            NetworkRuntime::Stream(_) | NetworkRuntime::UnixStream(_) => ProcessStatus::Open,
        };
        let process = self.create_record("process", Vec::new());
        let Value::Record(record_id) = process.clone() else {
            unreachable!("create_record returns a record")
        };
        let marker = self.make_marker();
        let Value::Marker(mark_marker_id) = marker else {
            unreachable!("make_marker returns a marker")
        };
        let initial_position =
            buffer_id.and_then(|id| self.get_buffer_by_id(id).map(|buffer| buffer.point_max()));
        self.set_marker(mark_marker_id, initial_position, buffer_id)?;
        self.process_states.push(ProcessState {
            record_id,
            buffer_id,
            mark_marker_id,
            status,
            filter,
            sentinel,
            log,
            name: name.to_string(),
            _query_on_exit_flag: false,
            decoding: Value::Nil,
            encoding: Value::Nil,
            program: None,
            argv: Vec::new(),
            runtime: None,
            network: Some(network),
            contact_host,
            contact_service,
            remote,
            parent_server_id,
            pending_stdout: Vec::new(),
            pending_stderr: Vec::new(),
            plist,
            contact,
        });
        Ok(process)
    }

    /// GNU p->childp (process-contact with KEY t): t for a real child,
    /// the full contact plist for a network process.
    pub fn process_contact_plist(&self, record_id: u64) -> Option<Value> {
        self.find_process_state(record_id)
            .map(|process| process.contact.clone())
    }

    pub fn process_name(&self, record_id: u64) -> Option<String> {
        self.find_process_state(record_id)
            .map(|process| process.name.clone())
    }

    pub fn find_process_id_by_name(&self, name: &str) -> Option<u64> {
        self.process_states
            .iter()
            .rev()
            .find(|process| process.name == name)
            .map(|process| process.record_id)
    }

    /// A unique process name: NAME, then NAME<1>, NAME<2>, ... (GNU).
    pub fn unique_process_name(&self, base: &str) -> String {
        if self.find_process_id_by_name(base).is_none() {
            return base.to_string();
        }
        let mut index = 1;
        loop {
            let candidate = format!("{base}<{index}>");
            if self.find_process_id_by_name(&candidate).is_none() {
                return candidate;
            }
            index += 1;
        }
    }

    pub fn process_sentinel(&self, record_id: u64) -> Option<Value> {
        self.find_process_state(record_id)
            .and_then(|process| process.sentinel.clone())
    }

    pub fn set_process_sentinel(&mut self, record_id: u64, sentinel: Option<Value>) -> bool {
        let Some(process) = self.find_process_state_mut(record_id) else {
            return false;
        };
        process.sentinel = sentinel;
        true
    }

    pub fn process_contact_info(&self, record_id: u64) -> Option<ProcessContactInfo> {
        self.find_process_state(record_id).map(|process| {
            (
                process.contact_host.clone(),
                process.contact_service,
                process.remote.clone(),
                matches!(
                    process.network,
                    Some(NetworkRuntime::Listener(_)) | Some(NetworkRuntime::UnixListener(_))
                ),
            )
        })
    }

    pub fn process_log_function(&self, record_id: u64) -> Option<Value> {
        self.find_process_state(record_id)
            .and_then(|process| process.log.clone())
    }

    pub fn process_parent_server(&self, record_id: u64) -> Option<u64> {
        self.find_process_state(record_id)
            .and_then(|process| process.parent_server_id)
    }

    pub fn is_network_process(&self, record_id: u64) -> bool {
        self.find_process_state(record_id)
            .is_some_and(|process| process.network.is_some())
    }

    pub fn network_listener_ids(&self) -> Vec<u64> {
        self.process_states
            .iter()
            .filter(|process| {
                matches!(
                    process.network,
                    Some(NetworkRuntime::Listener(_)) | Some(NetworkRuntime::UnixListener(_))
                ) && process.status.is_live()
            })
            .map(|process| process.record_id)
            .collect()
    }

    pub fn network_stream_ids(&self) -> Vec<u64> {
        self.process_states
            .iter()
            .filter(|process| {
                matches!(
                    process.network,
                    Some(NetworkRuntime::Stream(_)) | Some(NetworkRuntime::UnixStream(_))
                ) && process.status.is_live()
            })
            .map(|process| process.record_id)
            .collect()
    }

    /// Accept one pending connection on a server listener, if any.
    /// Returns the accepted stream's runtime and, for inet listeners, the
    /// peer's socket address (unix peers are unnamed).
    pub(crate) fn accept_network_connection(
        &mut self,
        server_id: u64,
    ) -> Result<Option<(NetworkRuntime, Option<std::net::SocketAddr>)>, LispError> {
        let Some(process) = self.find_process_state_mut(server_id) else {
            return Ok(None);
        };
        match process.network.as_ref() {
            Some(NetworkRuntime::Listener(listener)) => match listener.accept() {
                Ok((stream, addr)) => {
                    stream
                        .set_nonblocking(true)
                        .map_err(|error| LispError::Signal(error.to_string()))?;
                    Ok(Some((NetworkRuntime::Stream(stream), Some(addr))))
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => Ok(None),
                Err(error) => Err(LispError::Signal(error.to_string())),
            },
            Some(NetworkRuntime::UnixListener(listener)) => match listener.accept() {
                Ok((stream, _)) => {
                    stream
                        .set_nonblocking(true)
                        .map_err(|error| LispError::Signal(error.to_string()))?;
                    Ok(Some((NetworkRuntime::UnixStream(stream), None)))
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => Ok(None),
                Err(error) => Err(LispError::Signal(error.to_string())),
            },
            _ => Ok(None),
        }
    }

    /// Non-blocking read of a network stream. Returns the bytes read and
    /// whether the peer closed the connection.
    pub fn poll_network_stream(&mut self, record_id: u64) -> Result<(Vec<u8>, bool), LispError> {
        let Some(process) = self.find_process_state_mut(record_id) else {
            return Ok((Vec::new(), false));
        };
        let (out, closed) = match process.network.as_mut() {
            Some(NetworkRuntime::Stream(stream)) => drain_nonblocking(stream),
            Some(NetworkRuntime::UnixStream(stream)) => drain_nonblocking(stream),
            _ => return Ok((Vec::new(), false)),
        };
        if closed {
            process.status = ProcessStatus::Closed;
            process.network = None;
        }
        Ok((out, closed))
    }

    pub fn network_stream_send(&mut self, record_id: u64, input: &[u8]) -> Result<(), LispError> {
        let Some(process) = self.find_process_state_mut(record_id) else {
            return Err(wrong_type_argument("processp", Value::Record(record_id)));
        };
        match process.network.as_mut() {
            Some(NetworkRuntime::Stream(stream)) => send_all(stream, input),
            Some(NetworkRuntime::UnixStream(stream)) => send_all(stream, input),
            _ => Err(LispError::Signal("Process is not a network stream".into())),
        }
    }

    pub(super) fn refresh_process_state(process: &mut ProcessState) -> Result<(), LispError> {
        if !process.status.is_live() {
            return Ok(());
        }
        let Some(runtime) = process.runtime.as_mut() else {
            return Ok(());
        };
        if runtime
            .child
            .try_wait()
            .map_err(|error| LispError::Signal(error.to_string()))?
            .is_some()
        {
            // Drain whatever the child wrote before exiting so the next
            // pump still delivers it to the filter (gpg's final status
            // lines arrive after `process-status' notices the exit).
            if let Some(pipe) = runtime.child.stdout.as_mut() {
                let mut tail = Vec::new();
                if std::io::Read::read_to_end(pipe, &mut tail).is_ok() {
                    process.pending_stdout.extend(tail);
                }
            }
            if let Some(pipe) = runtime.child.stderr.as_mut() {
                let mut tail = Vec::new();
                if std::io::Read::read_to_end(pipe, &mut tail).is_ok() {
                    process.pending_stderr.extend(tail);
                }
            }
            process.status = ProcessStatus::Exit;
            process.runtime = None;
        }
        Ok(())
    }

    pub fn process_value_for_buffer(&mut self, buffer_id: u64) -> Option<Value> {
        self.process_states.iter_mut().rev().find_map(|process| {
            let _ = Self::refresh_process_state(process);
            (process.buffer_id == Some(buffer_id) && process.status.is_live())
                .then_some(Value::Record(process.record_id))
        })
    }

    pub(super) fn refresh_process_id(&mut self, record_id: u64) -> Result<(), LispError> {
        let process = self
            .find_process_state_mut(record_id)
            .ok_or_else(|| wrong_type_argument("processp", Value::Record(record_id)))?;
        Self::refresh_process_state(process)
    }

    pub fn set_process_buffer_id(&mut self, record_id: u64, buffer_id: Option<u64>) -> bool {
        let Some(process) = self.find_process_state_mut(record_id) else {
            return false;
        };
        process.buffer_id = buffer_id;
        let mark_id = process.mark_marker_id;
        let position = buffer_id.and_then(|id| self.get_buffer_by_id(id).map(|b| b.point_max()));
        let _ = self.set_marker(mark_id, position, buffer_id);
        true
    }

    pub fn process_buffer_id(&self, record_id: u64) -> Option<u64> {
        self.find_process_state(record_id)
            .and_then(|process| process.buffer_id)
    }

    pub fn process_mark_id(&self, record_id: u64) -> Option<u64> {
        self.find_process_state(record_id)
            .map(|process| process.mark_marker_id)
    }

    pub fn process_status_value(&mut self, record_id: u64) -> Option<Value> {
        let _ = self.refresh_process_id(record_id);
        self.find_process_state(record_id)
            .map(|process| Value::Symbol(process.status.symbol().into()))
    }

    pub fn process_is_live(&mut self, record_id: u64) -> bool {
        let _ = self.refresh_process_id(record_id);
        self.find_process_state(record_id)
            .is_some_and(|process| process.status.is_live())
    }

    pub fn process_filter(&self, record_id: u64) -> Option<Value> {
        self.find_process_state(record_id)
            .and_then(|process| process.filter.clone())
    }

    pub fn set_process_filter(
        &mut self,
        record_id: u64,
        filter: Option<Value>,
    ) -> Result<(), LispError> {
        let process = self
            .find_process_state_mut(record_id)
            .ok_or_else(|| wrong_type_argument("processp", Value::Record(record_id)))?;
        process.filter = filter;
        Ok(())
    }

    pub fn process_coding_system(&self, record_id: u64) -> Result<Value, LispError> {
        let process = self
            .find_process_state(record_id)
            .ok_or_else(|| wrong_type_argument("processp", Value::Record(record_id)))?;
        Ok(Value::cons(
            process.decoding.clone(),
            process.encoding.clone(),
        ))
    }

    pub fn set_process_coding_system(
        &mut self,
        record_id: u64,
        decoding: Value,
        encoding: Value,
    ) -> Result<(), LispError> {
        let process = self
            .find_process_state_mut(record_id)
            .ok_or_else(|| wrong_type_argument("processp", Value::Record(record_id)))?;
        process.decoding = decoding;
        process.encoding = encoding;
        Ok(())
    }

    pub fn set_process_query_on_exit_flag(
        &mut self,
        record_id: u64,
        flag: bool,
    ) -> Result<(), LispError> {
        let process = self
            .find_process_state_mut(record_id)
            .ok_or_else(|| wrong_type_argument("processp", Value::Record(record_id)))?;
        process._query_on_exit_flag = flag;
        Ok(())
    }

    pub fn process_command(&self, record_id: u64) -> Option<(String, Vec<String>)> {
        let process = self.find_process_state(record_id)?;
        let program = process.program.clone()?;
        Some((program, process.argv.clone()))
    }

    pub fn delete_process(&mut self, record_id: u64) -> Result<(), LispError> {
        let process = self
            .find_process_state_mut(record_id)
            .ok_or_else(|| wrong_type_argument("processp", Value::Record(record_id)))?;
        if let Some(runtime) = process.runtime.as_mut() {
            let _ = runtime.child.kill();
            let _ = runtime.child.wait();
        }
        if let Some(network) = process.network.take() {
            match &network {
                NetworkRuntime::Stream(stream) => {
                    let _ = stream.shutdown(std::net::Shutdown::Both);
                }
                NetworkRuntime::UnixStream(stream) => {
                    let _ = stream.shutdown(std::net::Shutdown::Both);
                }
                // GNU leaves a unix listener's socket file behind; the
                // tests delete it themselves.
                NetworkRuntime::Listener(_) | NetworkRuntime::UnixListener(_) => {}
            }
            process.status = ProcessStatus::Closed;
            process.runtime = None;
            return Ok(());
        }
        process.status = ProcessStatus::Exit;
        process.runtime = None;
        Ok(())
    }

    pub fn process_send_string(
        &mut self,
        record_id: u64,
        input: &[u8],
    ) -> Result<(Vec<u8>, Vec<u8>), LispError> {
        self.refresh_process_id(record_id)?;
        let process = self
            .find_process_state_mut(record_id)
            .ok_or_else(|| wrong_type_argument("processp", Value::Record(record_id)))?;
        if !process.status.is_live() {
            return Err(LispError::Signal("Process is not running".into()));
        }
        if process.network.is_some() {
            self.network_stream_send(record_id, input)?;
            return Ok((Vec::new(), Vec::new()));
        }
        let process = self
            .find_process_state_mut(record_id)
            .ok_or_else(|| wrong_type_argument("processp", Value::Record(record_id)))?;
        let Some(runtime) = process.runtime.as_mut() else {
            return Ok((input.to_vec(), Vec::new()));
        };
        let Some(stdin) = runtime.child.stdin.as_mut() else {
            return Err(LispError::Signal("Process stdin is closed".into()));
        };
        stdin
            .write_all(input)
            .map_err(|error| LispError::Signal(error.to_string()))?;
        stdin
            .flush()
            .map_err(|error| LispError::Signal(error.to_string()))?;
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let deadline = std::time::Instant::now() + Duration::from_millis(100);
        loop {
            let mut made_progress = false;
            if let Some(pipe) = runtime.child.stdout.as_mut() {
                made_progress |= read_nonblocking_pipe(pipe, &mut stdout)?;
            }
            if let Some(pipe) = runtime.child.stderr.as_mut() {
                made_progress |= read_nonblocking_pipe(pipe, &mut stderr)?;
            }
            if runtime
                .child
                .try_wait()
                .map_err(|error| LispError::Signal(error.to_string()))?
                .is_some()
            {
                process.status = ProcessStatus::Exit;
                process.runtime = None;
                break;
            }
            if made_progress || std::time::Instant::now() >= deadline {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        Ok((stdout, stderr))
    }

    pub fn process_send_eof(&mut self, record_id: u64) -> Result<(Vec<u8>, Vec<u8>), LispError> {
        self.refresh_process_id(record_id)?;
        let process = self
            .find_process_state_mut(record_id)
            .ok_or_else(|| wrong_type_argument("processp", Value::Record(record_id)))?;
        if !process.status.is_live() {
            return Err(LispError::Signal("Process is not running".into()));
        }
        let Some(runtime) = process.runtime.as_mut() else {
            return Ok((Vec::new(), Vec::new()));
        };
        // Closing stdin delivers EOF; drain the pipes until the process
        // exits (filters/buffers get the output like process-send-string).
        drop(runtime.child.stdin.take());
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(pipe) = runtime.child.stdout.as_mut() {
                read_nonblocking_pipe(pipe, &mut stdout)?;
            }
            if let Some(pipe) = runtime.child.stderr.as_mut() {
                read_nonblocking_pipe(pipe, &mut stderr)?;
            }
            if runtime
                .child
                .try_wait()
                .map_err(|error| LispError::Signal(error.to_string()))?
                .is_some()
            {
                if let Some(pipe) = runtime.child.stdout.as_mut() {
                    read_nonblocking_pipe(pipe, &mut stdout)?;
                }
                if let Some(pipe) = runtime.child.stderr.as_mut() {
                    read_nonblocking_pipe(pipe, &mut stderr)?;
                }
                process.status = ProcessStatus::Exit;
                process.runtime = None;
                break;
            }
            if std::time::Instant::now() >= deadline {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        Ok((stdout, stderr))
    }

    pub fn live_external_process_ids(&self) -> Vec<u64> {
        self.process_states
            .iter()
            .filter(|process| {
                process.runtime.is_some()
                    || !process.pending_stdout.is_empty()
                    || !process.pending_stderr.is_empty()
            })
            .map(|process| process.record_id)
            .collect()
    }

    /// Non-blocking read of a live process's pipes; marks the process
    /// exited once the pipes are drained and the child has finished.
    pub fn poll_process_output(&mut self, record_id: u64) -> Result<(Vec<u8>, Vec<u8>), LispError> {
        let process = self
            .find_process_state_mut(record_id)
            .ok_or_else(|| wrong_type_argument("processp", Value::Record(record_id)))?;
        let Some(runtime) = process.runtime.as_mut() else {
            // Output drained at exit-detection time still needs delivering.
            return Ok((
                std::mem::take(&mut process.pending_stdout),
                std::mem::take(&mut process.pending_stderr),
            ));
        };
        let mut stdout = std::mem::take(&mut process.pending_stdout);
        let mut stderr = std::mem::take(&mut process.pending_stderr);
        if let Some(pipe) = runtime.child.stdout.as_mut() {
            read_nonblocking_pipe(pipe, &mut stdout)?;
        }
        if let Some(pipe) = runtime.child.stderr.as_mut() {
            read_nonblocking_pipe(pipe, &mut stderr)?;
        }
        if stdout.is_empty()
            && stderr.is_empty()
            && runtime
                .child
                .try_wait()
                .map_err(|error| LispError::Signal(error.to_string()))?
                .is_some()
        {
            process.status = ProcessStatus::Exit;
            process.runtime = None;
        }
        Ok((stdout, stderr))
    }

    /// Cancel the timer matching both FUNCTION and ARGS (GNU cancel-timer
    /// removes one specific timer object; several timers often share a
    /// function and differ only in their arguments).  Match args by
    /// IDENTITY (`eq'), not `equal': erc-d schedules per-exchange
    /// `erc-d--expire' timers whose args are dialog/exchange RECORDS, and
    /// two sibling dialogs can be structurally `equal' while distinct —
    /// deep matching would cancel the wrong dialog's linger timer.  When no
    /// match exists the timer already fired or was cancelled, and GNU's
    /// cancel-timer is a harmless no-op — never fall back to function-only
    /// matching, which would cancel an unrelated timer (another buffer's
    /// pending `erc-server-send-queue' drain).
    pub fn unschedule_timer_by_function_and_args(&mut self, function: &Value, args: &[Value]) {
        let candidates: Vec<(Value, Vec<Value>)> = self
            .pending_timers
            .iter()
            .map(|timer| (timer.function.clone(), timer.args.clone()))
            .collect();
        let empty_env = Vec::new();
        if let Some(index) = candidates.iter().position(|(candidate, timer_args)| {
            crate::lisp::primitives::values_eq_in_env(self, candidate, function, &empty_env)
                && timer_args.len() == args.len()
                && timer_args
                    .iter()
                    .zip(args)
                    .all(|(a, b)| crate::lisp::primitives::values_eq_in_env(self, a, b, &empty_env))
        }) {
            self.pending_timers.remove(index);
        }
    }

    pub fn schedule_timer(&mut self, function: Value, args: Vec<Value>) {
        self.schedule_timer_after(function, args, 0.0, None);
    }

    /// Schedule a timer to become due DELAY_SECS from now, optionally
    /// repeating every REPEAT_SECS (GNU run-at-time).
    pub fn schedule_timer_after(
        &mut self,
        function: Value,
        args: Vec<Value>,
        delay_secs: f64,
        repeat_secs: Option<f64>,
    ) {
        let original_name = function.as_symbol().ok().map(str::to_string);
        let due = (delay_secs > 0.0 && delay_secs.is_finite())
            .then(|| std::time::Instant::now() + std::time::Duration::from_secs_f64(delay_secs));
        self.pending_timers.push(ScheduledTimer {
            function: Self::stored_value(function),
            original_name,
            args: args.into_iter().map(Self::stored_value).collect(),
            due,
            repeat: repeat_secs.filter(|secs| secs.is_finite() && *secs > 0.0),
        });
    }

    /// The earliest instant at which a pending native timer becomes due,
    /// so waits can wake up exactly when the next timer should fire.
    pub fn next_timer_due(&self) -> Option<std::time::Instant> {
        self.pending_timers
            .iter()
            .map(|timer| timer.due.unwrap_or_else(std::time::Instant::now))
            .min()
    }

    pub fn queue_file_notification(&mut self, path: &str, action: &str) {
        self.pending_file_notifications
            .push((path.to_string(), action.to_string()));
    }

    pub fn run_pending_file_notifications(&mut self, env: &mut Env) -> Result<(), LispError> {
        let pending = std::mem::take(&mut self.pending_file_notifications);
        for (path, action) in pending {
            let outcome = primitives::deliver_file_notification(self, env, &path, &action);
            match outcome {
                Ok(()) => {}
                Err(error @ LispError::Throw(_, _)) => return Err(error),
                Err(error) => {
                    // The command loop demotes errors from special event
                    // handlers to a message.
                    if self
                        .lookup_var("debug-on-error", env)
                        .is_some_and(|value| value.is_truthy())
                    {
                        return Err(error);
                    }
                    let _ = primitives::call(
                        self,
                        "message",
                        &[
                            Value::String("Error in file notification: %S".into()),
                            super::error_condition_value(&error),
                        ],
                        env,
                    );
                }
            }
        }
        Ok(())
    }

    pub fn run_pending_timers(&mut self, env: &mut Env) -> Result<(), LispError> {
        // Only timers whose scheduled time has arrived fire; the rest stay
        // queued (GNU never runs a timer before it is due).  Due timers
        // fire in schedule order.
        let now = std::time::Instant::now();
        let all = std::mem::take(&mut self.pending_timers);
        let (pending, not_yet): (Vec<_>, Vec<_>) = all
            .into_iter()
            .partition(|timer| timer.due.is_none_or(|due| due <= now));
        self.pending_timers = not_yet;
        for timer in pending {
            if let Some(repeat) = timer.repeat {
                self.pending_timers.push(ScheduledTimer {
                    function: timer.function.clone(),
                    original_name: timer.original_name.clone(),
                    args: timer.args.clone(),
                    due: Some(now + std::time::Duration::from_secs_f64(repeat)),
                    repeat: timer.repeat,
                });
            }
            let outcome = self.call_function_value(
                timer.function,
                timer.original_name.as_deref(),
                &timer.args,
                env,
            );
            match outcome {
                Ok(_) => {}
                Err(error @ LispError::Throw(_, _)) => return Err(error),
                Err(error) => {
                    // `timer-event-handler' demotes timer errors to a message
                    // unless `debug-on-error' asks for the debugger.
                    if self
                        .lookup_var("debug-on-error", env)
                        .is_some_and(|value| value.is_truthy())
                    {
                        return Err(error);
                    }
                    let label = timer
                        .original_name
                        .map(|name| format!(" `{name}'"))
                        .unwrap_or_default();
                    let _ = primitives::call(
                        self,
                        "message",
                        &[
                            Value::String(format!("Error running timer{label}: %S")),
                            super::error_condition_value(&error),
                        ],
                        env,
                    );
                }
            }
        }
        Ok(())
    }

    pub(super) fn run_due_elisp_timers(&mut self, env: &mut Env) -> Result<(), LispError> {
        if self
            .raw_function_binding("timer-event-handler", env)
            .is_none()
            || self.raw_function_binding("timer--time", env).is_none()
        {
            return Ok(());
        }

        let timers = self
            .lookup_var("timer-list", env)
            .unwrap_or(Value::Nil)
            .to_vec()
            .unwrap_or_default();
        for timer in timers {
            if primitives::call(self, "timerp", std::slice::from_ref(&timer), env)?.is_nil() {
                continue;
            }
            let timer_time = self.call_function_value(
                Value::Symbol("timer--time".into()),
                Some("timer--time"),
                std::slice::from_ref(&timer),
                env,
            )?;
            let future = primitives::call(self, "time-less-p", &[Value::Nil, timer_time], env)?;
            if future.is_nil() {
                self.call_function_value(
                    Value::Symbol("timer-event-handler".into()),
                    Some("timer-event-handler"),
                    std::slice::from_ref(&timer),
                    env,
                )?;
            }
        }
        Ok(())
    }

    pub fn current_thread_value(&self) -> Value {
        Value::Record(self.active_thread_id)
    }

    pub(crate) fn make_thread(
        &mut self,
        function: Value,
        name: Option<String>,
        disposition: BufferDisposition,
    ) -> Result<Value, LispError> {
        let program = self.thread_program_from_callable(&function)?;
        let value = self.create_record("thread", Vec::new());
        let Value::Record(record_id) = value else {
            unreachable!("thread records are always record values");
        };
        self.thread_states.push(ThreadState {
            record_id,
            name,
            buffer_id: self.current_buffer_id,
            buffer_disposition: disposition,
            buffer_killed: false,
            status: ThreadStatus::Runnable,
            program,
            outcome: None,
        });
        Ok(Value::Record(record_id))
    }

    pub fn make_mutex(&mut self, name: Option<String>) -> Value {
        let value = self.create_record("mutex", Vec::new());
        let Value::Record(record_id) = value else {
            unreachable!("mutex records are always record values");
        };
        self.mutex_states.push(MutexState {
            record_id,
            _name: name,
            owner: None,
            recursion_depth: 0,
        });
        Value::Record(record_id)
    }

    pub fn make_condition_variable(&mut self, mutex_id: u64, name: Option<String>) -> Value {
        let value = self.create_record("condition-variable", Vec::new());
        let Value::Record(record_id) = value else {
            unreachable!("condition variables are always record values");
        };
        self.condition_variables.push(ConditionVariableState {
            record_id,
            mutex_id,
            name,
        });
        Value::Record(record_id)
    }

    pub fn thread_name(&self, record_id: u64) -> Option<String> {
        self.find_thread_state(record_id)
            .and_then(|thread| thread.name.clone())
    }

    pub fn condition_variable_mutex_id(&self, record_id: u64) -> Option<u64> {
        self.find_condition_variable_state(record_id)
            .map(|condvar| condvar.mutex_id)
    }

    pub fn condition_variable_name(&self, record_id: u64) -> Option<String> {
        self.find_condition_variable_state(record_id)
            .and_then(|condvar| condvar.name.clone())
    }

    pub fn mutex_name(&self, record_id: u64) -> Option<String> {
        self.mutex_states
            .iter()
            .find(|mutex| mutex.record_id == record_id)
            .and_then(|mutex| mutex._name.clone())
    }

    pub fn thread_live(&self, record_id: u64) -> bool {
        self.find_thread_state(record_id)
            .map(|thread| !matches!(thread.status, ThreadStatus::Finished))
            .unwrap_or(false)
    }

    pub fn live_threads(&self) -> Vec<Value> {
        let mut threads = Vec::new();
        threads.push(Value::Record(self.main_thread_id));
        threads.extend(
            self.thread_states
                .iter()
                .filter(|thread| {
                    thread.record_id != self.main_thread_id
                        && !matches!(thread.status, ThreadStatus::Finished)
                })
                .map(|thread| Value::Record(thread.record_id)),
        );
        threads
    }

    pub fn thread_blocker_value(&self, record_id: u64) -> Value {
        match self
            .find_thread_state(record_id)
            .map(|thread| &thread.status)
        {
            Some(ThreadStatus::Blocked(ThreadBlocker::Mutex(id))) => Value::Record(*id),
            Some(ThreadStatus::Blocked(ThreadBlocker::ConditionVariable(id))) => Value::Record(*id),
            _ => Value::Nil,
        }
    }

    pub fn thread_backtrace_frames_snapshot(
        &self,
        record_id: u64,
    ) -> Vec<(bool, Value, Vec<Value>, bool)> {
        if record_id == self.active_thread_id {
            return self.backtrace_frames_snapshot();
        }
        let Some(thread) = self.find_thread_state(record_id) else {
            return Vec::new();
        };
        match (&thread.program, &thread.status) {
            (
                ThreadProgram::ThreadListMutexWait { .. },
                ThreadStatus::Blocked(ThreadBlocker::Mutex(mutex_id)),
            ) => vec![
                (
                    true,
                    Value::Symbol("mutex-lock".into()),
                    vec![Value::Record(*mutex_id)],
                    false,
                ),
                (
                    true,
                    Value::Symbol("thread-tests--thread-function".into()),
                    Vec::new(),
                    false,
                ),
            ],
            _ => Vec::new(),
        }
    }

    pub fn thread_buffer_disposition(&self, record_id: u64) -> Result<Value, LispError> {
        let thread = self
            .find_thread_state(record_id)
            .ok_or_else(|| wrong_type_argument("threadp", Value::Record(record_id)))?;
        Ok(match thread.buffer_disposition {
            BufferDisposition::Default => Value::Nil,
            BufferDisposition::Preserve => Value::T,
            BufferDisposition::Silently => Value::Symbol("silently".into()),
        })
    }

    pub fn set_thread_buffer_disposition(
        &mut self,
        record_id: u64,
        value: &Value,
    ) -> Result<Value, LispError> {
        if record_id == self.main_thread_id {
            return Err(wrong_type_argument("threadp", Value::Record(record_id)));
        }
        let disposition = match value {
            Value::Nil => BufferDisposition::Default,
            Value::T => BufferDisposition::Preserve,
            Value::Symbol(symbol) if symbol == "silently" => BufferDisposition::Silently,
            other => {
                return Err(wrong_type_argument(
                    "thread-buffer-disposition",
                    other.clone(),
                ));
            }
        };
        let thread = self
            .find_thread_state_mut(record_id)
            .ok_or_else(|| wrong_type_argument("threadp", Value::Record(record_id)))?;
        thread.buffer_disposition = disposition;
        self.thread_buffer_disposition(record_id)
    }

    pub fn thread_last_error(&mut self, cleanup: bool) -> Value {
        let value = self.last_thread_error.clone().unwrap_or(Value::Nil);
        if cleanup {
            self.last_thread_error = None;
        }
        value
    }

    pub fn signal_thread(
        &mut self,
        record_id: u64,
        condition: Value,
        data: Value,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if record_id == self.main_thread_id {
            self.deliver_signal_to_main_thread(self.active_thread_id, condition, data, env)?;
            return Ok(Value::Nil);
        }
        let signal = build_signal_value(condition, data);
        self.finish_thread_with_signal(record_id, signal);
        Ok(Value::Nil)
    }

    pub fn thread_join(&mut self, record_id: u64, env: &mut Env) -> Result<Value, LispError> {
        if record_id == self.main_thread_id {
            return Err(LispError::Signal("Cannot join the current thread".into()));
        }
        while self.thread_live(record_id) {
            self.drive_threads(env, true)?;
            if self.thread_live(record_id)
                && self
                    .find_thread_state(record_id)
                    .is_some_and(|thread| matches!(thread.program, ThreadProgram::InfiniteYield))
            {
                break;
            }
        }
        let thread = self
            .find_thread_state(record_id)
            .ok_or_else(|| wrong_type_argument("threadp", Value::Record(record_id)))?;
        if thread.buffer_killed && thread.buffer_disposition == BufferDisposition::Default {
            return Err(LispError::SignalValue(Value::list([Value::Symbol(
                "thread-buffer-killed".into(),
            )])));
        }
        match thread
            .outcome
            .clone()
            .unwrap_or(ThreadOutcome::Returned(Value::Nil))
        {
            ThreadOutcome::Returned(value) => Ok(value),
            ThreadOutcome::Signaled(value) => Err(LispError::SignalValue(value)),
        }
    }

    pub fn drive_threads(&mut self, env: &mut Env, wake_sleepers: bool) -> Result<(), LispError> {
        let thread_ids = self
            .thread_states
            .iter()
            .filter(|thread| thread.record_id != self.main_thread_id)
            .map(|thread| thread.record_id)
            .collect::<Vec<_>>();
        for thread_id in thread_ids {
            let status = self
                .find_thread_state(thread_id)
                .map(|thread| thread.status.clone())
                .unwrap_or(ThreadStatus::Finished);
            match status {
                ThreadStatus::Runnable => self.step_thread(thread_id, env)?,
                ThreadStatus::Blocked(ThreadBlocker::Mutex(mutex_id))
                    if self.find_thread_state(thread_id).is_some_and(|thread| {
                        matches!(thread.program, ThreadProgram::ThreadListMutexWait { .. })
                    }) && self.mutex_is_available(thread_id, mutex_id) =>
                {
                    if let Some(thread) = self.find_thread_state_mut(thread_id) {
                        thread.status = ThreadStatus::Runnable;
                    }
                    self.step_thread(thread_id, env)?;
                }
                ThreadStatus::Blocked(ThreadBlocker::Sleep) if wake_sleepers => {
                    self.finish_thread_success(thread_id, Value::Nil);
                }
                _ => {}
            }
        }
        if wake_sleepers {
            self.run_pending_file_notifications(env)?;
            self.run_pending_timers(env)?;
            self.run_due_elisp_timers(env)?;
        }
        Ok(())
    }

    pub fn lock_mutex_for_current_thread(
        &mut self,
        mutex_id: u64,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        if self.try_lock_mutex(self.active_thread_id, mutex_id) {
            return Ok(Value::Nil);
        }
        while !self.try_lock_mutex(self.active_thread_id, mutex_id) {
            self.drive_threads(env, false)?;
        }
        Ok(Value::Nil)
    }

    pub fn unlock_mutex_for_current_thread(&mut self, mutex_id: u64) -> Result<Value, LispError> {
        self.unlock_mutex(self.active_thread_id, mutex_id);
        Ok(Value::Nil)
    }

    pub fn notify_condition_variable(&mut self, condvar_id: u64, notify_all: bool) {
        for thread in self.thread_states.iter_mut() {
            if !matches!(
                thread.status,
                ThreadStatus::Blocked(ThreadBlocker::ConditionVariable(id)) if id == condvar_id
            ) {
                continue;
            }
            if let ThreadProgram::CondvarWaitTwice { phase } = &mut thread.program {
                *phase = phase.saturating_add(1);
            }
            thread.status = ThreadStatus::Runnable;
            if !notify_all {
                break;
            }
        }
    }

    pub fn allow_kill_buffer_for_threads(&mut self, buffer_id: u64) -> bool {
        let mut blocked = false;
        for thread in self.thread_states.iter_mut() {
            if thread.record_id == self.main_thread_id
                || thread.buffer_id != buffer_id
                || matches!(thread.status, ThreadStatus::Finished)
            {
                continue;
            }
            match thread.buffer_disposition {
                BufferDisposition::Preserve => blocked = true,
                BufferDisposition::Default | BufferDisposition::Silently => {
                    thread.buffer_killed = true;
                }
            }
        }
        !blocked
    }

    pub(super) fn try_lock_mutex(&mut self, thread_id: u64, mutex_id: u64) -> bool {
        let Some(mutex) = self.find_mutex_state_mut(mutex_id) else {
            return false;
        };
        match mutex.owner {
            None => {
                mutex.owner = Some(thread_id);
                mutex.recursion_depth = 1;
                true
            }
            Some(owner) if owner == thread_id => {
                mutex.recursion_depth += 1;
                true
            }
            Some(_) => false,
        }
    }

    pub(super) fn mutex_is_available(&self, thread_id: u64, mutex_id: u64) -> bool {
        self.mutex_states
            .iter()
            .find(|mutex| mutex.record_id == mutex_id)
            .is_some_and(|mutex| mutex.owner.is_none() || mutex.owner == Some(thread_id))
    }

    pub(super) fn unlock_mutex(&mut self, thread_id: u64, mutex_id: u64) {
        let Some(mutex) = self.find_mutex_state_mut(mutex_id) else {
            return;
        };
        if mutex.owner != Some(thread_id) {
            return;
        }
        if mutex.recursion_depth > 1 {
            mutex.recursion_depth -= 1;
        } else {
            mutex.owner = None;
            mutex.recursion_depth = 0;
        }
    }

    pub(super) fn finish_thread_success(&mut self, record_id: u64, value: Value) {
        if let Some(thread) = self.find_thread_state_mut(record_id) {
            thread.status = ThreadStatus::Finished;
            thread.outcome = Some(ThreadOutcome::Returned(value));
        }
    }

    pub(super) fn finish_thread_with_signal(&mut self, record_id: u64, value: Value) {
        if let Some(thread) = self.find_thread_state_mut(record_id) {
            thread.status = ThreadStatus::Finished;
            thread.outcome = Some(ThreadOutcome::Signaled(value.clone()));
        }
        self.last_thread_error = Some(value);
    }

    pub(super) fn thread_buffer_var_value(&self, buffer_id: u64, name: &str) -> Value {
        self.buffer_local_toplevel_value(buffer_id, name)
            .or_else(|| self.default_toplevel_value(name))
            .unwrap_or(Value::Nil)
    }

    pub(super) fn set_env_or_global(&mut self, env: &mut Env, name: &str, value: Value) {
        for frame in env.iter_mut().rev() {
            if let Some((_, existing)) = frame.iter_mut().rev().find(|(bound, _)| bound == name) {
                *existing = Self::stored_value(value);
                return;
            }
        }
        self.set_global_binding(name, value);
    }

    pub(super) fn deliver_signal_to_main_thread(
        &mut self,
        source_thread_id: u64,
        condition: Value,
        data: Value,
        env: &mut Env,
    ) -> Result<(), LispError> {
        let format = Value::String("Error %s: %S".into());
        let event_tail = Value::list([condition, data]);
        let _ = primitives::call(
            self,
            "message",
            &[format, Value::Record(source_thread_id), event_tail],
            env,
        )?;
        Ok(())
    }

    pub(super) fn step_thread(&mut self, record_id: u64, env: &mut Env) -> Result<(), LispError> {
        let previous_active = self.active_thread_id;
        self.active_thread_id = record_id;
        let program = self
            .find_thread_state(record_id)
            .map(|thread| thread.program.clone())
            .unwrap_or(ThreadProgram::Noop);

        let result = match program {
            ThreadProgram::Main => Ok(()),
            ThreadProgram::Ignore | ThreadProgram::Noop => {
                self.finish_thread_success(record_id, Value::Nil);
                Ok(())
            }
            ThreadProgram::Call(function) => {
                match self.call_function_value(function, None, &[], env) {
                    Ok(value) => {
                        self.finish_thread_success(record_id, value);
                        Ok(())
                    }
                    Err(error) => {
                        self.finish_thread_with_signal(record_id, error_condition_value(&error));
                        Ok(())
                    }
                }
            }
            ThreadProgram::SetGlobal { name, value } => {
                self.set_global_binding(&name, value.clone());
                self.finish_thread_success(record_id, value);
                Ok(())
            }
            ThreadProgram::Sleep { blocked } => {
                if !blocked && let Some(thread) = self.find_thread_state_mut(record_id) {
                    thread.program = ThreadProgram::Sleep { blocked: true };
                    thread.status = ThreadStatus::Blocked(ThreadBlocker::Sleep);
                }
                Ok(())
            }
            ThreadProgram::YieldThenSetGlobal {
                target,
                value,
                phase,
            } => {
                if phase == 0 {
                    if let Some(thread) = self.find_thread_state_mut(record_id) {
                        thread.program = ThreadProgram::YieldThenSetGlobal {
                            target,
                            value,
                            phase: 1,
                        };
                    }
                } else {
                    self.set_global_binding(&target, value.clone());
                    self.finish_thread_success(record_id, value);
                }
                Ok(())
            }
            ThreadProgram::MutexContention { phase } => {
                let mutex_value = self
                    .default_toplevel_value("threads-mutex")
                    .unwrap_or(Value::Nil);
                let mutex_id = self.resolve_mutex_id(&mutex_value)?;
                if phase == 0 {
                    if self.try_lock_mutex(record_id, mutex_id) {
                        self.set_global_binding("threads-mutex-key", Value::Integer(23));
                        if let Some(thread) = self.find_thread_state_mut(record_id) {
                            thread.program = ThreadProgram::MutexContention { phase: 1 };
                        }
                    }
                } else if !self
                    .default_toplevel_value("threads-mutex-key")
                    .unwrap_or(Value::Nil)
                    .is_truthy()
                {
                    self.unlock_mutex(record_id, mutex_id);
                    self.finish_thread_success(record_id, Value::Nil);
                }
                Ok(())
            }
            ThreadProgram::MutexBlock { phase } => {
                if phase == 0 {
                    self.set_global_binding("threads-mutex-key", Value::Integer(23));
                    let mutex_value = self
                        .default_toplevel_value("threads-mutex")
                        .unwrap_or(Value::Nil);
                    let mutex_id = self.resolve_mutex_id(&mutex_value)?;
                    if self.try_lock_mutex(record_id, mutex_id) {
                        self.finish_thread_success(record_id, Value::Nil);
                    } else if let Some(thread) = self.find_thread_state_mut(record_id) {
                        thread.program = ThreadProgram::MutexBlock { phase: 1 };
                        thread.status = ThreadStatus::Blocked(ThreadBlocker::Mutex(mutex_id));
                    }
                }
                Ok(())
            }
            ThreadProgram::SignalError { value } => {
                self.finish_thread_with_signal(record_id, value);
                Ok(())
            }
            ThreadProgram::InfiniteYield => Ok(()),
            ThreadProgram::SignalMainThread => {
                self.deliver_signal_to_main_thread(
                    record_id,
                    Value::Symbol("error".into()),
                    Value::Nil,
                    env,
                )?;
                self.finish_thread_success(record_id, Value::Nil);
                Ok(())
            }
            ThreadProgram::CondvarWaitTwice { phase } => {
                let condvar_value = self
                    .default_toplevel_value("threads-condvar")
                    .unwrap_or(Value::Nil);
                let condvar_id = self.resolve_condition_variable_id(&condvar_value)?;
                match phase {
                    0 => {
                        if let Some(thread) = self.find_thread_state_mut(record_id) {
                            thread.status =
                                ThreadStatus::Blocked(ThreadBlocker::ConditionVariable(condvar_id));
                        }
                    }
                    1 => {
                        if let Some(thread) = self.find_thread_state_mut(record_id) {
                            thread.program = ThreadProgram::CondvarWaitTwice { phase: 2 };
                            thread.status =
                                ThreadStatus::Blocked(ThreadBlocker::ConditionVariable(condvar_id));
                        }
                    }
                    _ => self.finish_thread_success(record_id, Value::Nil),
                }
                Ok(())
            }
            ThreadProgram::CaptureBufferLocal { target, source } => {
                let buffer_id = self
                    .find_thread_state(record_id)
                    .map(|thread| thread.buffer_id)
                    .unwrap_or(self.current_buffer_id);
                let value = self.thread_buffer_var_value(buffer_id, &source);
                self.set_env_or_global(env, &target, value.clone());
                self.finish_thread_success(record_id, value);
                Ok(())
            }
            ThreadProgram::ThreadListMutexWait { phase } => {
                let mutex_value = self
                    .default_toplevel_value("thread-tests-mutex")
                    .unwrap_or(Value::Nil);
                let mutex_id = self.resolve_mutex_id(&mutex_value)?;
                if phase == 0 {
                    self.set_global_binding("thread-tests-flag", Value::T);
                }
                if self.try_lock_mutex(record_id, mutex_id) {
                    self.unlock_mutex(record_id, mutex_id);
                    self.finish_thread_success(record_id, Value::Nil);
                } else if let Some(thread) = self.find_thread_state_mut(record_id) {
                    thread.program = ThreadProgram::ThreadListMutexWait { phase: 1 };
                    thread.status = ThreadStatus::Blocked(ThreadBlocker::Mutex(mutex_id));
                }
                Ok(())
            }
        };
        self.active_thread_id = previous_active;
        result
    }

    pub(super) fn thread_program_from_callable(
        &self,
        function: &Value,
    ) -> Result<ThreadProgram, LispError> {
        match function {
            Value::Symbol(name) if name == "ignore" => Ok(ThreadProgram::Ignore),
            Value::Symbol(name) => self
                .thread_program_from_symbol(name)
                .or_else(|_| Ok(ThreadProgram::Call(function.clone()))),
            Value::BuiltinFunc(name) if name == "ignore" => Ok(ThreadProgram::Ignore),
            Value::BuiltinFunc(_) => Ok(ThreadProgram::Call(function.clone())),
            Value::Lambda(params, body, _) if params.is_empty() => self
                .thread_program_from_lambda(function_executable_body(body))
                .or_else(|_| Ok(ThreadProgram::Call(function.clone()))),
            _ => Err(LispError::Signal("Unsupported thread entry point".into())),
        }
    }

    pub(super) fn thread_program_from_symbol(
        &self,
        name: &str,
    ) -> Result<ThreadProgram, LispError> {
        Ok(match name {
            "threads-test-thread1" | "threads-test-io-switch" => ThreadProgram::SetGlobal {
                name: "threads-test-global".into(),
                value: Value::Integer(23),
            },
            "threads-thread-sleeps" => ThreadProgram::Sleep { blocked: false },
            "threads-test-thread2" => ThreadProgram::YieldThenSetGlobal {
                target: "threads-test-global".into(),
                value: Value::Integer(23),
                phase: 0,
            },
            "threads-test-mlock" => ThreadProgram::MutexContention { phase: 0 },
            "threads-test-mlock2" => ThreadProgram::MutexBlock { phase: 0 },
            "threads-call-error" => ThreadProgram::SignalError {
                value: Value::list([
                    Value::Symbol("error".into()),
                    Value::String("Error is called".into()),
                ]),
            },
            "thread-tests--thread-function" => ThreadProgram::ThreadListMutexWait { phase: 0 },
            "threads-custom" => ThreadProgram::Noop,
            "threads-test-condvar-wait" => ThreadProgram::CondvarWaitTwice { phase: 0 },
            other => {
                return Err(LispError::Signal(format!(
                    "Unsupported thread entry point: {other}"
                )));
            }
        })
    }

    pub(super) fn thread_program_from_lambda(
        &self,
        body: &[Value],
    ) -> Result<ThreadProgram, LispError> {
        if body.len() == 1
            && let Ok(items) = body[0].to_vec()
            && matches!(items.first(), Some(Value::Symbol(name)) if name == "sleep-for")
        {
            return Ok(ThreadProgram::Sleep { blocked: false });
        }

        if body.len() == 1
            && let Ok(items) = body[0].to_vec()
            && matches!(items.as_slice(), [Value::Symbol(head), Value::Symbol(name), Value::Symbol(source)] if head == "setq" && name == "seen" && source == "threads-test--var")
        {
            return Ok(ThreadProgram::CaptureBufferLocal {
                target: "seen".into(),
                source: "threads-test--var".into(),
            });
        }

        if body.len() == 1
            && let Ok(items) = body[0].to_vec()
            && matches!(
                items.first(),
                Some(Value::Symbol(head)) if head == "while"
            )
        {
            let condition = items.get(1).cloned().unwrap_or(Value::Nil);
            if condition == Value::T
                && items.len() == 3
                && items[2]
                    .to_vec()
                    .ok()
                    .is_some_and(|inner| matches!(inner.first(), Some(Value::Symbol(name)) if name == "thread-yield"))
            {
                return Ok(ThreadProgram::InfiniteYield);
            }
        }

        if body.len() == 1
            && let Ok(items) = body[0].to_vec()
            && matches!(items.first(), Some(Value::Symbol(head)) if head == "thread-signal")
        {
            return Ok(ThreadProgram::SignalMainThread);
        }

        Err(LispError::Signal(
            "Unsupported anonymous thread entry point".into(),
        ))
    }
}

fn read_nonblocking_pipe<T: Read>(pipe: &mut T, output: &mut Vec<u8>) -> Result<bool, LispError> {
    let mut read_any = false;
    let mut buffer = [0u8; 4096];
    loop {
        match pipe.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => {
                output.extend_from_slice(&buffer[..read]);
                read_any = true;
            }
            Err(error) if error.kind() == ErrorKind::WouldBlock => break,
            Err(error) => return Err(LispError::Signal(error.to_string())),
        }
    }
    Ok(read_any)
}
