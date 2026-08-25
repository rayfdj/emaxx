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

fn record_child_exit(process: &mut ProcessState, status: std::process::ExitStatus) {
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;

        if let Some(signal) = status.signal() {
            process.status = ProcessStatus::Signal;
            process.exit_code = Some(signal);
            process.exit_signal = Some(signal);
            return;
        }
    }
    process.status = ProcessStatus::Exit;
    process.exit_code = status.code();
    process.exit_signal = None;
}

enum ChildStatusEvent {
    Stopped,
    Continued,
    Exited(std::process::ExitStatus),
}

#[cfg(unix)]
fn poll_child_status(child: &mut Child) -> std::io::Result<Option<ChildStatusEvent>> {
    use std::os::unix::process::ExitStatusExt;

    let mut status = 0;
    loop {
        // Unlike `Child::try_wait', GNU observes job-control transitions as
        // well as terminal exits.  WUNTRACED/WCONTINUED preserves that
        // process.c contract while remaining nonblocking.
        // SAFETY: CHILD owns this live pid and STATUS is initialized,
        // writable caller storage for waitpid's result.
        let result = unsafe {
            libc::waitpid(
                child.id() as libc::pid_t,
                &mut status,
                libc::WNOHANG | libc::WUNTRACED | libc::WCONTINUED,
            )
        };
        if result == 0 {
            return Ok(None);
        }
        if result < 0 {
            let error = std::io::Error::last_os_error();
            if error.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error);
        }
        if libc::WIFSTOPPED(status) {
            return Ok(Some(ChildStatusEvent::Stopped));
        }
        if libc::WIFCONTINUED(status) {
            return Ok(Some(ChildStatusEvent::Continued));
        }
        return Ok(Some(ChildStatusEvent::Exited(
            std::process::ExitStatus::from_raw(status),
        )));
    }
}

#[cfg(not(unix))]
fn poll_child_status(child: &mut Child) -> std::io::Result<Option<ChildStatusEvent>> {
    child
        .try_wait()
        .map(|status| status.map(ChildStatusEvent::Exited))
}

#[cfg(unix)]
fn signal_event_message(signal: i32) -> String {
    // GNU uses strsignal and lowercases its initial character for sentinel
    // events (for example, SIGPIPE begins with "broken pipe"; Darwin's
    // description also includes the signal number).
    // SAFETY: strsignal returns either null or a process-lifetime C string.
    let description = unsafe {
        let pointer = libc::strsignal(signal);
        (!pointer.is_null()).then(|| {
            std::ffi::CStr::from_ptr(pointer)
                .to_string_lossy()
                .into_owned()
        })
    }
    .unwrap_or_else(|| "unknown".into());
    let mut characters = description.chars();
    let Some(first) = characters.next() else {
        return "unknown\n".into();
    };
    format!("{}{}\n", first.to_lowercase(), characters.as_str())
}

#[cfg(not(unix))]
fn signal_event_message(_signal: i32) -> String {
    "killed\n".into()
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

fn terminate_child_without_blocking(mut runtime: RunningProcess) {
    let _ = runtime.child.kill();
    if matches!(runtime.child.try_wait(), Ok(Some(_))) {
        return;
    }
    // GNU `delete-process' closes the Lisp process immediately and reaps the
    // OS child later.  Waiting inline can deadlock when a killed server is
    // still unwinding an active connection (notably gnutls-serv on Darwin).
    // Retain the Child and its pipes in one detached reaper until wait(2)
    // completes, so prompt deletion does not trade the stall for a zombie.
    let _ = std::thread::Builder::new()
        .name("process-reaper".into())
        .spawn(move || {
            let _ = runtime.child.wait();
        });
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

    /// GNU's `Vprocess_alist' membership: a process stays visible to
    /// `process-list', `get-process', and `get-buffer-process' after it
    /// exits until status_notify processes the change — process.c:7853
    /// removes it there (`delete-exited-processes', default t) right
    /// before the sentinel runs.  `sentinel_notified' marks exactly that
    /// point, so an exited-but-unnotified process is still listed, and a
    /// filter or hook running on its final output can still find it.
    fn in_process_alist(process: &ProcessState) -> bool {
        process.status.is_live() || !process.sentinel_notified
    }

    pub fn process_list_value(&self) -> Value {
        Value::list(
            self.process_states
                .iter()
                .rev()
                .filter(|process| Self::in_process_alist(process))
                .map(|process| Value::Record(process.record_id)),
        )
    }

    pub fn set_process_plist_value(&mut self, record_id: u64, plist: Value) -> bool {
        if let Some(process) = self.find_process_state_mut(record_id) {
            process.plist = Self::stored_value(plist);
            true
        } else {
            false
        }
    }

    pub fn set_process_gnutls_boot_parameters(
        &mut self,
        record_id: u64,
        parameters: Value,
    ) -> bool {
        let parameters = Self::stored_value(parameters);
        let Some(process) = self.find_process_state_mut(record_id) else {
            return false;
        };
        drop(std::mem::replace(
            &mut process.gnutls.boot_parameters,
            parameters,
        ));
        true
    }

    pub(crate) fn process_gnutls_boot_parameters(&self, record_id: u64) -> Option<Value> {
        self.find_process_state(record_id)
            .map(|process| process.gnutls.boot_parameters.clone())
    }

    pub(crate) fn clear_process_gnutls_boot_parameters(&mut self, record_id: u64) -> bool {
        self.set_process_gnutls_boot_parameters(record_id, Value::Nil)
    }

    pub fn process_gnutls_initstage(&self, record_id: u64) -> Option<i64> {
        self.find_process_state(record_id)
            .map(|process| process.gnutls.initstage)
    }

    pub fn deinit_process_gnutls(&mut self, record_id: u64) -> Option<bool> {
        self.find_process_state_mut(record_id).map(|process| {
            let was_active = process.gnutls.active;
            if was_active {
                process.gnutls.session = None;
                process.gnutls.peer_status = Value::Nil;
                process.gnutls.active = false;
                if process.gnutls.initstage >= GNUTLS_STAGE_INIT {
                    process.gnutls.initstage = GNUTLS_STAGE_INIT - 1;
                }
            }
            was_active
        })
    }

    pub(crate) fn install_process_gnutls(
        &mut self,
        record_id: u64,
        session: ProcessGnuTlsSession,
        initstage: i64,
        peer_status: Value,
    ) -> Result<(), LispError> {
        let process = self
            .find_process_state_mut(record_id)
            .ok_or_else(|| wrong_type_argument("processp", Value::Record(record_id)))?;
        process.gnutls.session = Some(session);
        process.gnutls.initstage = initstage;
        process.gnutls.active = true;
        process.gnutls.peer_status = peer_status;
        Ok(())
    }

    pub(crate) fn continue_process_gnutls_handshake(
        &mut self,
        record_id: u64,
    ) -> Result<(std::ffi::c_int, *mut std::ffi::c_void), LispError> {
        let process = self
            .find_process_state_mut(record_id)
            .ok_or_else(|| wrong_type_argument("processp", Value::Record(record_id)))?;
        let session = process
            .gnutls
            .session
            .as_mut()
            .ok_or_else(|| LispError::Signal("GnuTLS session is not initialized".into()))?;
        let result = session.handshake(false)?;
        Ok((result, session.raw_state()))
    }

    pub(crate) fn finish_process_gnutls_handshake(
        &mut self,
        record_id: u64,
        peer_status: Value,
    ) -> Result<(), LispError> {
        let process = self
            .find_process_state_mut(record_id)
            .ok_or_else(|| wrong_type_argument("processp", Value::Record(record_id)))?;
        process.gnutls.initstage = 9;
        process.gnutls.peer_status = Self::stored_value(peer_status);
        drop(std::mem::replace(
            &mut process.gnutls.boot_parameters,
            Value::Nil,
        ));
        Ok(())
    }

    pub(crate) fn process_gnutls_peer_status(&self, record_id: u64) -> Option<Value> {
        self.find_process_state(record_id)
            .map(|process| process.gnutls.peer_status.clone())
    }

    pub(crate) fn process_gnutls_bye(
        &mut self,
        record_id: u64,
        continue_transport: bool,
    ) -> Result<std::ffi::c_int, LispError> {
        let process = self
            .find_process_state_mut(record_id)
            .ok_or_else(|| wrong_type_argument("processp", Value::Record(record_id)))?;
        let session = process
            .gnutls
            .session
            .as_mut()
            .ok_or_else(|| LispError::Signal("GnuTLS session is not initialized".into()))?;
        Ok(session.bye(continue_transport))
    }

    #[cfg(unix)]
    pub(crate) fn process_network_transport_handle(
        &self,
        record_id: u64,
    ) -> Result<usize, LispError> {
        use std::os::fd::AsRawFd;

        let process = self
            .find_process_state(record_id)
            .ok_or_else(|| wrong_type_argument("processp", Value::Record(record_id)))?;
        match process.network.as_ref() {
            Some(NetworkRuntime::Stream(stream)) => Ok(stream.as_raw_fd() as usize),
            Some(NetworkRuntime::UnixStream(stream)) => Ok(stream.as_raw_fd() as usize),
            _ => Err(LispError::Signal(
                "GnuTLS requires a connected stream process".into(),
            )),
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
                    .is_some_and(|record| record.kind == RecordKind::Thread) =>
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
                    .is_some_and(|record| record.kind == RecordKind::Mutex) =>
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
                    .is_some_and(|record| record.kind == RecordKind::ConditionVariable) =>
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
                    .is_some_and(|record| record.kind == RecordKind::Process) =>
            {
                Ok(*id)
            }
            other => Err(wrong_type_argument("processp", other.clone())),
        }
    }

    pub(crate) fn create_process(
        &mut self,
        buffer_id: Option<u64>,
        program: Option<String>,
        argv: Vec<String>,
        runtime: Option<RunningProcess>,
        name: Option<String>,
    ) -> Result<Value, LispError> {
        // GNU names the process after the NAME argument (uniquified with
        // <N> on collision), not the program.
        let name = self.unique_process_name(&name.or_else(|| program.clone()).unwrap_or_default());
        let process = self.create_pseudovector(RecordKind::Process, "process", Vec::new());
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
        let (decoding, encoding) = self
            .lookup_var("default-process-coding-system", &Env::new())
            .and_then(|value| value.cons_values())
            .unwrap_or((Value::Nil, Value::Nil));
        let os_pid = runtime.as_ref().map(|runtime| runtime.child.id());
        let kind = if program.is_some() {
            ProcessKind::Real
        } else {
            ProcessKind::Pipe
        };
        self.process_states.push(ProcessState {
            record_id,
            kind,
            buffer_id,
            mark_marker_id,
            status: ProcessStatus::Run,
            filter: None,
            sentinel: None,
            sentinel_notified: false,
            log: None,
            name,
            thread_id: Some(self.active_thread_id),
            query_on_exit_flag: true,
            traffic_stopped: false,
            inherit_coding_system_flag: false,
            decoding,
            encoding,
            program,
            argv,
            stderr_process_id: None,
            exit_code: None,
            exit_signal: None,
            os_pid,
            runtime,
            network: None,
            serial: None,
            contact_host: None,
            contact_service: None,
            remote: None,
            parent_server_id: None,
            pending_stdout: Vec::new(),
            pending_stderr: Vec::new(),
            output_delivery_count: 0,
            plist: Value::Nil,
            gnutls: ProcessGnuTlsState::default(),
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
        inherit_coding_system_flag: bool,
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
        decoding: Value,
        encoding: Value,
    ) -> Result<Value, LispError> {
        let status = match &network {
            NetworkRuntime::Listener(_) | NetworkRuntime::UnixListener(_) => ProcessStatus::Listen,
            NetworkRuntime::Stream(_)
            | NetworkRuntime::Datagram { .. }
            | NetworkRuntime::UnixStream(_) => ProcessStatus::Open,
        };
        let process = self.create_pseudovector(RecordKind::Process, "process", Vec::new());
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
            kind: ProcessKind::Network,
            buffer_id,
            mark_marker_id,
            status,
            filter,
            sentinel,
            sentinel_notified: false,
            log,
            name: name.to_string(),
            thread_id: Some(self.active_thread_id),
            query_on_exit_flag: true,
            traffic_stopped: false,
            inherit_coding_system_flag,
            decoding,
            encoding,
            program: None,
            argv: Vec::new(),
            stderr_process_id: None,
            exit_code: None,
            exit_signal: None,
            os_pid: None,
            runtime: None,
            network: Some(network),
            serial: None,
            contact_host,
            contact_service,
            remote,
            parent_server_id,
            pending_stdout: Vec::new(),
            pending_stderr: Vec::new(),
            output_delivery_count: 0,
            plist,
            gnutls: ProcessGnuTlsState::default(),
            contact,
        });
        Ok(process)
    }

    /// Create a serial connection process backed by an already-open port.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn create_serial_process(
        &mut self,
        name: &str,
        buffer_id: u64,
        filter: Option<Value>,
        sentinel: Option<Value>,
        plist: Value,
        serial: super::SerialRuntime,
        contact: Value,
        decoding: Value,
        encoding: Value,
        query_on_exit_flag: bool,
        stopped: bool,
    ) -> Result<Value, LispError> {
        let name = self.unique_process_name(name);
        let process = self.create_pseudovector(RecordKind::Process, "process", Vec::new());
        let Value::Record(record_id) = process.clone() else {
            unreachable!("create_record returns a record")
        };
        let marker = self.make_marker();
        let Value::Marker(mark_marker_id) = marker else {
            unreachable!("make_marker returns a marker")
        };
        let initial_position = self
            .get_buffer_by_id(buffer_id)
            .map(|buffer| buffer.point_max());
        self.set_marker(mark_marker_id, initial_position, Some(buffer_id))?;
        self.process_states.push(ProcessState {
            record_id,
            kind: ProcessKind::Serial,
            buffer_id: Some(buffer_id),
            mark_marker_id,
            status: ProcessStatus::Open,
            filter,
            sentinel,
            sentinel_notified: false,
            log: None,
            name,
            thread_id: Some(self.active_thread_id),
            query_on_exit_flag,
            traffic_stopped: stopped,
            inherit_coding_system_flag: false,
            decoding,
            encoding,
            program: None,
            argv: Vec::new(),
            stderr_process_id: None,
            exit_code: None,
            exit_signal: None,
            os_pid: None,
            runtime: None,
            network: None,
            serial: Some(serial),
            contact_host: None,
            contact_service: None,
            remote: None,
            parent_server_id: None,
            pending_stdout: Vec::new(),
            pending_stderr: Vec::new(),
            output_delivery_count: 0,
            plist,
            gnutls: ProcessGnuTlsState::default(),
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

    pub fn process_os_id(&self, record_id: u64) -> Option<u32> {
        self.find_process_state(record_id)
            .and_then(|process| process.os_pid)
    }

    pub(crate) fn process_output_delivery_count(&self, record_id: u64) -> Option<u64> {
        self.find_process_state(record_id)
            .map(|process| process.output_delivery_count)
    }

    pub(crate) fn note_process_output_delivery(&mut self, record_id: u64) {
        if let Some(process) = self.find_process_state_mut(record_id) {
            process.output_delivery_count = process.output_delivery_count.saturating_add(1);
        }
    }

    /// GNU `process-command': child processes expose the program followed by
    /// its argument vector; pipe and network process records have no command.
    pub fn process_command_value(&self, record_id: u64) -> Option<Value> {
        self.find_process_state(record_id).map(|process| {
            let Some(program) = process.program.as_ref() else {
                return if process.traffic_stopped {
                    Value::T
                } else {
                    Value::Nil
                };
            };
            Value::list(
                std::iter::once(program)
                    .chain(process.argv.iter())
                    .cloned()
                    .map(|value| Value::String(value.into())),
            )
        })
    }

    pub fn find_process_id_by_name(&self, name: &str) -> Option<u64> {
        self.process_states
            .iter()
            .rev()
            .find(|process| process.name == name && Self::in_process_alist(process))
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

    /// GNU exposes the native default sentinel as an ordinary function
    /// object.  `None' is only Emaxx's optimized internal representation.
    pub fn process_sentinel_value(&self, record_id: u64) -> Option<Value> {
        self.find_process_state(record_id).map(|process| {
            process
                .sentinel
                .clone()
                .unwrap_or_else(|| Value::symbol("internal-default-process-sentinel"))
        })
    }

    pub fn set_process_sentinel(&mut self, record_id: u64, sentinel: Option<Value>) -> bool {
        let Some(process) = self.find_process_state_mut(record_id) else {
            return false;
        };
        process.sentinel = sentinel;
        true
    }

    pub fn set_process_stderr(&mut self, record_id: u64, stderr_process_id: Option<u64>) -> bool {
        let Some(process) = self.find_process_state_mut(record_id) else {
            return false;
        };
        process.stderr_process_id = stderr_process_id;
        true
    }

    pub fn process_stderr(&self, record_id: u64) -> Option<u64> {
        self.find_process_state(record_id)
            .and_then(|process| process.stderr_process_id)
    }

    /// Return child/pipe exit events that still need their one terminal
    /// sentinel call.  GNU prepends every process to `Vprocess_alist' and
    /// `status_notify' walks that alist in order, so a newer child is
    /// notified before the older pipe supplied through its `:stderr' option.
    pub fn take_pending_subprocess_exit_events(&mut self) -> Vec<(u64, String)> {
        let active_thread_id = self.active_thread_id;
        let completed_children = self
            .process_states
            .iter()
            .filter(|process| {
                matches!(process.kind, ProcessKind::Real | ProcessKind::Pipe)
                    && matches!(process.status, ProcessStatus::Exit | ProcessStatus::Signal)
                    && process.stderr_process_id.is_some()
                    && process
                        .thread_id
                        .is_none_or(|thread_id| thread_id == active_thread_id)
            })
            .filter_map(|process| {
                process
                    .stderr_process_id
                    .map(|stderr_id| (stderr_id, process.exit_code))
            })
            .collect::<Vec<_>>();
        for (stderr_id, exit_code) in completed_children {
            if let Some(stderr) = self.find_process_state_mut(stderr_id)
                && stderr.status.is_live()
            {
                stderr.status = ProcessStatus::Exit;
                stderr.exit_code = exit_code;
            }
        }

        let mut events = Vec::new();
        // `process_states' is append-only creation order.  Reverse iteration
        // mirrors GNU's newest-first `Vprocess_alist' status traversal.
        for process in self.process_states.iter_mut().rev() {
            if !matches!(process.kind, ProcessKind::Real | ProcessKind::Pipe)
                || !matches!(process.status, ProcessStatus::Exit | ProcessStatus::Signal)
                || process.sentinel_notified
                || process
                    .thread_id
                    .is_some_and(|thread_id| thread_id != active_thread_id)
            {
                continue;
            }
            process.sentinel_notified = true;
            let event = match process.exit_code {
                _ if process.exit_signal.is_some() => {
                    signal_event_message(process.exit_signal.expect("checked signal"))
                }
                Some(0) => "finished\n".to_string(),
                Some(code) => format!("exited abnormally with code {code}\n"),
                None => "killed\n".to_string(),
            };
            events.push((process.record_id, event));
        }
        events
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
            .is_some_and(|process| process.kind == ProcessKind::Network)
    }

    pub fn is_serial_process(&self, record_id: u64) -> bool {
        self.find_process_state(record_id)
            .is_some_and(|process| process.kind == ProcessKind::Serial)
    }

    pub fn set_process_contact_plist(
        &mut self,
        record_id: u64,
        contact: Value,
    ) -> Result<(), LispError> {
        let process = self
            .find_process_state_mut(record_id)
            .ok_or_else(|| wrong_type_argument("processp", Value::Record(record_id)))?;
        process.contact = contact;
        Ok(())
    }

    #[cfg(unix)]
    pub fn serial_process_fd(&self, record_id: u64) -> Result<std::os::fd::RawFd, LispError> {
        use std::os::fd::AsRawFd;

        let process = self
            .find_process_state(record_id)
            .ok_or_else(|| wrong_type_argument("processp", Value::Record(record_id)))?;
        let Some(serial) = process.serial.as_ref() else {
            return Err(LispError::Signal("Not a serial process".into()));
        };
        Ok(serial.port.as_raw_fd())
    }

    pub fn process_datagram_address(
        &self,
        record_id: u64,
    ) -> Result<Option<std::net::SocketAddr>, LispError> {
        let process = self
            .find_process_state(record_id)
            .ok_or_else(|| wrong_type_argument("processp", Value::Record(record_id)))?;
        let Some(NetworkRuntime::Datagram { socket, remote }) = process.network.as_ref() else {
            return Ok(None);
        };
        Ok(Some(remote.unwrap_or_else(|| {
            socket
                .local_addr()
                .map(|address| match address {
                    std::net::SocketAddr::V4(_) => std::net::SocketAddr::from(([0, 0, 0, 0], 0)),
                    std::net::SocketAddr::V6(_) => {
                        std::net::SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 0], 0))
                    }
                })
                .unwrap_or_else(|_| std::net::SocketAddr::from(([0, 0, 0, 0], 0)))
        })))
    }

    pub fn set_process_datagram_address(
        &mut self,
        record_id: u64,
        address: std::net::SocketAddr,
    ) -> Result<bool, LispError> {
        let process = self
            .find_process_state_mut(record_id)
            .ok_or_else(|| wrong_type_argument("processp", Value::Record(record_id)))?;
        let Some(NetworkRuntime::Datagram { socket, remote }) = process.network.as_mut() else {
            return Ok(false);
        };
        let same_family = socket
            .local_addr()
            .is_ok_and(|local| local.is_ipv4() == address.is_ipv4());
        if !same_family {
            return Ok(false);
        }
        *remote = Some(address);
        Ok(true)
    }

    pub fn network_listener_ids(&self) -> Vec<u64> {
        self.process_states
            .iter()
            .filter(|process| {
                matches!(
                    process.network,
                    Some(NetworkRuntime::Listener(_)) | Some(NetworkRuntime::UnixListener(_))
                ) && process.status.is_live()
                    && !process.traffic_stopped
                    && process
                        .thread_id
                        .is_none_or(|thread_id| thread_id == self.active_thread_id)
            })
            .map(|process| process.record_id)
            .collect()
    }

    pub fn connection_stream_ids(&self) -> Vec<u64> {
        self.process_states
            .iter()
            .filter(|process| {
                (matches!(
                    process.network,
                    Some(NetworkRuntime::Stream(_))
                        | Some(NetworkRuntime::Datagram { .. })
                        | Some(NetworkRuntime::UnixStream(_))
                ) || process.serial.is_some())
                    && process.status.is_live()
                    && process
                        .thread_id
                        .is_none_or(|thread_id| thread_id == self.active_thread_id)
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

    /// Non-blocking read of a network or serial connection. Returns the bytes
    /// read and whether the peer/device closed the connection.
    pub fn poll_connection_stream(&mut self, record_id: u64) -> Result<(Vec<u8>, bool), LispError> {
        let Some(process) = self.find_process_state_mut(record_id) else {
            return Ok((Vec::new(), false));
        };
        let (out, closed) = if let Some(session) = process.gnutls.session.as_mut() {
            session.receive()?
        } else {
            match process.network.as_mut() {
                Some(NetworkRuntime::Stream(stream)) => drain_nonblocking(stream),
                Some(NetworkRuntime::UnixStream(stream)) => drain_nonblocking(stream),
                Some(NetworkRuntime::Datagram { socket, remote }) => {
                    let mut bytes = vec![0; 65_535];
                    match socket.recv_from(&mut bytes) {
                        Ok((length, peer)) => {
                            bytes.truncate(length);
                            *remote = Some(peer);
                            (bytes, false)
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            (Vec::new(), false)
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {
                            (Vec::new(), false)
                        }
                        Err(_) => (Vec::new(), false),
                    }
                }
                _ => {
                    let Some(serial) = process.serial.as_mut() else {
                        return Ok((Vec::new(), false));
                    };
                    let mut bytes = vec![0; 65_535];
                    match serial.port.read(&mut bytes) {
                        Ok(0) => (Vec::new(), true),
                        Ok(length) => {
                            bytes.truncate(length);
                            (bytes, false)
                        }
                        Err(error)
                            if matches!(
                                error.kind(),
                                ErrorKind::WouldBlock
                                    | ErrorKind::TimedOut
                                    | ErrorKind::Interrupted
                            ) =>
                        {
                            (Vec::new(), false)
                        }
                        Err(_) => (Vec::new(), true),
                    }
                }
            }
        };
        if closed {
            process.gnutls.session = None;
            process.gnutls.active = false;
            process.gnutls.peer_status = Value::Nil;
            process.status = ProcessStatus::Closed;
            process.network = None;
            process.serial = None;
        }
        Ok((out, closed))
    }

    pub fn connection_send(&mut self, record_id: u64, input: &[u8]) -> Result<(), LispError> {
        let Some(process) = self.find_process_state_mut(record_id) else {
            return Err(wrong_type_argument("processp", Value::Record(record_id)));
        };
        if let Some(session) = process.gnutls.session.as_mut() {
            return session.send_all(input);
        }
        match process.network.as_mut() {
            Some(NetworkRuntime::Stream(stream)) => send_all(stream, input),
            Some(NetworkRuntime::UnixStream(stream)) => send_all(stream, input),
            Some(NetworkRuntime::Datagram {
                socket,
                remote: Some(remote),
            }) => socket
                .send_to(input, *remote)
                .map(|_| ())
                .map_err(|error| LispError::Signal(error.to_string())),
            Some(NetworkRuntime::Datagram { remote: None, .. }) => {
                Err(LispError::Signal("Datagram address is not set".into()))
            }
            _ => {
                let Some(serial) = process.serial.as_mut() else {
                    return Err(LispError::Signal(
                        "Process has no writable connection".into(),
                    ));
                };
                serial
                    .port
                    .write_all(input)
                    .map_err(|error| LispError::Signal(error.to_string()))
            }
        }
    }

    pub(super) fn refresh_process_state(process: &mut ProcessState) -> Result<(), LispError> {
        if !process.status.is_live() {
            return Ok(());
        }
        let Some(runtime) = process.runtime.as_mut() else {
            return Ok(());
        };
        if let Some(event) = poll_child_status(&mut runtime.child)
            .map_err(|error| LispError::Signal(error.to_string()))?
        {
            match event {
                ChildStatusEvent::Stopped => process.status = ProcessStatus::Stop,
                ChildStatusEvent::Continued => process.status = ProcessStatus::Run,
                ChildStatusEvent::Exited(status) => {
                    // Drain whatever the child wrote before exiting so the
                    // next pump still delivers it to the filter (gpg's final
                    // status lines arrive after `process-status' notices the
                    // exit).
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
                    if let Some(pty) = runtime.pty_output.as_mut() {
                        let mut tail = Vec::new();
                        let _ = read_nonblocking_pipe(pty, &mut tail);
                        process.pending_stdout.extend(tail);
                    }
                    drop(runtime.pty_slave_guard.take());
                    record_child_exit(process, status);
                    process.runtime = None;
                }
            }
        }
        Ok(())
    }

    pub fn process_value_for_buffer(&mut self, buffer_id: u64) -> Option<Value> {
        self.process_states.iter_mut().rev().find_map(|process| {
            let _ = Self::refresh_process_state(process);
            (process.buffer_id == Some(buffer_id) && Self::in_process_alist(process))
                .then_some(Value::Record(process.record_id))
        })
    }

    pub(crate) fn refresh_process_id(&mut self, record_id: u64) -> Result<(), LispError> {
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
        self.find_process_state(record_id).map(|process| {
            if process.traffic_stopped {
                Value::symbol("stop")
            } else if process.kind == ProcessKind::Pipe {
                Value::symbol(match process.status {
                    ProcessStatus::Run => "open",
                    ProcessStatus::Exit => "closed",
                    _ => process.status.symbol(),
                })
            } else {
                Value::Symbol(process.status.symbol().into())
            }
        })
    }

    pub fn process_exit_status_value(&mut self, record_id: u64) -> Option<Value> {
        let _ = self.refresh_process_id(record_id);
        self.find_process_state(record_id)
            .map(|process| Value::Integer(process.exit_code.unwrap_or(0) as i64))
    }

    pub(crate) fn mark_network_process_connecting(&mut self, record_id: u64) -> bool {
        let Some(process) = self.find_process_state_mut(record_id) else {
            return false;
        };
        process.status = ProcessStatus::Connect;
        true
    }

    pub(crate) fn connecting_network_processes(&self) -> Vec<u64> {
        let active_thread_id = self.active_thread_id;
        self.process_states
            .iter()
            .filter(|process| {
                process.status == ProcessStatus::Connect
                    && process.network.is_some()
                    && process
                        .thread_id
                        .is_none_or(|thread_id| thread_id == active_thread_id)
            })
            .map(|process| process.record_id)
            .collect()
    }

    pub(crate) fn mark_network_process_open(&mut self, record_id: u64) -> bool {
        let Some(process) = self.find_process_state_mut(record_id) else {
            return false;
        };
        process.status = ProcessStatus::Open;
        true
    }

    pub(crate) fn mark_network_process_failed(&mut self, record_id: u64) -> bool {
        let Some(process) = self.find_process_state_mut(record_id) else {
            return false;
        };
        process.status = ProcessStatus::Failed;
        true
    }

    pub fn process_is_live(&self, record_id: u64) -> bool {
        // GNU `process-live-p' observes the status last delivered by the
        // process event loop; it does not reap a fast child itself.  Eager
        // refresh here can make Eshell skip deferral before the sentinel has
        // closed handles and resumed the command.
        self.find_process_state(record_id)
            .is_some_and(|process| process.status.is_live())
    }

    pub fn process_filter(&self, record_id: u64) -> Option<Value> {
        self.find_process_state(record_id)
            .and_then(|process| process.filter.clone())
    }

    /// GNU exposes the native default filter as an ordinary function object.
    /// `None' remains the direct Rust fast path inside the event pump.
    pub fn process_filter_value(&self, record_id: u64) -> Option<Value> {
        self.find_process_state(record_id).map(|process| {
            process
                .filter
                .clone()
                .unwrap_or_else(|| Value::symbol("internal-default-process-filter"))
        })
    }

    pub fn process_output_paused(&self, record_id: u64) -> bool {
        self.find_process_state(record_id).is_some_and(|process| {
            process.traffic_stopped
                || process
                    .filter
                    .as_ref()
                    .is_some_and(|filter| *filter == Value::T)
        })
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
        process.query_on_exit_flag = flag;
        Ok(())
    }

    pub fn process_query_on_exit_flag(&self, record_id: u64) -> Result<bool, LispError> {
        self.find_process_state(record_id)
            .map(|process| process.query_on_exit_flag)
            .ok_or_else(|| wrong_type_argument("processp", Value::Record(record_id)))
    }

    pub fn set_process_inherit_coding_system_flag(
        &mut self,
        record_id: u64,
        flag: bool,
    ) -> Result<(), LispError> {
        let process = self
            .find_process_state_mut(record_id)
            .ok_or_else(|| wrong_type_argument("processp", Value::Record(record_id)))?;
        process.inherit_coding_system_flag = flag;
        Ok(())
    }

    pub fn process_inherit_coding_system_flag(&self, record_id: u64) -> Result<bool, LispError> {
        self.find_process_state(record_id)
            .map(|process| process.inherit_coding_system_flag)
            .ok_or_else(|| wrong_type_argument("processp", Value::Record(record_id)))
    }

    pub fn process_type_name(&self, record_id: u64) -> Result<&'static str, LispError> {
        let process = self
            .find_process_state(record_id)
            .ok_or_else(|| wrong_type_argument("processp", Value::Record(record_id)))?;
        Ok(match process.kind {
            ProcessKind::Real => "real",
            ProcessKind::Pipe => "pipe",
            ProcessKind::Network => "network",
            ProcessKind::Serial => "serial",
        })
    }

    pub fn set_process_window_size(
        &mut self,
        record_id: u64,
        height: u16,
        width: u16,
    ) -> Result<bool, LispError> {
        let process = self
            .find_process_state_mut(record_id)
            .ok_or_else(|| wrong_type_argument("processp", Value::Record(record_id)))?;
        if process.kind != ProcessKind::Real {
            return Ok(false);
        }
        let Some(runtime) = process.runtime.as_ref() else {
            return Ok(false);
        };
        let Some(pty) = runtime.pty_input.as_ref() else {
            return Ok(false);
        };
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;

            let size = libc::winsize {
                ws_row: height,
                ws_col: width,
                ws_xpixel: 0,
                ws_ypixel: 0,
            };
            // SAFETY: the PTY descriptor stays live through this call, and
            // TIOCSWINSZ only reads the supplied initialized structure.
            Ok(unsafe { libc::ioctl(pty.as_raw_fd(), libc::TIOCSWINSZ, &size) } == 0)
        }
        #[cfg(not(unix))]
        {
            let _ = (pty, height, width);
            Ok(false)
        }
    }

    pub fn process_running_child_value(&self, record_id: u64) -> Result<Value, LispError> {
        let process = self
            .find_process_state(record_id)
            .ok_or_else(|| wrong_type_argument("processp", Value::Record(record_id)))?;
        if process.kind != ProcessKind::Real {
            return Err(LispError::Signal(format!(
                "Process {} is not a subprocess",
                process.name
            )));
        }
        let Some(runtime) = process.runtime.as_ref() else {
            return Err(LispError::Signal(format!(
                "Process {} is not active",
                process.name
            )));
        };
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;

            let input_fd = runtime
                .pty_input
                .as_ref()
                .map(AsRawFd::as_raw_fd)
                .or_else(|| runtime.child.stdin.as_ref().map(AsRawFd::as_raw_fd));
            let Some(input_fd) = input_fd else {
                return Err(LispError::Signal(format!(
                    "Process {} is not active",
                    process.name
                )));
            };
            let mut foreground_group: libc::pid_t = -1;
            // SAFETY: input_fd is a live process input descriptor and the
            // ioctl writes one pid_t into initialized caller-owned storage.
            if unsafe {
                libc::ioctl(
                    input_fd,
                    libc::TIOCGPGRP,
                    &mut foreground_group as *mut libc::pid_t,
                )
            } == 0
            {
                if process
                    .os_pid
                    .is_some_and(|pid| pid == foreground_group as u32)
                {
                    return Ok(Value::Nil);
                }
                if foreground_group >= 0 {
                    return Ok(Value::Integer(i64::from(foreground_group)));
                }
            }
            Ok(Value::T)
        }
        #[cfg(not(unix))]
        {
            let _ = runtime;
            Ok(Value::T)
        }
    }

    pub fn set_process_traffic_stopped(
        &mut self,
        record_id: u64,
        stopped: bool,
    ) -> Result<bool, LispError> {
        let process = self
            .find_process_state_mut(record_id)
            .ok_or_else(|| wrong_type_argument("processp", Value::Record(record_id)))?;
        if process.kind == ProcessKind::Real {
            return Ok(false);
        }
        process.traffic_stopped = stopped;
        Ok(true)
    }

    #[cfg(unix)]
    pub fn signal_process_group(
        &mut self,
        record_id: u64,
        signal: i32,
        current_group: &Value,
    ) -> Result<(), LispError> {
        use std::os::fd::AsRawFd;

        let process = self
            .find_process_state(record_id)
            .ok_or_else(|| wrong_type_argument("processp", Value::Record(record_id)))?;
        if process.kind != ProcessKind::Real {
            return Err(LispError::Signal(format!(
                "Process {} is not a subprocess",
                process.name
            )));
        }
        let Some(runtime) = process.runtime.as_ref() else {
            return Err(LispError::Signal(format!(
                "Process {} is not active",
                process.name
            )));
        };
        let pid = process
            .os_pid
            .ok_or_else(|| LispError::Signal(format!("Process {} is not active", process.name)))?
            as libc::pid_t;
        let uses_input_pty = runtime.pty_input.is_some();
        let mut group = pid;
        if uses_input_pty && current_group.is_truthy() {
            if let Some(input) = runtime.pty_input.as_ref() {
                let mut foreground_group: libc::pid_t = -1;
                // SAFETY: input is a live PTY descriptor and ioctl writes one
                // pid_t into initialized caller-owned storage.
                if unsafe {
                    libc::ioctl(
                        input.as_raw_fd(),
                        libc::TIOCGPGRP,
                        &mut foreground_group as *mut libc::pid_t,
                    )
                } == 0
                    && foreground_group >= 0
                {
                    group = foreground_group;
                }
            }
            if matches!(current_group, Value::Symbol(symbol) if symbol == "lambda") && group == pid
            {
                return Ok(());
            }
        }
        // SAFETY: negative GROUP addresses the child-owned process group;
        // spawn_persistent_process establishes that group before exec.
        unsafe {
            libc::kill(-group, signal);
        }
        if signal == libc::SIGCONT
            && let Some(process) = self.find_process_state_mut(record_id)
        {
            // GNU publishes `run' before the kernel's WCONTINUED event.
            process.status = ProcessStatus::Run;
        }
        Ok(())
    }

    pub fn delete_process(&mut self, record_id: u64) -> Result<(&'static str, bool), LispError> {
        let process = self
            .find_process_state_mut(record_id)
            .ok_or_else(|| wrong_type_argument("processp", Value::Record(record_id)))?;
        let kind = process.kind;
        // `status_notify' claims a terminal transition before invoking its
        // sentinel.  A sentinel such as `compilation-sentinel' may then call
        // `delete-process' on itself; GNU removes the process from the
        // notification registry first, so that nested deletion must not
        // invoke the same sentinel recursively.
        let notify_sentinel = !process.sentinel_notified;
        // GNU clears the connection flow-control marker while closing the
        // descriptor, so a stopped connection reports `closed' afterwards.
        process.traffic_stopped = false;
        process.gnutls.session = None;
        process.gnutls.active = false;
        process.gnutls.peer_status = Value::Nil;
        if let Some(runtime) = process.runtime.take() {
            terminate_child_without_blocking(runtime);
        }
        if let Some(network) = process.network.take() {
            match &network {
                NetworkRuntime::Stream(stream) => {
                    let _ = stream.shutdown(std::net::Shutdown::Both);
                }
                NetworkRuntime::UnixStream(stream) => {
                    let _ = stream.shutdown(std::net::Shutdown::Both);
                }
                NetworkRuntime::Datagram { .. } => {}
                // GNU leaves a unix listener's socket file behind; the
                // tests delete it themselves.
                NetworkRuntime::Listener(_) | NetworkRuntime::UnixListener(_) => {}
            }
            process.status = ProcessStatus::Closed;
            process.runtime = None;
            process.sentinel_notified = true;
            return Ok(("deleted\n", notify_sentinel));
        }
        if process.serial.take().is_some() {
            process.status = ProcessStatus::Closed;
            process.runtime = None;
            process.sentinel_notified = true;
            return Ok(("deleted\n", notify_sentinel));
        }
        if kind == ProcessKind::Pipe {
            process.status = ProcessStatus::Closed;
            process.exit_code = Some(0);
            process.exit_signal = None;
        } else {
            process.status = ProcessStatus::Signal;
            #[cfg(unix)]
            {
                process.exit_code = Some(libc::SIGKILL);
                process.exit_signal = Some(libc::SIGKILL);
            }
            #[cfg(not(unix))]
            {
                process.exit_code = None;
                process.exit_signal = None;
            }
        }
        process.runtime = None;
        process.sentinel_notified = true;
        Ok((
            if kind == ProcessKind::Pipe {
                "finished\n"
            } else {
                "killed\n"
            },
            notify_sentinel,
        ))
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
        if matches!(process.kind, ProcessKind::Network | ProcessKind::Serial) {
            self.connection_send(record_id, input)?;
            return Ok((Vec::new(), Vec::new()));
        }
        // GNU's send_process blocks until the pty or pipe accepts the bytes,
        // accepting pending process output while it waits so a full write
        // buffer cannot deadlock against an unread child.  A nonblocking fd
        // therefore retries on EAGAIN instead of surfacing "Resource
        // temporarily unavailable" to Lisp.
        let mut pumped_stdout = Vec::new();
        let mut pumped_stderr = Vec::new();
        let mut offset = 0usize;
        while offset < input.len() {
            let write_result = {
                let process = self
                    .find_process_state_mut(record_id)
                    .ok_or_else(|| wrong_type_argument("processp", Value::Record(record_id)))?;
                let Some(runtime) = process.runtime.as_mut() else {
                    return Ok((input[offset..].to_vec(), Vec::new()));
                };
                let stdin: &mut dyn Write = if let Some(pty) = runtime.pty_input.as_mut() {
                    pty
                } else if let Some(stdin) = runtime.child.stdin.as_mut() {
                    stdin
                } else {
                    return Err(LispError::Signal("Process stdin is closed".into()));
                };
                stdin.write(&input[offset..])
            };
            match write_result {
                Ok(0) => return Err(LispError::Signal("Process stdin is closed".into())),
                Ok(written) => {
                    if std::env::var_os("EMAXX_DEBUG_PROCESS_IO").is_some() {
                        eprintln!(
                            "PROC-IO send#{record_id} wrote {written} of {}: {:?}",
                            input.len(),
                            String::from_utf8_lossy(
                                &input[offset..(offset + written).min(offset + 120)]
                            )
                        );
                    }
                    offset += written;
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    let (out, err) = self.poll_process_output(record_id)?;
                    pumped_stdout.extend_from_slice(&out);
                    pumped_stderr.extend_from_slice(&err);
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                Err(error) => return Err(LispError::Signal(error.to_string())),
            }
        }
        {
            let process = self
                .find_process_state_mut(record_id)
                .ok_or_else(|| wrong_type_argument("processp", Value::Record(record_id)))?;
            if let Some(runtime) = process.runtime.as_mut() {
                let stdin: Option<&mut dyn Write> = if let Some(pty) = runtime.pty_input.as_mut() {
                    Some(pty)
                } else if let Some(stdin) = runtime.child.stdin.as_mut() {
                    Some(stdin)
                } else {
                    None
                };
                if let Some(stdin) = stdin
                    && let Err(error) = stdin.flush()
                    && !matches!(
                        error.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
                    )
                {
                    return Err(LispError::Signal(error.to_string()));
                }
            }
        }
        // GNU queues the bytes and returns; filters run when the event loop
        // later accepts process output.  Only output drained while waiting
        // for a full pipe is handed back for delivery here.
        Ok((pumped_stdout, pumped_stderr))
    }

    /// process.c Fprocess_tty_name: the pty device name, nil for pipes.
    /// STREAM selects which half must be a pty (stdin/stdout); nil accepts
    /// either.
    /// Any spawned thread the scheduler could still advance: runnable now,
    /// or parked in a sleep that a wake pass will finish.
    pub(crate) fn has_advanceable_spawned_thread(&self) -> bool {
        self.thread_states.iter().any(|thread| {
            thread.record_id != self.main_thread_id
                && matches!(
                    thread.status,
                    ThreadStatus::Runnable | ThreadStatus::Blocked(ThreadBlocker::Sleep)
                )
        })
    }

    pub(crate) fn current_thread_is_main(&self) -> bool {
        self.active_thread_id == self.main_thread_id
    }

    pub(crate) fn note_stepped_yield(&mut self) {
        self.fruitless_stepped_yields = self.fruitless_stepped_yields.saturating_add(1);
        if std::env::var_os("EMAXX_DEBUG_YIELD").is_some()
            && self.fruitless_stepped_yields % 10_000 == 0
        {
            eprintln!("YIELD-GUARD count={}", self.fruitless_stepped_yields);
        }
    }

    pub(crate) fn reset_stepped_yields(&mut self) {
        self.fruitless_stepped_yields = 0;
    }

    pub(crate) fn stepped_yield_exhausted(&self) -> bool {
        // Generous: real cooperative handoffs reset this in drive_threads
        // whenever another thread actually runs.
        self.fruitless_stepped_yields > 10_000
    }

    pub(crate) fn process_tty_name(
        &self,
        record_id: u64,
        stream: Option<&Value>,
    ) -> Option<String> {
        let process = self.find_process_state(record_id)?;
        let runtime = process.runtime.as_ref()?;
        let stream_matches = match stream.and_then(|value| value.as_symbol().ok()) {
            Some("stdin") => runtime.pty_input.is_some(),
            Some("stdout") | Some("stderr") => runtime.pty_output.is_some(),
            _ => runtime.pty_input.is_some() || runtime.pty_output.is_some(),
        };
        if stream_matches {
            runtime.pty_slave_name.clone()
        } else {
            None
        }
    }

    pub fn process_send_eof(&mut self, record_id: u64) -> Result<(Vec<u8>, Vec<u8>), LispError> {
        // GNU reaps child status in its event loop.  Polling again here can
        // invalidate Eshell's immediately preceding `process-live-p' check
        // and turn normal pipeline teardown into a spurious race error.
        let process = self
            .find_process_state_mut(record_id)
            .ok_or_else(|| wrong_type_argument("processp", Value::Record(record_id)))?;
        if !process.status.is_live() {
            return Err(LispError::Signal("Process is not running".into()));
        }
        let Some(runtime) = process.runtime.as_mut() else {
            return Ok((Vec::new(), Vec::new()));
        };
        // A PTY uses canonical Ctrl-D for EOF (as GNU's child_setup_tty
        // configures it).  Keep the master alive until the child consumes
        // the queued input: closing an input-only PTY here can deliver SIGHUP
        // before the pipeline head has forwarded its final output.
        if let Some(pty) = runtime.pty_input.as_mut() {
            pty.write_all(&[4])
                .map_err(|error| LispError::Signal(error.to_string()))?;
            pty.flush()
                .map_err(|error| LispError::Signal(error.to_string()))?;
        }
        drop(runtime.child.stdin.take());
        // GNU only makes EOF visible after already queued input and returns.
        // Output remains owned by the normal event pump, which preserves
        // filter ordering and avoids an arbitrary synchronous drain timeout.
        Ok((Vec::new(), Vec::new()))
    }

    pub fn live_external_process_ids(&self) -> Vec<u64> {
        self.process_states
            .iter()
            .filter(|process| {
                process.runtime.is_some()
                    || !process.pending_stdout.is_empty()
                    || !process.pending_stderr.is_empty()
            })
            .filter(|process| {
                process
                    .thread_id
                    .is_none_or(|thread_id| thread_id == self.active_thread_id)
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
        let before = (stdout.len(), stderr.len());
        if let Some(pipe) = runtime.child.stdout.as_mut() {
            read_nonblocking_pipe(pipe, &mut stdout)?;
        }
        if let Some(pipe) = runtime.child.stderr.as_mut() {
            read_nonblocking_pipe(pipe, &mut stderr)?;
        }
        if let Some(pty) = runtime.pty_output.as_mut() {
            read_nonblocking_pipe(pty, &mut stdout)?;
        }
        if std::env::var_os("EMAXX_DEBUG_PROCESS_IO").is_some()
            && (stdout.len() > before.0 || stderr.len() > before.1)
        {
            eprintln!(
                "PROC-IO poll#{record_id} +out {} +err {}: {:?}",
                stdout.len() - before.0,
                stderr.len() - before.1,
                String::from_utf8_lossy(&stdout[before.0..stdout.len().min(before.0 + 120)])
            );
        }
        if let Some(event) = poll_child_status(&mut runtime.child)
            .map_err(|error| LispError::Signal(error.to_string()))?
        {
            match event {
                ChildStatusEvent::Stopped => process.status = ProcessStatus::Stop,
                ChildStatusEvent::Continued => process.status = ProcessStatus::Run,
                ChildStatusEvent::Exited(status) => {
                    // The child can exit between the non-blocking reads above
                    // and status polling.  Drain once more after observing
                    // exit: all pipe writers are closed now, so this owns the
                    // final bytes before dropping the runtime.
                    if let Some(pipe) = runtime.child.stdout.as_mut() {
                        read_nonblocking_pipe(pipe, &mut stdout)?;
                    }
                    if let Some(pipe) = runtime.child.stderr.as_mut() {
                        read_nonblocking_pipe(pipe, &mut stderr)?;
                    }
                    if let Some(pty) = runtime.pty_output.as_mut() {
                        read_nonblocking_pipe(pty, &mut stdout)?;
                    }
                    drop(runtime.pty_slave_guard.take());
                    record_child_exit(process, status);
                    process.runtime = None;
                }
            }
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

    /// Put the still-unfired tail of a due-timer batch back before timers
    /// scheduled by callbacks from that batch.  A timer callback can perform
    /// a nonlocal exit (`throw'); GNU leaves every other timer active, while
    /// dropping our detached batch here would silently cancel them.
    fn restore_unfired_timer_batch(&mut self, pending: impl Iterator<Item = ScheduledTimer>) {
        let mut unfired = pending.collect::<Vec<_>>();
        unfired.append(&mut self.pending_timers);
        self.pending_timers = unfired;
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
        // A kernel notification belongs only to watches that existed when
        // the filesystem operation occurred.  Resolving recipients later
        // would replay an old event to a newly registered watch for the same
        // path, which can tear down that new watch before its first real
        // event arrives.
        let callbacks = self.file_notify_callbacks_for_path(path);
        self.refresh_file_notify_fingerprints_for_path(path);
        self.pending_file_notifications
            .push(PendingFileNotification {
                path: path.to_string(),
                action: action.to_string(),
                callbacks,
            });
    }

    pub(crate) fn register_file_notify_watch(
        &mut self,
        descriptor: i64,
        path: Option<String>,
        callback: Value,
    ) {
        let fingerprint = path.as_deref().map(file_notify_fingerprint);
        self.file_notify_watches.insert(
            descriptor,
            FileNotifyWatch {
                path,
                callback: Self::stored_value(callback),
                active: true,
                fingerprint,
            },
        );
    }

    pub(crate) fn remove_file_notify_watch(&mut self, descriptor: i64) {
        self.file_notify_watches.remove(&descriptor);
    }

    pub(crate) fn file_notify_watch_is_active(&self, descriptor: i64) -> bool {
        self.file_notify_watches
            .get(&descriptor)
            .is_some_and(|watch| watch.active)
    }

    pub(crate) fn invalidate_file_notify_watches_for_path(&mut self, path: &str) {
        for watch in self.file_notify_watches.values_mut() {
            if watch.path.as_deref() == Some(path) {
                watch.active = false;
            }
        }
    }

    pub(crate) fn file_notify_callbacks_for_path(&self, path: &str) -> Vec<(i64, Value)> {
        self.file_notify_watches
            .iter()
            .filter_map(|(descriptor, watch)| {
                watch
                    .path
                    .as_deref()
                    .filter(|watched| file_notify_watch_covers(watched, path))
                    .map(|_| (*descriptor, watch.callback.clone()))
            })
            .collect()
    }

    fn refresh_file_notify_fingerprints_for_path(&mut self, event_path: &str) {
        for watch in self.file_notify_watches.values_mut() {
            let Some(watched_path) = watch.path.as_deref() else {
                continue;
            };
            if file_notify_watch_covers(watched_path, event_path) {
                watch.fingerprint = Some(file_notify_fingerprint(watched_path));
            }
        }
    }

    fn poll_external_file_notifications(&mut self) {
        let mut changed_paths = Vec::<(String, String)>::new();
        for watch in self.file_notify_watches.values_mut() {
            if !watch.active {
                continue;
            }
            let Some(path) = watch.path.as_deref() else {
                continue;
            };
            let current = file_notify_fingerprint(path);
            if watch.fingerprint.as_ref() == Some(&current) {
                continue;
            }
            watch.fingerprint = Some(current.clone());
            if !changed_paths
                .iter()
                .any(|(changed_path, _)| changed_path == path)
            {
                let action = match current {
                    FileNotifyFingerprint::Missing => "deleted",
                    FileNotifyFingerprint::Present { .. } => "changed",
                };
                changed_paths.push((path.to_string(), action.to_string()));
            }
        }

        for (path, action) in changed_paths {
            let callbacks = self.file_notify_callbacks_for_path(&path);
            self.pending_file_notifications
                .push(PendingFileNotification {
                    path,
                    action,
                    callbacks,
                });
        }
    }

    pub fn run_pending_file_notifications(&mut self, env: &mut Env) -> Result<(), LispError> {
        let pending = std::mem::take(&mut self.pending_file_notifications);
        for notification in pending {
            let outcome = primitives::deliver_file_notification(
                self,
                env,
                &notification.path,
                &notification.action,
                notification.callbacks,
            );
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

    fn run_pending_native_timers(&mut self, env: &mut Env) -> Result<(), LispError> {
        // Only timers whose scheduled time has arrived fire; the rest stay
        // queued (GNU never runs a timer before it is due).  Due timers
        // fire in schedule order.
        let now = std::time::Instant::now();
        let all = std::mem::take(&mut self.pending_timers);
        let (pending, not_yet): (Vec<_>, Vec<_>) = all
            .into_iter()
            .partition(|timer| timer.due.is_none_or(|due| due <= now));
        self.pending_timers = not_yet;
        let mut pending = pending.into_iter();
        while let Some(timer) = pending.next() {
            if let Some(repeat) = timer.repeat {
                self.pending_timers.push(ScheduledTimer {
                    function: timer.function.clone(),
                    original_name: timer.original_name.clone(),
                    args: timer.args.clone(),
                    due: Some(now + std::time::Duration::from_secs_f64(repeat)),
                    repeat: timer.repeat,
                });
            }
            self.begin_timer_callback();
            let outcome = self.call_function_value(
                timer.function,
                timer.original_name.as_deref(),
                &timer.args,
                env,
            );
            self.end_timer_callback();
            match outcome {
                Ok(_) => {}
                Err(error @ LispError::Throw(_, _)) => {
                    self.restore_unfired_timer_batch(pending);
                    return Err(error);
                }
                Err(error) => {
                    // `timer-event-handler' demotes timer errors to a message
                    // unless `debug-on-error' asks for the debugger.
                    if self
                        .lookup_var("debug-on-error", env)
                        .is_some_and(|value| value.is_truthy())
                    {
                        self.restore_unfired_timer_batch(pending);
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
                            Value::String(format!("Error running timer{label}: %S").into()),
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
            if self
                .call_function_value(
                    Value::Symbol("timerp".into()),
                    Some("timerp"),
                    std::slice::from_ref(&timer),
                    env,
                )?
                .is_nil()
            {
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
                self.begin_timer_callback();
                let outcome = self.call_function_value(
                    Value::Symbol("timer-event-handler".into()),
                    Some("timer-event-handler"),
                    std::slice::from_ref(&timer),
                    env,
                );
                self.end_timer_callback();
                outcome?;
            }
        }
        Ok(())
    }

    /// Pump every timer representation that can be live in an interpreter.
    ///
    /// Bootstrap calls use the native queue; after GNU timer.el loads, timer
    /// objects live in `timer-list`.  Keeping this as one event-loop operation
    /// prevents waits and recursive command loops from silently servicing
    /// only one side of that boundary.
    pub(crate) fn run_pending_timer_events(&mut self, env: &mut Env) -> Result<(), LispError> {
        self.run_pending_native_timers(env)?;
        self.run_due_elisp_timers(env)
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
        let value = self.create_pseudovector(RecordKind::Thread, "thread", Vec::new());
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
            waiting_for_user_input: false,
        });
        Ok(Value::Record(record_id))
    }

    pub fn waiting_for_user_input(&self) -> bool {
        self.find_thread_state(self.active_thread_id)
            .is_some_and(|thread| thread.waiting_for_user_input)
    }

    pub fn set_waiting_for_user_input(&mut self, waiting: bool) -> bool {
        let Some(thread) = self.find_thread_state_mut(self.active_thread_id) else {
            return false;
        };
        std::mem::replace(&mut thread.waiting_for_user_input, waiting)
    }

    pub fn process_thread_value(&self, record_id: u64) -> Result<Value, LispError> {
        let process = self
            .find_process_state(record_id)
            .ok_or_else(|| wrong_type_argument("processp", Value::Record(record_id)))?;
        Ok(process.thread_id.map(Value::Record).unwrap_or(Value::Nil))
    }

    pub fn set_process_thread_id(
        &mut self,
        record_id: u64,
        thread_id: Option<u64>,
    ) -> Result<(), LispError> {
        let process = self
            .find_process_state_mut(record_id)
            .ok_or_else(|| wrong_type_argument("processp", Value::Record(record_id)))?;
        process.thread_id = thread_id;
        Ok(())
    }

    pub fn ensure_process_owned_by_current_thread(&self, record_id: u64) -> Result<(), LispError> {
        let process = self
            .find_process_state(record_id)
            .ok_or_else(|| wrong_type_argument("processp", Value::Record(record_id)))?;
        let Some(thread_id) = process.thread_id else {
            return Ok(());
        };
        if thread_id == self.active_thread_id {
            return Ok(());
        }
        let owner = self
            .thread_name(thread_id)
            .unwrap_or_else(|| format!("#<thread id:{thread_id}>"));
        Err(LispError::Signal(format!(
            "Attempt to accept output from process {} locked to thread {owner}",
            process.name
        )))
    }

    fn unlock_processes_for_thread(&mut self, thread_id: u64) {
        for process in &mut self.process_states {
            if process.thread_id == Some(thread_id) {
                process.thread_id = None;
            }
        }
    }

    pub fn make_mutex(&mut self, name: Option<String>) -> Value {
        let value = self.create_pseudovector(RecordKind::Mutex, "mutex", Vec::new());
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
        let value = self.create_pseudovector(
            RecordKind::ConditionVariable,
            "condition-variable",
            Vec::new(),
        );
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
        // GNU thread.c walks the blocked thread's real backtrace.  Emaxx has
        // no per-thread backtrace capture yet, so report none rather than
        // synthesising frames.
        let _ = &thread.status;
        Vec::new()
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
        let entry_yield_count = self.fruitless_stepped_yields;
        // Never re-step the thread that is currently executing: it reached
        // this scheduler pass from inside its own body (thread-yield, a
        // blocking wait), and stepping it again would re-enter that body
        // from the top -- the recursion that wedged threads-let-binding.
        let thread_ids = self
            .thread_states
            .iter()
            .filter(|thread| {
                thread.record_id != self.main_thread_id
                    && thread.record_id != self.active_thread_id
            })
            .map(|thread| thread.record_id)
            .collect::<Vec<_>>();
        for thread_id in thread_ids {
            let status = self
                .find_thread_state(thread_id)
                .map(|thread| thread.status.clone())
                .unwrap_or(ThreadStatus::Finished);
            match status {
                ThreadStatus::Runnable => {
                    if thread_id != self.active_thread_id {
                        self.fruitless_stepped_yields = 0;
                    }
                    self.step_thread(thread_id, env)?;
                }
                ThreadStatus::Blocked(ThreadBlocker::Sleep) if wake_sleepers => {
                    self.finish_thread_success(thread_id, Value::Nil);
                }
                _ => {}
            }
        }
        let _ = entry_yield_count;
        if wake_sleepers {
            // Native file operations enqueue their own exact events.  This
            // metadata scan supplies the host-backend half of kqueue for
            // changes made by subprocesses or other processes.
            self.poll_external_file_notifications();
            self.run_pending_file_notifications(env)?;
            self.run_pending_timer_events(env)?;
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
        let thread_id = self.active_thread_id;
        if let Some(thread) = self.find_thread_state_mut(thread_id) {
            thread.status = ThreadStatus::Blocked(ThreadBlocker::Mutex(mutex_id));
        }
        while !self.try_lock_mutex(thread_id, mutex_id) {
            // This scheduler runs a spawned thread's whole body inside one
            // `step_thread' call from the owning (usually main) thread.  If
            // the mutex is held by a thread that is suspended up-stack
            // waiting for THIS step to return -- the main thread cannot be
            // driven by drive_threads at all -- no interleaving can ever
            // release it: spinning here burned CPU forever on GNU's
            // thread-tests.el, whose child locks a mutex its parent holds
            // across the child's entire lifetime.  GNU's preemptive threads
            // simply block and later resume; this model cannot, so the
            // honest degraded behavior is to signal the deadlock (the file
            // then completes with mismatching outcomes instead of hanging).
            // Disclosed in docs/honesty-audit-2026-08-18.md.
            let holder = self
                .find_mutex_state_mut(mutex_id)
                .and_then(|mutex| mutex.owner);
            if holder == Some(self.main_thread_id) && thread_id != self.main_thread_id {
                if let Some(thread) = self.find_thread_state_mut(thread_id) {
                    thread.status = ThreadStatus::Runnable;
                }
                return Err(LispError::Signal(
                    "Cooperative thread model deadlock: mutex is held by the suspended parent thread"
                        .into(),
                ));
            }
            if let Err(error) = self.drive_threads(env, false) {
                if let Some(thread) = self.find_thread_state_mut(thread_id) {
                    thread.status = ThreadStatus::Runnable;
                }
                return Err(error);
            }
        }
        if let Some(thread) = self.find_thread_state_mut(thread_id) {
            thread.status = ThreadStatus::Runnable;
        }
        Ok(Value::Nil)
    }

    pub fn unlock_mutex_for_current_thread(&mut self, mutex_id: u64) -> Result<Value, LispError> {
        self.unlock_mutex(self.active_thread_id, mutex_id);
        Ok(Value::Nil)
    }

    pub fn wait_condition_variable(
        &mut self,
        condvar_id: u64,
        env: &mut Env,
    ) -> Result<Value, LispError> {
        let mutex_id = self
            .condition_variable_mutex_id(condvar_id)
            .ok_or_else(|| {
                wrong_type_argument("condition-variable-p", Value::Record(condvar_id))
            })?;
        let thread_id = self.active_thread_id;
        let saved_depth = self
            .mutex_states
            .iter()
            .find(|mutex| mutex.record_id == mutex_id && mutex.owner == Some(thread_id))
            .map(|mutex| mutex.recursion_depth)
            .filter(|depth| *depth > 0)
            .ok_or_else(|| {
                // thread.c:499 spells this with an ASCII apostrophe; the
                // curl is applied only when the effective quoting style is
                // `curve'.
                LispError::Signal(format!(
                    "Condition variable{}s mutex is not held by current thread",
                    if crate::lisp::primitives::values::effective_text_quoting_style(self, env) == "curve" {
                    '\u{2019}'
                } else {
                    '\''
                }
                ))
            })?;
        if let Some(mutex) = self.find_mutex_state_mut(mutex_id) {
            mutex.owner = None;
            mutex.recursion_depth = 0;
        }
        if let Some(thread) = self.find_thread_state_mut(thread_id) {
            thread.status = ThreadStatus::Blocked(ThreadBlocker::ConditionVariable(condvar_id));
        }

        let wait_result = loop {
            if !self.find_thread_state(thread_id).is_some_and(|thread| {
                matches!(
                    thread.status,
                    ThreadStatus::Blocked(ThreadBlocker::ConditionVariable(id))
                        if id == condvar_id
                )
            }) {
                break Ok(());
            }
            // Same cooperative-model limit as the yield loop (finding 84):
            // a stepped thread waiting on a condvar only its suspended
            // parent can notify would spin here forever; GNU's preemptive
            // threads would block and get woken.  Signal instead.
            if thread_id != self.main_thread_id {
                self.note_stepped_yield();
                if self.stepped_yield_exhausted() {
                    self.reset_stepped_yields();
                    if let Some(thread) = self.find_thread_state_mut(thread_id) {
                        thread.status = ThreadStatus::Runnable;
                    }
                    while !self.try_lock_mutex(thread_id, mutex_id) {
                        self.drive_threads(env, false)?;
                    }
                    break Err(LispError::Signal(
                        "Cooperative thread model deadlock: condition variable can only be notified by the suspended parent thread".into(),
                    ));
                }
            }
            if let Err(error) = self.drive_threads(env, false) {
                break Err(error);
            }
        };

        while !self.try_lock_mutex(thread_id, mutex_id) {
            self.drive_threads(env, false)?;
        }
        for _ in 1..saved_depth {
            // Restoring recursive ownership is observable behavior, not a
            // debug-only invariant.  Keeping the state transition inside
            // `debug_assert!` made release builds silently restore depth one.
            let restored = self.try_lock_mutex(thread_id, mutex_id);
            debug_assert!(restored);
        }
        wait_result.map(|()| Value::Nil)
    }

    pub fn notify_condition_variable(
        &mut self,
        condvar_id: u64,
        notify_all: bool,
        env: &Env,
    ) -> Result<(), LispError> {
        let mutex_id = self
            .condition_variable_mutex_id(condvar_id)
            .ok_or_else(|| {
                wrong_type_argument("condition-variable-p", Value::Record(condvar_id))
            })?;
        if !self.mutex_states.iter().any(|mutex| {
            mutex.record_id == mutex_id
                && mutex.owner == Some(self.active_thread_id)
                && mutex.recursion_depth > 0
        }) {
            // thread.c:558, as above.
            return Err(LispError::Signal(format!(
                "Condition variable{}s mutex is not held by current thread",
                if crate::lisp::primitives::values::effective_text_quoting_style(self, env) == "curve" {
                    '\u{2019}'
                } else {
                    '\''
                }
            )));
        }
        for thread in self.thread_states.iter_mut() {
            if !matches!(
                thread.status,
                ThreadStatus::Blocked(ThreadBlocker::ConditionVariable(id)) if id == condvar_id
            ) {
                continue;
            }
            thread.status = ThreadStatus::Runnable;
            if !notify_all {
                break;
            }
        }
        Ok(())
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
        self.unlock_processes_for_thread(record_id);
    }

    pub(super) fn finish_thread_with_signal(&mut self, record_id: u64, value: Value) {
        if let Some(thread) = self.find_thread_state_mut(record_id) {
            thread.status = ThreadStatus::Finished;
            thread.outcome = Some(ThreadOutcome::Signaled(value.clone()));
        }
        self.unlock_processes_for_thread(record_id);
        self.last_thread_error = Some(value);
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
            ThreadProgram::Sleep { blocked } => {
                if !blocked && let Some(thread) = self.find_thread_state_mut(record_id) {
                    thread.program = ThreadProgram::Sleep { blocked: true };
                    thread.status = ThreadStatus::Blocked(ThreadBlocker::Sleep);
                }
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
            Value::Symbol(_) => Ok(ThreadProgram::Call(function.clone())),
            Value::BuiltinFunc(name) if name == "ignore" => Ok(ThreadProgram::Ignore),
            Value::BuiltinFunc(_) => Ok(ThreadProgram::Call(function.clone())),
            Value::Lambda(lambda) if lambda.params.is_empty() => self
                .thread_program_from_lambda(function_executable_body(&lambda.body))
                .or_else(|_| Ok(ThreadProgram::Call(function.clone()))),
            _ => Err(LispError::Signal("Unsupported thread entry point".into())),
        }
    }

    /// Classify a thread body by *shape* for the cooperative scheduler.
    ///
    /// Emaxx has no preemptive threads: `ThreadProgram::Call' runs a body to
    /// completion in one scheduler step, so only the shapes recognised here can
    /// interleave with the main thread at all.  This must stay shape-generic —
    /// never keyed to any function or variable name — and an unrecognised body
    /// falls through to `Call', which is honest about running without
    /// interleaving.  Genuine cooperative stepping of arbitrary bodies is a
    /// tracked architectural gap.
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

fn file_notify_watch_covers(watched_path: &str, event_path: &str) -> bool {
    let watched = watched_path.trim_end_matches('/');
    if watched == event_path.trim_end_matches('/') {
        return true;
    }
    event_path
        .rsplit_once('/')
        .is_some_and(|(parent, _)| parent == watched)
}

fn file_notify_fingerprint(path: &str) -> FileNotifyFingerprint {
    match fs::symlink_metadata(path) {
        Ok(metadata) => FileNotifyFingerprint::Present {
            modified: metadata.modified().ok(),
            len: metadata.len(),
            is_directory: metadata.is_dir(),
        },
        Err(_) => FileNotifyFingerprint::Missing,
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
            #[cfg(unix)]
            Err(error) if error.raw_os_error() == Some(libc::EIO) => break,
            Err(error) => return Err(LispError::Signal(error.to_string())),
        }
    }
    Ok(read_any)
}
