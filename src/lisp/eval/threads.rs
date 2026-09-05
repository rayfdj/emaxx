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

    pub(crate) fn current_thread_process_output_delivery_count(&self) -> u64 {
        self.process_states
            .iter()
            .filter(|process| {
                process
                    .thread_id
                    .is_none_or(|thread_id| thread_id == self.active_thread_id)
            })
            .fold(0_u64, |total, process| {
                total.saturating_add(process.output_delivery_count)
            })
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
    /// A real or pipe process that has exited and whose sentinel has not
    /// run yet: the "tick != update_tick" state process.c status_notify
    /// drains and notifies.
    pub fn process_exit_awaits_notification(&self, record_id: u64) -> bool {
        self.find_process_state(record_id).is_some_and(|process| {
            matches!(process.kind, ProcessKind::Real | ProcessKind::Pipe)
                && matches!(process.status, ProcessStatus::Exit | ProcessStatus::Signal)
                && !process.sentinel_notified
        })
    }

    pub fn take_pending_subprocess_exit_events(&mut self) -> Vec<(u64, String)> {
        self.take_pending_subprocess_exit_events_for(None)
    }

    /// Restricted form used by `accept-process-output's JUST-THIS-ONE mode.
    /// Status changes remain recorded on every process, but only the selected
    /// process may run its sentinel during that wait.
    pub fn take_pending_subprocess_exit_events_for(
        &mut self,
        only_process_id: Option<u64>,
    ) -> Vec<(u64, String)> {
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
                    && only_process_id.is_none_or(|id| {
                        process.record_id == id || process.stderr_process_id == Some(id)
                    })
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
                || only_process_id.is_some_and(|id| process.record_id != id)
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

    /// The live socket descriptor behind a network process, or None when the
    /// process has no runtime left.  process.c:2984 reads `p->infd' here and
    /// signals "Process is not running" when it is negative.
    pub fn network_socket_fd(&self, record_id: u64) -> Option<std::os::fd::RawFd> {
        use std::os::fd::AsRawFd;
        match self.find_process_state(record_id)?.network.as_ref()? {
            crate::lisp::eval::NetworkRuntime::Listener(listener) => Some(listener.as_raw_fd()),
            crate::lisp::eval::NetworkRuntime::Stream(stream) => Some(stream.as_raw_fd()),
            crate::lisp::eval::NetworkRuntime::Datagram { socket, .. } => Some(socket.as_raw_fd()),
            crate::lisp::eval::NetworkRuntime::UnixListener(listener) => Some(listener.as_raw_fd()),
            crate::lisp::eval::NetworkRuntime::UnixStream(stream) => Some(stream.as_raw_fd()),
        }
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

    /// process.c:2990 `pset_childp (p, plist_put (p->childp, option, value))'
    /// -- an accepted socket option becomes visible through `process-contact'.
    pub fn put_process_contact_option(&mut self, record_id: u64, option: &Value, value: Value) {
        let Some(process) = self.find_process_state_mut(record_id) else {
            return;
        };
        // plist_put walks with CONSP guards and never discards; using
        // unwrap_or_default() here would replace a dotted or improper contact
        // plist with just this one pair.
        let Ok(mut items) = process.contact.to_vec() else {
            return;
        };
        // plist_put compares with EQ, not by name: a symbol from another
        // obarray is a DIFFERENT key even when it prints the same, so it
        // appends rather than replacing.  Emaxx encodes that identity in the
        // raw symbol name, so compare raw names here -- deliberately NOT the
        // visible name used for the option-table lookup.
        let key_name = option.as_symbol().ok();
        let mut index = 0;
        while index + 1 < items.len() {
            if items[index].as_symbol().ok() == key_name {
                items[index + 1] = value;
                process.contact = Value::list(items);
                return;
            }
            index += 2;
        }
        // plist_put appends when the key is absent, storing the CALLER's
        // symbol -- so `(process-contact p t)' followed by `plist-get' with
        // the interned keyword legitimately misses a foreign-obarray key,
        // exactly as it does in GNU.
        items.push(option.clone());
        items.push(value);
        process.contact = Value::list(items);
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
            && self.fruitless_stepped_yields.is_multiple_of(10_000)
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
            Some("stdout") => runtime.pty_output.is_some(),
            // A separate :stderr destination is always a pipe.  Only merged
            // stderr shares the output PTY and therefore has no Child handle.
            Some("stderr") => runtime.pty_output.is_some() && runtime.child.stderr.is_none(),
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
        let callbacks = self.file_notify_callbacks_for_path(path, action);
        self.refresh_file_notify_fingerprints_for_path(path);
        self.push_pending_file_notification(path, action, callbacks);
    }

    pub fn queue_file_rename_notification(&mut self, source: &str, target: &str) {
        let source_normalized = source.trim_end_matches('/');
        let mut paired = Vec::new();
        let mut source_only = Vec::new();
        let mut target_only = Vec::new();
        for watch in self.file_notify_watches.values() {
            if !watch.active {
                continue;
            }
            if watch.backend != FileNotifyBackend::SyntheticKqueue {
                continue;
            }
            let Some(watched) = watch.path.as_deref() else {
                continue;
            };
            let covers_source = file_notify_watch_covers(watched, source);
            let covers_target = file_notify_watch_covers(watched, target);
            if covers_source
                && covers_target
                && watched.trim_end_matches('/') != source_normalized
                && watch.flags.iter().any(|flag| flag == "rename")
            {
                paired.push((watch.descriptor.clone(), watch.callback.clone()));
            } else if covers_source && watch.flags.iter().any(|flag| flag == "rename") {
                source_only.push((watch.descriptor.clone(), watch.callback.clone()));
            } else if covers_target && watch.flags.iter().any(|flag| flag == "create") {
                target_only.push((watch.descriptor.clone(), watch.callback.clone()));
            }
        }
        self.refresh_file_notify_fingerprints_for_path(source);
        self.refresh_file_notify_fingerprints_for_path(target);
        self.push_pending_file_notification_with_secondary(source, "renamed", Some(target), paired);
        self.push_pending_file_notification(source, "renamed", source_only);
        self.push_pending_file_notification(target, "created", target_only);
    }

    pub fn queue_directory_deletion_notification(&mut self, path: &str) {
        // kqueue reports deletion of an entry to a parent-directory watch,
        // but deletion of the watched directory itself as EV_REVOKE.  The
        // latter becomes only `stopped' in filenotify.el; reporting both
        // delete and revoke invents an extra `deleted' callback.
        let normalized = path.trim_end_matches('/');
        let mut parent_callbacks = Vec::new();
        let mut exact_callbacks = Vec::new();
        for watch in self.file_notify_watches.values() {
            if !watch.active {
                continue;
            }
            if watch.backend != FileNotifyBackend::SyntheticKqueue {
                continue;
            }
            let Some(watched) = watch.path.as_deref() else {
                continue;
            };
            if watched.trim_end_matches('/') == normalized
                && watch.flags.iter().any(|f| f == "revoke")
            {
                exact_callbacks.push((watch.descriptor.clone(), watch.callback.clone()));
            } else if file_notify_watch_covers(watched, path)
                && watch.flags.iter().any(|f| f == "delete")
            {
                parent_callbacks.push((watch.descriptor.clone(), watch.callback.clone()));
            }
        }
        self.refresh_file_notify_fingerprints_for_path(path);
        self.push_pending_file_notification(path, "deleted", parent_callbacks);
        self.push_pending_file_notification(path, "revoked", exact_callbacks);
    }

    fn push_pending_file_notification(
        &mut self,
        path: &str,
        action: &str,
        callbacks: Vec<(Value, Value)>,
    ) {
        self.push_pending_file_notification_with_secondary(path, action, None, callbacks);
    }

    fn push_pending_file_notification_with_secondary(
        &mut self,
        path: &str,
        action: &str,
        secondary_path: Option<&str>,
        mut callbacks: Vec<(Value, Value)>,
    ) {
        // kqueue readiness is level/coalescing based: several operations on
        // the same watched object before the next event-loop drain produce
        // one callback per flag, not one synthetic callback per syscall.
        callbacks.retain(|(descriptor, _)| {
            !self.pending_file_notifications.iter().any(|pending| {
                pending.action == action
                    && pending.path == path
                    && pending.secondary_path.as_deref() == secondary_path
                    && pending
                        .callbacks
                        .iter()
                        .any(|(pending_descriptor, _)| pending_descriptor == descriptor)
            })
        });
        if callbacks.is_empty() {
            return;
        }
        self.pending_file_notifications
            .push(PendingFileNotification {
                path: path.to_string(),
                secondary_path: secondary_path.map(str::to_string),
                action: action.to_string(),
                callbacks,
                raw_event: None,
            });
    }

    pub(crate) fn register_file_notify_watch(
        &mut self,
        descriptor: i64,
        path: Option<String>,
        flags: Vec<String>,
        callback: Value,
    ) -> Result<(), LispError> {
        let fingerprint = path.as_deref().map(file_notify_fingerprint);
        let directory_snapshot = path
            .as_deref()
            .map(|path| {
                file_notify_directory_snapshot(path).map_err(|error| {
                    primitives::file_operation_error("Reading directory", &error, path)
                })
            })
            .transpose()?
            .flatten();
        self.file_notify_watches.insert(
            descriptor,
            FileNotifyWatch {
                descriptor: Value::Integer(descriptor),
                path,
                flags,
                callback: Self::stored_value(callback),
                active: true,
                backend: FileNotifyBackend::SyntheticKqueue,
                fingerprint,
                directory_snapshot,
                #[cfg(target_os = "macos")]
                host_handle: None,
                #[cfg(target_os = "linux")]
                host_watch_descriptor: None,
                #[cfg(target_os = "linux")]
                host_mask: 0,
            },
        );
        Ok(())
    }

    #[cfg(target_os = "macos")]
    fn close_empty_kqueue_queue(&mut self) {
        if !self
            .file_notify_watches
            .values()
            .any(|watch| watch.backend == FileNotifyBackend::Kqueue)
        {
            self.file_notify_kqueue = None;
        }
    }

    #[cfg(target_os = "macos")]
    pub(crate) fn register_kqueue_file_notify_watch(
        &mut self,
        path: String,
        flags: Vec<String>,
        callback: Value,
    ) -> Result<i64, LispError> {
        use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

        let queue = if let Some(queue) = &self.file_notify_kqueue {
            queue.0.clone()
        } else {
            // SAFETY: kqueue has no pointer arguments and returns a new owned
            // descriptor on success.
            let descriptor = unsafe { libc::kqueue() };
            if descriptor < 0 {
                let error = std::io::Error::last_os_error();
                return Err(file_notify_host_error(
                    "File watching is not available",
                    None,
                    &error,
                ));
            }
            // SAFETY: DESCRIPTOR was returned by kqueue above and ownership
            // is transferred exactly once into OwnedFd.
            let queue = std::sync::Arc::new(unsafe { OwnedFd::from_raw_fd(descriptor) });
            self.file_notify_kqueue = Some(FileNotifyHostQueue(queue.clone()));
            queue
        };

        let encoded = match std::ffi::CString::new(path.as_bytes()) {
            Ok(encoded) => encoded,
            Err(_) => {
                self.close_empty_kqueue_queue();
                return Err(primitives::file_operation_error(
                    "File cannot be opened",
                    &std::io::Error::new(ErrorKind::InvalidInput, "file name contains a NUL byte"),
                    &path,
                ));
            }
        };
        // GNU opens vnode watches without following symlinks.  O_EVTONLY
        // obtains a descriptor that consumes no read permission and exists
        // specifically for kqueue/FSEvents observation on Darwin.
        let open_flags = libc::O_NONBLOCK | libc::O_CLOEXEC | libc::O_EVTONLY | libc::O_SYMLINK;
        // SAFETY: ENCODED is NUL-terminated and OPEN_FLAGS requires no mode
        // argument because O_CREAT is absent.
        let descriptor = unsafe { libc::open(encoded.as_ptr(), open_flags) };
        if descriptor < 0 {
            let error = std::io::Error::last_os_error();
            self.close_empty_kqueue_queue();
            return Err(primitives::file_operation_error(
                "File cannot be opened",
                &error,
                &path,
            ));
        }
        // SAFETY: DESCRIPTOR was returned by open above and ownership is
        // transferred exactly once into OwnedFd.
        let handle = std::sync::Arc::new(unsafe { OwnedFd::from_raw_fd(descriptor) });

        let mut filter_flags = 0_u32;
        for flag in &flags {
            filter_flags |= match flag.as_str() {
                "delete" => libc::NOTE_DELETE,
                "write" => libc::NOTE_WRITE,
                "extend" => libc::NOTE_EXTEND,
                "attrib" => libc::NOTE_ATTRIB,
                "link" => libc::NOTE_LINK,
                "rename" => libc::NOTE_RENAME,
                "revoke" => libc::NOTE_REVOKE,
                _ => 0,
            };
        }

        // SAFETY: zero is a valid starting representation for kevent; every
        // field consumed by kevent is initialized below.
        let mut event = unsafe { std::mem::zeroed::<libc::kevent>() };
        event.ident = descriptor as libc::uintptr_t;
        event.filter = libc::EVFILT_VNODE;
        event.flags = libc::EV_ADD | libc::EV_ENABLE | libc::EV_CLEAR;
        event.fflags = filter_flags;
        // SAFETY: EVENT points to one initialized change, the output list is
        // empty, and QUEUE/HANDLE remain owned by Arc values.
        let registered = unsafe {
            libc::kevent(
                queue.as_raw_fd(),
                &event,
                1,
                std::ptr::null_mut(),
                0,
                std::ptr::null(),
            )
        };
        if registered < 0 {
            let error = std::io::Error::last_os_error();
            self.close_empty_kqueue_queue();
            return Err(primitives::file_operation_error(
                "Cannot watch file",
                &error,
                &path,
            ));
        }

        let fingerprint = file_notify_fingerprint(&path);
        let directory_snapshot = match file_notify_directory_snapshot(&path) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                self.close_empty_kqueue_queue();
                return Err(primitives::file_operation_error(
                    "Reading directory",
                    &error,
                    &path,
                ));
            }
        };
        self.file_notify_watches.insert(
            descriptor as i64,
            FileNotifyWatch {
                descriptor: Value::Integer(descriptor as i64),
                path: Some(path),
                flags,
                callback: Self::stored_value(callback),
                active: true,
                backend: FileNotifyBackend::Kqueue,
                fingerprint: Some(fingerprint),
                directory_snapshot,
                host_handle: Some(handle),
            },
        );
        Ok(descriptor as i64)
    }

    /// aspect_to_inotifymask runs before Finotify_add_watch checks its
    /// FILE-NAME, so callers validate the aspects first and only then look
    /// at the other arguments.
    #[cfg(target_os = "linux")]
    pub(crate) fn validate_inotify_aspects(&self, flags: &[String]) -> Result<(), LispError> {
        inotify_mask_from_flags(flags).map(|_| ())
    }

    #[cfg(target_os = "linux")]
    fn close_empty_inotify_queue(&mut self) {
        if !self
            .file_notify_watches
            .values()
            .any(|watch| watch.backend == FileNotifyBackend::Inotify)
        {
            self.file_notify_inotify = None;
        }
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn register_inotify_file_notify_watch(
        &mut self,
        path: String,
        flags: Vec<String>,
        callback: Value,
    ) -> Result<Value, LispError> {
        use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

        let queue = if let Some(queue) = &self.file_notify_inotify {
            queue.0.clone()
        } else {
            // SAFETY: inotify_init1 returns a new descriptor on success.
            let descriptor = unsafe { libc::inotify_init1(libc::IN_NONBLOCK | libc::IN_CLOEXEC) };
            if descriptor < 0 {
                let error = std::io::Error::last_os_error();
                return Err(file_notify_host_error(
                    "File watching is not available",
                    None,
                    &error,
                ));
            }
            // SAFETY: ownership of the new descriptor transfers exactly once.
            let queue = std::sync::Arc::new(unsafe { OwnedFd::from_raw_fd(descriptor) });
            self.file_notify_inotify = Some(FileNotifyHostQueue(queue.clone()));
            queue
        };

        let mask = match inotify_mask_from_flags(&flags) {
            Ok(mask) => mask,
            Err(error) => {
                self.close_empty_inotify_queue();
                return Err(error);
            }
        };
        let encoded = match std::ffi::CString::new(path.as_bytes()) {
            Ok(encoded) => encoded,
            Err(_) => {
                let error = std::io::Error::from_raw_os_error(libc::EINVAL);
                self.close_empty_inotify_queue();
                return Err(file_notify_host_error(
                    "Could not add watch for file",
                    Some(path.as_str()),
                    &error,
                ));
            }
        };
        // GNU combines masks for callers that watch the same inode and then
        // filters each callback with the mask stored on its logical watch.
        // SAFETY: QUEUE is live and ENCODED is NUL-terminated.
        let watch_descriptor = unsafe {
            libc::inotify_add_watch(
                queue.as_raw_fd(),
                encoded.as_ptr(),
                mask | libc::IN_MASK_ADD | libc::IN_EXCL_UNLINK,
            )
        };
        if watch_descriptor < 0 {
            let error = std::io::Error::last_os_error();
            self.close_empty_inotify_queue();
            return Err(file_notify_host_error(
                "Could not add watch for file",
                Some(path.as_str()),
                &error,
            ));
        }

        let mut id = 0_i64;
        loop {
            let occupied = self.file_notify_watches.values().any(|watch| {
                watch.host_watch_descriptor == Some(watch_descriptor)
                    && watch
                        .descriptor
                        .cons_values()
                        .is_some_and(|(_, existing_id)| existing_id == Value::Integer(id))
            });
            if !occupied {
                break;
            }
            id = id.checked_add(1).ok_or_else(|| {
                file_notify_host_error(
                    "Could not add watch for file",
                    Some(path.as_str()),
                    &std::io::Error::from_raw_os_error(libc::EINVAL),
                )
            })?;
        }
        let descriptor = Value::cons(
            Value::Integer(i64::from(watch_descriptor)),
            Value::Integer(id),
        );
        let mut internal_key = -1_i64;
        while self.file_notify_watches.contains_key(&internal_key) {
            internal_key = internal_key.checked_sub(1).ok_or_else(|| {
                file_notify_host_error(
                    "Could not add watch for file",
                    Some(path.as_str()),
                    &std::io::Error::from_raw_os_error(libc::EINVAL),
                )
            })?;
        }
        self.file_notify_watches.insert(
            internal_key,
            FileNotifyWatch {
                descriptor: descriptor.clone(),
                path: Some(path),
                flags,
                callback: Self::stored_value(callback),
                active: true,
                backend: FileNotifyBackend::Inotify,
                fingerprint: None,
                directory_snapshot: None,
                host_watch_descriptor: Some(watch_descriptor),
                host_mask: mask,
            },
        );
        Ok(descriptor)
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn remove_inotify_file_notify_watch(
        &mut self,
        descriptor: &Value,
    ) -> Result<(), LispError> {
        use std::os::fd::AsRawFd;

        let Some((key, watch_descriptor)) =
            self.file_notify_watches.iter().find_map(|(key, watch)| {
                (watch.backend == FileNotifyBackend::Inotify && watch.descriptor == *descriptor)
                    .then_some((*key, watch.host_watch_descriptor))
            })
        else {
            // GNU accepts a well-formed descriptor that is no longer live.
            return Ok(());
        };
        let Some(watch_descriptor) = watch_descriptor else {
            return Ok(());
        };
        self.file_notify_watches.remove(&key);
        let descriptor_still_shared = self.file_notify_watches.values().any(|watch| {
            watch.backend == FileNotifyBackend::Inotify
                && watch.host_watch_descriptor == Some(watch_descriptor)
        });
        if !descriptor_still_shared && let Some(queue) = &self.file_notify_inotify {
            // SAFETY: WATCH_DESCRIPTOR belongs to this inotify instance and
            // no remaining logical watch uses it.
            if unsafe { libc::inotify_rm_watch(queue.0.as_raw_fd(), watch_descriptor) } != 0 {
                let error = std::io::Error::last_os_error();
                // GNU removes the logical descriptor (and closes the shared
                // inotify fd when this was its last watch) before reporting
                // the host removal error.  Keep that cleanup ordering so an
                // already-invalid kernel watch cannot strand the queue.
                self.close_empty_inotify_queue();
                // remove_descriptor reports the kernel watch descriptor, not
                // a file name, as the data element.
                return Err(primitives::file_notify_error_with_errno(
                    "Could not rm watch",
                    &error,
                    Value::Integer(i64::from(watch_descriptor)),
                ));
            }
        }
        if !self
            .file_notify_watches
            .values()
            .any(|watch| watch.backend == FileNotifyBackend::Inotify)
        {
            self.file_notify_inotify = None;
        }
        Ok(())
    }

    #[cfg(target_os = "linux")]
    pub(crate) fn inotify_file_notify_watch_is_active(&self, descriptor: &Value) -> bool {
        self.file_notify_watches.values().any(|watch| {
            watch.backend == FileNotifyBackend::Inotify
                && watch.active
                && watch.descriptor == *descriptor
        })
    }

    pub(crate) fn remove_file_notify_watch(&mut self, descriptor: &Value) -> bool {
        let key = self
            .file_notify_watches
            .iter()
            .find_map(|(key, watch)| (watch.descriptor == *descriptor).then_some(*key));
        let removed = key
            .and_then(|key| self.file_notify_watches.remove(&key))
            .is_some();
        #[cfg(target_os = "macos")]
        if !self
            .file_notify_watches
            .values()
            .any(|watch| watch.backend == FileNotifyBackend::Kqueue)
        {
            self.file_notify_kqueue = None;
        }
        removed
    }

    pub(crate) fn file_notify_watch_is_active(&self, descriptor: &Value) -> bool {
        self.file_notify_watches
            .values()
            .any(|watch| watch.active && watch.descriptor == *descriptor)
    }

    pub(crate) fn file_notify_watch_count(&self) -> usize {
        #[cfg(target_os = "macos")]
        {
            self.file_notify_watches
                .values()
                .filter(|watch| watch.backend == FileNotifyBackend::Kqueue)
                .count()
        }
        #[cfg(not(target_os = "macos"))]
        self.file_notify_watches.len()
    }

    pub(crate) fn invalidate_file_notify_watches_for_path(&mut self, path: &str) {
        for watch in self.file_notify_watches.values_mut() {
            if watch.backend != FileNotifyBackend::SyntheticKqueue {
                // The native backends' terminal event is authoritative:
                // kqueue reports delete/rename/revoke and inotify reports
                // delete-self/move-self followed by ignored.  Closing the
                // host handle here would suppress that callback sequence.
                continue;
            }
            if watch.path.as_deref() == Some(path) {
                watch.active = false;
            }
        }
    }

    pub(crate) fn file_notify_callbacks_for_path(
        &self,
        path: &str,
        action: &str,
    ) -> Vec<(Value, Value)> {
        let backend_flag = match action {
            "created" => "create",
            "deleted" => "delete",
            "changed" => "write",
            "attribute-changed" => "attrib",
            "renamed" => "rename",
            "revoked" => "revoke",
            other => other,
        };
        self.file_notify_watches
            .values()
            .filter_map(|watch| {
                if watch.backend != FileNotifyBackend::SyntheticKqueue {
                    return None;
                }
                watch
                    .path
                    .as_deref()
                    .filter(|_| watch.active)
                    .filter(|_| watch.flags.iter().any(|flag| flag == backend_flag))
                    .filter(|watched| file_notify_watch_covers(watched, path))
                    .map(|_| (watch.descriptor.clone(), watch.callback.clone()))
            })
            .collect()
    }

    fn refresh_file_notify_fingerprints_for_path(&mut self, event_path: &str) {
        for watch in self.file_notify_watches.values_mut() {
            if watch.backend != FileNotifyBackend::SyntheticKqueue {
                // Kernel events carry the authoritative flags, basenames,
                // rename data, and lifetime.  Synthetic bookkeeping must
                // neither consume nor reshape them.
                continue;
            }
            let Some(watched_path) = watch.path.as_deref() else {
                continue;
            };
            if file_notify_watch_covers(watched_path, event_path) {
                watch.fingerprint = Some(file_notify_fingerprint(watched_path));
                // A native file operation has already succeeded at this
                // point.  If an independent permissions race makes the
                // watched directory unreadable, retain the last good image;
                // the next host readiness poll reports that backend error.
                if let Ok(snapshot) = file_notify_directory_snapshot(watched_path) {
                    watch.directory_snapshot = snapshot;
                }
            }
        }
    }

    #[cfg(target_os = "macos")]
    fn poll_external_file_notifications(&mut self) -> Result<(), LispError> {
        use std::os::fd::AsRawFd;

        let Some(queue) = self
            .file_notify_kqueue
            .as_ref()
            .map(|queue| queue.0.clone())
        else {
            return Ok(());
        };
        let mut ready = Vec::<(i64, u32)>::new();
        loop {
            // SAFETY: zero is a valid output buffer for kevent and TIMEOUT is
            // a nonblocking zero-duration poll.
            let mut event = unsafe { std::mem::zeroed::<libc::kevent>() };
            let timeout = libc::timespec {
                tv_sec: 0,
                tv_nsec: 0,
            };
            // SAFETY: EVENT is writable storage for one result; no change
            // list is supplied; QUEUE remains owned for the whole call.
            let count = unsafe {
                libc::kevent(
                    queue.as_raw_fd(),
                    std::ptr::null(),
                    0,
                    &mut event,
                    1,
                    &timeout,
                )
            };
            if count == 0 {
                break;
            }
            if count < 0 {
                let error = std::io::Error::last_os_error();
                if error.kind() == ErrorKind::Interrupted {
                    continue;
                }
                return Err(file_notify_host_io_error(
                    "Cannot read file notification queue",
                    "",
                    &error,
                ));
            }
            ready.push((event.ident as i64, event.fflags));
        }

        for (descriptor, host_flags) in ready {
            let Some((
                active,
                path,
                public_descriptor,
                callback_value,
                fingerprint,
                snapshot,
                flags,
            )) = self.file_notify_watches.get(&descriptor).map(|watch| {
                (
                    watch.active,
                    watch.path.clone(),
                    watch.descriptor.clone(),
                    watch.callback.clone(),
                    watch.fingerprint.clone(),
                    watch.directory_snapshot.clone(),
                    watch.flags.clone(),
                )
            })
            else {
                continue;
            };
            if !active {
                continue;
            }
            let Some(path) = path else {
                continue;
            };
            let callback = (public_descriptor, callback_value);
            let is_directory = matches!(
                fingerprint,
                Some(FileNotifyFingerprint::Present {
                    is_directory: true,
                    ..
                })
            );

            if is_directory
                && host_flags & libc::NOTE_WRITE != 0
                && let Some(next) = file_notify_directory_snapshot(&path).map_err(|error| {
                    primitives::file_operation_error("Reading directory", &error, &path)
                })?
            {
                for (event_path, action, secondary_path) in file_notify_directory_changes(
                    &path,
                    snapshot.as_deref().unwrap_or_default(),
                    &next,
                ) {
                    let backend_flag = file_notify_backend_flag(&action);
                    if flags.iter().any(|flag| flag == backend_flag) {
                        self.push_pending_file_notification_with_secondary(
                            &event_path,
                            &action,
                            secondary_path.as_deref(),
                            vec![callback.clone()],
                        );
                    }
                }
                if let Some(live) = self.file_notify_watches.get_mut(&descriptor) {
                    live.directory_snapshot = Some(next);
                    live.fingerprint = Some(file_notify_fingerprint(&path));
                }
            }

            let direct_host_flags = host_flags
                & (libc::NOTE_DELETE
                    | libc::NOTE_EXTEND
                    | libc::NOTE_ATTRIB
                    | libc::NOTE_LINK
                    | libc::NOTE_RENAME
                    | libc::NOTE_REVOKE
                    | if is_directory { 0 } else { libc::NOTE_WRITE });
            if direct_host_flags != 0 {
                let current = file_notify_fingerprint(&path);
                if let Some(live) = self.file_notify_watches.get_mut(&descriptor) {
                    live.fingerprint = Some(current);
                }
                let mut actions = Vec::new();
                for (mask, action) in [
                    (libc::NOTE_DELETE, "delete"),
                    (libc::NOTE_WRITE, "write"),
                    (libc::NOTE_EXTEND, "extend"),
                    (libc::NOTE_ATTRIB, "attrib"),
                    (libc::NOTE_LINK, "link"),
                    (libc::NOTE_RENAME, "rename"),
                    (libc::NOTE_REVOKE, "revoke"),
                ] {
                    if direct_host_flags & mask != 0 && flags.iter().any(|flag| flag == action) {
                        actions.insert(0, Value::Symbol(action.into()));
                    }
                }
                if !actions.is_empty() {
                    let raw_event = Value::list([
                        callback.0.clone(),
                        Value::list(actions),
                        Value::String(path.clone().into()),
                    ]);
                    self.pending_file_notifications
                        .push(PendingFileNotification {
                            path: String::new(),
                            secondary_path: None,
                            action: String::new(),
                            callbacks: vec![callback.clone()],
                            raw_event: Some(raw_event),
                        });
                }
            }

            if host_flags & (libc::NOTE_DELETE | libc::NOTE_RENAME | libc::NOTE_REVOKE) != 0 {
                self.file_notify_watches.remove(&descriptor);
            }
        }
        if !self
            .file_notify_watches
            .values()
            .any(|watch| watch.backend == FileNotifyBackend::Kqueue)
        {
            self.file_notify_kqueue = None;
        }
        Ok(())
    }

    #[cfg(target_os = "linux")]
    fn poll_external_file_notifications(&mut self) -> Result<(), LispError> {
        use std::os::fd::AsRawFd;

        let Some(queue) = self
            .file_notify_inotify
            .as_ref()
            .map(|queue| queue.0.clone())
        else {
            return Ok(());
        };
        let header_size = std::mem::size_of::<libc::inotify_event>();
        let mut buffer = [0_u8; 65_536];
        loop {
            // SAFETY: BUFFER is writable for its full length and QUEUE stays
            // open throughout the nonblocking read.
            let count =
                unsafe { libc::read(queue.as_raw_fd(), buffer.as_mut_ptr().cast(), buffer.len()) };
            if count < 0 {
                let error = std::io::Error::last_os_error();
                if error.kind() == ErrorKind::Interrupted {
                    continue;
                }
                if error.kind() == ErrorKind::WouldBlock {
                    break;
                }
                return Err(file_notify_host_io_error(
                    "Error while reading file system events",
                    "",
                    &error,
                ));
            }
            if count == 0 {
                break;
            }

            let count = count as usize;
            let mut offset = 0_usize;
            while offset < count {
                if count - offset < header_size {
                    return Err(file_notify_host_error(
                        "Malformed file system event",
                        None,
                        &std::io::Error::from_raw_os_error(libc::EINVAL),
                    ));
                }
                // SAFETY: the size check above establishes a complete header;
                // kernel event records need not be aligned in a byte buffer.
                let event = unsafe {
                    std::ptr::read_unaligned(
                        buffer[offset..].as_ptr().cast::<libc::inotify_event>(),
                    )
                };
                let record_size = header_size.checked_add(event.len as usize).ok_or_else(|| {
                    file_notify_host_error(
                        "Malformed file system event",
                        None,
                        &std::io::Error::from_raw_os_error(libc::EINVAL),
                    )
                })?;
                if record_size > count - offset {
                    return Err(file_notify_host_error(
                        "Malformed file system event",
                        None,
                        &std::io::Error::from_raw_os_error(libc::EINVAL),
                    ));
                }
                let name = if event.len == 0 {
                    None
                } else {
                    let bytes = &buffer[offset + header_size..offset + record_size];
                    let end = bytes
                        .iter()
                        .position(|byte| *byte == 0)
                        .unwrap_or(bytes.len());
                    Some(String::from_utf8_lossy(&bytes[..end]).into_owned())
                };

                let mut recipients = self
                    .file_notify_watches
                    .values()
                    .filter(|watch| {
                        watch.active
                            && watch.backend == FileNotifyBackend::Inotify
                            && watch.host_watch_descriptor == Some(event.wd)
                            && watch.host_mask & event.mask != 0
                    })
                    .filter_map(|watch| {
                        name.clone()
                            .or_else(|| watch.path.clone())
                            .map(|event_name| {
                                (watch.descriptor.clone(), watch.callback.clone(), event_name)
                            })
                    })
                    .collect::<Vec<_>>();
                // inotify.c keeps logical watches for one kernel descriptor
                // sorted by their public ID and enqueues callbacks in that
                // order.  Hash-map iteration here would make delivery order
                // nondeterministic whenever callers share an inode.
                recipients.sort_by_key(|(descriptor, _, _)| {
                    descriptor
                        .cons_values()
                        .and_then(|(_, id)| id.as_integer().ok())
                        .unwrap_or(i64::MAX)
                });
                for (descriptor, callback, event_name) in recipients {
                    let raw_event = Value::list([
                        descriptor.clone(),
                        inotify_aspects(event.mask),
                        Value::String(event_name.into()),
                        Value::Integer(i64::from(event.cookie)),
                    ]);
                    self.pending_file_notifications
                        .push(PendingFileNotification {
                            path: String::new(),
                            secondary_path: None,
                            action: String::new(),
                            callbacks: vec![(descriptor, callback)],
                            raw_event: Some(raw_event),
                        });
                }

                if event.mask & libc::IN_IGNORED != 0 {
                    let keys = self
                        .file_notify_watches
                        .iter()
                        .filter_map(|(key, watch)| {
                            (watch.backend == FileNotifyBackend::Inotify
                                && watch.host_watch_descriptor == Some(event.wd))
                            .then_some(*key)
                        })
                        .collect::<Vec<_>>();
                    for key in keys {
                        self.file_notify_watches.remove(&key);
                    }
                }
                offset += record_size;
            }
        }

        if !self
            .file_notify_watches
            .values()
            .any(|watch| watch.backend == FileNotifyBackend::Inotify)
        {
            self.file_notify_inotify = None;
        }
        Ok(())
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    fn poll_external_file_notifications(&mut self) -> Result<(), LispError> {
        Ok(())
    }

    /// Dispatch queued notifications the way read_char dispatches buffered
    /// FILE_NOTIFY_EVENTs: each callback runs through the special-event
    /// binding, and a signal or nonlocal exit propagates to the reader while
    /// the events behind it stay queued for the next read.  Only the command
    /// loop's own condition handler turns such an error into a message, so
    /// nothing is demoted here.
    pub fn run_pending_file_notifications(&mut self, env: &mut Env) -> Result<bool, LispError> {
        self.run_pending_file_notifications_inner(env, true)
    }

    /// Handler-backed watches model Tramp's monitor process filters, whose
    /// callbacks fire inside any process wait.  Kernel watch descriptors are
    /// keyboard-class in process.c (add_keyboard_wait_descriptor), so a
    /// READ_KBD 0 wait never even reads them; their events stay queued for
    /// the keyboard readers.
    pub(crate) fn run_pending_synthetic_file_notifications(
        &mut self,
        env: &mut Env,
    ) -> Result<bool, LispError> {
        self.run_pending_file_notifications_inner(env, false)
    }

    fn run_pending_file_notifications_inner(
        &mut self,
        env: &mut Env,
        include_kernel_events: bool,
    ) -> Result<bool, LispError> {
        let (pending, retained): (Vec<_>, Vec<_>) =
            std::mem::take(&mut self.pending_file_notifications)
                .into_iter()
                .partition(|notification| {
                    include_kernel_events || notification.raw_event.is_none()
                });
        self.pending_file_notifications = retained;
        let ran = !pending.is_empty();
        let mut pending = pending.into_iter();
        while let Some(mut notification) = pending.next() {
            let mut callbacks = std::mem::take(&mut notification.callbacks).into_iter();
            while let Some(callback) = callbacks.next() {
                let outcome = if let Some(event) = &notification.raw_event {
                    primitives::deliver_raw_file_notification(
                        self,
                        env,
                        event.clone(),
                        vec![callback],
                    )
                } else {
                    primitives::deliver_file_notification(
                        self,
                        env,
                        &notification.path,
                        &notification.action,
                        notification.secondary_path.as_deref(),
                        vec![callback],
                    )
                };
                let Err(error) = outcome else {
                    continue;
                };
                // This input event was consumed, but later recipients and
                // later kernel events remain queued.  Dropping the tail here
                // made one failing callback erase unrelated watches.
                let remaining_callbacks = callbacks.collect::<Vec<_>>();
                let mut remaining = Vec::new();
                if !remaining_callbacks.is_empty() {
                    notification.callbacks = remaining_callbacks;
                    remaining.push(notification);
                }
                remaining.extend(pending);
                remaining.append(&mut self.pending_file_notifications);
                self.pending_file_notifications = remaining;
                return Err(error);
            }
        }
        Ok(ran)
    }

    pub fn service_file_notifications(&mut self, env: &mut Env) -> Result<bool, LispError> {
        self.poll_external_file_notifications()?;
        self.run_pending_file_notifications(env)
    }

    fn run_pending_native_timers(&mut self, env: &mut Env) -> Result<bool, LispError> {
        // Only timers whose scheduled time has arrived fire; the rest stay
        // queued (GNU never runs a timer before it is due).  Due timers
        // fire in schedule order.
        let now = std::time::Instant::now();
        let all = std::mem::take(&mut self.pending_timers);
        let (pending, not_yet): (Vec<_>, Vec<_>) = all
            .into_iter()
            .partition(|timer| timer.due.is_none_or(|due| due <= now));
        self.pending_timers = not_yet;
        let ran = !pending.is_empty();
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
        Ok(ran)
    }

    /// Pump every timer representation that can be live in an interpreter.
    ///
    /// Bootstrap calls use the native queue; after GNU timer.el loads, timer
    /// objects live in `timer-list`.  Keeping this as one event-loop operation
    /// prevents waits and recursive command loops from silently servicing
    /// only one side of that boundary.
    pub(crate) fn run_pending_timer_events(&mut self, env: &mut Env) -> Result<bool, LispError> {
        let idle = primitives::tty_current_idle_duration()
            .map(|duration| duration.as_secs_f64())
            .unwrap_or(0.0);
        self.run_pending_timer_events_with_idle(env, idle)
    }

    fn run_pending_timer_events_with_idle(
        &mut self,
        env: &mut Env,
        idle: f64,
    ) -> Result<bool, LispError> {
        let native_ran = self.run_pending_native_timers(env)?;
        let lisp_ran = primitives::run_due_timers(self, env, idle)?;
        Ok(native_ran || lisp_ran)
    }

    /// Service the non-process half of wait_reading_process_output.  This is
    /// shared by the top-level key wait and Lisp commands that perform their
    /// own terminal read, so neither path can starve watches, timers, or
    /// cooperative threads while waiting for keyboard input.
    pub(crate) fn service_async_runtime_events(
        &mut self,
        env: &mut Env,
        wake_sleepers: bool,
        idle_seconds: Option<f64>,
    ) -> Result<bool, LispError> {
        let user_signal_events = if self.waiting_for_user_input()
            && primitives::unread_command_events(self, env)?.is_empty()
        {
            primitives::run_pending_user_signal_events(self, env)?
        } else {
            false
        };
        let file_events = self.service_file_notifications(env)?;
        let timer_events = if let Some(idle) = idle_seconds {
            self.run_pending_timer_events_with_idle(env, idle)?
        } else {
            self.run_pending_timer_events(env)?
        };
        let thread_events = self.thread_states.iter().any(|thread| {
            thread.record_id != self.main_thread_id
                && (matches!(thread.status, ThreadStatus::Runnable)
                    || wake_sleepers
                        && matches!(thread.status, ThreadStatus::Blocked(ThreadBlocker::Sleep)))
        });
        self.drive_threads_inner(env, wake_sleepers, false)?;
        Ok(user_signal_events || file_events || timer_events || thread_events)
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
        // thread.c Fthread_signal: the current thread signals itself at once;
        // the main thread receives a THREAD_EVENT through the input queue;
        // any other thread has the error stored for delivery when it runs.
        if record_id == self.active_thread_id {
            return Err(LispError::SignalValue(build_signal_value(condition, data)));
        }
        if record_id == self.main_thread_id {
            self.deliver_signal_to_main_thread(self.active_thread_id, condition, data, env)?;
            return Ok(Value::Nil);
        }
        let signal = build_signal_value(condition, data);
        self.finish_thread_with_signal(record_id, signal, true);
        Ok(Value::Nil)
    }

    pub fn thread_join(&mut self, record_id: u64, env: &mut Env) -> Result<Value, LispError> {
        if record_id == self.main_thread_id {
            return Err(LispError::Signal("Cannot join the current thread".into()));
        }
        while self.thread_live(record_id) {
            // Fthread_join waits for the target without returning to the
            // process event loop.  Sleeping cooperative threads must still
            // advance, but unrelated timers and file notifications remain
            // pending until an actual event-loop wait.
            self.drive_threads_inner(env, true, false)?;
            if self.thread_live(record_id)
                && self
                    .find_thread_state(record_id)
                    .is_some_and(|thread| matches!(thread.program, ThreadProgram::InfiniteYield))
            {
                break;
            }
            // A child asleep for SECONDS keeps the joiner waiting for that
            // long; nap until its deadline or the next timer the child will
            // run, whichever is first.
            let sleeping_until = self
                .find_thread_state(record_id)
                .and_then(|thread| match thread.program {
                    ThreadProgram::Sleep {
                        blocked: true,
                        until,
                        ..
                    } => until,
                    _ => None,
                });
            if let Some(until) = sleeping_until {
                let now = std::time::Instant::now();
                let mut nap = until.saturating_duration_since(now);
                if let Some(due) = self.next_timer_due() {
                    nap = nap.min(due.saturating_duration_since(now));
                }
                if !nap.is_zero() {
                    std::thread::sleep(nap.min(std::time::Duration::from_millis(10)));
                }
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
            // thread.c:815 runs the whole thread function inside
            // `internal_condition_case (..., record_thread_error)': a body
            // error is ALWAYS caught, recorded for `thread-last-error', and
            // the thread finishes normally with a nil result.  Fthread_join's
            // re-signal branch (thread.c:1088) reads `error_symbol', which
            // belongs to the separate `thread-signal' delivery machinery and
            // is cleared by the target's own handler -- so joining an errored
            // thread returns NIL.  Re-raising here made
            // `(thread-join (make-thread (lambda () (car 42))))' signal in
            // the JOINING thread where GNU answers nil, which is the
            // `threads-errors' mismatch in test/src/thread-tests.el.
            // `finish_thread_with_signal' has already stored the error for
            // `thread-last-error', exactly as `record_thread_error' does.
            ThreadOutcome::Signaled {
                delivered: false, ..
            } => Ok(Value::Nil),
            // The injected signal comes back out of the join, exactly as
            // GNU's snapshot re-raise does.  (One disclosed shortcut: GNU
            // returns nil if the target managed to PROCESS the delivery
            // before join was called, because the snapshot is then already
            // clear.  Emaxx's cooperative `thread-signal' kills the target
            // instantly, so that window does not exist here and every
            // delivered signal re-raises.)
            ThreadOutcome::Signaled {
                value,
                delivered: true,
            } => Err(LispError::SignalValue(value)),
        }
    }

    pub fn drive_threads(&mut self, env: &mut Env, wake_sleepers: bool) -> Result<(), LispError> {
        self.drive_threads_inner(env, wake_sleepers, wake_sleepers)
    }

    fn drive_threads_inner(
        &mut self,
        env: &mut Env,
        wake_sleepers: bool,
        service_events: bool,
    ) -> Result<(), LispError> {
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
                    && !self.executing_thread_ids.contains(&thread.record_id)
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
                    // The child sits in wait_reading_process_output for its
                    // SECONDS: until they elapse it runs the timers that come
                    // due, on its own specpdl (thread.c swaps the bindings),
                    // and only then returns.
                    let until =
                        self.find_thread_state(thread_id)
                            .and_then(|thread| match thread.program {
                                ThreadProgram::Sleep { until, .. } => until,
                                _ => None,
                            });
                    if until.is_some_and(|until| std::time::Instant::now() < until) {
                        self.run_pending_timer_events_as_thread(thread_id, env)?;
                    } else {
                        self.finish_thread_success(thread_id, Value::Nil);
                    }
                }
                _ => {}
            }
        }
        let _ = entry_yield_count;
        if service_events {
            // wait_reading_process_output selects keyboard-class descriptors
            // (the inotify/kqueue queue among them) only for a READ_KBD wait,
            // and read_char then dispatches the buffered special events.  A
            // READ_KBD 0 wait such as accept-process-output or sleep-for runs
            // timers and delivers handler-backed notifications, which GNU
            // receives as monitor process output, but never reads that queue.
            if self.waiting_for_user_input() {
                if primitives::unread_command_events(self, env)?.is_empty() {
                    let _ = primitives::run_pending_user_signal_events(self, env)?;
                }
                let _ = self.service_file_notifications(env)?;
            } else {
                let _ = self.run_pending_synthetic_file_notifications(env)?;
            }
            let _ = self.run_pending_timer_events(env)?;
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
                    if crate::lisp::primitives::values::effective_text_quoting_style(self, env)
                        == "curve"
                    {
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
                if crate::lisp::primitives::values::effective_text_quoting_style(self, env)
                    == "curve"
                {
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

    pub(super) fn finish_thread_with_signal(
        &mut self,
        record_id: u64,
        value: Value,
        delivered: bool,
    ) {
        if let Some(thread) = self.find_thread_state_mut(record_id) {
            thread.status = ThreadStatus::Finished;
            thread.outcome = Some(ThreadOutcome::Signaled {
                value: value.clone(),
                delivered,
            });
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
        // keyboard.c: `(thread-event THREAD ERROR-SYMBOL DATA)', stored in
        // the input queue; read_char runs `special-event-map's binding
        // (thread-handle-event's message) when the main thread next reads.
        let _ = env;
        self.pending_thread_events.push(Value::list([
            Value::symbol("thread-event"),
            Value::Record(source_thread_id),
            condition,
            data,
        ]));
        Ok(())
    }

    /// Run the due timers as THREAD would inside its own wait: with the
    /// driving thread's dynamic bindings and handlers swapped out
    /// (thread.c:87-100) and THREAD as the current thread.
    fn run_pending_timer_events_as_thread(
        &mut self,
        record_id: u64,
        env: &mut Env,
    ) -> Result<(), LispError> {
        let previous_active = self.active_thread_id;
        self.active_thread_id = record_id;
        let swap_start = self.thread_swap_boundaries.last().copied().unwrap_or(0);
        self.swap_special_bindings_for_thread_switch(swap_start, false);
        self.thread_swap_boundaries
            .push(self.active_special_restores.len());
        let parent_handlers = std::mem::take(&mut self.active_handlers);
        let mut thread_env = Vec::new();
        let result = self.run_pending_timer_events(&mut thread_env);
        self.active_handlers = parent_handlers;
        self.thread_swap_boundaries.pop();
        self.swap_special_bindings_for_thread_switch(swap_start, true);
        self.active_thread_id = previous_active;
        let _ = env;
        result.map(|_| ())
    }

    pub(super) fn step_thread(&mut self, record_id: u64, env: &mut Env) -> Result<(), LispError> {
        let _ = &env;
        // An event wait can nest scheduler passes: A waits for process I/O,
        // the pump runs B, and B pumps again.  `active_thread_id' identifies
        // B at that point but A is still live on the Rust stack.  Re-entering
        // A from the top recursively restarts its Lisp body and eventually
        // reports `excessive-lisp-nesting'.
        if self.executing_thread_ids.contains(&record_id) {
            return Ok(());
        }
        self.executing_thread_ids.push(record_id);
        let previous_active = self.active_thread_id;
        let previous_buffer_id = self.current_buffer_id();
        let thread_buffer_id = self
            .find_thread_state(record_id)
            .map(|thread| thread.buffer_id)
            .unwrap_or(previous_buffer_id);
        if self.has_buffer_id(thread_buffer_id)
            && let Err(error) = self.set_current_buffer_id(thread_buffer_id)
        {
            self.executing_thread_ids.pop();
            return Err(error);
        }
        self.active_thread_id = record_id;
        let program = self
            .find_thread_state(record_id)
            .map(|thread| thread.program.clone())
            .unwrap_or(ThreadProgram::Noop);

        let mut result = match program {
            ThreadProgram::Main => Ok(()),
            ThreadProgram::Ignore | ThreadProgram::Noop => {
                self.finish_thread_success(record_id, Value::Nil);
                Ok(())
            }
            ThreadProgram::Call(function) => {
                // Each GNU thread gets its OWN specpdl: the spawner's dynamic
                // `let' bindings are invisible to the child, which sees
                // globals (and writes globals with `setq').  Passing the
                // driving thread's `env' here leaked the parent's entire
                // dynamic stack into the child --
                // `(let ((zz 1)) (thread-join (make-thread (lambda () zz))))'
                // answered 1 where GNU answers the global.  Lexical captures
                // live inside `function' itself and are unaffected.
                let mut thread_env = Vec::new();
                // ...and its own view of the dynamic cells: swap every live
                // special binding out for the duration of the body
                // (thread.c:87-100), so the child reads and writes GLOBALS.
                // The swap is two-way; see
                // `swap_special_bindings_for_thread_switch'.
                let swap_start = self.thread_swap_boundaries.last().copied().unwrap_or(0);
                self.swap_special_bindings_for_thread_switch(swap_start, false);
                self.thread_swap_boundaries
                    .push(self.active_special_restores.len());
                // GNU also swaps the HANDLER list per thread (m_handlerlist):
                // a child starts with no condition handlers, so the parent's
                // `handler-bind' handlers must not see the child's signals.
                // ERT is the proving case -- ert.el:803 wraps every test body
                // in `(handler-bind (((error quit) debugfun)) ...)', and with
                // a shared stack a child's `(error ...)' ran ERT's debugfun,
                // whose `cl-return-from' then died at the thread boundary as
                // `(no-catch --cl-block-error-- nil)' instead of the child's
                // real error reaching `thread-last-error'.
                // (Catches need no such swap: a child's `throw' bubbles as a
                // Rust Err to this boundary and is recorded as no-catch,
                // which is GNU's per-thread catchlist behaviour already.)
                let parent_handlers = std::mem::take(&mut self.active_handlers);
                let outcome = self.call_function_value(function, None, &[], &mut thread_env);
                self.active_handlers = parent_handlers;
                self.thread_swap_boundaries.pop();
                self.swap_special_bindings_for_thread_switch(swap_start, true);
                match outcome {
                    Ok(value) => {
                        self.finish_thread_success(record_id, value);
                        Ok(())
                    }
                    Err(error) => {
                        self.finish_thread_with_signal(
                            record_id,
                            error_condition_value(&error),
                            false,
                        );
                        Ok(())
                    }
                }
            }
            ThreadProgram::Sleep {
                blocked, seconds, ..
            } => {
                if !blocked && let Some(thread) = self.find_thread_state_mut(record_id) {
                    thread.program = ThreadProgram::Sleep {
                        blocked: true,
                        seconds,
                        until: Some(
                            std::time::Instant::now()
                                + std::time::Duration::from_secs_f64(seconds.max(0.0)),
                        ),
                    };
                    thread.status = ThreadStatus::Blocked(ThreadBlocker::Sleep);
                }
                Ok(())
            }
            ThreadProgram::InfiniteYield => Ok(()),
        };
        let current_thread_buffer_id = self.current_buffer_id();
        if let Some(thread) = self.find_thread_state_mut(record_id) {
            thread.buffer_id = current_thread_buffer_id;
        }
        if self.has_buffer_id(previous_buffer_id)
            && let Err(error) = self.set_current_buffer_id(previous_buffer_id)
            && result.is_ok()
        {
            result = Err(error);
        }
        self.active_thread_id = previous_active;
        let popped = self.executing_thread_ids.pop();
        debug_assert_eq!(popped, Some(record_id));
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
            && items.len() == 2
            && let Some(seconds) = match &items[1] {
                Value::Integer(seconds) => Some(*seconds as f64),
                Value::Float(seconds) => Some(*seconds),
                _ => None,
            }
        {
            return Ok(ThreadProgram::Sleep {
                blocked: false,
                seconds,
                until: None,
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

#[cfg(target_os = "macos")]
fn file_notify_backend_flag(action: &str) -> &str {
    match action {
        "created" => "create",
        "deleted" => "delete",
        "changed" => "write",
        "attribute-changed" => "attrib",
        "renamed" => "rename",
        "revoked" => "revoke",
        other => other,
    }
}

#[cfg(target_os = "linux")]
fn inotify_mask_from_flags(flags: &[String]) -> Result<u32, LispError> {
    let mut mask = 0_u32;
    for flag in flags {
        mask |= match flag.as_str() {
            "access" => libc::IN_ACCESS,
            "attrib" => libc::IN_ATTRIB,
            "close-write" => libc::IN_CLOSE_WRITE,
            "close-nowrite" => libc::IN_CLOSE_NOWRITE,
            "create" => libc::IN_CREATE,
            "delete" => libc::IN_DELETE,
            "delete-self" => libc::IN_DELETE_SELF,
            "modify" => libc::IN_MODIFY,
            "move-self" => libc::IN_MOVE_SELF,
            "moved-from" => libc::IN_MOVED_FROM,
            "moved-to" => libc::IN_MOVED_TO,
            "open" => libc::IN_OPEN,
            "move" => libc::IN_MOVED_FROM | libc::IN_MOVED_TO,
            "close" => libc::IN_CLOSE_WRITE | libc::IN_CLOSE_NOWRITE,
            "dont-follow" => libc::IN_DONT_FOLLOW,
            "onlydir" => libc::IN_ONLYDIR,
            "ignored" => libc::IN_IGNORED,
            "unmount" => libc::IN_UNMOUNT,
            "t" | "all-events" => libc::IN_ALL_EVENTS,
            _ => {
                let aspect = if flag == "nil" {
                    Value::Nil
                } else {
                    Value::Symbol(flag.clone().into())
                };
                // symbol_to_inotifymask sets errno to EINVAL before
                // report_file_notify_error renders it into the data.
                return Err(primitives::file_notify_error_with_errno(
                    "Unknown aspect",
                    &std::io::Error::from_raw_os_error(libc::EINVAL),
                    aspect,
                ));
            }
        };
    }
    Ok(mask)
}

#[cfg(target_os = "linux")]
fn inotify_aspects(mask: u32) -> Value {
    let mut aspects = Vec::new();
    for (bit, name) in [
        (libc::IN_ACCESS, "access"),
        (libc::IN_ATTRIB, "attrib"),
        (libc::IN_CLOSE_WRITE, "close-write"),
        (libc::IN_CLOSE_NOWRITE, "close-nowrite"),
        (libc::IN_CREATE, "create"),
        (libc::IN_DELETE, "delete"),
        (libc::IN_DELETE_SELF, "delete-self"),
        (libc::IN_MODIFY, "modify"),
        (libc::IN_MOVE_SELF, "move-self"),
        (libc::IN_MOVED_FROM, "moved-from"),
        (libc::IN_MOVED_TO, "moved-to"),
        (libc::IN_OPEN, "open"),
        (libc::IN_IGNORED, "ignored"),
        (libc::IN_ISDIR, "isdir"),
        (libc::IN_Q_OVERFLOW, "q-overflow"),
        (libc::IN_UNMOUNT, "unmount"),
    ] {
        if mask & bit != 0 {
            // GNU conses each match while walking this table, so the public
            // list is the reverse of the test order above.
            aspects.insert(0, Value::Symbol(name.into()));
        }
    }
    Value::list(aspects)
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
fn file_notify_host_error(message: &str, path: Option<&str>, error: &std::io::Error) -> LispError {
    let rendered = error.to_string();
    let detail = rendered
        .split_once(" (os error")
        .map_or(rendered.as_str(), |(detail, _)| detail);
    let mut condition = vec![
        Value::Symbol("file-notify-error".into()),
        Value::String(message.into()),
        Value::String(detail.into()),
    ];
    if let Some(path) = path {
        condition.push(Value::String(path.into()));
    }
    LispError::SignalValue(Value::list(condition))
}

#[cfg(any(target_os = "macos", target_os = "linux"))]
fn file_notify_host_io_error(message: &str, path: &str, error: &std::io::Error) -> LispError {
    let rendered = error.to_string();
    let detail = rendered
        .split_once(" (os error")
        .map_or(rendered.as_str(), |(detail, _)| detail);
    let mut condition = vec![
        Value::Symbol("file-notify-error".into()),
        Value::String(message.into()),
        Value::String(detail.into()),
    ];
    if !path.is_empty() {
        condition.push(Value::String(path.into()));
    }
    LispError::SignalValue(Value::list(condition))
}

fn file_notify_directory_snapshot(
    path: &str,
) -> Result<Option<Vec<FileNotifyDirectoryEntry>>, std::io::Error> {
    let metadata = match fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    if !metadata.is_dir() {
        return Ok(None);
    }
    let mut snapshot = fs::read_dir(path)?
        .map(|entry| {
            let entry = entry?;
            // `directory-files-and-attributes' obtains each entry with
            // AT_SYMLINK_NOFOLLOW before kqueue.c extracts these fields.
            // Following a child symlink here would compare the target's
            // inode/times instead and misclassify link replacement/rename.
            let metadata = fs::symlink_metadata(entry.path())?;
            #[cfg(unix)]
            let (inode, status_changed) = {
                use std::os::unix::fs::MetadataExt;
                (
                    metadata.ino(),
                    Some((metadata.ctime(), metadata.ctime_nsec())),
                )
            };
            #[cfg(not(unix))]
            let (inode, status_changed) = (0, None);
            Ok(FileNotifyDirectoryEntry {
                name: entry.file_name().to_string_lossy().into_owned(),
                inode,
                modified: metadata.modified().ok(),
                status_changed,
                len: metadata.len(),
            })
        })
        .collect::<Result<Vec<_>, std::io::Error>>()?;
    snapshot.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(Some(snapshot))
}

#[cfg(target_os = "macos")]
fn file_notify_directory_changes(
    directory: &str,
    previous: &[FileNotifyDirectoryEntry],
    current: &[FileNotifyDirectoryEntry],
) -> Vec<(String, String, Option<String>)> {
    let mut events = Vec::new();
    let mut current_used = vec![false; current.len()];
    let full_path = |name: &str| {
        std::path::Path::new(directory)
            .join(name)
            .to_string_lossy()
            .into_owned()
    };

    for old in previous {
        if let Some((index, new)) = current
            .iter()
            .enumerate()
            .find(|(index, candidate)| !current_used[*index] && candidate.inode == old.inode)
        {
            current_used[index] = true;
            let old_path = full_path(&old.name);
            if old.name != new.name {
                events.push((old_path, "renamed".into(), Some(full_path(&new.name))));
            } else {
                if old.modified != new.modified || old.len != new.len {
                    events.push((old_path.clone(), "changed".into(), None));
                }
                if old.status_changed != new.status_changed {
                    events.push((old_path, "attribute-changed".into(), None));
                }
            }
            continue;
        }

        // Replacing a directory entry under the same name is neither a
        // create/delete pair nor a rename in GNU's kqueue directory diff; it
        // is a pending entry resolved as a write.
        if let Some((index, _)) = current
            .iter()
            .enumerate()
            .find(|(index, candidate)| !current_used[*index] && candidate.name == old.name)
        {
            current_used[index] = true;
            events.push((full_path(&old.name), "changed".into(), None));
        } else {
            events.push((full_path(&old.name), "deleted".into(), None));
        }
    }

    for (index, new) in current.iter().enumerate() {
        if current_used[index] {
            continue;
        }
        let path = full_path(&new.name);
        events.push((path.clone(), "created".into(), None));
        if new.len > 0 {
            events.push((path, "changed".into(), None));
        }
    }
    events
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
