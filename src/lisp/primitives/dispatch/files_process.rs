use super::*;

#[cfg(unix)]
static ACCOUNT_DATABASE_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(unix)]
fn system_user_names() -> Vec<String> {
    let _guard = ACCOUNT_DATABASE_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut names = Vec::new();
    // SAFETY: getpwent owns its returned storage until the next passwd
    // database call.  The process-wide lock prevents another Rust test or
    // interpreter from advancing that cursor while the name is copied.
    unsafe {
        libc::setpwent();
        loop {
            let entry = libc::getpwent();
            if entry.is_null() {
                break;
            }
            let name = (*entry).pw_name;
            if !name.is_null() {
                names.push(
                    std::ffi::CStr::from_ptr(name)
                        .to_string_lossy()
                        .into_owned(),
                );
            }
        }
        libc::endpwent();
    }
    names.reverse();
    names
}

#[cfg(not(unix))]
fn system_user_names() -> Vec<String> {
    Vec::new()
}

#[cfg(unix)]
fn system_group_names() -> Vec<String> {
    let _guard = ACCOUNT_DATABASE_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut names = Vec::new();
    // SAFETY: getgrent follows the same process-global cursor contract as
    // getpwent.  Copy each name while holding the shared database lock.
    unsafe {
        libc::setgrent();
        loop {
            let entry = libc::getgrent();
            if entry.is_null() {
                break;
            }
            let name = (*entry).gr_name;
            if !name.is_null() {
                names.push(
                    std::ffi::CStr::from_ptr(name)
                        .to_string_lossy()
                        .into_owned(),
                );
            }
        }
        libc::endgrent();
    }
    names.reverse();
    names
}

#[cfg(not(unix))]
fn system_group_names() -> Vec<String> {
    Vec::new()
}

fn removing_old_name_error(path: &str, error: &std::io::Error) -> LispError {
    let condition = if error.kind() == ErrorKind::NotFound {
        "file-missing"
    } else {
        "file-error"
    };
    let rendered = error.to_string();
    let detail = rendered
        .split_once(" (os error")
        .map_or(rendered.as_str(), |(detail, _)| detail);
    LispError::SignalValue(Value::list([
        Value::Symbol(condition.into()),
        Value::String("Removing old name".into()),
        Value::String(detail.into()),
        Value::String(path.into()),
    ]))
}

fn move_to_system_trash(path: &str) -> Result<(), trash::Error> {
    #[cfg(target_os = "macos")]
    {
        use trash::macos::{DeleteMethod, TrashContextExtMacos};

        let mut context = trash::TrashContext::new();
        // NSFileManager is the native, non-interactive backend.  The crate's
        // Finder default shells out through AppleScript and can prompt for
        // automation permission, which is wrong for a Lisp primitive.
        context.set_delete_method(DeleteMethod::NsFileManager);
        context.delete(path)
    }
    #[cfg(not(target_os = "macos"))]
    {
        trash::delete(path)
    }
}

fn buffer_visiting_exact_file_name(
    interp: &Interpreter,
    expanded_file_name: &str,
) -> Option<(u64, String)> {
    interp.buffer_list.iter().find_map(|(id, name)| {
        interp
            .get_buffer_by_id(*id)
            .is_some_and(|buffer| buffer.file.as_deref() == Some(expanded_file_name))
            .then(|| (*id, name.clone()))
    })
}

fn bury_buffer_in_lists(
    interp: &mut Interpreter,
    buffer_id: u64,
    env: &mut Env,
) -> Result<(), LispError> {
    if let Some(index) = interp
        .buffer_list
        .iter()
        .position(|(candidate_id, _)| *candidate_id == buffer_id)
    {
        let entry = interp.buffer_list.remove(index);
        interp.buffer_list.push(entry);
        run_named_hooks(interp, "buffer-list-update-hook", env, Some(buffer_id))?;
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum AddressFamily {
    Both,
    Ipv4,
    Ipv6,
}

impl AddressFamily {
    fn parse(value: Option<&Value>) -> Result<Self, LispError> {
        match value {
            None | Some(Value::Nil) => Ok(Self::Both),
            Some(Value::Symbol(family)) if family == "ipv4" => Ok(Self::Ipv4),
            Some(Value::Symbol(family)) if family == "ipv6" => Ok(Self::Ipv6),
            _ => Err(LispError::Signal("Unsupported family".into())),
        }
    }

    fn includes(self, address: std::net::IpAddr) -> bool {
        match self {
            Self::Both => true,
            Self::Ipv4 => address.is_ipv4(),
            Self::Ipv6 => address.is_ipv6(),
        }
    }

    fn resolver_family(self) -> libc::c_int {
        match self {
            Self::Both => libc::AF_UNSPEC,
            Self::Ipv4 => libc::AF_INET,
            Self::Ipv6 => libc::AF_INET6,
        }
    }
}

fn network_lookup_addresses(
    host: &str,
    family: AddressFamily,
    numeric: bool,
) -> Result<Vec<std::net::SocketAddr>, String> {
    // process.c passes the string data to getaddrinfo, so an embedded NUL
    // terminates the name instead of making construction of a Rust CString
    // fail.  The public primitive has already rejected non-ASCII names.
    let host_prefix = host
        .as_bytes()
        .split(|byte| *byte == 0)
        .next()
        .unwrap_or_default();
    let host = std::ffi::CString::new(host_prefix).expect("NUL-free hostname prefix");
    let mut hints = unsafe { std::mem::zeroed::<libc::addrinfo>() };
    hints.ai_family = family.resolver_family();
    hints.ai_socktype = libc::SOCK_DGRAM;
    if numeric {
        hints.ai_flags = libc::AI_NUMERICHOST;
    }

    let mut result = std::ptr::null_mut();
    let status = unsafe { libc::getaddrinfo(host.as_ptr(), std::ptr::null(), &hints, &mut result) };
    if status != 0 {
        let message = unsafe { libc::gai_strerror(status) };
        return Err(if message.is_null() {
            format!("getaddrinfo error {status}")
        } else {
            unsafe { std::ffi::CStr::from_ptr(message) }
                .to_string_lossy()
                .into_owned()
        });
    }

    let mut addresses = Vec::new();
    let mut current = result;
    while !current.is_null() {
        let entry = unsafe { &*current };
        let address = match entry.ai_family {
            libc::AF_INET
                if !entry.ai_addr.is_null()
                    && entry.ai_addrlen as usize >= std::mem::size_of::<libc::sockaddr_in>() =>
            {
                let address = unsafe { &*entry.ai_addr.cast::<libc::sockaddr_in>() };
                Some(std::net::SocketAddr::new(
                    std::net::IpAddr::V4(std::net::Ipv4Addr::from(u32::from_be(
                        address.sin_addr.s_addr,
                    ))),
                    u16::from_be(address.sin_port),
                ))
            }
            libc::AF_INET6
                if !entry.ai_addr.is_null()
                    && entry.ai_addrlen as usize >= std::mem::size_of::<libc::sockaddr_in6>() =>
            {
                let address = unsafe { &*entry.ai_addr.cast::<libc::sockaddr_in6>() };
                Some(std::net::SocketAddr::V6(std::net::SocketAddrV6::new(
                    std::net::Ipv6Addr::from(address.sin6_addr.s6_addr),
                    u16::from_be(address.sin6_port),
                    address.sin6_flowinfo,
                    address.sin6_scope_id,
                )))
            }
            _ => None,
        };
        if let Some(address) = address {
            addresses.push(address);
        }
        current = entry.ai_next;
    }
    unsafe { libc::freeaddrinfo(result) };
    Ok(addresses)
}

fn interface_broadcast(ip: std::net::IpAddr, mask: std::net::IpAddr) -> std::net::IpAddr {
    match (ip, mask) {
        (std::net::IpAddr::V4(ip), std::net::IpAddr::V4(mask)) => {
            std::net::IpAddr::V4(std::net::Ipv4Addr::from(u32::from(ip) | !u32::from(mask)))
        }
        (std::net::IpAddr::V6(ip), std::net::IpAddr::V6(mask)) => {
            std::net::IpAddr::V6(std::net::Ipv6Addr::from(u128::from(ip) | !u128::from(mask)))
        }
        _ => unreachable!("interface address and mask families differ"),
    }
}

/// The `_IOWR' encoding from macOS `sys/ioccom.h': the request number is
/// DERIVED from the group letter, the ordinal, and the size of the payload
/// struct.  Computing it beats transcribing 0xc0206911 and friends, which
/// would be a magic constant valid only for one struct layout on one platform.
#[cfg(target_os = "macos")]
const fn iowr(group: u8, number: u8, size: usize) -> libc::c_ulong {
    const IOC_INOUT: libc::c_ulong = 0xc000_0000;
    const IOCPARM_MASK: usize = 0x1fff;
    IOC_INOUT
        | (((size & IOCPARM_MASK) as libc::c_ulong) << 16)
        | ((group as libc::c_ulong) << 8)
        | number as libc::c_ulong
}

/// process.c:4386 `ifflag_table', in GNU's order.  The result list is built by
/// consing, so it comes out REVERSED -- lo0 reads (multicast running loopback
/// up), which is why the order here matters.  On a Cocoa build GNU spells
/// IFF_NOTRAILERS "smart" rather than "notrailers" (process.c:4412).
#[cfg(target_os = "macos")]
const IFFLAG_TABLE: &[(libc::c_int, &str)] = &[
    (libc::IFF_UP, "up"),
    (libc::IFF_BROADCAST, "broadcast"),
    (libc::IFF_DEBUG, "debug"),
    (libc::IFF_LOOPBACK, "loopback"),
    (libc::IFF_POINTOPOINT, "pointopoint"),
    (libc::IFF_RUNNING, "running"),
    (libc::IFF_NOARP, "noarp"),
    (libc::IFF_PROMISC, "promisc"),
    (libc::IFF_NOTRAILERS, "smart"),
    (libc::IFF_ALLMULTI, "allmulti"),
    (libc::IFF_MULTICAST, "multicast"),
    (libc::IFF_OACTIVE, "oactive"),
    (libc::IFF_SIMPLEX, "simplex"),
    (libc::IFF_LINK0, "link0"),
    (libc::IFF_LINK1, "link1"),
    (libc::IFF_LINK2, "link2"),
];

/// GNU `conv_sockaddr_to_lisp' for the AF_INET case: [a b c d PORT].
#[cfg(target_os = "macos")]
fn sockaddr_storage_to_lisp(raw: &libc::sockaddr) -> Value {
    if raw.sa_family as libc::c_int != libc::AF_INET {
        return Value::Nil;
    }
    // SAFETY: sa_family says AF_INET, so the storage is a sockaddr_in.
    let inet = unsafe { &*(raw as *const libc::sockaddr).cast::<libc::sockaddr_in>() };
    let octets = u32::from_be(inet.sin_addr.s_addr).to_be_bytes();
    super::super::processes::sockaddr_vector(std::net::SocketAddr::new(
        std::net::IpAddr::V4(std::net::Ipv4Addr::from(octets)),
        u16::from_be(inet.sin_port),
    ))
}

/// process.c:4459 `network_interface_info'.  Returns
/// (ADDR BCAST NETMASK HWADDR FLAGS), IPv4 only, with each component nil when
/// its query fails -- and nil overall when EVERY query failed, which is how a
/// nonexistent interface answers.  The arm this replaces was a bare
/// `Ok(Value::Nil)' sitting beside a genuinely implemented
/// `network-interface-list' (audit finding 111).
fn network_interface_info(args: &[Value]) -> Result<Value, LispError> {
    // GNU's arity is exactly 1 (process.c:4652), and `need_args' only checks a
    // MINIMUM -- so the obvious call accepted two arguments and returned data
    // where GNU signals wrong-number-of-arguments.  The generated arity table
    // already recorded (1, 1); only this call site disagreed.
    need_arg_range("network-interface-info", args, 1, 1)?;
    let name = string_text(&args[0])?;

    #[cfg(not(target_os = "macos"))]
    {
        // STILL THE CHEAT ON THIS PLATFORM, and deliberately labelled as such.
        // GNU does NOT compile the body out on GNU/Linux: SIOCGIFFLAGS,
        // SIOCGIFADDR, SIOCGIFNETMASK, SIOCGIFBRDADDR and SIOCGIFHWADDR are
        // all defined there and process.c:4518 returns a full result
        // including the hardware address.  Finding 111 is fixed for macOS
        // only; a Linux port needs the ifr_hwaddr branch rather than the
        // getifaddrs walk below.
        let _ = name;
        Ok(Value::Nil)
    }

    #[cfg(target_os = "macos")]
    {
        let mut request: libc::ifreq = unsafe { std::mem::zeroed() };
        if name.len() >= request.ifr_name.len() {
            // process.c:4473.
            return Err(LispError::Signal("interface name too long".into()));
        }
        for (slot, byte) in request.ifr_name.iter_mut().zip(name.bytes()) {
            *slot = byte as libc::c_char;
        }

        // SAFETY: a plain socket used only as an ioctl handle; closed below.
        let socket = unsafe { libc::socket(libc::AF_INET, libc::SOCK_STREAM, 0) };
        if socket < 0 {
            return Ok(Value::Nil);
        }
        let mut any = false;
        let size = std::mem::size_of::<libc::ifreq>();

        // FLAGS first, because the result list is consed in reverse.
        let mut flags_value = Value::Nil;
        let mut probe = request;
        // SAFETY: `probe' is a fully initialised ifreq of the size encoded in
        // the request number, and `socket' is live for every call below.
        if unsafe { libc::ioctl(socket, iowr(b'i', 17, size), &mut probe) } == 0 {
            any = true;
            // process.c:4494: ifr_flags is a short, so widen without sign
            // extension before testing bits.
            let mut flags = unsafe { probe.ifr_ifru.ifru_flags } as u16 as libc::c_int;
            let mut items = Vec::new();
            for (bit, symbol) in IFFLAG_TABLE {
                if flags == 0 {
                    break;
                }
                if flags & bit != 0 {
                    items.push(Value::symbol(symbol));
                    flags -= bit;
                }
            }
            // process.c:4506 names leftover bits by their bit NUMBER.
            for ordinal in 0..32 {
                if flags == 0 {
                    break;
                }
                if flags & 1 != 0 {
                    items.push(Value::Integer(ordinal));
                }
                flags >>= 1;
            }
            items.reverse();
            flags_value = Value::list(items);
        }

        // HWADDR: macOS has no SIOCGIFHWADDR, so GNU walks getifaddrs looking
        // for the AF_LINK entry (process.c:4531-4552).
        // process.c does NOT set `any' in the getifaddrs branch; only the
        // ioctl branches count toward it, so an interface yielding only a MAC
        // still answers nil overall.  Mirrored deliberately.
        let hwaddr_value = link_layer_address(&name);

        let mut address_for = |number: u8| -> Value {
            let mut probe = request;
            // SAFETY: as above.
            if unsafe { libc::ioctl(socket, iowr(b'i', number, size), &mut probe) } == 0 {
                any = true;
                // SAFETY: the kernel fills ifru_addr for these requests.
                return sockaddr_storage_to_lisp(unsafe { &probe.ifr_ifru.ifru_addr });
            }
            Value::Nil
        };
        let netmask_value = address_for(37);
        let broadcast_value = address_for(35);
        let addr_value = address_for(33);

        // SAFETY: `socket' was opened above and is not used after this.
        unsafe { libc::close(socket) };

        if !any {
            return Ok(Value::Nil);
        }
        Ok(Value::list([
            addr_value,
            broadcast_value,
            netmask_value,
            hwaddr_value,
            flags_value,
        ]))
    }
}

/// process.c:4531: the AF_LINK entry whose name matches and whose link-layer
/// address is exactly 6 bytes becomes (SA_FAMILY . [b0 .. b5]).
#[cfg(target_os = "macos")]
fn link_layer_address(name: &str) -> Value {
    let mut list: *mut libc::ifaddrs = std::ptr::null_mut();
    // SAFETY: getifaddrs allocates the list; freeifaddrs releases it below.
    if unsafe { libc::getifaddrs(&mut list) } != 0 {
        return Value::Nil;
    }
    let mut result = Value::Nil;
    let mut entry = list;
    while !entry.is_null() {
        // SAFETY: the list is NUL-terminated by a null ifa_next.
        let current = unsafe { &*entry };
        entry = current.ifa_next;
        if current.ifa_addr.is_null() {
            continue;
        }
        // SAFETY: ifa_addr is non-null and at least a sockaddr header.
        let family = unsafe { (*current.ifa_addr).sa_family } as libc::c_int;
        if family != libc::AF_LINK {
            continue;
        }
        // SAFETY: ifa_name is a NUL-terminated C string owned by the list.
        let entry_name = unsafe { std::ffi::CStr::from_ptr(current.ifa_name) };
        if entry_name.to_bytes() != name.as_bytes() {
            continue;
        }
        // SAFETY: AF_LINK means the address is a sockaddr_dl.  Read it
        // unaligned through a raw pointer rather than forming a reference:
        // the kernel's sockaddr_dl is VARIABLE-LENGTH and an entry shorter
        // than the declared struct would make a reference invalid.
        let link_pointer = current.ifa_addr.cast::<libc::sockaddr_dl>();
        let link = unsafe { std::ptr::read_unaligned(link_pointer) };
        if link.sdl_alen != 6 {
            continue;
        }
        // process.c:4548 uses LLADDR(sdl), i.e. `sdl_data + sdl_nlen' --
        // POINTER ARITHMETIC into a variable-length tail.  Indexing the
        // libc-declared `[c_char; 12]' instead panics for any interface whose
        // name is 7 bytes or longer and which has a 6-byte MAC: `bridge0'
        // needs indices 7..=12 out of a declared length of 12.  That is a
        // hard crash, not a wrong answer, and no test reached it because
        // `lo0' has no MAC and `en0' has a 3-byte name.
        let address = unsafe {
            std::ptr::addr_of!((*link_pointer).sdl_data)
                .cast::<u8>()
                .add(link.sdl_nlen as usize)
        };
        let bytes: Vec<Value> = (0..6)
            .map(|offset| Value::Integer(unsafe { *address.add(offset) } as i64))
            .collect();
        let mut vector = vec![Value::symbol("vector-literal")];
        vector.extend(bytes);
        result = Value::cons(Value::Integer(family as i64), Value::list(vector));
        break;
    }
    // SAFETY: `list' came from a successful getifaddrs and is freed once.
    unsafe { libc::freeifaddrs(list) };
    result
}

fn network_interface_list(args: &[Value]) -> Result<Value, LispError> {
    need_arg_range("network-interface-list", args, 0, 2)?;
    let full = args.first().is_some_and(Value::is_truthy);
    let family = AddressFamily::parse(args.get(1))?;
    let Ok(interfaces) = if_addrs::get_if_addrs() else {
        return Ok(Value::Nil);
    };
    Ok(Value::list(interfaces.into_iter().filter_map(
        |interface| {
            let (ip, mask) = match interface.addr {
                if_addrs::IfAddr::V4(address) => (
                    std::net::IpAddr::V4(address.ip),
                    std::net::IpAddr::V4(address.netmask),
                ),
                if_addrs::IfAddr::V6(address) => (
                    std::net::IpAddr::V6(address.ip),
                    std::net::IpAddr::V6(address.netmask),
                ),
            };
            family.includes(ip).then(|| {
                let ip_value = sockaddr_vector(std::net::SocketAddr::new(ip, 0));
                if full {
                    Value::list([
                        Value::string(&interface.name),
                        ip_value,
                        sockaddr_vector(std::net::SocketAddr::new(
                            interface_broadcast(ip, mask),
                            0,
                        )),
                        sockaddr_vector(std::net::SocketAddr::new(mask, 0)),
                    ])
                } else {
                    Value::cons(Value::string(&interface.name), ip_value)
                }
            })
        },
    )))
}

define_dispatch!(
    pub(super) fn call(
        interp: &mut Interpreter,
        name: &str,
        args: &[Value],
        env: &mut crate::lisp::types::Env,
    ) -> Result<Value, LispError> {
        match name {
            "set-buffer" => {
                need_args(name, args, 1)?;
                let id = interp.resolve_buffer_id(&args[0])?;
                interp.set_current_buffer_id(id)?;
                Ok(Value::buffer(id, interp.buffer.name.clone()))
            }
            "buffer-file-name" => {
                need_arg_range(name, args, 0, 1)?;
                let requested = args.first().filter(|value| !value.is_nil());
                if let Some(Value::Buffer(buffer)) = requested
                    && !interp.has_buffer_id(buffer.id)
                    && let Some(file) = interp.killed_buffer_file_name(buffer.id)
                {
                    return Ok(file
                        .clone()
                        .map(|value| Value::String(value.into()))
                        .unwrap_or(Value::Nil));
                }
                let buffer_id = if let Some(buffer) = requested {
                    interp.resolve_buffer_id(buffer)?
                } else {
                    interp.current_buffer_id()
                };
                // `buffer-file-name' is a per-buffer special variable.  A
                // dynamic binding in the current buffer is observable both
                // through the variable and through this primitive; reading
                // only the host Buffer field lost bindings used by ordinary
                // save code (including backup creation).
                if buffer_id == interp.current_buffer_id() {
                    return Ok(interp
                        .lookup_var("buffer-file-name", env)
                        .unwrap_or(Value::Nil));
                }
                Ok(interp
                    .get_buffer_by_id(buffer_id)
                    .and_then(|buffer| buffer.file.clone())
                    .map(|value| Value::String(value.into()))
                    .unwrap_or(Value::Nil))
            }
            "visited-file-modtime" => Ok(interp
                .buffer
                .visited_file_modtime()
                .and_then(|modtime| system_time_list_value(modtime.modified).ok())
                .unwrap_or(Value::Integer(0))),
            "verify-visited-file-modtime" => {
                need_arg_range(name, args, 0, 1)?;
                let buffer_id = match args.first() {
                    None | Some(Value::Nil) => interp.current_buffer_id(),
                    Some(buffer) => interp.resolve_buffer_id(buffer)?,
                };
                let remote_visit = interp.buffer_remote_prefix(buffer_id).is_some();
                let Some(buffer) = interp.get_buffer_by_id(buffer_id) else {
                    return Ok(Value::Nil);
                };
                let Some(path) = buffer.file.as_deref() else {
                    return Ok(Value::T);
                };
                let current = file_modtime(path)?;
                let visited = buffer.visited_file_modtime();
                // GNU's unknown timestamp sentinel means there is nothing to
                // verify, so the file is considered unchanged.
                if visited.is_none() {
                    return Ok(Value::T);
                }
                // Tramp reports remote modification times with one-second
                // resolution; a same-second rewrite looks unchanged.
                let unchanged = if remote_visit {
                    modtimes_equal_whole_seconds(&visited, &current)
                } else {
                    visited == current
                };
                Ok(if unchanged { Value::T } else { Value::Nil })
            }
            "set-visited-file-modtime" => {
                if args.len() > 1 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let modtime = match args.first() {
                    None | Some(Value::Nil) => {
                        if let Some(path) = interp.buffer.file.clone() {
                            file_modtime(&path)?
                        } else {
                            None
                        }
                    }
                    Some(Value::Integer(0)) => None,
                    Some(value) => Some(file_modtime_from_value(interp, value)?),
                };
                interp.buffer.set_visited_file_modtime(modtime);
                Ok(Value::Nil)
            }
            "set-buffer-major-mode" => {
                need_args(name, args, 1)?;
                let buffer_id = interp.resolve_buffer_id(&args[0])?;
                let buffer_name = interp
                    .get_buffer_by_id(buffer_id)
                    .map(|buffer| buffer.name.clone())
                    .ok_or_else(|| {
                        LispError::Signal("Attempt to set major mode for a dead buffer".into())
                    })?;
                let mode = if buffer_name == "*scratch*" {
                    interp
                        .lookup_var("initial-major-mode", env)
                        .unwrap_or(Value::Nil)
                } else {
                    super::call(
                        interp,
                        "default-value",
                        &[Value::Symbol("major-mode".into())],
                        env,
                    )?
                };
                if mode.is_nil() {
                    return Ok(Value::Nil);
                }
                let saved_buffer_id = interp.current_buffer_id();
                interp.set_current_buffer_id(buffer_id)?;
                let result = interp.call_function_value(mode, None, &[], env);
                if interp.has_buffer_id(saved_buffer_id) {
                    interp.set_current_buffer_id(saved_buffer_id)?;
                }
                result?;
                Ok(Value::Nil)
            }
            "get-file-buffer" => {
                need_args(name, args, 1)?;
                let expanded = string_text(&super::call(
                    interp,
                    "expand-file-name",
                    &[args[0].clone()],
                    env,
                )?)?;
                Ok(buffer_visiting_exact_file_name(interp, &expanded)
                    .map(|(id, name)| Value::buffer(id, name))
                    .unwrap_or(Value::Nil))
            }
            "get-truename-buffer" => {
                need_args(name, args, 1)?;
                let file = string_text(&args[0])?;
                Ok(interp
                    .buffer_list
                    .iter()
                    .find_map(|(id, name)| {
                        interp
                            .get_buffer_by_id(*id)
                            .and_then(|buffer| buffer.file_truename.as_deref())
                            .filter(|truename| *truename == file)
                            .map(|_| Value::buffer(*id, name.clone()))
                    })
                    .unwrap_or(Value::Nil))
            }
            "find-buffer" => {
                need_args(name, args, 2)?;
                let variable = args[0].as_symbol()?;
                Ok(interp
                    .buffer_list
                    .iter()
                    .find_map(|(id, name)| {
                        let value = match variable {
                            "buffer-file-name" => interp.get_buffer_by_id(*id).and_then(|buffer| {
                                buffer.file.clone().map(|value| Value::String(value.into()))
                            }),
                            "buffer-file-truename" => {
                                interp.get_buffer_by_id(*id).and_then(|buffer| {
                                    buffer
                                        .file_truename
                                        .clone()
                                        .map(|value| Value::String(value.into()))
                                })
                            }
                            _ => interp.buffer_local_value(*id, variable),
                        };
                        value
                            .filter(|value| values_equal(interp, value, &args[1]))
                            .map(|_| Value::buffer(*id, name.clone()))
                    })
                    .unwrap_or(Value::Nil))
            }
            "expand-file-name" => {
                if args.is_empty() || args.len() > 2 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let path = string_text(&args[0])?;
                let base = match args.get(1) {
                    Some(value) if !value.is_nil() => Some(string_text(value)?),
                    _ => interp
                        .lookup_var("default-directory", env)
                        .and_then(|value| string_like(&value).map(|string| string.text)),
                };
                Ok(string_like_value(
                    expand_file_name_runtime(interp, env, &path, base.as_deref())?,
                    Vec::new(),
                ))
            }
            "substitute-in-file-name" => {
                need_args(name, args, 1)?;
                Ok(Value::String(
                    substitute_in_file_name_in_env(interp, env, &string_text(&args[0])?).into(),
                ))
            }
            "file-name-directory" => {
                need_args(name, args, 1)?;
                Ok(file_name_directory(&string_text(&args[0])?)
                    .map(|value| Value::String(value.into()))
                    .unwrap_or(Value::Nil))
            }
            "file-name-nondirectory" => {
                need_args(name, args, 1)?;
                Ok(Value::String(
                    file_name_nondirectory(&string_text(&args[0])?).into(),
                ))
            }
            "file-name-as-directory" => {
                need_args(name, args, 1)?;
                Ok(Value::String(
                    file_name_as_directory(&string_text(&args[0])?).into(),
                ))
            }
            "directory-file-name" => {
                need_args(name, args, 1)?;
                Ok(Value::String(
                    directory_file_name(&string_text(&args[0])?).into(),
                ))
            }
            "directory-name-p" => {
                need_args(name, args, 1)?;
                Ok(if directory_name_p(&string_text(&args[0])?) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "file-name-absolute-p" => {
                need_args(name, args, 1)?;
                Ok(if file_name_absolute_p(&string_text(&args[0])?) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "file-name-case-insensitive-p" => {
                need_args(name, args, 1)?;
                // fileio.c:2689.  CHECK_STRING, expand, then walk up the tree
                // until `file_name_case_insensitive_err' is conclusive: it
                // returns negative for case-insensitive, 0 for sensitive, and
                // a positive errno for "could not tell", in which case GNU
                // retries on the parent.  The walk stops when the parent stops
                // changing -- which is why a missing path answers nil rather
                // than inheriting the root's answer: `file-name-directory' of
                // "/nope/deep/" is itself, so the second hop already matches.
                //
                // The constant nil this replaces made
                // files-tests-file-name-non-special-file-name-case-insensitive-p
                // pass trivially, because that test compares the predicate's
                // handler and non-handler answers against each other and any
                // constant satisfies it (audit finding 108).
                let mut path = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
                validate_file_name(&path)?;
                loop {
                    let err = super::super::system::file_name_case_insensitive_err(&path);
                    if err <= 0 {
                        return Ok(if err < 0 { Value::T } else { Value::Nil });
                    }
                    match super::super::system::file_name_directory(&path) {
                        Some(parent) if parent != path => path = parent,
                        _ => return Ok(Value::Nil),
                    }
                }
            }
            "file-name-concat" => Ok(Value::String(
                file_name_concat(
                    &args
                        .iter()
                        .filter(|value| !value.is_nil())
                        .map(string_text)
                        .collect::<Result<Vec<_>, _>>()?,
                )
                .into(),
            )),
            "find-file-name-handler" => {
                need_args(name, args, 2)?;
                let file = string_text(&args[0])?;
                let operation = args[1].as_symbol()?;
                Ok(find_file_name_handler(interp, env, &file, operation)?.unwrap_or(Value::Nil))
            }
            "unhandled-file-name-directory" => {
                need_args(name, args, 1)?;
                Ok(Value::String(
                    file_name_as_directory(&string_text(&args[0])?).into(),
                ))
            }
            "get-load-suffixes" => {
                need_args(name, args, 0)?;
                get_load_suffixes_value(interp, env)
            }
            "load" => {
                if args.is_empty() || args.len() > 5 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let target = string_text(&args[0])?;
                let noerror = args.get(1).is_some_and(Value::is_truthy);
                let Some(path) = resolve_load_target_in_env(interp, &target, env) else {
                    if noerror {
                        return Ok(Value::Nil);
                    }
                    return Err(LispError::SignalValue(Value::list([
                        Value::Symbol("file-missing".into()),
                        Value::String("Cannot open load file".into()),
                        Value::String("No such file or directory".into()),
                        Value::String(target.into()),
                    ])));
                };
                let path = maybe_swap_for_native(interp, &target, &path, env)?;
                interp.load_resolved_path(&path, env, args.get(2).is_some_and(Value::is_truthy))
            }
            "locate-file-internal" => {
                need_args(name, args, 4)?;
                locate_file_internal(interp, &args[0], &args[1], &args[2], &args[3], env)
            }
            "directory-files" => {
                need_arg_range(name, args, 1, 5)?;
                let directory = string_text(&args[0])?;
                let full = args.get(1).is_some_and(Value::is_truthy);
                let matcher = args.get(2).filter(|value| !value.is_nil());
                let nosort = args.get(3).is_some_and(Value::is_truthy);
                let count = args
                    .get(4)
                    .filter(|value| !value.is_nil())
                    .map(Value::as_integer)
                    .transpose()?
                    .map(|value| value.max(0) as usize);
                directory_files(interp, &directory, full, matcher, nosort, count, env)
            }
            "directory-files-and-attributes" => {
                need_arg_range(name, args, 1, 6)?;
                let directory = string_text(&args[0])?;
                let full = args.get(1).is_some_and(Value::is_truthy);
                let id_format = args.get(4).cloned().unwrap_or(Value::Nil);
                let mut directory_files_args = vec![
                    args[0].clone(),
                    args.get(1).cloned().unwrap_or(Value::Nil),
                    args.get(2).cloned().unwrap_or(Value::Nil),
                    args.get(3).cloned().unwrap_or(Value::Nil),
                ];
                if let Some(count) = args.get(5) {
                    directory_files_args.push(count.clone());
                }
                let file_names =
                    super::call(interp, "directory-files", &directory_files_args, env)?;
                let entries = file_names
                    .to_vec()?
                    .into_iter()
                    .map(|name_value| {
                        let name_text = string_text(&name_value)?;
                        let attribute_path = if full {
                            name_text.clone()
                        } else {
                            Path::new(&resolve_file_name_in_env(interp, env, &directory))
                                .join(&name_text)
                                .display()
                                .to_string()
                        };
                        let attributes = super::call(
                            interp,
                            "file-attributes",
                            &[Value::String(attribute_path.into()), id_format.clone()],
                            env,
                        )?;
                        Ok(Value::cons(name_value, attributes))
                    })
                    .collect::<Result<Vec<_>, LispError>>()?;
                Ok(Value::list(entries))
            }
            "file-directory-p" | "file-accessible-directory-p" => {
                need_args(name, args, 1)?;
                let requested = string_text(&args[0])?;
                let path = resolve_file_name_in_env(interp, env, &requested);
                validate_file_name(&path)?;
                Ok(
                    if fs::metadata(&path)
                        .map(|metadata| metadata.is_dir())
                        .unwrap_or(false)
                        && (name == "file-directory-p" || file_readable_p(&path))
                    {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
            "file-readable-p" => {
                need_args(name, args, 1)?;
                let path = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
                Ok(if file_readable_p(&path) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "access-file" => {
                need_args(name, args, 2)?;
                let requested = string_text(&args[0])?;
                let path = resolve_file_name_in_env(interp, env, &requested);
                if file_readable_p(&path) {
                    Ok(Value::Nil)
                } else {
                    Err(LispError::SignalValue(file_error_with_detail_value(
                        &string_text(&args[1])?,
                        "Permission denied",
                        &requested,
                    )))
                }
            }
            "file-regular-p" => {
                need_args(name, args, 1)?;
                let path = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
                Ok(
                    if fs::metadata(&path)
                        .map(|metadata| metadata.is_file())
                        .unwrap_or(false)
                    {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
            "file-writable-p" => {
                need_args(name, args, 1)?;
                let path = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
                Ok(if file_writable_p(&path) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "file-exists-p" => {
                need_args(name, args, 1)?;
                let path = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
                validate_file_name(&path)?;
                Ok(if fs::metadata(path).is_ok() {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "file-executable-p" => {
                need_args(name, args, 1)?;
                let path = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
                Ok(if file_executable_p(&path) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "file-attributes" => {
                need_arg_range(name, args, 1, 3)?;
                // GNU's Ffile_attributes treats nil like a file name that
                // does not exist.  bookmark.el relies on this while its
                // optional timestamp is still unset.
                if args[0].is_nil() {
                    return Ok(Value::Nil);
                }
                let path = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
                validate_file_name(&path)?;
                let metadata = match fs::symlink_metadata(&path) {
                    Ok(metadata) => metadata,
                    Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Value::Nil),
                    Err(error) => return Err(LispError::Signal(error.to_string())),
                };
                let file_type = metadata.file_type();
                let type_value = if file_type.is_dir() {
                    Value::T
                } else if file_type.is_symlink() {
                    fs::read_link(&path)
                        .ok()
                        .map(|target| Value::String(target.to_string_lossy().into_owned().into()))
                        .unwrap_or(Value::String(path.clone().into()))
                } else {
                    Value::Nil
                };
                let accessed = metadata
                    .accessed()
                    .ok()
                    .map(system_time_list_value)
                    .transpose()?
                    .unwrap_or(Value::Integer(0));
                let modified = metadata
                    .modified()
                    .ok()
                    .map(system_time_list_value)
                    .transpose()?
                    .unwrap_or(Value::Integer(0));
                // GNU's status-change field is Unix ctime, not birth/creation
                // time.  They diverge as soon as metadata or mtime changes.
                #[cfg(unix)]
                let changed = unix_time_list_value(metadata.ctime(), metadata.ctime_nsec());
                #[cfg(not(unix))]
                let changed = metadata
                    .created()
                    .ok()
                    .map(system_time_list_value)
                    .transpose()?
                    .unwrap_or_else(|| modified.clone());
                #[cfg(unix)]
                let (links, uid, gid, inode, device) = (
                    metadata.nlink() as i64,
                    metadata.uid() as i64,
                    metadata.gid() as i64,
                    metadata.ino() as i64,
                    metadata.dev() as i64,
                );
                #[cfg(not(unix))]
                let (links, uid, gid, inode, device) = (1, 0, 0, 0, 0);
                let string_ids =
                    args.get(1).and_then(|value| value.as_symbol().ok()) == Some("string");
                let user = if string_ids {
                    #[cfg(unix)]
                    {
                        Value::String(
                            user_name_from_uid(uid as u32)
                                .unwrap_or_else(|| uid.to_string())
                                .into(),
                        )
                    }
                    #[cfg(not(unix))]
                    {
                        Value::String(uid.to_string().into())
                    }
                } else {
                    Value::Integer(uid)
                };
                let group = if string_ids {
                    Value::String(
                        group_name_from_gid(gid)?
                            .unwrap_or_else(|| gid.to_string())
                            .into(),
                    )
                } else {
                    Value::Integer(gid)
                };
                Ok(Value::list([
                    type_value,
                    Value::Integer(links),
                    user,
                    group,
                    accessed,
                    modified,
                    changed,
                    Value::Integer(metadata.len() as i64),
                    // GNU dired.c renders the real lstat mode bits through
                    // filemodestring; fabricating a constant here diverges
                    // for every chmod'd file.
                    Value::String(file_mode_string_for_metadata(&metadata).into()),
                    Value::Nil,
                    Value::Integer(inode),
                    Value::Integer(device),
                ]))
            }
            "file-attributes-lessp" => {
                need_args(name, args, 2)?;
                super::call(
                    interp,
                    "string-lessp",
                    &[args[0].car()?, args[1].car()?],
                    env,
                )
            }
            "system-users" => {
                need_args(name, args, 0)?;
                let mut users = system_user_names()
                    .into_iter()
                    .map(|value| Value::String(value.into()))
                    .collect::<Vec<_>>();
                if users.is_empty() {
                    users.push(
                        interp
                            .lookup_var("user-real-login-name", env)
                            .unwrap_or(Value::Nil),
                    );
                }
                Ok(Value::list(users))
            }
            "system-groups" => {
                need_args(name, args, 0)?;
                Ok(Value::list(
                    system_group_names()
                        .into_iter()
                        .map(|value| Value::String(value.into())),
                ))
            }
            "car-less-than-car" => {
                need_args(name, args, 2)?;
                super::call(interp, "<", &[args[0].car()?, args[1].car()?], env)
            }
            "file-newer-than-file-p" => {
                need_args(name, args, 2)?;
                let first = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
                let second = resolve_file_name_in_env(interp, env, &string_text(&args[1])?);
                validate_file_name(&first)?;
                validate_file_name(&second)?;
                let Ok(first_modified) =
                    fs::metadata(first).and_then(|metadata| metadata.modified())
                else {
                    return Ok(Value::Nil);
                };
                let newer = match fs::metadata(second).and_then(|metadata| metadata.modified()) {
                    Ok(second_modified) => first_modified > second_modified,
                    Err(_) => true,
                };
                Ok(if newer { Value::T } else { Value::Nil })
            }
            // This macOS build has no SELinux support, and Emaxx does not expose
            // a host ACL API yet.  Keep GNU's platform-degraded return values on
            // the native side of the boundary.
            "file-acl" => {
                need_args(name, args, 1)?;
                let _ = string_text(&args[0])?;
                Ok(Value::Nil)
            }
            "set-file-acl" => {
                need_args(name, args, 2)?;
                let _ = string_text(&args[0])?;
                Ok(Value::Nil)
            }
            "file-selinux-context" => {
                need_args(name, args, 1)?;
                let _ = string_text(&args[0])?;
                Ok(Value::list([
                    Value::Nil,
                    Value::Nil,
                    Value::Nil,
                    Value::Nil,
                ]))
            }
            "set-file-selinux-context" => {
                need_args(name, args, 2)?;
                let _ = string_text(&args[0])?;
                Ok(Value::Nil)
            }
            "copy-file" => {
                need_arg_range(name, args, 2, 6)?;
                let source = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
                validate_file_name(&source)?;
                let mut target = resolve_file_name_in_env(interp, env, &string_text(&args[1])?);
                validate_file_name(&target)?;
                if directory_name_p(&target) {
                    target = file_name_concat(&[target, file_name_nondirectory(&source)]);
                }
                let target_existed = fs::symlink_metadata(&target).is_ok();
                if target_existed && args.get(2).is_none_or(Value::is_nil) {
                    return Err(file_operation_error(
                        "Copying file",
                        &std::io::Error::from(ErrorKind::AlreadyExists),
                        &target,
                    ));
                }
                fs::copy(&source, &target).map_err(|error| {
                    let path = if error.kind() == ErrorKind::NotFound
                        && fs::symlink_metadata(&source).is_err()
                    {
                        &source
                    } else {
                        &target
                    };
                    file_operation_error("Copying file", &error, path)
                })?;
                // fileio.c:Fcopy_file KEEP-TIME: give the copy the same
                // last-modified (and access) time as the original.  Dired's
                // default `dired-copy-preserve-time' rides on this.
                if args.get(3).is_some_and(Value::is_truthy)
                    && let Ok(metadata) = fs::metadata(&source)
                {
                    let mut times = fs::FileTimes::new();
                    if let Ok(modified) = metadata.modified() {
                        times = times.set_modified(modified);
                    }
                    if let Ok(accessed) = metadata.accessed() {
                        times = times.set_accessed(accessed);
                    }
                    // fileio.c only warns ("Cannot set file date") when this
                    // fails; the copy itself already succeeded.
                    if let Ok(file) = fs::File::options().write(true).open(&target) {
                        let _ = file.set_times(times);
                    }
                }
                if !target_existed {
                    dispatch_file_notification(interp, env, &target, "created")?;
                }
                dispatch_file_notification(interp, env, &target, "changed")?;
                Ok(Value::Nil)
            }
            "rename-file" => {
                need_arg_range(name, args, 2, 3)?;
                let source = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
                validate_file_name(&source)?;
                let mut target = resolve_file_name_in_env(interp, env, &string_text(&args[1])?);
                validate_file_name(&target)?;
                if directory_name_p(&target) {
                    target = file_name_concat(&[target, file_name_nondirectory(&source)]);
                }
                let target_metadata = fs::symlink_metadata(&target);
                if target_metadata.is_ok() && args.get(2).is_none_or(Value::is_nil) {
                    return Err(file_operation_error(
                        "Renaming file",
                        &std::io::Error::from(ErrorKind::AlreadyExists),
                        &target,
                    ));
                }
                fs::rename(&source, &target).map_err(|error| {
                    let path = if error.kind() == ErrorKind::NotFound
                        && fs::symlink_metadata(&source).is_err()
                    {
                        &source
                    } else {
                        &target
                    };
                    file_operation_error("Renaming file", &error, path)
                })?;
                interp.queue_file_rename_notification(&source, &target);
                interp.invalidate_file_notify_watches_for_path(&source);
                Ok(Value::Nil)
            }
            "system-move-file-to-trash" => {
                need_args(name, args, 1)?;
                let path = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
                validate_file_name(&path)?;
                if let Err(error) = fs::symlink_metadata(&path) {
                    return Err(removing_old_name_error(&path, &error));
                }
                move_to_system_trash(&path).map_err(|error| {
                    LispError::SignalValue(file_error_with_detail_value(
                        "Removing old name",
                        &error.to_string(),
                        &path,
                    ))
                })?;
                dispatch_file_notification(interp, env, &path, "deleted")?;
                interp.invalidate_file_notify_watches_for_path(&path);
                Ok(Value::Nil)
            }
            "delete-file-internal" => {
                need_args(name, args, 1)?;
                let path = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
                validate_file_name(&path)?;
                let removed = match fs::remove_file(&path) {
                    Ok(()) => true,
                    Err(error) if error.kind() == ErrorKind::NotFound => false,
                    Err(error) => return Err(LispError::Signal(error.to_string())),
                };
                if removed {
                    dispatch_file_notification(interp, env, &path, "deleted")?;
                    interp.invalidate_file_notify_watches_for_path(&path);
                }
                Ok(Value::Nil)
            }
            "delete-directory-internal" => {
                need_args(name, args, 1)?;
                let path = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
                validate_file_name(&path)?;
                fs::remove_dir(&path).map_err(|error| LispError::Signal(error.to_string()))?;
                interp.queue_directory_deletion_notification(&path);
                interp.invalidate_file_notify_watches_for_path(&path);
                Ok(Value::Nil)
            }

            "make-directory-internal" => {
                need_args(name, args, 1)?;
                let path = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
                validate_file_name(&path)?;
                fs::create_dir(&path).map_err(|error| LispError::Signal(error.to_string()))?;
                dispatch_file_notification(interp, env, &path, "created")?;
                Ok(Value::Nil)
            }
            "add-name-to-file" => {
                need_arg_range(name, args, 2, 3)?;
                let source = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
                let mut target = resolve_file_name_in_env(interp, env, &string_text(&args[1])?);
                if directory_name_p(&target) {
                    target = file_name_concat(&[target, file_name_nondirectory(&source)]);
                }
                if fs::symlink_metadata(&target).is_ok() {
                    let accept_existing = match args.get(2) {
                        Some(Value::Integer(_)) => call_named_function(
                            interp,
                            "yes-or-no-p",
                            &[Value::String(
                                format!("File {target} exists; keep it? ").into(),
                            )],
                            env,
                        )?
                        .is_truthy(),
                        Some(value) => value.is_truthy(),
                        None => false,
                    };
                    if accept_existing {
                        return Ok(Value::Nil);
                    }
                    return Err(file_operation_error(
                        "Adding new name",
                        &std::io::Error::from(ErrorKind::AlreadyExists),
                        &target,
                    ));
                }
                fs::hard_link(&source, &target)
                    .map_err(|error| file_operation_error("Adding new name", &error, &target))?;
                Ok(Value::Nil)
            }
            "make-temp-file-internal" => {
                need_args(name, args, 4)?;
                let prefix = string_text(&args[0])?;
                let suffix = string_text(&args[2])?;
                validate_file_name(&prefix)?;
                validate_file_name(&suffix)?;
                Ok(Value::String(
                    make_temp_file_internal(&prefix, &args[1], &suffix, args.get(3))?.into(),
                ))
            }
            "file-locked-p" => {
                need_args(name, args, 1)?;
                file_locked_p(interp, env, &string_text(&args[0])?)
            }
            "memory-info" => {
                need_args(name, args, 0)?;
                #[cfg(target_os = "linux")]
                {
                    let mut info = std::mem::MaybeUninit::<libc::sysinfo>::uninit();
                    // SAFETY: sysinfo initializes the supplied structure on
                    // success, and we read it only after a zero return value.
                    if unsafe { libc::sysinfo(info.as_mut_ptr()) } == 0 {
                        // SAFETY: established by the successful sysinfo call.
                        let info = unsafe { info.assume_init() };
                        let units = u64::from(info.mem_unit);
                        return Ok(Value::list([
                            Value::Integer((info.totalram.saturating_mul(units) / 1024) as i64),
                            Value::Integer((info.freeram.saturating_mul(units) / 1024) as i64),
                            Value::Integer((info.totalswap.saturating_mul(units) / 1024) as i64),
                            Value::Integer((info.freeswap.saturating_mul(units) / 1024) as i64),
                        ]));
                    }
                }
                Ok(Value::Nil)
            }
            "lock-file" => {
                need_args(name, args, 1)?;
                lock_file_path(interp, env, &string_text(&args[0])?)?;
                Ok(Value::Nil)
            }
            "unlock-file" => {
                need_args(name, args, 1)?;
                unlock_file_path(interp, env, &string_text(&args[0])?)
            }
            "write-region" => {
                if args.len() < 3 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                write_region_value(interp, args, env)
            }
            #[cfg(target_os = "linux")]
            "inotify-add-watch" => {
                need_args(name, args, 3)?;
                // Finotify_add_watch converts ASPECT to its mask before
                // CHECK_STRING examines FILE-NAME, and symbol_to_inotifymask
                // reports any entry it cannot map -- a non-symbol included --
                // as an unknown aspect with errno set to EINVAL.
                let aspects = if args[1].is_nil() {
                    Vec::new()
                } else if args[1].cons_values().is_some() {
                    args[1].to_vec()?
                } else {
                    vec![args[1].clone()]
                };
                let flags = aspects
                    .iter()
                    .map(|aspect| {
                        aspect.as_symbol().map(str::to_string).map_err(|_| {
                            crate::lisp::primitives::file_notify_error_with_errno(
                                "Unknown aspect",
                                &std::io::Error::from_raw_os_error(libc::EINVAL),
                                aspect.clone(),
                            )
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                interp.validate_inotify_aspects(&flags)?;
                let path = string_text(&args[0])?;
                interp.register_inotify_file_notify_watch(path, flags, args[2].clone())
            }
            #[cfg(target_os = "linux")]
            "inotify-rm-watch" => {
                need_args(name, args, 1)?;
                let valid = args[0].cons_values().is_some_and(|(watch, id)| {
                    matches!(watch, Value::Integer(value) if value >= 0 && value <= i64::from(i32::MAX))
                        && matches!(id, Value::Integer(value) if value >= 0)
                });
                if !valid {
                    // report_file_notify_error renders whatever errno the
                    // preceding host call left behind, so GNU's own text here
                    // depends on session history; only the shape is stable.
                    return Err(crate::lisp::primitives::file_notify_error_with_errno(
                        "Invalid descriptor ",
                        &std::io::Error::last_os_error(),
                        args[0].clone(),
                    ));
                }
                interp.remove_inotify_file_notify_watch(&args[0])?;
                Ok(Value::T)
            }
            #[cfg(target_os = "linux")]
            "inotify-valid-p" => {
                need_args(name, args, 1)?;
                Ok(if interp.inotify_file_notify_watch_is_active(&args[0]) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "kqueue-add-watch" => {
                need_args(name, args, 3)?;
                let path = directory_file_name(&resolve_file_name_in_env(
                    interp,
                    env,
                    &string_text(&args[0])?,
                ));
                // kqueue.c signals file-missing before registering anything;
                // Emaxx accepted any path and returned a live descriptor
                // (finding 13's "never fails" half, per the second audit).
                if let Err(error) = std::fs::metadata(&path) {
                    return Err(file_operation_error("File does not exist", &error, &path));
                }
                let flags = args[1]
                    .to_vec()?
                    .into_iter()
                    .filter_map(|flag| flag.as_symbol().ok().map(str::to_string))
                    .collect::<Vec<_>>();
                if !function_value_p(interp, &args[2], env) {
                    return Err(wrong_type_argument("invalid-function", args[2].clone()));
                }
                // kqueue.c deliberately reserves fifty descriptors for the
                // rest of Emacs and reports `file-notify-error' before a
                // watch can exhaust the process limit.  Besides matching the
                // public failure contract, this keeps callers able to remove
                // every watch and clean up after the limit is reached.
                #[cfg(unix)]
                {
                    let mut limit = std::mem::MaybeUninit::<libc::rlimit>::uninit();
                    // SAFETY: getrlimit initializes LIMIT on success.
                    let maximum =
                        if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, limit.as_mut_ptr()) } == 0
                        {
                            // SAFETY: the successful call above initialized LIMIT.
                            unsafe { limit.assume_init() }.rlim_cur as usize
                        } else {
                            256
                        };
                    if maximum < 50 || maximum - 50 < interp.file_notify_watch_count() {
                        return Err(LispError::SignalValue(Value::list([
                            Value::Symbol("file-notify-error".into()),
                            Value::String(
                                "File watching not possible, no file descriptor left".into(),
                            ),
                            Value::Integer(interp.file_notify_watch_count() as i64),
                        ])));
                    }
                }
                // Watches taken from a remotely visited buffer model Tramp's
                // gio monitors: they outlive deletions of the watched file, so
                // they get no local path registration to invalidate.
                let remote_watch = parse_remote_file_name(&string_text(&args[0])?).is_some()
                    || interp
                        .buffer_remote_prefix(interp.current_buffer_id())
                        .is_some();
                #[cfg(target_os = "macos")]
                if !remote_watch {
                    let descriptor =
                        interp.register_kqueue_file_notify_watch(path, flags, args[2].clone())?;
                    return Ok(Value::Integer(descriptor));
                }
                let descriptor =
                    FILE_NOTIFY_DESCRIPTOR_COUNTER.fetch_add(1, AtomicOrdering::Relaxed) as i64;
                interp.register_file_notify_watch(
                    descriptor,
                    (!remote_watch).then_some(path),
                    flags,
                    args[2].clone(),
                )?;
                Ok(Value::Integer(descriptor))
            }
            "kqueue-rm-watch" => {
                need_args(name, args, 1)?;
                if !interp.remove_file_notify_watch(&args[0]) {
                    return Err(LispError::SignalValue(Value::list([
                        Value::Symbol("file-notify-error".into()),
                        Value::String("Not a watch descriptor".into()),
                        args[0].clone(),
                    ])));
                }
                Ok(Value::T)
            }
            "kqueue-valid-p" => {
                need_args(name, args, 1)?;
                Ok(if interp.file_notify_watch_is_active(&args[0]) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "default-file-modes" => {
                need_args(name, args, 0)?;
                Ok(Value::Integer(interp.default_file_modes))
            }
            "file-modes" => {
                need_arg_range(name, args, 1, 2)?;
                let path = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
                validate_file_name(&path)?;
                #[cfg(unix)]
                {
                    let metadata = if args.get(1).is_some_and(Value::is_truthy) {
                        fs::symlink_metadata(&path)
                    } else {
                        fs::metadata(&path)
                    };
                    Ok(metadata
                        .map(|metadata| {
                            Value::Integer((metadata.permissions().mode() & 0o7777) as i64)
                        })
                        .unwrap_or(Value::Nil))
                }
                #[cfg(not(unix))]
                {
                    Ok(if fs::metadata(&path).is_ok() {
                        Value::Integer(0)
                    } else {
                        Value::Nil
                    })
                }
            }
            "set-default-file-modes" => {
                need_args(name, args, 1)?;
                let mode = args[0].as_integer()?;
                interp.default_file_modes = mode & 0o777;
                Ok(Value::Nil)
            }
            "set-file-modes" => {
                need_args(name, args, 2)?;
                let path = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
                validate_file_name(&path)?;
                let mode = args[1].as_integer()?;
                #[cfg(unix)]
                {
                    let mut permissions = fs::metadata(&path)
                        .map_err(|error| file_operation_error("Setting file modes", &error, &path))?
                        .permissions();
                    permissions.set_mode(mode as u32);
                    fs::set_permissions(&path, permissions).map_err(|error| {
                        file_operation_error("Setting file modes", &error, &path)
                    })?;
                }
                dispatch_file_notification(interp, env, &path, "attribute-changed")?;
                Ok(Value::Nil)
            }
            "file-name-all-completions" => {
                need_args(name, args, 2)?;
                let prefix = string_text(&args[0])?;
                let directory = resolve_file_name_in_env(interp, env, &string_text(&args[1])?);
                // GNU opens the directory before synthesizing its `./' and `../'
                // candidates.  Returning those entries for a directory that
                // cannot be opened misleads partial completion into treating a
                // nonexistent component as an exact directory.
                let entries = std::fs::read_dir(&directory).map_err(|error| {
                    let rendered = error.to_string();
                    let detail = rendered
                        .split_once(" (os error")
                        .map(|(detail, _)| detail)
                        .unwrap_or(rendered.as_str());
                    LispError::SignalValue(Value::list([
                        Value::Symbol(
                            if error.kind() == ErrorKind::NotFound {
                                "file-missing"
                            } else {
                                "file-error"
                            }
                            .into(),
                        ),
                        Value::String("Opening directory".into()),
                        Value::String(detail.into()),
                        Value::String(directory.clone().into()),
                    ]))
                })?;
                let ignore_case = completion_ignores_case(interp, env);
                let regexp_list = interp
                    .lookup_var("completion-regexp-list", env)
                    .and_then(|value| value.to_vec().ok())
                    .unwrap_or_default();
                let matches = |candidate: &str| -> Result<bool, LispError> {
                    if !completion_matches_prefix(&prefix, candidate, ignore_case) {
                        return Ok(false);
                    }
                    for pattern in &regexp_list {
                        if !completion_regex_matches(interp, env, candidate, pattern)? {
                            return Ok(false);
                        }
                    }
                    Ok(true)
                };
                let mut names: Vec<String> = Vec::new();
                for (candidate, rendered) in [(".", "./"), ("..", "../")] {
                    if matches(candidate)? {
                        names.push(rendered.to_string());
                    }
                }
                for entry in entries.flatten() {
                    let file_name = entry.file_name().to_string_lossy().into_owned();
                    if !matches(&file_name)? {
                        continue;
                    }
                    // Directories (following symlinks) get a trailing slash.
                    let is_directory = std::fs::metadata(entry.path())
                        .map(|metadata| metadata.is_dir())
                        .unwrap_or(false);
                    names.push(if is_directory {
                        format!("{file_name}/")
                    } else {
                        file_name
                    });
                }
                names.sort();
                Ok(Value::list(
                    names.into_iter().map(|value| Value::String(value.into())),
                ))
            }
            "file-name-completion" => {
                need_arg_range(name, args, 2, 3)?;
                let directory = file_name_as_directory(&resolve_file_name_in_env(
                    interp,
                    env,
                    &string_text(&args[1])?,
                ));
                let names = match super::call(
                    interp,
                    "file-name-all-completions",
                    &[args[0].clone(), args[1].clone()],
                    env,
                ) {
                    Ok(names) => names,
                    Err(LispError::SignalValue(condition))
                        if condition
                            .to_vec()
                            .ok()
                            .and_then(|items| items.first().cloned())
                            .and_then(|head| head.as_symbol().ok().map(str::to_string))
                            .as_deref()
                            == Some("file-missing") =>
                    {
                        return Ok(Value::Nil);
                    }
                    Err(error) => return Err(error),
                };
                // GNU dired.c specbinds DIRECTORY while its optional predicate
                // examines each relative candidate.  Reuse the ordinary
                // completion engine so predicates, regexp filters, case folding,
                // and exact-match results share one implementation.
                let restore = interp.bind_special_dynamic(
                    "default-directory",
                    Value::String(directory.into()),
                    env,
                )?;
                let result = (|| {
                    let matches = all_completions(
                        interp,
                        &[
                            args[0].clone(),
                            names,
                            args.get(2).cloned().unwrap_or(Value::Nil),
                        ],
                        env,
                    )?
                    .to_vec()?;
                    // dired.c prefers ordinary entries over `.' / `..' and
                    // completion-ignored-extensions, but falls back to ignored
                    // candidates when they are the only matches.
                    let ignored_extensions = interp
                        .lookup_var("completion-ignored-extensions", env)
                        .and_then(|value| value.to_vec().ok())
                        .unwrap_or_default()
                        .into_iter()
                        .filter_map(|value| string_text(&value).ok())
                        .collect::<Vec<_>>();
                    let requested_len = string_text(&args[0])?.chars().count();
                    let preferred = matches
                        .iter()
                        .filter(|candidate| {
                            let Ok(candidate) = string_text(candidate) else {
                                return true;
                            };
                            candidate != "./"
                                && candidate != "../"
                                && !(candidate.chars().count() > requested_len
                                    && ignored_extensions
                                        .iter()
                                        .any(|suffix| candidate.ends_with(suffix)))
                        })
                        .cloned()
                        .collect::<Vec<_>>();
                    let candidates = if preferred.is_empty() {
                        matches
                    } else {
                        preferred
                    };
                    try_completion(
                        interp,
                        &[args[0].clone(), Value::list(candidates), Value::Nil],
                        env,
                    )
                })();
                let restore_result = interp.restore_special_dynamic(restore, env);
                match (result, restore_result) {
                    (Err(error), _) => Err(error),
                    (Ok(_), Err(error)) => Err(error),
                    (Ok(value), Ok(())) => Ok(value),
                }
            }
            "set-file-times" => {
                need_arg_range(name, args, 1, 3)?;
                let path = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
                validate_file_name(&path)?;
                let modified = match args.get(1) {
                    None | Some(Value::Nil) => SystemTime::now(),
                    Some(value) => file_modtime_from_value(interp, value)?.modified,
                };
                set_file_times_path(&path, modified, args.get(2).is_some_and(Value::is_truthy))?;
                dispatch_file_notification(interp, env, &path, "attribute-changed")?;
                Ok(Value::T)
            }
            "insert-file-contents" => insert_file_contents(interp, env, args, false),

            "file-system-info" => {
                need_args(name, args, 1)?;
                let path = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
                validate_file_name(&path)?;
                #[cfg(unix)]
                {
                    let path = CString::new(path.as_bytes())
                        .map_err(|_| LispError::TypeError("string".into(), "nul-byte".into()))?;
                    // SAFETY: `statvfs` initializes the pointed-to structure on
                    // success, and `path` is a live NUL-terminated C string.
                    let mut stats = unsafe { std::mem::zeroed::<libc::statvfs>() };
                    // SAFETY: both pointers remain valid for the duration of the
                    // call and `stats` has the platform's exact `statvfs` layout.
                    if unsafe { libc::statvfs(path.as_ptr(), &mut stats) } != 0 {
                        return Ok(Value::Nil);
                    }
                    let block_size = if stats.f_frsize == 0 {
                        stats.f_bsize
                    } else {
                        stats.f_frsize
                    };
                    let bytes = |blocks| {
                        normalize_bigint_value(BigInt::from(block_size) * BigInt::from(blocks))
                    };
                    Ok(Value::list([
                        bytes(stats.f_blocks),
                        bytes(stats.f_bfree),
                        bytes(stats.f_bavail),
                    ]))
                }
                #[cfg(not(unix))]
                {
                    Ok(Value::Nil)
                }
            }
            "file-symlink-p" => {
                need_args(name, args, 1)?;
                let path = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
                validate_file_name(&path)?;
                let target = fs::symlink_metadata(&path)
                    .ok()
                    .filter(|metadata| metadata.file_type().is_symlink())
                    .and_then(|_| fs::read_link(&path).ok());
                Ok(target
                    .map(|path| Value::String(path.to_string_lossy().into_owned().into()))
                    .unwrap_or(Value::Nil))
            }
            "make-symbolic-link" => {
                if args.len() < 2 || args.len() > 3 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let target = string_text(&args[0])?;
                let mut link = resolve_file_name_in_env(interp, env, &string_text(&args[1])?);
                if directory_name_p(&link) {
                    link = file_name_concat(&[link, file_name_nondirectory(&target)]);
                }
                validate_file_name(&target)?;
                validate_file_name(&link)?;
                if fs::symlink_metadata(&link).is_ok() {
                    let replace = match args.get(2) {
                        Some(Value::Integer(_)) => call_named_function(
                            interp,
                            "yes-or-no-p",
                            &[Value::String(
                                format!("File {link} exists; replace it? ").into(),
                            )],
                            env,
                        )?
                        .is_truthy(),
                        Some(value) => value.is_truthy(),
                        None => false,
                    };
                    if !replace {
                        return Err(file_operation_error(
                            "Making symbolic link",
                            &std::io::Error::from(ErrorKind::AlreadyExists),
                            &link,
                        ));
                    }
                    fs::remove_file(&link).map_err(|error| {
                        file_operation_error("Removing old name", &error, &link)
                    })?;
                }
                #[cfg(unix)]
                {
                    symlink(&target, &link).map_err(|error| {
                        file_operation_error("Making symbolic link", &error, &link)
                    })?;
                    Ok(Value::Nil)
                }
                #[cfg(not(unix))]
                {
                    Err(LispError::Signal("make-symbolic-link not supported".into()))
                }
            }
            "call-process" => {
                if args.is_empty() {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let program = string_text(&args[0])?;
                let input = match args.get(1) {
                    Some(value) if !value.is_nil() => match value {
                        Value::Integer(0) => None,
                        _ => {
                            let requested_infile = string_text(value)?;
                            let infile = unquote_local_file_name(&requested_infile)
                                .unwrap_or(requested_infile);
                            // GNU report_file_error: an unreadable INFILE is a
                            // `file-error' (epg's tty probe catches those).
                            Some(fs::read(&infile).map_err(|error| {
                                LispError::SignalValue(
                                    crate::lisp::primitives::file_io::file_error_value(
                                        &error.to_string(),
                                        &infile,
                                    ),
                                )
                            })?)
                        }
                    },
                    _ => None,
                };
                let destination = args.get(2).unwrap_or(&Value::Nil);
                let argv = args
                    .get(4..)
                    .unwrap_or(&[])
                    .iter()
                    .map(string_text)
                    .collect::<Result<Vec<_>, _>>()?;
                let process_output =
                    run_external_process(interp, &program, &argv, input.as_deref(), env)?;
                write_process_output(
                    interp,
                    destination,
                    &process_output.stdout,
                    &process_output.stderr,
                    "call-process",
                    args,
                    env,
                )?;
                Ok(Value::Integer(exit_status_code(&process_output.status)))
            }
            "make-process" | "make-pipe-process" => make_process_value(interp, env, args),
            "get-buffer-process" => {
                need_arg_range(name, args, 0, 1)?;
                let buffer_id = match args.first() {
                    None | Some(Value::Nil) => Some(interp.current_buffer_id()),
                    Some(buffer) if string_like(buffer).is_some() => string_like(buffer)
                        .and_then(|name| interp.find_buffer(&name.text).map(|(id, _)| id)),
                    Some(buffer) => Some(interp.resolve_buffer_id(buffer)?),
                };
                Ok(buffer_id
                    .and_then(|id| interp.process_value_for_buffer(id))
                    .unwrap_or(Value::Nil))
            }
            "process-buffer" => {
                need_args(name, args, 1)?;
                let process_id = interp.resolve_process_id(&args[0])?;
                Ok(interp
                    .process_buffer_id(process_id)
                    .and_then(|buffer_id| interp.buffer_identity_value(buffer_id))
                    .unwrap_or(Value::Nil))
            }
            "process-mark" => {
                need_args(name, args, 1)?;
                let process_id = interp.resolve_process_id(&args[0])?;
                let marker_id = interp
                    .process_mark_id(process_id)
                    .ok_or_else(|| LispError::Signal("Invalid process mark".into()))?;
                Ok(Value::Marker(marker_id))
            }
            "process-status" => {
                need_args(name, args, 1)?;
                let process_id = interp.resolve_process_id(&args[0])?;
                interp
                    .process_status_value(process_id)
                    .ok_or_else(|| LispError::Signal("Invalid process state".into()))
            }
            "process-exit-status" => {
                need_args(name, args, 1)?;
                let process_id = interp.resolve_process_id(&args[0])?;
                interp
                    .process_exit_status_value(process_id)
                    .ok_or_else(|| LispError::Signal("Invalid process state".into()))
            }
            "process-id" => {
                need_args(name, args, 1)?;
                let process_id = interp.resolve_process_id(&args[0])?;
                Ok(interp
                    .process_os_id(process_id)
                    .map(|pid| Value::Integer(i64::from(pid)))
                    .unwrap_or(Value::Nil))
            }
            "process-list" => {
                need_args(name, args, 0)?;
                Ok(interp.process_list_value())
            }
            "process-plist" => {
                need_args(name, args, 1)?;
                let process_id = interp.resolve_process_id(&args[0])?;
                Ok(interp.process_plist_value(process_id).unwrap_or(Value::Nil))
            }
            "set-process-plist" => {
                need_args(name, args, 2)?;
                let process_id = interp.resolve_process_id(&args[0])?;
                interp.set_process_plist_value(process_id, args[1].clone());
                Ok(args[1].clone())
            }
            "process-attributes" => {
                need_args(name, args, 1)?;
                Ok(process_attributes_value(args[0].as_integer()?))
            }
            "list-system-processes" => {
                need_args(name, args, 0)?;
                Ok(list_system_processes_value())
            }
            "process-coding-system" => {
                need_args(name, args, 1)?;
                let process_id = interp.resolve_process_id(&args[0])?;
                interp.process_coding_system(process_id)
            }
            "set-process-coding-system" => {
                need_arg_range(name, args, 1, 3)?;
                let process_id = interp.resolve_process_id(&args[0])?;
                let decoding = args.get(1).cloned().unwrap_or(Value::Nil);
                let encoding = args.get(2).cloned().unwrap_or(Value::Nil);
                interp.set_process_coding_system(process_id, decoding, encoding)?;
                Ok(Value::Nil)
            }
            "internal-default-process-filter" => {
                need_args(name, args, 2)?;
                let process_id = interp.resolve_process_id(&args[0])?;
                let text = string_text(&args[1])?;
                internal_default_process_filter(interp, process_id, &text)?;
                Ok(Value::Nil)
            }
            "process-filter" => {
                need_args(name, args, 1)?;
                let process_id = interp.resolve_process_id(&args[0])?;
                Ok(interp
                    .process_filter_value(process_id)
                    .expect("resolved process has process state"))
            }
            "set-process-filter" => {
                need_args(name, args, 2)?;
                let process_id = interp.resolve_process_id(&args[0])?;
                let filter = if args[1].is_nil() {
                    None
                } else {
                    Some(args[1].clone())
                };
                interp.set_process_filter(process_id, filter)?;
                Ok(if args[1].is_nil() {
                    Value::symbol("internal-default-process-filter")
                } else {
                    args[1].clone()
                })
            }
            "internal-default-process-sentinel" => {
                need_args(name, args, 2)?;
                let process_id = interp.resolve_process_id(&args[0])?;
                let message = string_text(&args[1])?;
                internal_default_process_sentinel(interp, process_id, &message)?;
                Ok(Value::Nil)
            }
            "set-process-sentinel" => {
                need_args(name, args, 2)?;
                let process_id = interp.resolve_process_id(&args[0])?;
                let sentinel = (!args[1].is_nil()).then(|| args[1].clone());
                interp.set_process_sentinel(process_id, sentinel);
                Ok(if args[1].is_nil() {
                    Value::symbol("internal-default-process-sentinel")
                } else {
                    args[1].clone()
                })
            }
            "set-process-buffer" => {
                need_args(name, args, 2)?;
                let process_id = interp.resolve_process_id(&args[0])?;
                let buffer_id = if args[1].is_nil() {
                    None
                } else {
                    Some(interp.resolve_buffer_id(&args[1])?)
                };
                interp.set_process_buffer_id(process_id, buffer_id);
                Ok(args[1].clone())
            }
            "process-sentinel" => {
                need_args(name, args, 1)?;
                let process_id = interp.resolve_process_id(&args[0])?;
                Ok(interp
                    .process_sentinel_value(process_id)
                    .expect("resolved process has process state"))
            }
            "process-thread" => {
                need_args(name, args, 1)?;
                let process_id = interp.resolve_process_id(&args[0])?;
                interp.process_thread_value(process_id)
            }
            "set-process-thread" => {
                need_args(name, args, 2)?;
                let process_id = interp.resolve_process_id(&args[0])?;
                let thread_id = if args[1].is_nil() {
                    None
                } else {
                    Some(interp.resolve_thread_id(&args[1])?)
                };
                interp.set_process_thread_id(process_id, thread_id)?;
                Ok(args[1].clone())
            }
            "process-datagram-address" => {
                need_args(name, args, 1)?;
                let process_id = interp.resolve_process_id(&args[0])?;
                Ok(interp
                    .process_datagram_address(process_id)?
                    .map(sockaddr_vector)
                    .unwrap_or(Value::Nil))
            }
            "set-process-datagram-address" => {
                need_args(name, args, 2)?;
                let process_id = interp.resolve_process_id(&args[0])?;
                let Some(address) = socket_addr_from_value(&args[1]) else {
                    return Ok(Value::Nil);
                };
                Ok(
                    if interp.set_process_datagram_address(process_id, address)? {
                        args[1].clone()
                    } else {
                        Value::Nil
                    },
                )
            }
            "process-type" => {
                need_args(name, args, 1)?;
                let process = process_designator_value(interp, args.first())?;
                let process_id = interp.resolve_process_id(&process)?;
                Ok(Value::symbol(interp.process_type_name(process_id)?))
            }
            "process-inherit-coding-system-flag" => {
                need_args(name, args, 1)?;
                let process_id = interp.resolve_process_id(&args[0])?;
                Ok(if interp.process_inherit_coding_system_flag(process_id)? {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "set-process-inherit-coding-system-flag" => {
                need_args(name, args, 2)?;
                let process_id = interp.resolve_process_id(&args[0])?;
                interp.set_process_inherit_coding_system_flag(process_id, args[1].is_truthy())?;
                Ok(args[1].clone())
            }
            "set-process-window-size" => {
                need_args(name, args, 3)?;
                let process_id = interp.resolve_process_id(&args[0])?;
                let height = u16::try_from(args[1].as_integer()?)
                    .map_err(|_| LispError::Signal("Args out of range".into()))?;
                let width = u16::try_from(args[2].as_integer()?)
                    .map_err(|_| LispError::Signal("Args out of range".into()))?;
                Ok(
                    if interp.set_process_window_size(process_id, height, width)? {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
            "process-running-child-p" => {
                need_arg_range(name, args, 0, 1)?;
                let process = process_designator_value(interp, args.first())?;
                let process_id = interp.resolve_process_id(&process)?;
                interp.process_running_child_value(process_id)
            }
            "process-name" => {
                need_args(name, args, 1)?;
                let process_id = interp.resolve_process_id(&args[0])?;
                Ok(interp
                    .process_name(process_id)
                    .map(|value| Value::String(value.into()))
                    .unwrap_or(Value::Nil))
            }
            "process-command" => {
                need_args(name, args, 1)?;
                let process_id = interp.resolve_process_id(&args[0])?;
                Ok(interp
                    .process_command_value(process_id)
                    .unwrap_or(Value::Nil))
            }
            "process-tty-name" => {
                need_arg_range(name, args, 1, 2)?;
                let process_id = interp.resolve_process_id(&args[0])?;
                if let Some(stream) = args.get(1).filter(|stream| stream.is_truthy())
                    && !matches!(stream.as_symbol(), Ok("stdin" | "stdout" | "stderr"))
                {
                    return Err(LispError::SignalValue(Value::list([
                        Value::Symbol("error".into()),
                        Value::String("Unknown stream".into()),
                        stream.clone(),
                    ])));
                }
                // process.c: the name of the process's terminal, nil for a
                // pipe.  The stale nil-for-everything answer here made
                // python.el treat pty processes as pipes and send a
                // 5 KB line into the canonical 1024-byte pty buffer -- the
                // exchange wedged both sides and python-tests.el never
                // finished (2026-08-24).
                Ok(interp
                    .process_tty_name(process_id, args.get(1))
                    .map(|name| Value::String(name.into()))
                    .unwrap_or(Value::Nil))
            }
            "get-process" => {
                need_args(name, args, 1)?;
                if matches!(&args[0], Value::Record(_)) {
                    return Ok(args[0].clone());
                }
                let requested = string_text(&args[0])?;
                Ok(interp
                    .find_process_id_by_name(&requested)
                    .map(Value::Record)
                    .unwrap_or(Value::Nil))
            }
            "process-contact" => {
                // GNU Fprocess_contact: the stored contact plist (p->childp)
                // is t for a real child and is returned as-is no matter the
                // KEY; for a network process KEY t returns the whole plist,
                // KEY nil the (HOST SERVICE) pair, any other KEY a plist_get.
                need_arg_range(name, args, 1, 3)?;
                let process_id = interp.resolve_process_id(&args[0])?;
                if matches!(args.get(1), Some(Value::Symbol(key)) if key == ":remote")
                    && let Some(address) = interp.process_datagram_address(process_id)?
                {
                    return Ok(sockaddr_vector(address));
                }
                let contact = interp
                    .process_contact_plist(process_id)
                    .unwrap_or(Value::Nil);
                if !matches!(contact, Value::Cons(_)) {
                    return Ok(contact);
                }
                match args.get(1) {
                    Some(Value::T) => Ok(contact),
                    None | Some(Value::Nil) if interp.is_serial_process(process_id) => {
                        Ok(Value::list([
                            contact_plist_get(&contact, ":port"),
                            contact_plist_get(&contact, ":speed"),
                        ]))
                    }
                    None | Some(Value::Nil) => Ok(Value::list([
                        contact_plist_get(&contact, ":host"),
                        contact_plist_get(&contact, ":service"),
                    ])),
                    Some(key) => Ok(key
                        .as_symbol()
                        .map(|key| contact_plist_get(&contact, key))
                        .unwrap_or(Value::Nil)),
                }
            }
            "make-network-process" => make_network_process(interp, args, env),
            "make-serial-process" => make_serial_process(interp, args, env),
            "serial-process-configure" => serial_process_configure(interp, args),
            "set-network-process-option" => {
                // process.c:2962.  Finding 79 fixed the processp/network
                // half; the option itself was still never read, so every
                // call returned t whether or not anything was set -- and
                // GNU's arity is 3, not 2 (audit finding 103).
                need_arg_range(name, args, 3, 4)?;
                let process_id = interp.resolve_process_id(&args[0])?;
                if !interp.is_network_process(process_id) {
                    return Err(LispError::Signal("Process is not a network process".into()));
                }
                // process.c:2984-2986: a closed socket is "not running".
                let Some(fd) = interp.network_socket_fd(process_id) else {
                    return Err(LispError::Signal("Process is not running".into()));
                };
                // process.c:2881 CHECK_SYMBOL (opt).
                // process.c:2881 matches by SYMBOL NAME (strcmp), not by eq,
                // so an uninterned or foreign-obarray symbol works in GNU.
                // Emaxx stores those with internal markers in the raw name, so
                // matching the raw name would silently turn a supported option
                // into "Unknown or unsupported option".
                let raw = args[1]
                    .as_symbol()
                    .map_err(|_| wrong_type_argument("symbolp", args[1].clone()))?;
                let option = crate::lisp::types::visible_symbol_name(raw).to_string();
                if set_socket_option(fd, &option, &args[1], &args[2])? {
                    // process.c:2990 records the accepted option on the
                    // process's contact plist, which `process-contact' reads.
                    interp.put_process_contact_option(process_id, &args[1], args[2].clone());
                    return Ok(Value::T);
                }
                // process.c:2994-2997.
                match args.get(3) {
                    Some(value) if !value.is_nil() => Ok(Value::Nil),
                    _ => Err(LispError::Signal("Unknown or unsupported option".into())),
                }
            }
            "network-interface-list" => network_interface_list(args),
            "network-interface-info" => network_interface_info(args),
            "network-lookup-address-info" => {
                need_arg_range(name, args, 1, 3)?;
                let host = string_text(&args[0])?;
                if !host.is_ascii() {
                    return Err(LispError::Signal(format!(
                        "Non-ASCII hostname {host} detected, please use `puny-encode-domain'"
                    )));
                }
                let family = AddressFamily::parse(args.get(1))?;
                let numeric = match args.get(2) {
                    None | Some(Value::Nil) => false,
                    Some(Value::Symbol(hint)) if hint == "numeric" => true,
                    _ => return Err(LispError::Signal("Unsupported hints value".into())),
                };
                let addresses = match network_lookup_addresses(&host, family, numeric) {
                    Ok(addresses) => addresses,
                    Err(error) => {
                        let _ = super::call(
                            interp,
                            "message",
                            &[Value::String(format!("{host}/0 {error}").into())],
                            env,
                        )?;
                        return Ok(Value::Nil);
                    }
                };
                Ok(Value::list(addresses.into_iter().map(sockaddr_vector)))
            }
            "delete-process" => {
                need_args(name, args, 1)?;
                let process_value = process_designator_value(interp, args.first())?;
                let process_id = interp.resolve_process_id(&process_value)?;
                delete_process_notifying(interp, process_id, env)?;
                Ok(Value::Nil)
            }
            "internal-default-interrupt-process" => {
                need_arg_range(name, args, 0, 2)?;
                let original = args.first().cloned().unwrap_or(Value::Nil);
                let process = process_designator_value(interp, args.first())?;
                let process_id = interp.resolve_process_id(&process)?;
                #[cfg(unix)]
                interp.signal_process_group(
                    process_id,
                    libc::SIGINT,
                    args.get(1).unwrap_or(&Value::Nil),
                )?;
                #[cfg(not(unix))]
                return Err(LispError::Signal(
                    "Process signals are unavailable on this platform".into(),
                ));
                Ok(original)
            }
            "interrupt-process" => {
                need_arg_range(name, args, 0, 2)?;
                super::call(
                    interp,
                    "run-hook-with-args-until-success",
                    &[
                        Value::symbol("interrupt-process-functions"),
                        args.first().cloned().unwrap_or(Value::Nil),
                        args.get(1).cloned().unwrap_or(Value::Nil),
                    ],
                    env,
                )
            }
            "kill-process" => {
                need_arg_range(name, args, 0, 2)?;
                let original = args.first().cloned().unwrap_or(Value::Nil);
                let process_value = process_designator_value(interp, args.first())?;
                let process_id = interp.resolve_process_id(&process_value)?;
                #[cfg(unix)]
                interp.signal_process_group(
                    process_id,
                    libc::SIGKILL,
                    args.get(1).unwrap_or(&Value::Nil),
                )?;
                #[cfg(not(unix))]
                return Err(LispError::Signal(
                    "Process signals are unavailable on this platform".into(),
                ));
                Ok(original)
            }
            "quit-process" => {
                need_arg_range(name, args, 0, 2)?;
                let original = args.first().cloned().unwrap_or(Value::Nil);
                let process = process_designator_value(interp, args.first())?;
                let process_id = interp.resolve_process_id(&process)?;
                #[cfg(unix)]
                interp.signal_process_group(
                    process_id,
                    libc::SIGQUIT,
                    args.get(1).unwrap_or(&Value::Nil),
                )?;
                #[cfg(not(unix))]
                return Err(LispError::Signal(
                    "Process signals are unavailable on this platform".into(),
                ));
                Ok(original)
            }
            "stop-process" | "continue-process" => {
                need_arg_range(name, args, 0, 2)?;
                let original = args.first().cloned().unwrap_or(Value::Nil);
                let process = process_designator_value(interp, args.first())?;
                let process_id = interp.resolve_process_id(&process)?;
                let stop = name == "stop-process";
                if !interp.set_process_traffic_stopped(process_id, stop)? {
                    #[cfg(unix)]
                    interp.signal_process_group(
                        process_id,
                        if stop { libc::SIGTSTP } else { libc::SIGCONT },
                        args.get(1).unwrap_or(&Value::Nil),
                    )?;
                    #[cfg(not(unix))]
                    return Err(LispError::Signal(
                        "Process signals are unavailable on this platform".into(),
                    ));
                }
                Ok(original)
            }
            "set-process-query-on-exit-flag" => {
                need_args(name, args, 2)?;
                let process_id = interp.resolve_process_id(&args[0])?;
                interp.set_process_query_on_exit_flag(process_id, args[1].is_truthy())?;
                Ok(args[1].clone())
            }
            "process-query-on-exit-flag" => {
                need_args(name, args, 1)?;
                let process_id = interp.resolve_process_id(&args[0])?;
                Ok(if interp.process_query_on_exit_flag(process_id)? {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "internal-default-signal-process" => {
                need_arg_range(name, args, 2, 3)?;
                #[cfg(unix)]
                {
                    let Some(pid) = signal_process_target_pid(interp, &args[0])? else {
                        return Ok(Value::Nil);
                    };
                    let signal = process_signal_number(&args[1])?;
                    // SAFETY: `kill' does not dereference pointers; PID and signal
                    // are validated Lisp integers or known platform constants.
                    Ok(Value::Integer(i64::from(unsafe {
                        libc::kill(pid, signal)
                    })))
                }
                #[cfg(not(unix))]
                {
                    Err(LispError::Signal(
                        "Process signals are unavailable on this platform".into(),
                    ))
                }
            }
            "signal-process" => {
                need_arg_range(name, args, 2, 3)?;
                super::call(
                    interp,
                    "run-hook-with-args-until-success",
                    &[
                        Value::symbol("signal-process-functions"),
                        args[0].clone(),
                        args[1].clone(),
                        args.get(2).cloned().unwrap_or(Value::Nil),
                    ],
                    env,
                )
            }
            "signal-names" => {
                need_args(name, args, 0)?;
                #[cfg(unix)]
                {
                    Ok(signal_names_value())
                }
                #[cfg(not(unix))]
                {
                    Ok(Value::Nil)
                }
            }
            "waiting-for-user-input-p" => {
                need_args(name, args, 0)?;
                Ok(if interp.waiting_for_user_input() {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "process-send-eof" => {
                need_arg_range(name, args, 0, 1)?;
                let process = match args.first() {
                    Some(value) if !value.is_nil() => value.clone(),
                    _ => call(interp, "get-buffer-process", &[Value::Nil], env)?,
                };
                let process_id = interp.resolve_process_id(&process)?;
                let (stdout, stderr) = interp.process_send_eof(process_id)?;
                deliver_process_streams(interp, process_id, &stdout, &stderr, env)?;
                Ok(process)
            }
            "process-send-string" => {
                need_args(name, args, 2)?;
                let process_id = interp.resolve_process_id(&args[0])?;
                let input = string_text(&args[1])?;
                // GNU encodes eight-bit (raw byte) characters as their single
                // byte value; epg pipes binary signatures to gpg this way.
                let encoded = crate::lisp::primitives::encode_utf8_bytes(&input, false)?;
                let (stdout, stderr) = interp.process_send_string(process_id, &encoded)?;
                deliver_process_streams(interp, process_id, &stdout, &stderr, env)?;
                Ok(Value::Nil)
            }
            "process-send-region" => {
                need_args(name, args, 3)?;
                let process_id = interp.resolve_process_id(&args[0])?;
                let start = position_from_value(interp, &args[1])?;
                let end = position_from_value(interp, &args[2])?;
                let input = interp
                    .buffer
                    .buffer_substring(start, end)
                    .map_err(|error| LispError::Signal(error.to_string()))?;
                let encoded = crate::lisp::primitives::encode_utf8_bytes(&input, false)?;
                let (stdout, stderr) = interp.process_send_string(process_id, &encoded)?;
                deliver_process_streams(interp, process_id, &stdout, &stderr, env)?;
                Ok(Value::Nil)
            }
            "zlib-decompress-region" => {
                need_arg_range(name, args, 2, 3)?;
                let start = position_from_value(interp, &args[0])?;
                let end = position_from_value(interp, &args[1])?;
                if interp.buffer.is_multibyte() {
                    return Err(LispError::Signal(
                        "This function can be called only in unibyte buffers".into(),
                    ));
                }
                let compressed = interp
                    .buffer
                    .buffer_substring(start, end)
                    .map_err(|error| LispError::Signal(error.to_string()))?;
                let input = encode_raw_text_bytes(&compressed)?;
                ensure_region_modifiable(interp, start, end, env)?;
                ensure_no_supersession_threat(interp, env)?;
                let overlay_calls = overlay_change_hook_calls(&interp.buffer, start, end, start);
                run_overlay_hook_calls(interp, &overlay_calls, false, env)?;
                run_change_hooks(
                    interp,
                    "before-change-functions",
                    &[Value::Integer(start as i64), Value::Integer(end as i64)],
                    env,
                )?;

                let mut output = Vec::new();
                let (complete, remaining) = if input.starts_with(&[0x1f, 0x8b]) {
                    let mut decoder = flate2::bufread::GzDecoder::new(&input[..]);
                    let result = std::io::Read::read_to_end(&mut decoder, &mut output);
                    (result.is_ok(), decoder.get_ref().len())
                } else {
                    let mut decoder = flate2::bufread::ZlibDecoder::new(&input[..]);
                    let result = std::io::Read::read_to_end(&mut decoder, &mut output);
                    (result.is_ok(), decoder.get_ref().len())
                };
                let old_length = end - start;
                if !complete && args.get(2).is_none_or(Value::is_nil) {
                    run_change_hooks(
                        interp,
                        "after-change-functions",
                        &[
                            Value::Integer(start as i64),
                            Value::Integer(end as i64),
                            Value::Integer(old_length as i64),
                        ],
                        env,
                    )?;
                    run_overlay_hook_calls(interp, &overlay_calls, true, env)?;
                    return Ok(Value::Nil);
                }

                let saved_point = interp.buffer.point();
                interp
                    .delete_region_current_buffer(start, end)
                    .map_err(|error| LispError::Signal(error.to_string()))?;
                interp.buffer.goto_char(start);
                let text = decode_raw_text_bytes(&output);
                interp.insert_current_buffer(&text);
                interp
                    .buffer
                    .goto_char(saved_point.min(interp.buffer.point_max()));
                run_change_hooks(
                    interp,
                    "after-change-functions",
                    &[
                        Value::Integer(start as i64),
                        Value::Integer((start + output.len()) as i64),
                        Value::Integer(old_length as i64),
                    ],
                    env,
                )?;
                run_overlay_hook_calls(interp, &overlay_calls, true, env)?;
                if complete {
                    Ok(Value::T)
                } else {
                    Ok(Value::Integer(remaining as i64))
                }
            }
            "libxml-parse-xml-region" | "libxml-parse-html-region" => {
                need_arg_range(name, args, 0, 4)?;
                let start = match args.first() {
                    None | Some(Value::Nil) => interp.buffer.point_min(),
                    Some(start) => position_from_value(interp, start)?,
                };
                let end = match args.get(1) {
                    None | Some(Value::Nil) => interp.buffer.point_max(),
                    Some(end) => position_from_value(interp, end)?,
                };
                // GNU's `validate_region' canonicalizes reversed bounds before
                // handing the bytes to libxml2.
                let (start, end) = if start <= end {
                    (start, end)
                } else {
                    (end, start)
                };
                if let Some(base_url) = args.get(2)
                    && !base_url.is_nil()
                    && string_like(base_url).is_none()
                {
                    return Err(LispError::WrongTypeArgument(
                        "stringp".into(),
                        base_url.clone(),
                    ));
                }
                let source = interp
                    .buffer
                    .buffer_substring(start, end)
                    .map_err(|error| LispError::Signal(error.to_string()))?;
                let discard_comments = args.get(3).is_some_and(Value::is_truthy);
                if name == "libxml-parse-html-region" {
                    Ok(parse_html_region(&source, discard_comments))
                } else {
                    Ok(parse_xml_region(&source, discard_comments))
                }
            }
            "call-process-region" => {
                if args.len() < 3 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let (start, end) = if args[0].is_nil() && args[1].is_nil() {
                    (interp.buffer.point_min(), interp.buffer.point_max())
                } else {
                    (
                        position_from_value(interp, &args[0])?,
                        position_from_value(interp, &args[1])?,
                    )
                };
                let input = interp
                    .buffer
                    .buffer_substring(start, end)
                    .map_err(|error| LispError::Signal(error.to_string()))?;
                let program = string_text(&args[2])?;
                let delete_region = args.get(3).is_some_and(Value::is_truthy);
                let destination = args.get(4).unwrap_or(&Value::Nil);
                let argv = args
                    .get(6..)
                    .unwrap_or(&[])
                    .iter()
                    .map(string_text)
                    .collect::<Result<Vec<_>, _>>()?;
                let process_output =
                    run_external_process(interp, &program, &argv, Some(input.as_bytes()), env)?;
                if delete_region {
                    interp
                        .buffer
                        .delete_region(start, end)
                        .map_err(|error| LispError::Signal(error.to_string()))?;
                }
                write_process_output(
                    interp,
                    destination,
                    &process_output.stdout,
                    &process_output.stderr,
                    "call-process-region",
                    args,
                    env,
                )?;
                Ok(Value::Integer(exit_status_code(&process_output.status)))
            }
            "kill-buffer" => {
                need_arg_range(name, args, 0, 1)?;
                let id = if let Some(buffer) = args.first().filter(|buffer| !buffer.is_nil()) {
                    match interp.resolve_buffer_id(buffer) {
                        Ok(id) => id,
                        Err(_) if matches!(buffer, Value::Buffer(_)) => return Ok(Value::Nil),
                        Err(error) => return Err(error),
                    }
                } else {
                    interp.current_buffer_id()
                };
                let inhibit_hooks = interp.buffer_hooks_inhibited(id);
                let saved = interp.current_buffer_id();
                let switched = saved != id;
                if switched {
                    interp.set_current_buffer_id(id)?;
                }
                let consultation = (|| -> Result<bool, LispError> {
                    if !inhibit_hooks {
                        for hook in
                            hook_values(interp, "kill-buffer-query-functions", env, Some(id))
                        {
                            if call_function_value(interp, &hook, &[], env)?.is_nil() {
                                return Ok(false);
                            }
                        }
                    }
                    let mut modified = interp
                        .get_buffer_by_id(id)
                        .map(|buffer| buffer.is_modified() && buffer.file.is_some())
                        .unwrap_or(false);
                    if modified && interp.in_interactive_call() {
                        let answer = call_named_function(
                            interp,
                            "kill-buffer--possibly-save",
                            &[Value::buffer(id, interp.buffer.name.clone())],
                            env,
                        )?;
                        if answer.is_nil() {
                            return Ok(false);
                        }
                        // The interactive helper can save the buffer.  GNU
                        // rechecks BUF_MODIFF after it returns before deciding
                        // whether a recent auto-save is disposable.
                        modified = interp
                            .get_buffer_by_id(id)
                            .map(|buffer| buffer.is_modified() && buffer.file.is_some())
                            .unwrap_or(false);
                    }
                    if modified {
                        let auto_save_path = interp
                            .buffer_local_value(id, "buffer-auto-save-file-name")
                            .as_ref()
                            .and_then(|value| string_text(value).ok());
                        let visited_path = interp
                            .get_buffer_by_id(id)
                            .and_then(|buffer| buffer.file.as_ref())
                            .cloned();
                        if let Some(path) = auto_save_path.as_ref()
                            && fs::metadata(path).is_ok()
                            && visited_path.as_ref() != Some(path)
                            && interp
                                .lookup_var("kill-buffer-delete-auto-save-files", env)
                                .is_some_and(|value| value.is_truthy())
                            && interp
                                .lookup_var("delete-auto-save-files", env)
                                .is_some_and(|value| value.is_truthy())
                            && interp
                                .get_buffer_by_id(id)
                                .is_some_and(|buffer| buffer.is_autosaved())
                        {
                            let delete = call_named_function(
                                interp,
                                "yes-or-no-p",
                                &[Value::String("Delete auto-save file? ".into())],
                                env,
                            )?;
                            if delete.is_truthy() {
                                let _ = fs::remove_file(path);
                            }
                        }
                    }
                    if !inhibit_hooks {
                        // The kill hooks run with the dying buffer current, as
                        // in GNU (auto-revert's rm-watch reads its locals).
                        run_named_hooks(interp, "kill-buffer-hook", env, Some(id))?;
                    }
                    Ok(true)
                })();
                let restore = if switched && interp.has_buffer_id(saved) {
                    interp.set_current_buffer_id(saved)
                } else {
                    Ok(())
                };
                let proceed = match (consultation, restore) {
                    (Err(error), _) => return Err(error),
                    (Ok(_), Err(error)) => return Err(error),
                    (Ok(proceed), Ok(())) => proceed,
                };
                if !proceed {
                    return Ok(Value::Nil);
                }
                if !interp.allow_kill_buffer_for_threads(id) {
                    return Ok(Value::Nil);
                }
                // GNU releases the target buffer's lock only after every query
                // and hook has accepted the kill, immediately before teardown.
                unlock_buffer_by_id(interp, env, id)?;
                interp.kill_buffer_id(id);
                if !inhibit_hooks {
                    run_named_hooks(interp, "buffer-list-update-hook", env, None)?;
                }
                Ok(Value::T)
            }
            "bury-buffer-internal" => {
                if name == "bury-buffer-internal" {
                    need_args(name, args, 1)?;
                } else {
                    need_arg_range(name, args, 0, 1)?;
                }
                let id = if let Some(buffer) = args.first().filter(|value| !value.is_nil()) {
                    interp.resolve_buffer_id(buffer)?
                } else {
                    interp.current_buffer_id()
                };
                bury_buffer_in_lists(interp, id, env)?;
                if name == "bury-buffer-internal" {
                    return Ok(Value::Nil);
                }
                if id == interp.current_buffer_id()
                    && let Some((next_id, _)) = interp
                        .buffer_list
                        .iter()
                        .find(|(buffer_id, _)| *buffer_id != id)
                        .cloned()
                {
                    interp.switch_to_buffer_id(next_id)?;
                }
                Ok(Value::Nil)
            }

            "make-marker" => Ok(interp.make_marker()),
            "copy-marker" => {
                need_args(name, args, 1)?;
                let insertion_type = args.get(1).is_some_and(Value::is_truthy);
                interp.copy_marker_value(&args[0], insertion_type)
            }
            "point-marker" => {
                interp.copy_marker_value(&Value::Integer(interp.buffer.point() as i64), false)
            }
            "mark-marker" => Ok(interp.buffer_mark_marker_value()),
            "point-min-marker" => {
                interp.copy_marker_value(&Value::Integer(interp.buffer.point_min() as i64), false)
            }
            "point-max-marker" => {
                interp.copy_marker_value(&Value::Integer(interp.buffer.point_max() as i64), false)
            }
            "marker-buffer" => {
                need_args(name, args, 1)?;
                let marker_id = marker_id_from_value(&args[0])?;
                match interp.marker_buffer_id(marker_id) {
                    Some(buffer_id) => {
                        let buffer_name = interp
                            .buffer_list
                            .iter()
                            .find(|(id, _)| *id == buffer_id)
                            .map(|(_, name)| name.clone())
                            .unwrap_or_else(|| "*unknown*".to_string());
                        Ok(Value::buffer(buffer_id, buffer_name))
                    }
                    None => Ok(Value::Nil),
                }
            }
            "marker-position" => {
                need_args(name, args, 1)?;
                let marker_id = marker_id_from_value(&args[0])?;
                Ok(interp
                    .marker_position(marker_id)
                    .map(|pos| Value::Integer(pos as i64))
                    .unwrap_or(Value::Nil))
            }
            "marker-last-position" => {
                need_args(name, args, 1)?;
                let marker_id = marker_id_from_value(&args[0])?;
                Ok(interp
                    .marker_last_position(marker_id)
                    .map(|pos| Value::Integer(pos as i64))
                    .unwrap_or(Value::Nil))
            }
            "marker-insertion-type" => {
                need_args(name, args, 1)?;
                let marker_id = marker_id_from_value(&args[0])?;
                Ok(
                    if interp.marker_insertion_type(marker_id).unwrap_or(false) {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
            "set-marker-insertion-type" => {
                need_args(name, args, 2)?;
                let marker_id = marker_id_from_value(&args[0])?;
                let insertion_type = args[1].is_truthy();
                interp.set_marker_insertion_type(marker_id, insertion_type);
                Ok(if insertion_type { Value::T } else { Value::Nil })
            }
            "set-marker" => {
                need_args(name, args, 2)?;
                let marker_id = marker_id_from_value(&args[0])?;
                let (position, buffer_id) = marker_target(interp, &args[1], args.get(2))?;
                interp.set_marker(marker_id, position, buffer_id)?;
                Ok(args[0].clone())
            }
            "region-beginning" => match interp.buffer.region() {
                Some((beg, _)) => Ok(Value::Integer(beg as i64)),
                None => Err(LispError::Signal(
                    "The mark is not set now, so there is no region".into(),
                )),
            },
            "region-end" => match interp.buffer.region() {
                Some((_, end)) => Ok(Value::Integer(end as i64)),
                None => Err(LispError::Signal(
                    "The mark is not set now, so there is no region".into(),
                )),
            },
        }
    }
);

fn process_designator_value(
    interp: &mut Interpreter,
    designator: Option<&Value>,
) -> Result<Value, LispError> {
    let requested = designator.cloned().unwrap_or(Value::Nil);
    let process = match designator {
        None | Some(Value::Nil) => interp.process_value_for_buffer(interp.current_buffer_id()),
        Some(process @ Value::Record(_)) => Some(process.clone()),
        Some(value) if string_like(value).is_some() => {
            let name = string_text(value)?;
            interp
                .find_process_id_by_name(&name)
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
    };
    process.ok_or_else(|| wrong_type_argument("processp", requested))
}

fn make_process_value(
    interp: &mut Interpreter,
    env: &mut Env,
    args: &[Value],
) -> Result<Value, LispError> {
    // process.c:Fmake_process returns nil before parsing an empty plist.
    // For nonempty plists it performs :file-handler dispatch before
    // validating any other keyword value, so a remote handler owns even
    // contracts such as `:command nil'.
    if args.is_empty() {
        return Ok(Value::Nil);
    }
    let file_handler = args
        .as_chunks::<2>()
        .0
        .iter()
        .find(|pair| matches!(&pair[0], Value::Symbol(key) if key == ":file-handler"))
        .is_some_and(|pair| pair[1].is_truthy());
    if file_handler {
        let default_directory = interp
            .lookup_var("default-directory", env)
            .and_then(|value| string_like(&value).map(|string| string.text))
            .unwrap_or_default();
        if let Some(handler) =
            find_file_name_handler(interp, env, &default_directory, "make-process")?
        {
            let handler_args = std::iter::once(Value::symbol("make-process"))
                .chain(args.iter().cloned())
                .collect::<Vec<_>>();
            return interp.call_function_value(handler, None, &handler_args, env);
        }
    }
    let mut parsed = parse_make_process_args(interp, args)?;
    let inherit_coding_system = parsed.buffer_id.is_some()
        && interp
            .lookup_var("inherit-process-coding-system", env)
            .is_some_and(|value| value.is_truthy());
    if let Some(program) = parsed.program.as_deref()
        && unquote_local_file_name(program).is_some()
    {
        parsed.program = Some(resolve_file_name_in_env(interp, env, program));
    }
    // process.c finds the executable before forking; `process-command'
    // keeps the name the caller gave.
    let executable = match parsed.program.as_deref() {
        Some(program) => Some(locate_program_for_exec(interp, env, program, true)?),
        None => None,
    };
    let runtime = executable.as_ref().map(|command| {
        spawn_persistent_process(
            interp,
            command,
            &parsed.argv,
            env,
            parsed.connection_type.as_ref(),
            parsed.stderr_process_id.is_some() || parsed.stderr_buffer_id.is_some(),
        )
    });
    let runtime = runtime.transpose()?;
    if let Some(stderr_buffer_id) = parsed.stderr_buffer_id {
        let requested_name = parsed
            .name
            .clone()
            .or_else(|| parsed.program.clone())
            .unwrap_or_default();
        let stderr = interp.create_process(
            Some(stderr_buffer_id),
            None,
            Vec::new(),
            None,
            Some(format!("{requested_name} stderr")),
        )?;
        let stderr_process_id = interp.resolve_process_id(&stderr)?;
        interp.set_process_query_on_exit_flag(stderr_process_id, parsed.query_on_exit_flag)?;
        parsed.stderr_process_id = Some(stderr_process_id);
    }
    let process = interp.create_process(
        parsed.buffer_id,
        parsed.program,
        parsed.argv,
        runtime,
        parsed.name,
    )?;
    let process_id = interp.resolve_process_id(&process)?;
    interp.set_process_query_on_exit_flag(process_id, parsed.query_on_exit_flag)?;
    interp.set_process_inherit_coding_system_flag(process_id, inherit_coding_system)?;
    interp.set_process_filter(process_id, parsed.filter)?;
    interp.set_process_sentinel(process_id, parsed.sentinel);
    interp.set_process_stderr(process_id, parsed.stderr_process_id);
    if let Some((decoding, encoding)) = parsed.coding {
        interp.set_process_coding_system(process_id, decoding, encoding)?;
    }
    Ok(process)
}

#[cfg(unix)]
fn process_signal_number(value: &Value) -> Result<i32, LispError> {
    if let Ok(number) = value.as_integer() {
        return i32::try_from(number)
            .map_err(|_| LispError::Signal("Signal number is out of range".into()));
    }
    let name = value.as_symbol()?.to_ascii_uppercase();
    let abbreviation = name.strip_prefix("SIG").unwrap_or(&name);
    if let Ok(number) = abbreviation.parse::<i32>() {
        return Ok(number);
    }
    if let Some((number, _)) = named_signal_candidates()
        .into_iter()
        .find(|(_, candidate)| *candidate == abbreviation)
    {
        return Ok(number);
    }
    if let Some(number) = realtime_signal_number(abbreviation) {
        return Ok(number);
    }
    Err(LispError::Signal(format!("Undefined signal name {name}")))
}

#[cfg(unix)]
fn named_signal_candidates() -> Vec<(i32, &'static str)> {
    let mut signals = vec![
        (libc::SIGHUP, "HUP"),
        (libc::SIGINT, "INT"),
        (libc::SIGQUIT, "QUIT"),
        (libc::SIGILL, "ILL"),
        (libc::SIGTRAP, "TRAP"),
        (libc::SIGABRT, "ABRT"),
        (libc::SIGFPE, "FPE"),
        (libc::SIGKILL, "KILL"),
        (libc::SIGSEGV, "SEGV"),
        (libc::SIGBUS, "BUS"),
        (libc::SIGPIPE, "PIPE"),
        (libc::SIGALRM, "ALRM"),
        (libc::SIGTERM, "TERM"),
        (libc::SIGUSR1, "USR1"),
        (libc::SIGUSR2, "USR2"),
        (libc::SIGCHLD, "CHLD"),
        (libc::SIGURG, "URG"),
        (libc::SIGSTOP, "STOP"),
        (libc::SIGTSTP, "TSTP"),
        (libc::SIGCONT, "CONT"),
        (libc::SIGTTIN, "TTIN"),
        (libc::SIGTTOU, "TTOU"),
        (libc::SIGSYS, "SYS"),
    ];
    // sig2str's preferred XSI spelling is POLL when the host defines it.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    signals.push((libc::SIGPOLL, "POLL"));
    signals.extend([
        (libc::SIGVTALRM, "VTALRM"),
        (libc::SIGPROF, "PROF"),
        (libc::SIGXCPU, "XCPU"),
        (libc::SIGXFSZ, "XFSZ"),
        // Historical IOT is an accepted alias, but ABRT remains canonical.
        (libc::SIGABRT, "IOT"),
    ]);
    #[cfg(any(
        target_vendor = "apple",
        target_os = "freebsd",
        target_os = "netbsd",
        target_os = "openbsd",
        target_os = "dragonfly"
    ))]
    signals.push((libc::SIGEMT, "EMT"));
    // Historical CLD is an accepted alias, but CHLD remains canonical.
    signals.push((libc::SIGCHLD, "CLD"));
    #[cfg(any(target_os = "linux", target_os = "android"))]
    signals.push((libc::SIGPWR, "PWR"));
    signals.push((libc::SIGWINCH, "WINCH"));
    #[cfg(any(
        target_vendor = "apple",
        target_os = "freebsd",
        target_os = "netbsd",
        target_os = "openbsd",
        target_os = "dragonfly"
    ))]
    signals.push((libc::SIGINFO, "INFO"));
    signals.push((libc::SIGIO, "IO"));
    #[cfg(any(target_os = "linux", target_os = "android"))]
    signals.push((libc::SIGSTKFLT, "STKFLT"));
    signals.push((0, "EXIT"));
    signals
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn realtime_signal_bounds() -> Option<(i32, i32)> {
    let minimum = libc::SIGRTMIN();
    let maximum = libc::SIGRTMAX();
    (minimum <= maximum).then_some((minimum, maximum))
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn realtime_signal_bounds() -> Option<(i32, i32)> {
    None
}

#[cfg(unix)]
fn realtime_signal_number(name: &str) -> Option<i32> {
    let (minimum, maximum) = realtime_signal_bounds()?;
    if let Some(delta) = name.strip_prefix("RTMIN") {
        let delta = if delta.is_empty() {
            0
        } else {
            delta.strip_prefix('+')?.parse::<i32>().ok()?
        };
        return (0..=maximum - minimum)
            .contains(&delta)
            .then_some(minimum + delta);
    }
    let delta = name.strip_prefix("RTMAX")?;
    let delta = if delta.is_empty() {
        0
    } else {
        delta.parse::<i32>().ok()?
    };
    (minimum - maximum..=0)
        .contains(&delta)
        .then_some(maximum + delta)
}

#[cfg(unix)]
fn signal_names_value() -> Value {
    let mut names = std::collections::BTreeMap::new();
    for (number, name) in named_signal_candidates() {
        names.entry(number).or_insert_with(|| name.to_string());
    }
    if let Some((minimum, maximum)) = realtime_signal_bounds() {
        for number in minimum..=maximum {
            names.entry(number).or_insert_with(|| {
                if number <= minimum + (maximum - minimum) / 2 {
                    let delta = number - minimum;
                    if delta == 0 {
                        "RTMIN".into()
                    } else {
                        format!("RTMIN+{delta}")
                    }
                } else {
                    let delta = number - maximum;
                    if delta == 0 {
                        "RTMAX".into()
                    } else {
                        format!("RTMAX{delta}")
                    }
                }
            });
        }
    }
    Value::list(
        names
            .into_values()
            .rev()
            .map(|value| Value::String(value.into()))
            .collect::<Vec<_>>(),
    )
}

#[cfg(unix)]
fn signal_process_target_pid(
    interp: &mut Interpreter,
    target: &Value,
) -> Result<Option<libc::pid_t>, LispError> {
    if let Some(name) = string_like(target) {
        if let Some(process_id) = interp.find_process_id_by_name(&name.text) {
            let pid = interp
                .process_os_id(process_id)
                .ok_or_else(|| LispError::Signal(format!("Cannot signal process {}", name.text)))?;
            return Ok(Some(pid as libc::pid_t));
        }
        return Ok(name.text.parse::<libc::pid_t>().ok());
    }
    if let Ok(pid) = target.as_integer() {
        return i32::try_from(pid)
            .map(Some)
            .map_err(|_| LispError::Signal("Process id is out of range".into()));
    }
    let process = process_designator_value(interp, Some(target))?;
    let process_id = interp.resolve_process_id(&process)?;
    let name = interp.process_name(process_id).unwrap_or_default();
    interp
        .process_os_id(process_id)
        .map(|pid| Some(pid as libc::pid_t))
        .ok_or_else(|| LispError::Signal(format!("Cannot signal process {name}")))
}

/// GNU dired.c formats slot 8 of `file-attributes' with filemodestring
/// over the real lstat mode: a file-type character followed by three
/// permission triads with setuid/setgid/sticky markers.
#[cfg(unix)]
fn file_mode_string_for_metadata(metadata: &std::fs::Metadata) -> String {
    use std::os::unix::fs::MetadataExt;
    let mode = metadata.mode();
    // The `as u32' casts are needed on macOS, where libc::mode_t is u16;
    // on GNU/Linux mode_t is already u32 and clippy flags them as no-ops.
    #[allow(clippy::unnecessary_cast)]
    let type_char = match mode & (libc::S_IFMT as u32) {
        m if m == libc::S_IFDIR as u32 => 'd',
        m if m == libc::S_IFLNK as u32 => 'l',
        m if m == libc::S_IFCHR as u32 => 'c',
        m if m == libc::S_IFBLK as u32 => 'b',
        m if m == libc::S_IFIFO as u32 => 'p',
        m if m == libc::S_IFSOCK as u32 => 's',
        _ => '-',
    };
    let mut rendered = String::with_capacity(10);
    rendered.push(type_char);
    for (read_bit, write_bit, exec_bit, special_bit, exec_char, no_exec_char) in [
        (0o400u32, 0o200u32, 0o100u32, 0o4000u32, 's', 'S'),
        (0o040, 0o020, 0o010, 0o2000, 's', 'S'),
        (0o004, 0o002, 0o001, 0o1000, 't', 'T'),
    ] {
        rendered.push(if mode & read_bit != 0 { 'r' } else { '-' });
        rendered.push(if mode & write_bit != 0 { 'w' } else { '-' });
        let executable = mode & exec_bit != 0;
        rendered.push(if mode & special_bit != 0 {
            if executable { exec_char } else { no_exec_char }
        } else if executable {
            'x'
        } else {
            '-'
        });
    }
    rendered
}

#[cfg(not(unix))]
fn file_mode_string_for_metadata(metadata: &std::fs::Metadata) -> String {
    let writable = !metadata.permissions().readonly();
    let type_char = if metadata.file_type().is_dir() {
        'd'
    } else {
        '-'
    };
    let mut rendered = String::with_capacity(10);
    rendered.push(type_char);
    for _ in 0..3 {
        rendered.push('r');
        rendered.push(if writable { 'w' } else { '-' });
        rendered.push(if metadata.file_type().is_dir() {
            'x'
        } else {
            '-'
        });
    }
    rendered
}

/// process.c:2877 `set_socket_option'.  Returns whether the option was
/// recognised AND applied; the caller turns a false into GNU's
/// "Unknown or unsupported option" (or nil under NO-ERROR).
///
/// The table mirrors process.c:2839.  SO_PRIORITY is the only entry GNU
/// compiles out on Darwin, so asking for it is genuinely unsupported there
/// rather than unimplemented; SO_BINDTODEVICE is defined on both platforms
/// (each under its own number -- see the constant below) and GNU accepts
/// it, so omitting it would be a regression -- verified against the
/// oracle, which returns t and records the loopback name on the contact
/// plist.
fn set_socket_option(
    fd: std::os::fd::RawFd,
    option: &str,
    option_symbol: &Value,
    value: &Value,
) -> Result<bool, LispError> {
    use std::ffi::c_void;

    enum Kind {
        Bool,
        #[cfg(target_os = "linux")]
        Int,
        Linger,
        IfName,
    }
    let entry = match option {
        ":broadcast" => Some((libc::SO_BROADCAST, Kind::Bool)),
        ":dontroute" => Some((libc::SO_DONTROUTE, Kind::Bool)),
        ":keepalive" => Some((libc::SO_KEEPALIVE, Kind::Bool)),
        ":oobinline" => Some((libc::SO_OOBINLINE, Kind::Bool)),
        ":reuseaddr" => Some((libc::SO_REUSEADDR, Kind::Bool)),
        ":linger" => Some((libc::SO_LINGER, Kind::Linger)),
        ":bindtodevice" => Some((SO_BINDTODEVICE, Kind::IfName)),
        // process.c:2846 compiles this row under #ifdef SO_PRIORITY,
        // which GNU/Linux defines and Darwin does not (oracle probe
        // sopri.el: :priority 3 answers t and lands on the contact
        // plist; any non-fixnum is "Bad option value for :priority").
        #[cfg(target_os = "linux")]
        ":priority" => Some((libc::SO_PRIORITY, Kind::Int)),
        _ => None,
    };
    let Some((optnum, kind)) = entry else {
        return Ok(false);
    };

    // process.c:2921 rejects a non-string, non-nil device name BEFORE the
    // syscall, with a distinct message from the unknown-option one.
    let device = if let Kind::IfName = kind {
        match value {
            Value::Nil => None,
            // process.c:2920 gates on STRINGP.  Emaxx has two string
            // representations (`String' and the shared `StringObject'), so
            // test through `string_text' rather than one variant.
            _ => match string_text(value) {
                Ok(text) => Some(text),
                Err(_) => {
                    return Err(LispError::Signal(format!("Bad option value for {option}")));
                }
            },
        }
    } else {
        None
    };

    // process.c:2907 SOPT_INT: only an int-ranged fixnum reaches the
    // kernel; anything else is "Bad option value" BEFORE the syscall.
    #[cfg(target_os = "linux")]
    let int_value = if let Kind::Int = kind {
        match value {
            Value::Integer(count) => match libc::c_int::try_from(*count) {
                Ok(count) => Some(count),
                Err(_) => {
                    return Err(LispError::Signal(format!("Bad option value for {option}")));
                }
            },
            _ => {
                return Err(LispError::Signal(format!("Bad option value for {option}")));
            }
        }
    } else {
        None
    };

    // SAFETY: `fd' is a live descriptor owned by the process record, and each
    // arm passes a pointer to a correctly sized local of the type the option
    // expects.
    let applied = unsafe {
        match kind {
            #[cfg(target_os = "linux")]
            Kind::Int => {
                let optval: libc::c_int = int_value.expect("int option value vetted above");
                libc::setsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    optnum,
                    std::ptr::addr_of!(optval).cast::<c_void>(),
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                )
            }
            Kind::Bool => {
                let optval: libc::c_int = if value.is_nil() { 0 } else { 1 };
                libc::setsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    optnum,
                    std::ptr::addr_of!(optval).cast::<c_void>(),
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                )
            }
            Kind::Linger => {
                // process.c:2931.  Only an int-ranged fixnum sets l_linger;
                // anything else (float, string, bignum, t) leaves it 0 and
                // just toggles l_onoff.  Truncating an out-of-range integer
                // would put a value in the kernel that GNU never would.
                let seconds = match value {
                    Value::Integer(count) => i32::try_from(*count).ok(),
                    _ => None,
                };
                let linger = libc::linger {
                    l_onoff: match seconds {
                        Some(_) => 1,
                        None if value.is_nil() => 0,
                        None => 1,
                    },
                    l_linger: seconds.unwrap_or(0),
                };
                libc::setsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    optnum,
                    std::ptr::addr_of!(linger).cast::<c_void>(),
                    std::mem::size_of::<libc::linger>() as libc::socklen_t,
                )
            }
            Kind::IfName => {
                // process.c:2913-2925: a zeroed IFNAMSIZ+1 buffer, at most
                // IFNAMSIZ bytes copied in, and always IFNAMSIZ handed to the
                // kernel -- unbinding needs the zeroed buffer, not a short one.
                let mut devname = [0u8; libc::IFNAMSIZ + 1];
                if let Some(text) = device.as_deref() {
                    let bytes = text.as_bytes();
                    let length = bytes.len().min(libc::IFNAMSIZ);
                    devname[..length].copy_from_slice(&bytes[..length]);
                }
                libc::setsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    optnum,
                    devname.as_ptr().cast::<c_void>(),
                    libc::IFNAMSIZ as libc::socklen_t,
                )
            }
        }
    };
    // process.c:2953 saves errno on the line after the call for a reason, and
    // fileio.c:293 spells it out: building a Lisp string can itself set errno.
    // Capture before allocating anything.
    let failure = (applied < 0).then(std::io::Error::last_os_error);
    if let Some(error) = failure {
        // process.c:2940 `report_file_errno ("Cannot set network option",
        // list2 (opt, val), errno)' -- a file-error carrying the option and
        // value as data, not an `error' with them folded into the message.
        // list2 (opt, val) carries the CALLER's symbol, so an `eq' test on
        // the error data succeeds for a foreign-obarray keyword.  Rebuilding
        // the symbol here was the same identity bug fixed on the plist path.
        return Err(LispError::SignalValue(Value::list([
            Value::symbol("file-error"),
            Value::string("Cannot set network option"),
            Value::string(&super::super::processes::network_io_error_detail(&error)),
            option_symbol.clone(),
            value.clone(),
        ])));
    }
    Ok(true)
}

/// The kernel's own SO_BINDTODEVICE where libc exposes it (Linux: 25).
/// Hardcoding one platform's number sent Linux setsockopt an unknown
/// option (0x1134 is Darwin's) and every bind came back ENOPROTOOPT.
#[cfg(target_os = "linux")]
const SO_BINDTODEVICE: libc::c_int = libc::SO_BINDTODEVICE;

/// macOS sys/socket.h:190.  The libc crate does not expose this for Darwin.
#[cfg(not(target_os = "linux"))]
const SO_BINDTODEVICE: libc::c_int = 0x1134;
