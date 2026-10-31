use super::*;
use crate::lisp::types::Kind;
use crate::lisp::types::LispErrorKind;

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
        interp.bury_buffer_in_selected_frame(buffer_id);
        // run_buffer_list_update_hook: not for a buffer whose hooks are
        // inhibited.
        if !interp.buffer_hooks_inhibited(buffer_id) {
            run_named_hooks(interp, "buffer-list-update-hook", env, Some(buffer_id))?;
        }
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
        match value.map(|v| v.kind()) {
            None | Some(Kind::Nil) => Ok(Self::Both),
            Some(Kind::Symbol(family)) if family == "ipv4" => Ok(Self::Ipv4),
            Some(Kind::Symbol(family)) if family == "ipv6" => Ok(Self::Ipv6),
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
        let bytes = (0..6).map(|offset| Value::Integer(unsafe { *address.add(offset) } as i64));
        result = Value::cons(Value::Integer(family as i64), Value::vector(bytes));
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
    // process.c network_interface_list conses each AF_INET/AF_INET6 row
    // onto the front of the result, so the list runs newest-first; the
    // fe80:: link-local rows are part of GNU's list (the crate's
    // `link-local' feature keeps them).
    let mut rows = interfaces
        .into_iter()
        .filter_map(|interface| {
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
        })
        .collect::<Vec<_>>();
    rows.reverse();
    Ok(Value::list(rows))
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
                // buffer.c:Fset_buffer: Fget_buffer and nsberror, then the
                // buffer's liveness.
                let buffer = interp.get_buffer_or_error(&args[0])?;
                let id = interp
                    .live_buffer_id(buffer)
                    .ok_or_else(|| LispError::Signal("Selecting deleted buffer".into()))?;
                interp.set_current_buffer_id(id)?;
                Ok(Value::Buffer(buffer))
            }
            "buffer-file-name" => {
                need_arg_range(name, args, 0, 1)?;
                // buffer.c:decode_buffer: a buffer, live or killed.
                let buffer = interp.decode_buffer(args.first())?;
                if !buffer.ptr_eq(&interp.buffer) {
                    return Ok(buffer
                        .borrow()
                        .file
                        .clone()
                        .map(|value| Value::String(value.into()))
                        .unwrap_or(Value::Nil));
                }
                // `buffer-file-name' is a per-buffer special variable.  A
                // dynamic binding in the current buffer is observable both
                // through the variable and through this primitive; reading
                // only the host Buffer field lost bindings used by ordinary
                // save code (including backup creation).
                Ok(interp
                    .lookup_var("buffer-file-name", env)
                    .unwrap_or(Value::Nil))
            }
            "visited-file-modtime" => Ok(interp
                .buffer
                .borrow()
                .visited_file_modtime()
                .and_then(|modtime| system_time_list_value(modtime.modified).ok())
                .unwrap_or(Value::Integer(0))),
            "verify-visited-file-modtime" => {
                need_arg_range(name, args, 0, 1)?;
                // fileio.c: decode_buffer, so a killed buffer's file and
                // time are read as it left them.
                let buffer = interp.decode_buffer(args.first())?;
                let remote_visit = interp
                    .live_buffer_id(buffer)
                    .is_some_and(|id| interp.buffer_remote_prefix(id).is_some());
                let buffer = buffer.borrow();
                let Some(path) = buffer.file.as_deref() else {
                    return Ok(Value::T);
                };
                let current = file_modtime(interp, env, path)?;
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
                let modtime = match args.first().map(|v| v.kind()) {
                    None | Some(Kind::Nil) => {
                        if let Some(path) = interp.buffer.borrow().file.clone() {
                            file_modtime(interp, env, &path)?
                        } else {
                            None
                        }
                    }
                    Some(Kind::Integer(0)) => None,
                    Some(value) => Some(file_modtime_from_value(interp, &value.value())?),
                };
                interp.buffer.borrow_mut().set_visited_file_modtime(modtime);
                Ok(Value::Nil)
            }
            "set-buffer-major-mode" => {
                need_args(name, args, 1)?;
                let Kind::Buffer(buffer) = args[0].kind() else {
                    return Err(LispError::WrongTypeArgument("bufferp".into(), args[0]));
                };
                let buffer_id = interp.live_buffer_id(buffer).ok_or_else(|| {
                    LispError::Signal("Attempt to set major mode for a dead buffer".into())
                })?;
                let buffer_name = buffer.borrow().name.clone();
                let mode = if buffer_name == "*scratch*" {
                    interp
                        .lookup_var("initial-major-mode", env)
                        .unwrap_or(Value::Nil)
                } else {
                    let mode = super::call(
                        interp,
                        "default-value",
                        &[Value::Symbol("major-mode".into())],
                        env,
                    )?;
                    // buffer.c: a nil default takes the current buffer's
                    // mode unless that is a special mode.
                    let current = interp.lookup_var("major-mode", env).unwrap_or(Value::Nil);
                    if mode.is_nil()
                        && super::call(interp, "get", &[current, Value::symbol("mode-class")], env)?
                            .is_nil()
                    {
                        current
                    } else {
                        mode
                    }
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
                let expanded =
                    string_text(&super::call(interp, "expand-file-name", &[args[0]], env)?)?;
                Ok(buffer_visiting_exact_file_name(interp, &expanded)
                    .map(|(id, name)| interp.buffer_value(id).expect("live buffer object"))
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
                            .filter(|buffer| buffer.file_truename.as_deref() == Some(file.as_str()))
                            .map(|_| interp.buffer_value(*id).expect("live buffer object"))
                    })
                    .unwrap_or(Value::Nil))
            }
            "find-buffer" => {
                need_args(name, args, 2)?;
                let variable = args[0].as_symbol()?;
                let candidates = interp
                    .buffer_list
                    .iter()
                    .filter_map(|(id, name)| {
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
                        value.map(|value| (*id, value))
                    })
                    .collect::<Vec<_>>();
                for (id, value) in candidates {
                    if values_equal(interp, &value, &args[1])? {
                        return Ok(interp.buffer_value(id).expect("live buffer object"));
                    }
                }
                Ok(Value::Nil)
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
                    None,
                ))
            }
            "substitute-in-file-name" => {
                need_args(name, args, 1)?;
                Ok(Value::String(
                    substitute_in_file_name_in_env(interp, env, &string_text(&args[0])?).into(),
                ))
            }
            // fileio.c's file name syntax works on the string's internal
            // bytes and keeps its multibyteness (make_specified_string),
            // without its properties.
            "file-name-directory" => {
                need_args(name, args, 1)?;
                let (bytes, multibyte) = file_name_bytes(&args[0])?;
                let mut p = bytes.len();
                while p != 0 && bytes[p - 1] != b'/' {
                    p -= 1;
                }
                if p == 0 {
                    return Ok(Value::Nil);
                }
                make_specified_string(bytes[..p].to_vec(), multibyte)
                    .map_err(|error| string_storage_error(interp, env, error))
            }
            "file-name-nondirectory" => {
                need_args(name, args, 1)?;
                let (bytes, multibyte) = file_name_bytes(&args[0])?;
                let mut p = bytes.len();
                while p != 0 && bytes[p - 1] != b'/' {
                    p -= 1;
                }
                make_specified_string(bytes[p..].to_vec(), multibyte)
                    .map_err(|error| string_storage_error(interp, env, error))
            }
            "file-name-as-directory" => {
                need_args(name, args, 1)?;
                let (mut bytes, multibyte) = file_name_bytes(&args[0])?;
                if bytes.is_empty() {
                    bytes.extend_from_slice(b"./");
                } else if bytes.last() != Some(&b'/') {
                    bytes.push(b'/');
                }
                make_specified_string(bytes, multibyte)
                    .map_err(|error| string_storage_error(interp, env, error))
            }
            "directory-file-name" => {
                need_args(name, args, 1)?;
                let (mut bytes, multibyte) = file_name_bytes(&args[0])?;
                // Remove any final slashes; leave "/" and "//" alone and
                // treat "///" and longer as "/".
                if !(bytes.len() == 2 && bytes[0] == b'/') {
                    while bytes.len() > 1 && bytes.last() == Some(&b'/') {
                        bytes.pop();
                    }
                }
                make_specified_string(bytes, multibyte)
                    .map_err(|error| string_storage_error(interp, env, error))
            }
            "directory-name-p" => {
                need_args(name, args, 1)?;
                let (bytes, _) = file_name_bytes(&args[0])?;
                Ok(if bytes.last() == Some(&b'/') {
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
                Ok(find_file_name_handler(interp, env, &file, args[1])?.unwrap_or(Value::Nil))
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
            "load" => load_file(interp, args, env).map(|(result, _)| result),
            "locate-file-internal" => {
                need_arg_range(name, args, 2, 4)?;
                locate_file_internal(
                    interp,
                    &args[0],
                    &args[1],
                    args.get(2).unwrap_or(&Value::Nil),
                    args.get(3).unwrap_or(&Value::Nil),
                    env,
                )
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
                    args[0],
                    args.get(1).cloned().unwrap_or(Value::Nil),
                    args.get(2).cloned().unwrap_or(Value::Nil),
                    args.get(3).cloned().unwrap_or(Value::Nil),
                ];
                if let Some(count) = args.get(5) {
                    directory_files_args.push(*count);
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
                            &[Value::String(attribute_path.into()), id_format],
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
                let host = host_file_name(interp, env, &path)?;
                let directory = if name == "file-directory-p" {
                    fs::metadata(&host).is_ok_and(|metadata| metadata.is_dir())
                } else {
                    accessible_directory(host.as_os_str().as_encoded_bytes()).is_ok()
                };
                Ok(if directory { Value::T } else { Value::Nil })
            }
            "file-readable-p" => {
                need_args(name, args, 1)?;
                let path = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
                Ok(if file_readable_p(&host_file_name(interp, env, &path)?) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "access-file" => {
                need_args(name, args, 2)?;
                let requested = string_text(&args[0])?;
                let path = resolve_file_name_in_env(interp, env, &requested);
                if file_readable_p(&host_file_name(interp, env, &path)?) {
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
                    if fs::metadata(host_file_name(interp, env, &path)?)
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
                Ok(if file_writable_p(&host_file_name(interp, env, &path)?) {
                    Value::T
                } else {
                    Value::Nil
                })
            }
            "file-exists-p" => {
                need_args(name, args, 1)?;
                let path = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
                Ok(
                    if fs::metadata(host_file_name(interp, env, &path)?).is_ok() {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
            "file-executable-p" => {
                need_args(name, args, 1)?;
                let path = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
                Ok(if file_executable_p(&host_file_name(interp, env, &path)?) {
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
                // Ffile_attributes expands the name under a catch-all:
                // a name expand-file-name refuses has no attributes.
                if path.contains('\0') {
                    return Ok(Value::Nil);
                }
                let host = host_file_name(interp, env, &path)?;
                let metadata = match fs::symlink_metadata(&host) {
                    Ok(metadata) => metadata,
                    Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Value::Nil),
                    Err(error) => return Err(LispError::Signal(error.to_string())),
                };
                let file_type = metadata.file_type();
                let type_value = if file_type.is_dir() {
                    Value::T
                } else if file_type.is_symlink() {
                    match fs::read_link(&host) {
                        Ok(target) => decode_host_file_name(interp, env, &target)?,
                        Err(_) => Value::String(path.clone().into()),
                    }
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
                let string_ids = args.get(1).and_then(|value| value.symbol_str()) == Some("string");
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
                let first = host_file_name(interp, env, &first)?;
                let second = host_file_name(interp, env, &second)?;
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
                // fileio.c:Fcopy_file, past the handler lookup that
                // expanded FILE and NEWNAME.
                use std::io::{Read, Write};
                use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
                use std::os::unix::io::{AsRawFd, IntoRawFd};
                let file = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
                validate_file_name(&file)?;
                let mut newname = resolve_file_name_in_env(interp, env, &string_text(&args[1])?);
                validate_file_name(&newname)?;
                if directory_name_p(&newname) {
                    newname = file_name_concat(&[newname, file_name_nondirectory(&file)]);
                }
                let ok_if_already_exists = args.get(2).copied().unwrap_or(Value::Nil);
                let interactive = matches!(ok_if_already_exists.kind(), Kind::Integer(_));
                let keep_time = args.get(3).is_some_and(Value::is_truthy);
                let preserve_uid_gid = args.get(4).is_some_and(Value::is_truthy);
                let preserve_permissions = args.get(5).is_some_and(Value::is_truthy);
                let host_file = host_file_name(interp, env, &file)?;
                let host_newname = host_file_name(interp, env, &newname)?;
                let mut input = fs::OpenOptions::new()
                    .read(true)
                    .custom_flags(libc::O_NONBLOCK)
                    .open(&host_file)
                    .map_err(|error| file_operation_error("Opening input file", &error, &file))?;
                let st = input
                    .metadata()
                    .map_err(|error| file_operation_error("Input file status", &error, &file))?;
                // We can copy only regular files.
                if !st.is_file() {
                    return Err(LispError::SignalValue(file_errno_error_value(
                        "Non-regular file",
                        if st.is_dir() {
                            libc::EISDIR
                        } else {
                            libc::EINVAL
                        },
                        &file,
                    )));
                }
                let new_mask = st.mode() & if preserve_uid_gid { 0o700 } else { 0o777 };
                let mut already_exists = false;
                let mut output = match fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .mode(new_mask)
                    .open(&host_newname)
                {
                    Ok(output) => output,
                    Err(error) if error.kind() == ErrorKind::AlreadyExists => {
                        if ok_if_already_exists.is_nil() || interactive {
                            barf_or_query_if_file_exists(
                                interp,
                                env,
                                &newname,
                                true,
                                "copy to it",
                                interactive,
                                false,
                            )?;
                        }
                        already_exists = true;
                        fs::OpenOptions::new()
                            .write(true)
                            .open(&host_newname)
                            .map_err(|error| {
                                file_operation_error("Opening output file", &error, &newname)
                            })?
                    }
                    Err(error) => {
                        return Err(file_operation_error(
                            "Opening output file",
                            &error,
                            &newname,
                        ));
                    }
                };
                let mut oldsize = 0;
                if already_exists {
                    let out_st = output.metadata().map_err(|error| {
                        file_operation_error("Output file status", &error, &newname)
                    })?;
                    if st.dev() == out_st.dev() && st.ino() == out_st.ino() {
                        return Err(file_operation_error_files(
                            "Input and output files are the same",
                            &std::io::Error::from_raw_os_error(0),
                            &[&file, &newname],
                        ));
                    }
                    if out_st.is_file() {
                        oldsize = out_st.len();
                    }
                }
                interp.maybe_quit(env)?;
                let (ifd, ofd) = (input.as_raw_fd(), output.as_raw_fd());
                let insize = st.len();
                let mut newsize = 0;
                #[cfg(target_os = "linux")]
                while newsize < insize {
                    // At most 1 GiB a call, as fileio.c's COPY_MAX.
                    let len = (insize - newsize).min(1 << 30) as usize;
                    // SAFETY: both descriptors are open; null offsets use
                    // and advance the files' own.
                    let copied = unsafe {
                        libc::copy_file_range(
                            ifd,
                            std::ptr::null_mut(),
                            ofd,
                            std::ptr::null_mut(),
                            len,
                            0,
                        )
                    };
                    if copied <= 0 {
                        break;
                    }
                    newsize += copied as u64;
                    interp.maybe_quit(env)?;
                }
                // read+write when copy_file_range failed or the input is
                // empty and so could be a /proc file; it either succeeds
                // or reports the error more precisely.
                if newsize != insize || insize == 0 {
                    let mut buf = [0u8; 16 * 1024];
                    loop {
                        let copied = match input.read(&mut buf) {
                            Ok(0) => break,
                            Ok(copied) => copied,
                            Err(error) if error.kind() == ErrorKind::Interrupted => continue,
                            Err(error) => {
                                return Err(file_operation_error("Read error", &error, &file));
                            }
                        };
                        output.write_all(&buf[..copied]).map_err(|error| {
                            file_operation_error("Write error", &error, &newname)
                        })?;
                        newsize += copied as u64;
                    }
                }
                // Truncate an existing output file after writing the data.
                if newsize < oldsize {
                    output.set_len(newsize).map_err(|error| {
                        file_operation_error("Truncating output file", &error, &newname)
                    })?;
                }
                // The original file's permissions, and if requested its
                // owner and group.
                let realmask = (!interp.default_file_modes & 0o777) as u32;
                let mut preserved_permissions = st.mode() & 0o7777;
                let mut default_permissions = st.mode() & 0o777 & !realmask;
                if preserve_uid_gid {
                    // SAFETY: fchown on the open output descriptor; -1
                    // keeps the owner.
                    let changed = unsafe { libc::fchown(ofd, st.uid(), st.gid()) } == 0;
                    if !changed {
                        if unsafe { libc::fchown(ofd, libc::uid_t::MAX, st.gid()) } == 0 {
                            preserved_permissions &= !0o4000;
                        } else {
                            // The group is wrong: no setuid or setgid, and
                            // the other bits as the group's.
                            preserved_permissions &= !0o6000;
                            preserved_permissions &= !0o70;
                            preserved_permissions |= (preserved_permissions & 7) << 3;
                            default_permissions &= !0o70;
                            default_permissions |= (default_permissions & 7) << 3;
                        }
                    }
                }
                // qcopy_acl of a file with no ACL beyond its mode sets the
                // mode; otherwise a new file gets the default permissions
                // its creation mask did not give it.
                let chmod = if preserve_permissions {
                    Some(preserved_permissions)
                } else if already_exists || (new_mask & !realmask) == default_permissions {
                    None
                } else {
                    Some(default_permissions)
                };
                // SAFETY: fchmod on the open output descriptor.
                if let Some(mode) = chmod
                    && unsafe { libc::fchmod(ofd, mode as libc::mode_t) } != 0
                {
                    return Err(file_operation_error(
                        "Copying permissions to",
                        &std::io::Error::last_os_error(),
                        &newname,
                    ));
                }
                if keep_time {
                    let times = [
                        libc::timespec {
                            tv_sec: st.atime() as libc::time_t,
                            tv_nsec: st.atime_nsec() as _,
                        },
                        libc::timespec {
                            tv_sec: st.mtime() as libc::time_t,
                            tv_nsec: st.mtime_nsec() as _,
                        },
                    ];
                    // SAFETY: futimens reads the two timespecs.
                    if unsafe { libc::futimens(ofd, times.as_ptr()) } != 0 {
                        return Err(LispError::SignalValue(Value::list([
                            Value::symbol("file-date-error"),
                            Value::string("Cannot set file date"),
                            Value::string(&newname),
                        ])));
                    }
                }
                // emacs_close: a close interrupted by a signal closed.
                let ofd = output.into_raw_fd();
                // SAFETY: the descriptor was just released by its File.
                if unsafe { libc::close(ofd) } < 0 {
                    let error = std::io::Error::last_os_error();
                    if error.kind() != ErrorKind::Interrupted {
                        return Err(file_operation_error("Write error", &error, &newname));
                    }
                }
                drop(input);
                if !already_exists {
                    dispatch_file_notification(interp, env, &newname, "created")?;
                }
                dispatch_file_notification(interp, env, &newname, "changed")?;
                Ok(Value::Nil)
            }
            "rename-file" => {
                need_arg_range(name, args, 2, 3)?;
                // fileio.c:Frename_file, past the handler lookup that
                // expanded FILE and NEWNAME (under FILE's directory file
                // name when NEWNAME is a directory name).
                let file = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
                validate_file_name(&file)?;
                let mut newname = resolve_file_name_in_env(interp, env, &string_text(&args[1])?);
                validate_file_name(&newname)?;
                if directory_name_p(&newname) {
                    newname = file_name_concat(&[
                        newname,
                        file_name_nondirectory(&directory_file_name(&file)),
                    ]);
                }
                let mut ok_if_already_exists = args.get(2).copied().unwrap_or(Value::Nil);
                let interactive = matches!(ok_if_already_exists.kind(), Kind::Integer(_));
                let host_file = host_file_name(interp, env, &file)?;
                let host_newname = host_file_name(interp, env, &newname)?;
                let mut plain_rename = ok_if_already_exists.is_truthy() && !interactive;
                let mut rename_errno = 0;
                if !plain_rename {
                    match renameat_noreplace(&host_file, &host_newname) {
                        Ok(()) => {
                            interp.queue_file_rename_notification(&file, &newname);
                            interp.invalidate_file_notify_watches_for_path(&file);
                            return Ok(Value::Nil);
                        }
                        Err(errno) => {
                            rename_errno = errno;
                            if matches!(
                                errno,
                                libc::EEXIST | libc::EINVAL | libc::ENOSYS | libc::ENOTSUP
                            ) {
                                barf_or_query_if_file_exists(
                                    interp,
                                    env,
                                    &newname,
                                    errno == libc::EEXIST,
                                    "rename to it",
                                    interactive,
                                    false,
                                )?;
                                plain_rename = true;
                            }
                        }
                    }
                }
                if plain_rename {
                    match fs::rename(&host_file, &host_newname) {
                        Ok(()) => {
                            interp.queue_file_rename_notification(&file, &newname);
                            interp.invalidate_file_notify_watches_for_path(&file);
                            return Ok(Value::Nil);
                        }
                        Err(error) => rename_errno = error.raw_os_error().unwrap_or(libc::EIO),
                    }
                    // Don't prompt again.
                    ok_if_already_exists = Value::T;
                } else if ok_if_already_exists.is_truthy() {
                    ok_if_already_exists = Value::T;
                }
                let renaming_error = |errno: i32| {
                    file_operation_error_files(
                        "Renaming",
                        &std::io::Error::from_raw_os_error(errno),
                        &[&file, &newname],
                    )
                };
                if rename_errno != libc::EXDEV {
                    return Err(renaming_error(rename_errno));
                }
                // Across file systems: copy, then delete the original.
                let mut dirp = directory_name_p(&file);
                let mut file_type = None;
                if !dirp {
                    let metadata = fs::symlink_metadata(&host_file).map_err(|error| {
                        renaming_error(error.raw_os_error().unwrap_or(libc::EIO))
                    })?;
                    dirp = metadata.is_dir();
                    file_type = Some(metadata.file_type());
                }
                let (file_value, newname_value) = (Value::string(&file), Value::string(&newname));
                if dirp {
                    call_named_function(
                        interp,
                        "copy-directory",
                        &[file_value, newname_value, Value::T, Value::Nil],
                        env,
                    )?;
                } else if file_type.is_some_and(|kind| kind.is_file()) {
                    call(
                        interp,
                        "copy-file",
                        &[
                            file_value,
                            newname_value,
                            ok_if_already_exists,
                            Value::T,
                            Value::T,
                            Value::T,
                        ],
                        env,
                    )?;
                } else if file_type.is_some_and(|kind| kind.is_symlink()) {
                    let target = fs::read_link(&host_file).map_err(|error| {
                        renaming_error(error.raw_os_error().unwrap_or(libc::EIO))
                    })?;
                    let target = decode_host_file_name(interp, env, &target)?;
                    call(
                        interp,
                        "make-symbolic-link",
                        &[target, newname_value, ok_if_already_exists],
                        env,
                    )?;
                } else {
                    return Err(renaming_error(rename_errno));
                }
                let binding =
                    interp.bind_special_dynamic("delete-by-moving-to-trash", Value::Nil, env)?;
                let deleted = if dirp {
                    call_named_function(interp, "delete-directory", &[file_value, Value::T], env)
                } else {
                    call_named_function(interp, "delete-file", &[file_value, Value::Nil], env)
                };
                interp.restore_special_dynamic(binding, env)?;
                deleted?;
                Ok(Value::Nil)
            }
            "system-move-file-to-trash" => {
                need_args(name, args, 1)?;
                let path = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
                if let Err(error) = fs::symlink_metadata(host_file_name(interp, env, &path)?) {
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
                // Fdelete_file_internal: `unlink' failing with anything but
                // ENOENT is report_file_error ("Removing old name", filename)
                // -- the errno's own condition (`permission-denied' for
                // EACCES, which url-cache's pruner and callers handling
                // `file-error' expect), not a plain `error'.
                let removed = match fs::remove_file(host_file_name(interp, env, &path)?) {
                    Ok(()) => true,
                    Err(error) if error.kind() == ErrorKind::NotFound => false,
                    Err(error) => {
                        return Err(file_operation_error("Removing old name", &error, &path));
                    }
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
                // Fdelete_directory_internal: report_file_error ("Removing
                // directory", directory) on `rmdir' failure.
                fs::remove_dir(host_file_name(interp, env, &path)?)
                    .map_err(|error| file_operation_error("Removing directory", &error, &path))?;
                interp.queue_directory_deletion_notification(&path);
                interp.invalidate_file_notify_watches_for_path(&path);
                Ok(Value::Nil)
            }

            "make-directory-internal" => {
                need_args(name, args, 1)?;
                let path = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
                // Fmake_directory_internal: emacs_mkdir (dir, 0777 &
                // ~auto_saving_dir_umask), report_file_error ("Creating
                // directory", directory).
                use std::os::unix::fs::DirBuilderExt;
                std::fs::DirBuilder::new()
                    .mode(0o777 & !crate::lisp::primitives::file_io::auto_saving_dir_umask())
                    .create(host_file_name(interp, env, &path)?)
                    .map_err(|error| file_operation_error("Creating directory", &error, &path))?;
                dispatch_file_notification(interp, env, &path, "created")?;
                Ok(Value::Nil)
            }
            "add-name-to-file" => {
                need_arg_range(name, args, 2, 3)?;
                // fileio.c:Fadd_name_to_file, past the handler lookups that
                // expanded FILE and NEWNAME.
                let file = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
                let mut newname = resolve_file_name_in_env(interp, env, &string_text(&args[1])?);
                if directory_name_p(&newname) {
                    newname = file_name_concat(&[newname, file_name_nondirectory(&file)]);
                }
                let ok_if_already_exists = args.get(2).copied().unwrap_or(Value::Nil);
                let interactive = matches!(ok_if_already_exists.kind(), Kind::Integer(_));
                let host_file = host_file_name(interp, env, &file)?;
                let host_newname = host_file_name(interp, env, &newname)?;
                let mut linked = fs::hard_link(&host_file, &host_newname);
                if linked
                    .as_ref()
                    .is_err_and(|error| error.kind() == ErrorKind::AlreadyExists)
                {
                    if ok_if_already_exists.is_nil() || interactive {
                        barf_or_query_if_file_exists(
                            interp,
                            env,
                            &newname,
                            true,
                            "make it a new name",
                            interactive,
                            false,
                        )?;
                    }
                    let _ = fs::remove_file(&host_newname);
                    linked = fs::hard_link(&host_file, &host_newname);
                }
                linked.map_err(|error| {
                    file_operation_error_files("Adding new name", &error, &[&file, &newname])
                })?;
                Ok(Value::Nil)
            }
            "make-temp-file-internal" => {
                need_args(name, args, 4)?;
                let prefix = string_text(&args[0])?;
                let suffix = string_text(&args[2])?;
                validate_file_name(&prefix)?;
                validate_file_name(&suffix)?;
                Ok(Value::String(
                    make_temp_file_internal(interp, env, &prefix, &args[1], &suffix, args.get(3))?
                        .into(),
                ))
            }
            "file-locked-p" => {
                need_args(name, args, 1)?;
                file_locked_p(interp, env, args[0])
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
                lock_file_primitive(interp, env, args[0])
            }
            "unlock-file" => {
                need_args(name, args, 1)?;
                unlock_file_primitive(interp, env, args[0])
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
                    vec![args[1]]
                };
                let flags = aspects
                    .iter()
                    .map(|aspect| {
                        aspect.as_symbol().map(str::to_string).map_err(|_| {
                            crate::lisp::primitives::file_notify_error_with_errno(
                                "Unknown aspect",
                                &std::io::Error::from_raw_os_error(libc::EINVAL),
                                *aspect,
                            )
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                interp.validate_inotify_aspects(&flags)?;
                let path = string_text(&args[0])?;
                let encoded = encode_file_name(interp, env, &path)?;
                interp.register_inotify_file_notify_watch(path, encoded, flags, args[2])
            }
            #[cfg(target_os = "linux")]
            "inotify-rm-watch" => {
                need_args(name, args, 1)?;
                let valid = args[0].cons_values().is_some_and(|(watch, id)| {
                    matches!(watch.kind(), Kind::Integer(value) if value >= 0 && value <= i64::from(i32::MAX))
                        && matches!(id.kind(), Kind::Integer(value) if value >= 0)
                });
                if !valid {
                    // report_file_notify_error renders whatever errno the
                    // preceding host call left behind, so GNU's own text here
                    // depends on session history; only the shape is stable.
                    return Err(crate::lisp::primitives::file_notify_error_with_errno(
                        "Invalid descriptor ",
                        &std::io::Error::last_os_error(),
                        args[0],
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
                if let Err(error) = fs::metadata(host_file_name(interp, env, &path)?) {
                    return Err(file_operation_error("File does not exist", &error, &path));
                }
                let flags = args[1]
                    .to_vec()?
                    .into_iter()
                    .filter_map(|flag| flag.symbol_str().map(str::to_string))
                    .collect::<Vec<_>>();
                if !function_value_p(interp, &args[2], env) {
                    return Err(wrong_type_argument("invalid-function", args[2]));
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
                        interp.register_kqueue_file_notify_watch(path, flags, args[2])?;
                    return Ok(Value::Integer(descriptor));
                }
                let descriptor =
                    FILE_NOTIFY_DESCRIPTOR_COUNTER.fetch_add(1, AtomicOrdering::Relaxed) as i64;
                interp.register_file_notify_watch(
                    descriptor,
                    (!remote_watch).then_some(path),
                    flags,
                    args[2],
                )?;
                Ok(Value::Integer(descriptor))
            }
            "kqueue-rm-watch" => {
                need_args(name, args, 1)?;
                if !interp.remove_file_notify_watch(&args[0]) {
                    return Err(LispError::SignalValue(Value::list([
                        Value::Symbol("file-notify-error".into()),
                        Value::String("Not a watch descriptor".into()),
                        args[0],
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
                let path = host_file_name(interp, env, &path)?;
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
                // fileio.c Fset_default_file_modes: the process's file mode
                // creation mask becomes ~MODE & 0777 (inherited by
                // subprocesses, honoured by every mkdir/open the runtime
                // makes), and `realmask' remembers it for
                // `default-file-modes'.
                let mode = args[0].as_integer()?;
                interp.default_file_modes = mode & 0o777;
                #[cfg(unix)]
                {
                    // SAFETY: umask only replaces the process's creation
                    // mask and cannot fail.
                    unsafe {
                        libc::umask((!mode & 0o777) as libc::mode_t);
                    }
                }
                Ok(Value::Nil)
            }
            "set-file-modes" => {
                need_args(name, args, 2)?;
                let path = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
                let host = host_file_name(interp, env, &path)?;
                let mode = args[1].as_integer()?;
                #[cfg(unix)]
                {
                    let mut permissions = fs::metadata(&host)
                        .map_err(|error| file_operation_error("Setting file modes", &error, &path))?
                        .permissions();
                    permissions.set_mode(mode as u32);
                    fs::set_permissions(&host, permissions).map_err(|error| {
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
                // dired.c file_name_completion reads the directory's stream,
                // "." and ".." among its entries: a directory that cannot be
                // opened answers no candidates at all.
                let entries = read_directory_names(&host_file_name(interp, env, &directory)?)
                    .map_err(|(step, error)| {
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
                            Value::String(step.into()),
                            Value::String(detail.into()),
                            Value::String(directory.clone().into()),
                        ]))
                    })?;
                // Each name read is DECODE_FILE'd before it is matched.
                let host_directory = host_file_name(interp, env, &directory)?;
                let entries = entries
                    .into_iter()
                    .map(|entry| {
                        let host_path = host_directory.join(&entry);
                        decode_host_file_name_text(interp, env, &entry)
                            .map(|(name, multibyte)| (name, multibyte, host_path))
                    })
                    .collect::<Result<Vec<_>, LispError>>()?;
                let ignore_case = completion_ignores_case(interp, env);
                let regexp_list = interp
                    .lookup_var("completion-regexp-list", env)
                    .and_then(|value| value.to_vec().ok())
                    .unwrap_or_default();
                // dired.c file_name_completion folds with Vascii_canon_table.
                let case_table = ignore_case.then(|| interp.standard_case_tables().canon);
                let mut matches = |candidate: &str| -> Result<bool, LispError> {
                    if !completion_matches_prefix(&prefix, candidate, ignore_case) {
                        return Ok(false);
                    }
                    for pattern in &regexp_list {
                        if !completion_regex_matches(interp, env, candidate, pattern, case_table)? {
                            return Ok(false);
                        }
                    }
                    Ok(true)
                };
                let mut names: Vec<(String, bool)> = Vec::new();
                for (file_name, multibyte, host_path) in entries {
                    if !matches(&file_name)? {
                        continue;
                    }
                    // Directories (following symlinks) get a trailing slash.
                    let is_directory = fs::metadata(host_path)
                        .map(|metadata| metadata.is_dir())
                        .unwrap_or(false);
                    names.push((
                        if is_directory {
                            format!("{file_name}/")
                        } else {
                            file_name
                        },
                        multibyte,
                    ));
                }
                // The matches are consed up as they are read: last read first.
                Ok(Value::list(names.into_iter().rev().map(
                    |(text, multibyte)| {
                        make_shared_string_value_with_multibyte(text, None, multibyte)
                    },
                )))
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
                    &[args[0], args[1]],
                    env,
                )
                .map_err(LispError::into_kind)
                {
                    Ok(names) => names,
                    Err(LispErrorKind::SignalValue(condition))
                        if condition
                            .to_vec()
                            .ok()
                            .and_then(|items| items.first().cloned())
                            .and_then(|head| head.symbol_str().map(str::to_string))
                            .as_deref()
                            == Some("file-missing") =>
                    {
                        return Ok(Value::Nil);
                    }
                    Err(error) => return Err(LispError::from(error)),
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
                        &[args[0], names, args.get(2).cloned().unwrap_or(Value::Nil)],
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
                    try_completion(interp, &[args[0], Value::list(candidates), Value::Nil], env)
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
                let host = host_file_name(interp, env, &path)?;
                let modified = match args.get(1).map(|v| v.kind()) {
                    None | Some(Kind::Nil) => SystemTime::now(),
                    Some(value) => file_modtime_from_value(interp, &value.value())?.modified,
                };
                set_file_times_path(
                    &path,
                    &host,
                    modified,
                    args.get(2).is_some_and(Value::is_truthy),
                )?;
                dispatch_file_notification(interp, env, &path, "attribute-changed")?;
                Ok(Value::T)
            }
            "insert-file-contents" => insert_file_contents(interp, env, args, false),

            "file-system-info" => {
                need_args(name, args, 1)?;
                let path = resolve_file_name_in_env(interp, env, &string_text(&args[0])?);
                let path = encode_file_name(interp, env, &path)?;
                #[cfg(unix)]
                {
                    let path = CString::new(path)
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
                let path = host_file_name(interp, env, &path)?;
                // fileio.c emacs_readlinkat: the link's contents DECODE_FILE'd.
                let target = fs::symlink_metadata(&path)
                    .ok()
                    .filter(|metadata| metadata.file_type().is_symlink())
                    .and_then(|_| fs::read_link(&path).ok());
                match target {
                    Some(target) => decode_host_file_name(interp, env, &target),
                    None => Ok(Value::Nil),
                }
            }
            "make-symbolic-link" => {
                if args.len() < 2 || args.len() > 3 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                // fileio.c:Fmake_symbolic_link, past the handler lookup
                // that expanded LINKNAME (and, for an integer
                // OK-IF-ALREADY-EXISTS, a `~' or `/:' TARGET).
                let target = string_text(&args[0])?;
                let mut linkname = resolve_file_name_in_env(interp, env, &string_text(&args[1])?);
                if directory_name_p(&linkname) {
                    linkname = file_name_concat(&[linkname, file_name_nondirectory(&target)]);
                }
                let ok_if_already_exists = args.get(2).copied().unwrap_or(Value::Nil);
                let interactive = matches!(ok_if_already_exists.kind(), Kind::Integer(_));
                let host_target = host_file_name(interp, env, &target)?;
                let host_link = host_file_name(interp, env, &linkname)?;
                let mut linked = symlink(&host_target, &host_link);
                if let Err(error) = &linked {
                    if error.raw_os_error() == Some(libc::ENOSYS) {
                        return Err(LispError::SignalValue(Value::list([
                            Value::symbol("file-error"),
                            Value::string("Symbolic links are not supported"),
                        ])));
                    }
                    if error.kind() == ErrorKind::AlreadyExists {
                        if ok_if_already_exists.is_nil() || interactive {
                            barf_or_query_if_file_exists(
                                interp,
                                env,
                                &linkname,
                                true,
                                "make it a link",
                                interactive,
                                false,
                            )?;
                        }
                        let _ = fs::remove_file(&host_link);
                        linked = symlink(&host_target, &host_link);
                    }
                }
                linked.map_err(|error| {
                    file_operation_error_files(
                        "Making symbolic link",
                        &error,
                        &[&target, &linkname],
                    )
                })?;
                Ok(Value::Nil)
            }
            "call-process" => {
                // callproc.c Fcall_process: INFILE (nil for the null device)
                // expanded in the process's directory and opened before
                // call_process runs.
                if args.is_empty() {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let infile = match args.get(1).filter(|infile| !infile.is_nil()) {
                    Some(infile) => {
                        let infile = string_text(infile)?;
                        let directory = process_current_directory(interp, env)?;
                        expand_file_name_in_env(interp, env, &infile, directory.as_deref())
                    }
                    None => NULL_DEVICE.to_string(),
                };
                let infile = unquote_local_file_name(&infile).unwrap_or(infile);
                let filefd =
                    fs::File::open(host_file_name(interp, env, &infile)?).map_err(|error| {
                        file_operation_error("Opening process input file", &error, &infile)
                    })?;
                call_process(interp, env, args, filefd, None)
            }
            "make-process" | "make-pipe-process" => make_process_value(interp, env, args),
            "get-buffer-process" => {
                need_arg_range(name, args, 1, 1)?;
                // process.c:Fget_buffer_process: nil names no buffer, and
                // Fget_buffer takes a buffer object as it is, killed or
                // not, and a name to its live buffer.
                let buffer_id = match args[0].kind() {
                    Kind::Nil => None,
                    Kind::Buffer(buffer) => Some(buffer.id),
                    _ => match string_like(&args[0]) {
                        Some(name) => interp.find_buffer(&name.text).map(|(id, _)| id),
                        None => return Err(wrong_type_argument("stringp", args[0])),
                    },
                };
                Ok(buffer_id
                    .and_then(|id| interp.process_value_for_buffer(id))
                    .unwrap_or(Value::Nil))
            }
            "process-buffer" => {
                need_args(name, args, 1)?;
                let process_id = interp.resolve_process_id(&args[0])?;
                Ok(interp.process_buffer_value(process_id))
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
                // Fprocess_status: a name finds only a process, and none
                // is nil; anything else is get_process's.
                let process = match string_like(&args[0]) {
                    Some(text) => match interp.find_process_id_by_name(&text.text) {
                        Some(id) => interp.record_value(id),
                        None => return Ok(Value::Nil),
                    },
                    None => get_process(interp, args.first())?,
                };
                let process_id = interp.resolve_process_id(&process)?;
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
                interp.set_process_plist_value(process_id, args[1]);
                Ok(args[1])
            }
            "process-attributes" => {
                need_args(name, args, 1)?;
                Ok(process_attributes_value(args[0].as_integer()?))
            }
            "list-system-processes" => {
                need_args(name, args, 0)?;
                list_system_processes_value()
            }
            "process-coding-system" => {
                need_args(name, args, 1)?;
                let process_id = interp.resolve_process_id(&args[0])?;
                interp.process_coding_system(process_id)
            }
            "set-process-coding-system" => {
                need_arg_range(name, args, 1, 3)?;
                // process.c Fset_process_coding_system: both checked, the
                // encoding's eol completed (coding_inherit_eol_type).
                let process_id = interp.resolve_process_id(&args[0])?;
                let decoding = args.get(1).cloned().unwrap_or(Value::Nil);
                let encoding = args.get(2).cloned().unwrap_or(Value::Nil);
                checked_coding_given(interp, &decoding)?;
                checked_coding_given(interp, &encoding)?;
                let encoding = crate::lisp::primitives::file_io::inherit_write_coding_eol(
                    interp,
                    encoding,
                    Value::Nil,
                )?;
                interp.set_process_coding_system(process_id, decoding, encoding)?;
                interp.reset_process_coding_structs(process_id);
                Ok(Value::Nil)
            }
            "internal-default-process-filter" => {
                need_args(name, args, 2)?;
                let process_id = interp.resolve_process_id(&args[0])?;
                if string_like(&args[1]).is_none() {
                    return Err(LispError::WrongTypeArgument("stringp".into(), args[1]));
                }
                internal_default_process_filter(interp, process_id, args[1], env)?;
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
                    Some(args[1])
                };
                interp.set_process_filter(process_id, filter)?;
                Ok(if args[1].is_nil() {
                    Value::symbol("internal-default-process-filter")
                } else {
                    args[1]
                })
            }
            "internal-default-process-sentinel" => {
                need_args(name, args, 2)?;
                let process_id = interp.resolve_process_id(&args[0])?;
                let message = string_text(&args[1])?;
                internal_default_process_sentinel(interp, process_id, &message, env)?;
                Ok(Value::Nil)
            }
            "set-process-sentinel" => {
                need_args(name, args, 2)?;
                let process_id = interp.resolve_process_id(&args[0])?;
                let sentinel = (!args[1].is_nil()).then(|| args[1]);
                interp.set_process_sentinel(process_id, sentinel);
                Ok(if args[1].is_nil() {
                    Value::symbol("internal-default-process-sentinel")
                } else {
                    args[1]
                })
            }
            "set-process-buffer" => {
                need_args(name, args, 2)?;
                let process_id = interp.resolve_process_id(&args[0])?;
                // process.c: CHECK_BUFFER; a killed buffer leaves the
                // process with no live buffer.
                let buffer_id = match args[1].kind() {
                    Kind::Nil => None,
                    Kind::Buffer(buffer) => interp.live_buffer_id(buffer),
                    _ => return Err(LispError::WrongTypeArgument("bufferp".into(), args[1])),
                };
                interp.set_process_buffer_id(process_id, buffer_id, args[1]);
                Ok(args[1])
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
                Ok(args[1])
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
                        args[1]
                    } else {
                        Value::Nil
                    },
                )
            }
            "process-type" => {
                need_args(name, args, 1)?;
                let process = get_process(interp, args.first())?;
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
                Ok(args[1])
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
                let process = get_process(interp, args.first())?;
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
                        *stream,
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
                if matches!(args[0].kind(), Kind::Record(_)) {
                    return Ok(args[0]);
                }
                let requested = string_text(&args[0])?;
                Ok(interp
                    .find_process_id_by_name(&requested)
                    .map(|id| interp.record_value(id))
                    .unwrap_or(Value::Nil))
            }
            "process-contact" => {
                // GNU Fprocess_contact: the stored contact plist (p->childp)
                // is t for a real child and is returned as-is no matter the
                // KEY; for a network process KEY t returns the whole plist,
                // KEY nil the (HOST SERVICE) pair, any other KEY a plist_get.
                need_arg_range(name, args, 1, 3)?;
                let process_id = interp.resolve_process_id(&args[0])?;
                if matches!(args.get(1).map(|v| v.kind()), Some(Kind::Symbol(key)) if key == ":remote")
                    && let Some(address) = interp.process_datagram_address(process_id)?
                {
                    return Ok(sockaddr_vector(address));
                }
                let contact = interp
                    .process_contact_plist(process_id)
                    .unwrap_or(Value::Nil);
                if !matches!(contact.kind(), Kind::Cons(_)) {
                    return Ok(contact);
                }
                match args.get(1).map(|v| v.kind()) {
                    Some(Kind::T) => Ok(contact),
                    None | Some(Kind::Nil) if interp.is_serial_process(process_id) => {
                        Ok(Value::list([
                            contact_plist_get(&contact, ":port"),
                            contact_plist_get(&contact, ":speed"),
                        ]))
                    }
                    None | Some(Kind::Nil) => Ok(Value::list([
                        contact_plist_get(&contact, ":host"),
                        contact_plist_get(&contact, ":service"),
                    ])),
                    Some(key) => Ok(key
                        .value()
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
                    .map_err(|_| wrong_type_argument("symbolp", args[1]))?;
                let option = crate::lisp::types::visible_symbol_name(raw).to_string();
                if set_socket_option(fd, &option, &args[1], &args[2])? {
                    // process.c:2990 records the accepted option on the
                    // process's contact plist, which `process-contact' reads.
                    interp.put_process_contact_option(process_id, &args[1], args[2]);
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
                let numeric = match args.get(2).map(|v| v.kind()) {
                    None | Some(Kind::Nil) => false,
                    Some(Kind::Symbol(hint)) if hint == "numeric" => true,
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
                need_arg_range(name, args, 0, 1)?;
                // Fdelete_process: the interactive spec passes `message'
                // for the current buffer's process, and reports it.
                let interactive = args.first().and_then(Value::symbol_str) == Some("message");
                let designator = if interactive { None } else { args.first() };
                let process_value = get_process(interp, designator)?;
                let process_id = interp.resolve_process_id(&process_value)?;
                delete_process_notifying(interp, process_id, env)?;
                if interactive {
                    super::call(interp, "message", &[Value::string("Deleted process")], env)?;
                }
                Ok(Value::Nil)
            }
            "internal-default-interrupt-process" => {
                need_arg_range(name, args, 0, 2)?;
                let original = args.first().cloned().unwrap_or(Value::Nil);
                let process = get_process(interp, args.first())?;
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
                let process_value = get_process(interp, args.first())?;
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
                let process = get_process(interp, args.first())?;
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
                let process = get_process(interp, args.first())?;
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
                Ok(args[1])
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
                        args[0],
                        args[1],
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
                let process = get_process(interp, args.first())?;
                let process_id = interp.resolve_process_id(&process)?;
                crate::lisp::primitives::processes::wait_while_connecting(interp, env, process_id)?;
                // Fprocess_send_eof: a coding that requires flushing is
                // flushed first.
                crate::lisp::primitives::processes::flush_process_encoder(interp, process_id, env)?;
                let (stdout, stderr) = interp.process_send_eof(process_id)?;
                deliver_process_streams(interp, process_id, &stdout, &stderr, env)?;
                Ok(process)
            }
            "process-send-string" => {
                need_args(name, args, 2)?;
                // Fprocess_send_string: CHECK_STRING, get_process, then
                // send_process with the string as the object it encodes.
                let Some(string) = string_like(&args[1]) else {
                    return Err(LispError::WrongTypeArgument("stringp".into(), args[1]));
                };
                let process = get_process(interp, args.first())?;
                let process_id = interp.resolve_process_id(&process)?;
                // send_process: wait_while_connecting.
                crate::lisp::primitives::processes::wait_while_connecting(interp, env, process_id)?;
                let encoded = crate::lisp::primitives::processes::encode_process_input(
                    interp,
                    process_id,
                    &args[1],
                    string.multibyte,
                    env,
                )?;
                let (stdout, stderr) = interp.process_send_string(process_id, &encoded)?;
                deliver_process_streams(interp, process_id, &stdout, &stderr, env)?;
                Ok(Value::Nil)
            }
            "process-send-region" => {
                need_args(name, args, 3)?;
                // Fprocess_send_region: the region's text with the current
                // buffer as the object, encoded as its multibyteness says.
                let process = get_process(interp, args.first())?;
                let process_id = interp.resolve_process_id(&process)?;
                crate::lisp::primitives::processes::wait_while_connecting(interp, env, process_id)?;
                // validate_region, then the region's text.
                validate_region(interp, &args[1], &args[2])?;
                let text = super::buffer_edit::direct_buffer_substring_no_properties(
                    interp,
                    &[args[1], args[2]],
                    env,
                )?;
                let multibyte = interp.buffer.borrow().is_multibyte();
                let encoded = crate::lisp::primitives::processes::encode_process_input(
                    interp, process_id, &text, multibyte, env,
                )?;
                let (stdout, stderr) = interp.process_send_string(process_id, &encoded)?;
                deliver_process_streams(interp, process_id, &stdout, &stderr, env)?;
                Ok(Value::Nil)
            }
            "zlib-decompress-region" => {
                need_arg_range(name, args, 2, 3)?;
                // decompress.c: validate_region, then the unibyte check.
                let (start, end) = validate_region(interp, &args[0], &args[1])?;
                if interp.buffer.borrow().is_multibyte() {
                    return Err(LispError::Signal(
                        "This function can be called only in unibyte buffers".into(),
                    ));
                }
                let compressed = interp
                    .buffer
                    .borrow()
                    .buffer_substring(start, end)
                    .map_err(|error| LispError::Signal(error.to_string()))?;
                let input = encode_raw_text_bytes(&compressed)?;
                // zlib.c: modify_text over the region, then one change.
                modify_current_buffer(interp, start, end, false, env, |interp, _, env| {
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
                        // unwind_decompress balances the before-change call.
                        return Ok((
                            Some(AfterChange::new(start, old_length, old_length)),
                            Value::Nil,
                        ));
                    }

                    let saved_point = interp.buffer.borrow().point();
                    // del_range_2's make_buffer_string_both.
                    crate::lisp::primitives::textprop::update_deleted_text_properties(
                        interp, start, end, false, env,
                    )?;
                    interp
                        .delete_region_current_buffer_unreturned(start, end)
                        .map_err(|error| LispError::Signal(error.to_string()))?;
                    interp.buffer.borrow_mut().goto_char(start);
                    let text = decode_raw_text_bytes(&output);
                    interp.insert_current_buffer(&text);
                    {
                        let point = saved_point.min(interp.buffer.borrow().point_max());
                        interp.buffer.borrow_mut().goto_char(point);
                    }
                    let change = AfterChange::new(start, old_length, output.len());
                    if complete {
                        Ok((Some(change), Value::T))
                    } else {
                        Ok((Some(change), Value::Integer(remaining as i64)))
                    }
                })
            }
            "libxml-parse-xml-region" | "libxml-parse-html-region" => {
                need_arg_range(name, args, 0, 4)?;
                // xml.c:parse_region: nil bounds are point-min and
                // point-max, then validate_region.
                let start = match args.first().filter(|value| !value.is_nil()) {
                    Some(start) => *start,
                    None => Value::Integer(interp.buffer.borrow().point_min() as i64),
                };
                let end = match args.get(1).filter(|value| !value.is_nil()) {
                    Some(end) => *end,
                    None => Value::Integer(interp.buffer.borrow().point_max() as i64),
                };
                let (start, end) = validate_region(interp, &start, &end)?;
                if let Some(base_url) = args.get(2)
                    && !base_url.is_nil()
                    && string_like(base_url).is_none()
                {
                    return Err(LispError::WrongTypeArgument("stringp".into(), *base_url));
                }
                let source = interp
                    .buffer
                    .borrow()
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
                // callproc.c Fcall_process_region: the input written as
                // create_temp_file writes it, the region deleted before the
                // child runs, then call_process with INFILE in DELETE's place.
                if args.len() < 3 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                let (mut start, mut end) = (args[0], args[1]);
                let empty_input = if let Some(string) = string_like(&start) {
                    string.text.is_empty()
                } else if start.is_nil() {
                    interp.buffer.borrow().size_total() == 0
                } else {
                    let (from, to) = validate_region(interp, &start, &end)?;
                    start = Value::Integer(from as i64);
                    end = Value::Integer(to as i64);
                    from == to
                };
                let (filefd, infile, tempfile) = if empty_input {
                    let filefd = fs::File::open(NULL_DEVICE).map_err(|error| {
                        LispError::SignalValue(file_operation_error_value_without_file(
                            "Opening null device",
                            &error,
                        ))
                    })?;
                    (filefd, Value::Nil, None)
                } else {
                    let (mut filefd, tempfile_name, tempfile) = create_temp_file(interp, env)?;
                    let coding = interp
                        .lookup_var("coding-system-for-write", env)
                        .unwrap_or(Value::Nil);
                    let coding = if !coding.is_nil() {
                        coding
                    } else if !interp.buffer.borrow().is_multibyte() {
                        Value::symbol("raw-text")
                    } else {
                        let lookup = std::iter::once(Value::symbol(name))
                            .chain(args.iter().copied())
                            .collect::<Vec<_>>();
                        find_operation_coding_system_value(interp, &lookup, env)?
                            .cons_values()
                            .map_or(Value::Nil, |(_, encoding)| encoding)
                    };
                    let coding = complement_process_encoding_system(interp, env, coding)?;
                    let contents = crate::lisp::primitives::file_io::encode_process_input(
                        interp, start, end, coding, env,
                    )?;
                    filefd.write_all(&contents).map_err(|error| {
                        file_operation_error("Write error", &error, &tempfile_name)
                    })?;
                    std::io::Seek::rewind(&mut filefd).map_err(|error| {
                        file_operation_error("Setting file position", &error, &tempfile_name)
                    })?;
                    (filefd, Value::string(&tempfile_name), Some(tempfile))
                };
                if args.get(3).is_some_and(Value::is_truthy) {
                    if start.is_nil() {
                        let buffer_id = interp.current_buffer_id();
                        interp.restore_labeled_restrictions(buffer_id, Vec::new());
                        interp.buffer.borrow_mut().widen();
                        let (beg, z) = {
                            let buffer = interp.buffer.borrow();
                            (buffer.point_min(), buffer.point_max())
                        };
                        delete_region_with_hooks(interp, beg, z, env)?;
                    } else {
                        let from = position_from_value(interp, &start)?;
                        let to = position_from_value(interp, &end)?;
                        delete_region_with_hooks(interp, from, to, env)?;
                    }
                }
                // PROGRAM INFILE BUFFER DISPLAY ARGS..., INFILE naming the
                // input's file (a string only these arguments hold).
                let call_args = std::iter::once(args[2])
                    .chain(std::iter::once(infile))
                    .chain(args.get(4..).unwrap_or(&[]).iter().copied())
                    .collect::<Vec<_>>();
                interp.with_lisp_stack_roots(&call_args, |interp| {
                    call_process(interp, env, &call_args, filefd, tempfile)
                })
            }
            "kill-buffer" => {
                need_arg_range(name, args, 0, 1)?;
                // Fget_buffer and nsberror; a killed buffer is not killed
                // again.
                let id = match args.first().filter(|buffer| !buffer.is_nil()) {
                    Some(buffer) => {
                        let buffer = interp.get_buffer_or_error(buffer)?;
                        let Some(id) = interp.live_buffer_id(buffer) else {
                            return Ok(Value::Nil);
                        };
                        id
                    }
                    None => interp.current_buffer_id(),
                };
                // thread.c:thread_check_current_buffer: a buffer another
                // thread has current is not killed, nor asked about.
                if interp.thread_check_current_buffer(id) {
                    return Ok(Value::Nil);
                }
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
                            &[interp.buffer_value(id).expect("live buffer object")],
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
                            .and_then(|buffer| buffer.file.clone());
                        if let Some(path) = auto_save_path.as_ref()
                            && let Ok(host) = host_file_name(interp, env, path)
                            && fs::metadata(&host).is_ok()
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
                                let _ = fs::remove_file(host);
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
                // buffer.c:Fkill_buffer once nothing more is asked: a buffer
                // the hooks killed is reported killed, and the minibuffer
                // now current is not killed.
                if !interp.has_buffer_id(id) {
                    return Ok(Value::T);
                }
                let buffer = interp.buffer_value(id).expect("live buffer object");
                let minibuffer_buffer = |interp: &Interpreter| {
                    crate::lisp::primitives::window::window_buffer_id(
                        interp,
                        &interp.minibuffer_window_value(),
                    )
                };
                if minibuffer_buffer(interp) == Some(id) {
                    return Ok(Value::Nil);
                }
                // A base buffer's indirect buffers are killed first, in the
                // buffer list's order, so that nothing terrible happens if
                // they ask questions or their hooks fail.
                if interp.buffer_base_id(id).is_none() {
                    let indirect: Vec<u64> = interp
                        .buffer_list
                        .iter()
                        .map(|(other, _)| *other)
                        .filter(|other| interp.buffer_base_id(*other) == Some(id))
                        .collect();
                    if !indirect.is_empty() {
                        for other in indirect {
                            let Some(value) = interp.buffer_value(other) else {
                                continue;
                            };
                            super::call(interp, "kill-buffer", &[value], env)?;
                            if let Some(survivor) = interp.get_buffer_by_id(other) {
                                return Err(LispError::Signal(format!(
                                    "Unable to kill buffer whose indirect buffer `{}' cannot be killed",
                                    survivor.name
                                )));
                            }
                        }
                        if !interp.has_buffer_id(id) {
                            return Ok(Value::T);
                        }
                    }
                }
                // replace_buffer_in_windows, before another buffer is made
                // current: window.el's `replace-buffer-in-windows' shows
                // another buffer in each window showing this one and
                // removes it from every window's buffer lists.
                let replace = cached_symbol!("replace-buffer-in-windows").with(Clone::clone);
                if interp
                    .raw_function_binding_symbol(&replace, env)
                    .is_some_and(|function| !function.is_nil())
                {
                    call_function_value(interp, &Value::Symbol(replace), &[buffer], env)?;
                }
                if !interp.has_buffer_id(id) {
                    return Ok(Value::T);
                }
                // This buffer stops being current (Fset_buffer of
                // Fother_buffer); when no other buffer can be, it is not
                // killed.
                let frame = interp.selected_frame_id;
                if id == interp.current_buffer_id() {
                    let other =
                        super::buffer_meta::other_buffer(interp, buffer, false, frame, env)?;
                    let other = interp.resolve_buffer_id(&other)?;
                    interp.set_current_buffer_id(other)?;
                    if id == interp.current_buffer_id() {
                        return Ok(Value::Nil);
                    }
                }
                // Nor when the buffer now current is the minibuffer's and
                // this one is the sole other buffer.
                if minibuffer_buffer(interp) == Some(interp.current_buffer_id())
                    && super::buffer_meta::other_buffer(interp, buffer, false, frame, env)?.word()
                        == buffer.word()
                {
                    return Ok(Value::Nil);
                }
                // GNU releases the target buffer's lock only after every query
                // and hook has accepted the kill, immediately before teardown.
                unlock_buffer(interp, env, id)?;
                // kill_buffer_processes: the buffer's connections are
                // deleted and its subprocesses hung up.
                for (process_id, connection) in interp.buffer_processes(id) {
                    if connection {
                        delete_process_notifying(interp, process_id, env)?;
                    } else {
                        #[cfg(unix)]
                        interp.signal_process_group(process_id, libc::SIGHUP, &Value::Nil)?;
                    }
                }
                // Their sentinels may have killed it.
                if !interp.has_buffer_id(id) {
                    return Ok(Value::T);
                }
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
                // buffer.c: CHECK_BUFFER; a killed buffer is in no list.
                let id = match args.first().map(|value| value.kind()) {
                    None | Some(Kind::Nil) => interp.current_buffer_id(),
                    Some(Kind::Buffer(buffer)) => match interp.live_buffer_id(buffer) {
                        Some(id) => id,
                        None => return Ok(Value::Nil),
                    },
                    Some(_) => {
                        return Err(LispError::WrongTypeArgument("bufferp".into(), args[0]));
                    }
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
                let position = interp.buffer.borrow().point() as i64;
                interp.copy_marker_value(&Value::Integer(position), false)
            }
            "mark-marker" => Ok(interp.buffer_mark_marker_value()),
            "point-min-marker" => {
                let position = interp.buffer.borrow().point_min() as i64;
                interp.copy_marker_value(&Value::Integer(position), false)
            }
            "point-max-marker" => {
                let position = interp.buffer.borrow().point_max() as i64;
                interp.copy_marker_value(&Value::Integer(position), false)
            }
            "marker-buffer" => {
                need_args(name, args, 1)?;
                let marker_id = marker_id_from_value(&args[0])?;
                Ok(marker_id.buffer().map(Value::Buffer).unwrap_or(Value::Nil))
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
                Ok(Value::Integer(marker_id.last_position() as i64))
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
                Ok(args[1])
            }
            "set-marker" => {
                need_args(name, args, 2)?;
                set_marker_value(interp, &args[0], &args[1], args.get(2))
            }
            "region-beginning" => match interp.buffer.borrow().region() {
                Some((beg, _)) => Ok(Value::Integer(beg as i64)),
                None => Err(LispError::Signal(
                    "The mark is not set now, so there is no region".into(),
                )),
            },
            "region-end" => match interp.buffer.borrow().region() {
                Some((_, end)) => Ok(Value::Integer(end as i64)),
                None => Err(LispError::Signal(
                    "The mark is not set now, so there is no region".into(),
                )),
            },
        }
    }
);

/// process.c:get_process: the process NAME designates -- a process, the
/// name of a process or of a buffer, a buffer, or nil for the current
/// buffer -- signaling when there is none.
fn get_process(interp: &mut Interpreter, name: Option<&Value>) -> Result<Value, LispError> {
    let name = name.copied().unwrap_or(Value::Nil);
    let buffer = if let Some(text) = string_like(&name) {
        // Fget_process, then Fget_buffer.
        if let Some(id) = interp.find_process_id_by_name(&text.text) {
            return Ok(interp.record_value(id));
        }
        interp
            .find_buffer_id(&text.text)
            .ok_or_else(|| LispError::Signal(format!("Process {} does not exist", text.text)))?
    } else {
        match name.kind() {
            Kind::Nil => interp.current_buffer_id(),
            Kind::Buffer(buffer)
                if interp
                    .buffer_object(buffer.id)
                    .is_some_and(|live| live.ptr_eq(&buffer)) =>
            {
                buffer.id
            }
            Kind::Buffer(_) => {
                return Err(LispError::Signal(
                    "Attempt to get process for a dead buffer".into(),
                ));
            }
            _ => {
                // CHECK_PROCESS.
                interp.resolve_process_id(&name)?;
                return Ok(name);
            }
        }
    };
    interp.process_value_for_buffer(buffer).ok_or_else(|| {
        let buffer_name = interp
            .buffer_list
            .iter()
            .find(|(id, _)| *id == buffer)
            .map(|(_, name)| name.as_str())
            .unwrap_or_default();
        LispError::Signal(format!("Buffer {buffer_name} has no process"))
    })
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
        .find(|pair| matches!(pair[0].kind(), Kind::Symbol(key) if key == ":file-handler"))
        .is_some_and(|pair| pair[1].is_truthy());
    if file_handler {
        let default_directory = interp
            .lookup_var("default-directory", env)
            .and_then(|value| string_like(&value).map(|string| string.text))
            .unwrap_or_default();
        if let Some(handler) = find_file_name_handler(
            interp,
            env,
            &default_directory,
            Value::symbol("make-process"),
        )? {
            let handler_args = std::iter::once(Value::symbol("make-process"))
                .chain(args.iter().cloned())
                .collect::<Vec<_>>();
            return interp.call_function_value(handler, None, &handler_args, env);
        }
    }
    let mut parsed = parse_make_process_args(interp, args)?;
    // Fmake_pipe_process opens its pipes once it has its name, and fails
    // when a channel lies beyond FD_SETSIZE.
    let pipe_channels = parsed
        .program
        .is_none()
        .then(open_pipe_process_channels)
        .transpose()?;
    let inherit_coding_system = parsed.buffer_id.is_some()
        && interp
            .lookup_var("inherit-process-coding-system", env)
            .is_some_and(|value| value.is_truthy());
    if let Some(program) = parsed.program.as_deref()
        && unquote_local_file_name(program).is_some()
    {
        parsed.program = Some(resolve_file_name_in_env(interp, env, program));
    }
    // Fmake_pipe_process always has a buffer: `:buffer', else the one
    // its name names, created when missing.
    if parsed.program.is_none() && parsed.buffer_id.is_none() {
        parsed.buffer_id = process_buffer_target(interp, &parsed.name_value)?;
    }
    let buffer = parsed
        .buffer_id
        .and_then(|id| interp.buffer_value(id))
        .unwrap_or(Value::Nil);
    let (decoding, encoding) = if parsed.program.is_some() {
        make_process_coding_systems(
            interp,
            env,
            parsed.coding_value,
            parsed.name_value,
            buffer,
            parsed.command,
        )?
    } else {
        connection_coding_systems(
            interp,
            env,
            ConnectionCoding::Pipe,
            parsed.coding_value,
            buffer,
        )?
    };
    // create_process's setup_process_coding_systems sets both up.
    checked_coding_given(interp, &decoding)?;
    checked_coding_given(interp, &encoding)?;
    // process.c finds the executable before forking; `process-command'
    // keeps the name the caller gave.
    let executable = match parsed.program.as_deref() {
        Some(program) => {
            let program = locate_program_for_exec(interp, env, program, true)?;
            Some(host_file_name(interp, env, &program)?.into_os_string())
        }
        None => None,
    };
    let arguments = if executable.is_some() {
        let command = parsed.command.to_vec()?;
        make_process_arguments(interp, env, command.get(1..).unwrap_or(&[]), encoding)?
    } else {
        Vec::new()
    };
    // Fmake_process makes a `:stderr' buffer's pipe process before
    // create_process forks.
    let stderr_channels = parsed
        .stderr_buffer_id
        .map(|_| open_pipe_process_channels())
        .transpose()?;
    let runtime = executable.as_ref().map(|command| {
        spawn_persistent_process(
            interp,
            command,
            &arguments,
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
        if let Some(channels) = stderr_channels {
            interp.install_pipe_process_channels(stderr_process_id, channels);
        }
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
    if let Some(channels) = pipe_channels {
        interp.install_pipe_process_channels(process_id, channels);
    }
    // create_process closes its standard error pipe process's input.
    if executable.is_some()
        && let Some(stderr_process_id) = parsed.stderr_process_id
    {
        interp.close_pipe_process_input(stderr_process_id);
    }
    interp.set_process_query_on_exit_flag(process_id, parsed.query_on_exit_flag)?;
    interp.set_process_inherit_coding_system_flag(process_id, inherit_coding_system)?;
    interp.set_process_filter(process_id, parsed.filter)?;
    interp.set_process_sentinel(process_id, parsed.sentinel);
    interp.set_process_stderr(process_id, parsed.stderr_process_id);
    interp.set_process_coding_system(process_id, decoding, encoding)?;
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
    let process = get_process(interp, Some(target))?;
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
fn file_mode_string_for_metadata(metadata: &fs::Metadata) -> String {
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
fn file_mode_string_for_metadata(metadata: &fs::Metadata) -> String {
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
        match value.kind() {
            crate::lisp::types::Kind::Nil => None,
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
        match value.kind() {
            crate::lisp::types::Kind::Integer(count) => match libc::c_int::try_from(count) {
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
                let seconds = match value.kind() {
                    crate::lisp::types::Kind::Integer(count) => i32::try_from(count).ok(),
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
            *option_symbol,
            *value,
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

/// CHECK_STRING, then the string's internal bytes and multibyteness.
fn file_name_bytes(value: &Value) -> Result<(Vec<u8>, bool), LispError> {
    match value.kind() {
        Kind::StringObject(state) => {
            let state = state.borrow();
            Ok((state.bytes().to_vec(), state.is_multibyte()))
        }
        _ => Err(wrong_type_argument("stringp", *value)),
    }
}

/// gnulib renameatu with RENAME_NOREPLACE (fileio.c
/// emacs_renameat_noreplace): FROM renamed to TO unless TO exists, else
/// the errno -- ENOSYS where the system cannot refuse to replace.
fn renameat_noreplace(from: &std::path::Path, to: &std::path::Path) -> Result<(), i32> {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::ffi::OsStrExt;
        let from = std::ffi::CString::new(from.as_os_str().as_bytes()).map_err(|_| libc::EINVAL)?;
        let to = std::ffi::CString::new(to.as_os_str().as_bytes()).map_err(|_| libc::EINVAL)?;
        // SAFETY: both names are NUL-terminated and live across the call.
        let result = unsafe {
            libc::renameat2(
                libc::AT_FDCWD,
                from.as_ptr(),
                libc::AT_FDCWD,
                to.as_ptr(),
                libc::RENAME_NOREPLACE,
            )
        };
        if result == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error()
                .raw_os_error()
                .unwrap_or(libc::EIO))
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (from, to);
        Err(libc::ENOSYS)
    }
}
