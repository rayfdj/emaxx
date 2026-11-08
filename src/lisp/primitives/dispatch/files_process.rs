use super::*;
use crate::lisp::types::Kind;

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

/// FOR_EACH_LIVE_BUFFER's first buffer whose NAME (its file name or
/// truename) is a string Fstring_equal to FILENAME, or nil.
fn buffer_visiting_file_name(
    interp: &Interpreter,
    name: impl Fn(&crate::buffer::Buffer) -> Value,
    filename: &Value,
) -> Value {
    interp
        .buffer_list
        .iter()
        .find_map(|(id, _)| {
            let buffer = interp.get_buffer_by_id(*id)?;
            let file = name(&buffer);
            (file.is_string() && crate::lisp::primitives::strings_equal(&file, filename))
                .then(|| interp.buffer_value(*id))
                .flatten()
        })
        .unwrap_or(Value::Nil)
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
            run_hook(
                interp,
                &cached_symbol!("buffer-list-update-hook").symbol(),
                env,
            )?;
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
    /// process.c's NILP (family), then EQ (family, Qipv4) and EQ (family,
    /// Qipv6): a symbol with position is its symbol while
    /// symbols-with-pos-enabled, a positioned nil not nil.
    fn parse(
        interp: &Interpreter,
        value: Option<&Value>,
        unsupported: &str,
    ) -> Result<Self, LispError> {
        let Some(value) = value.copied().filter(|value| !value.is_nil()) else {
            return Ok(Self::Both);
        };
        match crate::lisp::primitives::lisp_symbol(interp, value).map(|symbol| symbol.kind()) {
            Some(Kind::Symbol(family)) if family.is_named("ipv4") => Ok(Self::Ipv4),
            Some(Kind::Symbol(family)) if family.is_named("ipv6") => Ok(Self::Ipv6),
            _ => Err(LispError::Signal(unsupported.into())),
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
    host: &[u8],
    family: AddressFamily,
    numeric: bool,
) -> Result<Vec<std::net::SocketAddr>, String> {
    // process.c passes the string data to getaddrinfo, so an embedded NUL
    // terminates the name instead of making construction of a Rust CString
    // fail.
    let host_prefix = host.split(|byte| *byte == 0).next().unwrap_or_default();
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

fn network_interface_list(interp: &Interpreter, args: &[Value]) -> Result<Value, LispError> {
    need_arg_range("network-interface-list", args, 0, 2)?;
    let full = args.first().is_some_and(Value::is_truthy);
    // Fnetwork_interface_list's error ("Unsupported address family").
    let family = AddressFamily::parse(interp, args.get(1), "Unsupported address family")?;
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
                    return Ok(buffer.borrow().file_name());
                }
                // `buffer-file-name' is a per-buffer special variable.  A
                // dynamic binding in the current buffer is observable both
                // through the variable and through this primitive; reading
                // only the host Buffer field lost bindings used by ordinary
                // save code (including backup creation).
                Ok(interp
                    .lookup_var_key(cached_symbol!("buffer-file-name"), env)
                    .unwrap_or(Value::Nil))
            }
            "visited-file-modtime" => {
                need_args(name, args, 0)?;
                // buffer_visited_file_modtime: the flag 0 for an unknown
                // time and -1 for a file that did not exist.
                let stamp = interp.buffer.borrow().visited_file_modtime().stamp;
                match stamp {
                    crate::buffer::ModtimeStamp::Unknown => Ok(Value::Integer(0)),
                    crate::buffer::ModtimeStamp::Nonexistent => Ok(Value::Integer(-1)),
                    crate::buffer::ModtimeStamp::Time(time) => system_time_list_value(time),
                }
            }
            "verify-visited-file-modtime" => {
                need_arg_range(name, args, 0, 1)?;
                // fileio.c: decode_buffer, so a killed buffer's file and
                // time are read as it left them.
                let buffer = interp.decode_buffer(args.first())?;
                let buffer = buffer.borrow();
                let Some(path) = buffer.file_name_text() else {
                    return Ok(Value::T);
                };
                let path = path.as_str();
                let visited = buffer.visited_file_modtime();
                if visited.stamp == crate::buffer::ModtimeStamp::Unknown {
                    return Ok(Value::T);
                }
                // The file's time, or time_error_value's for the failure,
                // and its size.
                let (stamp, size) = match stat_visited_modtime(interp, env, path)? {
                    Ok(current) => (current.stamp, current.size),
                    Err(error) => (time_error_value(&error), -1),
                };
                Ok(
                    if visited.stamp == stamp && (visited.size < 0 || size == visited.size) {
                        Value::T
                    } else {
                        Value::Nil
                    },
                )
            }
            "set-visited-file-modtime" => {
                // fileio.c:Fset_visited_file_modtime: a FLAG of 0 or -1
                // or a time, of unknown size; or else the visited file's
                // status, which an indirect buffer does not have.  Its
                // preamble passes the expanded visited name after FLAG.
                let time_flag = args.first().copied().unwrap_or(Value::Nil);
                let modtime = if !time_flag.is_nil() {
                    let stamp = match time_flag.kind() {
                        Kind::Integer(flag) => match flag {
                            0 => crate::buffer::ModtimeStamp::Unknown,
                            -1 => crate::buffer::ModtimeStamp::Nonexistent,
                            // check_integer_range (TIME_FLAG, -1, 0).
                            _ => {
                                return Err(LispError::SignalValue(Value::list([
                                    Value::symbol("args-out-of-range"),
                                    time_flag,
                                    Value::Integer(-1),
                                    Value::Integer(0),
                                ])));
                            }
                        },
                        _ => crate::buffer::ModtimeStamp::Time(
                            file_modtime_from_value(interp, &time_flag)?.modified,
                        ),
                    };
                    crate::buffer::VisitedModtime { stamp, size: -1 }
                } else if interp.buffer_base_id(interp.current_buffer_id()).is_some() {
                    return Err(LispError::Signal(
                        "An indirect buffer does not have a visited file".into(),
                    ));
                } else {
                    let path = string_text(&args[1])?;
                    match stat_visited_modtime(interp, env, &path)? {
                        Ok(modtime) => modtime,
                        // file_attribute_errno: a missing file leaves the
                        // recorded time as it was.
                        Err(error)
                            if time_error_value(&error)
                                == crate::buffer::ModtimeStamp::Nonexistent =>
                        {
                            return Ok(Value::Nil);
                        }
                        Err(error) => {
                            return Err(crate::lisp::primitives::file_io::report_file_errno(
                                interp,
                                "Getting attributes",
                                args[1],
                                &error,
                            ));
                        }
                    }
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
                // strcmp (SSDATA (BVAR (XBUFFER (buffer), name)),
                // "*scratch*") == 0.
                let is_scratch = crate::lisp::primitives::sdata_c_string_is(
                    &buffer.borrow().name(),
                    b"*scratch*",
                );
                let mode = if is_scratch {
                    interp
                        .lookup_var_key(cached_symbol!("initial-major-mode"), env)
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
                    let current = interp
                        .lookup_var_key(cached_symbol!("major-mode"), env)
                        .unwrap_or(Value::Nil);
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
                // buffer.c:Fget_file_buffer past the preamble's CHECK_STRING,
                // Fexpand_file_name and `get-file-buffer' handler: the first
                // live buffer whose string file name is Fstring_equal to the
                // expanded FILENAME.
                Ok(buffer_visiting_file_name(
                    interp,
                    |buffer| buffer.file_name(),
                    &args[0],
                ))
            }
            "get-truename-buffer" => {
                need_args(name, args, 1)?;
                // buffer.c:Fget_truename_buffer compares each string
                // truename with Fstring_equal, so a symbol is taken by its
                // name and FILENAME is only checked once a truename meets it.
                let truenames = interp
                    .buffer_list
                    .iter()
                    .filter_map(|(id, _)| {
                        let truename = interp.get_buffer_by_id(*id)?.file_truename();
                        truename.is_string().then_some((*id, truename))
                    })
                    .collect::<Vec<_>>();
                if truenames.is_empty() {
                    return Ok(Value::Nil);
                }
                let file = Value::StringObject(crate::lisp::primitives::string_comparison_object(
                    interp, &args[0], env,
                )?);
                Ok(truenames
                    .into_iter()
                    .find(|(_, truename)| crate::lisp::primitives::strings_equal(truename, &file))
                    .map(|(id, _)| interp.buffer_value(id).expect("live buffer object"))
                    .unwrap_or(Value::Nil))
            }
            "find-buffer" => {
                need_args(name, args, 2)?;
                // buffer.c:Ffind_buffer: the first live buffer whose
                // Fbuffer_local_value of VARIABLE is `equal' to VALUE.
                let buffers = interp
                    .buffer_list
                    .iter()
                    .map(|(id, _)| *id)
                    .collect::<Vec<_>>();
                for id in buffers {
                    let Some(buffer) = interp.buffer_value(id) else {
                        continue;
                    };
                    let value = super::call(interp, "buffer-local-value", &[args[0], buffer], env)?;
                    if values_equal(interp, &args[1], &value)? {
                        return Ok(buffer);
                    }
                }
                Ok(Value::Nil)
            }
            "expand-file-name" => {
                if args.is_empty() || args.len() > 2 {
                    return Err(LispError::WrongNumberOfArgs(name.into(), args.len()));
                }
                crate::lisp::primitives::fexpand_file_name(
                    interp,
                    env,
                    args[0],
                    args.get(1).copied().unwrap_or(Value::Nil),
                )
            }
            "substitute-in-file-name" => {
                need_args(name, args, 1)?;
                crate::lisp::primitives::system::substitute_in_file_name(interp, env, args[0])
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
                let mut path = string_text(&args[0])?;
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
            "file-name-concat" => {
                if args.is_empty() {
                    return Err(LispError::WrongNumberOfArgs(name.into(), 0));
                }
                // fileio.c:Ffile_name_concat: the strings that are neither
                // nil nor empty, each followed by a slash unless it is the
                // last or ends in one (a slash it starts with stays).  A
                // result is multibyte when a component is multibyte and
                // not ASCII, and then a unibyte non-ASCII one is made
                // multibyte, its raw bytes eight-bit characters.  (GNU
                // sizes the result by the components' characters before
                // that conversion, and when no component was multibyte
                // writes past a unibyte result's end; Emaxx keeps such a
                // component's bytes.)
                let mut elements = Vec::new();
                let mut multibytes = 0;
                for arg in args {
                    if arg.is_nil() {
                        continue;
                    }
                    let (bytes, multibyte) = file_name_bytes(arg)?;
                    if bytes.is_empty() {
                        continue;
                    }
                    if multibyte && !bytes.is_ascii() {
                        multibytes += 1;
                    }
                    elements.push((bytes, multibyte));
                }
                let multibyte = multibytes != 0;
                let mut result = Vec::new();
                let count = elements.len();
                for (index, (bytes, element_multibyte)) in elements.into_iter().enumerate() {
                    if multibyte && !element_multibyte {
                        for byte in bytes {
                            super::strings::push_byte_as_multibyte(&mut result, byte);
                        }
                    } else {
                        result.extend_from_slice(&bytes);
                    }
                    if index + 1 < count && result.last() != Some(&b'/') {
                        result.push(b'/');
                    }
                }
                make_specified_string(result, multibyte)
                    .map_err(|error| string_storage_error(interp, env, error))
            }
            "find-file-name-handler" => {
                need_args(name, args, 2)?;
                let file = string_text(&args[0])?;
                Ok(find_file_name_handler(interp, env, &file, args[1])?.unwrap_or(Value::Nil))
            }
            // fileio.c:Funhandled_file_name_directory without a handler:
            // Ffile_name_as_directory, which may have one.
            "unhandled-file-name-directory" => {
                need_args(name, args, 1)?;
                super::call(interp, "file-name-as-directory", &[args[0]], env)
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
                let full = args.get(1).is_some_and(Value::is_truthy);
                let matcher = args.get(2).filter(|value| !value.is_nil()).copied();
                let nosort = args.get(3).is_some_and(Value::is_truthy);
                let count = directory_files_count(args.get(4))?;
                directory_files(
                    interp,
                    args[0],
                    full,
                    matcher.as_ref(),
                    nosort,
                    count,
                    None,
                    env,
                )
            }
            // dired.c:Fdirectory_files_and_attributes past its expansion
            // and handler lookup: directory_files_internal with ATTRS.
            "directory-files-and-attributes" => {
                need_arg_range(name, args, 1, 6)?;
                let full = args.get(1).is_some_and(Value::is_truthy);
                let matcher = args.get(2).filter(|value| !value.is_nil()).copied();
                let nosort = args.get(3).is_some_and(Value::is_truthy);
                let id_format = args.get(4).copied().unwrap_or(Value::Nil);
                let count = directory_files_count(args.get(5))?;
                directory_files(
                    interp,
                    args[0],
                    full,
                    matcher.as_ref(),
                    nosort,
                    count,
                    Some(id_format),
                    env,
                )
            }
            // fileio.c:Ffile_directory_p: file_directory_p of the name
            // expand_and_dir_to_file made, ENCODE_FILE'd.
            "file-directory-p" => {
                need_args(name, args, 1)?;
                let host = encoded_file_name(interp, env, &args[0])?;
                Ok(lisp_truth(file_directory_p(interp, env, &host)?))
            }
            // fileio.c:Ffile_accessible_directory_p: the expanded name
            // ENCODE_FILE'd, file_accessible_directory_p.
            "file-accessible-directory-p" => {
                need_args(name, args, 1)?;
                let host = encoded_file_name(interp, env, &args[0])?;
                Ok(lisp_truth(
                    accessible_directory(host.as_os_str().as_encoded_bytes()).is_ok(),
                ))
            }
            // fileio.c:check_file_access: the expanded name ENCODE_FILE'd
            // and file_access_p's faccessat (AMODE, AT_EACCESS).
            "file-exists-p" | "file-readable-p" | "file-executable-p" => {
                need_args(name, args, 1)?;
                let amode = match name {
                    "file-exists-p" => libc::F_OK,
                    "file-readable-p" => libc::R_OK,
                    _ => libc::X_OK,
                };
                let host = encoded_file_name(interp, env, &args[0])?;
                Ok(lisp_truth(file_access_p(&host, amode).is_ok()))
            }
            "access-file" => {
                // fileio.c:Faccess_file past its handler lookup: faccessat
                // (R_OK, AT_EACCESS) of the ENCODE_FILE'd expanded name,
                // and on failure report_file_error (STRING, FILENAME) with
                // its errno -- FILENAME as given, which the preamble
                // passes after the two arguments.
                need_args(name, args, 3)?;
                let host = encoded_file_name(interp, env, &args[0])?;
                match faccessat_errno(&host, libc::R_OK) {
                    Ok(()) => Ok(Value::Nil),
                    // report_file_error (SSDATA (string), filename).
                    Err(error) => Err(crate::lisp::primitives::file_io::report_file_errno_sdata(
                        interp, &args[1], args[2], &error,
                    )),
                }
            }
            // fileio.c:Ffile_regular_p: an fstatat of the name
            // expand_and_dir_to_file made, which GNU passes as its SSDATA,
            // not ENCODE_FILE'd.
            "file-regular-p" => {
                need_args(name, args, 1)?;
                let (bytes, _) = file_name_bytes(&args[0])?;
                let regular = std::ffi::CString::new(bytes).is_ok_and(|name| {
                    use std::os::unix::ffi::OsStrExt;
                    fs::metadata(std::ffi::OsStr::from_bytes(name.as_bytes()))
                        .is_ok_and(|metadata| metadata.is_file())
                });
                Ok(lisp_truth(regular))
            }
            // fileio.c:Ffile_writable_p: W_OK on the expanded name, and
            // for one that does not exist W_OK | X_OK on its directory.
            "file-writable-p" => {
                need_args(name, args, 1)?;
                let host = encoded_file_name(interp, env, &args[0])?;
                Ok(lisp_truth(file_writable_p(&host)))
            }
            // dired.c:Ffile_attributes past its expansion and handler
            // lookup: file_attributes of the name ENCODE_FILE'd.
            "file-attributes" => {
                need_arg_range(name, args, 1, 2)?;
                let id_format = args.get(1).copied().unwrap_or(Value::Nil);
                let host = encoded_file_name(interp, env, &args[0])?;
                file_attributes(interp, env, &host, args[0], id_format)
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
                            .lookup_var_key(cached_symbol!("user-real-login-name"), env)
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
                let first = string_text(&args[0])?;
                let second = string_text(&args[1])?;
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
            "file-acl" => {
                need_args(name, args, 1)?;
                // fileio.c:Ffile_acl: expand_and_dir_to_file, ENCODE_FILE,
                // acl_get_file and acl_to_text.
                let absname = string_text(&args[0])?;
                let path = host_file_name(interp, env, &absname)?;
                #[cfg(target_os = "linux")]
                if let Some(libacl) = crate::lisp::primitives::system::acl::libacl() {
                    use crate::lisp::primitives::system::acl::{ACL_TYPE_ACCESS, acl_errno_valid};
                    let Some(path) = c_file_name(&path) else {
                        return Ok(Value::Nil);
                    };
                    // SAFETY: PATH is NUL-terminated; the ACL and its text
                    // are libacl's, freed once each with acl_free.
                    unsafe {
                        let acl = (libacl.get_file)(path.as_ptr(), ACL_TYPE_ACCESS);
                        if acl.is_null() {
                            let errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
                            if errno == libc::ENOENT
                                || errno == libc::ENOTDIR
                                || !acl_errno_valid(errno)
                            {
                                return Ok(Value::Nil);
                            }
                            return Err(crate::lisp::primitives::file_io::report_file_errno(
                                interp,
                                "Getting ACLs",
                                args[0],
                                &std::io::Error::from_raw_os_error(errno),
                            ));
                        }
                        let text = (libacl.to_text)(acl, std::ptr::null_mut());
                        if text.is_null() {
                            let error = std::io::Error::last_os_error();
                            (libacl.free)(acl);
                            return Err(crate::lisp::primitives::file_io::report_file_errno(
                                interp,
                                "Getting ACLs",
                                args[0],
                                &error,
                            ));
                        }
                        let bytes = std::ffi::CStr::from_ptr(text).to_bytes().to_vec();
                        (libacl.free)(text.cast());
                        (libacl.free)(acl);
                        return crate::lisp::primitives::strings::make_string(bytes)
                            .map_err(|error| LispError::Signal(error.to_string()));
                    }
                }
                let _ = path;
                Ok(Value::Nil)
            }
            "set-file-acl" => {
                need_args(name, args, 2)?;
                // fileio.c:Fset_file_acl: a string ACL through acl_from_text
                // and acl_set_file; t when it is set, nil otherwise.
                let absname = string_text(&args[0])?;
                #[cfg(target_os = "linux")]
                if let Kind::StringObject(acl_string) = args[1].kind()
                    && let Some(libacl) = crate::lisp::primitives::system::acl::libacl()
                {
                    use crate::lisp::primitives::system::acl::{ACL_TYPE_ACCESS, acl_errno_valid};
                    // SSDATA: the string's bytes up to a NUL.
                    let mut text = acl_string.borrow().bytes().to_vec();
                    if let Some(nul) = text.iter().position(|byte| *byte == 0) {
                        text.truncate(nul);
                    }
                    let text = std::ffi::CString::new(text).expect("no NUL is left");
                    // SAFETY: TEXT and PATH are NUL-terminated; the ACL is
                    // libacl's, freed once with acl_free.
                    unsafe {
                        let acl = (libacl.from_text)(text.as_ptr());
                        if acl.is_null() {
                            let error = std::io::Error::last_os_error();
                            if acl_errno_valid(error.raw_os_error().unwrap_or(0)) {
                                return Err(crate::lisp::primitives::file_io::report_file_errno(
                                    interp,
                                    "Converting ACL",
                                    args[0],
                                    &error,
                                ));
                            }
                            return Ok(Value::Nil);
                        }
                        let path = host_file_name(interp, env, &absname)?;
                        let failed = match c_file_name(&path) {
                            Some(path) => {
                                (libacl.set_file)(path.as_ptr(), ACL_TYPE_ACCESS, acl) != 0
                            }
                            None => true,
                        };
                        let error = std::io::Error::last_os_error();
                        (libacl.free)(acl);
                        if failed && acl_errno_valid(error.raw_os_error().unwrap_or(0)) {
                            return Err(crate::lisp::primitives::file_io::report_file_errno(
                                interp,
                                "Setting ACL",
                                args[0],
                                &error,
                            ));
                        }
                        return Ok(if failed { Value::Nil } else { Value::T });
                    }
                }
                let _ = absname;
                Ok(Value::Nil)
            }
            // This build has no SELinux support: GNU's answers without
            // HAVE_LIBSELINUX enabled.
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
                let file = string_text(&args[0])?;
                validate_file_name(&file)?;
                let newname = string_text(&args[1])?;
                validate_file_name(&newname)?;
                // The Lisp names report_file_error is given: FILE and the
                // expand_cp_target NEWNAME the preamble made.
                let file_value = args[0];
                let newname_value = args[1];
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
                    .map_err(|error| {
                        crate::lisp::primitives::file_io::report_file_errno(
                            interp,
                            "Opening input file",
                            file_value,
                            &error,
                        )
                    })?;
                let st = input.metadata().map_err(|error| {
                    crate::lisp::primitives::file_io::report_file_errno(
                        interp,
                        "Input file status",
                        file_value,
                        &error,
                    )
                })?;
                // We can copy only regular files.
                if !st.is_file() {
                    return Err(crate::lisp::primitives::file_io::report_file_errno(
                        interp,
                        "Non-regular file",
                        file_value,
                        &std::io::Error::from_raw_os_error(if st.is_dir() {
                            libc::EISDIR
                        } else {
                            libc::EINVAL
                        }),
                    ));
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
                                crate::lisp::primitives::file_io::report_file_errno(
                                    interp,
                                    "Opening output file",
                                    newname_value,
                                    &error,
                                )
                            })?
                    }
                    Err(error) => {
                        return Err(crate::lisp::primitives::file_io::report_file_errno(
                            interp,
                            "Opening output file",
                            newname_value,
                            &error,
                        ));
                    }
                };
                let mut oldsize = 0;
                if already_exists {
                    let out_st = output.metadata().map_err(|error| {
                        crate::lisp::primitives::file_io::report_file_errno(
                            interp,
                            "Output file status",
                            newname_value,
                            &error,
                        )
                    })?;
                    if st.dev() == out_st.dev() && st.ino() == out_st.ino() {
                        return Err(crate::lisp::primitives::file_io::report_file_errno_files(
                            interp,
                            "Input and output files are the same",
                            &[file_value, newname_value],
                            &std::io::Error::from_raw_os_error(0),
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
                                return Err(crate::lisp::primitives::file_io::report_file_errno(
                                    interp,
                                    "Read error",
                                    file_value,
                                    &error,
                                ));
                            }
                        };
                        output.write_all(&buf[..copied]).map_err(|error| {
                            crate::lisp::primitives::file_io::report_file_errno(
                                interp,
                                "Write error",
                                newname_value,
                                &error,
                            )
                        })?;
                        newsize += copied as u64;
                    }
                }
                // Truncate an existing output file after writing the data.
                if newsize < oldsize {
                    output.set_len(newsize).map_err(|error| {
                        crate::lisp::primitives::file_io::report_file_errno(
                            interp,
                            "Truncating output file",
                            newname_value,
                            &error,
                        )
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
                // qcopy_acl: the mode, then the file's ACL attributes.
                #[cfg(target_os = "linux")]
                if preserve_permissions {
                    let (Some(src_name), Some(dst_name)) =
                        (c_file_name(&host_file), c_file_name(&host_newname))
                    else {
                        unreachable!("c_file_name stops at a NUL")
                    };
                    if crate::lisp::primitives::system::acl::qcopy_acl(
                        &src_name,
                        ifd,
                        &dst_name,
                        ofd,
                        preserved_permissions as libc::mode_t,
                    ) != 0
                    {
                        return Err(crate::lisp::primitives::file_io::report_file_errno(
                            interp,
                            "Copying permissions to",
                            newname_value,
                            &std::io::Error::last_os_error(),
                        ));
                    }
                }
                // Otherwise a new file gets the default permissions its
                // creation mask did not give it.
                let chmod = if preserve_permissions {
                    cfg!(not(target_os = "linux")).then_some(preserved_permissions)
                } else if already_exists || (new_mask & !realmask) == default_permissions {
                    None
                } else {
                    Some(default_permissions)
                };
                // SAFETY: fchmod on the open output descriptor.
                if let Some(mode) = chmod
                    && unsafe { libc::fchmod(ofd, mode as libc::mode_t) } != 0
                {
                    return Err(crate::lisp::primitives::file_io::report_file_errno(
                        interp,
                        "Copying permissions to",
                        newname_value,
                        &std::io::Error::last_os_error(),
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
                            newname_value,
                        ])));
                    }
                }
                // emacs_close: a close interrupted by a signal closed.
                let ofd = output.into_raw_fd();
                // SAFETY: the descriptor was just released by its File.
                if unsafe { libc::close(ofd) } < 0 {
                    let error = std::io::Error::last_os_error();
                    if error.kind() != ErrorKind::Interrupted {
                        return Err(crate::lisp::primitives::file_io::report_file_errno(
                            interp,
                            "Write error",
                            newname_value,
                            &error,
                        ));
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
                let file = string_text(&args[0])?;
                validate_file_name(&file)?;
                let newname = string_text(&args[1])?;
                validate_file_name(&newname)?;
                // The Lisp names Frename_file reports and passes on: FILE
                // and the expand_cp_target NEWNAME the preamble made.
                let file_value = args[0];
                let newname_value = args[1];
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
                    crate::lisp::primitives::file_io::report_file_errno_files(
                        interp,
                        "Renaming",
                        &[file_value, newname_value],
                        &std::io::Error::from_raw_os_error(errno),
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
                let path = string_text(&args[0])?;
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
                let filename =
                    crate::lisp::primitives::fexpand_file_name(interp, env, args[0], Value::Nil)?;
                let path = string_text(&filename)?;
                // Fdelete_file_internal: `unlink' failing with anything but
                // ENOENT is report_file_error ("Removing old name", filename)
                // -- the errno's own condition (`permission-denied' for
                // EACCES, which url-cache's pruner and callers handling
                // `file-error' expect), not a plain `error'.
                let removed = match fs::remove_file(host_file_name(interp, env, &path)?) {
                    Ok(()) => true,
                    Err(error) if error.kind() == ErrorKind::NotFound => false,
                    Err(error) => {
                        return Err(crate::lisp::primitives::file_io::report_file_errno(
                            interp,
                            "Removing old name",
                            filename,
                            &error,
                        ));
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
                // Fdelete_directory_internal: the expanded name through
                // Fdirectory_file_name, and report_file_error ("Removing
                // directory", directory) on `rmdir' failure.
                let directory =
                    crate::lisp::primitives::fexpand_file_name(interp, env, args[0], Value::Nil)?;
                let directory = super::call(interp, "directory-file-name", &[directory], env)?;
                let path = string_text(&directory)?;
                fs::remove_dir(host_file_name(interp, env, &path)?).map_err(|error| {
                    crate::lisp::primitives::file_io::report_file_errno(
                        interp,
                        "Removing directory",
                        directory,
                        &error,
                    )
                })?;
                interp.queue_directory_deletion_notification(&path);
                interp.invalidate_file_notify_watches_for_path(&path);
                Ok(Value::Nil)
            }

            "make-directory-internal" => {
                need_args(name, args, 1)?;
                let directory =
                    crate::lisp::primitives::fexpand_file_name(interp, env, args[0], Value::Nil)?;
                let path = string_text(&directory)?;
                // Fmake_directory_internal: emacs_mkdir (dir, 0777 &
                // ~auto_saving_dir_umask), report_file_error ("Creating
                // directory", directory).
                use std::os::unix::fs::DirBuilderExt;
                std::fs::DirBuilder::new()
                    .mode(0o777 & !crate::lisp::primitives::file_io::auto_saving_dir_umask())
                    .create(host_file_name(interp, env, &path)?)
                    .map_err(|error| {
                        crate::lisp::primitives::file_io::report_file_errno(
                            interp,
                            "Creating directory",
                            directory,
                            &error,
                        )
                    })?;
                dispatch_file_notification(interp, env, &path, "created")?;
                Ok(Value::Nil)
            }
            "add-name-to-file" => {
                need_arg_range(name, args, 2, 3)?;
                // fileio.c:Fadd_name_to_file, past the handler lookups that
                // expanded FILE and NEWNAME.
                let file = string_text(&args[0])?;
                let newname = string_text(&args[1])?;
                // The Lisp names report_file_error is given: FILE and the
                // expand_cp_target NEWNAME the preamble made.
                let newname_value = args[1];
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
                    crate::lisp::primitives::file_io::report_file_errno_files(
                        interp,
                        "Adding new name",
                        &[args[0], newname_value],
                        &error,
                    )
                })?;
                Ok(Value::Nil)
            }
            "make-temp-file-internal" => {
                need_args(name, args, 4)?;
                // CHECK_STRING (prefix); CHECK_STRING (suffix).
                for value in [args[0], args[2]] {
                    if !value.is_string() {
                        return Err(LispError::WrongTypeArgument("stringp".into(), value));
                    }
                }
                make_temp_file_internal(interp, env, args[0], &args[1], args[2], args.get(3))
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
                // Each aspect as the object it is: a non-symbol is unknown
                // to symbol_to_inotifymask.
                let flags = aspects;
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
                // kqueue.c: Fdirectory_file_name (Fexpand_file_name (FILE,
                // Qnil)).
                if !args[0].is_string() {
                    return Err(LispError::WrongTypeArgument("stringp".into(), args[0]));
                }
                let file =
                    crate::lisp::primitives::fexpand_file_name(interp, env, args[0], Value::Nil)?;
                let file = super::call(interp, "directory-file-name", &[file], env)?;
                let path = string_text(&file)?;
                // kqueue.c signals file-missing, naming FILE, before
                // registering anything; Emaxx accepted any path and returned
                // a live descriptor (finding 13's "never fails" half, per the
                // second audit).
                if let Err(error) = fs::metadata(host_file_name(interp, env, &path)?) {
                    return Err(report_file_errno(
                        interp,
                        "File does not exist",
                        file,
                        &error,
                    ));
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
                #[cfg(target_os = "macos")]
                {
                    let descriptor =
                        interp.register_kqueue_file_notify_watch(path, flags, args[2])?;
                    Ok(Value::Integer(descriptor))
                }
                #[cfg(not(target_os = "macos"))]
                {
                    let descriptor =
                        FILE_NOTIFY_DESCRIPTOR_COUNTER.fetch_add(1, AtomicOrdering::Relaxed) as i64;
                    interp.register_file_notify_watch(descriptor, Some(path), flags, args[2])?;
                    Ok(Value::Integer(descriptor))
                }
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
                let path = string_text(&args[0])?;
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
                // CHECK_FIXNUM (mode).
                let mode = args[0].as_fixnum()?;
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
                need_arg_range(name, args, 2, 3)?;
                // fileio.c:Fset_file_modes: CHECK_FIXNUM (mode), then
                // fchmodat with the low 12 bits, not following a symbolic
                // link when FLAG is non-nil (symlink_nofollow_flag).
                let Kind::Integer(mode) = args[1].kind() else {
                    return Err(LispError::WrongTypeArgument("fixnump".into(), args[1]));
                };
                let nofollow = args.get(2).is_some_and(Value::is_truthy);
                let path = string_text(&args[0])?;
                let host = host_file_name(interp, env, &path)?;
                fchmodat_errno(&host, (mode & 0o7777) as libc::mode_t, nofollow).map_err(
                    |error| {
                        crate::lisp::primitives::file_io::report_file_errno(
                            interp,
                            "Doing chmod",
                            args[0],
                            &error,
                        )
                    },
                )?;
                dispatch_file_notification(interp, env, &path, "attribute-changed")?;
                Ok(Value::Nil)
            }
            "file-name-all-completions" => {
                need_args(name, args, 2)?;
                file_name_completion(interp, env, args[0], args[1], true, Value::Nil)
            }
            "file-name-completion" => {
                need_arg_range(name, args, 2, 3)?;
                let predicate = args.get(2).copied().unwrap_or(Value::Nil);
                file_name_completion(interp, env, args[0], args[1], false, predicate)
            }
            "set-file-times" => {
                need_arg_range(name, args, 1, 3)?;
                let path = string_text(&args[0])?;
                let host = host_file_name(interp, env, &path)?;
                let modified = match args.get(1).map(|v| v.kind()) {
                    None | Some(Kind::Nil) => SystemTime::now(),
                    Some(value) => file_modtime_from_value(interp, &value.value())?.modified,
                };
                set_file_times_path(
                    interp,
                    args[0],
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
                let path = string_text(&args[0])?;
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
            // fileio.c:Ffile_symlink_p: CHECK_STRING, the expanded name
            // ENCODE_FILE'd, and emacs_readlinkat: the link's contents
            // DECODE_FILE'd, nil when readlinkat fails.
            "file-symlink-p" => {
                need_args(name, args, 1)?;
                let host = encoded_file_name(interp, env, &args[0])?;
                match fs::read_link(&host) {
                    Ok(target) => decode_host_file_name(interp, env, &target),
                    Err(_) => Ok(Value::Nil),
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
                let linkname = string_text(&args[1])?;
                // The Lisp names report_file_error is given: TARGET and the
                // expand_cp_target LINKNAME the preamble made.
                let linkname_value = args[1];
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
                    crate::lisp::primitives::file_io::report_file_errno_files(
                        interp,
                        "Making symbolic link",
                        &[args[0], linkname_value],
                        &error,
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
                        // Fexpand_file_name against get_current_directory
                        // (false), then CHECK_STRING.
                        let directory = process_current_directory(interp, env, false)?;
                        let infile = fexpand_file_name(interp, env, *infile, directory)?;
                        if !infile.is_string() {
                            return Err(LispError::WrongTypeArgument("stringp".into(), infile));
                        }
                        infile
                    }
                    None => Value::string(NULL_DEVICE),
                };
                // remove_slash_colon, ENCODE_FILE and emacs_open; an error
                // names INFILE without its "/:", the string
                // make_specified_string copies from its data.
                let infile_text = string_text(&infile)?;
                let infile = if infile_text.starts_with("/:") {
                    super::call(
                        interp,
                        "substring-no-properties",
                        &[infile, Value::Integer(2)],
                        env,
                    )?
                } else {
                    infile
                };
                let filefd =
                    fs::File::open(encoded_file_name(interp, env, &infile)?).map_err(|error| {
                        crate::lisp::primitives::file_io::report_file_errno(
                            interp,
                            "Opening process input file",
                            infile,
                            &error,
                        )
                    })?;
                call_process(interp, env, args, filefd, None)
            }
            "make-process" => make_process_value(interp, env, args),
            "make-pipe-process" => make_pipe_process_value(interp, env, args),
            "get-buffer-process" => {
                need_arg_range(name, args, 1, 1)?;
                // process.c:Fget_buffer_process: nil names no buffer, and
                // Fget_buffer takes a buffer object as it is, killed or
                // not, and a name to its live buffer.
                let buffer_id = match args[0].kind() {
                    Kind::Nil => None,
                    Kind::Buffer(buffer) => Some(buffer.id),
                    Kind::StringObject(_) => interp.find_buffer_named(&args[0]),
                    _ => return Err(wrong_type_argument("stringp", args[0])),
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
                let process = match args[0].kind() {
                    Kind::StringObject(_) => match interp.find_process_named(&args[0]) {
                        Some(id) => interp.record_value(id),
                        None => return Ok(Value::Nil),
                    },
                    _ => get_process(interp, args.first())?,
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
                Ok(interp.process_id_value(process_id))
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
                // sysdep.c:system_process_attributes: CHECK_NUMBER, then
                // CONS_TO_INTEGER into a pid_t.
                if !matches!(
                    args[0].kind(),
                    Kind::Integer(_) | Kind::BigInteger(_) | Kind::Float(_)
                ) {
                    return Err(LispError::WrongTypeArgument("numberp".into(), args[0]));
                }
                let pid = crate::lisp::primitives::system::cons_to_signed(
                    &args[0],
                    i64::from(i32::MIN),
                    i64::from(i32::MAX),
                )?;
                Ok(process_attributes_value(pid))
            }
            "list-system-processes" => {
                need_args(name, args, 0)?;
                list_system_processes_value(interp)
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
                load_coding_system(interp, &decoding, env)?;
                checked_coding_given(interp, &decoding)?;
                load_coding_system(interp, &encoding, env)?;
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
                let filter = if args[1].is_nil() {
                    Value::symbol("internal-default-process-filter")
                } else {
                    args[1]
                };
                // A connection's contact keeps its :filter.
                if interp.is_connection_process(process_id) {
                    interp.process_contact_plist_put(
                        process_id,
                        Value::symbol(":filter"),
                        filter,
                    )?;
                }
                Ok(filter)
            }
            "internal-default-process-sentinel" => {
                need_args(name, args, 2)?;
                let process_id = interp.resolve_process_id(&args[0])?;
                internal_default_process_sentinel(interp, process_id, args[1], env)?;
                Ok(Value::Nil)
            }
            "set-process-sentinel" => {
                need_args(name, args, 2)?;
                let process_id = interp.resolve_process_id(&args[0])?;
                let sentinel = (!args[1].is_nil()).then(|| args[1]);
                interp.set_process_sentinel(process_id, sentinel);
                let sentinel = if args[1].is_nil() {
                    Value::symbol("internal-default-process-sentinel")
                } else {
                    args[1]
                };
                // A connection's contact keeps its :sentinel.
                if interp.is_connection_process(process_id) {
                    interp.process_contact_plist_put(
                        process_id,
                        Value::symbol(":sentinel"),
                        sentinel,
                    )?;
                }
                Ok(sentinel)
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
                // pset_buffer and update_process_mark only for a buffer
                // that is not EQ to the process's.
                if !crate::lisp::types::eq(interp.process_buffer_value(process_id), args[1]) {
                    interp.set_process_buffer_only(process_id, buffer_id, args[1]);
                    interp.update_process_mark(process_id);
                }
                // A connection's contact keeps its :buffer.
                if interp.is_connection_process(process_id) {
                    interp.process_contact_plist_put(
                        process_id,
                        Value::symbol(":buffer"),
                        args[1],
                    )?;
                }
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
                // conv_sockaddr_to_lisp of datagram_address[channel].
                crate::lisp::primitives::processes::wait_for_socket_fds(
                    interp,
                    env,
                    process_id,
                    "process-datagram-address",
                )?;
                #[cfg(unix)]
                return Ok(interp
                    .process_datagram_address(process_id)?
                    .map(|address| {
                        crate::lisp::primitives::processes::conv_sockaddr_to_lisp(&address)
                    })
                    .unwrap_or(Value::Nil));
                #[cfg(not(unix))]
                {
                    let _ = process_id;
                    Ok(Value::Nil)
                }
            }
            "set-process-datagram-address" => {
                need_args(name, args, 2)?;
                let process_id = interp.resolve_process_id(&args[0])?;
                // wait_for_socket_fds; nil for no datagram process; then
                // get_lisp_to_sockaddr_size: nil for an address object of
                // no family, or of a length the datagram address has not.
                #[cfg(unix)]
                {
                    crate::lisp::primitives::processes::wait_for_socket_fds(
                        interp,
                        env,
                        process_id,
                        "set-process-datagram-address",
                    )?;
                    if interp.process_datagram_address(process_id)?.is_none() {
                        return Ok(Value::Nil);
                    }
                    let Some((family, len)) =
                        crate::lisp::primitives::processes::lisp_to_sockaddr_size(&args[1])
                    else {
                        return Ok(Value::Nil);
                    };
                    let address = crate::lisp::primitives::processes::conv_lisp_to_sockaddr(
                        family, &args[1], len,
                    );
                    return Ok(
                        if interp.set_process_datagram_address(process_id, address)? {
                            args[1]
                        } else {
                            Value::Nil
                        },
                    );
                }
                #[cfg(not(unix))]
                {
                    let _ = process_id;
                    Ok(Value::Nil)
                }
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
                // p->name itself.
                Ok(interp.process_name(process_id).unwrap_or(Value::Nil))
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
                // PROCESSP (name) answers it; anything else is CHECK_STRING'd.
                if matches!(args[0].kind(), Kind::Record(record) if record.kind == crate::lisp::eval::RecordKind::Process)
                {
                    return Ok(args[0]);
                }
                if !matches!(args[0].kind(), Kind::StringObject(_)) {
                    return Err(wrong_type_argument("stringp", args[0]));
                }
                Ok(interp
                    .find_process_named(&args[0])
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
                // A connection with no socket yet waits for it, unless
                // NO-BLOCK.
                if interp.is_network_process(process_id)
                    && interp.network_socket_fd(process_id).is_none()
                {
                    if args.get(2).is_some_and(|no_block| !no_block.is_nil()) {
                        return Ok(Value::Nil);
                    }
                    crate::lisp::primitives::processes::wait_for_socket_fds(
                        interp,
                        env,
                        process_id,
                        "process-contact",
                    )?;
                }
                // DATAGRAM_CONN_P with KEY t or :remote: plist_put of the
                // datagram address into the contact.
                #[cfg(unix)]
                if (matches!(args.get(1).map(|v| v.kind()), Some(Kind::T))
                    || matches!(args.get(1).map(|v| v.kind()), Some(Kind::Symbol(key)) if key.is_named(":remote")))
                    && let Some(address) = interp.process_datagram_address(process_id)?
                {
                    let address =
                        crate::lisp::primitives::processes::conv_sockaddr_to_lisp(&address);
                    interp.process_contact_plist_put(
                        process_id,
                        Value::symbol(":remote"),
                        address,
                    )?;
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
                    // "it is t for a pipe connection".
                    None | Some(Kind::Nil) if interp.is_pipe_process(process_id) => Ok(Value::T),
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
                // wait_for_socket_fds, then process.c:2984-2986: a closed
                // socket is "not running".
                crate::lisp::primitives::processes::wait_for_socket_fds(
                    interp,
                    env,
                    process_id,
                    "set-network-process-option",
                )?;
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
                if set_socket_option(interp, fd, &option, &args[1], &args[2])? {
                    // process.c:2990 records the accepted option on the
                    // process's contact plist, which `process-contact' reads.
                    interp.process_contact_plist_put(process_id, args[1], args[2])?;
                    return Ok(Value::T);
                }
                // process.c:2994-2997.
                match args.get(3) {
                    Some(value) if !value.is_nil() => Ok(Value::Nil),
                    _ => Err(LispError::Signal("Unknown or unsupported option".into())),
                }
            }
            "network-interface-list" => network_interface_list(interp, args),
            "network-interface-info" => network_interface_info(args),
            "network-lookup-address-info" => {
                need_arg_range(name, args, 1, 3)?;
                // CHECK_STRING, the family and the hint, then
                // network_lookup_address_info_1: a multibyte NAME holding
                // a non-ASCII character is refused with its SSDATA, and
                // getaddrinfo is given that data.
                let Kind::StringObject(state) = args[0].kind() else {
                    return Err(wrong_type_argument("stringp", args[0]));
                };
                // Fnetwork_lookup_address_info's ("Unsupported family").
                let family = AddressFamily::parse(interp, args.get(1), "Unsupported family")?;
                // NILP (hint), then EQ (hint, Qnumeric).
                let numeric = match args.get(2).copied().filter(|hint| !hint.is_nil()) {
                    None => false,
                    Some(hint) => match crate::lisp::primitives::lisp_symbol(interp, hint)
                        .map(|symbol| symbol.kind())
                    {
                        Some(Kind::Symbol(hint)) if hint.is_named("numeric") => true,
                        _ => return Err(LispError::Signal("Unsupported hints value".into())),
                    },
                };
                let (host, non_ascii) = {
                    let state = state.borrow();
                    (
                        state.bytes().to_vec(),
                        state.is_multibyte() && state.bytes().len() != state.len(),
                    )
                };
                if non_ascii {
                    return Err(crate::lisp::primitives::values::doprnt_error_sdata(
                        Some(interp),
                        "Non-ASCII hostname {} detected, please use `puny-encode-domain'",
                        &[args[0]],
                    ));
                }
                let addresses = match network_lookup_addresses(&host, family, numeric) {
                    Ok(addresses) => addresses,
                    Err(error) => {
                        // Fformat ("%s/%s %s", host, "0", the error), and
                        // message ("%s", SSDATA (msg)).
                        let msg = super::call(
                            interp,
                            "format",
                            &[
                                Value::string("%s/%s %s"),
                                args[0],
                                Value::string("0"),
                                Value::string(&error),
                            ],
                            env,
                        )?;
                        // "%s" of SSDATA: the bytes before the first NUL.
                        let bytes = crate::lisp::primitives::sdata_c_string(&msg);
                        super::display::vmessage(interp, bytes, env)?;
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
                process_send_signal(
                    interp,
                    env,
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
                process_send_signal(
                    interp,
                    env,
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
                process_send_signal(
                    interp,
                    env,
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
                    process_send_signal(
                        interp,
                        env,
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
                    let signal = process_signal_code(interp, &args[1])?;
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
                // DATAGRAM_CONN_P: a datagram socket takes no EOF.
                if interp.process_datagram_address(process_id)?.is_some() {
                    return Ok(process);
                }
                // "Make sure the process is really alive", then a coding
                // that requires flushing is flushed.
                interp.refresh_process_id(process_id)?;
                interp.update_process_status(process_id);
                interp.process_send_check(process_id)?;
                crate::lisp::primitives::processes::flush_process_encoder(interp, process_id, env)?;
                if interp.process_pty_in(process_id) {
                    // `if (p->pty_in) send_process (proc, "\004", 1, Qnil)':
                    // the pty's EOF character, its channel kept, for a
                    // child on a pty and a childless pty alike.
                    interp.process_send_check(process_id)?;
                    send_process(interp, env, process_id, b"\x04")?;
                    return Ok(process);
                }
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
                // send_process: wait_while_connecting and
                // wait_for_tls_negotiation.
                crate::lisp::primitives::processes::wait_while_connecting(interp, env, process_id)?;
                crate::lisp::primitives::processes::wait_for_tls_negotiation(
                    interp, env, process_id,
                )?;
                let encoded = crate::lisp::primitives::processes::encode_process_input(
                    interp,
                    process_id,
                    &args[1],
                    string.multibyte,
                    env,
                )?;
                send_process(interp, env, process_id, &encoded)?;
                Ok(Value::Nil)
            }
            "process-send-region" => {
                need_args(name, args, 3)?;
                // Fprocess_send_region: the region's text with the current
                // buffer as the object, encoded as its multibyteness says.
                let process = get_process(interp, args.first())?;
                let process_id = interp.resolve_process_id(&process)?;
                // validate_region, then wait_while_connecting, and
                // send_process's wait_for_tls_negotiation.
                validate_region(interp, &args[1], &args[2])?;
                crate::lisp::primitives::processes::wait_while_connecting(interp, env, process_id)?;
                crate::lisp::primitives::processes::wait_for_tls_negotiation(
                    interp, env, process_id,
                )?;
                let text = super::buffer_edit::direct_buffer_substring_no_properties(
                    interp,
                    &[args[1], args[2]],
                    env,
                )?;
                let multibyte = interp.buffer.borrow().is_multibyte();
                let encoded = crate::lisp::primitives::processes::encode_process_input(
                    interp, process_id, &text, multibyte, env,
                )?;
                send_process(interp, env, process_id, &encoded)?;
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
                        // unwind_decompress balances the before-change call,
                        // with update_compositions where the output went.
                        return Ok((
                            Some(
                                AfterChange::new(start, old_length, old_length)
                                    .composing(end, end, CHECK_HEAD),
                            ),
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
                    let change = AfterChange::new(start, old_length, output.len())
                        .composing(start, start, CHECK_HEAD);
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
                        file_operation_error_value_without_file(
                            interp,
                            "Opening null device",
                            &error,
                        )
                    })?;
                    (filefd, Value::Nil, None)
                } else {
                    let (mut filefd, tempfile_name, tempfile) = create_temp_file(interp, env)?;
                    let coding = interp
                        .lookup_var_key(cached_symbol!("coding-system-for-write"), env)
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
                        report_file_errno(interp, "Write error", tempfile_name, &error)
                    })?;
                    std::io::Seek::rewind(&mut filefd).map_err(|error| {
                        report_file_errno(interp, "Setting file position", tempfile_name, &error)
                    })?;
                    (filefd, tempfile_name, Some(tempfile))
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
                // Run hooks with the buffer to be killed as the current
                // buffer, in record_unwind_protect_excursion: the buffer
                // that was current, and its point, come back after.
                let excursion = interp.specpdl_index();
                interp.record_unwind_protect_excursion();
                if let Err(error) = interp.set_current_buffer_id(id) {
                    let _ = interp.unbind_to(excursion, env);
                    return Err(error);
                }
                // Some(answer) returns ANSWER (unbind_to (count, ...)),
                // None goes on to the teardown.
                let consultation = (|| -> Result<Option<Value>, LispError> {
                    // CALLN (Frun_hook_with_args_until_failure,
                    // Qkill_buffer_query_functions).
                    if !inhibit_hooks {
                        let failed = run_hook_with_args(
                            interp,
                            &cached_symbol!("kill-buffer-query-functions").symbol(),
                            env,
                            |interp, function, env| {
                                let answer = call_function_value(interp, &function, &[], env)?;
                                Ok(if answer.is_nil() {
                                    Value::T
                                } else {
                                    Value::Nil
                                })
                            },
                        )?;
                        if failed.is_truthy() {
                            return Ok(Some(Value::Nil));
                        }
                    }
                    let mut modified = interp
                        .get_buffer_by_id(id)
                        .map(|buffer| buffer.is_modified() && buffer.visits_file())
                        .unwrap_or(false);
                    if modified && interp.in_interactive_call() {
                        let answer = call_named_function(
                            interp,
                            "kill-buffer--possibly-save",
                            &[interp.buffer_value(id).expect("live buffer object")],
                            env,
                        )?;
                        if answer.is_nil() {
                            return Ok(Some(Value::Nil));
                        }
                        // The interactive helper can save the buffer.  GNU
                        // rechecks BUF_MODIFF after it returns before deciding
                        // whether a recent auto-save is disposable.
                        modified = interp
                            .get_buffer_by_id(id)
                            .map(|buffer| buffer.is_modified() && buffer.visits_file())
                            .unwrap_or(false);
                    }
                    // Delete the autosave file, if requested: the C
                    // variables, Frecent_auto_save_p, a string name that
                    // Ffile_exists_p finds and that is not the visited
                    // file's, then do_yes_or_no_p and the Lisp function.
                    let auto_save_name = interp
                        .buffer_local_value_key(id, cached_symbol!("buffer-auto-save-file-name"))
                        .unwrap_or(Value::Nil);
                    let wanted =
                        |interp: &Interpreter,
                         env: &Env,
                         name: crate::lisp::types::symbols::SymbolKey| {
                            interp
                                .lookup_var_key(name, env)
                                .is_some_and(|value| value.is_truthy())
                        };
                    if modified
                        && wanted(
                            interp,
                            env,
                            cached_symbol!("kill-buffer-delete-auto-save-files"),
                        )
                        && wanted(interp, env, cached_symbol!("delete-auto-save-files"))
                        && interp
                            .get_buffer_by_id(id)
                            .is_some_and(|buffer| buffer.recent_auto_save_p())
                        && matches!(auto_save_name.kind(), Kind::StringObject(_))
                        && super::call(interp, "file-exists-p", &[auto_save_name], env)?.is_truthy()
                        && {
                            let file_name = interp
                                .get_buffer_by_id(id)
                                .map(|buffer| buffer.file_name())
                                .unwrap_or(Value::Nil);
                            super::call(interp, "string-equal", &[auto_save_name, file_name], env)?
                                .is_nil()
                        }
                    {
                        let delete = call_named_function(
                            interp,
                            "yes-or-no-p",
                            &[Value::String("Delete auto-save file? ".into())],
                            env,
                        )?;
                        if delete.is_truthy() {
                            call_named_function(
                                interp,
                                "delete-auto-save-file-if-necessary",
                                &[],
                                env,
                            )?;
                        }
                    }
                    // If the hooks have killed the buffer, exit now.
                    if !interp.has_buffer_id(id) {
                        return Ok(Some(Value::T));
                    }
                    if !inhibit_hooks {
                        // The kill hooks run with the dying buffer current, as
                        // in GNU (auto-revert's rm-watch reads its locals).
                        run_hook(interp, &cached_symbol!("kill-buffer-hook").symbol(), env)?;
                    }
                    Ok(None)
                })();
                let unbind = interp.unbind_to(excursion, env);
                if let Some(answer) = consultation.and_then(|answer| unbind.map(|()| answer))? {
                    return Ok(answer);
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
                            let survivor =
                                interp.get_buffer_by_id(other).map(|buffer| buffer.name());
                            if let Some(survivor) = survivor {
                                // SDATA (BVAR (other, name)).
                                return Err(crate::lisp::primitives::values::doprnt_error_sdata(
                                    Some(interp),
                                    "Unable to kill buffer whose indirect buffer `{}' cannot be killed",
                                    &[survivor],
                                ));
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
                crate::lisp::primitives::processes::kill_buffer_processes(interp, env, Some(id))?;
                // Their sentinels may have killed it.
                if !interp.has_buffer_id(id) {
                    return Ok(Value::T);
                }
                interp.kill_buffer_id(id)?;
                if !inhibit_hooks {
                    run_hook(
                        interp,
                        &cached_symbol!("buffer-list-update-hook").symbol(),
                        env,
                    )?;
                }
                Ok(Value::T)
            }
            "bury-buffer-internal" => {
                need_args(name, args, 1)?;
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
                Ok(Value::Nil)
            }

            "make-marker" => Ok(interp.make_marker()),
            "copy-marker" => {
                // marker.c:Fcopy_marker takes 0 to 2 arguments; an omitted
                // MARKER is nil, a marker pointing nowhere.
                need_arg_range(name, args, 0, 2)?;
                let insertion_type = args.get(1).is_some_and(Value::is_truthy);
                interp
                    .copy_marker_value(&args.first().copied().unwrap_or(Value::Nil), insertion_type)
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
pub(crate) fn get_process(
    interp: &mut Interpreter,
    name: Option<&Value>,
) -> Result<Value, LispError> {
    let name = name.copied().unwrap_or(Value::Nil);
    let buffer = if matches!(name.kind(), Kind::StringObject(_)) {
        // Fget_process, then Fget_buffer.
        if let Some(id) = interp.find_process_named(&name) {
            return Ok(interp.record_value(id));
        }
        interp.find_buffer_named(&name).ok_or_else(|| {
            // error ("Process %s does not exist", SDATA (name)).
            crate::lisp::primitives::values::doprnt_error_sdata(
                None,
                "Process {} does not exist",
                &[name],
            )
        })?
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
        // error ("Buffer %s has no process", SDATA (BVAR (XBUFFER (obj),
        // name))).
        let buffer_name = interp
            .buffer_list
            .iter()
            .find(|(id, _)| *id == buffer)
            .map_or(Value::Nil, |(_, name)| *name);
        crate::lisp::primitives::values::doprnt_error_sdata(
            None,
            "Buffer {} has no process",
            &[buffer_name],
        )
    })
}

/// process.c:process_send_signal with NOMSG false: a `real' process with
/// its channel open; with CURRENT_GROUP non-nil and a pty input, the pty's
/// character for SIGNAL (SIGNALS_VIA_CHARACTERS) sent down it, else the
/// signal to the process group; SIGCONT's `run' then reaches the sentinels
/// through status_notify.
#[cfg(unix)]
fn process_send_signal(
    interp: &mut Interpreter,
    env: &mut Env,
    process_id: u64,
    signal: i32,
    current_group: &Value,
) -> Result<(), LispError> {
    interp.process_signal_check(process_id)?;
    let current_group = if interp.process_pty_in(process_id) {
        *current_group
    } else {
        Value::Nil
    };
    if !current_group.is_nil()
        && let Some(character) = interp.pty_signal_character(process_id, signal)
    {
        return send_process(interp, env, process_id, &[character]);
    }
    let Some(target) = interp.process_signal_target(process_id, &current_group)? else {
        return Ok(());
    };
    // SIGCONT: the status `run' and a new tick, then status_notify (NULL,
    // NULL), which reports every process whose status changed, before the
    // signal goes while the child is alive.
    if signal == libc::SIGCONT {
        interp.record_process_continue(process_id);
        crate::lisp::primitives::processes::status_notify(interp, env, None)?;
    }
    interp.send_process_signal(process_id, &target, signal);
    Ok(())
}

/// process.c:send_process's loop over BYTES once its checks pass: BYTES
/// put at the back of the write queue when it holds anything; then,
/// until the queue is empty, its front (else BYTES) written as much as
/// the channel takes, and while it would block the rest put back at the
/// queue's front and wait_reading_process_output (0, 20 ms) accepting
/// output and running timers (whose sends queue behind it, and which
/// may delete the process: the next write's channel check then
/// signals), then the queue read again.
pub(crate) fn send_process(
    interp: &mut Interpreter,
    env: &mut Env,
    process_id: u64,
    bytes: &[u8],
) -> Result<(), LispError> {
    if !interp.process_send_begin(process_id)? {
        return Ok(());
    }
    let mut given = Some(bytes.to_vec());
    if interp.write_queue_pending(process_id) {
        interp.write_queue_push(process_id, given.take().unwrap_or_default(), false);
    }
    // do ... while (!NILP (p->write_queue)): the queue's front, else what
    // was given when the queue was empty.
    loop {
        let batch = interp
            .write_queue_pop(process_id)
            .or_else(|| given.take())
            .unwrap_or_default();
        let mut offset = 0;
        while offset < batch.len() {
            let written = interp.process_write_some(process_id, &batch[offset..])?;
            // A write to a stream gives up the adaptive read delay
            // (adaptive_read_buffering 1).
            if interp.reset_read_output_delay_after_write(process_id) {
                crate::lisp::primitives::processes::process_output_delay_written();
            }
            // emacs_gnutls_write's messages, shown as it makes them.
            super::gnutls::flush_gnutls_log(interp, env)?;
            match written {
                Some(written) => offset += written,
                None => {
                    interp.write_queue_push(process_id, batch[offset..].to_vec(), true);
                    // send_process: wait_reading_process_output (0, 20 *
                    // 1000 * 1000, 0, 0, Qnil, NULL, 0).
                    crate::lisp::primitives::processes::wait_twenty_milliseconds(interp, env)?;
                    break;
                }
            }
        }
        if !interp.write_queue_pending(process_id) {
            break;
        }
    }
    interp.process_send_flush(process_id)
}

/// fns.c:plist_get over the arguments, which Flist (nargs, args) makes a
/// plist: the value after the first key EQ to KEY at an even position,
/// nil when there is none, an odd list's last key having no value.
fn plist_argument(args: &[Value], key: &str) -> Value {
    args.as_chunks::<2>()
        .0
        .iter()
        .find(|[k, _]| crate::lisp::types::eq_symbol_named(*k, key))
        .map_or(Value::Nil, |[_, value]| *value)
}

/// process.c:get_required_string_keyword_param: plist_member's KEY, which
/// must have a value after it ("Missing %s keyword parameter"), a string
/// ("%s value not a string").
fn required_string_argument(args: &[Value], key: &str) -> Result<Value, LispError> {
    let position = args
        .iter()
        .step_by(2)
        .position(|candidate| crate::lisp::types::eq_symbol_named(*candidate, key));
    match position.and_then(|index| args.get(2 * index + 1)) {
        None => Err(LispError::Signal(format!(
            "Missing {key} keyword parameter"
        ))),
        Some(value) if matches!(value.kind(), Kind::StringObject(_)) => Ok(*value),
        Some(_) => Err(LispError::Signal(format!("{key} value not a string"))),
    }
}

/// Fget_buffer_create (VALUE, Qnil) of a non-nil process buffer argument,
/// with the buffer's id while it is live.
fn process_buffer_argument(
    interp: &mut Interpreter,
    env: &mut Env,
    value: &Value,
) -> Result<(Option<u64>, Value), LispError> {
    crate::lisp::primitives::processes::process_buffer_target(interp, env, value)
}

/// setup_process_coding_systems' checks of DECODING and ENCODING, which
/// may signal, then pset_decode_coding_system and its sibling.
fn setup_process_coding_systems(
    interp: &mut Interpreter,
    env: &mut Env,
    process_id: u64,
    decoding: Value,
    encoding: Value,
) -> Result<(), LispError> {
    load_coding_system(interp, &decoding, env)?;
    checked_coding_given(interp, &decoding)?;
    load_coding_system(interp, &encoding, env)?;
    checked_coding_given(interp, &encoding)?;
    interp.set_process_coding_system(process_id, decoding, encoding)?;
    Ok(())
}

/// process.c:Fmake_pipe_process, in its order: nil for no arguments; the
/// required string `:name'; make_process, which remove_process undoes if
/// what follows signals; the two pipes ("Creating pipe"); `:buffer', else
/// the name, through Fget_buffer_create; the arguments as the process's
/// contact, a copy of `:plist' as its plist, `:sentinel', `:filter',
/// `:noquery', and `:stop' leaving it stopped; then its coding systems,
/// whose setup may signal.  It reads no `:command', `:stderr' or
/// `:file-handler'.
pub(crate) fn make_pipe_process_value(
    interp: &mut Interpreter,
    env: &mut Env,
    args: &[Value],
) -> Result<Value, LispError> {
    if args.is_empty() {
        return Ok(Value::Nil);
    }
    let contact = Value::list(args.iter().copied());
    let name = required_string_argument(args, ":name")?;
    let process = interp.create_process(false, name)?;
    let process_id = interp.resolve_process_id(&process)?;
    let made = (|| -> Result<(), LispError> {
        let channels = open_pipe_process_channels(interp)?;
        interp.install_pipe_process_channels(process_id, channels);
        // add_process_read_fd, then p->adaptive_read_buffering.
        let adaptive = interp
            .lookup_var_key(cached_symbol!("process-adaptive-read-buffering"), env)
            .unwrap_or(Value::Nil);
        interp.set_process_adaptive_read_buffering(process_id, adaptive);
        let buffer = match plist_argument(args, ":buffer") {
            buffer if buffer.is_nil() => name,
            buffer => buffer,
        };
        let (buffer_id, buffer) = process_buffer_argument(interp, env, &buffer)?;
        interp.set_process_buffer_id(process_id, buffer_id, buffer);
        interp.set_process_contact_plist(process_id, contact)?;
        let plist = super::call(
            interp,
            "copy-sequence",
            &[plist_argument(args, ":plist")],
            env,
        )?;
        interp.set_process_plist_value(process_id, plist);
        let present = |value: Value| (!value.is_nil()).then_some(value);
        interp.set_process_sentinel(process_id, present(plist_argument(args, ":sentinel")));
        interp.set_process_filter(process_id, present(plist_argument(args, ":filter")))?;
        if plist_argument(args, ":noquery").is_truthy() {
            interp.set_process_query_on_exit_flag(process_id, false)?;
        }
        if plist_argument(args, ":stop").is_truthy() {
            interp.set_process_traffic_stopped(process_id, true)?;
        }
        let (decoding, encoding) = connection_coding_systems(
            interp,
            env,
            ConnectionCoding::Pipe,
            plist_argument(args, ":coding"),
            buffer,
        )?;
        setup_process_coding_systems(interp, env, process_id, decoding, encoding)
    })();
    if let Err(error) = made {
        interp.remove_process(process_id);
        return Err(error);
    }
    Ok(process)
}

/// process.c:Fmake_process, in its order: nil for no arguments; a
/// `:file-handler' that the current buffer's directory has; `:buffer'
/// through Fget_buffer_create; get_current_directory (true) ("Setting
/// current directory"); the required string `:name'; `:command''s car
/// as the program, CHECK_STRING'd; `:stderr', a pipe process or a buffer
/// for which Fmake_pipe_process makes one; make_process, which
/// remove_process undoes if what follows signals; `:stop', which must be
/// nil; `:connection-type'; the coding systems; then, with a program,
/// its search ("Searching for program"), each argument's CHECK_STRING and
/// encoding, the coding systems' setup and the fork (create_process), and
/// without one create_pty: a `real' process with no child, pid -2, on a
/// pty unless the connection type is a pipe.
fn make_process_value(
    interp: &mut Interpreter,
    env: &mut Env,
    args: &[Value],
) -> Result<Value, LispError> {
    use crate::lisp::primitives::processes as procs;
    if args.is_empty() {
        return Ok(Value::Nil);
    }
    if plist_argument(args, ":file-handler").is_truthy() {
        let default_directory = interp
            .lookup_var_key(cached_symbol!("default-directory"), env)
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
    let (buffer_id, buffer) =
        process_buffer_argument(interp, env, &plist_argument(args, ":buffer"))?;
    let current_dir = procs::process_current_directory(interp, env, true)?;
    let name = required_string_argument(args, ":name")?;
    let command = plist_argument(args, ":command");
    let program = command
        .cons_values()
        .map_or(Value::Nil, |(program, _)| program);
    if !program.is_nil() && !matches!(program.kind(), Kind::StringObject(_)) {
        return Err(LispError::WrongTypeArgument("stringp".into(), program));
    }
    let query_on_exit = plist_argument(args, ":noquery").is_nil();
    let xstderr = plist_argument(args, ":stderr");
    let stderr_process_id = if let Ok(stderr_id) = interp.resolve_process_id(&xstderr) {
        if !interp.is_pipe_process(stderr_id) {
            return Err(LispError::Signal("Process is not a pipe process".into()));
        }
        Some(stderr_id)
    } else if xstderr.is_nil() {
        None
    } else {
        // CHECK_STRING (program), then Fmake_pipe_process with :name
        // NAME " stderr", :buffer Fget_buffer_create (xstderr) and
        // :noquery.
        if !matches!(program.kind(), Kind::StringObject(_)) {
            return Err(LispError::WrongTypeArgument("stringp".into(), program));
        }
        let stderr_name = super::call(interp, "concat", &[name, Value::string(" stderr")], env)?;
        let (_, stderr_buffer) = process_buffer_argument(interp, env, &xstderr)?;
        let stderr = make_pipe_process_value(
            interp,
            env,
            &[
                Value::symbol(":name"),
                stderr_name,
                Value::symbol(":buffer"),
                stderr_buffer,
                Value::symbol(":noquery"),
                if query_on_exit { Value::Nil } else { Value::T },
            ],
        )?;
        Some(interp.resolve_process_id(&stderr)?)
    };
    let process = interp.create_process(true, name)?;
    let process_id = interp.resolve_process_id(&process)?;
    let made = (|| -> Result<(), LispError> {
        // pset_buffer, and update_process_mark's marker at its end.
        interp.set_process_buffer_id(process_id, buffer_id, buffer);
        let present = |value: Value| (!value.is_nil()).then_some(value);
        interp.set_process_sentinel(process_id, present(plist_argument(args, ":sentinel")));
        interp.set_process_filter(process_id, present(plist_argument(args, ":filter")))?;
        let command_copy = super::call(interp, "copy-sequence", &[command], env)?;
        interp.set_process_command(process_id, command_copy);
        interp.set_process_query_on_exit_flag(process_id, query_on_exit)?;
        // CHECK_TYPE (NILP (tem), Qnull, tem).
        let stop = plist_argument(args, ":stop");
        if !stop.is_nil() {
            return Err(LispError::WrongTypeArgument("null".into(), stop));
        }
        let default_pty = interp
            .lookup_var_key(cached_symbol!("process-connection-type"), env)
            .is_some_and(|value| value.is_truthy());
        let connection_type = plist_argument(args, ":connection-type");
        let pty_modes = procs::process_connection_pty_modes(
            interp,
            (!connection_type.is_nil()).then_some(&connection_type),
            default_pty,
        )?;
        interp.set_process_stderr(process_id, stderr_process_id);
        let (decoding, encoding) = make_process_coding_systems(
            interp,
            env,
            plist_argument(args, ":coding"),
            name,
            buffer,
            command,
            !program.is_nil(),
        )?;
        interp.set_process_coding_system(process_id, decoding, encoding)?;
        // !(NILP (buffer) || !inherit_process_coding_system).
        let inherit_coding_system = !buffer.is_nil()
            && interp
                .lookup_var_key(cached_symbol!("inherit-process-coding-system"), env)
                .is_some_and(|value| value.is_truthy());
        interp.set_process_inherit_coding_system_flag(process_id, inherit_coding_system)?;
        // p->adaptive_read_buffering from `process-adaptive-read-buffering'.
        let adaptive = interp
            .lookup_var_key(cached_symbol!("process-adaptive-read-buffering"), env)
            .unwrap_or(Value::Nil);
        interp.set_process_adaptive_read_buffering(process_id, adaptive);
        if program.is_nil() {
            // create_pty: allocate_pty when the connection type asks for a
            // pty, which FD_SETSIZE refuses ("Opening pty"), then, where
            // `! defined (USG) || defined (USG_SUBTTY_WORKS)', its tty; no
            // pty when none is to be had.  Only a pty's coding systems are
            // set up.
            let pty = if pty_modes.0 || pty_modes.1 {
                procs::allocate_pty()
            } else {
                None
            };
            let Some((master, pty_name)) = pty else {
                return interp.make_childless_pty_process(process_id, None);
            };
            use std::os::fd::AsRawFd;
            if procs::beyond_fd_setsize(&[master.as_raw_fd()]) {
                return Err(procs::fd_setsize_error(interp, "Opening pty"));
            }
            let slave = if procs::PARENT_OPENS_PTY_TTY {
                Some(procs::open_pty_tty(interp, &pty_name)?)
            } else {
                None
            };
            // fcntl (pty_fd, F_SETFL, O_NONBLOCK).
            // SAFETY: the master is an open descriptor this owns.
            unsafe {
                let flags = libc::fcntl(master.as_raw_fd(), libc::F_GETFL);
                libc::fcntl(master.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK);
            }
            let pty_name = pty_name.to_string_lossy().into_owned();
            interp.make_childless_pty_process(process_id, Some((master, slave, Some(pty_name))))?;
            return setup_process_coding_systems(interp, env, process_id, decoding, encoding);
        }
        let executable = locate_program_for_exec(interp, env, program, true)?;
        let executable = host_file_name(interp, env, &executable)?.into_os_string();
        let arguments = command.to_vec()?;
        let arguments =
            make_process_arguments(interp, env, arguments.get(1..).unwrap_or(&[]), encoding)?;
        let mut setup_coding = |interp: &mut Interpreter, env: &mut Env| {
            setup_process_coding_systems(interp, env, process_id, decoding, encoding)
        };
        let runtime = spawn_persistent_process(
            interp,
            env,
            &executable,
            &arguments,
            current_dir,
            procs::ProcessChannels {
                pty_modes,
                stderr_process_id,
            },
            &mut setup_coding,
        )?;
        interp.attach_process_runtime(process_id, runtime);
        Ok(())
    })();
    if let Err(error) = made {
        interp.remove_process(process_id);
        return Err(error);
    }
    Ok(process)
}

/// process.c:Finternal_default_signal_process's SIGCODE: a fixnum in
/// int's range, else a symbol abbr_to_signal knows.
#[cfg(unix)]
fn process_signal_code(interp: &Interpreter, value: &Value) -> Result<i32, LispError> {
    if let Kind::Integer(number) = value.kind() {
        // check_integer_range (sigcode, INT_MIN, INT_MAX).
        return i32::try_from(number).map_err(|_| {
            LispError::SignalValue(Value::list([
                Value::symbol("args-out-of-range"),
                *value,
                Value::Integer(i64::from(i32::MIN)),
                Value::Integer(i64::from(i32::MAX)),
            ]))
        });
    }
    // CHECK_SYMBOL, then SYMBOL_NAME of XSYMBOL.
    let symbol = check_symbol(interp, *value)?;
    let symbol_name = symbol.as_symbol()?;
    abbr_to_signal(symbol_name)
        .ok_or_else(|| LispError::Signal(format!("Undefined signal name {symbol_name}")))
}

/// process.c:abbr_to_signal: NAME less a `SIG' or `sig' prefix, upcased
/// into a 20-byte buffer (a longer name is no signal), as gnulib's
/// str2signum reads it: decimal digits up to SIGNUM_BOUND, a name in its
/// table, or RTMIN+N / RTMAX-N.
#[cfg(unix)]
fn abbr_to_signal(name: &str) -> Option<i32> {
    let name = name
        .strip_prefix("SIG")
        .or_else(|| name.strip_prefix("sig"))
        .unwrap_or(name);
    if name.len() >= 20 {
        return None;
    }
    let signame = name.to_ascii_uppercase();
    if signame.starts_with(|ch: char| ch.is_ascii_digit()) {
        // strtol, all of it, at most SIGNUM_BOUND (NSIG - 1).
        let digits = signame.bytes().all(|byte| byte.is_ascii_digit());
        let number = signame.parse::<i64>().ok().filter(|_| digits)?;
        return (number <= i64::from(SIGNUM_BOUND)).then_some(number as i32);
    }
    if let Some((number, _)) = named_signal_candidates()
        .into_iter()
        .find(|(_, candidate)| *candidate == signame)
    {
        return Some(number);
    }
    realtime_signal_number(&signame)
}

/// gnulib sig2str.h's SIGNUM_BOUND: NSIG - 1.
#[cfg(any(target_os = "linux", target_os = "android"))]
const SIGNUM_BOUND: i32 = 64;
#[cfg(not(any(target_os = "linux", target_os = "android")))]
const SIGNUM_BOUND: i32 = 31;

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

/// SSDATA of an encoded file name for a C call: its bytes up to a NUL.
#[cfg(target_os = "linux")]
fn c_file_name(path: &std::path::Path) -> Option<std::ffi::CString> {
    use std::os::unix::ffi::OsStrExt;
    let bytes = path.as_os_str().as_bytes();
    let end = bytes
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(bytes.len());
    std::ffi::CString::new(&bytes[..end]).ok()
}

/// process.c:Finternal_default_signal_process's PROCESS: a string names
/// a process, else is read as a number, whose whole text it must be (nil
/// otherwise); any other non-number is get_process's.  A number is
/// CONS_TO_INTEGER's pid_t; a process with no pid of its own cannot be
/// signalled.
#[cfg(unix)]
fn signal_process_target_pid(
    interp: &mut Interpreter,
    target: &Value,
) -> Result<Option<libc::pid_t>, LispError> {
    let numberp = |value: &Value| {
        matches!(
            value.kind(),
            Kind::Integer(_) | Kind::BigInteger(_) | Kind::Float(_)
        )
    };
    let mut process = *target;
    if let Kind::StringObject(string) = target.kind() {
        let found = interp
            .find_process_named(target)
            .map(|id| interp.record_value(id));
        process = match found {
            Some(found) => found,
            None => {
                let bytes = string.borrow().bytes().to_vec();
                match crate::lisp::primitives::numeric_time::string_to_number_prefix(&bytes) {
                    (Some(number), len) if len == bytes.len() => number,
                    _ => return Ok(None),
                }
            }
        };
    } else if !numberp(&process) {
        process = get_process(interp, Some(&process))?;
    }
    if numberp(&process) {
        let pid = crate::lisp::primitives::system::cons_to_signed(
            &process,
            i64::from(libc::pid_t::MIN),
            i64::from(libc::pid_t::MAX),
        )?;
        return Ok(Some(pid as libc::pid_t));
    }
    let process_id = interp.resolve_process_id(&process)?;
    match interp.process_os_id(process_id) {
        Some(pid) if pid > 0 => Ok(Some(pid as libc::pid_t)),
        _ => {
            // error ("Cannot signal process %s", SDATA (XPROCESS
            // (process)->name)).
            let name = interp.process_name(process_id).unwrap_or(Value::Nil);
            Err(crate::lisp::primitives::values::doprnt_error_sdata(
                None,
                "Cannot signal process {}",
                &[name],
            ))
        }
    }
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
pub(crate) fn set_socket_option(
    interp: &Interpreter,
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
        return Err(report_file_errno_files(
            interp,
            "Cannot set network option",
            &[*option_symbol, *value],
            &error,
        ));
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

/// dired.c:Ffile_name_completion and Ffile_name_all_completions past their
/// handlers: DIRECTORY expanded, then file_name_completion with
/// `default-directory' bound to it.
fn file_name_completion(
    interp: &mut Interpreter,
    env: &mut Env,
    file: Value,
    directory: Value,
    all: bool,
    predicate: Value,
) -> Result<Value, LispError> {
    let dirname = super::call(interp, "expand-file-name", &[directory], env)?;
    if !file.is_string() {
        return Err(wrong_type_argument("stringp", file));
    }
    // FILE, the directory, PREDICATE, the best match (or the list of
    // matches) and the name in hand, held across the Lisp called here.
    let mut roots = crate::lisp::alloc::RootedVec::from_vec(vec![
        file,
        dirname,
        predicate,
        Value::Nil,
        Value::Nil,
    ]);
    let restore = interp.bind_special_dynamic("default-directory", dirname, env)?;
    let result = file_name_completion_1(interp, env, &mut roots, all);
    let restored = interp.restore_special_dynamic(restore, env);
    result.and_then(|value| restored.map(|()| value))
}

/// fns.c:Fcompare_strings of S1 [B1, E1) and S2 [B2, E2), an end of nil
/// meaning the string's end.
#[allow(clippy::too_many_arguments)]
fn compare_strings_value(
    interp: &mut Interpreter,
    env: &mut Env,
    s1: Value,
    b1: usize,
    e1: Option<usize>,
    s2: Value,
    b2: usize,
    e2: Option<usize>,
    ignore_case: bool,
) -> Result<Value, LispError> {
    let end = |e: Option<usize>| e.map_or(Value::Nil, |e| Value::Integer(e as i64));
    super::call(
        interp,
        "compare-strings",
        &[
            s1,
            Value::Integer(b1 as i64),
            end(e1),
            s2,
            Value::Integer(b2 as i64),
            end(e2),
            if ignore_case { Value::T } else { Value::Nil },
        ],
        env,
    )
}

fn file_name_completion_1(
    interp: &mut Interpreter,
    env: &mut Env,
    roots: &mut crate::lisp::alloc::RootedVec<Value>,
    all: bool,
) -> Result<Value, LispError> {
    const FILE: usize = 0;
    const DIRNAME: usize = 1;
    const PREDICATE: usize = 2;
    const BEST: usize = 3;
    const NAME: usize = 4;
    let schars = |value: &Value| string_like(value).map_or(0, |text| text.text.chars().count());
    let ignore_case = completion_ignores_case(interp, env);
    let encoded_file = encode_file_name(interp, env, &string_text(&roots[FILE])?)?;
    let dirname = string_text(&roots[DIRNAME])?;
    // A file-name coding that decomposes characters (HFS+) needs the
    // decoded names compared too, FILE recomposed first.
    let coding = interp
        .lookup_var_key(cached_symbol!("file-name-coding-system"), env)
        .filter(Value::is_truthy)
        .or_else(|| interp.lookup_var_key(cached_symbol!("default-file-name-coding-system"), env))
        .unwrap_or(Value::Nil);
    let check_decoded = !coding.is_nil() && {
        let plist = super::call(interp, "coding-system-plist", &[coding], env)?;
        super::plist_get(
            plist,
            crate::lisp::types::PropertyName::symbol(":decomposed-characters"),
        )
        .is_truthy()
    };
    if check_decoded && string_like(&roots[FILE]).is_some_and(|text| text.multibyte) {
        roots[FILE] = decode_file_name(interp, env, &encoded_file)?;
    }
    let host_directory = host_file_name(interp, env, &directory_file_name(&dirname))?;
    let entries = read_directory_entries(&host_directory).map_err(|(step, error)| {
        let rendered = error.to_string();
        let detail = rendered
            .split_once(" (os error")
            .map(|(detail, _)| detail)
            .unwrap_or(rendered.as_str());
        LispError::SignalValue(Value::list([
            Value::symbol(if error.kind() == ErrorKind::NotFound {
                "file-missing"
            } else {
                "file-error"
            }),
            Value::String(step.into()),
            Value::String(detail.into()),
            Value::String(dirname.clone().into()),
        ]))
    })?;
    // completion_ignore_case's regexps fold with Vascii_canon_table.
    let case_table = ignore_case.then(|| interp.standard_case_tables().canon);
    let file_len = schars(&roots[FILE]);
    let enc_file_len = encoded_file.len();
    let mut bestmatchsize = 0usize;
    let mut matchcount = 0;
    // Until a file that cannot be excluded is seen, "." and ".." and the
    // completion-ignored-extensions are kept.
    let mut includeall = true;
    'entries: for (entry, kind) in entries {
        use std::os::unix::ffi::OsStrExt;
        let bytes = entry.as_bytes();
        let len = bytes.len();
        if len < enc_file_len || (!ignore_case && !bytes.starts_with(&encoded_file)) {
            continue;
        }
        roots[NAME] = decode_file_name(interp, env, bytes)?;
        let name_text = string_text(&roots[NAME])?;
        let name_bytes = name_text.as_bytes();
        let name_len = schars(&roots[NAME]);
        if ignore_case
            && !compare_strings_value(
                interp,
                env,
                roots[NAME],
                0,
                Some(file_len),
                roots[FILE],
                0,
                Some(file_len),
                true,
            )?
            .eq_value(Value::T)
        {
            continue;
        }
        // dirent_type: a directory as such; a link or an unknown type by
        // file_name_completion_dirp's stat; anything else not one.
        let directoryp = match kind {
            libc::DT_DIR => true,
            libc::DT_LNK | libc::DT_UNKNOWN => {
                fs::metadata(host_directory.join(&entry)).is_ok_and(|meta| meta.is_dir())
            }
            _ => false,
        };
        if !all {
            let extensions = interp
                .lookup_var_key(cached_symbol!("completion-ignored-extensions"), env)
                .unwrap_or(Value::Nil);
            let mut canexclude = false;
            if directoryp && (bytes == b"." || bytes == b"..") {
                // "." and ".." are never interesting as completions.
                canexclude = true;
            } else if len > enc_file_len {
                let mut tail = extensions;
                while let Some((elt, rest)) = tail.cons_values() {
                    tail = rest;
                    let Some(elt_text) = string_like(&elt).map(|text| text.text) else {
                        continue;
                    };
                    let elt_bytes = elt_text.as_bytes();
                    let elt_chars = elt_text.chars().count();
                    // A directory is ignored by an extension ending in a
                    // slash, compared without it; a file by any other.
                    let (elt_len, elt_chars, skip) = if directoryp {
                        let Some(elt_len) = elt_bytes.len().checked_sub(1).filter(|&n| n > 0)
                        else {
                            continue;
                        };
                        if elt_bytes[elt_len] != b'/' {
                            continue;
                        }
                        let Some(skip) = name_bytes.len().checked_sub(elt_len) else {
                            continue;
                        };
                        (elt_len, elt_chars - 1, skip)
                    } else {
                        let Some(skip) = len.checked_sub(elt_bytes.len()) else {
                            continue;
                        };
                        (elt_bytes.len(), elt_chars, skip)
                    };
                    if !ignore_case
                        && name_bytes.get(skip..skip + elt_len) != Some(&elt_bytes[..elt_len])
                    {
                        continue;
                    }
                    if ignore_case {
                        let Some(skip) = name_len.checked_sub(elt_chars) else {
                            continue;
                        };
                        if !compare_strings_value(
                            interp,
                            env,
                            roots[NAME],
                            skip,
                            None,
                            elt,
                            0,
                            Some(elt_chars),
                            true,
                        )?
                        .eq_value(Value::T)
                        {
                            continue;
                        }
                    }
                    canexclude = true;
                    break;
                }
            }
            if !includeall && canexclude {
                continue;
            }
            if includeall && !canexclude {
                // One file that cannot be excluded excludes the others.
                includeall = false;
                roots[BEST] = Value::Nil;
                bestmatchsize = 0;
                matchcount = 0;
            }
        }
        let mut regexps = interp
            .lookup_var_key(cached_symbol!("completion-regexp-list"), env)
            .unwrap_or(Value::Nil);
        while let Some((regexp, rest)) = regexps.cons_values() {
            regexps = rest;
            if !completion_regex_matches(interp, env, &name_text, &regexp, case_table)? {
                continue 'entries;
            }
        }
        if directoryp {
            roots[NAME] = super::call(interp, "file-name-as-directory", &[roots[NAME]], env)?;
        }
        if !roots[PREDICATE].is_nil()
            && interp
                .call_function_value(roots[PREDICATE], None, &[roots[NAME]], env)?
                .is_nil()
        {
            continue;
        }
        // Encoded names that match while the decoded ones do not ("a" and
        // a decomposed "a-ring") are refused.
        if check_decoded
            && file_len <= schars(&roots[NAME])
            && !compare_strings_value(
                interp,
                env,
                roots[NAME],
                0,
                Some(file_len),
                roots[FILE],
                0,
                Some(file_len),
                ignore_case,
            )?
            .eq_value(Value::T)
        {
            continue;
        }
        if matchcount <= 1 {
            matchcount += 1;
        }
        if all {
            roots[BEST] = Value::cons(roots[NAME], roots[BEST]);
        } else if roots[BEST].is_nil() {
            roots[BEST] = roots[NAME];
            bestmatchsize = schars(&roots[NAME]);
        } else {
            let name_chars = schars(&roots[NAME]);
            let compare = bestmatchsize.min(name_chars);
            let cmp = compare_strings_value(
                interp,
                env,
                roots[BEST],
                0,
                Some(compare),
                roots[NAME],
                0,
                Some(compare),
                ignore_case,
            )?;
            let matchsize = match cmp.kind() {
                Kind::Integer(cmp) => cmp.unsigned_abs() as usize - 1,
                _ => compare,
            };
            if ignore_case {
                // An exact match but for case is the best match, so that
                // its case pattern is the answer's; else a match keeping
                // the input's case is preferred.
                let best_chars = schars(&roots[BEST]);
                let dir = usize::from(directoryp);
                let exact = matchsize == name_chars;
                if (exact && matchsize + dir < best_chars)
                    || (exact == (matchsize + dir == best_chars)
                        && compare_strings_value(
                            interp,
                            env,
                            roots[NAME],
                            0,
                            Some(file_len),
                            roots[FILE],
                            0,
                            None,
                            false,
                        )?
                        .eq_value(Value::T)
                        && !compare_strings_value(
                            interp,
                            env,
                            roots[BEST],
                            0,
                            Some(file_len),
                            roots[FILE],
                            0,
                            None,
                            false,
                        )?
                        .eq_value(Value::T))
                {
                    roots[BEST] = roots[NAME];
                }
            }
            bestmatchsize = matchsize;
            // Reduced to FILE itself with no other completion possible.
            if matchsize <= file_len
                && !includeall
                && (!ignore_case || matchsize == 0)
                && matchcount > 1
            {
                break;
            }
        }
    }
    if all || roots[BEST].is_nil() {
        return Ok(roots[BEST]);
    }
    // t when FILE is the sole match exactly, case counted.
    if matchcount == 1
        && super::call(interp, "equal", &[roots[BEST], roots[FILE]], env)?.is_truthy()
    {
        return Ok(Value::T);
    }
    super::call(
        interp,
        "substring",
        &[
            roots[BEST],
            Value::Integer(0),
            Value::Integer(bestmatchsize as i64),
        ],
        env,
    )
}

fn lisp_truth(value: bool) -> Value {
    if value { Value::T } else { Value::Nil }
}

/// ENCODE_FILE of the Lisp file name NAME, as a host path.
/// dired.c:directory_files_internal's CHECK_FIXNAT (COUNT), nil for no
/// limit.
fn directory_files_count(count: Option<&Value>) -> Result<Option<usize>, LispError> {
    match count.map(|value| value.kind()) {
        None | Some(Kind::Nil) => Ok(None),
        Some(Kind::Integer(count)) if count >= 0 => Ok(Some(count as usize)),
        Some(_) => Err(wrong_type_argument(
            "wholenump",
            count.copied().unwrap_or(Value::Nil),
        )),
    }
}

/// coding.c:DECODE_SYSTEM: BYTES (a unibyte string) decoded with
/// `locale-coding-system' without recording it as
/// `last-coding-system-used'; a copy of the unibyte string when that is
/// nil.
fn decode_system(
    interp: &mut Interpreter,
    env: &mut Env,
    bytes: &[u8],
) -> Result<Value, LispError> {
    let raw = super::super::coding::bytes_to_unibyte_value(bytes);
    let coding = interp
        .lookup_var_key(cached_symbol!("locale-coding-system"), env)
        .unwrap_or(Value::Nil);
    if coding.is_nil() {
        return Ok(raw);
    }
    let Some(coding_name) = coding.symbol_str().map(str::to_owned) else {
        return Err(LispError::WrongTypeArgument(
            "coding-system-p".into(),
            coding,
        ));
    };
    super::super::coding::decode_coding_text_recording(
        interp,
        &raw,
        Some(&coding_name),
        false,
        false,
        None,
        false,
        env,
    )
}

/// INT_TO_INTEGER of an unsigned stat field: a fixnum, or a bignum
/// past `most-positive-fixnum'.
fn unsigned_integer(value: u64) -> Value {
    normalize_bigint_value(BigInt::from(value))
}

/// dired.c:file_attributes: the attributes of the file HOST names, not
/// following a final symbolic link, FILENAME the name an error reports.
/// A name that does not exist, or whose directory is not one, has none
/// (nil), as has a symbolic link that stops being one before it is read;
/// any other failure is a `file-error' "Getting attributes" (or "Reading
/// symbolic link").  ID-FORMAT other than nil and `integer' gives the
/// owner and group as names (DECODE_SYSTEM'd), each a number when it has
/// none.
pub(crate) fn file_attributes(
    interp: &mut Interpreter,
    env: &mut Env,
    host: &std::path::Path,
    filename: Value,
    id_format: Value,
) -> Result<Value, LispError> {
    let metadata = match fs::symlink_metadata(host) {
        Ok(metadata) => metadata,
        Err(error) => return file_metadata_errno(interp, "Getting attributes", filename, &error),
    };
    let file_type = if metadata.file_type().is_symlink() {
        // fileio.c:check_emacs_readlinkat.
        match fs::read_link(host) {
            Ok(target) => decode_host_file_name(interp, env, &target)?,
            Err(error) if error.raw_os_error() == Some(libc::EINVAL) => Value::Nil,
            Err(error) => file_metadata_errno(interp, "Reading symbolic link", filename, &error)?,
        }
    } else {
        lisp_truth(metadata.is_dir())
    };
    if metadata.file_type().is_symlink() && file_type.is_nil() {
        return Ok(Value::Nil);
    }
    let names = !(id_format.is_nil() || id_format.is_key(cached_symbol!("integer")));
    let uid = metadata.uid();
    let gid = metadata.gid();
    let user = match names.then(|| user_name_from_uid(uid)).flatten() {
        Some(name) => decode_system(interp, env, name.as_bytes())?,
        None => unsigned_integer(u64::from(uid)),
    };
    let group = match names
        .then(|| group_name_from_gid(i64::from(gid)))
        .transpose()?
        .flatten()
    {
        Some(name) => decode_system(interp, env, name.as_bytes())?,
        None => unsigned_integer(u64::from(gid)),
    };
    Ok(Value::list([
        file_type,
        Value::Integer(metadata.nlink() as i64),
        user,
        group,
        unix_time_list_value(metadata.atime(), metadata.atime_nsec()),
        unix_time_list_value(metadata.mtime(), metadata.mtime_nsec()),
        unix_time_list_value(metadata.ctime(), metadata.ctime_nsec()),
        normalize_bigint_value(BigInt::from(metadata.size())),
        Value::String(file_mode_string_for_metadata(&metadata).into()),
        Value::T,
        unsigned_integer(metadata.ino()),
        unsigned_integer(metadata.dev()),
    ]))
}

fn encoded_file_name(
    interp: &Interpreter,
    env: &crate::lisp::types::Env,
    name: &Value,
) -> Result<std::path::PathBuf, LispError> {
    host_file_name(interp, env, &string_text(name)?)
}

/// fileio.c:file_access_p: faccessat (AT_FDCWD, FILE, AMODE, AT_EACCESS).
fn file_access_p(file: &std::path::Path, amode: libc::c_int) -> std::io::Result<()> {
    faccessat_errno(file, amode)
}

/// fileio.c:file_directory_p on GNU/Linux: an O_PATH | O_DIRECTORY open
/// of FILE (emacs_openat, which retries an interrupted open after
/// maybe_quit); only a kernel without O_PATH (EINVAL) falls back on
/// file_accessible_directory_p, and when that is refused (EACCES) on an
/// fstatat.
fn file_directory_p(
    interp: &mut Interpreter,
    env: &mut crate::lisp::types::Env,
    file: &std::path::Path,
) -> Result<bool, LispError> {
    use std::os::unix::ffi::OsStrExt;
    let Ok(name) = std::ffi::CString::new(file.as_os_str().as_bytes()) else {
        return Ok(false);
    };
    loop {
        // SAFETY: `name' is a valid NUL-terminated string for the call.
        let fd = unsafe {
            libc::openat(
                libc::AT_FDCWD,
                name.as_ptr(),
                libc::O_PATH | libc::O_CLOEXEC | libc::O_DIRECTORY,
            )
        };
        if fd >= 0 {
            // SAFETY: FD was opened just above and is closed once.
            unsafe { libc::close(fd) };
            return Ok(true);
        }
        match std::io::Error::last_os_error().raw_os_error() {
            Some(libc::EINTR) => interp.maybe_quit(env)?,
            Some(libc::EINVAL) => break,
            _ => return Ok(false),
        }
    }
    match accessible_directory(file.as_os_str().as_encoded_bytes()) {
        Ok(()) => return Ok(true),
        Err(error) if error.raw_os_error() != Some(libc::EACCES) => return Ok(false),
        Err(_) => {}
    }
    Ok(match fs::metadata(file) {
        Ok(metadata) => metadata.is_dir(),
        Err(error) => error.raw_os_error() == Some(libc::EOVERFLOW),
    })
}

/// faccessat (AT_FDCWD, HOST, MODE, AT_EACCESS), its errno on failure.
fn faccessat_errno(host: &std::path::Path, mode: libc::c_int) -> std::io::Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let name = std::ffi::CString::new(host.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::from_raw_os_error(libc::EINVAL))?;
    // SAFETY: `name' is a valid NUL-terminated string for the call.
    let status = unsafe { libc::faccessat(libc::AT_FDCWD, name.as_ptr(), mode, libc::AT_EACCESS) };
    if status == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

/// emacs_fchmodat (AT_FDCWD, HOST, MODE, NOFOLLOW), its errno on failure.
fn fchmodat_errno(
    host: &std::path::Path,
    mode: libc::mode_t,
    nofollow: bool,
) -> std::io::Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let name = std::ffi::CString::new(host.as_os_str().as_bytes())
        .map_err(|_| std::io::Error::from_raw_os_error(libc::EINVAL))?;
    let flags = if nofollow {
        libc::AT_SYMLINK_NOFOLLOW
    } else {
        0
    };
    // SAFETY: `name' is a valid NUL-terminated string for the call.
    let status = unsafe { libc::fchmodat(libc::AT_FDCWD, name.as_ptr(), mode, flags) };
    if status == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}
