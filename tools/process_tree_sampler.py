"""Sample a launched process and its descendants on Darwin or Linux.

This is diagnostic sampling, not complete accounting: short-lived children
can disappear between samples. The compiler profiler also retains /usr/bin/time
usage for every compiler invocation. No process environments are inspected.
"""
import ctypes
import json
import os
from pathlib import Path
import platform
import time

class TaskInfo(ctypes.Structure):
    _fields_ = [(name, ctypes.c_uint64) for name in (
        "virtual_size", "resident_size", "total_user", "total_system", "threads_user", "threads_system"
    )] + [(name, ctypes.c_int32) for name in (
        "policy", "faults", "pageins", "cow_faults", "messages_sent", "messages_received",
        "syscalls_mach", "syscalls_unix", "csw", "threadnum", "numrunning", "priority"
    )]

class BsdInfo(ctypes.Structure):
    _fields_ = [(name, ctypes.c_uint32) for name in (
        "flags", "status", "xstatus", "pid", "ppid", "uid", "gid", "ruid",
        "rgid", "svuid", "svgid", "reserved"
    )] + [("comm", ctypes.c_char * 16), ("name", ctypes.c_char * 32)] + [
        (name, ctypes.c_uint32) for name in (
            "nfiles", "pgid", "pjobc", "tdev", "tpgid"
        )
    ] + [("nice", ctypes.c_int32), ("start_sec", ctypes.c_uint64),
         ("start_usec", ctypes.c_uint64)]


class DarwinSampler:
    cpu_ticks_per_second = 1_000_000_000

    def __init__(self):
        self.argv_cache = {}
        self.system = ctypes.CDLL('/usr/lib/libSystem.B.dylib', use_errno=True)
        self.system.sysctl.argtypes = [ctypes.c_void_p, ctypes.c_uint, ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p, ctypes.c_size_t]
        self.system.sysctl.restype = ctypes.c_int
        self.library = ctypes.CDLL('/usr/lib/libproc.dylib', use_errno=True)
        self.library.proc_listchildpids.argtypes = [ctypes.c_int, ctypes.c_void_p, ctypes.c_int]
        self.library.proc_listchildpids.restype = ctypes.c_int
        self.library.proc_pidinfo.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_uint64, ctypes.c_void_p, ctypes.c_int]
        self.library.proc_pidinfo.restype = ctypes.c_int
        self.library.proc_pidpath.argtypes = [ctypes.c_int, ctypes.c_void_p, ctypes.c_uint32]
        self.library.proc_pidpath.restype = ctypes.c_int

    def arguments(self, pid, path, started):
        key = (pid, path, started)
        if key not in self.argv_cache:
            mib = (ctypes.c_int * 3)(1, 49, pid)  # CTL_KERN, KERN_PROCARGS2
            buffer = ctypes.create_string_buffer(65536)
            length = ctypes.c_size_t(len(buffer))
            args = []
            if self.system.sysctl(mib, 3, buffer, ctypes.byref(length), None, 0) == 0:
                data = buffer.raw[:length.value]
                argc = int.from_bytes(data[:4], 'little', signed=True)
                cursor = data.find(b'\0', 4) + 1  # Skip executable path.
                while cursor < len(data) and data[cursor] == 0:
                    cursor += 1
                for unused in range(max(0, argc)):
                    end = data.find(b'\0', cursor)
                    if end < 0:
                        break
                    args.append(data[cursor:end].decode(errors='replace'))
                    cursor = end + 1
                # Stop at argc: never read or retain the environment tail.
            self.argv_cache[key] = args
        return self.argv_cache[key]

    def info(self, pid):
        path = ctypes.create_string_buffer(4096)
        info = TaskInfo()
        bsd = BsdInfo()
        if self.library.proc_pidinfo(pid, 3, 0, ctypes.byref(bsd), ctypes.sizeof(bsd)) != ctypes.sizeof(bsd):
            return None
        started = (bsd.start_sec, bsd.start_usec)
        if self.library.proc_pidinfo(pid, 4, 0, ctypes.byref(info), ctypes.sizeof(info)) != ctypes.sizeof(info):
            return None
        self.library.proc_pidpath(pid, path, len(path))
        path = path.value.decode(errors='replace')
        arguments = self.arguments(pid, path, started)
        if (self.library.proc_pidinfo(pid, 3, 0, ctypes.byref(bsd), ctypes.sizeof(bsd)) != ctypes.sizeof(bsd)
                or (bsd.start_sec, bsd.start_usec) != started):
            return None
        return dict(pid=pid, parent=bsd.ppid, path=path, start_identity=started,
                    cpu_user_raw=info.total_user, cpu_system_raw=info.total_system,
                    resident_bytes=info.resident_size,
                    arguments=arguments)

    def children(self, pid):
        # libproc returns a PID count, not the number of bytes written:
        # https://github.com/apple-oss-distributions/xnu/blob/main/libsyscall/wrappers/libproc/libproc.c
        capacity = 256
        while capacity <= 65536:
            children = (ctypes.c_int * capacity)()
            count = self.library.proc_listchildpids(pid, children, ctypes.sizeof(children))
            if count < capacity:
                return [child for child in list(children)[:max(0, count)] if child > 0]
            capacity *= 2
        raise RuntimeError('Process tree exceeds the diagnostic buffer')


class LinuxSampler:
    def __init__(self):
        self.cpu_ticks_per_second = os.sysconf('SC_CLK_TCK')
        self.page_size = os.sysconf('SC_PAGESIZE')

    def info(self, pid):
        root = Path('/proc') / str(pid)
        try:
            # comm may contain spaces and ')'; numeric fields follow its last ')'.
            # https://man7.org/linux/man-pages/man5/proc_pid_stat.5.html
            fields = (root / 'stat').read_text().rsplit(')', 1)[1].split()
            started = int(fields[19])  # Field 22; fields[0] is field 3, state.
            command = (root / 'cmdline').read_bytes().rstrip(b'\0')
            arguments = [part.decode(errors='replace') for part in command.split(b'\0')] if command else []
            try:
                path = os.readlink(root / 'exe')
            except FileNotFoundError:
                path = ''  # A zombie can still supply its final stat counters.
            # Refuse to combine records if the PID was reused between reads.
            last = (root / 'stat').read_text().rsplit(')', 1)[1].split()
            if int(last[19]) != started:
                return None
            return dict(pid=pid, parent=int(fields[1]), path=path,
                        start_identity=started, arguments=arguments,
                        cpu_user_raw=int(fields[11]), cpu_system_raw=int(fields[12]),
                        resident_bytes=int(fields[21]) * self.page_size)
        except (FileNotFoundError, ProcessLookupError):
            return None

    def children(self, pid):
        # Children belong to individual tasks; visiting every thread also finds
        # subprocesses created from worker threads without scanning other users.
        # https://www.kernel.org/doc/html/latest/filesystems/proc.html
        children = set()
        for path in (Path('/proc') / str(pid) / 'task').glob('*/children'):
            try:
                children.update(int(value) for value in path.read_text().split())
            except (FileNotFoundError, ProcessLookupError):
                pass
        return sorted(children)


class ProcessTreeSampler:
    def __init__(self):
        if platform.system() == 'Darwin':
            self.backend = DarwinSampler()
        elif platform.system() == 'Linux':
            self.backend = LinuxSampler()
        else:
            raise RuntimeError('Process sampling supports Darwin and Linux')

    def sample(self, root_pid):
        rows = []
        pending = [(root_pid, None)]
        seen = set()
        while pending:
            pid, parent = pending.pop()
            if pid in seen:
                continue
            seen.add(pid)
            info = self.backend.info(pid)
            if info is None or (parent is not None and info['parent'] != parent):
                continue
            rows.append(info)
            pending.extend((child, pid) for child in self.backend.children(pid))
        return dict(wall_time=time.time(), cpu_ticks_per_second=self.backend.cpu_ticks_per_second,
                    processes=rows)

    def wait(self, process, destination, timeout=1200):
        deadline = time.monotonic() + timeout
        with destination.open('w') as output:
            while process.poll() is None:
                output.write(json.dumps(self.sample(process.pid)) + '\n')
                if time.monotonic() >= deadline:
                    raise TimeoutError('Diagnostic child exceeded its time limit')
                time.sleep(0.025)
        return process.wait()
