"""Read-only GDB observer for source204 ELF 6ea03c1a (driver verifies SHA).

Inspected entry ABI: cleanup_vector's header is rdi; NativeHeap collection
receives heap/stack-top/runtime-root-pointer/root-count in rdi/rsi/rdx/rcx.
The heap's stack-bottom field is at byte 0x48. At mark_stack+0x8e, rax/rbx
are the running thread's conservative scan bounds. No inferior calls or stores.
"""

import json
import struct
import gdb


active = None
ready = False
collection = 0
removed = []
sources = []


def emit(event, **fields):
    print("CENSUS_TRACE " + json.dumps(dict(event=event, test=active, collection=collection, **fields)), flush=True)


def words(start, length):
    if not 0 <= length <= 1024 * 1024:
        raise ValueError("root range exceeds bounded observer")
    return list(struct.unpack(f"<{length}Q", gdb.selected_inferior().read_memory(start, length * 8)))


def reg(name):
    return int(gdb.parse_and_eval("$" + name))


class EndTest(gdb.FinishBreakpoint):
    def stop(self):
        global active, ready
        emit("test returned")
        active = None
        ready = False
        return False

    def out_of_scope(self):
        global active, ready
        emit("test unwound")
        active = None
        ready = False


class TestEntry(gdb.Breakpoint):
    def __init__(self, length, name):
        self.test = name
        symbol = f"_RNvYNCNvNtNtNtCsa7TutTRCzEQ_5emaxx4lisp10primitives5tests{length}{name}0INtNtNtCs4NRVxsYgnAr_4core3ops8function6FnOnceuE9call_onceBc_"
        super().__init__("*" + symbol, internal=True)

    def stop(self):
        global active, ready, collection
        active, ready, collection = self.test, False, 0
        emit("test entered")
        EndTest(internal=True)
        return False


class Initialized(gdb.FinishBreakpoint):
    def stop(self):
        global ready
        ready = True
        emit("interpreter initialized")
        return False


class Initialize(gdb.Breakpoint):
    def stop(self):
        if active:
            Initialized(internal=True)
        return False


class Collection(gdb.Breakpoint):
    def stop(self):
        if not ready:
            return False
        global collection, removed
        collection += 1
        removed = []
        emit("explicit collection")
        # The first sweep includes unrelated startup garbage, not the objects
        # behind the measured first-to-second census delta. Retain its roots
        # and observe cleanup in collections 2..4 without an unbounded log.
        cleanup.enabled = 2 <= collection <= 4
        capture.enabled = collection <= 4
        host_stack.enabled = collection <= 4
        report.enabled = collection <= 4
        return False


class Capture(gdb.Breakpoint):
    def stop(self):
        global sources
        bottom = words(reg("rdi") + 0x48, 1)[0]
        top, start, count = reg("rsi"), reg("rdx"), reg("rcx")
        sources = [("runtime roots", start, words(start, count))]
        if bottom:
            low, high = (min(top, bottom) + 7) & ~7, max(top, bottom)
            sources.append(("native stack", low, words(low, (high - low) // 8)))
        frames = []
        frame = gdb.newest_frame()
        while frame:
            frames.append(dict(function=frame.name(), pc=hex(frame.pc()), sp=hex(int(frame.read_register("rsp")))))
            frame = frame.older()
        emit("roots", ranges=[dict(kind=k, start=hex(s), words=[hex(v) for v in vs]) for k, s, vs in sources], frames=frames)
        return False


class Cleanup(gdb.Breakpoint):
    def stop(self):
        header = reg("rdi")
        size = words(header, 1)[0]
        pseudo = bool(size & (1 << 62))
        tag = (size >> 24) & 63 if pseudo else 0
        if tag in [1, 40, 41]:
            return False
        slots = ((size & 4095) + ((size >> 12) & 4095)) if pseudo else size
        payload = words(header + 8, min(slots, 64))
        matches = [dict(kind=kind, address=hex(base + i * 8), word=hex(value))
                   for kind, base, values in sources for i, value in enumerate(values)
                   if header <= (value & ~7) < header + (slots + 1) * 8]
        record = dict(header=hex(header), size=hex(size), tag=tag, slots=slots,
                      payload=[hex(v) for v in payload], direct_root_matches=matches)
        # Inspected RecordState payload starts with Rust Vec(capacity, ptr,
        # length), then its id, type tag and owner/kind. Inline Lisp records
        # have traced header slots; host records have only rest words.
        if tag == 34 and size & 4095 == 0:
            capacity, pointer, length = payload[:3]
            if length > capacity or length > 4096 or pointer & 7:
                raise ValueError("unexpected host record slot layout")
            record["record_fields"] = [hex(v) for v in words(pointer, length)]
        removed.append(record)
        if len(removed) > 4096:
            raise ValueError("cleanup inventory exceeds bounded observer")
        return False


class Report(gdb.Breakpoint):
    def stop(self):
        emit("swept vectors", cleanup_observed=collection >= 2, objects=removed)
        cleanup.enabled = False
        capture.enabled = False
        host_stack.enabled = False
        return False


class HostStack(gdb.Breakpoint):
    def stop(self):
        start, end = reg("rax") & ~7, reg("rbx")
        values = words(start, (end - start) // 8)
        sources.append(("host conservative stack", start, values))
        frames = []
        frame = gdb.newest_frame()
        while frame:
            frames.append(dict(function=frame.name(), pc=hex(frame.pc()), sp=hex(int(frame.read_register("rsp")))))
            frame = frame.older()
        emit("host stack", start=hex(start), end=hex(end), words=[hex(v) for v in values], frames=frames)
        return False


gdb.execute("set pagination off")
gdb.execute("set print thread-events off")
gdb.execute("set disable-randomization off")
gdb.execute("set language c")
gdb.execute("set python print-stack full")
TestEntry(50, "native_pseudovector_census_matches_gnu_word_layout")
TestEntry(56, "native_vector_and_closure_census_matches_gnu_word_layout")
Initialize("*_RNvNtCsa7TutTRCzEQ_5emaxx12test_support38initialized_upstream_batch_interpreter", internal=True)
Collection("*_RNvNtNtNtNtCsa7TutTRCzEQ_5emaxx4lisp10primitives8dispatch12misc_keymaps22direct_garbage_collect", internal=True)
cleanup = Cleanup("*_RNvNtNtNtCsa7TutTRCzEQ_5emaxx4lisp5alloc7vectors14cleanup_vector", internal=True)
capture = Capture("*_RNvMsd_NtNtNtCsa7TutTRCzEQ_5emaxx4lisp11native_comp7runtimeNtB5_10NativeHeap23collect_below_stack_top", internal=True)
report = Report("*_RNvNtNtNtNtCsa7TutTRCzEQ_5emaxx4lisp10primitives8dispatch12misc_keymaps22garbage_collect_report", internal=True)
host_stack = HostStack("*_RINvNtNtCsa7TutTRCzEQ_5emaxx4lisp5alloc10mark_stackQNCNvMsw_NtB4_4evalNtBW_11Interpreter22weak_hash_reachabilitys_0EB6_+0x8e", internal=True)
cleanup.enabled = capture.enabled = report.enabled = host_stack.enabled = False
gdb.execute("run")
emit("inferior finished")
