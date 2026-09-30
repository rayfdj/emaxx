"""Read-only GDB observer for source204 ELF 6ea03c1a (driver verifies SHA).

Inspected entry ABI: cleanup_vector's header is rdi; NativeHeap collection
receives heap/stack-top/runtime-root-pointer/root-count in rdi/rsi/rdx/rcx.
The heap's stack-bottom field is at byte 0x48. No inferior calls or stores.
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
        cleanup.enabled = collection <= 4
        capture.enabled = collection <= 4
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
        removed.append(dict(header=hex(header), size=hex(size), tag=tag, slots=slots,
                            payload=[hex(v) for v in payload], direct_root_matches=matches))
        if len(removed) > 4096:
            raise ValueError("cleanup inventory exceeds bounded observer")
        return False


class Report(gdb.Breakpoint):
    def stop(self):
        emit("swept vectors", objects=removed)
        cleanup.enabled = False
        capture.enabled = False
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
cleanup.enabled = capture.enabled = report.enabled = False
gdb.execute("run")
emit("inferior finished")
