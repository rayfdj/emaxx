"""Read GNU's vector heap at explicit GC boundaries from GDB.

This observer targets the retained 64-bit little-endian GNU build. It reads
memory only: no inferior calls, stores, warm-up collections or Lisp changes.
Debugger runs are diagnostic evidence, never ordinary validation.
"""

import bisect
import json
import struct

import gdb


PREFIX = "GNU_CENSUS_TRACE "
LIMIT = 8
collections = 0
active = 0
snapshots = 0
errors = []
previous_live = {}
types = {}


def emit(event, **fields):
    print('\n' + PREFIX + json.dumps(dict(event=event, collection=active, **fields)), flush=True)


def integer(expression):
    return int(gdb.parse_and_eval(expression))


def memory(address, count):
    return bytes(gdb.selected_inferior().read_memory(address, count))


def describe(address, data):
    header = struct.unpack_from("<Q", data)[0]
    marked = bool(header & (1 << 63))
    size = header & ((1 << 63) - 1)
    pseudo = bool(size & (1 << 62))
    kind = (size >> 24) & 63 if pseudo else 0
    if pseudo and kind == 12:  # PVEC_BOOL_VECTOR, checked against debug enums.
        bits = struct.unpack_from("<q", data, 8)[0]
        if bits < 0:
            raise ValueError("negative bool-vector size")
        nbytes = 16 + ((bits + 63) // 64) * 8
    elif pseudo:
        nbytes = 8 * (1 + (size & 4095) + ((size >> 12) & 4095))
    else:
        nbytes = 8 * (1 + size)
    if nbytes < 8 or nbytes % 8:
        raise ValueError("invalid vector size")
    return dict(address=address, header=header, marked=marked, kind=types.get(kind, str(kind)),
                kind_number=kind, bytes=nbytes,
                words=list(struct.unpack("<" + "Q" * (min(len(data), nbytes, 136) // 8),
                                         data[:min(len(data), nbytes, 136)])))


def heap():
    records = {}
    visited = set()
    block = integer("vector_blocks")
    while block:
        if block in visited or len(visited) >= 65536:
            raise ValueError("invalid or excessive vector-block chain")
        visited.add(block)
        data = memory(block, 4096)
        offset = 0
        while offset <= 4088 - 16:
            row = describe(block + offset, data[offset:4088])
            if offset + row['bytes'] > 4088:
                raise ValueError("vector crosses its block")
            if row['kind_number'] != 1:  # PVEC_FREE.
                records[row['address']] = row
            offset += row['bytes']
        if offset != 4088:
            raise ValueError("incomplete vector-block inventory")
        block = struct.unpack_from("<Q", data, 4088)[0]
    large = integer("large_vectors")
    visited = set()
    while large:
        if large in visited or len(visited) >= 65536:
            raise ValueError("invalid or excessive large-vector chain")
        visited.add(large)
        row = describe(large + 8, memory(large + 8, 16))
        row = describe(large + 8, memory(large + 8, min(row['bytes'], 136)))
        records[row['address']] = row
        large = struct.unpack("<Q", memory(large, 8))[0]
    return records


def stack_candidates(records):
    start = integer("$sp")
    end = integer("current_thread->m_stack_bottom")
    if not 0 <= end - start <= 16 * 1024 * 1024:
        raise ValueError("unexpected stack bounds")
    addresses = sorted(records)
    raw = memory(start, end - start)
    candidates = []
    for offset in range(0, len(raw) - 7, 8):
        word = struct.unpack_from("<Q", raw, offset)[0]
        position = bisect.bisect_right(addresses, word) - 1
        if position >= 0:
            base = addresses[position]
            if base <= word < base + records[base]['bytes']:
                candidates.append(dict(stack_address=start + offset, word=word,
                                       object_address=base, offset=word - base))
    return dict(start=start, end=end, candidates=candidates)


def guarded(callback):
    try:
        callback()
    except BaseException as error:
        errors.append(repr(error))
        emit('observer error', error=repr(error))
        raise


class Sweep(gdb.Breakpoint):
    def __init__(self):
        super().__init__('sweep_vectors', internal=True)

    def stop(self):
        def observe():
            global previous_live, snapshots
            if not active:
                return
            records = heap()
            live = {address: row for address, row in records.items() if row['marked']}
            dropped = [row for address, row in previous_live.items() if address not in live]
            emit('marked vector inventory', objects=list(records.values()),
                 live_objects=len(live), live_slots=sum(row['bytes'] // 8 for row in live.values()),
                 dropped_previous_live=dropped, stack=stack_candidates(records))
            previous_live = live
            snapshots += 1
        guarded(observe)
        return False


class Returned(gdb.FinishBreakpoint):
    def __init__(self):
        super().__init__(gdb.newest_frame(), internal=True)

    def stop(self):
        global active
        guarded(lambda: emit('explicit GC returned', total_vector_slots=integer('gcstat.total_vector_slots')))
        active = 0
        return False


class Explicit(gdb.Breakpoint):
    def __init__(self):
        super().__init__('Fgarbage_collect', internal=True)

    def stop(self):
        def observe():
            global active, collections
            collections += 1
            if collections <= LIMIT:
                active = collections
                emit('explicit GC entered', backtrace=gdb.execute('bt 10', to_string=True))
                Returned()
        guarded(observe)
        return False


gdb.execute('set pagination off')
gdb.execute('set confirm off')
gdb.execute('set print thread-events off')
gdb.execute('set startup-with-shell off')
# Keep ordinary ASLR; the breakpoint itself still makes this a diagnostic run.
gdb.execute('set disable-randomization off')
gdb.execute('handle SIGPIPE nostop noprint pass')
gdb.execute('handle SIGALRM nostop noprint pass')
gdb.execute('starti')
for expression, expected in [('sizeof (Lisp_Object)', 8), ('sizeof (bits_word)', 8), ('header_size', 8),
                             ('bool_header_size', 16), ('roundup_size', 8),
                             ('VECTOR_BLOCK_BYTES', 4088), ('large_vector_offset', 8),
                             ('PVEC_FREE', 1), ('PVEC_BOOL_VECTOR', 12),
                             ('PSEUDOVECTOR_SIZE_BITS', 12), ('PSEUDOVECTOR_AREA_BITS', 24)]:
    if integer(expression) != expected:
        raise ValueError('unexpected GNU ABI: ' + expression)
types = {field.enumval: field.name for field in gdb.lookup_type('enum pvec_type').fields()}
Sweep()
Explicit()
gdb.execute('continue')
emit('observer completed', explicit_collections=collections, captured_sweeps=snapshots, errors=errors)
if errors or snapshots != LIMIT or collections < LIMIT:
    raise ValueError('incomplete GNU GC observation')
