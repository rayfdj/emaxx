"""GDB-only hardware watchpoints for the inspected source166/source198 binaries.

The driver checks an exact SHA-256 before using these inspected ABI offsets.
Both binaries use the same three entry symbols and argument registers, 0x48
native stack-bottom offset, rbx root cursor, and cons metadata layout. These
are specific inspected binaries, not assumptions about arbitrary Rust builds.
No inferior function is called and no runtime source or heap value is changed.
"""

import json
import os
import struct
import gdb


def word(address):
    return struct.unpack('<Q', gdb.selected_inferior().read_memory(address, 8))[0]


def emit(record):
    print('EXACT_ROOT ' + json.dumps(record), flush=True)


watched_keys = []
collection_sources = []
collection_frames = []
native_values = (0, 0)
unreadable_weak_key_names = {}


def read_words(start, length):
    if length > 16 * 1024 * 1024:
        raise RuntimeError('unexpectedly large root range in the inspected binary')
    data = gdb.selected_inferior().read_memory(start, length * 8)
    return list(struct.unpack(f'<{length}Q', data))


class CollectionEntry(gdb.Breakpoint):
    def __init__(self):
        symbol = '_RNvMsd_NtNtNtCsa7TutTRCzEQ_5emaxx4lisp11native_comp7runtimeNtB5_10NativeHeap23collect_below_stack_top'
        super().__init__(symbol, internal=True)

    def stop(self):
        if not watched_keys:
            return False
        global collection_sources, collection_frames
        registers = {name: int(gdb.parse_and_eval('$' + name))
                     for name in ['rdi', 'rsi', 'rdx', 'rcx']}
        # Inspected function entry: heap, stack top, runtime roots, length.
        bottom = word(registers['rdi'] + 0x48)
        collection_sources = [('runtime roots', registers['rdx'],
                               read_words(registers['rdx'], registers['rcx']))]
        if bottom:
            low = (min(bottom, registers['rsi']) + 7) & ~7
            high = max(bottom, registers['rsi'])
            collection_sources.append(('native stack', low, read_words(low, (high - low) // 8)))
        collection_frames = []
        frame = gdb.newest_frame()
        while frame is not None:
            collection_frames.append({'function': frame.name(), 'pc': hex(frame.pc()),
                                      'sp': int(frame.read_register('rsp'))})
            frame = frame.older()
        emit({'event': 'collection sources', 'stack_top': hex(registers['rsi']),
              'native_stack_bottom': hex(bottom),
              'frames': collection_frames,
              'ranges': [{'kind': kind, 'start': hex(start), 'words': len(values)}
                         for kind, start, values in collection_sources]})
        return False


class ReachabilityEntry(gdb.Breakpoint):
    def __init__(self):
        symbol = '_RNvMsw_NtNtCsa7TutTRCzEQ_5emaxx4lisp4evalNtB5_11Interpreter22weak_hash_reachability'
        super().__init__(symbol, internal=True)

    def stop(self):
        if not watched_keys:
            return False
        global native_values
        # The struct return uses rdi; native_roots is rcx/r8.
        native_values = (int(gdb.parse_and_eval('$rcx')), int(gdb.parse_and_eval('$r8')))
        emit({'event': 'native values', 'start': hex(native_values[0]),
              'words': native_values[1]})
        return False


def retaining_native_root():
    frame = gdb.newest_frame().older()
    if frame is None or 'weak_hash_reachability' not in (frame.name() or ''):
        return None
    address = int(frame.read_register('rbx'))
    start, length = native_values
    if not start <= address < start + length * 8:
        return None
    root = word(address)
    # Match tagged and untagged pointers, including either word of a cons.
    mask = ~15 if root & 7 == 3 else ~7
    sources = [{'kind': kind, 'address': hex(base + index * 8), 'raw': hex(raw)}
               for kind, base, values in collection_sources
               for index, raw in enumerate(values) if raw & mask == root & mask]
    for source in sources:
        if source['kind'] != 'native stack':
            continue
        location = int(source['address'], 16)
        source['frames'] = [dict(frame, offset=location - frame['sp'])
                            for frame, caller in zip(collection_frames, collection_frames[1:])
                            if frame['sp'] <= location < caller['sp']]
        source['surrounding_words'] = [hex(value) for value in read_words(location - 64, 17)]
    record = {'root': hex(root), 'root_index': (address - start) // 8,
              'source_matches': sources}
    if root & 7 in [3, 5]:
        try:
            record['payload_words'] = [hex(value) for value in read_words(root & ~7, 16)]
        except gdb.MemoryError as error:
            record['payload_read_error'] = str(error)
    return record


class MarkWatch(gdb.Breakpoint):
    def __init__(self, key, table):
        self.key = key
        self.table = table
        self.base = (key & ~7) & ~0x7fff
        index = ((key & ~7) - self.base) // 16
        self.address = self.base + 0x5518 + (index // 64) * 8
        self.mask = 1 << (index % 64)
        expression = f'(*(unsigned long long *){self.address:#x} & {self.mask:#x})'
        super().__init__(expression, gdb.BP_WATCHPOINT, wp_class=gdb.WP_WRITE, internal=True)
        emit({'event': 'watch installed', 'key': hex(key), 'table': hex(table),
              'bitmap_word': hex(self.address), 'mask': hex(self.mask)})

    def stop(self):
        marked = bool(word(self.address) & self.mask)
        emit({'event': 'mark bit changed', 'key': hex(self.key), 'table': hex(self.table),
              'marked': marked,
              'retaining_native_root': retaining_native_root() if marked else None,
              'epoch': struct.unpack('<I', gdb.selected_inferior().read_memory(self.base + 0x5460, 4))[0]})
        for command in ['bt 24', 'info registers', 'x/48gx $rsp']:
            print(gdb.execute(command, to_string=True), flush=True)
        return False


class WeakInsertion(gdb.Breakpoint):
    def __init__(self):
        # Exact function from the retained binary's ELF symbol table.
        symbol = '_RNvMs_NtNtNtNtCsa7TutTRCzEQ_5emaxx4lisp5alloc7vectors11hash_tablesNtB4_12HashTableRef6insert'
        super().__init__(symbol, internal=True)
        self.condition = '($rdx & 7) == 3 && (*(unsigned char *)(*(unsigned long long *)$rdi + 61) & 7) == 1'
        self.watches = []

    def stop(self):
        key = int(gdb.parse_and_eval('$rdx'))
        table = word(int(gdb.parse_and_eval('$rdi')))
        try:
            head = word(key & ~7)
            if head & 7 or not head:
                return False
            text = word(head)
            # repr(C) StringCell: storage word, Rust String(cap, ptr, len).
            length = word(text + 24)
            if length > 128:
                return False
            name = bytes(gdb.selected_inferior().read_memory(word(text + 16), length))
            if name.decode('utf-8', errors='replace') != os.environ['EMAXX_WATCH_KEY_HEAD']:
                return False
        except (gdb.MemoryError, OverflowError) as error:
            # A cons key's car need not be a readable allocated symbol.
            # Preserve these rejected candidates instead of letting GDB lose
            # the callback with an unreported Python exception.
            kind = type(error).__name__
            unreadable_weak_key_names[kind] = unreadable_weak_key_names.get(kind, 0) + 1
            if unreadable_weak_key_names[kind] == 1:
                emit({'event': 'unreadable weak-key name', 'key': hex(key),
                      'table': hex(table), 'kind': kind, 'error': str(error)})
            return False
        emit({'event': 'weak insertion', 'key': hex(key), 'table': hex(table),
              'head': name.decode('utf-8')})
        watched_keys.append(key)
        print(gdb.execute('bt 12', to_string=True), flush=True)
        self.watches.append(MarkWatch(key, table))
        if len(self.watches) == 2:
            self.enabled = False
        return False


gdb.execute('set pagination off')
gdb.execute('set print thread-events off')
gdb.execute('set disable-randomization off')
gdb.execute('set language c')
gdb.execute('set python print-stack full')
WeakInsertion()
CollectionEntry()
ReachabilityEntry()
gdb.execute('run')
emit({'event': 'inferior finished',
      'unreadable_weak_key_names': unreadable_weak_key_names})
