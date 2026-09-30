"""GDB-only hardware watchpoints for the retained source166 Linux executable.

The driver checks its exact SHA-256 before using these inspected ABI offsets.
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
        emit({'event': 'mark bit changed', 'key': hex(self.key), 'table': hex(self.table),
              'marked': bool(word(self.address) & self.mask),
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
        except gdb.MemoryError:
            return False
        emit({'event': 'weak insertion', 'key': hex(key), 'table': hex(table),
              'head': name.decode('utf-8')})
        print(gdb.execute('bt 12', to_string=True), flush=True)
        self.watches.append(MarkWatch(key, table))
        if len(self.watches) == 2:
            self.enabled = False
        return False


gdb.execute('set pagination off')
gdb.execute('set print thread-events off')
gdb.execute('set disable-randomization off')
gdb.execute('set language c')
WeakInsertion()
gdb.execute('run')
emit({'event': 'inferior finished'})
