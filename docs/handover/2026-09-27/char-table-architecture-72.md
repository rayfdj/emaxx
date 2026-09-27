# Char-table work after canonical-overlay checkpoint

Production checkpoint: 91000d02da3729a38d7ceb6c1a929ade1d347eba.
Its source71e runtime controls and whole candidate are sealed separately.
The main tree currently adds only one char-table differential test and fixture.
No production char-table edit has been made yet.

GNU revision: 636f166cfc86aa90d63f592fd99f3fdd9ef95ebd.
Configured layout: gnu-char-table-layout-72.json. PVEC_CHAR_TABLE=32,
PVEC_SUB_CHAR_TABLE=33; root is552bytes before extras: header8, default8,
parent16, purpose24, ascii32, contents40[64], extras552. Standard slots68.
Subtable: header8, depth i32 at8, min_char i32 at12, contents16; GC starts
at slot1. Radix bits[6,4,5,7], subtable slot counts16,32,128.

GNU owners:
- lisp.h:Lisp_Char_Table/Lisp_Sub_Char_Table/CHAR_TABLE_REF_ASCII/CHAR_TABLE_SET
- chartab.c:Fmake_char_table, make_sub_char_table, char_table_ascii,
  copy_char_table/copy_sub_char_table, char_table_ref, char_table_set,
  char_table_set_range, parent/range/extra-slot primitives, optimize and map
- alloc.c:mark_char_table skips packed depth/min_char for subtable nodes
- data.c/fns.c/pdumper.c/lread.c remaining reference paths must be inspected

Concrete current source differences requiring baseline and correction:
- CharTableState keeps an append-only entries log, an ASCII entry-index array,
  a derived BTreeMap of resolved ranges, global generations and lookup caches.
- char_table_get returns explicit nil without default/parent fallback, and
  chooses parent before a non-nil child default. GNU resolves nil through the
  current table's default before consulting the parent.
- make-char-table INIT fills actual contents and extras in GNU; changing only
  the default must not overwrite those initialized entries.
- char_table_range searches historical exact ranges instead of the value at
  the requested starting character.
- equal compares historical writes, not the current GNU table object graph.
- overwritten values remain in the old log and are still traced.
- set-char-table-parent currently lacks GNU cycle rejection.
- extra-slot getters clamp negative indices and return nil for missing slots;
  setters grow storage. GNU bounds are fixed at allocation and violations error.
- optimize-char-table is currently a no-op and ignores the supplied predicate;
  GNU's tree optimization calls the predicate and changes representation.

Baseline fixture: tests/fixtures/char-table-authoritative-values.el.
GNU receipts72 and72b retained an obsolete value conservatively while its
allocating evaluator frame remained active. Receipt72c puts allocation and
replacement in a separate Lisp call frame and then proves weak-key count0.
This preserves the eventual-reclamation requirement, rather than expecting1.
The same fixture checks defaults, inheritance, initial/extra values, range
lookup, equality independent of overwritten history, and uninterned purpose
identity. Source72a is unchanged production71 plus that one test and fixture.
Its runtime baseline is now a preserved failure (char-table-controls-72.json),
exit101, one test executed, no ignore or timeout, source/binary verified.
Rust: (parent parent nil nil nil nil parent nil (nil nil t) (replacement 1))
GNU:  (initial initial default parent parent replacement-default range t
       (slot-initial slot-initial t) (replacement 0))
The complete results and failure backtrace are retained in the raw log.

Migration constraints:
- Allocate inline GNU root/subtable slots in the existing vector allocator.
  One header and actual Cell<Value> slots, no duplicate payload or per-access map.
- Canonical CharTableRef/SubCharTableRef words must be used by interpreter,
  VM, native code, roots, image copier, dumper and reader.
- Eliminate the ID vector, native handle, mark-ID set and historical log/indexes.
- Port the actual radix lookup/set/range/copy/ASCII behavior; preserve purpose
  object identity rather than reconstructing a symbol from a string.
- Review compressed Unicode-property subtables and #^/#^^ reading/printing;
  do not silently flatten, substitute or skip existing tests.
- Existing regex/syntax/case caches rely on generation bumps through the Rust
  mutation door. Raw native field writes bypass it. Keeping those caches
  unmodified would make the new shared object stale. GNU uses direct table
  access in matching; inspect the actual owners and remove invalid assumptions.
- Account real root/subtable allocations and trace only current slots. Verify
  survival of the parent/default/extra values and reclamation after replacement.
- Frame remains the final NativeIdentity bridge after char-table conversion.
  ConsCell still80bytes: ConsWords16 + two ConsValueCell24 + mark/serial16.
  Its reads check agreement; writes touch epoch/watchers/native dirty queues.
  Removing those remains required, not a deferred performance claim.
