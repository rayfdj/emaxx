# Round 364 (D118), to be re-implemented on 654b4b0

The first implementation was lost with its container (uncommitted).  This is
what it was: the design that passed every comparison with GNU, what was
learned on the way, and the tests.  Base: `runtime-performance` at 654b4b0.
GNU's source is `/home/user/emacs/src/print.c` (30.2); the oracle binary is
`/home/user/emacs/src/emacs`.

## Goal

print.c writes while it walks: `printchar` hands one character to the
output and `strout` a block.  PRINTCHARFUN after print_prepare is: nil =
print_buffer (a buffer or marker; print_finish inserts it); t = stdout in a
batch session (`fwrite` per printchar, one `fwrite` per strout) or the echo
area + message log interactively; anything else = a function called with
each character.  Emaxx at 654b4b0 builds the whole text and hands it over at
the end.  Everything Lisp can observe (function sinks, print-unreadable-
function, errors mid-print, stdout write boundaries, terpri's ENSURE) must be
print.c's.

## Structure of the finished implementation

### src/lisp/primitives/print/output.rs
`#[derive(Clone, Default)] struct PrintOutput { bytes: Vec<u8>, multibyte:
bool, function_codes: Vec<(usize, u32)>, strouts: Vec<Strout>,
strout_called: bool, plain: bool }`; `struct Strout { start, end, octets:
bool }`; `pub(crate) enum Piece<'a> { Chars(&[u8]), Strout(&[u8]),
Octets(&[u8]) }`.
- bytes are Emacs-internal text; an octet >= 0x80 of a C string is stored
  as its raw-byte character (2 bytes).  `is_multibyte()` = a multibyte
  character joined.
- `push_code(u32)`, `push(char)`, `push_chars` = printchars; `push_str` /
  `append_strout(&PrintOutput)` = one strout; `push_octets(&[u8])` (alias
  `push_c_string(&str)`) = one strout of octets; `push_c_function_byte(u8)`
  = a C string byte for a function sink: the function's argument is `byte
  as c_char as i32 as u32` (recorded in function_codes when it differs).
- `mark_strout(start, octets)` sets strout_called and returns early when
  `plain` (a plain output keeps no piece bookkeeping: used for print_buffer
  and function sinks, for speed).  `PrintOutput::plain()`.
- `pieces()` iterator (Chars between strouts), `last_printchar()` (the last
  character of the last Chars piece, None if the output ends in a strout
  ... the last character *printchar* wrote), `written()` (strout_called ||
  !empty), `strout_called()`, `has_octets()` (a non-ASCII octets strout),
  `has_function_codes()`, `codes()` (function arguments), `text_parts()`,
  `host_text()`, `octets()`, `into_value`, `len()`, `clear()`.
- `overwritten_by(saved)` = print_unwind: copy SAVED's first SCHARS bytes
  (`restored_len()` = min(char count, byte len)) over the start, keep own
  length; non-ASCII result re-canonicalized by `canonical_text` (what
  decodes stays, any other byte becomes its raw-byte character).
  `from_memory(&[u8], multibyte)`.  See code/output_tail.rs.

### src/lisp/primitives/print.rs
- `pub(crate) enum PrintSink { Buffer, Stdout, Echo, Function(Value) }`,
  `PrintSink::of(interp, stream, env)`: nil -> standard-output; buffer or
  marker -> Buffer; t -> Stdout if the Lisp variable `noninteractive' is
  non-nil else Echo; else Function.  (GNU reads the C flag: recorded
  finding P2-3, not this round.)
- `deliver(interp, env, sink, &PrintOutput)`: Buffer: append to
  `interp.print_globals.buffer`; Stdout/Echo/Function: code/print_rs.rs.
- `printchar(interp, env, sink, ch)`, `printchar_unsigned` (Fwrite_char),
  `print_string`, `print_string_to_function`, `with_print_buffer`,
  `print_finish_text`, `standard_display_table_p`, `raw_octets`,
  `deliver_through_display_table`: code/print_rs.rs.
- `StringEscapes { multibyte, newlines, control, nonascii }`;
  `render_prin1_string(escapes, state, &mut out)`; `prin1_string_char(
  escapes, state, &mut offset, &mut need_nonhex, out)` = one turn of
  print_object's string loop (fetch_string_char_advance; at the string's end
  its terminating NUL: `state.bytes().get(*offset).copied().unwrap_or(0)`).
- `PrintOptions { escape, circle, continuous_numbering, gensym,
  integers_as_characters, symbols_bare, length: Option<usize>, level:
  Option<i64>, quoted, output_is_function }`; `print_options(interp)` reads
  the DEFVARs as C variables (`symbol_value_key`, never lexical).
  `print-level' is any fixnum (negative -> every cons "...").
- `print_pruned_string(...) -> (Option<Intervals>, bool /*itself*/)`:
  print_prune_string_charset copies the string (Fcopy_sequence) unless
  `print-charset-text-property' is t or an unsafe charset is found.
- `prepare_print_numbering(interp, env, continuous)`, `print_to_sink(interp,
  env, value, escape, sink)`.

### src/lisp/primitives/print/object.rs
- `PrintGlobals { depth: isize, being_printed: [Value; PRINT_CIRCLE],
  new_backquote_output: i64, pub(crate) buffer: Option<PrintOutput>,
  pub(crate) inserting: usize, pub(crate) memory: Option<Vec<u8>> }`
  (print.c's statics, on the interpreter).
- `thread_local! static PRSTACK: RefCell<RootedVec<PrintEntry>>` with
  `prstack_sp()`, `prstack_push`, `prstack_pop`, `prstack_top(base_sp) ->
  Option<(usize, PrintEntry)>`, `prstack_set(index, entry)`.  One stack for
  every print (as reader.rs's RDSTACK): each print works above its base_sp;
  a non-local exit leaves its entries and the outer print finishes them
  (GNU `(a b z) c)').  `#[derive(Clone, Copy)] enum PrintEntry { List {last,
  maxlen, tortoise, n, m, tortoise_idx}, Rbrac, Vector {obj, size, idx, end:
  &'static str, truncated}, Hash {table, nobjs, idx, printed, truncated} }`.
- `struct Printer { out: PrintOutput, held: bool, sink: PrintSink, options,
  escapes, read: u16 }`; bits CIRCLE, GENSYM, INTEGERS_AS_CHARACTERS,
  SYMBOLS_BARE, LENGTH, LEVEL, QUOTED, ESCAPE_MULTIBYTE, ESCAPE_NEWLINES,
  ESCAPE_CONTROL, ESCAPE_NONASCII, ESCAPES; `const STDOUT_PENDING: usize =
  4096`.
- `pub(crate) fn print(interp, env, obj, escape, sink)`: reads only
  `print-continuous-numbering' and `print-circle' (print_preprocess when
  circle); `out` = the taken print_buffer for Buffer (`held`), default for
  Stdout, plain for Echo/Function; runs the walk; flushes before reporting
  the walk's error (`flushed.and(walked)`).
- `flush`: Buffer: hand `out` back to `interp.print_globals.buffer`;
  Stdout/Echo: deliver + clear; Function: nothing pending.  `resume`:
  re-take print_buffer after Lisp ran.  `sync`: Buffer/Echo no-op; Stdout
  flush at >= STDOUT_PENDING; Function always.  `lisp_ran`: read = 0.
  `vars(interp, bits)`: read the variables of BITS not yet read (lazily,
  where print.c reads them).  A function sink: `printchar_code` calls the
  function directly per character and then `lisp_ran()`.
- Order rule everywhere: what print.c writes before it reads is written
  (and for a function, delivered) before the read: `(` then print-length
  and the first element; quote char then XCAR (XCDR); `[` then
  print-length; " data (" then the table's count; `#N=` then Fputhash; space
  then XCAR (next); `"` then the escape variables per character;
  `#<buffer ` then the name; a string's text then `string_intervals` again;
  a symbol's name read per character; `#s(hash-table` then test/weakness/
  count.  `next_obj` copies the top entry, steps it, writes it back with
  `prstack_set` and pops as print.c does.
- print-unreadable-function: flush, call with the callback buffer current
  (restore the buffer current before when the callback buffer is live),
  `resume`, `lisp_ran`.
- Pieces (for stdout writes and terpri): fixnum = one strout; nil and t =
  printchars; octalout = printchars; `\x..` escapes and `\ ` = strouts;
  `#<subr NAME>` = print_c_string "#<subr ", print_c_string name, printchar
  '>'; frame/window/process/thread/mutex/condvar names through print_string;
  print_bool_vector's pieces; a string found in `print-number-table' =
  strout of its data (`strout_string`, code/object_snippets.rs).
- print_depth > PRINT_CIRCLE error "Apparently circular structure being
  printed"; `being_printed` without print-circle.

### src/lisp/primitives/file_io.rs
`with_print_stream` / `print_prepared` (print_prepare + print_finish around
a body given the sink), `print_finish_insertion`, `with_printer_destination`,
`select_printer_destination`, `write_printer_output` (write_string),
`write_c_string`, `printchar_to_stream(interp, ch: u32, to_stdout, env)`,
`print_object_to_stream` (Fprin1/Fprinc/Fprint; Fprint's newlines are
printchars), `print_object_to_buffer`: code/file_io.rs.  Removed:
`write_prepared_printer_output`, `record_batch_standard_output_char`,
`native_print_updates_batch_last_char`, `printchar_to_stderr` (now
printchar_to_stream).

### src/lisp/primitives/hash_insert.rs
`insert_print_buffer` (insert_1_both with PREPARE + signal_after_change (PT
- pos, 0, pos), no update_compositions): code/file_io.rs.

### src/lisp/primitives/dispatch/display.rs
- `print_error_message(interp, env, obj, stream: Value, context:
  Option<&[u8]>, caller: Value)`: context through `write_c_string`; caller
  logged for a symbol and for `t` ("t: "); used by `error-message-string`
  (hidden prin1 buffer) and `command-error-default-function` (batch:
  `external-debugging-output' + `terpri' + `kill-emacs -1'; interactive:
  `Value::T`).
- `terpri` per Fterpri inside with_print_stream; ENSURE: error for a
  function ("Unsupported function argument" datum); batch non-buffer sinks
  use `batch_standard_output_last_char`; Echo: the echo text non-empty and
  not ending in newline; else Fbolp.
- `write-char`: CHECK_FIXNUM; `code as u32`; Function: call with that
  number; else `printchar_unsigned`.
- `external-debugging-output`: CHECK_FIXNUM; printchar_to_stream(code as
  u32, stderr).
- `princ` fast path (Fprinc): a string with no intervals and
  `print-continuous-numbering' nil -> print_string only (no depth/numbering
  reset).
- `setup_echo_area_for_printing`; `echo_area_print_logged_as(interp, logged,
  shown, env)`; ECHO_AREA_MESSAGE_VALUE is None while the echo text is empty
  (`current-message' nil).

### src/lisp/primitives/batch_stdout.rs
`pub(crate) fn write_chars(chars: &[u8])` -> `Stream::xsputn_chars` (one
fwrite per character: a fast path copies the characters that fit a stream in
put mode exactly as those calls would; a line-buffered stream's up to a
newline; anything else per-character `xsputn`).  Tests
`characters_fill_the_buffer_where_a_block_is_written_whole`,
`a_run_of_characters_is_written_as_each_character_is` (use `.expect`, never
`.unwrap()`: `clippy::unwrap_used` is denied crate-wide).

### src/lisp/primitives/ccl.rs
`written_character(c: i32)`: negative -> low byte: ASCII as itself, else
raw-byte char `0x3FFF00 + byte`.

## GNU semantics learned (all verified against the oracle)
- printchar to stdout = `fwrite (str, 1, len, stdout)` per character;
  strout one fwrite; print_string = one strout for nil/t sinks, per-char
  printchar for functions re-reading the string; `printchar_stdout_last`
  only set by printchar; `noninteractive_need_newline` set even by an empty
  strout; with `standard-display-table' each character goes through
  printchar_to_stream (last char set before each printchar's write, the
  newline owed only after a write that happened: code/print_rs.rs).
- print_buffer is one static: a nested print's print_prepare saves
  `make_string_from_bytes (buffer, pos, pos_byte)` and print_unwind copies
  back SCHARS bytes, leaving pos/pos_byte where the inner print ended:
  `(prin1-to-string (list 1 2 (current-buffer) 3))` with a
  print-unreadable-function calling `(prin1-to-string 'zz)` is "(1zz 3)".
- Fprinc fast path; print-level any fixnum; print_prune_string_charset's
  copy; static prstack never unwound; record_unwind_current_buffer in
  print_prepare (every sink) and around print-unreadable-function;
  setup_echo_area_for_printing at print_prepare for t interactively.
- print_finish: insert_1_both (buffer, pos, pos_byte, 0, 1, 0) in the
  buffer current then (nchars == 0 returns at once: no prepare, a read-only
  buffer takes it), then `signal_after_change (PT - print_buffer.pos, 0,
  print_buffer.pos)` (always), then `set_marker_both (old_printcharfun,
  Qnil, PT, PT_BYTE)` in the current buffer, then SET_PT_BOTH for a marker
  stream, then unbind.  insert_1_both copies from print_buffer after
  prepare_to_modify_buffer, so a nested print from a before-change hook
  clobbers text and pos (model: PrintGlobals.inserting/memory).
- write-char/external-debugging-output take any fixnum as `unsigned int`
  (see extra/README for the map).
- strout of a C string (size == size_byte): function sink gets `int ch =
  ptr[i]` (signed char on x86-64) as unsigned; echo area inserts each byte
  as a character and message_dolog takes the bytes as text.
- print_string for a function: `size` and `size_byte` read once; loop to
  size_byte reading the live string (a shortened string is read at its
  terminating NUL; stop past it).
- GNU-undefined cases NOT to emulate: see records/findings364.src.

## Performance (measured on the first implementation; do not regress)
perf1.el-style loops against GNU / round 363: stdout 0.116 s (GNU 0.188,
r363 0.122); prin1-to-string 0.109-0.126 (GNU 0.13, r363 0.09); function
stream 0.46-0.57 (GNU 0.37, r363 0.39); small objects 0.54-0.68 (GNU 1.0,
r363 0.54-0.58).  What made it fast: the walk writes straight into
print_buffer (`held`/`resume`, plain output), STDOUT_PENDING batching +
`xsputn_chars`, lazy `vars` with `lisp_ran`, direct function call per
character.

## Rules
- No `#[allow(...)]`/`#[expect(...)]`; clippy `--all-targets --all-features
  -D warnings` clean (denied here: unwrap_used, too_many_arguments (max 7),
  blocks_in_conditions); `cargo fmt`.
- No AI attribution anywhere (commits, PRs).  Commit as `git -c
  user.name=Ray -c user.email=26018378+rayfdj@users.noreply.github.com -c
  commit.gpgsign=false`.
- tools/round_audit.py must not be modified without the user's approval
  (its H2 fires on a changed line naming `print-integers-as-characters';
  the user decides).
- One heavy build at a time (two rustc of the crate OOM the 15 GB box).
- Validate only on the final source: probes (final/, extra/), forms, fuzz,
  strace, tty, then gate (scripts/localgate.sh), integration (cli,
  cli_parity, ert_runner, native_comp_identity, native_thread_continuations,
  package_lifecycle, runtime_ownership), clippy.
