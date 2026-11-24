# The call from C into byte code (D17, D25): measurements and staged plan

Written on 23 November 2026 before round 383, from callgrind runs on round
377's release build. Paths beginning `/home/user/` and `/tmp/` are those of
the container it was written in; line numbers are of that tree, and the
expected counts are estimates. HANDOVER.md records what each stage measured.
Stages S0 to S3 are round 383.

Written 23 Nov 2026 by a read-only planner (no cargo, no build). Measured with callgrind on
/home/user/emaxx/target/prof-r377/release/emaxx (round 377's source; emaxx-draft is at 40f2c8fa,
round 378, windows only: the call path is the same) and GNU /home/user/emacs-mirror/emacs/src/emacs.
Scratch: the scratch directory d17/plan/
(run.sh/all.sh regenerate every profile in about a minute, --dump-instr=yes; *.lst per-instruction
listings made with ../instr.py and ../selfdiff.py (the d17 folder's earlier tools),
groups.py sums them by purpose, guards.txt, beh/*.el behaviour probes run on both editors).
Every count below is instructions a callback call (run N1 and N2, difference over the calls).

| probe | Emaxx | GNU | over |
|---|---|---|---|
| mapc-bc | 414.7 | 288.9 | +125.8 |
| mapc-bc-bc | 395.5 | 286.5 | +109.0 |
| mapatoms-bc | 404.1 | 298.9 | +105.2 |
| mapatoms-closure | 502.0 | 458.6 | +43.4 |
| maphash-bc | 417.0 | 291.1 | +125.9 |

Guards (Emaxx/GNU): lex-rec 360.1/314.9, byte-calls 447.6/468.0, dyn-rec 634.0/636.2,
interp-ignore 2762/2218, mapc-interp 1459.7/2435.8, native-base 51.1/50.0, native-subr
78.1/73.0, native-cons 93.7/79.4.

## Summary

Design: keep Emaxx's fused Ffuncall (call_function_value's plain path already does Ffuncall,
funcall_general and funcall_lambda in one frame, 121 against GNU's 148, because GNU's two
middle layers are tail jumps whose prologues GCC does not shrink-wrap) and make the byte-code
entry GNU's exec_byte_code-from-C: no environment frame for lexical byte code (funcall_lambda
calls exec_byte_code without a specbind), setup_frame (entry cache in place of AREF and the
template, frame, arguments) and GNU's `exit:` in the same frame as Ffuncall's record, then the
verified loop. Emaxx keeps what GNU has no counterpart for: verification and its entry cache,
the activation's watermarks (non-local exits are Rust returns, not longjmps), the separate
verified loop (also entered from run_loop), the profiler's deferred sampling, the Edebug and
backtrace-eval captures (the environment is not on the specpdl). The map functions keep
mapcar1's shape: one Ffuncall per element, the function cell read at each call.

| stage | what | mapc-bc / mapc-bc-bc / mapatoms-bc / mapatoms-closure / maphash-bc (est.) |
|---|---|---|
| now | | 414.7 / 395.5 / 404.1 / 502.0 / 417.0 |
| S1 | no environment frame for lexical byte code | 385 / 383 / 374 / 489 / 387 |
| S2 | exec_byte_code's entry in Ffuncall's frame | 361 / 359 / 350 / 465 / 363 |
| S3 | record pop as specpdl_ptr--; the exit's tests the construction answers | 349 / 347 / 338 / 453 / 351 |
| S4 | Ffuncall's signature (no name) for Rust callers | 344 / 342 / 333 / 448 / 346 |
| S5 | the trace flag read as one byte (optional) | 341 / 339 / 330 / 445 / 343 |
| D9 S3+S4 | (D9's plan) header caller test, saved_top | ~336 / 334 / 325 / 440 / 338 |
| GNU | | 288.9 / 286.5 / 298.9 / 458.6 / 291.1 |

D25 (mapatoms-closure) closes at S3. D17 ends about +35 to +47 over GNU on mapc-bc (stage
estimates are from the listings, about ±10); what remains is named in section 3. lex-rec, byte-calls, dyn-rec: unchanged except S3's shared
record pop (-3..-5 a return, optional) and D9's stages; interp-ignore and native-*: unchanged.

## 1. Measurement and attribution (mapc-bc, plain r377)

Per function. Emaxx: funcall_closure 198.0, call_function_value 121.0, run_verified 61.0,
map_sequence_of 22.0, sequence_length_value 10.1. GNU: exec_byte_code 112.0, Ffuncall 61.0,
funcall_general 47.0, funcall_lambda 40.0, mapcar1 17.0, list_length 9.5. So the call is
380 against 260; call_function_value is 27 under GNU's three functions; funcall_closure +
run_verified is 259 against exec_byte_code 112 (+147).

Per group (sums of the listings fc.lst, cfv.lst, rv.lst, ms.lst, g-*.lst by groups.py; each
instruction counted once; excess by row +5.5, -9, +15, +8, +5, +53, -32, +56, +24):

| group | Emaxx | GNU | Emaxx-only part, and what GNU does instead |
|---|---|---|---|
| map loop + length | 22 + 10 | 17 + 9.5 | env passed on the stack (7-word call), `None` name, Result test: GNU's call1 passes nargs and a 2-word array |
| argument handling | call 6 + takes 7 + push 19 = 32 | call 4 + template 15 + PUSH 14 + fill 8 = 41 | none (Emaxx's count mask and switch are cheaper than the template decode and PUSH loop) |
| backtrace record | push 17, debug flag 10, pop 15 = 42 | record_in_backtrace 21, depth--/flag 5, specpdl_ptr-- 1 = 27 | profiler sample tests 5+4 (GNU's SIGPROF handler records in place); get(depth) bounds 2 and len == depth+1 4 (GNU's count is its record by construction); owns_nothing 5 (specpdl entries own nothing) |
| depth, quit, GC, debug checks | 12 + 12 = 24 | 16 | edebug_may_be_entered 3, backtrace-eval name 7 (GNU's Edebug and backtrace-eval read the environment off the specpdl), original_name test 2 (Ffuncall has no name) |
| callee lookup + entry cache | 29 + cache 15 + copy 7 = 51 | funcall_general 20, funcall_lambda 15, AREF/frame 11 = 46 | the cache lookup is verification's (GNU trusts the compiler) |
| frame entry | env 30, ensure 4, overflow 10, header 11, locals 6, marks 10 = 71 | overflow 2, frame fields 12, locals 4 = 18 | env frame (GNU: no specbind for lexical byte code); marks (GNU's handlers restore bc->fp, pdlcount and depth on longjmp); lazy stack (GNU allocates at thread start); two-compare overflow and `base` (D9 S4) |
| prologues and epilogues | call_function_value 17, funcall_closure 22, run_verified 17 = 56 | Ffuncall 14, funcall_general 27, funcall_lambda 25, exec_byte_code 22 = 88 | a third real frame (funcall_closure: exec_byte_code's entry lives in the loop's function); GNU wastes 52 in two tail-jump frames, Emaxx fuses them |
| loop protocol | VmFrames + call 18, run_verified's field copies 19, words/top 3, code bytes 9, trace 4, loop entry 13 = 66 | first dispatch 10 | the loop's state is exec_byte_code's locals in GNU (VmFrames + copies 37); trace flag (none); code bytes 9 (SDATA 1) |
| return | Breturn 10, tags 11, exit tests 11, restore 6 = 38 | Breturn 6, exit 3, result + stack protector 5 = 14 | Result/Option/Some tags (GNU returns in rax, errors longjmp); four exit tests (GNU none: hand-made code that leaves entries is undefined there, see beh/unbal.el: GNU segfaults); stack restore 6 (bc->fp 3) |
| total | 412 (+2.7) | 286.5 (+2.4) | |

funcall_closure by piece (198): prologue 12, environment test 10, EnvFrame::dynamic push 8,
entry_program 15, takes 7, program copy 7, ensure_allocated 4, EntryMarks 10, overflow test
10, header and frame/base/top 11, push_arguments_within 19, Running/op_error_frames/pc/
quitcounter 6, code bytes 9, words/top 3, trace_load_errors 4, VmFrames + call 18, Option
test 3, exit tests 11, stack restore 6, Ok tag 3, environment truncate 12, epilogue 10.
call_function_value (121): prologue 7, name test 2, symbol + cell 8, closure tag + pvec 15,
size 2, code is a string 4, gate 4, atimer 3, edebug 3, backtrace-eval name 7, GC 3,
depth++ 2, record (profiler 5, capacity 4, stores 8), call 6, Result 3, debug flag 10, pop
(profiler 4, len 4, owns_nothing 5, set_len + depth-- 2), epilogue 10. run_verified (61):
prologue 26 (7 pushes, 19 copies of VmFrames fields and stack arguments), loop entry 13,
Breturn 10 (top stored, bc_frames above frames_base), epilogue 12.

Other probes. mapc-bc-bc: funcall_closure 181 (the environment test without the push and
truncate, 13 instead of 30), rest identical. mapatoms-bc: lifted loop 24 (GNU map_obarray
23.8 + mapatoms_1 15) + the same 121 + 198 + 61. mapatoms-closure: call_function_value 115
(a closure value: no cell, no name test), funcall_closure 181, run_verified 96, get 85.7
(GNU exec_byte_code 183, funcall_general 42, Fget + plist_get + Fassq 93.7): the call is
+102, the body -44, the loop -15. maphash-bc: lifted loop 34.4 against Fmaphash 23.4
(slot walk returning Option<(index, key, value)>, 2-argument call setup), call as mapc-bc.

## 2. Inventory: each check and conversion on the path

call_function_value (core.rs:1499; plain_funcall 1549; plain_bcall_entry 2659; record
variables.rs:2972/3186; settle_call_result core.rs:2204; leave_plain_funcall variables.rs:3220):
- `original_name.is_none()`: selects the unnamed path. GNU: Ffuncall has no name. Goes (S4).
- symbol_cell + function cell: funcall_general's SYMBOLP/`u.s.function`. Keep. An alias
  (cell holds a symbol) goes to the general path, as GNU's indirect_function loop.
- closure tag, PVEC_CLOSURE, size, code slot a string: CLOSUREP in funcall_general and
  funcall_lambda, FIXNUMP(ARGLIST) in funcall_lambda (Emaxx's template test is in the cache,
  `takes`/`arguments`). Keep.
- depth < call_depth_gate: GNU's `++lisp_eval_depth > max_lisp_eval_depth`, maybe_quit's
  quit flag and debug_on_next_call, folded (the gate is i64::MIN for quit, termination,
  debug-on-next-call, Emaxx's call profiler; C stack depth). Keep.
- atimer::signals_pending: maybe_quit's pending_signals. Keep.
- edebug_may_be_entered, backtrace-eval name: no GNU counterpart; the record's lexical
  context capture, needed while Env is not on the specpdl. Keep (by symbol identity is -2,
  with D14's well-known symbols; not proposed here).
- collection_cannot_be_due: maybe_gc. Keep.
- record push: profiler sample test (keep: deferred sampling files a sample under the frames
  that stood, GNU's handler records at once), capacity (grow_specpdl's end test), 4 stores.
- Result test after the callee: Rust's Err for longjmp. Keep (S2 folds it into the merged
  frame).
- frame_debug_on_exit(depth): bounds test goes (S3: depth is the innermost record by
  construction); the flag test is GNU's backtrace_debug_on_exit.
- pop: profiler test keep; `len == depth + 1` goes to debug_assert (S3: finish_activation
  truncates to marks.backtrace = depth + 1 on every path, the fast return tests it);
  owns_nothing keep (a frame with a detail must be dropped), its address shared with the flag
  test; set_len = specpdl_ptr--; depth-- = lisp_eval_depth--.

funcall_closure (vm.rs:4129) with run_entered_inline/run_entered_with (4214/4263):
- environment test + EnvFrame::dynamic push + truncate: GNU's funcall_lambda calls
  exec_byte_code with no specbind for a FIXNUMP template; it specbinds
  internal-interpreter-environment to nil only for a list arglist (dynamic binding) when the
  environment differs. Goes for lexical byte code (S1); stays for run_dynamic and a dynamic
  fallback. The comments at vm.rs:4123-4127 and core.rs:2237-2241 say funcall_lambda binds
  for byte code: wrong for lexical byte code, corrected in S1.
- entry_program (TLS table by closure identity, code_changes, vector_sweeps): verification's
  lookup; GNU reads AREF CODE/CONSTANTS/STACK_DEPTH. Keep (the anti-cheat control requires the
  live constants vector, which the cache holds by reference).
- takes(argc): setup_frame's template decode and arity test. Keep.
- program copy (before anything runs Lisp: re-entrancy of the cache slot). Keep.
- ensure_allocated: GNU allocates the stack at thread start. Keep (BcStack::clone copies the
  whole 4 MB allocation when allocated); optional: an unallocated stack's limit 0 so the
  overflow test's cold path allocates (-2, with D9 S4's one compare).
- EntryMarks (frame/base/top, specpdl, backtrace, bc_frames, bc_handlers): no GNU
  counterpart (handlers restore on longjmp). Keep: finish_activation needs them on every Err.
- enter_frame (two compares, header, frame/base/top): setup_frame; D9 S4 makes it one compare
  and drops base.
- push_arguments_within: PUSH loop and nil fill. Keep.
- Running, op_error_frames, pc, quitcounter in memory: the loop's state, which run_loop
  resumes. Keep.
- bytes_unguarded's borrow assertion + string read: SDATA. Use bytes_unchanged (entry_program's
  code_changes check vouches that no guard was taken) (S3, -2).
- trace_load_errors(): Emaxx's diagnostic (EMAXX_TRACE_LOAD_ERRORS), an OnceLock read. The
  compat harness sets it on the measured Emaxx runner (src/bin/compat-harness.rs:3379), so
  it must stay cheap and must not close the call gate (that would put every call of the frozen
  suite on the full path). S5 reads it as one latched byte (-3).
- VmFrames build, two stack arguments (pc, quitcounter), Option result: the loop is its own
  function. Keep (experiment X1).
- exit tests: bc_frames == frames_base is answered by Breturn's last_above (debug_assert, S3,
  or D9 S3 if it lands first); backtrace == mark keep (a stale record would borrow a dead
  argument array); specpdl == mark and handlers == base keep (hand-made code may leave a
  binding or handler: GNU crashes, Emaxx undoes it; probe beh/unbal.el).
- stack restore from marks: GNU's `bc->fp = bc->fp->saved_fp`. Keep.

## 3. Target design

Entry from C, as GNU's Ffuncall -> funcall_general -> funcall_lambda -> exec_byte_code:
- call_function_value (Ffuncall, funcall_general and funcall_lambda fused, as now): gate,
  symbol's cell, CLOSUREP and byte code, record_in_backtrace, maybe_gc; then, in the same frame
  (S2), exec_byte_code's entry for a closure the cache verified with these arguments: frame
  (header saved_pc 0 as a C entry once D9 S3 lands), arguments, the verified loop; at its
  Breturn GNU's `exit:` (caller's frame and top restored) after the two balance tests;
  `lisp_eval_depth--`, the debug-on-exit flag with the record innermost, `specpdl_ptr--`.
- No environment frame for lexical byte code (GNU leaves Vinternal_interpreter_environment as
  it is); dynamic-binding byte code keeps the nil frame when the environment differs (GNU's
  specbind of lexenv nil), in run_dynamic and in the VM's enter_dynamic_frame precondition.
- Non-local exits: an Err reaches finish_activation (marks), then settle_call_result and the
  record's truncation, as now. Nothing between the marks and finish_activation may return
  early after the merge.
- Emaxx-only, kept: verification + entry cache; marks; the verified loop as its own function
  (VmFrames: entered from run_loop too); deferred profiler sampling (two tests); Edebug and
  backtrace-eval captures (Env off the specpdl); Result returns.
- Map functions: mapcar1's loop (map_sequence_of), map_obarray (the shared enumeration) and
  DOHASH_SAFE (entry_at_or_after) each call Ffuncall once an element, through the nameless
  5-word entry (S4); the function cell and the cache are read at each call (a callback that
  redefines itself or a code-string store must be seen: no hoisting out of the loop).

What remains after S5 + D9 (mapc-bc ~+35..+47), by the groups of section 1: loop protocol +51
(VmFrames + run_verified's copies 37, words/top/code bytes 10: the loop is its own function;
X1), frame entry +18 (marks 10, lazy stack 4: Result exits, BcStack::clone), return +15 (tags 8,
balance tests 6), record +7 (profiler tests 9), checks +6 (Edebug/backtrace-eval captures 10,
which would go only if Env frames became specpdl entries of internal-interpreter-environment,
the A2.3 item), cache +5; against two frames where GNU has four (-54) and cheaper
argument handling (-15).

## 4. Stages

Each: measure the five probes + lex-rec, byte-calls, dyn-rec, interp-ignore, mapc-interp,
native-base/-subr/-cons, cg-call-loop, windows (`CG_JOBS=1 tools/perf/cg-table.py`), count
non-nop instructions of the touched functions from listings (alignment nops move), and time
mapc-bc ns/call on the plain build (min of 9) as round 370 did. Reject a stage that raises
any guard by more than 2 real instructions without a stated cause.

S0 tests first (no runtime change; ~300 lines). Existing guards: compat_01::
rust_callers_reach_byte_code_closures_given_named_or_aliased_as_funcall_does,
debugger_frames_enter_on_exit_and_at_the_next_call,
signals_enter_the_debugger_as_signal_or_quit_decides; primitives::tests::
native_mapatoms_preserves_collection_redefinition_and_nonlocal_exit,
mapatoms_walks_the_standard_obarray_in_place,
mapatoms_honors_dynamic_obarrays_legacy_conversion_and_function_cells,
verified_bytecode_follows_changed_shared_and_collected_code_like_gnu,
suspended_bytecode_retains_operand_and_unwind_roots,
a_signal_from_unbinding_a_byte_code_callee_comes_out_in_its_caller,
bytecode_calls_switch_frames_like_setup_frame, compiled_catches_and_condition_cases_run_as_gnu_runs_them,
bytecode_stack_capacity_and_nested_reservations_match_gnu; eval_02::
bytecode_calls_lay_their_frames_on_one_stack_and_unwind_them_as_gnu_does; eval_05::
backtrace_eval_selects_a_debugged_bytecode_callers_lexical_context; runtime.rs
native_mapcar_follows_fns_c_list_branch, native_maphash_calls_each_live_slot_through_funcall,
native_funcall_honors_gnu_debug_on_exit_frame_flag; cli_parity profiler_is_profiler_c_s,
dynamic_binding_byte_code_binds_as_funcall_lambda, c_stack_overflow_* (7417-7567);
anti_cheat native_comp_fast_paths_are_audited_against_gnu_c. New cli_parity (GNU oracle at
test time; each probe agrees on both editors today):
- beh/dbg.el: backtrace-debug in a mapcar and a maphash callback (debugger's value replaces
  the call's), debug-on-next-call set in one callback entering at the next, throw and signal
  out of mapc, backtrace depth after.
- beh/gc.el: argument held only by C's call survives garbage-collect in the callee (weak
  table), mapcar re-entered recursively, callback that fsets itself mid-mapcar, a callback
  yielding to a Lisp thread doing the same.
- beh/env1.el, env2.el (agreeing cases): eval nil/t of a variable the interpreted caller binds,
  a dynamic interpreted lambda, backtrace--locals and backtrace-eval of the caller's frames,
  macroexpand, an interpreted closure called from the byte code; beh/dyn-lex.el (lexical byte
  code from Rust calling dynamic byte code under a lexical caller).
- beh/prof.el: the memory profiler files a callback's allocation with mapc below it.
Library tests pinning Emaxx-only answers (GNU crashes): hand-made byte code (beh/unbal.el)
returning to C through mapcar, funcall and a direct call with a binding left (undone, special
back to `global'), and with a catch handler left (removed: a later throw to its tag is
no-catch).

S1 funcall_lambda's direct exec_byte_code: no environment frame for lexical byte code (~40
runtime lines + ~80 test: vm.rs funcall_closure, execute_closure's fallback, core.rs
execute_bytecode_funcall_body; comments). The push stays where the arglist is a list.
Lisp-visible, toward GNU: beh/env2.el case 3 and env1.el case 3: defvar-1 or
internal--define-uninitialized-variable from a mapcar/funcall callback under an interpreted
`let` of the same name: GNU defines it (ok, special), Emaxx signals "Defining as dynamic an
already lexical var" because the nil frame turns the caller's environment into an old value
for lexbound_p (definitions.rs:208). New cli_parity test fails before, passes after.
Audit (every Env reader reachable from a subr called by byte code must be one GNU has):
lexbound_p (fixed toward GNU), backtrace_eval_at variables.rs:3506 and
capture_backtrace_context 3102 (the capture now includes the caller's frames, as GNU's rewound
specpdl), with_macro_lexical_binding macros.rs:492/517 and every lookup in bindings.rs
(eval_sub and special forms only), funcall_interpreted_lambda's install test core.rs:2582
(GNU's !EQ test), eval_impl loading.rs:389/1150 (installs), local special declarations
variables.rs:883-915 (defvar form), enter_dynamic_frame vm.rs:1008 (declines under a lexical
environment: mixed code's first dynamic call takes the full arm, which pushes GNU's nil
frame; correct, slower; GNU specbinds there too). grep `env.last`, `current_environment`,
`environment_is_lexical`, `.eval(` in src/lisp/primitives for any other.
Expected -30 (interpreted caller), -13 (byte-code caller); guards 0.

S2 exec_byte_code's entry in Ffuncall's frame (~30 lines, vm.rs + core.rs): funcall_closure's
body becomes an #[inline(always)] function plain_funcall calls; the #[inline(never)]
funcall_closure stays for funcall_bytecode_closure (core.rs:2044). Saves funcall_closure's
prologue/epilogue, argument moves, Result/Option round trip. Expected -22..-28 on the five;
mapc-interp ±3. Risks: call_function_value is every Rust Ffuncall: its frame grows by ~0x168
(stack only; C_STACK_BYTES_PER_LEVEL is 8 KB, continuations.rs:77); register allocation of its
subr and native plain paths (add probe mapc-subr, `(mapc #'identity list)`, to tools/perf/probes
and cg-table.py's list); every Err must still pass finish_activation. Alternative if the subr
path worsens: call_function_value as a frameless dispatcher tail-calling a funcall_bytecode
that holds record + entry + pop (GNU's tail-jump chain). Tests: S0's set.

S3 the record's pop as specpdl_ptr-- and the exit's own tests (~150 lines, variables.rs,
core.rs, vm.rs): a helper popping the innermost record (debug_assert it is DEPTH; flag and
owns_nothing from one frame address; profiler test kept) used by plain_funcall (and,
optionally, return_to_bytecode_frame's leave_plain_funcall(record): lex-rec/byte-calls/dyn-rec
-3..-5); run_entered_with's bc_frames test to debug_assert (unless D9 S3 did it); bytes_unchanged
at the plain entry. Keep the backtrace, specpdl and handler tests. Expected -11..-13. Risks: a
broken invariant in release would leave a stale record; the assertions run in the debug-
assertion filter (bytecode/backtrace/debugger/handlers/unwind, as round 376's 159). Tests: S0's
debugger and unbalanced-code tests; round 376's
a_signal_from_unbinding_a_byte_code_callee_comes_out_in_its_caller.

S4 Ffuncall's signature for Rust callers (~50 lines, core.rs + hooks_overlays.rs): a nameless
`Interpreter::funcall(func, args, env)` (5 words, no stack argument) that the forwarder
(hooks_overlays.rs:8) calls; call_function_value keeps its name and signature (the anti-cheat
control calls it). Map loops and every hook runner benefit. Expected -5; mapc-interp -5.

S5 (optional) the trace flag as one latched byte instead of an OnceLock read (~30 lines, mod.rs,
vm.rs): -3 an entry; R4 lists the replaced static. Do not fold it into the call gate (see the
inventory: the frozen suite runs with it set).

D9 S3/S4 (owned by m2/d9/plan.md, not repeated here): saved_pc caller test, saved_top and one
compare: -5..-8 on these probes. Interleave by round; do not mix the two plans in one round.

Experiments, not stages until measured: X1 pc and quitcounter into VmFrames (run_verified loses
two stack arguments; previous round: one moved parameter cost 7 an iteration; `left` stays
first); X2 maphash's slot walk without the Option triple (-5..-8 on maphash-bc); X3 the
backtrace-eval test by symbol identity (with D14).

## 5. Traps

- round_audit H5: no #[allow], #[expect], cfg_attr(..., allow), crate-level or Cargo forms.
  Do not edit lines carrying existing ones: vm.rs:996 (enter_dynamic_frame), vm.rs:1874
  (run_verified), core.rs:634 and 784, verify.rs's clippy::vec_box, profiler.rs's unused_mut.
  New functions <= 7 parameters (clippy too_many_arguments).
- H6/R8: debug_assert! counts as an assertion; a test turned into debug_assert adds one;
  keep run_entered_with's `debug_assert!(setup_runs_lisp || admitted())` through the merge.
- R3 wording in added runtime lines (fallback, stub, special case, workaround, hack); R2 (`let
  _ =`, `.ok();`); R4: no new cache or registry (do not hoist the callee or its EntryProgram
  out of a map loop: GNU's call1 reads the cell each time); S5's static is a replacement, state it.
- R6: do not touch src/anti_cheat.rs, tools/round_audit.py, .cargo/config.toml, Cargo.toml,
  compat/. tools/perf probes are not R6 but their lines count.
- anti_cheat.rs: native_comp_fast_paths_are_audited_against_gnu_c calls
  `call_function_value(function, None, &[], ..)` on make-byte-code and checks an aset into the
  constants vector is seen (keep the name, the signature, the live constants); its fast-path
  inventory reads string literals between runtime.rs's anchors (invoke_context_free_subr ..
  invoke_subr .. slow_unary_subr; native_subr_address .. DirectFuncallTarget) and the
  generator's direct_native_target: these stages need not touch runtime.rs; if S4 reaches
  invoke_native_mapcar (4565, outside the anchors) add no primitive-name literal inside them;
  EXACT_NATIVE_C_FAST_PATHS's mapcar/maphash contract tests must pass; required test names
  (anti_cheat.rs:1112-1114, bytecode_reads_original_constants_* among them) are not renamed.
- Manifests: no new primitive, DEFSYM or C variable (funcall is a method, not a subr; no
  cached_symbol! of a non-GNU name). Root inventory: no new InterpreterState field holding a
  Value; any new per-thread field joins thread_context.rs's exchange list.
- Records: HANDOVER entry per round, docs/milestones.md D17/D25 lines, docs/plans/d17-*.md
  from this plan when S1 lands; state S1's Lisp-visible change in the round's record.

## 6. Found while planning (not fixed here unless said)

- S1 fixes: defvar-1 / internal--define-uninitialized-variable from byte code that C called
  under an interpreted let of that name (GNU ok, Emaxx error; beh/env1.el, env2.el).
- Hand-made byte code with a stack underflow (make-byte-code 257 with the bytes #o301 #o030
  #o001 #o207, constants [d17v bound], called with one argument) panics in run_fast
  (vm.rs:1600, index out of bounds) where GNU segfaults.
- The compat harness's EMAXX_TRACE_LOAD_ERRORS=1 turns off the VM's plain subr and native call
  paths (vm.rs:764, 865) on the measured runner: the frozen suite's timings do not exercise them.
