# A byte-code call's frame (D9, B4): measurements and staged plan

Written on 23 November 2026 before round 376, from a reading of the tree at
round 375 and callgrind runs on its release build. Line numbers are of that
tree, and the expected counts are estimates; HANDOVER.md records what each
stage measured. Stages S1 and S2 are round 376.

Measured on /home/user/emaxx/target/prof-r375/release/emaxx (emaxx-draft be6311e8).
Totals: lex-rec 371.1 / GNU 314.9, dyn-rec 693.0 / 636.2, byte-calls 459.6, mapc-bc 413.7.

## Summary
Correction: in lex-rec Emaxx's call+return is 295 (283 + two head re-dispatches); GNU's is
180 (call 144, Breturn 36), not 230. Excess ~+115, mostly the return (113 vs 36). Emaxx's
simple ops are ~60 cheaper (76 vs 135), hiding half. In dyn-rec the dynamic call+return is
580 vs ~493: +87 is D9's "~80".

Design: the 4-word header stays GNU's bc_frame with GNU's saved_top (caller's function slot
+ 1), so `base` goes; saved_pc == 0 marks a frame entered from Rust (GNU's NULL). Callee's
backtrace record = the last one (GNU's specpdl_ptr - 1); a handler records its frame
(act_rec); code and constants from the caller's closure (fun = fp->fun); op_error_frames is
0 at every call (per-activation count only). BcFrame 9 words -> 3: unwinds_len, verified,
code_changes. A per-activation code_changes snapshot is UNSOUND (an unverified caller could
be resumed in the verified loop). Keep the entry cache (it yields `verified`).

| Stage | What | lex / dyn / byte-calls / mapc (cumulative est.) |
|---|---|---|
| S1 | dynamic return in one pass | 371 / ~638 / 460 / 414 |
| S2 | last backtrace record; backtrace_depth, op_error_frames go | ~348 / ~612 / ~437 / 414 |
| S3 | caller test from header's saved_pc | ~341 / ~605 / ~430 / ~411 |
| S4 | GNU's saved_top; base goes; one-compare overflow test | ~330 / ~594 / ~419 / ~406 |
| S5 | handlers record their frame | ~328 / ~592 / ~417 / ~406 |
| S6 | program from the closure; BcFrame = 3 words | ±2, structural, optional |

After S2 D9 closes (dyn-rec under 636). B4 ~0.89 of GNU's instructions. D17 gains ~5-8 only.

Risks: (1) derived-record invariants (S2, S3): callee's record innermost at every frame exit;
a frame with saved_pc != 0 owns the last BcFrame; a broken path silently corrupts
debug-on-exit/backtraces. (2) GC rooting in S4: outgoing args stop being traced through the
bc stack and depend on the backtrace record (running thread eval.rs:5854, parked threads
thread_context.rs:66). (3) register allocation in run_verified (41 KB loop): measure non-nop
instructions on all probes and guards each stage.

## 1. Measurement and attribution

lex-rec, one level (all inlined in run_verified; listing d9/lr-e.lst).
Call 182: C1 Bcall arm 32 (func, symbol->function cell, next pc, top index, closure tag and
pvec type, 3 spills, 1 nop); C2 enter_closure_frame start 5; C3 plain_bcall_entry 18 (depth
gate 3, args_start 1, atimer 3, edebug 3, backtrace-eval name 7); C4 entry_program cache 15;
C5 collection_cannot_be_due 3; C6 takes(argc) 3; C7 enter_frame 23 (two-compare overflow 9,
header 4 stores, frame/base/top); C8 lisp_eval_depth++ 1; C9 backtrace record push 21
(profiler flag 4, capacity 3); C10 BcFrame build+push 28 (9 words); C11 callee Running 6;
C12 args pushed 8; C13 callee code entered 13; H head re-dispatch 6.
Return 113: R1 value/top/store 7; R2 bc_frames.last_above(frames_base) 11; R3
frame_debug_on_exit(backtrace_depth) 11; R4 unwinds > unwinds_len 3; R5 BcFrame fields 8;
R6 leave_plain_funcall 16 (profiler 3, len==depth+1 4, bounds 2, owns_nothing 5, store);
R7 depth-- 1; R8 leave_entered_frame + caller base recomputed 14; R9 replace_call 5; R10
program restored 4; R11 op_error_frames restored 2; R12 handlers truncated 3; R13
code_changes compared 3; R14 frames len-1 1; R15 resume 18; H 6.
Rest of level (dup, constant x2, =, goto-if-nil, stack-ref, 1-) 76.
GNU exec_byte_code 314.9: call 144 (Bcall decode 7, maybe_quit 8, depth 9,
record_in_backtrace 24, maybe_gc 2, debug_on_next_call 3, symbol/CLOSUREP/template 23,
setup_frame 66, dispatch 5); Breturn 36; rest 135.

dyn-rec, one level. Emaxx 693.0: run_verified 290.7 (call side 112, return 66, body 113);
enter_dynamic_frame 243.1 (pro/epilogue 15, env test 4, set-up 28, arglist walk ~45,
enter_frame 23, record 24, BcFrame 34, program 7, binding loop 13, specbind_plain_store 40,
result 6); return_unbinding 119.0 (pro/epilogue 15, loop 9, second pass of
return_to_bytecode_frame ~95: repeats top store, last_above, debug test, unwinds test ~25);
unbind_plain 40.0 (once per binding, own pro/epilogue). GNU 636.2: exec_byte_code 346.2,
funcall_lambda 130, unbind_to 65, specbind 48, funcall_general 47.

byte-calls: run_verified 459.3: call ~193, return ~113, rest ~153 (Bcall2).
mapc-bc: funcall_closure 198, call_function_value 121, run_verified 60 (prologue/spills 33,
Breturn caller test 3, epilogue 12).

## 2. Inventory
BcFrame (vm.rs:482-498; written 607-615 bytecode_call, 935-947 enter_closure_frame,
1005-1017 enter_dynamic_frame):
- program {code, constants, verified}: readers 1137/1150, 4709, 5035, 5049. GNU: fun =
  fp->fun; AREF CODE/CONSTANTS (bytecode.c:899-903, 976-980). Code/constants derivable (~2
  loads from header(caller).function; every frame's function is a closure except test-only
  `execute`). verified: no GNU counterpart, ~15 via cache and can miss: keep.
- base: 1136/1147, 4707, 5033, 5048. GNU saved_top (bytecode.c:894). Free if saved_top =
  function slot + 1.
- handlers_len: 1139/1152, 4711, 4963 (handler floor), 5037. GNU handler->act_rec
  (eval.c:1760; restored 1359). Derivable by Handler.frame == bc_stack.frame (one store per
  handler push).
- unwinds_len: 1130, 1253, 1268. GNU record's specpdl position / funcall_lambda's count
  (eval.c:3274/3355). Derivable at ~+6 per return: keep.
- backtrace_depth: 1129, 1135/1142, 1256, 1276, 4691. GNU specpdl_ptr - 1
  (bytecode.c:890-892). Derivable: backtrace len - 1 at exit, after op frames popped.
- op_error_frames: 1138/1151, 4710, 5036, 5050. Always 0 at a call (raised only at
  3863/3903/3999, each followed by Err; caught -> popped 4996; leaving -> popped 5023). Drop.
- code_changes: 1153. No GNU counterpart. Keep (admits test +~3; snapshot unsound).
Container refs: push 616, 948, 1018; pop_above 4686, 5016; truncate 4392; len 4290, 4359,
4573; eval.rs:6491, 7005; thread_context.rs:21,50; dump_roots.rs:426 (reason text).
Entry cache (verify.rs:310-324): identity, code_changes, vector_sweeps -> code, constants,
verified, stack_depth, entry_depth, count mask, arglist. GNU reads AREF and FIXNUMP of the
template (bytecode.c:795-805, 494-499). Keep.
Call-path checks: callee_function (vm.rs:713)/closure_ref in C1 (GNU BARE_SYMBOL_P,
CLOSUREP bytecode.c:793,795); depth < call_depth_gate 3 (core.rs:2659-2666, gate 2680: depth,
C stack, quit, termination, debug-on-next-call, profiling; GNU maybe_quit 771, depth 773,
debug_on_next_call 788); atimer::signals_pending 3; edebug_may_be_entered 3 (no GNU);
is_backtrace_eval_name 7 (core.rs:2773; no GNU); collection_cannot_be_due 3 (runtime.rs:1035;
maybe_gc 787); takes 3 (verify.rs:207; arity in setup_frame 525-528); enter_frame bounds 9
(vm.rs:85-88; GNU one compare 502-503). Return: debug_on_exit (bytecode.c:890), unwinds test
(unbind_to only for dynamic frames eval.c:3355), code_changes (none), profiler
samples_pending (variables.rs:3245; none), owns_nothing (none).

## 3. Target design
Header 4 words = bc_frame: saved_frame; saved_top = caller's function slot + 1 (Rust entry
keeps the Rust caller's real top); saved_pc = 0 for Rust entry, else >= 1; function (always a
closure). BcFrame {unwinds_len, verified, code_changes}. Handler gains `frame`.
op_error_frames per-activation only.
Normal return: test saved_pc; take last backtrace record, test debug flag, pop; unbind above
unwinds_len if any; pop header; write value at saved_top - 1; program from caller's closure +
record's verified.
Non-local exits (run_loop, per frame): search handlers whose frame is current; if match: pop
op frames, unwind to handler.unwind_len, jump. Else pop op frames, settle, unwind above
unwinds_len, truncate backtrace to the record's index (computed before unwind forms run), pop
header, take caller's program, op_error_frames = 0, continue with caller's handlers.
Stack overflow: test fails before anything is pushed. Thread switch: exchange swaps stack,
records, handlers, backtrace and specpdl together. Lisp sees the same records in the same
order.

## 4. Stages
Guards before/after every stage: lisp::primitives::tests::bytecode_calls_switch_frames_like_setup_frame,
compiled_catches_and_condition_cases_run_as_gnu_runs_them,
bytecode_stack_capacity_and_nested_reservations_match_gnu;
lisp::eval::tests::eval_02::bytecode_calls_lay_their_frames_on_one_stack_and_unwind_them_as_gnu_does
(ctl20e functions are dynamic-binding: exercises enter_dynamic_frame).
Measure each stage: lex-rec, dyn-rec, byte-calls, mapc-bc, mapc-bc-bc; guards cg-call-loop,
get-loop, cg-interp-dynamic, cg-interp-lexical, windows, interp-ignore; non-nop counts.

S1 dynamic return in one pass (~60 lines, vm.rs): return_to_bytecode_frame reads the fields,
calls out-of-line unbind_plain_to(interp, len) -> bool (unbind_quiet_innermost inlined),
continues inline. return_unbinding goes; unbind_plain stays for Bunbind. Non-plain entry ->
None after plain ones above it undone, as now. dyn-rec -50..-60. Tests existing:
primitives::tests::dynamic_binding_byte_code_arguments_as_funcall_lambda;
tests/cli_parity.rs::dynamic_binding_byte_code_binds_as_funcall_lambda;
bytecode::vm::tests::executes_dynamic_binding_fixture, executes_legacy_dynamic_argspec_fixture.
New: two formals, outer one watched (add-variable-watcher), called from byte code; throw from
200 levels of dynamic recursion caught at top, special's value after.

S2 callee's record is the last one (~200 lines; vm.rs + eval/variables.rs
InterpreterState::last_frame_debug_on_exit and a pop-last-funcall helper; leave_plain_funcall
stays for native/core.rs). backtrace_depth and op_error_frames leave BcFrame;
leave_frame/leave_frame_unwinding take index = backtrace len - 1 on entry. debug_assert! op
count 0 at frame entry; last record's arg pointer == caller's arg slots. lex -23, dyn -25,
byte-calls -23. Existing: compat_runtime_tests::compat_01::debugger_frames_enter_on_exit_and_at_the_next_call,
signals_enter_the_debugger_as_signal_or_quit_decides;
eval::tests::eval_05::backtrace_eval_selects_a_debugged_bytecode_callers_lexical_context.
New GNU-oracle: byte code catches Bnth/Baref/Bsetcar signal in own condition-case, then calls
a byte-code fn running mapbacktrace and marked by backtrace-debug; handler-bind around a
byte-code Baref error ((t aref 2) innermost); mapbacktrace inside unwind-protect cleanup of a
frame left by a signal.

S3 Breturn asks the header (~60 lines, vm.rs): saved_pc != 0 replaces last_above(frames_base)
in return_to_bytecode_frame, breturn!, run_fast return; run_entered_with's
bc_frames.len()==frames_base becomes debug_assert!; debug_assert! Bcall frame pc >= 1.
lex/dyn/byte-calls -7; mapc -3 (-11 above suspended frames). Existing:
primitives::tests::bytecode_calls_native_functions_nested_in_native_calls_like_funcall_subr;
compat_01::rust_callers_reach_byte_code_closures_given_named_or_aliased_as_funcall_does. New:
byte-code A (called from byte code) maps byte-code callback B that Bcalls C; mapbacktrace in
C; throw from C caught in A.

S4 GNU's saved_top (~150 lines, vm.rs): enter_frame takes saved top (function slot + 1 for
Bcall; real top for Rust entries); base leaves BcFrame; value written at top - 1. Verified
programs have stack_depth < 65535 (verify.rs:625) -> one-compare overflow test; bytecode_call
and run_frames keep two compares. Update BcHeader and TraceLispRoots docs. GC: outgoing args
traced only through the record (mirrors GNU's conservative marking above saved_top,
bytecode.c:417-423); nothing allocates between enter_frame and record push (vm.rs:925-932,
997-1004); bytecode_call pushes its record before maybe_gc. lex/dyn/byte-calls -11; mapc -5.
Update (assertions changed, not removed)
bytecode::vm::tests::bytecode_stack_footers_share_storage_and_preserve_borrowed_caller_arguments.
Existing: primitives::tests::suspended_bytecode_retains_operand_and_unwind_roots,
bytecode_live_fetch_preserves_changed_widths_operands_and_suspended_callers. New: weak table's
only key passed bc->bc, callee sets param nil, garbage-collect, (backtrace-frame) and table
count; same with callee parked in a Lisp thread; same with dynamic-binding callee.

S5 handlers record their frame (~100 lines, vm.rs): push_catch/push_condition_case store
bc_stack.frame; search uses frame == bc_stack.frame; return truncates own handlers without
popping registry entries (as now); handlers_len leaves BcFrame. -2. Existing:
bytecode::vm::tests::executes_condition_case_fixture, executes_unwind_protect_through_throw.
New: make-byte-code returning with a pushed handler then a signal in caller (pins Emaxx's
behaviour; GNU undefined); throw from callee past caller's catch to an outer one.

S6 program from caller's closure (~150 lines, optional): test-only execute builds a closure.
±2. Existing: bytecode::vm::tests::bytecode_reads_original_constants_after_vector_mutation,
bytecode_reads_original_constants_during_vector_mutation,
bytecode_constant_storage_does_not_retain_removed_values (anti_cheat.rs:1015-1019 requires
these names: do not rename), byte_code_calls_follow_the_callees_code_string_mutations,
primitives::tests::verified_bytecode_follows_changed_shared_and_collected_code_like_gnu,
suspended_bytecode_observes_live_constant_vector_mutation.

Left over vs GNU on lex-rec after S6 (~13): Bcall arm spills, backtrace-eval name test (7),
profiler test at record pop (3), head re-dispatches, base bookkeeping.

## 5. Traps
- No new lint suppressions. enter_dynamic_frame already has 8 params under an existing
  #[allow(clippy::too_many_arguments)] (vm.rs:972): leave that line untouched, new functions
  <= 7 params, delete dead code (e.g. Suspended::last_above after S3).
- H6: change assertions, never remove; S4 must still pass the overflow fixture.
- run_verified: 7 params, `left` first; do not add/reorder params or change VmFrames' fields
  (keep op_error_frames and frames_base there: exec_op and the error path use them).
- Record-keeping: dump_roots.rs:426 reason text, thread_context.rs exchange list,
  eval.rs:6491. Anti-cheat root inventory checks InterpreterState field types containing
  "Value" (anti_cheat.rs:1545-1580): don't name the new frame type with "Value".
- No statics/thread-locals (R4).
- B4 timed on the PGO build: confirm on a quiet host. Add docs/plans/d9-*.md when it lands.

## Not fixed (pre-existing, outside these behaviour-neutral stages)
- Debug-on-exit of a dynamic-binding byte-code callee called from byte code sees its argument
  still bound: GNU ((exit global)), Emaxx ((exit bound)) (probe d9/dbg-dyn.el); GNU's
  funcall_lambda unbinds before the Bcall arm's debugger runs.
- The exit debugger runs with lisp_eval_depth one higher than GNU's.
- A frame returning with a handler still pushed has it truncated without its registry entry
  popped.
