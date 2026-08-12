# Adopt GNU-style symbol objects with function cells to close the builtin-call performance gap

Companion issues: #10 (bytecode VM), #11 (startup image), #12 (source-interpreter
throughput).  This document covers the *call-path architecture* specifically:
why every emaxx function call costs ~10x what GNU pays, and a staged design to
fix it.  All measurements below were taken on the bytecode-VM branch with the
`bench/` suite in this repo, profiled with callgrind, against the GNU 30.2
oracle running identical oracle-compiled `.elc` kernels.

## 1. Problem statement and evidence

After ten rounds of profile-driven optimization (resolution caches, FNV hashing
everywhere, pooled stacks/frames, Bloom-filtered mutation watchers,
borrow-based walks, static-address facts caches), the bench suite sits at:

| kernel | ratio vs GNU | dominant cost |
|---|---|---|
| string, float, mapcar | **0.56x-0.85x (faster)** | — |
| sort | 1.6x | — |
| buffer, regex | 2.6-2.9x | mixed |
| loop, list, assq, vector | 3.3-3.9x | value representation |
| hash | 5.9x | per-call ceremony |
| fib | 6.8x | per-call ceremony |
| plist | 8.0x | per-call ceremony |

The remaining multiple on call-heavy kernels is **not** any single hot function
— callgrind shows it spread across the fixed ceremony every builtin call pays.
Per `(plist-get pl k)` call, emaxx executes ~700 instructions where GNU
executes ~60.  This is architectural, not a Rust-vs-C issue; the specific
mechanisms differ as follows.

### 1.1 What GNU does per call (with source references, GNU 30.2)

1. **Callee representation**: bytecode `Bcall2` pops a `Lisp_Object` — one
   tagged 8-byte machine word.
2. **Backtrace** — `record_in_backtrace` (`src/lisp.h:3805`): **five word
   stores**, and crucially `specpdl_ptr->bt.args = args` stores a *pointer to
   the args already on the VM stack*.  No copy, no allocation.  Pop is
   `specpdl_ptr--`.
3. **Resolution** — `funcall_general` (`src/eval.c:3035`):

   ```c
   fun = XSYMBOL (fun)->u.s.function   /* ONE field load */
   ```

   A GNU symbol is a struct with a function cell in it.  There is no string,
   no hash, no cache, no generation counter — redefinition (`fset`) *writes
   the same field* the call path reads, so the "cache" can never be stale.
4. **Dispatch** — `SUBRP (fun)` → `funcall_subr` (`src/eval.c:3136`): indirect
   call through a C function pointer stored in the subr object.  No name-based
   routing at all.
5. **Body** — `plist_get` (`src/fns.c:2606`): `EQ` is one word compare;
   `XCAR/XCDR` are pointer loads; `FOR_EACH_TAIL_SAFE` is Brent's cycle
   detection with counters, zero allocation.  Also note `Bnth` etc. are
   *dedicated opcodes* (`src/bytecode.c:1031`) that call `Fnth` directly.

### 1.2 What emaxx does per call today (with file references)

1. **Callee** is `Value::Symbol(SymbolName)` where `SymbolName` wraps
   `Rc<String>` — a symbol *is a string*.
2. **Resolution** — `resolve_symbol_call_with_frame_state`
   (`src/lisp/eval/core.rs`): scan env frames for function-shadowing state
   (now cached per-frame in `EnvFrame`), then FNV-hash the *name string* into
   `function_resolution_cache: HashMap<String, (u64, FunctionResolution)>`,
   compare a generation counter (`definition_generation`, bumped by
   `note_definition_changed()` — including conservatively by every
   `setcar`/`setcdr`!), clone the resolution.
3. **Backtrace** — `push_backtrace_frame` (`src/lisp/eval/variables.rs`):
   pop a pooled `Vec`, **clone every argument** into it (Rc refcount RMW
   each), write a ~100-byte `BacktraceFrame` struct (7 fields).
   `capture_current_backtrace_context` probes `special_variables_index` for
   `edebug-entered` (FNV set probe).  Pop drops all arg clones and recycles
   the Vec.
4. **Dispatch** — `call_with_facts` → `facts.module.call(...)` → e.g.
   `collections::call` (`src/lisp/primitives/dispatch/collections.rs`): a
   **string match over ~200 arm names** (length-switch + memcmp trees,
   ~100-200 instructions).
5. **Body**: every `car`/`cdr` is a `RefCell` borrow-flag check; every value
   copy is an `Rc` refcount RMW; every mutable borrow consults the
   cons-mutation watcher machinery (now Bloom-filtered); cycle detection
   allocates a `CycleGuard`.

### 1.3 Cost ranking

1. **No symbol objects** (string-keyed resolution + string-matched dispatch)
   — the largest and the subject of this issue.
2. **Value representation** (24-byte enum + Rc/RefCell vs tagged word) —
   already being migrated on main (`b912700` "Store cons fields in one shared
   cell", `53be89e` "Compact shared Lisp value payloads"); out of scope here
   but sequenced against it (section 6).
3. **Owned backtrace args** (per-call clone Vec vs GNU's borrowed pointer).
4. **Rc traffic vs deferred GC** — largely resolved by (2).

## 2. Design overview

Introduce **interned symbol objects**: a per-interpreter symbol table where
each symbol is a struct owning its function cell, plist, special-variable
flag, and dispatch metadata.  Function calls become: *symbol id → table row →
function cell load → direct handler call*.  Strings remain only for interning,
printing, and `intern`/`make-symbol` semantics.

The migration is staged so every stage lands green against the full validation
protocol (section 7) and is independently valuable.

### Stage A — symbol identity (`SymbolId`) without changing `Value`

**Goal**: give every interned name a stable dense id and a table row, while
`Value::Symbol(SymbolName)` stays the representation.

Data structures (new, in `src/lisp/eval.rs` or a new `src/lisp/symbols.rs`):

```rust
pub(crate) type SymbolId = u32;             // dense, 0 reserved

pub(crate) struct SymbolCell {
    name: SymbolName,                        // owning; keeps Rc alive forever
    function: Option<Value>,                 // THE function cell (fset/defalias target)
    subr: Option<SubrHandle>,                // Stage B: direct native handler
    facts: crate::lisp::primitives::NameFacts, // computed once at intern
    special: bool,                           // defvar'd (dynamic binding)
    plist_index: Option<usize>,              // into existing symbol_properties
}

pub(crate) struct SymbolTable {
    cells: Vec<SymbolCell>,
    // Interning maps. The by-address map is the hot path: interned
    // SymbolName Rcs are canonical (INTERNED_SYMBOL_NAMES dedups), and the
    // table holds a strong ref, so the address can never be reused.
    by_address: HashMap<usize, SymbolId, FnvBuildHasher>,
    by_name: HashMap<SharedText, SymbolId, FnvBuildHasher>, // cold path
}
```

Key invariant that makes `by_address` sound: `SymbolName::intern`
(`src/lisp/types.rs`) already dedups through the TLS `INTERNED_SYMBOL_NAMES`
set, so every interned occurrence of `plist-get` shares one `Rc<String>`.
`SymbolTable.cells[i].name` holds a strong reference, so `Rc::as_ptr` is a
stable key for the process lifetime.  Uninterned symbols (`make-symbol`, the
`\u{1F}` marker convention) do **not** get table rows — they keep today's
string paths (they cannot name builtins).

Wiring:

- `intern_symbol(name) -> SymbolId`: called from the reader / `intern` /
  `intern-soft` paths and lazily from resolution (miss → intern).
- **The function cell becomes the single source of truth** for
  `fset`/`defalias`/`defun`: `functions_index: HashMap<String, Value>` is
  replaced by (or becomes a compatibility view over) `SymbolCell.function`.
  Every current writer of `functions_index` (grep `functions_index` —
  `src/lisp/eval/bindings.rs`, `definitions.rs`) writes the cell instead.
  `fmakunbound` sets `None`.
- `definition_generation` stays for the interim source-form caches, but the
  *call path stops consulting it*: like GNU, redefinition writes the field
  the call reads.

**Resolution after Stage A** (`resolve_symbol_call`): env-shadow check
(existing per-frame cached verdict) → `by_address` probe (one usize hash —
effectively an identity hash) → `cells[id]` → if `function` is `Some(v)`
dispatch `v`, else if `facts.builtin` dispatch native, else void-function
error.  This removes: the string FNV hash, the String-key memcmp, the
generation compare, and the staleness class of bugs entirely.

Deliverable metric: `resolve_symbol_call_with_frame_state` (currently 4-7% of
call-heavy kernels) drops to a usize-keyed probe, and the `String`-keyed
`function_resolution_cache` is deleted.

### Stage B — direct native dispatch (`SubrHandle`)

**Goal**: eliminate the per-call string match inside `DispatchModule::call`
(`collections::call` alone is 8-10% of the plist kernel).

```rust
pub(crate) type SubrFn =
    fn(&mut Interpreter, &str, &[Value], &mut Env) -> Result<Value, LispError>;

pub(crate) struct SubrHandle {
    handler: SubrFn,          // the arm, extracted as a named fn
    // arity min/max could live here later for GNU-exact wrong-number-of-args
}
```

The churn is extracting arms from the giant `match name { ... }` blocks in
`src/lisp/primitives/dispatch/*.rs` into named `fn`s.  This does **not** need
to be big-bang:

1. Add a registration hook per module: `pub(super) fn subr(name: &str) ->
   Option<SubrFn>` next to the existing `handles(name)`.  A module can start
   by returning `None` for everything (fallback = today's `module.call` string
   match), then migrate its hottest arms one by one.  `SymbolCell.subr` is
   filled at intern time via `DispatchModule::for_name(name)` +
   `module.subr(name)`.
2. Migrate by profile order: `collections` (plist/alist/hash ops), `lists`,
   `numeric`, `predicates`, `strings`, `buffer_edit` cover every kernel-hot
   name.  The long tail can stay on the string match indefinitely —
   correctness is identical.
3. The existing `define_dispatch_modules!` macro
   (`src/lisp/primitives/dispatch.rs`) is the natural place to generate the
   plumbing; a per-module `define_subrs! { "plist-get" => plist_get, ... }`
   macro keeps arm inventories declarative, mirroring how
   `handles`/`prefer_builtin` inventories work today.

Call path after A+B: `id → cells[id] → subr.handler(interp, name, args, env)`
— a load and an indirect call, structurally identical to GNU's
`funcall_subr`.

Note the VM already has the same problem in miniature: GNU's dedicated opcodes
(`Bnth`, `Bpoint`, ...) route through `prim()` (`src/lisp/bytecode/vm.rs`),
which now caches `NameFacts` by static-string address.  With Stage B, `prim`
should instead resolve a `SubrHandle` once per name and cache *that* (same
static-address key), skipping `call_with_facts` entirely.

### Stage C — backtrace frames borrow args

**Goal**: remove the per-call arg-clone Vec (`push_backtrace_frame` +
`pop_backtrace_frame` = 5-8% of call-heavy kernels).

GNU stores a raw pointer into the live VM stack.  The safe-Rust equivalents,
in order of preference:

1. **Wait for the value migration (section 6)**: once `Value` is a tagged word
   (or even just a smaller Copy-dominant enum), cloning args is a memcpy and
   this stage becomes nearly free.  If Stage D lands first, C may be
   unnecessary.
2. **Length-only frames with lazy materialization**: the frame stores
   `args_len` and a *frame-relative* recipe; the full `args: Vec<Value>` is
   only materialized when something actually inspects the frame
   (`backtrace-frame`, debugger entry, `signal` handlers that walk frames).
   Inspection points are few and centralized (`variables.rs` backtrace
   accessors, `ert.rs`, error paths).  Risk: any inspection that happens
   *after* the call returns must not observe stale data — mitigated by
   materializing eagerly whenever `edebug_entered_active` or
   `debug-on-error`-class state is live (the existing
   `capture_current_backtrace_context` gate shows exactly how to make this
   cheap).
3. **`unsafe` GNU-style borrowed slice** with a scope-guard invariant (frame
   is pushed and popped inside the same call frame that owns the args slice).
   Documentable but last resort; emaxx has avoided `unsafe` in the
   interpreter so far and should keep it that way unless (1)/(2) prove
   insufficient.

### Stage D — value representation (coordinate, don't duplicate)

Main is already migrating (`b912700`, `53be89e`; see also #12 and
`docs/performance.md`).  The end state that closes the last multiple is
GNU-shaped: one tagged word, immediate fixnums, pointer+tag for heap objects.
This issue's contribution to that effort: **`Value::Symbol` should become
`Value::Symbol(SymbolId)`** (4 bytes; helps compaction) once Stage A's table
exists — printing/interning go through the table, and symbol equality becomes
an integer compare (today's `SharedText` ptr-eq fast path becomes exact
instead of best-effort).  This is the natural meeting point of the two work
streams and the reason to land Stage A soon.

## 3. Semantics that must be preserved (the tricky part)

Treat this list as the review checklist; each item has existing tests or
oracle behaviors that will catch violations:

1. **`defalias`/`fset` chains**: GNU resolves through symbol chains in
   `funcall_general` (`fun = XSYMBOL(fun)->u.s.function; if SYMBOLP(fun) fun
   = indirect_function(fun)`).  `SymbolCell.function = Value::Symbol(other)`
   must loop with cycle detection (`indirect_function` semantics,
   `cyclic-function-indirection` error).
2. **cl-flet / local function shadowing**: emaxx represents some local
   functions as values in lexical frames; `env_may_affect_function_resolution`
   (`src/lisp/eval/bindings.rs`, per-frame cached verdict) must keep gating
   the table fast path exactly as it gates the cache today.  When local
   context is present, fall back to the full `lookup_function` walk.
3. **`prefer_override` / `selected-window`**: the gate in
   `resolve_symbol_call_with_frame_state` encodes names where the native arm
   must win over Lisp definitions and vice versa; `NameFacts.prefer_override`
   moves into the row but the precedence logic must be preserved verbatim (it
   was tuned against oracle behavior).
4. **Autoloads**: `SymbolCell.function` holding an `(autoload FILE ...)` cons
   must keep today's load-then-rebind behavior (`call_function_value_inner`'s
   autoload arm).  `facts.autoloadable` (the preload table probe) also gates
   the DirectBuiltin verdict today — that gate moves to intern time.
5. **`unintern` / `obarray-remove` / multiple obarrays**: emaxx models
   obarrays as records with marker conventions (`OBARRAY_SYMBOL_MARKER`,
   `standard_obarray_contains_symbol` in `bindings.rs`).  `unintern` must
   *not* free the table row (values may still hold the symbol); it detaches
   the name from lookup maps only.  Symbols interned in non-standard obarrays
   need either separate tables or a per-row obarray tag — study
   `standard_obarray_contains_symbol` first.
6. **`symbol-with-pos`**: records wrapping symbols; all the
   `symbol_with_pos_parts` normalization
   (`src/lisp/primitives/interactive.rs`) sits *outside* the table and is
   unaffected, but any new symbol-identity compares must keep the
   `symbols-with-pos-enabled` env sensitivity.
7. **`makunbound`/`fmakunbound` and void-function errors** must produce
   byte-identical error data (`void-function` with the *original* symbol, not
   the alias target).
8. **Advice**: emaxx implements advice at the Lisp layer over
   `symbol-function`; as long as `fset` writes the cell, advice keeps
   working, but verify the `advice--cd*` reflection tests.
9. **Backtrace introspection**: `backtrace-frame`, `backtrace-eval`, edebug
   instrumentation, and ert failure backtraces read
   `BacktraceFrame.args`/`function` — Stage C must materialize identical
   values (the compat suite's edebug tests are the sentinel here).
10. **`definition_generation` consumers other than call resolution**: macro
    caches (`macros_name_counts`, `not_macro_generation`), the callsite-local
    `SourceFunctionCallCacheEntry` — these keep the generation mechanism;
    only the call path stops using it.  Note `setcar`/`setcdr` bump it
    conservatively (symbol plists are cons graphs); that stays.

## 4. What NOT to do (dead ends already explored)

- **Pointer-keyed resolution caches on `SymbolName` without a table**: sound
  only with a strong-ref registry — at which point you've built half the
  table; build the whole thing.
- **Per-callsite caches in `CachedProgram` constants**: the callee at
  `Op::Call` is argc-deep in the stack; pairing it with its pushing
  instruction needs stack simulation in the validator.  The symbol table
  makes this unnecessary.
- **Shrinking the string-match dispatch with perfect hashing**: still a
  string hash per call; fn pointers are strictly better and no harder.
- **Size hacks on `EnvFrame`**: a test asserts `size_of::<EnvFrame>() ==
  size_of::<usize>()` (`environment_frames_are_one_pointer_shallow_snapshots`)
  — per-frame caches belong *inside* the pointed-to allocation (see
  `FrameInner.function_verdict` for the pattern).

## 5. Expected wins (measured basis)

Callgrind per-call budget today (~700 Ir for a 2-arg builtin): resolution
~150, backtrace push/pop ~200, module string dispatch ~100-200,
`call_with_facts` plumbing ~50, arg clones/drops (representation-bound) ~150.

- Stage A: −120 or so (resolution → identity probe), and deletes a staleness
  bug class.
- Stage B: −100 to −200 (string match → indirect call).
- Stage C: −150 to −200 (no arg Vec).
- Together: ~700 → ~200 Ir; **plist should go from 8x to ~2.5-3x, hash from
  6x to ~2x, fib from ~7x to ~2-3x**.  Stage D (value migration) is what
  takes 2-3x territory to ~1x.

## 6. Sequencing against in-flight work

- **Value migration (main, in progress)**: Stage A is *independent* and
  should land first — it gives the migration `Value::Symbol(SymbolId)` as a
  compaction target.  Stage C should be *deferred* until it's clear whether
  the migration makes it moot.
- **Startup image (#11)**: the symbol table becomes part of the serialized
  image (rows + function cells).  Design the table so rows are index-stable
  and serializable (avoid raw pointers in `SymbolCell`; `SubrHandle`
  re-resolves by name at load).
- **Bytecode VM (#10)**: `prim()` and `Op::Call` are the first consumers of
  the fast path; the VM needs no structural change.

## 7. Validation protocol (non-negotiable, per stage)

1. `cargo test --release --lib`: the failure set must be **exactly** the
   known 36 environment-class failures (batch locale/network/desktop-lock
   class) — compare by name, not by count.
2. `bench/` suite: `emacs --batch -f batch-byte-compile
   bench/bench-kernels-*.el`, then `bench/driver.el` on both engines,
   min-of-5, **same-moment GNU control** (hosts migrate; never compare across
   runs).  Kernel outputs must stay byte-identical to the oracle.
3. Compat harness spot files (`compat/oracle.local.json` pointing at the GNU
   30.2 build): at minimum `subr-tests.el`, `fns-tests.el`, `eval-tests.el`,
   an edebug file, and one heavy library (`electric-tests.el`).  The
   harness's `SLOW` lines / `performance_regressions` list give the >2x
   report for free.
4. Callgrind before/after per stage on single-kernel drivers (pattern:
   `load .elc; run kernel; done`) — run from the repo root (relative `bench/`
   load) with a workload large enough that startup (~0.4G Ir) is noise.
5. No new `unsafe`, no behavioral flags: every stage must be on-by-default
   and GNU-faithful, or it doesn't merge.

## 8. Suggested landing order

1. **A1**: `SymbolTable` + interning + `by_address`; populate rows lazily;
   `SymbolCell.function` mirrors `functions_index` (double-write, table is
   read path) — lands with zero behavior change.
2. **A2**: flip writers to the cell, delete `functions_index` or reduce it to
   a debug view; delete `function_resolution_cache`.
3. **B1**: `subr` registration hook + migrate `collections`/`lists`/`numeric`
   hot arms; wire `prim()`.
4. **B2**: migrate remaining hot modules by profile.
5. **C** (only if Stage D hasn't landed): lazy backtrace args behind the
   existing debugger-active gates.
6. **D-handshake**: `Value::Symbol(SymbolId)` inside the value-migration
   branch.

Historical context: the per-call cost analysis, kernel ratios, and each prior
optimization round (with what worked and what was reverted) are in the git
history — `git log --grep="per-call"` and `git log --grep="dispatch"` find
the relevant commits, and `docs/performance.md` has further notes.
