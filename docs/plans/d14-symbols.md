# Symbols without their host text (D14, A3.6): inventory and staged plan

Written on 23 November 2026 before round 379, from a reading of the tree at
round 374 and callgrind runs on its release build. Paths beginning
`/home/user/` and `/tmp/` are those of the container it was written in; line
numbers are of that tree, and the expected counts are estimates. HANDOVER.md
records what each stage measured. Stages S0, S1 and S2 are round 379.

Read-only planning, 23 November 2026. Source read: `/home/user/emaxx-draft`,
whose checkout is `be6311e8` (fa8142d4 plus round 375's six window commits; no
symbol file differs between the two). Counts and file:line references are
`fa8142d4`'s (`git grep fa8142d4`). The checkout's line numbers are the same
except in the six files round 375 touched (eval.rs, eval/variables.rs,
eval/runtime.rs, dispatch/misc.rs, dispatch/display.rs, tty.rs), where they
move by 1 to about 50 lines. GNU source:
`/home/user/emacs-mirror/emacs/src`. Nothing was built. Measurements reuse the
prebuilt `target/prof-r374` (plain release) and GNU 30.2 under callgrind.
Probe files and outputs are in
`the scratch directory sym/`
(listed in the appendix).

## 0. The decision in one paragraph

A symbol becomes GNU's `struct Lisp_Symbol`: six words in a symbol block
(header with mark, flags and a two-bit `interned` field; name; value; function;
plist; next), its name the Lisp string it was given, its `interned` state a
field. There is no host text. Rust code names a well-known symbol by its object
(a `cached_symbol!` key, which is GNU's `Qfoo`: bound at construction, kept
across `unintern`). It reads a name only through explicit accessors over the
Lisp string (`name_text()`, `with_name_str(|s| ..)`). **`SymbolName::as_str()`
and `is_named()` are deleted, not redefined**, so that the compiler enumerates
every remaining reader and no site can silently start matching an uninterned
`nil` or `&rest`. Legitimately text-keyed after the change: the obarray (hash
and oblookup of the Lisp name's bytes, already GNU's), C-style lookups by Rust
text (`intern`/`intern_soft` of a literal: GNU's `intern ("...")`), printing,
`symbol-name`, messages, sorting by name, and the primitive dispatch keyed by a
**subr's** own name (D4's subject, never fed a symbol's text). One early,
contained stage (S1) stops building the marked text at `make-symbol` and builds
it only when a site asks. It removes about 880 of the probe's 2,262
instructions (to about 1,385; GNU 1,968). The later stages delete that
machinery with the rest of the host text. They are about faithfulness and three
real divergences, not about the probe.

## 1. What a symbol is in Emaxx today

**Handle.** `SymbolName(SymbolRef)` is the cell's address
(`src/lisp/types.rs:332-346`, `src/lisp/alloc/symbols.rs:690-745`). It is Copy.
Equality and Hash go by address (`types.rs:750-756`, `772-776`). **Ord goes by
host text, then address** (`types.rs:764-770`).

**Cell.** `SymbolCell` (`alloc/symbols.rs:79-99`), 64 bytes
(`alloc/symbols.rs:679-680`). Its layout is pinned by asserts (`:595-608`) and
mirrored by the image (`image_cell_words`, `:615-631`):

| offset | field | GNU (`lisp.h:821-866`, 48 bytes) |
|---|---|---|
| 0, 8 | `internal: SymbolText`: the host text, a `Box<str>` the cell owns or text of the image (`:29-75`) | none |
| 16 | `lisp_name` | `u.s.name` (8) |
| 24, 32 | `function`, `plist` | 24, 32 |
| 40 | `payload` (value, alias or counter) | `u.s.val` (16) |
| 48 | `next` (obarray bucket chain) | 40 |
| 56-63 | `flags` u8 (8 bits used), `image` bool, `state` u16 (all 16 bits used, `:101-151`), `mark` u32 epoch | header bitfield at 0 |

**Allocation.** Allocation is already alloc.c's: 21 cells a block
(`alloc.rs:78`, GNU's count), a free list through word 0 and a bump pointer
(`alloc/symbols.rs:751-800`). `sweep_symbols` (`:839-929`) `drop_in_place`s a
dead cell, and that drop frees its host text.

**The host text** is written at creation and never changes:
- An interned symbol of the initial obarray carries its name. This holds for
  `intern_with_lisp_name` (`types.rs:394-445`), `intern_initial_with`
  (`:613-632`) and `intern_image` (`:487-498`, the image's text, not copied).
- An uninterned symbol carries `name` + U+001F + a decimal serial
  (`make_uninterned`, `:657-674`; `make_uninterned_symbol_name`,
  `:2521-2550`). The serials come from three independent counters:
  `MAKE_SYMBOL_COUNTER` (`primitives.rs:273`, from 0) for `make-symbol`,
  dump-loaded and test-copied symbols; `READER_UNINTERNED_SYMBOL_COUNTER`
  (`reader.rs:96`, from 1) for `#:x`; and fixed ids in tests. Two different
  uninterned symbols can therefore carry the same host text (this was found by
  reading the code; no test or probe shows it).
- A private obarray's symbol carries `name` + U+001E + obarray id + `.` +
  serial (`make_fresh_obarray_symbol_name`, `types.rs:2552-2562`, made with
  `format!`; called from `completion.rs:322`).
- A name string changed later with `aset` leaves the host text stale. In GNU
  the symbol simply has the new name.

**Lisp name.** `lisp_name` is the string object given. `make-symbol` keeps
`args[0]` (`dispatch/misc.rs:916-922`), so `(eq (symbol-name s) str)` holds in
both editors for `make-symbol`, `intern` and a private `intern` (probe
`eq-name-*`: `(t t)` on both).

**Interned state.** It is only partly a field:
- `UNINTERNED` (state bit 32, `alloc/symbols.rs:110-112`, `218-224`) is set by
  `make-symbol` and by `unintern` (`types/symbols.rs:1140-1144`,
  `completion.rs:414`).
- "Interned in another obarray" has no bit. It is derived from the U+001E
  marker (`is_private_obarray_symbol(sym.as_str())`, 10 sites).
- "Initial obarray" means neither of the above.
- GNU has a two-bit `interned` field, set by `init_symbol`, `intern_sym` and
  `Funintern` (`alloc.c:3933-3947`, `lread.c:4910-4936`, `5090-5160`).

**Initial obarray.** Each editor has its own `Symbols` table
(`types/symbols.rs:939-950`): `StandardBuckets` running lread.c's algorithms
(`types/obarray.rs`). It hashes the Lisp name's bytes with `hash_string`, and
oblookup compares the current Lisp name's bytes and characters
(`obarray.rs:57-62`), so the obarray itself is already GNU's and needs no host
text. Lookups by Rust text use the text's bytes (`text_key`, `obarray.rs:131-140`).
In front of the buckets sits **`AddressNames`** (`types/symbols.rs:16-58`):
- It is a 4,096-slot cache keyed by a literal's address and length, checked by
  host-text equality (`types.rs:550-571`).
- Funintern in the initial obarray purges it by scanning every slot
  (`types/symbols.rs:52-57`).
- Measured, that scan costs **about 20,500 instructions per Funintern**
  (probe `intern-new`: 24,914 an iteration against GNU's 2,555).

**Private obarrays.** Their buckets are record slots (`RecordBuckets`), run by
the same algorithms. A miss allocates a new symbol with a fresh marked text
(`completion.rs:279-330`). That text costs **1,096 instructions a miss**
(`format!`; probe `priv-intern`: 5,132 an iteration against GNU's 2,424).

**Keys.** `KEY_NAMES` (`types/symbols.rs:181-937`) lists 750 names, all
interned by GNU at startup (probe `keys.el`: 0 missing). `cached_symbol!`
(`lisp/mod.rs:13-19`) yields a `SymbolKey`, a per-editor slot interned at the
end of construction (`eval.rs:9231`). That is GNU's `Qfoo`: after `unintern`
it still names the original object. A name not in `KEY_NAMES` is a compile
error (`SymbolKey::new` is const). The table allows at most 2,048 names
(`types/symbols.rs:1314-1334`).

**The dumper** writes a symbol record:
- a flags word, whose interned bits come from `UNINTERNED` or the marker
  (`pdumper/context.rs:2971-2981`);
- name, value, function, plist, and next (private symbols only);
- then a "Not GNU" key text (`:1519-1540`).

Image symbol blocks hold 8-word cells whose word 0 is the relocated key-text
pointer (`context.rs:1315-1335`). The loader handles each kind differently
(`load.rs:2093-2135`):
- An uninterned symbol is remade with a new serial.
- A private symbol is remade under its key.
- An initial-obarray symbol is mapped by its Lisp name (`intern_image`).

Images are tied to one build by their fingerprint (`pdumper/image.rs:32-60`),
so a format change needs no migration.

## 2. Every way Rust code observes the host text

Counts are at fa8142d4, non-test unless stated. The `.as_str()` count is
misleading: `as_str` is also `String`'s and `BuiltinRef`'s method (282 calls
in all, 261 outside tests). The host text is also read through `Display`,
`Debug`, `AsRef<str>`, `From<SymbolName> for String/SharedText`,
`into_string`, `Value::symbol_str()` (47) and `Value::as_symbol()` (83, via
`symbol_name_by_tag`, `types.rs:2157-2167`). An exact inventory needs the
compiler; that is stage S3.

There are three matching semantics today, and they decide behaviour:

- **T (text):** `is_named`, `as_str() == "..."`, `match sym.as_str()`,
  `symbol_str() == Some(..)` and `PropertyKey for &str` match any symbol whose
  host text is the plain name. That is the current interned symbol, every
  former holder of the name that `unintern` released (its host text stays
  plain), and image symbols. They never match an uninterned or private symbol
  (the marker).
- **I (intern by text):** a comparison against `Value::symbol("name")` matches
  only the symbol currently interned under the name.
- **K (key):** `cached_symbol!` matches only the original object, which is
  GNU's `EQ (sym, Qfoo)`.

### (a) Comparison with a literal

| form | sites | where (largest) |
|---|---|---|
| `SymbolName::is_named("lit")` | 138 (+20 in tests), 48 files | tty.rs 17, eval/variables.rs 12, coding.rs 11, processes.rs 9, display.rs 8, faces.rs 7, values.rs 6, keys.rs 6, eval/core.rs 6 |
| `is_named(var)` (text from a table) | 35 | gnutls.rs:619/634/1248/1731, faces.rs:420/615/842/873/1609, json.rs:68, image.rs:168, text.rs:95, file_io.rs:2187, eval/variables.rs:855/1011/1074/1921-1998 |
| `PropertyKey for &str` (`types.rs:1565-1581`) | about 50 literal-keyed lookups into the generic property functions (`strings.rs:1221-1479`, `buffer.rs:2596`) plus `PropertyName == "text"` (`types.rs:1500-1515`) | textprop.rs, buffer_edit.rs, fields.rs, insdel.rs, indent_motion.rs |
| `.as_str() ==/!= "lit"` | 7 | bindings.rs:355, runtime.rs:3831, symbol_cells.rs:314, variables.rs:3878/3909-3910, filelock.rs:722 |
| `.as_str().starts_with(..)` | 6 | vm.rs:1056/4468 (the `&` prefilter of the dynamic lambda list), types.rs:454 (keyword at intern), faces.rs:435, display.rs:2388, completion.rs:2174 |
| `symbol_str() ==/!= Some("lit")` | 6 | treesit.rs:369/374, ... |
| `match sym.as_str() { "a" => .. }` | about 10 of 22 `match x.as_str()` | native_comp/loader.rs:1330 (`&rest`/`&optional`), types/symbols.rs:1226 (nil/t), dump_roots.rs:1406, completion.rs:102/796, tty.rs:2567/2577 |
| `match x.symbol_str()` / `match x.as_symbol()?` | 4 / 5 | |

The 106 distinct `is_named` literal names break down as follows: 30 are keys
already, 87 are DEFSYMs, and 14 are neither. Of those 14, 8 are only in tests;
the runtime ones are `current-local-map`, `declare`, `defalias`,
`initial-window-system`, `region` and `ert-test-skipped`.

**Today, for an uninterned or private symbol with the same visible name,**
every (a) site answers *false*, as GNU's `EQ (sym, Qfoo)` does. The marker is
load-bearing: see the probe table below. **After `unintern` of a DEFSYM'd
name,** T sites answer *true* for both the old and the new symbol, where GNU
answers only for the old one.

### (b) Dispatch tables keyed by name text

- **The primitive dispatch** has about 1,474 string arms in
  `primitives/dispatch/*`, plus `direct_primitive` (`dispatch.rs:87-169`),
  `compute_name_facts`/`name_facts` (`:299-403`) and
  `DispatchModule::for_name`. It is keyed by the **subr's own name**
  (`BuiltinRef`). It is reached only after the calling symbol's function cell
  yields a subr (`eval/core.rs:1420-1446`, `2330-2360`, `2435-2452`;
  `bytecode/vm.rs:1355-1385`, which uses static names). It is **not reachable
  with a symbol's host text**:
  - `(funcall (make-symbol "car") '(1))` signals void-function in both
    editors.
  - An uninterned symbol whose function cell holds `#<subr car>` calls `car`,
    and its arity error names `#<subr car>`, in both (probe p6).
  - Two places still use the *calling symbol's* text:
    - `core.rs:1427` uses it as `original_name`, for backtrace and error
      naming.
    - **`core.rs:2420-2424`** makes `Value::BuiltinFunc(name)` from the call
      text when an autoload's file is missing. With a visible-name `as_str()`,
      an uninterned `car` whose autoload file is missing would call the
      builtin. This must be converted to compare the symbol, not its text.
- **Special forms** go through `NativeForm::for_name` (`core.rs:150-190`, 21
  forms), set from the subr's name in `NameFacts` (`dispatch.rs:357`). They
  are reached only through the function cell. `(eval (list (make-symbol "if")
  t 1 2))` signals void-function in both editors.
- **Keyword and option parsing** works through three forms:
  - `is_named(":kw")`: 9 sites.
  - `Value::symbol(":kw")` followed by `eq`: 72 sites. An example is
    make-hash-table (`dispatch/misc_keymaps.rs:739-757`), whose hash test
    descriptor also goes by name.
  - Faces keyword lists (`faces.rs:1609`), and the gnutls and process plists.

  An uninterned `:test` gives "Invalid argument list" in both editors. After
  `unintern` of `:test` or `:size`, GNU accepts only the original and Emaxx
  only the new symbol (semantics I).

### (c) Construction by text

| form | sites | names |
|---|---|---|
| `Value::symbol("lit")` | 1,005 (+533 tests) | 499: 91 keys, 355 DEFSYMs, 137 neither |
| `Value::Symbol("lit".into())` | 404 (+1,226 tests) | |
| `cached_symbol!("lit")` | 1,498 (+151 tests) | 702 |
| `intern_str(..)` | 104 | |
| `SymbolName::from("lit")`, `From<String>` | 5, plus runtime-built names | |

These look up the obarray by text: the address cache first, then oblookup of
the text's bytes. They need the host text only to validate a cache hit
(`types.rs:556`). Their semantics are I: the currently interned symbol, as C's
`intern ("...")`. For DEFSYM names GNU uses `Qfoo` (K). Where a constructed
symbol is *compared* (`eq(x, Value::symbol(..))`, 20 sites, plus the
make-hash-table pattern), it belongs to (a)/(b).

### (d) Uses that need the visible name as a string

- `visible_symbol_name(..)` has 33 sites, 12 of them on `sym.as_str()`: json.rs:1080,
  keys.rs:215 (event modifier parsing), buffer_meta.rs:875/1908,
  files_process.rs:2743, processes.rs:4853, dispatch/misc.rs:870/1263,
  values.rs:711-712, completion.rs:129, `render_error_symbol`
  (`types.rs:2598-2620`), and `Display for Value` (`types.rs:3400`).
- `name_text()` (29 sites) already reads the Lisp name. Its only host-text use
  is for a non-string name (`types.rs:706-710`).
- Printing (`print/object.rs:1067-1120`) already prints the Lisp name. It uses
  the marker only to decide `#:` (`:1085-1089`). GNU prints `#:` for anything
  not in the initial obarray, so a private symbol prints `#:` too (probe
  `private-print` agrees on both).
- `symbol-name` returns `lisp_name`.
- Messages, treesit language names (`require_treesit_language(language.as_symbol()?)`,
  treesit.rs:314/863; GNU reads `SYMBOL_NAME`), keymap and ccl names, and
  thread names go through `as_symbol()?` (about 40 sites).
- Hashing is by address, as GNU's `sxhash` of a symbol is: probe `sxhash`
  gives `(nil nil)` on both.
- `Ord` goes by host text. No runtime user was found; the only `sort` of
  symbol names is a test (`symbol_cells.rs:670`).

### (e) Uses that depend on the marker itself

- `is_uninterned_symbol` (2: coding.rs:787) and `is_private_obarray_symbol`
  (10: print/object.rs:1087, pdumper context.rs:1520/2976, load.rs:2113,
  eval.rs:4119 (test copier), coding.rs:788/873, image_install.rs:88, and
  `is_keyword` at types.rs:715).
- `is_visible_symbol_name` (9): the mapatoms filter (bindings.rs:1060),
  `standard_obarray_contains_object` (bindings.rs:1096), the plain/marked tests
  of `find_interned`/`find_initial`/`intern_initial_with` (types.rs:561-622),
  `Symbols::find`/`find_initial` (types/symbols.rs:1097-1113) and load.rs:2123.
- `find_by_host_text` (5, eval/coding.rs: `CodingRef`/`CharsetRef for str`)
  and `interned_cached` (4, eval/bindings.rs:93/288/880).
- The debug assertions in `intern_with_lisp_name` and `find` (types.rs:405-423,
  601-604).
- **`intern_with_lisp_name`'s marker sniffing** (types.rs:399-423). A Lisp
  name containing U+001E or U+001F is never interned: each `intern` makes a
  fresh uninterned symbol. In a debug or test build the debug assertion fires
  instead (found by reading the code; not run). This is one of the
  divergences below.
- The dump's key texts and the three serial counters.
- **Identity carried as text (e2).** About 35 calls pass `symbol.as_str()` into
  by-name methods (84 methods take `name: &str`):
  - `let_shadows_buffer_binding` ×5 (dispatch/misc.rs:1186/1217,
    buffer_meta.rs:869, variables.rs:1335/2264; defined variables.rs:852-859);
  - `find_symbol_value_resolved` ×3, `set_function_binding` ×2,
    `default_toplevel_value` ×2, `builtin_default_value`, `forwarded_c_value`,
    `per_buffer_predicate`, `clear_alias_by_name`, and others.

  For these, symbol identity *is* text identity. That is correct only as long
  as host texts are unique, which the separate counters do not guarantee (§1).
  A visible-name `as_str()` would merge every same-named pair.

### (f) Tests

- `is_named` in tests: 20 sites; reader.rs:2880-2891 asserts an uninterned
  `#:a` is not `a` or `:a`.
- `make_uninterned(.., id)` with fixed ids: about 17 sites (native_comp
  runtime tests ×9, symbol_cells.rs:633-634, eval/core.rs:3105,
  types.rs:4355/4383, ownership_tests.rs:15, pdumper/tests.rs:719, json.rs:1376,
  variables.rs:4519).
- `visible_symbol_name(symbol.as_str())` in test bodies: runtime.rs:12424,
  reader.rs:2887.
- Census and reclaim tests: types.rs:4343-4372.
- The pdumper tests: 9 `is_named` sites and layout checks.

No test asserts the marker's exact format.

### Probes: where the marker is load-bearing and where it is not enough

Run with `-Q --batch -l` on GNU and on `prof-r374`.

These answers are identical in both editors. Each is a case a visible-name
`as_str()` would break if a site were missed:

| probe | GNU = Emaxx |
|---|---|
| `(funcall (make-symbol "car") '(1))` | `(void-function car)` |
| `(let ((s (make-symbol "nil"))) (list (null s) (eq s nil) (symbol-name s) (if s 1 2) (not s) (and s 'yes) (consp s) (listp s) (symbolp s) (booleanp s)))` | `(nil nil "nil" 1 nil yes nil nil t nil)` |
| `(setq zzx (make-symbol "t"))` then `(list (if zzx 'a 'b) (eq zzx t) (when zzx 1) (or zzx 5))` | `(a nil 1 t)` |
| uninterned `:test` / `:size` to make-hash-table | `(error "Invalid argument list" :test)` |
| `(intern "foo" (obarray-make))` vs `'foo`: `(eq f 'foo) (memq f '(foo bar)) (memq 'foo (list f)) (symbol-name f) (equal f 'foo) (string= f 'foo)` | `(nil nil nil "foo" nil t)` |
| uninterned `if`/`quote`/`progn`/`setq` as a form's head | `(void-function if)` ... |
| `(funcall (list (make-symbol "lambda") '(x) 'x) 3)` | `(invalid-function (lambda (x) x))` |
| a lambda list `(a #:&rest b)` called with 2 / `(a #:&optional b)` with 3 | wrong-number-of-arguments / `(1 3)` |
| `(eval `(cond (,(make-symbol "t") 'yes) (t 'no)) t)` | `(void-variable t)` |
| print-gensym: `#:foo`, `(#:foo #:foo)`, `(#1=#:foo #1#)`, `#:`, `#:a\ b`, `#:\1`, `#:nil`; private `#:foo` | same |
| uninterned `face` as a text property; `:a` in plist-get | `(nil bold)`, `(nil nil 3)` |
| `(fset s (symbol-function 'car))` for uninterned `s`: call, arity error | `1`, `(wrong-number-of-arguments #<subr car> 0)` |

These answers differ, and the target is GNU's:

| probe (files p2, p3, p4-N) | GNU | Emaxx | cause |
|---|---|---|---|
| `(let* ((n (string ?a #x1f ?5))) (eq (intern n) (intern n)))`, likewise for U+001E and `?b` | `t`, intern-soft finds it, prints `a\5`, mapatoms sees it | `nil`, nil, `#:a\5`, nil | marker sniffing in `intern_with_lisp_name` (e) |
| `:size` uninterned then re-interned: make-hash-table with new / old | new rejected, old accepted | new accepted, old rejected | I semantics (`Value::symbol(":size")`) |
| same for `:test` | `"Invalid argument list"`, `equal` | `equal`, `"Invalid argument list"` | I |
| `invisible` re-interned: `invisible-p` with new / old | `nil`, `t` | `t`, `nil` | I |
| `&rest` re-interned, in a lambda list called with 3 | new `(1 3)`, old `(1 (2 3))` | both `(1 (2 3))` | T (`core.rs:2531`) |
| `arith-error` re-interned, `(/ 1 0)` under condition-case `error` | `(nil nil (t nil))` | `peculiar error` escapes the handler | I or T |
| `equal` re-interned: `:test old` | `equal` | error "Invalid hash table test" | I |
| `function` re-interned: printing; `fill-column`: boundp, local-if-set | same on both | same | (K already) |

A tenth case, `aset` on a DEFSYM's pure name, makes GNU crash. It has no
target and is listed for completeness only.

## 3. Where the cost of `make-symbol` goes today

The probe is `tools/perf/probes/make-symbol.el` at 200,000:600,000
iterations. Plain release `prof-r374` gives **2,262.1** instructions an
iteration and GNU **1,967.9**. The numbers are callgrind self and inclusive
costs per iteration, taken as the difference between the two runs
(`sym/cg2.{e,g}.{200000,600000}`). Collections are included: 11 per 400,000
symbols in *both* editors, the same trigger (48 bytes a symbol,
`eval.rs:4707`).

| piece | Emaxx | GNU |
|---|---|---|
| VM loop, the call to the subr | `run_verified` self 315.2 | `exec_byte_code` 291.0 + `funcall_subr` 39.0 |
| **the make-symbol arm, inclusive** | **853.7** | Fmake_symbol 29.2 + init_symbol 14.0 + block malloc ~60 |
| -- `lifted` self (need_args, the counter's atomic add, the result) | 45.0 | |
| -- `string_text(&args[0])`: a Rust copy of the name, used only as the marked text's visible part | 194.0 (`string_like` 75.0 self: borrow, `text_parts`, props clone, drop; `decode_bytes` 48.0 self + its `to_owned` allocation; `string_text` 30.0) | none |
| -- `make_uninterned_symbol_name`: `name\x1F<serial>` | **493.5** (268.0 self: the decimal digits and the push of each `char`; 209.5 `HostAllocator::alloc`, then malloc, `_int_malloc` and `malloc_consolidate`; 16.0 memcpy) | none |
| -- `allocate_with_text` | 79.2 (44.7 self: 8 words and `note_lisp_allocation`; ~31 block registration amortized over 21 cells) | in Fmake_symbol |
| -- `make_uninterned` self (`into_boxed_str`, mark uninterned) | 27.0 | init_symbol |
| -- free of the copied name | 15.0 | none |
| **collector, inclusive** | **1,092.3**: marking 768 (`trace_pending` 674 self, `mark_names` 60, ...); **the sweep's free of each dead symbol's host text 153.9** (1.01 frees a symbol); block unregister 44.7; vector growth 44; sweep self ~65 | ~1,535: `process_mark_stack` 1,026, `mark_char_table` 166, pdumper marks 247, sweeps ~30 |

Allocator calls per `make-symbol`: 2.02 `HostAllocator::alloc` and 2.03
`dealloc`, of which 1.01 `malloc` and 1.02 `free` reach libc. The copy of the
name is the second pair (served by the host allocator's small path). The
marked text is the libc pair, freed by the sweep. GNU allocates no heap memory
per symbol; it uses blocks only.

**Stage S1 removes:**

| item | instructions |
|---|---|
| `string_text` | 194 |
| `make_uninterned_symbol_name` | 493.5 |
| the copied name's free | 15 |
| most of `make_uninterned`'s self, and the atomic add | about 20 |
| the sweep's free | 154 |
| **total** | **about 877** |

Expected result: **about 1,385**, within ±40, since forced texts and code
layout move a little.

**S7 removes the rest of the host-text work.** The cell becomes 48 bytes. The
two words are no longer written at allocation. The sweep's drop and pending
check go away. The blocks touch 25% less memory. That is an estimated further
−10 to −30, to **about 1,360**.

**Untouched by this item:** the VM (315 against 330), the marking (768 against
GNU's ~1,500, cheaper because Emaxx's image blocks are skipped), and block
registration (31 + 45 amortized, a BTreeMap, comparable to GNU's
`mem_insert`/`lisp_malloc`).

Two neighbours of the same structure were measured at 20,000:60,000
(`sym/cg3.*`):
- **`intern-new`** (miss, intern, unintern in the initial obarray): Emaxx
  24,914, GNU 2,555. `unintern_standard_symbol` alone costs 20,711, the
  `AddressNames` purge. S2 removes it, leaving about 4,400.
- **`priv-intern`** (the same in an `obarray-make` table): Emaxx 5,132, GNU
  2,424. `make_fresh_obarray_symbol_name` costs 1,096, `string_text` ×2 costs
  388, and `apply_symbol_shorthands` ×2 costs 596 against GNU's
  `oblookup_considering_shorthand` 60. S2 (lazy private text) leaves about
  3,900, and S7 a few hundred less.

## 4. Target design (GNU's, structure included)

1. **The cell** is six words in GNU's order:
   - word 0: the header (`mark` u32 epoch, `state` u16, `flags` u8, and a
     `bits` u8 holding `image` and the two-bit `interned`);
   - then `name`, `val` (the current payload union), `function`, `plist`,
     `next`.

   48 bytes, `SYMBOLS_PER_BLOCK` 21 as now. The free list links through
   `next`, as `symbol_free_list->u.s.next` does, so the header's mark word
   stays readable (`FREE_MARK`) in free cells.
2. **Fmake_symbol** does `CHECK_STRING (name)` by kind, takes a cell from the
   block or free list, and runs `init_symbol`: name = the string,
   `interned = SYMBOL_UNINTERNED`. There is no counter, no copy and no malloc.
   `(eq (symbol-name s) str)` holds, as today.
3. **intern_driver** is `Fmake_symbol (string)` followed by `intern_sym`, which
   sets `INTERNED_IN_INITIAL_OBARRAY` or `INTERNED` and, for a keyword of the
   initial obarray (name byte 0 == `:`), makes it constant and self-valued.
   **Funintern** sets `UNINTERNED` and unlinks the bucket, for any obarray.
   Private obarrays use the same object kind and need no serial.
4. **Naming a well-known symbol** uses a key: `cached_symbol!("foo")` is
   `Qfoo`. Comparisons become `sym.is(K)` on a bare `SymbolName`, or
   `value.is_key(K)`, which follows `symbols-with-pos-enabled` as `EQ` does.
   Only names GNU interns at startup may become keys: a key's symbol is
   interned at construction, so any other name would show up in `intern-soft`
   and `mapatoms`. A name GNU's C never names, such as `ert-test-skipped`,
   uses `SymbolName::find_initial("..") == Some(sym)`, the I semantics of
   `(eq sym (intern-soft ".."))`.
5. **Remaining text keys, which are legitimate:**
   - the obarray (unchanged);
   - lookups by Rust text (`SymbolName::find`/`find_initial`/`intern_str`,
     GNU's `intern`/`intern_soft` of a C string), with no marker logic;
   - `symbol-name`, printing, messages and sorting, through the Lisp name;
   - the subr-name dispatch.
6. **Must stop being text-keyed:**
   - every (a) site;
   - (b)'s keyword parsing and `core.rs:2420-2424`;
   - every (e2) by-name call fed a symbol's text;
   - `PropertyKey for &str`;
   - `Ord`, which becomes Lisp name bytes then address, or goes if nothing
     needs it;
   - `Display`/`Debug`, which become the Lisp name, `#:` for a non-initial
     symbol in Debug.
7. **`as_str()` in the target: none, for any symbol.**
   - Reason 1: a `&str` with the symbol's lifetime cannot soundly borrow a Lisp
     string, which `aset` can rewrite and resize.
   - Reason 2: a redefinition to "the visible name" would turn every
     unconverted T site into a silent match for uninterned `nil`, `t`,
     `&rest`, `lambda`, `:test`, and so on.
   - Reason 3: GNU has no such accessor; C reads `SDATA (SYMBOL_NAME (s))`
     where it means text.

   The replacements are:
   - `name()` (`SYMBOL_NAME`, the string object);
   - `name_text()` (an owned copy, for messages);
   - `with_name_str(|s| ..)` (a scoped borrow with `with_string_text`'s rules:
     in place for ASCII and valid UTF-8, a decoded copy otherwise; the closure
     runs no Lisp);
   - `name_first_byte()` / `name_bytes_eq(b"..")` for the few byte tests GNU
     itself makes (`SREF (name, 0) == ':'` in `intern_sym`, nil and t at
     intern).

   `is_named`, `symbol_str()` and `as_symbol()` (as `&str`) are deleted. A
   `CHECK_SYMBOL` becomes `as_symbol_object() -> Result<SymbolName>`.
8. **The primitive dispatch** stays keyed by the subr's own static name until
   D4 gives subrs their function pointers. It must never take a symbol's text:
   the review rule is "a `primitives::call`/`name_facts`/`is_builtin` argument
   is a literal or a subr's name".
9. **The dump** writes 6-word symbol cells without a key-text section. The
   loader maps builtin symbols by Lisp name (`intern_image(name)`), makes an
   uninterned record `Fmake_symbol (name)`, and makes a private record a
   symbol with `INTERNED` that its obarray's buckets hold.
10. **The address cache** (`AddressNames`) is a GNU-absent identity cache
    (A2.3). In S2 it stops needing a purge: a hit is checked against the
    `interned` field. In S7 it checks the Lisp name's bytes instead of the
    host text. It can be deleted once construction by text of C-side names
    becomes keys (S8).

## 5. Stages

Each stage builds, passes the gate, and is behaviour-neutral except where
stated. Each targets ≤ ~600 hand-written lines; mechanical conversions are
grouped by file family.

**Every stage measures:**
- the probes: make-symbol, intern-new and priv-intern (add the two templates
  from `sym/` to `tools/perf/probes`), plus a new read-gensym probe (`read` of
  `#:x`);
- the guards: lex-rec, dyn-rec, interp-ignore, mapatoms-bc, mapatoms-closure,
  mapc-bc, princ-interp, windows, the native-* set, call-loop/get-loop,
  cg-interp-*;
- startup (`--eval '(princ 1)'`, 128.4M).

The probe tables p1, p3, p6 and p4-1..8 (appendix) are the Lisp-level
fixture every stage keeps green.

| stage | content | make-symbol | other probes |
|---|---|---|---|
| S0 | guard tests only | 2,262 | |
| S1 | make-symbol without host text | **~1,385** | read-gensym lower |
| S2 | `interned` field; private lazy; O(1) Funintern | ~1,385 | intern-new 24,914 → ~4,400; priv-intern 5,132 → ~3,900 |
| S3 | rename to `host_text()` (inventory) | ~1,385 | none |
| S4 | identity carried as text → symbols | ~1,385 | none |
| S5a-e | comparisons → keys | ~1,385 | small ±; check guards |
| S6 | name readers → Lisp name | ~1,385 | princ-interp ±0 |
| S7 | delete host text; 48-byte cell; dump | **~1,360** | intern-new, priv-intern a few hundred lower |
| S8 (optional) | construction by text → keys; drop address cache | ~1,360 | startup, interp-ignore may fall |

GNU is 1,968 for make-symbol, 2,555 for intern-new and 2,424 for priv-intern.

### S0. Guard tests (tests only; may be folded into S1)

Library tests that evaluate the probe forms and assert GNU 30.2's recorded
answers. The repository already records oracle answers this way in its print
tests.
- **Uninterned and private same-name cases** (probe p1, p3 and p6 rows that
  agree today): nil, t, `&rest`, `&optional`, `lambda`, `macro`, `autoload`,
  `interactive`, `declare`, `quote`/`function` printing, `if`/`progn`/`setq`
  heads, `:test`/`:size` keywords, `face`/`invisible` properties, `equal` as a
  hash test, `error`/`quit` condition symbols, subr through an uninterned
  alias, print-gensym, `memq`/`eq` against a private `foo`, `keywordp` of a
  private `:k`.
- **Byte-compiled variants** of the lambda-list cases (dynamic and lexical),
  so that vm.rs and loader.rs are covered as well as eval.

About 250 test lines. No count change.

### S1. `make-symbol` without the host text at creation (the contained stage)

What moves:
- **`alloc/symbols.rs`.** `SymbolText` keeps its two words but gets a *pending*
  state (a null text pointer):
  - `get() -> Option<&str>`;
  - `install(String) -> &str`, written once through `Cell`s under the
    one-Lisp-thread rule of `ProcessTable` (`types.rs:348-373`);
  - `Drop` skips the pending state.

  The layout asserts are unchanged. About 50 lines.
- **`types.rs`.**
  - `SymbolName::make_symbol(name: Value)`, Fmake_symbol's constructor:
    allocate pending and mark uninterned.
  - `as_str()` returns the text or, cold, builds it:
    `make_uninterned_symbol_name(<Lisp name read like oblookup's unguarded
    read, then decode_bytes>, next_make_symbol_id())`. Call the existing
    `next_make_symbol_id` and do **not** edit its `cfg_attr(..allow(dead_code))`
    line (H5).
  - **`is_named(text)` answers false for a pending text without building it.**
    This is sound: a pending text has never been observed, and the marked text
    it would become equals no literal.
  - `symbol_name_by_tag` (`:2157-2167`) goes through the same accessor, with
    no direct `cell.internal` read.
  - About 60 lines.
- **`dispatch/misc.rs:916-922`.** The arm checks `Kind::StringObject` (else
  `wrong_type_argument("stringp", ..)`, as `string_text` did) and calls
  `make_symbol(args[0])`.
- **`bytecode/vm.rs:1056`, `:4468`.** The `starts_with('&')` prefilter reads
  only a *built* text (`host_text_if_built()`), so a gensym parameter of
  dynamic byte code is not forced. A pending symbol is never the `&rest` key.
- **Optional, same constructor:** the reader's `#:x` (`reader.rs:2270-2281`,
  `2456-2480`) and the loader's uninterned records (`load.rs:2105-2110`).

About 150 hand-written lines.

**Behaviour.** Neutral, except in one edge case. A site that asks for the text
of an uninterned symbol whose name string was changed since `make-symbol` sees
the current name; today it sees the old one. That moves toward GNU, where the
name is the string.

**Risks.**
- Any direct reader of `cell.internal` that bypasses the accessor (grep
  `\.internal\b`).
- Threads: Lisp threads run one at a time. No host thread reads symbols
  (`processes.rs:1160/4385`, `file_system.rs:456` handle bytes only); the
  review checks this.
- A `RefCell` borrow panic if the builder used `borrow()` on a name string
  being mutated. Use the unguarded byte read.
- `Ord` between same-named uninterned symbols now follows forcing order. No
  runtime user exists; check again in review.
- The dumper forces texts at dump time, which is fine.

**Tests.**
- A fresh `make-symbol` has a pending text: a test accessor.
- Two same-named gensyms stay distinct after forcing.
- `(eq (symbol-name s) str)`.
- A name `aset` before the first request.
- `uninterned_symbols_remain_reclaimable` with pending symbols.
- S0's fixture.

**Measure.** make-symbol ~1,385. dyn-rec must not rise (vm.rs).

**Thrown away later?** Yes. S7 deletes `SymbolText`, the pending state and the
builder (about 80 lines). The *gain* is not thrown away: S7 removes the same
allocations for good. S1 closes D14's excess in one round while S3-S6 take
several.

### S2. `interned` as GNU's field; private symbols lazy; Funintern without the cache purge

- **`alloc/symbols.rs`.** `image: bool` becomes `bits: Cell<u8>`: bit 0 is the
  image flag, bits 1-2 are `interned`, in GNU's encoding (0 uninterned,
  1 interned, 2 in the initial obarray). Both `state` and `flags` are full.
  `is_uninterned()` reads the field. The `UNINTERNED` state bit goes. The
  image cell's byte 57 keeps bit 0 (`image_cell_words` `1 << 8`), and the
  loader sets the interned bits.
- **Setters:**
  - `file_initial` (intern_sym, initial);
  - the private bucket insert (`completion.rs:322-330`, and the loader's
    private path): `INTERNED`;
  - `forget` and private unintern (`types/symbols.rs:1140`,
    `completion.rs:414`): `UNINTERNED`;
  - `make_symbol`/`new_uninterned`: `UNINTERNED`.

  `obarray-clear` leaves the field as it is, as GNU's does.
- **Convert to the field:**
  - the 10 `is_private_obarray_symbol(sym.as_str())` and 2
    `is_uninterned_symbol` sites;
  - `is_keyword` (`types.rs:712-722`: `INTERNED_IN_INITIAL_OBARRAY` and name
    byte 0 `:`, as `Fkeywordp`);
  - `print_symbol`'s `#:` (`!INITIAL`, as print.c);
  - `symbol_flags_word` (`context.rs:2974-2981`) and `symbol_of_record`
    (`load.rs:2112-2121`, from the record's own bits);
  - `standard_obarray_contains_object` (`bindings.rs:1094-1100`),
    image_install.rs:88, coding.rs:787-788/873, and the test copier
    (eval.rs:4113-4122).
- **Private symbols get a pending text** too: the pending word carries the
  obarray id, and the builder appends the marker, id and serial. This saves
  the 1,096-instruction `format!` per miss.
- **`AddressNames`.** A hit also requires `interned == INITIAL`, so `forget`
  (the 4,096-slot scan) is deleted. Funintern drops from ~20,700 instructions
  to GNU's order.

About 350 hand-written lines.

**Behaviour.** Neutral: the field gives the answers the marker gave. A private
symbol uninterned from its obarray is `UNINTERNED` today too.

**Risks.**
- The dump's interned bits for private symbols, including
  `restore_dumped_order` and private records loaded with their buckets.
- An image symbol's byte 57.
- Any check of the old `UNINTERNED` bit outside alloc/symbols.rs.

**Tests.**
- The field after make-symbol, intern, private intern, unintern from each
  obarray, and dump and load.
- `keywordp` of a private `:k`, uninterned `:k`, re-interned `:k`.
- print-gensym of private and uninterned-from-private symbols.
- intern-new's count. The census of `AddressNames` slots no longer matters.

**Measure.** intern-new ~4,400, priv-intern ~3,900, make-symbol unchanged.

### S3. One name for the host text (inventory stage, mechanical)

- Rename `SymbolName::as_str` to `host_text`, and
  `Value::symbol_str`/`as_symbol` to `symbol_host_text`/`as_symbol_host_text`.
- Delete `AsRef<str>`, `Display`, `From<SymbolName>/From<&SymbolName> for
  String`, `From<SymbolName> for SharedText` and `into_string`, and write
  `host_text()` where the compiler asks.
- Turn the pure `SYMBOLP` uses (`as_symbol().is_ok()`, `as_symbol()?;` used
  only as a check, for instance ccl.rs:374/427/490, keymap.rs:208/290) into a
  text-free check.
- `Debug` prints `host_text()` explicitly.

Behaviour is unchanged by construction. The diff is mechanical, grouped by
file, compiler-guided; it cannot be done by grep, because `as_str` is a common
method name.

From here on, `grep -c host_text` is the exact, shrinking count of host-text
readers. Each later stage reports it.

### S4. Identity carried as text → symbols (family e2)

- **The ~35 calls that pass `symbol.host_text()` into by-name methods** take or
  use the `&SymbolName` twins, most of which exist (`*_symbol`):
  - `let_shadows_buffer_binding` and `active_global_special_value`
    (variables.rs:852-859, 1912-2000);
  - the by-name plist helpers (`get_symbol_property(name, property)`
    variables.rs:1008-1014, 1067-1081);
  - `find_symbol_value_resolved`, `set_function_binding`,
    `default_toplevel_value`, `builtin_default_value`, `forwarded_c_value`,
    `per_buffer_predicate`, `clear_alias_by_name`;
  - and the callers in dispatch/misc.rs, buffer_meta.rs, eval/bindings.rs,
    eval/coding.rs, symbol_cells.rs and pdumper.
- **Rename** `find_by_host_text`/`interned_cached` to `SymbolName::find`,
  callers with literals only.
- **Delete by-name methods left without callers**: clippy `-D warnings` flags
  them outside `types/`.

About 400 lines.

**Behaviour.** Neutral; text identity equalled object identity except for the
counter collision, which this removes.

**Tests.** Each converted method with an uninterned twin of a DEFVAR'd or
always-local name (let, makunbound, set-default): the twin is not the
variable.

### S5. Comparisons with literals → keys (families a and b), one round per family

**The conversion is mechanical, and keeps the flavour:**

| today | becomes |
|---|---|
| `Kind::Symbol(s) if s.is_named("x")` | `Kind::Symbol(s) if s.is(K)` (bare symbol only, as today) |
| `PropertyKey::is("x", v)` and `PropertyName == "x"` | `K.is(v)` (follows symbols-with-pos under the flag, as today) |
| `match sym.host_text() { .. }` | an `if`/`else if` chain of key compares, as C's `EQ` chains |
| `eq(x, Value::symbol("x"))` | `x.is_key(K)` |

Add each new name to `KEY_NAMES`, only if GNU interns it at startup. Re-run
`sym/keys.el` with the new names; all must be present. Use
`find_initial(..) == Some(sym)` for Lisp-level names (`ert-test-skipped`).

**Behaviour.** Neutral for every symbol except former and new holders of a
re-interned C-side name. There the converted site moves from T or I to GNU's K:
probes p4-1, 2, 3, 4, 6 and 8 become GNU's. Each round adds those probe
answers as tests for the names it converts.

**Rounds:**
- **S5a, evaluator and loaders.**
  - Files: eval/core.rs (1109-1123, 2531/2537, 3586), native_comp/loader.rs:1330,
    bytecode.rs:1250/1260, eval/macros.rs, definitions.rs, values.rs
    (1420/1533/1808, 2891-2893), predicates.rs:1003-1007, interactive.rs,
    reader.rs (non-test), types/symbols.rs:1226 (nil/t at intern by name bytes),
    types.rs:454 (`:` by name byte).
  - Also `core.rs:2420-2424`: the builtin made from the call text compares the
    symbol with the subr's symbol.
- **S5b, variables, plists and properties.** Remove `PropertyKey for &str`
  and `for &String`, and `PropertyKey::is_named`. Callers pass keys or
  `PropertyName`: strings.rs:1221-1479, buffer.rs:2596, textprop.rs,
  buffer_edit.rs, fields.rs, insdel.rs, indent_motion.rs, eval/variables.rs.
  Also the keyword parsers that compare constructed symbols: make-hash-table
  and its test descriptor (misc_keymaps.rs:739-760), the 20 `eq(..,
  Value::symbol(..))`.
- **S5c, display.** tty.rs (17), dispatch/display.rs (8) and display/mode_line.rs,
  faces.rs (7, incl. the keyword lists 1609), frames.rs, image.rs:167-168,
  fonts.
- **S5d, system.** coding.rs (11) and charset_table.rs (5), processes.rs (9),
  files_process.rs, gnutls.rs (the DIGESTS table → key per entry), loading.rs,
  keys.rs, completion.rs, buffer_edit.rs, collections.rs, json.rs, sqlite.rs,
  treesit.rs, numeric_time.rs, text.rs, file_io.rs, eval/threads.rs,
  eval/ert.rs, eval/runtime.rs, filelock.rs, symbol_cells.rs:314,
  bindings.rs:355, anti_cheat.rs:1160/1266 (R6).
- **S5e, the rest.** The remaining `match host_text()`, `symbol_host_text() ==
  Some(..)` and `as_symbol_host_text()?` followed by a match. Native code's
  remaining sites go here, keeping the fast-path anchors (§6).

Each round is ≤600 lines. make-symbol is unchanged. Watch guards where a hot
path changes (loader.rs, vm.rs, core.rs lambda lists: lex-rec, dyn-rec,
interp-ignore).

### S6. Uses that need the name read the Lisp name (family d)

- **Add** `with_name_str` and `name_first_byte`, and remove `name_text`'s
  host-text branch.
- **Convert** the 33 `visible_symbol_name` sites, `render_error_symbol`,
  `Display for Value` (types.rs:3400), json.rs:1080, keys.rs:215,
  buffer_meta.rs:875/1908, files_process.rs:2743, processes.rs:4853,
  misc.rs:870/1263, values.rs:711-712, completion.rs:129, and the treesit
  language and keymap names.
- **`Debug`** prints the Lisp name, `#:` before a non-initial symbol.
- **`Ord`** compares Lisp name bytes then address, or is deleted if S3-S5 left
  no user.
- **The mapatoms filter** (bindings.rs:1052-1064) reads the `interned` field.

About 400 lines.

**Behaviour.** Neutral where host text and name agree. They differ only for a
name string changed after creation (now GNU's) and, until S7, for a name
holding U+001E or U+001F.

**Tests.** Messages and `format-message` with uninterned and private symbols,
treesit or keymap names through an uninterned symbol (GNU reads
`SYMBOL_NAME`), the mapatoms count.

### S7. Delete the host text

**Remove:**
- `SymbolText`, `SymbolCell::internal`, `host_text`, `is_named`, `NameEntry`
  (dead code already, `types/symbols.rs:148-179`);
- the marker constants and helpers (`types.rs:19-25`, `2521-2594`), the three
  counters (`primitives.rs:273-279`, `reader.rs:96`, `types.rs:2552`);
- the marker logic in `intern_with_lisp_name`, `find`, `find_initial`,
  `intern_initial_with`, `find_interned` and `Symbols::find`/`find_initial`;
- the debug assertions on marked texts.

**Then:**
- Reorder the cell to GNU's order with the free link through `next`. Set the
  asserts to 48 bytes and new offsets (alloc/symbols.rs:595-608, 679-680), and
  relax the block assert (`:645`: 21×48 = 1,008 is not a multiple of 64; make
  it 16, check `image_symbol_bytes` packing and `live_symbol_holding`).
- The dump writes `[u64; 6]` cells, with no key section and no
  `DumpToDumpPtrRaw` for word 0 (context.rs:549-553, 1315-1335, 1519-1540).
  The loader drops `symbol_key`/`SymbolText::image` and goes by name and flags
  (load.rs:1705, 2093-2135; `intern_image(name)`).
- `intern_with_lisp_name(text: String, ..)` becomes
  `intern_name(name: Value)` plus `intern_str(&str)`: no `String` kept, the
  Lisp string made on a miss only.
- The `AddressNames` hit check compares the Lisp name's bytes (`name_is`,
  obarray.rs:57-62) and `interned == INITIAL`.
- `symbol_name_by_tag` and `symbol_str`/`as_symbol` are gone; S5 and S6
  converted their users.

About 500 hand-written lines, plus deletions.

**Behaviour.** Names with U+001E or U+001F intern normally (p2 and p3 become
GNU's). Otherwise neutral.

**Risks.**
- The image format and in-place mapping of image symbols.
- `name_text` for a symbol without a string name. GNU has none; the test
  copier (`eval.rs:4109-4126`, cfg(test)) must give the placeholder an empty
  string, not nil.
- The address cache's hit check costs a few more instructions (it reads the
  name string). Measure startup and interp-ignore; if they rise, S8 is the
  answer.
- H6: deleted assertions and tests must be outnumbered (§6).

**Tests.**
- `size_of::<SymbolCell>() == 48` and GNU's field order.
- Dump and load of uninterned, private, keyword and re-interned symbols.
- The U+001F/U+001E probes as GNU answers.
- `(eq (symbol-name s) str)` after dump and load.
- The free-list and sweep census tests.

**Measure.** make-symbol ~1,360. intern-new and priv-intern drop by the host
text allocation (~200-500).

### S8 (optional, outside D14). Construction by text of C-side names → keys

The ~1,400 `Value::symbol("lit")`/`Value::Symbol("lit".into())` sites whose
name is a DEFSYM, DEFUN or DEFVAR (355+ DEFSYM names) become `cached_symbol!`,
by file family. This is GNU's `Qfoo`, and it fixes I semantics after unintern.
Then delete `AddressNames`, the last GNU-absent identity cache of this item.

Raise the key table's bound past 2,048 if all DEFSYMs become keys
(`types/symbols.rs:1314-1334`). Watch compile time: `SymbolKey::new` is a
linear const search per site.

## 6. Traps and open questions

**Round audit (`tools/round_audit.py`):**
- **H5, the lint-suppression rule.**
  - No `#[allow]`/`#[expect]` may be added, and no line that carries one may
    be edited. In scope: `types.rs:1` `#![allow(dead_code)]`,
    `types.rs:1850/3711`, `primitives.rs:276` (`next_make_symbol_id`'s
    `cfg_attr`), `pdumper/context.rs` `SYMBOL_REDIRECT_MASK`/
    `SYMBOL_INTERNED_SHIFT`, and the generated manifests. Delete whole items
    instead.
  - `types.rs:1`'s module-wide `allow(dead_code)` covers `types/symbols.rs`,
    `obarray.rs` and `string_data.rs`: dead helpers there are *not* flagged, so
    S7's deletion list must be explicit.
  - Elsewhere, clippy `-D warnings` fails the build on any helper a stage
    leaves dead, so the stage deletes it.
- **H6, assertion balance.**
  - `debug_assert!` counts, so S7 removes at least 3 assertions
    (types.rs:405, 419, 601) plus marker-specific test assertions (reader.rs:2887-2891).
  - Every stage must add at least as many test functions and assertions as it
    removes. A changed assertion counts as one removed and one added, and is
    listed under R8.
- **R3:** new lines must not say "fallback", "fall back", "special case",
  "workaround", "hack" or "stub".
- **R4:** S2's cache change and any new counter or table are listed; S1 adds
  none if it reuses `next_make_symbol_id`.
- **R6:** edits to `src/anti_cheat.rs` (its two `is_named` uses, S5d) are
  review findings.

**Anti-cheat (`src/anti_cheat.rs`):**
- **Field audits.** `interpreter_value_fields_are_gc_roots_or_documented`,
  `..._dumped_or_documented` and `..._carried_or_documented` parse the
  Interpreter's fields; no stage should add one.
- **Native fast paths.** `native_comp_fast_paths_are_audited_against_gnu_c`
  keeps text anchors in `native_comp/runtime.rs` and the generator. Check them
  before S5d/S5e touch runtime.rs (`"make-symbol"` appears at runtime.rs:7430,
  9820, 10429).
- **Generated table.** CI compares the regenerated native subr table byte for
  byte; no stage touches generated files.
- **DEFSYM manifest.** The DEFSYM manifest gate is unaffected: `KEY_NAMES` is
  hand-kept.
- **Open question:** should a test check `KEY_NAMES` against GNU's startup
  obarray? About 345 entries are DEFVAR or preloaded-Lisp names outside the
  DEFSYM, DEFUN and bool/int manifests. It needs a generated manifest of the
  startup obarray, which is a generator change (R6), or the `keys.el` check
  recorded per round.

**Behaviour edges to state in each round's entry:**
- re-interned C-side names (S5, toward GNU);
- name strings changed after creation (S1 and S6, toward GNU);
- U+001E/U+001F names (S7, toward GNU);
- the serial collision (S4, removed).

None of these is behaviour-neutral in the strict sense. Each is a GNU
divergence today, recorded by the probes.

**Other traps:**
- **Symbols with position.** Converted sites must keep their flavour:
  bare-only `Kind::Symbol(..)` arms against `EQ`-like `is_key`. Test the
  byte-compiler path with `symbols-with-pos-enabled` for lambda lists and
  `declare`.
- **The name string and the collector.** The name string is already traced
  (`eval.rs:4930`), and interned names are rooted through the obarray. After
  S7 the string is the only name, so a symbol must never be made with a
  non-string name, the test copier included.
- **Threads.** S1's write-once text relies on one Lisp thread at a time. The
  host threads that exist (process readers, file watchers) must not read
  symbols; this is a review item, not a test.
- **Hash and order.** Hash is by address and unchanged. The text-keyed tables
  found (`builtin_doc_offsets`, the tty face cache by printed text,
  `RecordIdsByType` by host-record kind) are not keyed by host text.
- **Debug output.** Any runtime `format!("{:?}", symbol)` that reaches Lisp
  would change in S6. The compiler-guided S3 lists all `Display` uses; check
  `Debug` uses by deleting the impl in a scratch build.
- **Is D14 done after S1?** The item reads "make-symbol ~285 instructions
  over GNU". After S1 Emaxx is about 580 *under*. The structural items (A2.3,
  A3.4, A3.6, A3.8, the symbol part) stay open until S7.
- **Scope of S8.** Whether to convert construction by text for C-side names,
  and when, is not needed for D14. Until S8, `AddressNames` stays (with S2's
  O(1) Funintern).

## Appendix: probes and measurements (scratch, not committed)

All in `the scratch directory sym/`:

- `p1.el` → `p1.g`, `p1.e`: uninterned `car`/`nil`/`t`, name identity,
  keyword args, private `foo`, special-form heads, lambda lists, print-gensym.
  Identical.
- `p2.el` → `p2.g`/`p2.e`, `p3.el` → `p3.g`/`p3.e`: U+001F/U+001E names
  (differ), name mutation, voids, sort, sxhash, unintern, hash keys, plists,
  conditions, defalias/fset, macro/autoload heads, type-of. Identical except
  the U+001F/U+001E rows.
- `p4-1.el` … `p4-10.el`: re-interned `:size`, `:test`, `invisible`, `&rest`,
  `function`, `arith-error`, `fill-column`, `equal`, plus name `aset`. Results
  are in §2. Run them one per process, because GNU crashes on p4-10 (aset of a
  pure DEFSYM name), which takes the combined `p4.el` down.
- `p6.el` → `p6.g`, `p6.e`: subr through an uninterned alias, plist and face
  keys, cond `t`, `declare`/`interactive`, defvar, hooks, sort, `%S` escapes,
  intern-soft and unintern of a symbol, mapatoms and re-intern in private
  obarrays. All identical.
- `keys.el` (all 750 `KEY_NAMES` interned at GNU startup: 0 missing),
  `named.el` (the `is_named` literals: 8 missing, of which only
  `ert-test-skipped` is runtime).
- `cg2.{e,g}.{200000,600000}` (make-symbol), `cg3.{e,g}.{intern-new,priv-intern}.{20000,60000}`,
  with `tree.*`/`tree3.*` caller trees. `cg.{e,g}.{20000,60000}` are the
  earlier attempt's. At those counts GC granularity gives 2,132/1,816; use the
  standard counts.
- Scripts in `../symtools/`: `selfdiff.py A B DIV [MIN]` (self cost per unit),
  `treediff.py TREE_A TREE_B DIV FN...` (callee costs per unit, from
  `callgrind_annotate --tree=calling`), `calls.py FILE REGEX` (call counts).
- Probe templates for `tools/perf/probes`: `priv-intern.tpl`, `intern-new.tpl`
  (`@N@`, `DIV 1`).
