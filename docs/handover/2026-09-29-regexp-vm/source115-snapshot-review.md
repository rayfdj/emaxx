# Regexp snapshot review

The implementation is in the separately inventoried source115 snapshot, on
published c9ce1b2dc23cf20a400e3c2aa15d6afb6aebdb96. This review does not certify
the complete runtime or its performance.

GNU search.c/regex-emacs.c reads canonical syntax, category and case tables.
Emaxx's existing regexp backend embeds translated classes. It therefore must
validate their dependencies after both Lisp setters and native field stores.
The existing cache limits, regexp parser, translated patterns and result checks
are unchanged. The change removes graph reconstruction, the visited hash set
and signature-vector allocation from a warm cache check. Cache misses still
capture the actual graph; no mutation generation or setter notification stands
in for native writes.

The snapshot stores raw words and copied payloads, not an independent mutable
Lisp representation. It does not register those words as GC roots. Every node
is recorded before its outgoing edges are traversed. Validation first checks
the current root word, then each preceding node's entire payload before reading
its descendants. It stops on the first mismatch. Thus replacing an edge cannot
make validation dereference the old, possibly collected child. If allocator
reuse gives a new root the same address, matching current ancestor edges still
establishes each descendant's reachability. Node kinds, dimensions, subtable
depth/minimum and mutable contents are checked before accepting a hit. Checks
make no Lisp callbacks and do not collect; the existing runtime ownership
boundary excludes concurrent mutation during the check. This is a safety
argument, not a substitute for final ownership validation.

Capture preserves cycle termination and shared payload handling with a visited
set on misses. The new Lisp fixture changes shared syntax and category leaves,
replaces parent edges, collects and allocates replacement payloads, then probes
again. Its bytes and expected output agree with ordinary GNU and the published
baseline. Earlier fixture probes are retained: string-to-syntax returns a shared
standard descriptor, so the final fixture uses fresh conses where it needs fresh
identity. Existing native syntax-store and descriptor-mutation tests retain all
assertions. No test selector, timeout, expected result, ignore or comparison
normalization was changed.

Production changes contain no benchmark names, test-name matching, oracle
forwarding, canned values or new capability claims. They do not change collection
triggers, suppress collection or invent allocation counters. Cache data size,
ordinary execution behavior, full platform validation and real performance
remain to be checked; selected passes do not establish the full goal.
