# Network, TLS, HTTP, JSON, and JSON-RPC compatibility contract

This document records the permanent deterministic evidence for issue #35.
All comparisons use the pinned GNU Emacs 30.2 source at commit
`636f166cfc86aa90d63f592fd99f3fdd9ef95ebd`.  Permanent network tests use
loopback sockets or local subprocesses; they do not depend on a public host,
DNS server, certificate authority, or package archive.

## Upstream suite evidence

The frozen 7,883-outcome Darwin and Linux baselines already matched every
selected outcome in the relevant JSON, JSON-RPC, GnuTLS, network-stream, and
URL files except `lookup-hints-values` in `test/src/process-tests.el`.  That
last mismatch was specific to GNU/Linux: Emaxx used Rust's strict
`IpAddr::parse`, while GNU passes numeric syntax to the host's
`getaddrinfo(..., AI_NUMERICHOST)`, which accepts the platform's historical
IPv4 forms.

The issue-35 candidate delegates the primitive to the same host resolver.
The permanent oracle contract covers invalid hostnames, decimal IPv4 with one
through four components, hexadecimal and octal IPv4 forms, IPv4/IPv6 family
filtering, valid IPv6 compression and case, and invalid IPv6 forms.  It asks
GNU and Emaxx the same 29-address matrix and compares the returned address
vectors exactly.

Focused optimized replays on the candidate report:

- `test/lisp/jsonrpc-tests.el`, selector `all`: 9/9 matching.  This includes
  the four expensive/deferred tests omitted from the ordinary frozen selector.
- `test/lisp/net/gnutls-tests.el`, selector `all`: 7/7 matching, including
  the expensive AEAD test.
- `test/lisp/net/network-stream-tests.el`, selector `all`: 27/27 matching.
  These create real IPv4, IPv6 where available, Unix-domain, and TLS sockets
  and exercise synchronous and asynchronous clients, coding selection,
  certificate paths, filters, and server callbacks.
- `test/lisp/json-tests.el`, selector `all`: 59/59 matching.
- `test/src/json-tests.el`, selector `all`: 23/23 matching.
- The frozen URL corpus: 36/36 matching across URL authentication, expansion,
  file/future/handler behavior, parsing, TRAMP conversion, and utilities.

The exact focused replay form is:

```sh
LANG=C LC_ALL=C target/gate/compat-harness run \
  --scope SCOPE --selector all --file FILE --timeout-seconds 600
```

Use `scope=lisp` for the Lisp suites and `scope=src` for the native JSON
suite.  The compatibility harness enforces the repository anti-cheat gates
before each run and retains the unedited GNU pass, failure, and skip states.

## Real HTTP retrieval

The Rust test
`url_retrieve_synchronously_matches_gnu_over_a_real_local_http_connection`
starts independent one-shot loopback HTTP servers for GNU and Emaxx.  Each
editor calls the ordinary Lisp `url-retrieve-synchronously` entry point.  The
test compares HTTP status, a custom response header, exact body length,
SHA-256, body prefix, and buffer cleanup, and independently requires that both
servers received the intended HTTP/1.1 GET request and Host header.  A runtime
shortcut that fabricated the response without opening a connection therefore
cannot pass.

The fixture has a fixed response body and headers, binds an ephemeral loopback
port, has bounded accept/read waits, and serves exactly one request.  Run it
with:

```sh
cargo test --profile gate \
  lisp::primitives::tests::url_retrieve_synchronously_matches_gnu_over_a_real_local_http_connection \
  -- --exact --test-threads=1
```

Hosts whose sandbox denies loopback binds must run this exact named test
outside that restriction; the initial permission-denied result is not a code
failure and is not counted.

## JSON-RPC application evidence

GNU's nine upstream JSON-RPC tests create a real TCP client/server pair and
cover successful requests, protocol errors, internal errors, timeouts, late
responses, deferred requests, and multiple queued continuations.  The built-in
Eglot contract adds Content-Length-framed request/response/notification
traffic over a real subprocess plus unexpected disconnect, reconnect, and
orderly shutdown.  The lsp-mode package contract independently exercises the
same transport through a third-party client, including its genuine JSON-RPC
event buffer.

Those application contracts are documented in
[`eglot-compatibility.md`](eglot-compatibility.md) and
[`lsp-mode-compatibility.md`](lsp-mode-compatibility.md).  They supplement the
upstream suite; they do not replace it with fixture-shaped production code.

## TLS boundary

The upstream network-stream suite and native GnuTLS regressions use local
`gnutls-serv` processes.  The native contracts exercise encrypted process I/O,
synchronous and asynchronous handshakes, shutdown, live host-library
algorithm catalogs, X.509 peer details, an explicit local trust file, an
encrypted client key, and hostname mismatch rejection.  Certificates and
ports are generated locally; tests skip only when GNU's own prerequisite
probe says the required host tool or address family is unavailable.

The seven-test GnuTLS all-selector replay is semantically green but much
slower in Emaxx on the recorded Darwin host (about 39 seconds of test time
versus 0.6 seconds in GNU).  This is disclosed performance evidence for the
later release/performance milestone, not relabelled as semantic parity.

## Rejected evidence

Several focused `lookup-hints-values` harness attempts are not counted as
complete comparisons.  One sandboxed attempt prevented GNU's test file from
performing its load-time DNS process write.  Unrestricted attempts gave GNU a
complete pass and drove Emaxx through the full numeric-address list, but the
harness's total process timeout included roughly nine minutes of isolated
subject rebuilding and killed the ERT runner before it emitted its final JSON
record.  An earlier Emaxx artifact did emit the upstream result as passed.
The permanent 29-address live-oracle contract is the authoritative focused
evidence; the timed-out wrapper runs remain explicit rather than being called
passes.

A separate cancellation experiment is also excluded.  It set
`unread-command-events` in batch mode, but both editors completed the request
normally, so it did not exercise `jsonrpc-request`'s interactive
`:cancel-on-input` branch and would have been vacuous.  Timeout cancellation
and late-response removal are exercised non-vacuously by the upstream suite;
interactive input cancellation remains outside this batch-only contract.

## Scope boundary

This contract covers deterministic local client/server sockets, address
families available on the host, resolver success and errors, filters,
sentinels, coding, connection states, buffering, shutdown, local TLS trust and
hostname rejection, ordinary HTTP retrieval, JSON edge cases, and the
JSON-RPC workflows above.  It does not claim public-network availability,
every proxy/authentication scheme, system trust-store equivalence, certificate
revocation services, every TLS version/cipher combination, Windows sockets,
or interactive `:cancel-on-input`.  Those require their own same-input oracle
journeys rather than inference from the local contract.
