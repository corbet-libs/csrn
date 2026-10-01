# csrn — Assurance

A thin Rust facade connecting [Charter](https://github.com/corbet-foss/cchr) to
cvld's public trust feed. The follower lives in `feed` until it has a final
library name; it owns retries, cancellation and scheduling. Assurance owns no
second policy evaluator, trust revision, member registry or database.

The caller supplies Unix time and executes returned `Request` values. Only
`/v1/trust_feed` and `/v1/trust_changes` can be requested. Feed responses go to
Charter with authenticated origin evidence. Announcements cause a fetch; they
never become trust material. Timed refresh recovers lost announcements.

```rust
# fn example(now: u64) -> csrn::Result<()> {
use csrn::{Assurance, feed::Config};
let config = Config {
    refresh_seconds: 60, poll_seconds: 2, request_seconds: 30,
    initial_backoff_seconds: 2, maximum_backoff_seconds: 60,
    maximum_staleness_seconds: 120,
};
let mut assurance = Assurance::new("https://alpha.example.test", "alpha", config)?;
let request = assurance.start(now)?;
assert_eq!(request.path(), "/v1/trust_feed");
// Execute through the configured transport, then call on_feed with authenticated
// origin evidence and receive time. Poll next(now); stop() fences late replies.
# Ok(()) }
```

Optional `http` provides a native HTTPS adapter with bounded response collection,
redirect refusal, no cookies/bearers/proxy inheritance and a generic User-Agent.
Its `Received::authority` stamps successful origin-authenticated responses with
the caller's receive time. Browser runtimes supply their own network adapter;
the same synchronous state machine and vectors run on wasm.

Read [docs/CONTRACT.md](docs/CONTRACT.md) for states, ports, deadlines and limits.
FSL-1.1-ALv2. Nothing is published to a registry.

## Reuse and candidates

Reuse cchr/csgn/cshm for trust verification and schema semantics. Extract the feed
module after naming it; no speculative package is created. cvld's actual public
POST actions are the protocol. Importing its server package into the runtime
would couple a device consumer to issuer secrets/storage and the server graph.

[reqwest](https://docs.rs/reqwest/latest/reqwest/) and its
[redirect policy](https://docs.rs/reqwest/latest/reqwest/redirect/struct.Policy.html)
provide native HTTP/TLS; `Policy::none` enforces refusal. serde supplies bounded
announcement decoding and url validates origins. Generic retry crates such as
backoff can schedule retries, but do not own the required response-generation,
announcement and Charter freshness state; a small explicit machine keeps these
transitions deterministic and testable without a runtime or hidden wall clock.

The guard integration remains unavailable: cgrd cannot yet consume cplc's full
policy and separate revocations. `VerifiedCharter::admission_policy()` returns
`UnsupportedPolicy`. A fresh publication is not itself a positive admission.
