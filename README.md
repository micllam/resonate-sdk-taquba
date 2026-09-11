# resonate-sdk-taquba

Embedded network for the [Resonate Rust SDK](https://github.com/resonatehq/resonate-sdk-rs):
durable execution on an object store. The protocol runs in the SDK's
process over a [taquba](https://github.com/micllam/taquba) queue, and
its state lives in a [SlateDB](https://slatedb.io) store in the
application's own bucket. No Resonate server required. One process owns
the store, and a crash resumes from the last committed request at the
next open.

## Setup

Add the dependencies.

```toml
[dependencies]
resonate-sdk = { git = "https://github.com/resonatehq/resonate-sdk-rs", rev = "6545265" }
resonate-sdk-taquba = { git = "https://github.com/micllam/resonate-sdk-taquba" }
```

The taquba crate is re-exported as `resonate_sdk_taquba::taquba`. Enable
the `aws`, `gcp` or `azure` feature of this crate for a cloud bucket.
The in-memory and local-disk stores work without a feature.

## Usage

```rust
use std::sync::Arc;
use resonate_sdk::prelude::*;
use resonate_sdk_taquba::TaqubaStore;
use resonate_sdk_taquba::taquba::object_store::memory::InMemory;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = TaqubaStore::open(Arc::new(InMemory::new()), "resonate").await?;
    let network = store.network().group("workers").build();
    let resonate = Resonate::new(ResonateConfig {
        network: Some(Arc::new(network)),
        ..Default::default()
    });
    // resonate.register(..), resonate.run(..)
    resonate.stop().await?;
    store.close().await?;
    Ok(())
}
```

A store creates one network per SDK instance. The worker id and the
routing group are set on the builder:

```rust
let network = store
    .network()
    .pid("worker-1")     // defaults to a generated id
    .group("workers")    // defaults to "default"
    .build();
```

`TaqubaStore::open_with_options` accepts the queue's `OpenOptions`
(clock, flush interval, queue configuration) and the `StoreOptions`
(retry timeout, preload limit, poll interval).
`StoreOptions::queue_options` returns the queue options the store
requires, as the base of a caller's own.

Tests that advance time use taquba's `MockClock` as the queue's clock
and send the protocol's requests directly. The SDK computes a promise's
`timeoutAt` from the system clock, so a workflow run through the SDK
does not follow a mocked clock.

## Semantics

### A task is a job

Its dispatch is a job on the queue named by its target address.
Delivery claims the job and sends the execute message, the acquire and
every heartbeat renew the claim's lease, and a fulfil or a suspend
acknowledges the job with the settlement's effects. A release fails the
job, a lease that expires returns it to pending for the next delivery,
which sends the execute message again with the version unchanged, and a
restart requeues every claimed job at open.

### One transaction per request

One in-process lock serialises the requests. The records a request
changes, the jobs it enqueues and the timers it schedules commit
together, and a request that settles a task commits in the job's own
settlement.

### Messages and timers are jobs

An unblock message is a job on the queue named by the listener's
address, acknowledged once delivered, so delivery is at least once. A
promise deadline and a schedule firing are scheduled jobs on the store's
timer queue, fired by the store's timer worker.

### Time and storage

Every time is read from the queue's clock, so a store opened with a
`MockClock` runs the protocol under controlled time. Records are entries
in the queue's user KV namespace with a `resonate/` prefix, so the
namespace can be shared with the application's own entries.

## Limits

- A promise record, its parameter and value included, must fit the
  queue's KV value cap of 256 KiB.
- An execute message for a target group is delivered only while a
  network for that group is started in this process.
- The search operations read every record of their kind.
- Every request that writes awaits the store's write-ahead log flush,
  so `OpenOptions::flush_interval` is the lower bound on its latency.
  SlateDB's default is 100 ms.

## Tests

`cargo test` runs the SDK's own end-to-end suite against the network
(`tests/e2e.rs`), the contracts that depend on time under a mock clock
(`tests/protocol.rs`) and the four scenario forms of the Resonate
specification (`tests/scenarios.rs`). The scenarios write one trace each
to `target/traces/`, in the format the specification's checkers read.

## License

Apache-2.0.
