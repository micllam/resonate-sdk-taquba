//! The four scenario forms of the specification's `work/go` set, run
//! through the Rust SDK against a `TaqubaNetwork` and recorded as
//! traces in the format the specification's checkers read.
//!
//! Each scenario writes `target/traces/<scenario>.ndjson`, one line per
//! recorded request: `{"kind", "now", "req", "res"}`, ordered by `now`
//! and then by return order. The checkers are run on the files:
//!
//! ```bash
//! lake exe checktrace < target/traces/simple-run.ndjson
//! (cd valid/porc && go run ./cmd/lincheck) < target/traces/simple-run.ndjson
//! ```

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use resonate_sdk::network::Network;
use resonate_sdk::prelude::*;
use resonate_sdk_taquba::{TaqubaNetwork, TaqubaStore};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use taquba::object_store::memory::InMemory;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Args {
    loops: usize,
    fanout: usize,
    #[serde(rename = "sleepMs")]
    sleep_ms: u64,
    depth: usize,
}

#[resonate_sdk::function]
async fn step(n: i64) -> Result<i64> {
    Ok(n * 2)
}

#[resonate_sdk::function]
async fn step_remote(n: i64) -> Result<i64> {
    Ok(n * 2)
}

#[resonate_sdk::function]
async fn simple_run(ctx: &Context, a: Args) -> Result<i64> {
    let mut total = 0;
    for i in 0..a.loops {
        total += ctx.run(step, i as i64).await?;
    }
    Ok(total)
}

#[resonate_sdk::function]
async fn simple_rpc(ctx: &Context, a: Args) -> Result<i64> {
    let mut total = 0;
    for i in 0..a.loops {
        total += ctx.rpc::<i64>("step_remote", i as i64).await?;
    }
    Ok(total)
}

#[resonate_sdk::function]
async fn simple_sleep(ctx: &Context, a: Args) -> Result<i64> {
    for _ in 0..a.loops {
        ctx.sleep(Duration::from_millis(a.sleep_ms)).await?;
    }
    Ok(a.loops as i64)
}

#[resonate_sdk::function]
async fn fan_out(ctx: &Context, a: Args) -> Result<i64> {
    let mut futures = Vec::with_capacity(a.fanout);
    for i in 0..a.fanout {
        futures.push(ctx.run(step, i as i64).spawn()?);
    }
    let mut total = 0;
    for future in futures {
        total += future.await?;
    }
    Ok(total)
}

/// One recorded request.
#[derive(Debug, Clone)]
struct Event {
    kind: String,
    now: i64,
    req: Value,
    res: Value,
    /// Return order, for the order of events that share a `now`.
    returned: u64,
}

/// The request kinds both checkers read.
fn recordable(kind: &str) -> bool {
    matches!(
        kind,
        "promise.create"
            | "promise.get"
            | "promise.settle"
            | "promise.register_callback"
            | "promise.register_listener"
            | "task.get"
            | "task.acquire"
            | "task.suspend"
            | "task.fulfill"
            | "task.create"
            | "task.fence"
            | "task.release"
            | "task.heartbeat"
    )
}

/// A network that records every request it forwards, with the time the
/// store applied it at.
struct Recording {
    inner: TaqubaNetwork,
    events: Arc<Mutex<Vec<Event>>>,
    returns: Arc<AtomicU64>,
}

#[async_trait::async_trait]
impl Network for Recording {
    fn pid(&self) -> &str {
        self.inner.pid()
    }

    fn group(&self) -> &str {
        self.inner.group()
    }

    fn unicast(&self) -> &str {
        self.inner.unicast()
    }

    fn anycast(&self) -> &str {
        self.inner.anycast()
    }

    async fn start(&self) -> Result<()> {
        self.inner.start().await
    }

    async fn stop(&self) -> Result<()> {
        self.inner.stop().await
    }

    async fn send(&self, req: String) -> Result<String> {
        let (now, res) = self.inner.send_timed(&req).await;
        let request: Value = serde_json::from_str(&req).expect("the SDK sends JSON");
        let kind = request["kind"].as_str().unwrap_or_default().to_string();
        if recordable(&kind) {
            let returned = self.returns.fetch_add(1, Ordering::SeqCst);
            self.events.lock().unwrap().push(Event {
                kind,
                now,
                req: request["data"].clone(),
                res: serde_json::from_str(&res).expect("the store returns JSON"),
                returned,
            });
        }
        Ok(res)
    }

    fn recv(&self, callback: Box<dyn Fn(String) + Send + Sync>) {
        self.inner.recv(callback)
    }

    fn target_resolver(&self, target: &str) -> String {
        self.inner.target_resolver(target)
    }
}

fn write_trace(name: &str, events: &[Event]) -> PathBuf {
    let mut events = events.to_vec();
    events.sort_by_key(|e| (e.now, e.returned));
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/traces");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(format!("{name}.ndjson"));
    let lines: Vec<String> = events
        .iter()
        .map(|e| {
            serde_json::json!({
                "kind": e.kind,
                "now": e.now,
                "req": e.req,
                "res": e.res,
            })
            .to_string()
        })
        .collect();
    std::fs::write(&path, lines.join("\n") + "\n").unwrap();
    path
}

struct Run {
    resonate: Resonate,
    events: Arc<Mutex<Vec<Event>>>,
    store: TaqubaStore,
}

async fn start(client: &str) -> Run {
    let store = TaqubaStore::open(Arc::new(InMemory::new()), "resonate")
        .await
        .unwrap();
    let events = Arc::new(Mutex::new(Vec::new()));
    let network = Recording {
        inner: store.network().pid(client).build(),
        events: events.clone(),
        returns: Arc::new(AtomicU64::new(0)),
    };
    let resonate = Resonate::new(ResonateConfig {
        network: Some(Arc::new(network)),
        ..Default::default()
    });
    resonate.register(step).unwrap();
    resonate.register(step_remote).unwrap();
    resonate.register(simple_run).unwrap();
    resonate.register(simple_rpc).unwrap();
    resonate.register(simple_sleep).unwrap();
    resonate.register(fan_out).unwrap();
    Run {
        resonate,
        events,
        store,
    }
}

impl Run {
    async fn finish(self, name: &str, expected_events: usize) {
        self.resonate.stop().await.unwrap();
        drop(self.resonate);
        self.store.close().await.unwrap();
        let events = self.events.lock().unwrap().clone();
        let path = write_trace(name, &events);
        assert!(
            events.len() >= expected_events,
            "{name}: {} events recorded, at least {expected_events} expected ({})",
            events.len(),
            path.display()
        );
        // The store's time does not decrease across successive calls.
        let mut by_return = events.clone();
        by_return.sort_by_key(|e| e.returned);
        assert!(by_return.windows(2).all(|pair| pair[0].now <= pair[1].now));
        let kinds: Vec<&str> = events.iter().map(|e| e.kind.as_str()).collect();
        assert!(kinds.contains(&"task.create"), "{name}: {kinds:?}");
        assert!(kinds.contains(&"task.fulfill"), "{name}: {kinds:?}");
    }
}

const RUNS: usize = 3;

async fn result<T>(f: impl std::future::IntoFuture<Output = Result<T>>) -> T {
    tokio::time::timeout(Duration::from_secs(30), f.into_future())
        .await
        .expect("the run completes")
        .unwrap()
}

#[tokio::test]
async fn simple_run_scenario() {
    let run = start("c0").await;
    for i in 0..RUNS {
        let args = Args {
            loops: 2,
            ..Default::default()
        };
        let id = format!("c0-simple-run-{i}");
        let total = result(run.resonate.run(&id, simple_run, args)).await;
        assert_eq!(total, 2);
    }
    run.finish("simple-run", RUNS * 4).await;
}

#[tokio::test]
async fn simple_rpc_scenario() {
    let run = start("c0").await;
    for i in 0..RUNS {
        let args = Args {
            loops: 2,
            ..Default::default()
        };
        let id = format!("c0-simple-rpc-{i}");
        let total = result(run.resonate.run(&id, simple_rpc, args)).await;
        assert_eq!(total, 2);
    }
    run.finish("simple-rpc", RUNS * 6).await;
}

#[tokio::test]
async fn simple_sleep_scenario() {
    let run = start("c0").await;
    for i in 0..RUNS {
        let args = Args {
            loops: 2,
            sleep_ms: 10,
            ..Default::default()
        };
        let id = format!("c0-simple-sleep-{i}");
        let total = result(run.resonate.run(&id, simple_sleep, args)).await;
        assert_eq!(total, 2);
    }
    run.finish("simple-sleep", RUNS * 4).await;
}

#[tokio::test]
async fn fan_out_scenario() {
    let run = start("c0").await;
    for i in 0..RUNS {
        let args = Args {
            fanout: 3,
            depth: 1,
            ..Default::default()
        };
        let id = format!("c0-fan-out-{i}");
        let total = result(run.resonate.run(&id, fan_out, args)).await;
        assert_eq!(total, 6);
    }
    run.finish("fan-out", RUNS * 6).await;
}
