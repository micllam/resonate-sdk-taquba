//! The contracts of the network that depend on time, under a mock clock: lease
//! expiry, retry, promise deadlines, schedule firing and recovery at
//! reopen. Requests are sent as raw envelopes and messages are read
//! from the `recv` callback.

use std::sync::Arc;
use std::time::Duration;

use resonate_sdk::network::Network;
use resonate_sdk_taquba::{StoreOptions, TaqubaNetwork, TaqubaStore};
use serde_json::{Value, json};
use taquba::MockClock;
use taquba::object_store::memory::InMemory;
use tokio::sync::mpsc;

const T0: u64 = 1_700_000_000_000;
const RETRY: Duration = Duration::from_secs(5);

struct Harness {
    store: TaqubaStore,
    clock: MockClock,
    object_store: Arc<InMemory>,
}

async fn open(object_store: Arc<InMemory>, clock: MockClock) -> TaqubaStore {
    open_with(object_store, clock, StoreOptions::default()).await
}

async fn open_with(
    object_store: Arc<InMemory>,
    clock: MockClock,
    options: StoreOptions,
) -> TaqubaStore {
    let options = options.retry_timeout(RETRY);
    TaqubaStore::open_with_options(
        object_store,
        "resonate",
        StoreOptions::queue_options().clock(Arc::new(clock)),
        options,
    )
    .await
    .unwrap()
}

async fn harness() -> Harness {
    harness_with(StoreOptions::default()).await
}

async fn harness_with(options: StoreOptions) -> Harness {
    let clock = MockClock::new(T0);
    let object_store = Arc::new(InMemory::new());
    let store = open_with(object_store.clone(), clock.clone(), options).await;
    Harness {
        store,
        clock,
        object_store,
    }
}

struct Client {
    network: Arc<TaqubaNetwork>,
    inbox: mpsc::UnboundedReceiver<Value>,
}

impl Harness {
    async fn client(&self, group: &str, pid: &str) -> Client {
        let network = Arc::new(self.store.network().group(group).pid(pid).build());
        let (tx, inbox) = mpsc::unbounded_channel();
        network.recv(Box::new(move |raw| {
            let _ = tx.send(serde_json::from_str(&raw).unwrap());
        }));
        network.start().await.unwrap();
        Client { network, inbox }
    }

    fn now(&self) -> i64 {
        use taquba::Clock;
        self.clock.now_ms() as i64
    }

    /// Advance the clock, fire every timer that became due and requeue
    /// every job whose lease ended.
    async fn advance(&self, by: Duration) {
        self.clock.advance(by);
        self.store.queue().promote_scheduled_now().await.unwrap();
        self.store.queue().reap_now().await.unwrap();
    }
}

impl Client {
    async fn send(&self, kind: &str, data: Value) -> Value {
        let request = json!({
            "kind": kind,
            "head": { "corrId": "c", "version": "2026-04-01" },
            "data": data,
        });
        let raw = self.network.send(request.to_string()).await.unwrap();
        let response: Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(response["kind"], kind);
        assert_eq!(response["head"]["corrId"], "c");
        response
    }

    async fn message(&mut self) -> Value {
        tokio::time::timeout(Duration::from_secs(5), self.inbox.recv())
            .await
            .expect("a message arrives")
            .expect("the inbox is open")
    }

    async fn no_message(&mut self) {
        let quiet = tokio::time::timeout(Duration::from_millis(400), self.inbox.recv()).await;
        assert!(quiet.is_err(), "no message is expected: {quiet:?}");
    }

    async fn execute(&mut self) -> (String, i64) {
        let message = self.message().await;
        assert_eq!(message["kind"], "execute", "{message}");
        let task = &message["data"]["task"];
        (
            task["id"].as_str().unwrap().to_string(),
            task["version"].as_i64().unwrap(),
        )
    }
}

fn targeted(id: &str, target: &str, timeout_at: i64) -> Value {
    json!({
        "id": id,
        "timeoutAt": timeout_at,
        "param": { "data": "" },
        "tags": { "resonate:target": target, "resonate:scope": "global", "resonate:branch": id },
    })
}

fn acquire(id: &str, version: i64, pid: &str, ttl: i64) -> Value {
    json!({ "id": id, "version": version, "pid": pid, "ttl": ttl })
}

#[tokio::test]
async fn anycast_equals_the_resolved_group_target() {
    let h = harness().await;
    let network = h.store.network().group("g").pid("p").build();
    assert_eq!(network.anycast(), network.target_resolver("g"));
    assert_eq!(network.unicast(), "poll://uni@g/p");
}

#[tokio::test]
async fn execute_is_sent_again_when_a_lease_expires() {
    let h = harness().await;
    let mut c = h.client("g", "p").await;
    let target = c.network.anycast().to_string();
    let far = h.now() + 3_600_000;
    let created = c
        .send("promise.create", targeted("root", &target, far))
        .await;
    assert_eq!(created["head"]["status"], 200);
    assert_eq!(c.execute().await, ("root".to_string(), 0));

    let acquired = c.send("task.acquire", acquire("root", 0, "p", 1_000)).await;
    assert_eq!(acquired["head"]["status"], 200);
    assert_eq!(acquired["data"]["task"]["version"], 1);
    assert_eq!(acquired["data"]["task"]["state"], "acquired");

    h.advance(Duration::from_millis(999)).await;
    c.no_message().await;
    h.advance(Duration::from_millis(2)).await;
    assert_eq!(c.execute().await, ("root".to_string(), 1));

    // The lost holder is fenced out and the new holder is at version 2.
    let stale = c.send("task.acquire", acquire("root", 0, "p", 1_000)).await;
    assert_eq!(stale["head"]["status"], 409);
    let fresh = c.send("task.acquire", acquire("root", 1, "p", 1_000)).await;
    assert_eq!(fresh["data"]["task"]["version"], 2);
}

#[tokio::test]
async fn a_fulfil_after_a_lost_lease_is_refused_and_settles_nothing() {
    let h = harness().await;
    let mut c = h.client("g", "p").await;
    let target = c.network.anycast().to_string();
    let far = h.now() + 3_600_000;
    c.send("promise.create", targeted("root", &target, far))
        .await;
    c.execute().await;
    c.send("task.acquire", acquire("root", 0, "p", 1_000)).await;
    // The network is stopped, so no redelivery runs and the fulfil
    // observes the expired lease.
    c.network.stop().await.unwrap();
    h.clock.advance(Duration::from_millis(1_001));

    let fulfil = json!({
        "id": "root",
        "version": 1,
        "action": {
            "kind": "promise.settle",
            "data": { "id": "root", "state": "resolved", "value": { "data": "" } },
        },
    });
    let refused = c.send("task.fulfill", fulfil).await;
    assert_eq!(refused["head"]["status"], 409);
    let promise = c.send("promise.get", json!({ "id": "root" })).await;
    assert_eq!(promise["data"]["promise"]["state"], "pending");
    let task = c.send("task.get", json!({ "id": "root" })).await;
    assert_eq!(task["data"]["task"]["state"], "acquired");
}

#[tokio::test]
async fn a_heartbeat_moves_the_lease_deadline() {
    let h = harness().await;
    let mut c = h.client("g", "p").await;
    let target = c.network.anycast().to_string();
    let far = h.now() + 3_600_000;
    c.send("promise.create", targeted("root", &target, far))
        .await;
    c.execute().await;
    c.send("task.acquire", acquire("root", 0, "p", 1_000)).await;

    h.advance(Duration::from_millis(600)).await;
    let beat = c
        .send(
            "task.heartbeat",
            json!({ "pid": "p", "tasks": [{ "id": "root", "version": 1 }] }),
        )
        .await;
    assert_eq!(beat["head"]["status"], 200);
    // The original deadline passes without a redelivery.
    h.advance(Duration::from_millis(600)).await;
    c.no_message().await;
    // The moved deadline passes.
    h.advance(Duration::from_millis(500)).await;
    assert_eq!(c.execute().await, ("root".to_string(), 1));
}

#[tokio::test]
async fn an_unacquired_execute_is_sent_again_after_the_retry_timeout() {
    let h = harness().await;
    let mut c = h.client("g", "p").await;
    let target = c.network.anycast().to_string();
    let far = h.now() + 3_600_000;
    c.send("promise.create", targeted("root", &target, far))
        .await;
    assert_eq!(c.execute().await, ("root".to_string(), 0));
    h.advance(RETRY - Duration::from_millis(1)).await;
    c.no_message().await;
    h.advance(Duration::from_millis(2)).await;
    assert_eq!(c.execute().await, ("root".to_string(), 0));
}

#[tokio::test]
async fn a_deadline_settles_the_awaited_promises_and_wakes_the_awaiter() {
    let h = harness().await;
    let mut c = h.client("g", "p").await;
    let target = c.network.anycast().to_string();
    let far = h.now() + 3_600_000;
    let created = c
        .send(
            "task.create",
            json!({
                "pid": "p",
                "ttl": 60_000,
                "action": {
                    "kind": "promise.create",
                    "head": {},
                    "data": targeted("root", &target, far),
                },
            }),
        )
        .await;
    assert_eq!(created["head"]["status"], 200);
    assert_eq!(created["data"]["task"]["state"], "acquired");
    let version = created["data"]["task"]["version"].as_i64().unwrap();

    let deadline = h.now() + 1_000;
    for (id, timer) in [("root:0", "true"), ("root:1", "false")] {
        let fenced = c
            .send(
                "task.fence",
                json!({
                    "id": "root",
                    "version": version,
                    "action": {
                        "kind": "promise.create",
                        "head": {},
                        "data": {
                            "id": id,
                            "timeoutAt": deadline,
                            "param": {},
                            "tags": {
                                "resonate:scope": "global",
                                "resonate:branch": "root",
                                "resonate:timer": timer,
                                "resonate:target": target,
                            },
                        },
                    },
                }),
            )
            .await;
        assert_eq!(fenced["head"]["status"], 200);
        assert_eq!(fenced["data"]["action"]["head"]["status"], 200);
        // A deadline is kept only for a targeted promise, and a targeted
        // promise has a task, so each child's execute message arrives.
        assert_eq!(c.execute().await, (id.to_string(), 0));
    }
    let suspended = c
        .send(
            "task.suspend",
            json!({
                "id": "root",
                "version": version,
                "actions": [
                    { "kind": "promise.register_callback", "head": {},
                      "data": { "awaited": "root:0", "awaiter": "root" } },
                    { "kind": "promise.register_callback", "head": {},
                      "data": { "awaited": "root:1", "awaiter": "root" } },
                ],
            }),
        )
        .await;
    assert_eq!(suspended["head"]["status"], 200);

    h.advance(Duration::from_millis(1_001)).await;
    assert_eq!(c.execute().await, ("root".to_string(), version));
    c.no_message().await;

    let resumed = c
        .send("task.acquire", acquire("root", version, "p", 60_000))
        .await;
    assert_eq!(resumed["head"]["status"], 200);
    assert_eq!(resumed["data"]["promise"]["state"], "pending");
    let preload = resumed["data"]["preload"].as_array().unwrap();
    assert_eq!(preload.len(), 2);
    assert_eq!(preload[0]["id"], "root:0");
    assert_eq!(preload[0]["state"], "resolved");
    assert_eq!(preload[0]["settledAt"], deadline);
    assert_eq!(preload[1]["id"], "root:1");
    assert_eq!(preload[1]["state"], "rejected_timedout");
    assert_eq!(preload[1]["settledAt"], deadline);
}

#[tokio::test]
async fn a_request_naming_an_expired_promise_settles_it_first() {
    let h = harness().await;
    let c = h.client("g", "p").await;
    let deadline = h.now() + 100;
    // No timer is scheduled for a local promise, so only a request can
    // expire it.
    c.send(
        "promise.create",
        json!({ "id": "local", "timeoutAt": deadline, "tags": { "resonate:scope": "local" } }),
    )
    .await;
    h.clock.advance(Duration::from_millis(101));
    let got = c.send("promise.get", json!({ "id": "local" })).await;
    assert_eq!(got["data"]["promise"]["state"], "rejected_timedout");
    assert_eq!(got["data"]["promise"]["settledAt"], deadline);
}

#[tokio::test]
async fn a_settlement_unblocks_every_listener_once() {
    let h = harness().await;
    let mut c = h.client("g", "p").await;
    let far = h.now() + 3_600_000;
    c.send(
        "promise.create",
        json!({ "id": "p", "timeoutAt": far, "tags": { "resonate:scope": "global" } }),
    )
    .await;
    let address = c.network.unicast().to_string();
    for _ in 0..2 {
        let registered = c
            .send(
                "promise.register_listener",
                json!({ "awaited": "p", "address": address }),
            )
            .await;
        assert_eq!(registered["head"]["status"], 200);
    }
    let settled = c
        .send(
            "promise.settle",
            json!({ "id": "p", "state": "resolved", "value": { "data": "eyJ2IjoxfQ==" } }),
        )
        .await;
    assert_eq!(settled["data"]["promise"]["state"], "resolved");
    let unblock = c.message().await;
    assert_eq!(unblock["kind"], "unblock");
    assert_eq!(unblock["data"]["promise"]["id"], "p");
    assert_eq!(unblock["data"]["promise"]["value"]["data"], "eyJ2IjoxfQ==");
    c.no_message().await;
}

#[tokio::test]
async fn a_schedule_fires_and_creates_a_targeted_promise() {
    let h = harness().await;
    let mut c = h.client("g", "p").await;
    let target = c.network.anycast().to_string();
    // T0 is 2023-11-14T22:13:20Z. The next minute boundary is 40 s away.
    let created = c
        .send(
            "schedule.create",
            json!({
                "id": "tick",
                "cron": "* * * * *",
                "promiseId": "{{.id}}-{{.timestamp}}",
                "promiseTimeout": 60_000,
                "promiseParam": { "data": "" },
                "promiseTags": { "resonate:target": target },
            }),
        )
        .await;
    assert_eq!(created["head"]["status"], 200);
    let first = h.now() + 40_000;
    assert_eq!(created["data"]["schedule"]["nextRunAt"], first);

    h.advance(Duration::from_millis(39_999)).await;
    c.no_message().await;
    h.advance(Duration::from_millis(1)).await;
    let (id, version) = c.execute().await;
    assert_eq!(id, format!("tick-{first}"));
    assert_eq!(version, 0);

    let promise = c.send("promise.get", json!({ "id": id })).await;
    assert_eq!(promise["data"]["promise"]["createdAt"], first);
    assert_eq!(promise["data"]["promise"]["timeoutAt"], first + 60_000);
    assert_eq!(
        promise["data"]["promise"]["tags"]["resonate:schedule"],
        "tick"
    );
    let schedule = c.send("schedule.get", json!({ "id": "tick" })).await;
    assert_eq!(schedule["data"]["schedule"]["lastRunAt"], first);
    assert_eq!(schedule["data"]["schedule"]["nextRunAt"], first + 60_000);
}

#[tokio::test]
async fn undelivered_messages_and_timers_survive_a_reopen() {
    let h = harness().await;
    let far = h.now() + 3_600_000;
    {
        // A network that is not started applies requests and does not
        // deliver.
        let network = h.store.network().group("g").pid("p").build();
        let target = network.anycast().to_string();
        let request = json!({
            "kind": "promise.create",
            "head": { "corrId": "c", "version": "2026-04-01" },
            "data": targeted("root", &target, far),
        });
        network.send(request.to_string()).await.unwrap();
    }
    let Harness {
        store,
        clock,
        object_store,
    } = h;
    store.close().await.unwrap();

    let reopened = Harness {
        store: open(object_store.clone(), clock.clone()).await,
        clock,
        object_store,
    };
    let mut c = reopened.client("g", "p").await;
    assert_eq!(c.execute().await, ("root".to_string(), 0));
    reopened.advance(RETRY + Duration::from_millis(1)).await;
    assert_eq!(c.execute().await, ("root".to_string(), 0));
}

#[tokio::test]
async fn a_release_returns_the_task_to_pending_and_delivers_it_again() {
    let h = harness().await;
    let mut c = h.client("g", "p").await;
    let target = c.network.anycast().to_string();
    let far = h.now() + 3_600_000;
    c.send("promise.create", targeted("root", &target, far))
        .await;
    assert_eq!(c.execute().await, ("root".to_string(), 0));
    c.send("task.acquire", acquire("root", 0, "p", 60_000))
        .await;
    let released = c
        .send("task.release", json!({ "id": "root", "version": 1 }))
        .await;
    assert_eq!(released["head"]["status"], 200);
    assert_eq!(c.execute().await, ("root".to_string(), 1));
    let again = c
        .send("task.acquire", acquire("root", 1, "p", 60_000))
        .await;
    assert_eq!(again["data"]["task"]["version"], 2);
}
