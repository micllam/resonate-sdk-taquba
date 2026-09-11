//! The [`Network`] implementation: one per SDK instance, over a shared
//! store.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use resonate_sdk::error::Result;
use resonate_sdk::network::Network;
use taquba::{Claim, Queue, WorkerHandle};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::envelope::{Reply, Request, parse_request, response};
use crate::kernel;
use crate::store::Shared;

type Subscribers = Arc<RwLock<Vec<Box<dyn Fn(String) + Send + Sync>>>>;

/// A [`Network`] over a [`TaqubaStore`](crate::TaqubaStore). Requests
/// are applied to the store directly, and the store's messages for this
/// instance's addresses are delivered to the callback registered with
/// [`Network::recv`] while the network is started.
///
/// The unicast address is `poll://uni@{group}/{pid}` and the anycast
/// address `poll://any@{group}`. A target resolves to the anycast
/// address of that group, and a message is delivered to the network
/// whose address equals the message's address.
pub struct TaqubaNetwork {
    shared: Arc<Shared>,
    pid: String,
    group: String,
    unicast: String,
    anycast: String,
    subscribers: Subscribers,
    workers: Mutex<Vec<WorkerHandle<()>>>,
}

/// Builder for [`TaqubaNetwork`], returned by
/// [`TaqubaStore::network`](crate::TaqubaStore::network).
pub struct TaqubaNetworkBuilder {
    shared: Arc<Shared>,
    pid: Option<String>,
    group: Option<String>,
}

impl TaqubaNetworkBuilder {
    pub(crate) fn new(shared: Arc<Shared>) -> Self {
        Self {
            shared,
            pid: None,
            group: None,
        }
    }

    /// The worker id. Defaults to a generated id.
    #[must_use]
    pub fn pid(mut self, pid: impl Into<String>) -> Self {
        self.pid = Some(pid.into());
        self
    }

    /// The routing group. Defaults to `"default"`.
    #[must_use]
    pub fn group(mut self, group: impl Into<String>) -> Self {
        self.group = Some(group.into());
        self
    }

    /// Build the network.
    pub fn build(self) -> TaqubaNetwork {
        let pid = self.pid.unwrap_or_else(generated_pid);
        let group = self.group.unwrap_or_else(|| "default".to_string());
        TaqubaNetwork {
            unicast: format!("poll://uni@{group}/{pid}"),
            anycast: format!("poll://any@{group}"),
            shared: self.shared,
            pid,
            group,
            subscribers: Arc::new(RwLock::new(Vec::new())),
            workers: Mutex::new(Vec::new()),
        }
    }
}

/// A worker id distinct across processes and across builds within one
/// process.
fn generated_pid() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{:x}{:x}{:x}", std::process::id(), nanos, n)
}

impl TaqubaNetwork {
    /// Apply one request and return the time the store applied it at,
    /// in epoch milliseconds, with the response envelope. The time is
    /// read while the store's lock is held, so the times of successive
    /// calls do not decrease and a timer applied between two calls has a
    /// time between the calls' times. A trace recorder reads its `now`
    /// from this value.
    pub async fn send_timed(&self, raw: &str) -> (i64, String) {
        let request = match parse_request(raw) {
            Ok(request) => request,
            Err((kind, corr_id, reply)) => {
                return (
                    self.shared.now(),
                    response(&kind, &corr_id, reply).to_string(),
                );
            }
        };
        let Request { kind, head, data } = request;
        let (now, reply) = {
            let mut held = self.shared.lock.lock().await;
            let txn = self.shared.txn(&mut held);
            let now = txn.now;
            let reply = match kernel::handle(txn, &kind, &head.corr_id, data).await {
                Ok(reply) => reply,
                Err(e) => {
                    warn!(kind = %kind, "request failed: {e}");
                    Reply::err(500, e.to_string())
                }
            };
            (now, reply)
        };
        debug!(kind = %kind, status = reply.status(), "request applied");
        (now, response(&kind, &head.corr_id, reply).to_string())
    }
}

#[async_trait::async_trait]
impl Network for TaqubaNetwork {
    fn pid(&self) -> &str {
        &self.pid
    }

    fn group(&self) -> &str {
        &self.group
    }

    fn unicast(&self) -> &str {
        &self.unicast
    }

    fn anycast(&self) -> &str {
        &self.anycast
    }

    async fn start(&self) -> Result<()> {
        let mut workers = self.workers.lock().await;
        if !workers.is_empty() {
            return Ok(());
        }
        for address in [self.unicast.clone(), self.anycast.clone()] {
            let loop_ = DeliveryLoop {
                shared: self.shared.clone(),
                subscribers: self.subscribers.clone(),
                address,
            };
            let handle =
                WorkerHandle::spawn(std::future::pending::<()>(), move |stop| async move {
                    loop_.run(stop).await
                });
            workers.push(handle);
        }
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        let handles: Vec<WorkerHandle<()>> = std::mem::take(&mut *self.workers.lock().await);
        for handle in handles {
            handle.shutdown().await;
        }
        self.subscribers
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clear();
        Ok(())
    }

    async fn send(&self, req: String) -> Result<String> {
        Ok(self.send_timed(&req).await.1)
    }

    fn recv(&self, callback: Box<dyn Fn(String) + Send + Sync>) {
        self.subscribers
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(callback);
    }

    fn target_resolver(&self, target: &str) -> String {
        format!("poll://any@{target}")
    }
}

/// Delivers the jobs of one address: a claim loop that keeps the claim
/// of an execute job for the task from delivery until the task's
/// settlement, and acknowledges an unblock job once delivered. The
/// claim's lease is the retry timeout, so a task that is not acquired
/// in time is delivered again by the reaper's requeue.
struct DeliveryLoop {
    shared: Arc<Shared>,
    subscribers: Subscribers,
    address: String,
}

impl DeliveryLoop {
    async fn run(self, stop: CancellationToken) {
        let queue: &Queue = &self.shared.queue;
        loop {
            let claimed = tokio::select! {
                biased;
                _ = stop.cancelled() => return,
                claimed = queue.claim_with_wait(
                    &self.address,
                    self.shared.retry_timeout,
                    self.shared.poll_interval,
                ) => claimed,
            };
            let claim = match claimed {
                Ok(Some(claim)) => claim,
                Ok(None) => continue,
                Err(e) => {
                    warn!(address = %self.address, "delivery claim failed: {e}");
                    tokio::time::sleep(self.shared.poll_interval).await;
                    continue;
                }
            };
            if let Err(e) = self.deliver(claim).await {
                warn!(address = %self.address, "delivery failed: {e}");
            }
        }
    }

    async fn deliver(&self, claim: Claim) -> crate::error::Result<()> {
        let message = {
            let mut held = self.shared.lock.lock().await;
            kernel::deliver(self.shared.txn(&mut held), claim).await?
        };
        if let Some(message) = message {
            let raw = message.to_string();
            let subscribers = self
                .subscribers
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            for subscriber in subscribers.iter() {
                subscriber(raw.clone());
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{StoreOptions, TaqubaStore};
    use taquba::object_store::memory::InMemory;

    async fn store() -> TaqubaStore {
        TaqubaStore::open_with_options(
            Arc::new(InMemory::new()),
            "resonate",
            taquba::OpenOptions::default(),
            StoreOptions::default(),
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn defaults_are_the_default_group_and_a_generated_pid() {
        let store = store().await;
        let a = store.network().build();
        let b = store.network().build();
        assert_eq!(a.group(), "default");
        assert!(!a.pid().is_empty());
        assert_ne!(a.pid(), b.pid());
        assert_eq!(a.unicast(), format!("poll://uni@default/{}", a.pid()));
        assert_eq!(a.anycast(), "poll://any@default");
    }

    #[tokio::test]
    async fn stop_before_start_is_ok() {
        let store = store().await;
        let network = store.network().pid("p").group("g").build();
        network.stop().await.unwrap();
        network.start().await.unwrap();
        network.stop().await.unwrap();
    }
}
