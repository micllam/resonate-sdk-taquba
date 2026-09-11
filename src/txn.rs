//! One request's unit of work over the queue: reads through a write
//! buffer, the jobs and timers it produces, the claims it keeps and one
//! commit at the end. Requests run one at a time under the store's
//! lock, so a read outside the commit transaction observes the latest
//! committed state.

use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, UNIX_EPOCH};

use futures_util::TryStreamExt;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use taquba::{
    Claim, ClaimOutcome, EnqueueOptions, EnqueueRequest, Error as QueueError, Queue,
    SettlementEffects,
};

use crate::error::Result;
use crate::keys::{
    PROMISE_PREFIX, SCHEDULE_PREFIX, TASK_PREFIX, TIMER_QUEUE, branch_entry_id, branch_key,
    branch_prefix, promise_key, schedule_key, task_key,
};
use crate::records::{PromiseRecord, ScheduleRecord, StoredPromise, StoredTask, TAG_BRANCH};
use crate::timers::Timer;

/// Page size of the listing scans.
const SCAN_PAGE: usize = 256;

/// The payload of a job on an address queue.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum Delivery {
    /// The dispatch of a task. The execute message is built from the
    /// task's record at delivery, so a redelivery sends the current
    /// version.
    Execute { task: String },
    /// An unblock message, delivered as it is.
    Unblock { message: Value },
}

/// The claims of delivered execute jobs, by task id, held from delivery
/// until the task's settlement. The lease is process state, so a claim
/// does not survive a restart, and the next open requeues its job.
pub(crate) type Held = HashMap<String, Claim>;

/// Work that follows the commit.
enum After {
    /// Acknowledge a delivered claim.
    Ack(Claim),
    /// Hold a delivered execute job's claim for its task. A claim held
    /// before is dropped: its lease has ended, or ends with the
    /// reaper's requeue.
    Hold(String, Claim),
    /// Acknowledge the held claim of a task.
    Release(String),
    /// Fail the held claim of a task, so its job is delivered again.
    Nack(String),
    /// Claim a task's job by id and hold the claim.
    Claim {
        task: String,
        job: String,
        lease: Duration,
    },
}

pub(crate) struct Txn<'a> {
    queue: &'a Queue,
    held: &'a mut Held,
    preload_limit: usize,
    /// The current time in epoch milliseconds.
    pub now: i64,
    /// Entries written by this request. `None` records a deletion.
    writes: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    enqueues: Vec<EnqueueRequest>,
    after: Vec<After>,
}

impl<'a> Txn<'a> {
    pub fn new(queue: &'a Queue, held: &'a mut Held, preload_limit: usize, now: i64) -> Self {
        Self {
            queue,
            held,
            preload_limit,
            now,
            writes: BTreeMap::new(),
            enqueues: Vec::new(),
            after: Vec::new(),
        }
    }

    async fn get_raw(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(written) = self.writes.get(key) {
            return Ok(written.clone());
        }
        Ok(self.queue.kv_get(key).await?.map(|bytes| bytes.to_vec()))
    }

    async fn get_json<T: DeserializeOwned>(&self, key: &[u8]) -> Result<Option<T>> {
        match self.get_raw(key).await? {
            Some(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
            None => Ok(None),
        }
    }

    fn put_json<T: Serialize>(&mut self, key: Vec<u8>, value: &T) {
        let bytes = serde_json::to_vec(value).expect("records serialize without error");
        self.writes.insert(key, Some(bytes));
    }

    pub async fn get_promise(&self, id: &str) -> Result<Option<StoredPromise>> {
        self.get_json(&promise_key(id)).await
    }

    pub fn put_promise(&mut self, promise: &StoredPromise) {
        self.put_json(promise_key(&promise.id), promise);
    }

    pub fn index_branch(&mut self, promise: &PromiseRecord) {
        if let Some(branch) = promise.tags.get(TAG_BRANCH) {
            self.writes
                .insert(branch_key(branch, &promise.id), Some(Vec::new()));
        }
    }

    pub async fn get_task(&self, id: &str) -> Result<Option<StoredTask>> {
        self.get_json(&task_key(id)).await
    }

    pub fn put_task(&mut self, task: &StoredTask) {
        self.put_json(task_key(&task.id), task);
    }

    pub async fn get_schedule(&self, id: &str) -> Result<Option<ScheduleRecord>> {
        self.get_json(&schedule_key(id)).await
    }

    pub fn put_schedule(&mut self, schedule: &ScheduleRecord) {
        self.put_json(schedule_key(&schedule.id), schedule);
    }

    pub fn delete_schedule(&mut self, id: &str) {
        self.writes.insert(schedule_key(id), None);
    }

    /// The promises that share `promise`'s branch, other than the
    /// promise itself, in id order and at most `preload_limit` of them.
    /// A promise indexed by this request is included.
    pub async fn preload(&self, promise: &PromiseRecord) -> Result<Vec<PromiseRecord>> {
        let Some(branch) = promise.tags.get(TAG_BRANCH) else {
            return Ok(Vec::new());
        };
        let prefix = branch_prefix(branch);
        let limit = self.preload_limit;
        let mut ids: Vec<String> = Vec::new();
        {
            let mut entries = std::pin::pin!(self.queue.kv_entries(&prefix, SCAN_PAGE));
            while let Some((key, _)) = entries.try_next().await? {
                let Some(id) = branch_entry_id(&prefix, &key) else {
                    continue;
                };
                if id != promise.id {
                    ids.push(id.to_string());
                }
                if ids.len() > limit {
                    break;
                }
            }
        }
        for (key, value) in self.writes.range(prefix.clone()..) {
            if !key.starts_with(&prefix) {
                break;
            }
            if value.is_some()
                && let Some(id) = branch_entry_id(&prefix, key)
                && id != promise.id
                && !ids.iter().any(|known| known == id)
            {
                ids.push(id.to_string());
            }
        }
        ids.sort();
        ids.truncate(limit);
        let mut records = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(sibling) = self.get_promise(&id).await? {
                records.push(sibling.record);
            }
        }
        Ok(records)
    }

    /// Every promise, in id order.
    pub async fn promises(&self) -> Result<Vec<PromiseRecord>> {
        let stored: Vec<StoredPromise> = self.list(PROMISE_PREFIX).await?;
        Ok(stored.into_iter().map(|p| p.record).collect())
    }

    pub async fn tasks(&self) -> Result<Vec<StoredTask>> {
        self.list(TASK_PREFIX).await
    }

    pub async fn schedules(&self) -> Result<Vec<ScheduleRecord>> {
        self.list(SCHEDULE_PREFIX).await
    }

    /// Every committed entry with `prefix`, in key order.
    async fn list<T: DeserializeOwned>(&self, prefix: &[u8]) -> Result<Vec<T>> {
        let mut records = Vec::new();
        let mut stream = std::pin::pin!(self.queue.kv_entries(prefix, SCAN_PAGE));
        while let Some((_, value)) = stream.try_next().await? {
            records.push(serde_json::from_slice(&value)?);
        }
        Ok(records)
    }

    /// Enqueue an unblock message for the listener at `address`.
    pub fn send_unblock(&mut self, address: &str, message: Value) {
        let payload = Delivery::Unblock { message };
        self.enqueues.push(EnqueueRequest {
            queue: address.to_string(),
            payload: serde_json::to_vec(&payload).expect("a delivery serializes"),
            options: EnqueueOptions::default(),
        });
    }

    /// Enqueue the dispatch of `task` on its target's queue, at `run_at`
    /// when given, and write the task with the new job's id.
    pub fn dispatch(&mut self, task: &mut StoredTask, run_at: Option<i64>) {
        let job = self.queue.next_job_id();
        task.job = Some(job.clone());
        self.put_task(task);
        let payload = Delivery::Execute {
            task: task.id.clone(),
        };
        let run_at = run_at.map(|at| UNIX_EPOCH + Duration::from_millis(at.max(0) as u64));
        self.enqueues.push(EnqueueRequest {
            queue: task.target.clone(),
            payload: serde_json::to_vec(&payload).expect("a delivery serializes"),
            options: EnqueueOptions::default().id_override(job).run_at(run_at),
        });
    }

    /// Schedule `timer` to fire at epoch millisecond `at`.
    pub fn schedule_timer(&mut self, timer: Timer, at: i64) {
        let run_at = UNIX_EPOCH + Duration::from_millis(at.max(0) as u64);
        self.enqueues.push(EnqueueRequest {
            queue: TIMER_QUEUE.to_string(),
            payload: serde_json::to_vec(&timer).expect("a timer serializes"),
            options: EnqueueOptions::default().run_at(run_at),
        });
    }

    /// Whether `claim`'s lease is live at `now`. The lease is read from
    /// the queue's registry, so an expired lease is observed by the
    /// request that depends on it, before the reaper requeues the job.
    fn live(&self, claim: &Claim) -> bool {
        self.queue
            .lease_expiry(&claim.queue, &claim.id)
            .is_some_and(|expiry| i64::try_from(expiry).unwrap_or(i64::MAX) > self.now)
    }

    /// Whether the claim of `task_id`'s job is held with a live lease.
    /// A held claim whose lease ended is dropped: its job is pending
    /// again, or is requeued by the reaper.
    pub fn holds(&mut self, task_id: &str) -> bool {
        match self.held.get(task_id) {
            Some(claim) if self.live(claim) => true,
            Some(_) => {
                self.held.remove(task_id);
                false
            }
            None => false,
        }
    }

    /// Renew the lease of `task_id`'s held claim to `lease`. Returns
    /// whether the claim is held with a live lease and renewed.
    pub fn renew(&mut self, task_id: &str, lease: Duration) -> bool {
        if !self.holds(task_id) {
            return false;
        }
        let claim = &self.held[task_id];
        let renewed = self.queue.renew_lease(claim, lease).is_ok();
        if !renewed {
            self.held.remove(task_id);
        }
        renewed
    }

    /// Claim `job` by id for `task_id` and hold the claim. Returns
    /// whether the job was claimed.
    pub async fn claim_job(&mut self, task_id: &str, job: &str, lease: Duration) -> Result<bool> {
        match self.queue.claim_by_id(job, lease).await? {
            ClaimOutcome::Claimed(claim) => {
                self.held.insert(task_id.to_string(), *claim);
                Ok(true)
            }
            ClaimOutcome::NotClaimable | ClaimOutcome::NotFound => Ok(false),
        }
    }

    /// Whether the job `id` exists in any state.
    pub async fn job_exists(&self, id: &str) -> Result<bool> {
        Ok(self.queue.get_job(id).await?.is_some())
    }

    /// Acknowledge `claim` after the commit.
    pub fn ack_after_commit(&mut self, claim: Claim) {
        self.after.push(After::Ack(claim));
    }

    /// Hold `claim` for `task_id` after the commit.
    pub fn hold_after_commit(&mut self, task_id: &str, claim: Claim) {
        self.after.push(After::Hold(task_id.to_string(), claim));
    }

    /// Acknowledge the held claim of `task_id`, if any, after the commit.
    pub fn release(&mut self, task_id: &str) {
        self.after.push(After::Release(task_id.to_string()));
    }

    /// Fail the held claim of `task_id`, if any, after the commit.
    pub fn nack_after_commit(&mut self, task_id: &str) {
        self.after.push(After::Nack(task_id.to_string()));
    }

    /// Claim `task`'s job by id after the commit and hold the claim. A
    /// job that is not claimable stays where the commit put it.
    pub fn claim_after_commit(&mut self, task: &StoredTask, lease: Duration) {
        let job = task.job.clone().expect("a dispatched task has a job");
        self.after.push(After::Claim {
            task: task.id.clone(),
            job,
            lease,
        });
    }

    fn effects(&mut self) -> SettlementEffects {
        let mut effects = SettlementEffects::default().enqueues(std::mem::take(&mut self.enqueues));
        for (key, value) in std::mem::take(&mut self.writes) {
            match value {
                Some(value) => {
                    effects.kv_writes.insert(key, value);
                }
                None => effects.kv_deletes.push(key),
            }
        }
        effects
    }

    /// Commit every write, delete and enqueue as one transaction, then
    /// do the work that follows the commit. A request without effects
    /// does not commit.
    pub async fn commit(mut self) -> Result<()> {
        let effects = self.effects();
        if !effects.enqueues.is_empty()
            || !effects.kv_writes.is_empty()
            || !effects.kv_deletes.is_empty()
        {
            self.queue.commit_effects(effects).await?;
        }
        self.finish().await
    }

    /// Commit every write, delete and enqueue in the transaction that
    /// acknowledges the held claim of `task_id`, then do the work that
    /// follows the commit. Fails with `ClaimLost` when the claim is not
    /// held or its lease ended, and the store is then unchanged. The
    /// claim stays held on any other error.
    pub async fn commit_ack(mut self, task_id: &str) -> Result<()> {
        if !self.holds(task_id) {
            return Err(QueueError::ClaimLost.into());
        }
        let effects = self.effects();
        let acked = self.queue.ack_with(&self.held[task_id], effects).await;
        match acked {
            Ok(_) => {
                self.held.remove(task_id);
                self.finish().await
            }
            Err(QueueError::ClaimLost) => {
                self.held.remove(task_id);
                Err(QueueError::ClaimLost.into())
            }
            Err(e) => Err(e.into()),
        }
    }

    /// A claim whose lease ended was requeued by the reaper already, so
    /// a settlement that reports `ClaimLost` has had its effect.
    async fn finish(&mut self) -> Result<()> {
        for after in std::mem::take(&mut self.after) {
            let settled = match after {
                After::Ack(claim) => self.queue.ack(&claim).await,
                After::Hold(task, claim) => {
                    self.held.insert(task, claim);
                    Ok(())
                }
                After::Release(task) => match self.held.remove(&task) {
                    Some(claim) => self.queue.ack(&claim).await,
                    None => Ok(()),
                },
                After::Nack(task) => match self.held.remove(&task) {
                    Some(claim) => self.queue.nack(&claim, "released").await,
                    None => Ok(()),
                },
                After::Claim { task, job, lease } => {
                    if let ClaimOutcome::Claimed(claim) =
                        self.queue.claim_by_id(&job, lease).await?
                    {
                        self.held.insert(task, *claim);
                    }
                    Ok(())
                }
            };
            match settled {
                Ok(()) | Err(QueueError::ClaimLost) => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }
}
