//! The protocol's state machine over one request. Each handler reads
//! the records the request refers to, applies the transition, writes
//! the results through the request's [`Txn`] and commits.
//!
//! A task's dispatch is a job on its target's queue. Delivery of the
//! job claims it, the acquire and every heartbeat renew the claim's
//! lease, a fulfil or a suspend acknowledges it with the settlement's
//! effects, a release fails it, and a lease that expires returns the job
//! to pending, where the next delivery sends the execute message again.
//!
//! Every handler except the heartbeat first applies the deadline of
//! each promise it refers to: a pending promise past its `timeoutAt` is
//! settled before the request is evaluated, so a request never observes
//! an expired promise as pending.

use std::collections::BTreeSet;
use std::time::Duration;

use serde::Deserialize;
use serde_json::{Value, json};
use taquba::{Claim, Error as QueueError};

use crate::cron;
use crate::envelope::Reply;
use crate::error::{Error, Result};
use crate::records::{
    PROTOCOL_VERSION, PromiseRecord, PromiseState, PromiseValue, ScheduleRecord, SettleState,
    StoredPromise, StoredTask, TAG_DELAY, TAG_TARGET, Tags, TaskState, origin_of,
};
use crate::timers::Timer;
use crate::txn::{Delivery, Txn};

const DEFAULT_SEARCH_LIMIT: usize = 100;
const DEFAULT_SCHEDULE_SEARCH_LIMIT: usize = 10;
const MAX_SEARCH_LIMIT: usize = 1000;

/// Parse the request data, or return a 400 reply.
macro_rules! parse {
    ($data:expr) => {
        match serde_json::from_value($data) {
            Ok(data) => data,
            Err(e) => return Ok(Reply::err(400, format!("Invalid request: {e}"))),
        }
    };
}

/// The page size of a search, or return a 400 reply.
macro_rules! limit {
    ($limit:expr, $default:expr) => {
        match search_limit($limit, $default) {
            Ok(limit) => limit,
            Err(reply) => return Ok(reply),
        }
    };
}

/// Return an early reply.
macro_rules! reject {
    ($reply:expr) => {
        if let Some(reply) = $reply {
            return Ok(reply);
        }
    };
}

fn null_default<'de, D, T>(deserializer: D) -> std::result::Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Default + Deserialize<'de>,
{
    Ok(Option::<T>::deserialize(deserializer)?.unwrap_or_default())
}

#[derive(Deserialize)]
struct IdData {
    id: String,
}

#[derive(Deserialize)]
struct PromiseCreateData {
    id: String,
    #[serde(rename = "timeoutAt")]
    timeout_at: i64,
    #[serde(default, deserialize_with = "null_default")]
    param: PromiseValue,
    #[serde(default, deserialize_with = "null_default")]
    tags: Tags,
}

#[derive(Deserialize)]
struct PromiseSettleData {
    id: String,
    state: SettleState,
    #[serde(default, deserialize_with = "null_default")]
    value: PromiseValue,
}

#[derive(Deserialize)]
struct CallbackData {
    awaited: String,
    awaiter: String,
}

#[derive(Deserialize)]
struct ListenerData {
    awaited: String,
    address: String,
}

#[derive(Deserialize)]
struct PromiseSearchData {
    #[serde(default)]
    state: Option<PromiseState>,
    #[serde(default)]
    tags: Option<Tags>,
    #[serde(default)]
    limit: Option<i64>,
    #[serde(default)]
    cursor: Option<String>,
}

/// A request nested in a task request.
#[derive(Deserialize)]
struct Action<T> {
    kind: String,
    data: T,
}

#[derive(Deserialize)]
struct TaskCreateData {
    pid: String,
    ttl: i64,
    action: Action<PromiseCreateData>,
}

#[derive(Deserialize)]
struct TaskAcquireData {
    id: String,
    version: i64,
    pid: String,
    ttl: i64,
}

#[derive(Deserialize)]
struct TaskVersionData {
    id: String,
    version: i64,
}

#[derive(Deserialize)]
struct TaskSuspendData {
    id: String,
    version: i64,
    #[serde(default)]
    actions: Vec<Action<CallbackData>>,
}

#[derive(Deserialize)]
struct TaskFulfillData {
    id: String,
    version: i64,
    action: Action<PromiseSettleData>,
}

#[derive(Deserialize)]
struct TaskFenceData {
    id: String,
    version: i64,
    action: Action<Value>,
}

#[derive(Deserialize)]
struct TaskHeartbeatData {
    pid: String,
    #[serde(default)]
    tasks: Vec<TaskVersionData>,
}

#[derive(Deserialize)]
struct TaskSearchData {
    #[serde(default)]
    state: Option<TaskState>,
    #[serde(default)]
    limit: Option<i64>,
    #[serde(default)]
    cursor: Option<String>,
}

#[derive(Deserialize)]
struct ScheduleCreateData {
    id: String,
    cron: String,
    #[serde(rename = "promiseId")]
    promise_id: String,
    #[serde(rename = "promiseTimeout")]
    promise_timeout: i64,
    #[serde(rename = "promiseParam", default, deserialize_with = "null_default")]
    promise_param: PromiseValue,
    #[serde(rename = "promiseTags", default, deserialize_with = "null_default")]
    promise_tags: Tags,
}

#[derive(Deserialize)]
struct ScheduleSearchData {
    #[serde(default)]
    tags: Option<Tags>,
    #[serde(default)]
    limit: Option<i64>,
    #[serde(default)]
    cursor: Option<String>,
}

/// Dispatch one request of `kind` with `data`. Every handler commits
/// its own transaction.
pub(crate) async fn handle(txn: Txn<'_>, kind: &str, corr_id: &str, data: Value) -> Result<Reply> {
    match kind {
        "promise.create" => promise_create(txn, parse!(data)).await,
        "promise.get" => promise_get(txn, parse!(data)).await,
        "promise.settle" => promise_settle(txn, parse!(data)).await,
        "promise.register_callback" => promise_register_callback(txn, parse!(data)).await,
        "promise.register_listener" => promise_register_listener(txn, parse!(data)).await,
        "promise.search" => promise_search(txn, parse!(data)).await,
        "task.create" => task_create(txn, parse!(data)).await,
        "task.acquire" => task_acquire(txn, parse!(data)).await,
        "task.release" => task_release(txn, parse!(data)).await,
        "task.fulfill" => task_fulfill(txn, parse!(data)).await,
        "task.suspend" => task_suspend(txn, parse!(data)).await,
        "task.fence" => task_fence(txn, parse!(data), corr_id).await,
        "task.heartbeat" => task_heartbeat(txn, parse!(data)).await,
        "task.get" => task_get(txn, parse!(data)).await,
        "task.halt" => task_halt(txn, parse!(data)).await,
        "task.continue" => task_continue(txn, parse!(data)).await,
        "task.search" => task_search(txn, parse!(data)).await,
        "schedule.create" => schedule_create(txn, parse!(data)).await,
        "schedule.get" => schedule_get(txn, parse!(data)).await,
        "schedule.delete" => schedule_delete(txn, parse!(data)).await,
        "schedule.search" => schedule_search(txn, parse!(data)).await,
        other => Ok(Reply::err(400, format!("Unknown operation: {other}"))),
    }
}

/// Reply with `reply` after committing `txn`.
async fn done(txn: Txn<'_>, reply: Reply) -> Result<Reply> {
    txn.commit().await?;
    Ok(reply)
}

fn is_url(address: &str) -> bool {
    url::Url::parse(address).is_ok()
}

/// The lease of a task with a `ttl` of milliseconds.
fn lease_of(ttl: i64) -> Duration {
    Duration::from_millis(ttl.max(0) as u64)
}

/// Whether `ancestor` is `id` or a lineage prefix of it. The segment
/// separator after an ancestor without an origin separator is `:`, and
/// `.` after one with it.
fn is_ancestor(ancestor: &str, id: &str) -> bool {
    if ancestor == id {
        return true;
    }
    let separator = if ancestor.contains(':') { '.' } else { ':' };
    id.strip_prefix(ancestor)
        .is_some_and(|rest| rest.starts_with(separator))
}

fn validate_create(data: &PromiseCreateData) -> Option<Reply> {
    if data.id.is_empty() {
        return Some(Reply::err(400, "Promise ID is required"));
    }
    if data.id.contains('\0') {
        return Some(Reply::err(400, "Promise ID must not contain null bytes"));
    }
    if data.timeout_at < 0 {
        return Some(Reply::err(400, "Promise timeout must not be negative"));
    }
    if let Some(target) = data.tags.get(TAG_TARGET)
        && !is_url(target)
    {
        return Some(Reply::err(400, "Invalid resonate:target address"));
    }
    if let Some(origin) = data.tags.get("resonate:origin")
        && (origin.contains(':') || origin_of(&data.id) != origin)
    {
        return Some(Reply::err(400, "Invalid resonate:origin tag"));
    }
    for tag in ["resonate:branch", "resonate:parent"] {
        if let Some(ancestor) = data.tags.get(tag)
            && !is_ancestor(ancestor, &data.id)
        {
            return Some(Reply::err(400, format!("Invalid {tag} tag")));
        }
    }
    if let Some(prefix) = data.tags.get("resonate:prefix")
        && prefix.contains(':')
    {
        return Some(Reply::err(400, "Invalid resonate:prefix tag"));
    }
    if let Some(delay) = data.tags.get(TAG_DELAY) {
        let valid = delay
            .parse::<i64>()
            .is_ok_and(|delay| delay >= 0 && delay < data.timeout_at)
            && data.tags.contains_key(TAG_TARGET);
        if !valid {
            return Some(Reply::err(400, "Invalid resonate:delay tag"));
        }
    }
    None
}

fn validate_callback(awaited: &str, awaiter: &str) -> Option<Reply> {
    if awaited.is_empty() || awaiter.is_empty() {
        return Some(Reply::err(
            400,
            "Awaited and awaiter promise IDs are required",
        ));
    }
    if awaited == awaiter {
        return Some(Reply::err(400, "Awaited and awaiter must differ"));
    }
    if origin_of(awaited) != origin_of(awaiter) {
        return Some(Reply::err(400, "Awaited and awaiter must share an origin"));
    }
    None
}

fn search_limit(limit: Option<i64>, default: usize) -> std::result::Result<usize, Reply> {
    match limit {
        None => Ok(default),
        Some(limit) if (1..=MAX_SEARCH_LIMIT as i64).contains(&limit) => Ok(limit as usize),
        Some(_) => Err(Reply::err(
            400,
            format!("Invalid 'limit': must be between 1 and {MAX_SEARCH_LIMIT}"),
        )),
    }
}

/// One page of `items`, which are in id order: the items after
/// `cursor`, at most `limit` of them, and the cursor of the next page
/// when more remain.
fn page<T>(
    items: Vec<T>,
    id_of: impl Fn(&T) -> &str,
    cursor: Option<&str>,
    limit: usize,
) -> (Vec<T>, Option<String>) {
    let mut page: Vec<T> = items
        .into_iter()
        .filter(|item| cursor.is_none_or(|cursor| id_of(item) > cursor))
        .take(limit + 1)
        .collect();
    let next = (page.len() > limit).then(|| {
        page.truncate(limit);
        id_of(page.last().expect("a full page has a last item")).to_string()
    });
    (page, next)
}

fn tags_match(tags: &Tags, filter: Option<&Tags>) -> bool {
    filter.is_none_or(|filter| {
        filter
            .iter()
            .all(|(key, value)| tags.get(key) == Some(value))
    })
}

fn with_cursor(mut data: Value, cursor: Option<String>) -> Value {
    if let Some(cursor) = cursor {
        data["cursor"] = Value::String(cursor);
    }
    data
}

/// Settle every named promise whose deadline has passed.
async fn try_timeout(txn: &mut Txn<'_>, ids: &[&str]) -> Result<()> {
    for id in ids {
        if let Some(mut promise) = txn.get_promise(id).await?
            && promise.is_pending()
            && txn.now >= promise.record.timeout_at
        {
            expire_promise(txn, &mut promise).await?;
        }
    }
    Ok(())
}

/// Settle a pending promise by its deadline. The settlement is recorded
/// at the deadline and the value is unchanged.
pub(crate) async fn expire_promise(txn: &mut Txn<'_>, promise: &mut StoredPromise) -> Result<()> {
    promise.record.state = promise.timeout_state();
    promise.record.settled_at = Some(promise.record.timeout_at);
    settle_chain(txn, promise).await
}

/// Settle a pending promise by a client at `now`.
async fn settle(
    txn: &mut Txn<'_>,
    promise: &mut StoredPromise,
    state: SettleState,
    value: PromiseValue,
) -> Result<()> {
    promise.record.state = state.into();
    promise.record.value = value;
    promise.record.settled_at = Some(txn.now);
    settle_chain(txn, promise).await
}

/// The consequences of a settlement: the promise's own task is
/// fulfilled and its held claim released, every registered awaiter is
/// resumed and every listener receives an unblock message.
async fn settle_chain(txn: &mut Txn<'_>, promise: &mut StoredPromise) -> Result<()> {
    if let Some(mut task) = txn.get_task(&promise.record.id).await?
        && task.state != TaskState::Fulfilled
    {
        task.transition(TaskState::Fulfilled);
        task.resumes.clear();
        txn.put_task(&task);
        txn.release(&task.id);
    }
    let callbacks = std::mem::take(&mut promise.callbacks);
    let listeners = std::mem::take(&mut promise.listeners);
    txn.put_promise(promise);
    for awaiter in callbacks {
        if let Some(awaiting) = txn.get_promise(&awaiter).await?
            && awaiting.is_pending()
            && txn.now < awaiting.record.timeout_at
        {
            resume_awaiter(txn, &awaiter, &promise.record.id, Wake::Fanout).await?;
        }
    }
    if !listeners.is_empty() {
        let unblock = json!({
            "kind": "unblock",
            "head": {},
            "data": { "promise": promise.record },
        });
        for address in listeners {
            txn.send_unblock(&address, unblock.clone());
        }
    }
    Ok(())
}

/// How a settled awaited promise reached an awaiter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Wake {
    /// The awaited promise settled while the callback was registered.
    Fanout,
    /// The callback was registered on an awaited promise already settled.
    Registration,
}

/// Record a settled awaited promise against its awaiter's task and
/// dispatch a suspended task again.
async fn resume_awaiter(txn: &mut Txn<'_>, awaiter: &str, awaited: &str, wake: Wake) -> Result<()> {
    let Some(mut task) = txn.get_task(awaiter).await? else {
        return Ok(());
    };
    match task.state {
        TaskState::Suspended => {
            task.transition(TaskState::Pending);
            task.resumes = BTreeSet::from([awaited.to_string()]);
            txn.dispatch(&mut task, None);
        }
        TaskState::Pending | TaskState::Acquired => {
            task.resumes.insert(awaited.to_string());
            txn.put_task(&task);
        }
        TaskState::Halted if wake == Wake::Fanout => {
            task.resumes.insert(awaited.to_string());
            txn.put_task(&task);
        }
        TaskState::Halted | TaskState::Fulfilled => {}
    }
    Ok(())
}

/// Acquire a delivered task for `pid` with a lease of `ttl`
/// milliseconds: the version increments and the held claim's lease is
/// renewed. `false` when the task's claim is not held or its lease has
/// ended.
fn acquire(txn: &mut Txn<'_>, task: &mut StoredTask, pid: String, ttl: i64) -> bool {
    if !txn.renew(&task.id, lease_of(ttl)) {
        return false;
    }
    task.state = TaskState::Acquired;
    task.version += 1;
    task.pid = Some(pid);
    task.ttl = Some(ttl);
    task.resumes.clear();
    txn.put_task(task);
    true
}

/// How a created promise's task starts.
enum CreateMode {
    /// A targeted promise's task is pending and its dispatch enqueued,
    /// after `resonate:delay` when the tag's time is in the future.
    Dispatch,
    /// A targeted promise's task is acquired by the caller: its job is
    /// enqueued scheduled at the end of the lease and claimed after the
    /// commit.
    Acquire { pid: String, ttl: i64 },
}

/// Create a promise that does not exist. A promise whose deadline has
/// passed is created in its timeout state, with `createdAt` equal to
/// the deadline.
async fn create_promise(
    txn: &mut Txn<'_>,
    data: PromiseCreateData,
    created_at: Option<i64>,
    mode: CreateMode,
) -> Result<StoredPromise> {
    let now = txn.now;
    let already = now >= data.timeout_at;
    let mut promise = StoredPromise {
        record: PromiseRecord {
            id: data.id,
            state: PromiseState::Pending,
            param: data.param,
            value: PromiseValue::default(),
            tags: data.tags,
            timeout_at: data.timeout_at,
            created_at: if already {
                data.timeout_at
            } else {
                created_at.unwrap_or(now)
            },
            settled_at: None,
        },
        callbacks: Vec::new(),
        listeners: Vec::new(),
    };
    if already {
        promise.record.state = promise.timeout_state();
        promise.record.settled_at = Some(data.timeout_at);
    }
    txn.index_branch(&promise.record);
    txn.put_promise(&promise);
    if let Some(target) = promise.target() {
        let mut task = StoredTask::new(
            promise.record.id.clone(),
            target.to_string(),
            TaskState::Pending,
        );
        if already {
            task.state = TaskState::Fulfilled;
            txn.put_task(&task);
        } else {
            match mode {
                CreateMode::Acquire { pid, ttl } => {
                    task.state = TaskState::Acquired;
                    task.version = 1;
                    task.pid = Some(pid);
                    task.ttl = Some(ttl);
                    txn.dispatch(&mut task, Some(now.saturating_add(ttl)));
                    txn.claim_after_commit(&task, lease_of(ttl));
                }
                CreateMode::Dispatch => {
                    let delay = promise
                        .record
                        .tags
                        .get(TAG_DELAY)
                        .and_then(|delay| delay.parse::<i64>().ok())
                        .filter(|delay| now < *delay);
                    txn.dispatch(&mut task, delay);
                }
            }
        }
    }
    if promise.is_pending() && promise.is_external() {
        txn.schedule_timer(
            Timer::PromiseTimeout {
                id: promise.record.id.clone(),
            },
            promise.record.timeout_at,
        );
    }
    Ok(promise)
}

/// Fire a schedule at its `nextRunAt`: create the schedule's promise,
/// unless it exists, and advance the schedule by one firing.
pub(crate) async fn fire_schedule(txn: &mut Txn<'_>, mut schedule: ScheduleRecord) -> Result<()> {
    let fired_at = schedule.next_run_at;
    let promise_id = schedule
        .promise_id
        .replace("{{.id}}", &schedule.id)
        .replace("{{.timestamp}}", &fired_at.to_string());
    if txn.get_promise(&promise_id).await?.is_none() {
        let mut tags = schedule.promise_tags.clone();
        tags.insert("resonate:schedule".to_string(), schedule.id.clone());
        for key in [
            "resonate:origin",
            "resonate:branch",
            "resonate:parent",
            "resonate:prefix",
        ] {
            tags.insert(key.to_string(), promise_id.clone());
        }
        let data = PromiseCreateData {
            id: promise_id,
            timeout_at: fired_at.saturating_add(schedule.promise_timeout),
            param: schedule.promise_param.clone(),
            tags,
        };
        create_promise(txn, data, Some(fired_at), CreateMode::Dispatch).await?;
    }
    schedule.last_run_at = Some(fired_at);
    if let Some(next) = cron::next_after(&schedule.cron, fired_at) {
        schedule.next_run_at = next;
        txn.schedule_timer(
            Timer::Schedule {
                id: schedule.id.clone(),
                at: next,
            },
            next,
        );
    }
    txn.put_schedule(&schedule);
    Ok(())
}

/// Deliver a claimed job: the message to pass to the callback, or
/// `None` when the job is stale and is acknowledged. An unblock job is
/// acknowledged with its delivery. An execute job's claim is held for
/// the task, and a task that was acquired when its job came back has
/// lost its lease and returns to pending with its version unchanged.
pub(crate) async fn deliver(mut txn: Txn<'_>, claim: Claim) -> Result<Option<Value>> {
    let delivery: Delivery = serde_json::from_slice(&claim.payload)?;
    let id = match delivery {
        Delivery::Unblock { message } => {
            txn.ack_after_commit(claim);
            txn.commit().await?;
            return Ok(Some(message));
        }
        Delivery::Execute { task } => task,
    };
    try_timeout(&mut txn, &[&id]).await?;
    let task = txn.get_task(&id).await?;
    let version = match task {
        Some(mut task) if task.state == TaskState::Acquired => {
            task.transition(TaskState::Pending);
            txn.put_task(&task);
            task.version
        }
        Some(task) if task.state == TaskState::Pending => task.version,
        _ => {
            txn.ack_after_commit(claim);
            txn.commit().await?;
            return Ok(None);
        }
    };
    let message = json!({
        "kind": "execute",
        "head": {},
        "data": { "task": { "id": id, "version": version } },
    });
    txn.hold_after_commit(&id, claim);
    txn.commit().await?;
    Ok(Some(message))
}

async fn task_view(txn: &mut Txn<'_>, task: &StoredTask) -> Result<Value> {
    let promise = txn
        .get_promise(&task.id)
        .await?
        .expect("a task's promise exists");
    let preload = if task.state == TaskState::Fulfilled {
        Vec::new()
    } else {
        txn.preload(&promise.record).await?
    };
    Ok(json!({
        "task": task.record(),
        "promise": promise.record,
        "preload": preload,
    }))
}

async fn promise_create(mut txn: Txn<'_>, data: PromiseCreateData) -> Result<Reply> {
    reject!(validate_create(&data));
    try_timeout(&mut txn, &[&data.id]).await?;
    if let Some(existing) = txn.get_promise(&data.id).await? {
        return done(txn, Reply::ok(json!({ "promise": existing.record }))).await;
    }
    let promise = create_promise(&mut txn, data, None, CreateMode::Dispatch).await?;
    done(txn, Reply::ok(json!({ "promise": promise.record }))).await
}

async fn promise_get(mut txn: Txn<'_>, data: IdData) -> Result<Reply> {
    try_timeout(&mut txn, &[&data.id]).await?;
    let reply = match txn.get_promise(&data.id).await? {
        Some(promise) => Reply::ok(json!({ "promise": promise.record })),
        None => Reply::err(404, "Promise not found"),
    };
    done(txn, reply).await
}

async fn promise_settle(mut txn: Txn<'_>, data: PromiseSettleData) -> Result<Reply> {
    try_timeout(&mut txn, &[&data.id]).await?;
    let Some(mut promise) = txn.get_promise(&data.id).await? else {
        return done(txn, Reply::err(404, "Promise not found")).await;
    };
    if promise.is_pending() {
        settle(&mut txn, &mut promise, data.state, data.value).await?;
    }
    done(txn, Reply::ok(json!({ "promise": promise.record }))).await
}

async fn promise_register_callback(mut txn: Txn<'_>, data: CallbackData) -> Result<Reply> {
    reject!(validate_callback(&data.awaited, &data.awaiter));
    try_timeout(&mut txn, &[&data.awaited, &data.awaiter]).await?;
    let Some(mut awaited) = txn.get_promise(&data.awaited).await? else {
        return done(txn, Reply::err(404, "Awaited promise not found")).await;
    };
    let Some(awaiter) = txn.get_promise(&data.awaiter).await? else {
        return done(txn, Reply::err(422, "Awaiter promise not found")).await;
    };
    if awaiter.target().is_none() {
        let reply = Reply::err(422, "Awaiter promise has no resonate:target tag");
        return done(txn, reply).await;
    }
    if !awaited.is_external() {
        return done(txn, Reply::err(422, "Awaited promise is not awaitable")).await;
    }
    if awaiter.is_pending() {
        if awaited.is_pending() {
            register_callback(&mut txn, &mut awaited, &data.awaiter);
        } else {
            resume_awaiter(&mut txn, &data.awaiter, &data.awaited, Wake::Registration).await?;
        }
    }
    done(txn, Reply::ok(json!({ "promise": awaited.record }))).await
}

fn register_callback(txn: &mut Txn<'_>, awaited: &mut StoredPromise, awaiter: &str) {
    if !awaited.callbacks.iter().any(|id| id == awaiter) {
        awaited.callbacks.push(awaiter.to_string());
        txn.put_promise(awaited);
    }
}

async fn promise_register_listener(mut txn: Txn<'_>, data: ListenerData) -> Result<Reply> {
    if !is_url(&data.address) {
        return Ok(Reply::err(400, "Invalid listener address"));
    }
    try_timeout(&mut txn, &[&data.awaited]).await?;
    let Some(mut awaited) = txn.get_promise(&data.awaited).await? else {
        return done(txn, Reply::err(404, "Awaited promise not found")).await;
    };
    if !awaited.is_external() {
        return done(txn, Reply::err(422, "Awaited promise is not awaitable")).await;
    }
    if awaited.is_pending() && !awaited.listeners.contains(&data.address) {
        awaited.listeners.push(data.address);
        txn.put_promise(&awaited);
    }
    done(txn, Reply::ok(json!({ "promise": awaited.record }))).await
}

async fn promise_search(txn: Txn<'_>, data: PromiseSearchData) -> Result<Reply> {
    let limit = limit!(data.limit, DEFAULT_SEARCH_LIMIT);
    let promises: Vec<PromiseRecord> = txn
        .promises()
        .await?
        .into_iter()
        .filter(|p| data.state.is_none_or(|state| p.state == state))
        .filter(|p| tags_match(&p.tags, data.tags.as_ref()))
        .collect();
    let (page, cursor) = page(promises, |p| &p.id, data.cursor.as_deref(), limit);
    Ok(Reply::ok(with_cursor(json!({ "promises": page }), cursor)))
}

async fn task_create(mut txn: Txn<'_>, data: TaskCreateData) -> Result<Reply> {
    if data.action.kind != "promise.create" {
        return Ok(Reply::err(400, "Invalid action kind"));
    }
    if data.pid.is_empty() {
        return Ok(Reply::err(400, "Process ID is required"));
    }
    if data.ttl < 1 {
        return Ok(Reply::err(400, "TTL must be positive"));
    }
    let create = data.action.data;
    reject!(validate_create(&create));
    if !create.tags.contains_key(TAG_TARGET) {
        return Ok(Reply::err(400, "resonate:target tag is required"));
    }
    if create.tags.contains_key(TAG_DELAY) {
        return Ok(Reply::err(400, "resonate:delay tag is not allowed"));
    }
    try_timeout(&mut txn, &[&create.id]).await?;
    if let Some(mut task) = txn.get_task(&create.id).await? {
        return match task.state {
            TaskState::Pending => {
                // The delivered job's claim is held, or the job is
                // claimed by id from the queue.
                if !txn.holds(&task.id) {
                    let claimed = match &task.job {
                        Some(job) => txn.claim_job(&task.id, job, lease_of(data.ttl)).await?,
                        None => false,
                    };
                    if !claimed {
                        return done(txn, Reply::err(409, "Already exists")).await;
                    }
                }
                if !acquire(&mut txn, &mut task, data.pid, data.ttl) {
                    return done(txn, Reply::err(409, "Already exists")).await;
                }
                let view = task_view(&mut txn, &task).await?;
                done(txn, Reply::ok(view)).await
            }
            TaskState::Fulfilled => {
                let view = task_view(&mut txn, &task).await?;
                done(txn, Reply::ok(view)).await
            }
            TaskState::Acquired | TaskState::Suspended | TaskState::Halted => {
                done(txn, Reply::err(409, "Already exists")).await
            }
        };
    }
    if txn.get_promise(&create.id).await?.is_some() {
        let reply = Reply::err(422, "The promise does not have a resonate:target tag");
        return done(txn, reply).await;
    }
    let mode = CreateMode::Acquire {
        pid: data.pid,
        ttl: data.ttl,
    };
    let promise = create_promise(&mut txn, create, None, mode).await?;
    let task = txn
        .get_task(&promise.record.id)
        .await?
        .expect("a targeted promise has a task");
    let view = task_view(&mut txn, &task).await?;
    done(txn, Reply::ok(view)).await
}

async fn task_acquire(mut txn: Txn<'_>, data: TaskAcquireData) -> Result<Reply> {
    try_timeout(&mut txn, &[&data.id]).await?;
    let Some(mut task) = txn.get_task(&data.id).await? else {
        return done(txn, Reply::err(404, "Task not found")).await;
    };
    if task.state != TaskState::Pending {
        return done(txn, Reply::err(409, "Task is not pending")).await;
    }
    if task.version != data.version {
        return done(txn, Reply::err(409, "Version mismatch")).await;
    }
    if !acquire(&mut txn, &mut task, data.pid, data.ttl) {
        return done(txn, Reply::err(409, "Task is not pending")).await;
    }
    let view = task_view(&mut txn, &task).await?;
    done(txn, Reply::ok(view)).await
}

async fn task_release(mut txn: Txn<'_>, data: TaskVersionData) -> Result<Reply> {
    try_timeout(&mut txn, &[&data.id]).await?;
    let Some(mut task) = txn.get_task(&data.id).await? else {
        return done(txn, Reply::err(404, "Task not found")).await;
    };
    if !task.is_acquired_at(data.version) {
        let reply = Reply::err(409, "Task version mismatch or invalid state");
        return done(txn, reply).await;
    }
    task.transition(TaskState::Pending);
    txn.put_task(&task);
    // The job returns to pending and is delivered again.
    txn.nack_after_commit(&task.id);
    done(txn, Reply::ok(json!({}))).await
}

async fn task_fulfill(mut txn: Txn<'_>, data: TaskFulfillData) -> Result<Reply> {
    if data.action.kind != "promise.settle" {
        return Ok(Reply::err(400, "Invalid action kind"));
    }
    if data.action.data.id != data.id {
        return Ok(Reply::err(400, "Action must settle the task's promise"));
    }
    try_timeout(&mut txn, &[&data.id]).await?;
    let Some(task) = txn.get_task(&data.id).await? else {
        return done(txn, Reply::err(404, "Task not found")).await;
    };
    if !task.is_acquired_at(data.version) {
        let reply = Reply::err(409, "Task version mismatch or invalid state");
        return done(txn, reply).await;
    }
    let Some(mut promise) = txn.get_promise(&data.id).await? else {
        return done(txn, Reply::err(404, "Promise not found")).await;
    };
    if !txn.holds(&task.id) {
        let reply = Reply::err(409, "Task version mismatch or invalid state");
        return done(txn, reply).await;
    }
    if promise.is_pending() {
        settle(
            &mut txn,
            &mut promise,
            data.action.data.state,
            data.action.data.value,
        )
        .await?;
    }
    let reply = Reply::ok(json!({ "promise": promise.record }));
    commit_settlement(txn, &task.id, reply).await
}

/// Commit a settlement in the transaction that acknowledges the held
/// claim of `task_id`. A claim whose lease ended leaves the store
/// unchanged and is reported as a version conflict, as the task's job
/// is pending again.
async fn commit_settlement(txn: Txn<'_>, task_id: &str, reply: Reply) -> Result<Reply> {
    match txn.commit_ack(task_id).await {
        Ok(()) => Ok(reply),
        Err(Error::Storage(QueueError::ClaimLost)) => {
            Ok(Reply::err(409, "Task version mismatch or invalid state"))
        }
        Err(e) => Err(e),
    }
}

async fn task_suspend(mut txn: Txn<'_>, data: TaskSuspendData) -> Result<Reply> {
    if data.actions.is_empty() {
        return Ok(Reply::err(400, "Actions are required"));
    }
    let mut awaited_ids: Vec<&str> = Vec::with_capacity(data.actions.len());
    for action in &data.actions {
        if action.kind != "promise.register_callback" {
            return Ok(Reply::err(400, "Invalid action kind"));
        }
        if action.data.awaiter != data.id {
            return Ok(Reply::err(400, "Action awaiter must be the task"));
        }
        reject!(validate_callback(
            &action.data.awaited,
            &action.data.awaiter
        ));
        if awaited_ids.contains(&action.data.awaited.as_str()) {
            return Ok(Reply::err(400, "Awaited promises must be unique"));
        }
        awaited_ids.push(&action.data.awaited);
    }
    let mut named = vec![data.id.as_str()];
    named.extend(awaited_ids.iter().copied());
    try_timeout(&mut txn, &named).await?;
    let Some(mut task) = txn.get_task(&data.id).await? else {
        return done(txn, Reply::err(404, "Task not found")).await;
    };
    if !task.is_acquired_at(data.version) || !txn.holds(&task.id) {
        let reply = Reply::err(409, "Task is not acquired or version mismatch");
        return done(txn, reply).await;
    }
    let mut awaited = Vec::with_capacity(awaited_ids.len());
    for id in &awaited_ids {
        let Some(promise) = txn.get_promise(id).await? else {
            return done(txn, Reply::err(422, "Awaited promise not found")).await;
        };
        if !promise.is_external() {
            return done(txn, Reply::err(422, "Awaited promise is not awaitable")).await;
        }
        awaited.push(promise);
    }
    task.resumes.clear();
    if awaited.iter().any(|promise| !promise.is_pending()) {
        txn.put_task(&task);
        let own = txn
            .get_promise(&data.id)
            .await?
            .expect("a task's promise exists");
        let preload = txn.preload(&own.record).await?;
        return done(txn, Reply::Ok(300, json!({ "preload": preload }))).await;
    }
    for promise in awaited.iter_mut() {
        register_callback(&mut txn, promise, &data.id);
    }
    task.transition(TaskState::Suspended);
    txn.put_task(&task);
    commit_settlement(txn, &data.id, Reply::ok(json!({}))).await
}

async fn task_fence(mut txn: Txn<'_>, data: TaskFenceData, corr_id: &str) -> Result<Reply> {
    let Some(action_id) = data.action.data["id"].as_str().map(str::to_string) else {
        return Ok(Reply::err(400, "Action promise ID is required"));
    };
    if action_id == data.id {
        return Ok(Reply::err(
            400,
            "Action promise must differ from the task's promise",
        ));
    }
    if origin_of(&action_id) != origin_of(&data.id) {
        return Ok(Reply::err(400, "Action must belong to the task's origin"));
    }
    try_timeout(&mut txn, &[&data.id, &action_id]).await?;
    let Some(task) = txn.get_task(&data.id).await? else {
        return done(txn, Reply::err(404, "Task not found")).await;
    };
    if !task.is_acquired_at(data.version) {
        return done(txn, Reply::err(409, "Version mismatch")).await;
    }
    let inner = match data.action.kind.as_str() {
        "promise.create" => {
            let create: PromiseCreateData = match serde_json::from_value(data.action.data) {
                Ok(create) => create,
                Err(e) => return Ok(Reply::err(400, format!("Invalid action data: {e}"))),
            };
            reject!(validate_create(&create));
            let promise = match txn.get_promise(&create.id).await? {
                Some(existing) => existing,
                None => create_promise(&mut txn, create, None, CreateMode::Dispatch).await?,
            };
            Reply::ok(json!({ "promise": promise.record }))
        }
        "promise.settle" => {
            let settle_data: PromiseSettleData = match serde_json::from_value(data.action.data) {
                Ok(settle_data) => settle_data,
                Err(e) => return Ok(Reply::err(400, format!("Invalid action data: {e}"))),
            };
            match txn.get_promise(&settle_data.id).await? {
                None => Reply::err(404, "Promise not found"),
                Some(mut promise) => {
                    if promise.is_pending() {
                        settle(&mut txn, &mut promise, settle_data.state, settle_data.value)
                            .await?;
                    }
                    Reply::ok(json!({ "promise": promise.record }))
                }
            }
        }
        _ => return Ok(Reply::err(400, "Invalid fence action kind")),
    };
    let own = txn
        .get_promise(&data.id)
        .await?
        .expect("a task's promise exists");
    let preload = txn.preload(&own.record).await?;
    let reply = Reply::ok(json!({
        "action": {
            "kind": data.action.kind,
            "head": {
                "corrId": corr_id,
                "status": inner.status(),
                "version": PROTOCOL_VERSION,
            },
            "data": inner.into_data(),
        },
        "preload": preload,
    }));
    done(txn, reply).await
}

async fn task_heartbeat(mut txn: Txn<'_>, data: TaskHeartbeatData) -> Result<Reply> {
    for entry in data.tasks {
        let Some(task) = txn.get_task(&entry.id).await? else {
            continue;
        };
        if task.is_acquired_at(entry.version)
            && task.pid.as_deref() == Some(data.pid.as_str())
            && let Some(ttl) = task.ttl
        {
            txn.renew(&task.id, lease_of(ttl));
        }
    }
    done(txn, Reply::ok(json!({}))).await
}

async fn task_get(mut txn: Txn<'_>, data: IdData) -> Result<Reply> {
    try_timeout(&mut txn, &[&data.id]).await?;
    let reply = match txn.get_task(&data.id).await? {
        Some(task) => Reply::ok(json!({ "task": task.record() })),
        None => Reply::err(404, "Task not found"),
    };
    done(txn, reply).await
}

async fn task_halt(mut txn: Txn<'_>, data: IdData) -> Result<Reply> {
    try_timeout(&mut txn, &[&data.id]).await?;
    let Some(mut task) = txn.get_task(&data.id).await? else {
        return done(txn, Reply::err(404, "Task not found")).await;
    };
    match task.state {
        TaskState::Fulfilled => return done(txn, Reply::err(409, "Task is fulfilled")).await,
        TaskState::Halted => {}
        TaskState::Pending | TaskState::Acquired | TaskState::Suspended => {
            task.transition(TaskState::Halted);
            txn.put_task(&task);
            // A held claim is acknowledged: the job ends. A job still
            // waiting for delivery is dropped at its delivery.
            txn.release(&task.id);
        }
    }
    done(txn, Reply::ok(json!({}))).await
}

async fn task_continue(mut txn: Txn<'_>, data: IdData) -> Result<Reply> {
    try_timeout(&mut txn, &[&data.id]).await?;
    let Some(mut task) = txn.get_task(&data.id).await? else {
        return done(txn, Reply::err(404, "Task not found")).await;
    };
    if task.state != TaskState::Halted {
        return done(txn, Reply::err(409, "Task is not halted")).await;
    }
    task.transition(TaskState::Pending);
    txn.put_task(&task);
    // A job still waiting for delivery dispatches the task.
    let waiting = match &task.job {
        Some(job) => txn.job_exists(job).await?,
        None => false,
    };
    if !waiting {
        txn.dispatch(&mut task, None);
    }
    done(txn, Reply::ok(json!({}))).await
}

async fn task_search(txn: Txn<'_>, data: TaskSearchData) -> Result<Reply> {
    let limit = limit!(data.limit, DEFAULT_SEARCH_LIMIT);
    let mut tasks = Vec::new();
    for task in txn.tasks().await? {
        let record = task.record();
        if data.state.is_none_or(|state| record.state == state) {
            tasks.push(record);
        }
    }
    let (page, cursor) = page(tasks, |t| &t.id, data.cursor.as_deref(), limit);
    Ok(Reply::ok(with_cursor(json!({ "tasks": page }), cursor)))
}

async fn schedule_create(mut txn: Txn<'_>, data: ScheduleCreateData) -> Result<Reply> {
    if data.id.is_empty() {
        return Ok(Reply::err(400, "Schedule ID is required"));
    }
    if data.id.contains(':') {
        return Ok(Reply::err(400, "Schedule ID must not contain ':'"));
    }
    match data.promise_tags.get(TAG_TARGET) {
        None => return Ok(Reply::err(400, "resonate:target tag is required")),
        Some(target) if !is_url(target) => {
            return Ok(Reply::err(400, "Invalid resonate:target address"));
        }
        Some(_) => {}
    }
    if data.promise_timeout < 0 {
        return Ok(Reply::err(400, "Promise timeout must not be negative"));
    }
    if let Some(existing) = txn.get_schedule(&data.id).await? {
        return Ok(Reply::ok(json!({ "schedule": existing })));
    }
    let Some(next_run_at) = cron::next_after(&data.cron, txn.now) else {
        return Ok(Reply::err(400, "Invalid cron expression"));
    };
    let schedule = ScheduleRecord {
        id: data.id,
        cron: data.cron,
        promise_id: data.promise_id,
        promise_timeout: data.promise_timeout,
        promise_param: data.promise_param,
        promise_tags: data.promise_tags,
        created_at: txn.now,
        next_run_at,
        last_run_at: None,
    };
    txn.put_schedule(&schedule);
    txn.schedule_timer(
        Timer::Schedule {
            id: schedule.id.clone(),
            at: next_run_at,
        },
        next_run_at,
    );
    done(txn, Reply::ok(json!({ "schedule": schedule }))).await
}

async fn schedule_get(txn: Txn<'_>, data: IdData) -> Result<Reply> {
    Ok(match txn.get_schedule(&data.id).await? {
        Some(schedule) => Reply::ok(json!({ "schedule": schedule })),
        None => Reply::err(404, "Schedule not found"),
    })
}

async fn schedule_delete(mut txn: Txn<'_>, data: IdData) -> Result<Reply> {
    if txn.get_schedule(&data.id).await?.is_none() {
        return Ok(Reply::err(404, "Schedule not found"));
    }
    txn.delete_schedule(&data.id);
    done(txn, Reply::ok(json!({}))).await
}

async fn schedule_search(txn: Txn<'_>, data: ScheduleSearchData) -> Result<Reply> {
    let limit = limit!(data.limit, DEFAULT_SCHEDULE_SEARCH_LIMIT);
    let schedules: Vec<ScheduleRecord> = txn
        .schedules()
        .await?
        .into_iter()
        .filter(|s| tags_match(&s.promise_tags, data.tags.as_ref()))
        .collect();
    let (page, cursor) = page(schedules, |s| &s.id, data.cursor.as_deref(), limit);
    Ok(Reply::ok(with_cursor(json!({ "schedules": page }), cursor)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ancestor_check_follows_the_lineage_separators() {
        assert!(is_ancestor("root", "root"));
        assert!(is_ancestor("root", "root:0"));
        assert!(is_ancestor("root:0", "root:0.1"));
        assert!(!is_ancestor("root", "rooted:0"));
        assert!(!is_ancestor("root:0", "root:01"));
    }

    #[test]
    fn a_page_ends_at_the_limit_and_names_its_last_id() {
        let items = vec!["a", "b", "c"];
        let (page1, next) = page(items.clone(), |s| s, None, 2);
        assert_eq!(page1, vec!["a", "b"]);
        assert_eq!(next.as_deref(), Some("b"));
        let (page2, next) = page(items, |s| s, next.as_deref(), 2);
        assert_eq!(page2, vec!["c"]);
        assert_eq!(next, None);
    }
}
