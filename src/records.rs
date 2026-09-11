//! The protocol's records: their wire form, their stored form and the
//! tag rules that classify a promise.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

/// The protocol version of every response.
pub(crate) const PROTOCOL_VERSION: &str = "2026-04-01";

pub(crate) const TAG_TARGET: &str = "resonate:target";
pub(crate) const TAG_TIMER: &str = "resonate:timer";
pub(crate) const TAG_DELAY: &str = "resonate:delay";
pub(crate) const TAG_BRANCH: &str = "resonate:branch";
pub(crate) const TAG_SCOPE: &str = "resonate:scope";
pub(crate) const TAG_EXTERNAL: &str = "resonate:external";

pub(crate) type Tags = BTreeMap<String, String>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PromiseState {
    Pending,
    Resolved,
    Rejected,
    RejectedCanceled,
    RejectedTimedout,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TaskState {
    Pending,
    Acquired,
    Suspended,
    Halted,
    Fulfilled,
}

/// The states a client can settle a promise to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SettleState {
    Resolved,
    Rejected,
    RejectedCanceled,
}

impl From<SettleState> for PromiseState {
    fn from(state: SettleState) -> Self {
        match state {
            SettleState::Resolved => PromiseState::Resolved,
            SettleState::Rejected => PromiseState::Rejected,
            SettleState::RejectedCanceled => PromiseState::RejectedCanceled,
        }
    }
}

/// A promise's parameter or value: optional headers and an opaque
/// string the SDK encodes.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PromiseValue {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub headers: Option<BTreeMap<String, String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<String>,
}

/// A promise in its wire form.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PromiseRecord {
    pub id: String,
    pub state: PromiseState,
    #[serde(default)]
    pub param: PromiseValue,
    #[serde(default)]
    pub value: PromiseValue,
    #[serde(default)]
    pub tags: Tags,
    #[serde(rename = "timeoutAt")]
    pub timeout_at: i64,
    #[serde(rename = "createdAt")]
    pub created_at: i64,
    #[serde(rename = "settledAt", default, skip_serializing_if = "Option::is_none")]
    pub settled_at: Option<i64>,
}

/// A promise as stored: the wire record plus the registrations against
/// it. `callbacks` are the ids of the awaiting tasks and `listeners` the
/// addresses to send an unblock to, both in registration order.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StoredPromise {
    #[serde(flatten)]
    pub record: PromiseRecord,
    #[serde(default)]
    pub callbacks: Vec<String>,
    #[serde(default)]
    pub listeners: Vec<String>,
}

impl std::ops::Deref for StoredPromise {
    type Target = PromiseRecord;

    fn deref(&self) -> &PromiseRecord {
        &self.record
    }
}

impl std::ops::DerefMut for StoredPromise {
    fn deref_mut(&mut self) -> &mut PromiseRecord {
        &mut self.record
    }
}

impl PromiseRecord {
    pub fn is_pending(&self) -> bool {
        self.state == PromiseState::Pending
    }

    pub fn target(&self) -> Option<&str> {
        self.tags.get(TAG_TARGET).map(String::as_str)
    }

    pub fn is_timer(&self) -> bool {
        self.tags.get(TAG_TIMER).map(String::as_str) == Some("true")
    }

    /// The state a deadline settles this promise to.
    pub fn timeout_state(&self) -> PromiseState {
        if self.is_timer() {
            PromiseState::Resolved
        } else {
            PromiseState::RejectedTimedout
        }
    }

    /// Whether a task can await this promise.
    pub fn is_external(&self) -> bool {
        self.tags.get(TAG_SCOPE).map(String::as_str) == Some("global")
            || self.tags.get(TAG_EXTERNAL).map(String::as_str) == Some("true")
            || self.tags.contains_key(TAG_TARGET)
            || self.is_timer()
    }
}

/// The origin of a promise id: everything before the first `:`.
pub(crate) fn origin_of(id: &str) -> &str {
    id.split_once(':').map_or(id, |(origin, _)| origin)
}

/// A task in its wire form. `resumes` is the count of settled
/// awaited promises the task has not observed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct TaskRecord {
    pub id: String,
    pub state: TaskState,
    pub version: i64,
    pub resumes: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ttl: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid: Option<String>,
}

/// A task as stored. The task's dispatch is the job `job` on the queue
/// named by `target`: a pending task's job waits for delivery or is
/// delivered and not yet acquired, an acquired task's job is a claim
/// held by the store, and no live job exists for a task in the
/// `suspended`, `halted` or `fulfilled` state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StoredTask {
    pub id: String,
    pub state: TaskState,
    pub version: i64,
    pub target: String,
    #[serde(default)]
    pub job: Option<String>,
    #[serde(default)]
    pub pid: Option<String>,
    #[serde(default)]
    pub ttl: Option<i64>,
    #[serde(default)]
    pub resumes: BTreeSet<String>,
}

impl StoredTask {
    pub fn new(id: String, target: String, state: TaskState) -> Self {
        Self {
            id,
            state,
            version: 0,
            target,
            job: None,
            pid: None,
            ttl: None,
            resumes: BTreeSet::new(),
        }
    }

    pub fn record(&self) -> TaskRecord {
        TaskRecord {
            id: self.id.clone(),
            state: self.state,
            version: self.version,
            resumes: self.resumes.len() as i64,
            ttl: self.ttl,
            pid: self.pid.clone(),
        }
    }

    /// Whether the task is acquired at `version`.
    pub fn is_acquired_at(&self, version: i64) -> bool {
        self.state == TaskState::Acquired && self.version == version
    }

    /// Move the task to `state` without a holder: the pid and the ttl
    /// are cleared and the version is unchanged.
    pub fn transition(&mut self, state: TaskState) {
        self.state = state;
        self.pid = None;
        self.ttl = None;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ScheduleRecord {
    pub id: String,
    pub cron: String,
    #[serde(rename = "promiseId")]
    pub promise_id: String,
    #[serde(rename = "promiseTimeout")]
    pub promise_timeout: i64,
    #[serde(rename = "promiseParam", default)]
    pub promise_param: PromiseValue,
    #[serde(rename = "promiseTags", default)]
    pub promise_tags: Tags,
    #[serde(rename = "createdAt")]
    pub created_at: i64,
    #[serde(rename = "nextRunAt")]
    pub next_run_at: i64,
    #[serde(rename = "lastRunAt", default, skip_serializing_if = "Option::is_none")]
    pub last_run_at: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn origin_is_the_prefix_before_the_first_colon() {
        assert_eq!(origin_of("root"), "root");
        assert_eq!(origin_of("root:0.1"), "root");
        assert_eq!(origin_of("my.app:1:x"), "my.app");
    }

    #[test]
    fn stored_promise_serializes_the_wire_record_flat() {
        let stored = StoredPromise {
            record: PromiseRecord {
                id: "p".into(),
                state: PromiseState::Pending,
                param: PromiseValue::default(),
                value: PromiseValue::default(),
                tags: Tags::new(),
                timeout_at: 5,
                created_at: 1,
                settled_at: None,
            },
            callbacks: vec!["p:0".into()],
            listeners: Vec::new(),
        };
        let json = serde_json::to_value(&stored).unwrap();
        assert_eq!(json["timeoutAt"], 5);
        assert_eq!(json["callbacks"][0], "p:0");
        let back: StoredPromise = serde_json::from_value(json).unwrap();
        assert_eq!(back.callbacks, vec!["p:0".to_string()]);
    }
}
