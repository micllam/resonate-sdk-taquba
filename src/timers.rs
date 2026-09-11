//! The store's timers: scheduled jobs on the timer queue, one per
//! promise deadline and one per schedule firing. A promise timer acts
//! only while its promise is pending, and a schedule timer only while
//! the schedule's next firing is the one the timer was scheduled for,
//! so a timer that outlived its transition ends without effect. Task
//! deadlines are not timers: a task's retry and lease deadlines are the
//! lease of its job.

use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::kernel::{expire_promise, fire_schedule};
use crate::txn::Txn;

/// One scheduled deadline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum Timer {
    /// The `timeoutAt` of a pending promise.
    PromiseTimeout { id: String },
    /// The firing of a schedule at `at`.
    Schedule { id: String, at: i64 },
}

/// Apply a fired timer and commit.
pub(crate) async fn fire(mut txn: Txn<'_>, timer: Timer) -> Result<()> {
    match timer {
        Timer::PromiseTimeout { id } => {
            let Some(mut promise) = txn.get_promise(&id).await? else {
                return Ok(());
            };
            if !promise.is_pending() {
                return Ok(());
            }
            expire_promise(&mut txn, &mut promise).await?;
        }
        Timer::Schedule { id, at } => {
            let Some(schedule) = txn.get_schedule(&id).await? else {
                return Ok(());
            };
            if schedule.next_run_at != at {
                return Ok(());
            }
            fire_schedule(&mut txn, schedule).await?;
        }
    }
    txn.commit().await
}
