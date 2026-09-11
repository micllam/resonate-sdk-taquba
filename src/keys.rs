//! Keys of the protocol's records within the queue's user KV namespace
//! and the name of the queue the crate owns.
//!
//! Every key starts with `resonate/`, so the namespace can be shared
//! with an application's own entries. Promise and task keys end with the
//! full promise id, so a prefix scan lists promises in id order, which
//! is the order the search operations page by. The branch index maps a
//! `resonate:branch` tag value to the ids of the promises with that tag,
//! separated by a null byte, which the protocol forbids inside an id.

/// Prefix of every promise record.
pub(crate) const PROMISE_PREFIX: &[u8] = b"resonate/p/";
/// Prefix of every task record.
pub(crate) const TASK_PREFIX: &[u8] = b"resonate/t/";
/// Prefix of every schedule record.
pub(crate) const SCHEDULE_PREFIX: &[u8] = b"resonate/s/";
/// Prefix of the branch index.
const BRANCH_PREFIX: &[u8] = b"resonate/b/";

/// The queue of the crate's timer jobs.
pub(crate) const TIMER_QUEUE: &str = "resonate/timers";

pub(crate) fn promise_key(id: &str) -> Vec<u8> {
    join(PROMISE_PREFIX, id.as_bytes())
}

pub(crate) fn task_key(id: &str) -> Vec<u8> {
    join(TASK_PREFIX, id.as_bytes())
}

pub(crate) fn schedule_key(id: &str) -> Vec<u8> {
    join(SCHEDULE_PREFIX, id.as_bytes())
}

/// The index entry of promise `id` within branch `branch`.
pub(crate) fn branch_key(branch: &str, id: &str) -> Vec<u8> {
    let mut key = branch_prefix(branch);
    key.extend_from_slice(id.as_bytes());
    key
}

/// The prefix of every index entry within branch `branch`.
pub(crate) fn branch_prefix(branch: &str) -> Vec<u8> {
    let mut key = join(BRANCH_PREFIX, branch.as_bytes());
    key.push(0);
    key
}

/// The promise id of a branch index key with prefix `prefix`.
pub(crate) fn branch_entry_id<'a>(prefix: &[u8], key: &'a [u8]) -> Option<&'a str> {
    let rest = key.strip_prefix(prefix)?;
    std::str::from_utf8(rest).ok()
}

fn join(prefix: &[u8], id: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(prefix.len() + id.len());
    key.extend_from_slice(prefix);
    key.extend_from_slice(id);
    key
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn branch_index_entries_share_the_branch_prefix() {
        let prefix = branch_prefix("root:1");
        let key = branch_key("root:1", "root:1.0");
        assert!(key.starts_with(&prefix));
        assert_eq!(branch_entry_id(&prefix, &key), Some("root:1.0"));
        assert!(!branch_key("root:10", "root:10.0").starts_with(&prefix));
    }
}
