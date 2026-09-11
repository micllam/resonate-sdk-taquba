//! Cron expressions of schedules. A five-field expression is read with
//! a seconds field of zero, a six-field expression includes the seconds
//! field, and every time is UTC.

use chrono::{DateTime, Utc};

/// The first firing time strictly after epoch millisecond `after`, or
/// `None` when the expression does not parse or does not fire again.
pub(crate) fn next_after(expression: &str, after: i64) -> Option<i64> {
    let cron = croner::Cron::new(expression)
        .with_seconds_optional()
        .parse()
        .ok()?;
    let from = DateTime::<Utc>::from_timestamp_millis(after)?;
    cron.find_next_occurrence(&from, false)
        .ok()
        .map(|next| next.timestamp_millis())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn five_field_expressions_fire_at_the_next_minute_boundary() {
        // 2024-01-01T00:00:30Z
        let after = 1_704_067_230_000;
        assert_eq!(next_after("* * * * *", after), Some(1_704_067_260_000));
        assert_eq!(next_after("0 9 * * *", after), Some(1_704_099_600_000));
        assert_eq!(next_after("not a cron", after), None);
    }
}
