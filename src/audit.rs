//! Audit trail: append-only records with replay/inspection tooling.
//!
//! Records are written exclusively through `Store::append_audit` (SQLite
//! table `audit`); they are never mutated in place. Replay filters by object,
//! action, and recency, and formats records for CLI inspection.

use serde_json::json;

use crate::errors::Result;
use crate::persistence::{AuditEntry, Store};

/// Replay audit entries, newest first.
pub fn replay(
    store: &Store,
    object_id: Option<&uuid::Uuid>,
    action: Option<&str>,
    limit: u64,
) -> Result<Vec<AuditEntry>> {
    store.replay_audit(object_id, action, limit)
}

/// Format one audit entry for human output.
pub fn format_entry(e: &AuditEntry) -> String {
    fn quoted(value: &str) -> String {
        serde_json::to_string(value).unwrap_or_else(|_| "\"<invalid>\"".into())
    }
    fn quoted_optional(value: Option<&str>) -> String {
        value.map(quoted).unwrap_or_else(|| "-".into())
    }

    let object = e
        .object_id
        .map(|o| o.to_string())
        .unwrap_or_else(|| "-".into());
    format!(
        "#{} {} actor={} action={} object={} gen={} state={}->{} policy={} attempt={} node={} detail={}",
        e.id,
        e.ts_ms,
        quoted(&e.actor),
        quoted(&e.action),
        object,
        e.generation
            .map(|g| g.to_string())
            .unwrap_or_else(|| "-".into()),
        quoted_optional(e.prior_state.as_deref()),
        quoted_optional(e.new_state.as_deref()),
        quoted_optional(e.policy.as_deref()),
        e.attempt_id
            .map(|a| a.to_string())
            .unwrap_or_else(|| "-".into()),
        quoted_optional(e.node.as_deref()),
        serde_json::to_string(&e.detail).unwrap_or_else(|_| "{}".into()),
    )
}

/// JSON representation of a replay result.
pub fn replay_json(entries: &[AuditEntry]) -> serde_json::Value {
    json!(entries)
}

/// Verify the ordering and uniqueness of a newest-first audit replay.
///
/// SQLite assigns monotonically increasing ids, while [`replay`] returns the
/// newest row first. Consequently, each following id must be strictly lower
/// than the previous one. This detects duplicate or reordered replay rows; it
/// is not a cryptographic tamper-evidence check.
pub fn verify_append_only(entries: &[AuditEntry]) -> Result<bool> {
    for pair in entries.windows(2) {
        if pair[1].id >= pair[0].id {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Distinct action types observed in a replay (for inspection tooling).
pub fn distinct_actions(entries: &[AuditEntry]) -> Vec<String> {
    let mut v: Vec<String> = entries.iter().map(|e| e.action.clone()).collect();
    v.sort();
    v.dedup();
    v
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn entry(id: i64, action: &str) -> AuditEntry {
        AuditEntry {
            id,
            ts_ms: id,
            actor: "t".into(),
            action: action.into(),
            object_id: None,
            generation: None,
            prior_state: None,
            new_state: None,
            policy: None,
            attempt_id: None,
            node: None,
            detail: json!({}),
        }
    }

    #[test]
    fn append_only_verification() {
        assert!(verify_append_only(&[entry(3, "C"), entry(2, "B"), entry(1, "A")]).unwrap());
        assert!(!verify_append_only(&[entry(1, "A"), entry(2, "B")]).unwrap());
        assert!(!verify_append_only(&[entry(2, "A"), entry(2, "B")]).unwrap());
    }

    #[test]
    fn distinct_actions_sorted() {
        let entries = [entry(1, "B"), entry(2, "A"), entry(3, "A")];
        assert_eq!(distinct_actions(&entries), vec!["A", "B"]);
    }

    #[test]
    fn human_format_includes_actor_and_escapes_line_breaks() {
        let mut e = entry(1, "ACTION\nFORGED");
        e.actor = "actor\r\nforged".into();
        e.node = Some("node\nforged".into());
        let formatted = format_entry(&e);
        assert_eq!(formatted.lines().count(), 1);
        assert!(formatted.contains("actor=\"actor\\r\\nforged\""));
        assert!(formatted.contains("action=\"ACTION\\nFORGED\""));
        assert!(formatted.contains("node=\"node\\nforged\""));
    }

    #[test]
    fn replay_through_store() {
        let store = Store::open_in_memory().unwrap();
        let oid = Uuid::new_v4();
        store
            .append_audit(&AuditEntry {
                id: 0,
                ts_ms: 1,
                actor: "x".into(),
                action: "OBJECT_CREATED".into(),
                object_id: Some(oid),
                generation: Some(0),
                prior_state: None,
                new_state: Some("CREATED".into()),
                policy: None,
                attempt_id: None,
                node: None,
                detail: json!({}),
            })
            .unwrap();
        store
            .append_audit(&AuditEntry {
                id: 0,
                ts_ms: 2,
                actor: "x".into(),
                action: "OBJECT_TOUCHED".into(),
                object_id: Some(oid),
                generation: Some(0),
                prior_state: Some("CREATED".into()),
                new_state: Some("CREATED".into()),
                policy: None,
                attempt_id: None,
                node: None,
                detail: json!({}),
            })
            .unwrap();
        let all = replay(&store, None, None, 10).unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].action, "OBJECT_TOUCHED");
        assert!(verify_append_only(&all).unwrap());
        let filtered = replay(&store, Some(&oid), Some("OBJECT_CREATED"), 10).unwrap();
        assert_eq!(filtered.len(), 1);
        let none = replay(&store, Some(&Uuid::new_v4()), None, 10).unwrap();
        assert!(none.is_empty());
        assert!(format_entry(&filtered[0]).contains("OBJECT_CREATED"));
    }
}
