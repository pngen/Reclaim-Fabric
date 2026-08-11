//! Strict lifecycle state machine for tracked state objects.
//!
//! The state machine is a static, auditable transition table. Illegal
//! transitions fail deterministically with `InvalidTransition`. Repeated
//! requests of an already-satisfied transition are idempotent no-ops.

use serde::{Deserialize, Serialize};

use crate::errors::{ReclaimError, Result};

/// Lifecycle states.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LifecycleState {
    Created,
    Hot,
    Warm,
    Cold,
    Compressed,
    Deduped,
    Archived,
    Recomputable,
    ReclaimPending,
    Reclaimed,
    Failed,
}

impl LifecycleState {
    pub fn as_str(&self) -> &'static str {
        match self {
            LifecycleState::Created => "CREATED",
            LifecycleState::Hot => "HOT",
            LifecycleState::Warm => "WARM",
            LifecycleState::Cold => "COLD",
            LifecycleState::Compressed => "COMPRESSED",
            LifecycleState::Deduped => "DEDUPED",
            LifecycleState::Archived => "ARCHIVED",
            LifecycleState::Recomputable => "RECOMPUTABLE",
            LifecycleState::ReclaimPending => "RECLAIM_PENDING",
            LifecycleState::Reclaimed => "RECLAIMED",
            LifecycleState::Failed => "FAILED",
        }
    }

    pub fn parse(s: &str) -> Result<LifecycleState> {
        match s {
            "CREATED" => Ok(LifecycleState::Created),
            "HOT" => Ok(LifecycleState::Hot),
            "WARM" => Ok(LifecycleState::Warm),
            "COLD" => Ok(LifecycleState::Cold),
            "COMPRESSED" => Ok(LifecycleState::Compressed),
            "DEDUPED" => Ok(LifecycleState::Deduped),
            "ARCHIVED" => Ok(LifecycleState::Archived),
            "RECOMPUTABLE" => Ok(LifecycleState::Recomputable),
            "RECLAIM_PENDING" => Ok(LifecycleState::ReclaimPending),
            "RECLAIMED" => Ok(LifecycleState::Reclaimed),
            "FAILED" => Ok(LifecycleState::Failed),
            other => Err(ReclaimError::InvalidArgument(format!(
                "unknown lifecycle state: {other}"
            ))),
        }
    }
}

/// Guard result: either allowed, an idempotent no-op, or an error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransitionResult {
    Allowed,
    /// Already in the target state; caller treats as success.
    Noop,
}

/// Validates `from -> to` against the static transition table.
///
/// Table:
/// - CREATED -> HOT | WARM | COLD | FAILED
/// - HOT -> WARM | COLD | COMPRESSED | ARCHIVED | RECOMPUTABLE | DEDUPED | FAILED
/// - WARM -> HOT | COLD | COMPRESSED | ARCHIVED | RECOMPUTABLE | FAILED
/// - COLD -> HOT | WARM | COMPRESSED | ARCHIVED | RECOMPUTABLE | DEDUPED | FAILED
/// - COMPRESSED -> HOT | WARM | COLD | ARCHIVED | RECOMPUTABLE | FAILED
/// - DEDUPED -> HOT | WARM | COLD | ARCHIVED | RECOMPUTABLE | RECLAIM_PENDING | FAILED
/// - ARCHIVED -> HOT | WARM | RECOMPUTABLE | RECLAIM_PENDING | FAILED
/// - RECOMPUTABLE -> HOT | WARM | RECLAIM_PENDING | FAILED
/// - RECLAIM_PENDING -> RECLAIMED | HOT | WARM | FAILED (revival / abort)
/// - RECLAIMED -> (terminal)
/// - FAILED -> (terminal; re-registration creates a new object)
pub fn check_transition(from: LifecycleState, to: LifecycleState) -> Result<TransitionResult> {
    if from == to {
        return Ok(TransitionResult::Noop);
    }
    let allowed = match from {
        LifecycleState::Created => matches!(
            to,
            LifecycleState::Hot
                | LifecycleState::Warm
                | LifecycleState::Cold
                | LifecycleState::Failed
        ),
        LifecycleState::Hot => matches!(
            to,
            LifecycleState::Warm
                | LifecycleState::Cold
                | LifecycleState::Compressed
                | LifecycleState::Archived
                | LifecycleState::Recomputable
                | LifecycleState::Deduped
                | LifecycleState::Failed
        ),
        LifecycleState::Warm => matches!(
            to,
            LifecycleState::Hot
                | LifecycleState::Cold
                | LifecycleState::Compressed
                | LifecycleState::Archived
                | LifecycleState::Recomputable
                | LifecycleState::Failed
        ),
        LifecycleState::Cold => matches!(
            to,
            LifecycleState::Hot
                | LifecycleState::Warm
                | LifecycleState::Compressed
                | LifecycleState::Archived
                | LifecycleState::Recomputable
                | LifecycleState::Deduped
                | LifecycleState::Failed
        ),
        LifecycleState::Compressed => matches!(
            to,
            LifecycleState::Hot
                | LifecycleState::Warm
                | LifecycleState::Cold
                | LifecycleState::Archived
                | LifecycleState::Recomputable
                | LifecycleState::Failed
        ),
        LifecycleState::Deduped => matches!(
            to,
            LifecycleState::Hot
                | LifecycleState::Warm
                | LifecycleState::Cold
                | LifecycleState::Archived
                | LifecycleState::Recomputable
                | LifecycleState::ReclaimPending
                | LifecycleState::Failed
        ),
        LifecycleState::Archived => matches!(
            to,
            LifecycleState::Hot
                | LifecycleState::Warm
                | LifecycleState::Recomputable
                | LifecycleState::ReclaimPending
                | LifecycleState::Failed
        ),
        LifecycleState::Recomputable => matches!(
            to,
            LifecycleState::Hot
                | LifecycleState::Warm
                | LifecycleState::ReclaimPending
                | LifecycleState::Failed
        ),
        LifecycleState::ReclaimPending => matches!(
            to,
            LifecycleState::Reclaimed
                | LifecycleState::Hot
                | LifecycleState::Warm
                | LifecycleState::Failed
        ),
        LifecycleState::Reclaimed => false,
        LifecycleState::Failed => false,
    };
    if allowed {
        Ok(TransitionResult::Allowed)
    } else {
        Err(ReclaimError::InvalidTransition {
            from: from.as_str().to_string(),
            to: to.as_str().to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn happy_path_transitions() {
        assert_eq!(
            check_transition(LifecycleState::Created, LifecycleState::Hot).unwrap(),
            TransitionResult::Allowed
        );
        assert_eq!(
            check_transition(LifecycleState::Hot, LifecycleState::Warm).unwrap(),
            TransitionResult::Allowed
        );
        assert_eq!(
            check_transition(LifecycleState::Warm, LifecycleState::Cold).unwrap(),
            TransitionResult::Allowed
        );
        assert_eq!(
            check_transition(LifecycleState::Cold, LifecycleState::Compressed).unwrap(),
            TransitionResult::Allowed
        );
        assert_eq!(
            check_transition(LifecycleState::Cold, LifecycleState::Archived).unwrap(),
            TransitionResult::Allowed
        );
        assert_eq!(
            check_transition(LifecycleState::Compressed, LifecycleState::Archived).unwrap(),
            TransitionResult::Allowed
        );
        assert_eq!(
            check_transition(LifecycleState::Archived, LifecycleState::Recomputable).unwrap(),
            TransitionResult::Allowed
        );
        assert_eq!(
            check_transition(LifecycleState::Recomputable, LifecycleState::ReclaimPending).unwrap(),
            TransitionResult::Allowed
        );
        assert_eq!(
            check_transition(LifecycleState::ReclaimPending, LifecycleState::Reclaimed).unwrap(),
            TransitionResult::Allowed
        );
    }

    #[test]
    fn illegal_transitions_fail() {
        assert!(check_transition(LifecycleState::Created, LifecycleState::Reclaimed).is_err());
        assert!(check_transition(LifecycleState::Hot, LifecycleState::Created).is_err());
        assert!(check_transition(LifecycleState::Reclaimed, LifecycleState::Hot).is_err());
        assert!(check_transition(LifecycleState::Failed, LifecycleState::Hot).is_err());
        assert!(check_transition(LifecycleState::Cold, LifecycleState::ReclaimPending).is_err());
    }

    #[test]
    fn same_state_is_noop() {
        assert_eq!(
            check_transition(LifecycleState::Hot, LifecycleState::Hot).unwrap(),
            TransitionResult::Noop
        );
    }

    #[test]
    fn all_states_validated() {
        let states = [
            "CREATED",
            "HOT",
            "WARM",
            "COLD",
            "COMPRESSED",
            "DEDUPED",
            "ARCHIVED",
            "RECOMPUTABLE",
            "RECLAIM_PENDING",
            "RECLAIMED",
            "FAILED",
        ];
        for s in states {
            let st = LifecycleState::parse(s).unwrap();
            assert_eq!(st.as_str(), s);
        }
        assert!(LifecycleState::parse("NOPE").is_err());
    }
}
