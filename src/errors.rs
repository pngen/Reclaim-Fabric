//! Typed error model for Reclaim Fabric.
//!
//! Every fallible operation in the runtime returns `ReclaimError` rather than
//! panicking. Variants are classified so that callers (CLI, coordinator,
//! transport) can map them onto wire-level or user-level failures without
//! string matching.

use std::fmt;

use thiserror::Error;

/// Unified error type for the runtime.
#[derive(Debug, Error, Clone, PartialEq)]
pub enum ReclaimError {
    #[error("invalid lifecycle transition: {from} -> {to}")]
    InvalidTransition { from: String, to: String },

    #[error("stale coordinator epoch: expected {expected}, got {got}")]
    StaleEpoch { expected: u64, got: u64 },

    #[error("stale attempt: {attempt} is no longer current for object {object}")]
    StaleAttempt { attempt: String, object: String },

    #[error("reservation conflict: {0}")]
    ReservationConflict(String),

    #[error("dependency violation: {0}")]
    DependencyViolation(String),

    #[error("survivability violation: {0}")]
    SurvivabilityViolation(String),

    #[error("object is pinned and cannot be reclaimed: {0}")]
    PinnedObject(String),

    #[error("object is protected and cannot be reclaimed: {0}")]
    ProtectedObject(String),

    #[error("integrity failure: {0}")]
    IntegrityFailure(String),

    #[error("archive failure: {0}")]
    ArchiveFailure(String),

    #[error("compression failure: {0}")]
    CompressionFailure(String),

    #[error("decompression failure: {0}")]
    DecompressionFailure(String),

    #[error("transport error: {0}")]
    Transport(String),

    #[error("protocol error: {0}")]
    Protocol(String),

    #[error("persistence error: {0}")]
    Persistence(String),

    #[error("recovery error: {0}")]
    Recovery(String),

    #[error("policy error: {0}")]
    Policy(String),

    #[error("backend error: {0}")]
    Backend(String),

    #[error("capacity/pressure error: {0}")]
    Pressure(String),

    #[error("object not found: {0}")]
    NotFound(String),

    #[error("generation mismatch: {0}")]
    GenerationMismatch(String),

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("io error: {0}")]
    Io(String),

    #[error("dedup violation: {0}")]
    Dedup(String),

    #[error("internal error: {0}")]
    Internal(String),
}

impl ReclaimError {
    /// Short stable machine-readable class name (used on the wire and in audit).
    pub fn class(&self) -> &'static str {
        match self {
            ReclaimError::InvalidTransition { .. } => "invalid_transition",
            ReclaimError::StaleEpoch { .. } => "stale_epoch",
            ReclaimError::StaleAttempt { .. } => "stale_attempt",
            ReclaimError::ReservationConflict(_) => "reservation_conflict",
            ReclaimError::DependencyViolation(_) => "dependency_violation",
            ReclaimError::SurvivabilityViolation(_) => "survivability_violation",
            ReclaimError::PinnedObject(_) => "pinned_object",
            ReclaimError::ProtectedObject(_) => "protected_object",
            ReclaimError::IntegrityFailure(_) => "integrity_failure",
            ReclaimError::ArchiveFailure(_) => "archive_failure",
            ReclaimError::CompressionFailure(_) => "compression_failure",
            ReclaimError::DecompressionFailure(_) => "decompression_failure",
            ReclaimError::Transport(_) => "transport",
            ReclaimError::Protocol(_) => "protocol",
            ReclaimError::Persistence(_) => "persistence",
            ReclaimError::Recovery(_) => "recovery",
            ReclaimError::Policy(_) => "policy",
            ReclaimError::Backend(_) => "backend",
            ReclaimError::Pressure(_) => "pressure",
            ReclaimError::NotFound(_) => "not_found",
            ReclaimError::GenerationMismatch(_) => "generation_mismatch",
            ReclaimError::InvalidArgument(_) => "invalid_argument",
            ReclaimError::Io(_) => "io",
            ReclaimError::Dedup(_) => "dedup",
            ReclaimError::Internal(_) => "internal",
        }
    }
}

impl From<std::io::Error> for ReclaimError {
    fn from(e: std::io::Error) -> Self {
        ReclaimError::Io(e.to_string())
    }
}

impl From<rusqlite::Error> for ReclaimError {
    fn from(e: rusqlite::Error) -> Self {
        ReclaimError::Persistence(e.to_string())
    }
}

impl From<serde_json::Error> for ReclaimError {
    fn from(e: serde_json::Error) -> Self {
        ReclaimError::Internal(format!("json: {e}"))
    }
}

pub type Result<T> = std::result::Result<T, ReclaimError>;

/// Wrapper that keeps an error displayable on the wire (errors cross process
/// boundaries as typed class + message pairs).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct WireError {
    pub class: String,
    pub message: String,
}

impl fmt::Display for WireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.class, self.message)
    }
}

impl From<ReclaimError> for WireError {
    fn from(e: ReclaimError) -> Self {
        let class = e.class().to_string();
        WireError {
            class,
            message: e.to_string(),
        }
    }
}

impl From<WireError> for ReclaimError {
    fn from(w: WireError) -> Self {
        // `WireError.message` is the sender's `Display` representation.  Strip
        // the stable display prefix before rebuilding tuple variants so a
        // round trip does not duplicate it (for example,
        // "backend error: backend error: ...").  The three structured
        // variants are parsed back into their original fields.
        fn payload(message: String, prefix: &str) -> String {
            message
                .strip_prefix(prefix)
                .map(str::to_owned)
                .unwrap_or(message)
        }

        match w.class.as_str() {
            "invalid_transition" => {
                let fields = w
                    .message
                    .strip_prefix("invalid lifecycle transition: ")
                    .and_then(|s| s.split_once(" -> "));
                match fields {
                    Some((from, to)) => ReclaimError::InvalidTransition {
                        from: from.to_owned(),
                        to: to.to_owned(),
                    },
                    None => ReclaimError::Internal(w.message),
                }
            }
            "stale_epoch" => {
                let fields = w
                    .message
                    .strip_prefix("stale coordinator epoch: expected ")
                    .and_then(|s| s.split_once(", got "))
                    .and_then(|(expected, got)| {
                        Some((expected.parse::<u64>().ok()?, got.parse::<u64>().ok()?))
                    });
                match fields {
                    Some((expected, got)) => ReclaimError::StaleEpoch { expected, got },
                    None => ReclaimError::Internal(w.message),
                }
            }
            "stale_attempt" => {
                let fields = w
                    .message
                    .strip_prefix("stale attempt: ")
                    .and_then(|s| s.split_once(" is no longer current for object "));
                match fields {
                    Some((attempt, object)) => ReclaimError::StaleAttempt {
                        attempt: attempt.to_owned(),
                        object: object.to_owned(),
                    },
                    None => ReclaimError::Internal(w.message),
                }
            }
            "reservation_conflict" => {
                ReclaimError::ReservationConflict(payload(w.message, "reservation conflict: "))
            }
            "dependency_violation" => {
                ReclaimError::DependencyViolation(payload(w.message, "dependency violation: "))
            }
            "survivability_violation" => ReclaimError::SurvivabilityViolation(payload(
                w.message,
                "survivability violation: ",
            )),
            "pinned_object" => ReclaimError::PinnedObject(payload(
                w.message,
                "object is pinned and cannot be reclaimed: ",
            )),
            "protected_object" => ReclaimError::ProtectedObject(payload(
                w.message,
                "object is protected and cannot be reclaimed: ",
            )),
            "integrity_failure" => {
                ReclaimError::IntegrityFailure(payload(w.message, "integrity failure: "))
            }
            "archive_failure" => {
                ReclaimError::ArchiveFailure(payload(w.message, "archive failure: "))
            }
            "compression_failure" => {
                ReclaimError::CompressionFailure(payload(w.message, "compression failure: "))
            }
            "decompression_failure" => {
                ReclaimError::DecompressionFailure(payload(w.message, "decompression failure: "))
            }
            "transport" => ReclaimError::Transport(payload(w.message, "transport error: ")),
            "protocol" => ReclaimError::Protocol(payload(w.message, "protocol error: ")),
            "persistence" => ReclaimError::Persistence(payload(w.message, "persistence error: ")),
            "recovery" => ReclaimError::Recovery(payload(w.message, "recovery error: ")),
            "policy" => ReclaimError::Policy(payload(w.message, "policy error: ")),
            "backend" => ReclaimError::Backend(payload(w.message, "backend error: ")),
            "pressure" => ReclaimError::Pressure(payload(w.message, "capacity/pressure error: ")),
            "not_found" => ReclaimError::NotFound(payload(w.message, "object not found: ")),
            "generation_mismatch" => {
                ReclaimError::GenerationMismatch(payload(w.message, "generation mismatch: "))
            }
            "invalid_argument" => {
                ReclaimError::InvalidArgument(payload(w.message, "invalid argument: "))
            }
            "io" => ReclaimError::Io(payload(w.message, "io error: ")),
            "dedup" => ReclaimError::Dedup(payload(w.message, "dedup violation: ")),
            "internal" => ReclaimError::Internal(payload(w.message, "internal error: ")),
            _ => ReclaimError::Internal(w.message),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ReclaimError, WireError};

    #[test]
    fn wire_round_trip_preserves_typed_errors_and_messages() {
        let errors = [
            ReclaimError::InvalidTransition {
                from: "HOT".into(),
                to: "RECLAIMED".into(),
            },
            ReclaimError::StaleEpoch {
                expected: 42,
                got: 41,
            },
            ReclaimError::StaleAttempt {
                attempt: "attempt-id".into(),
                object: "object-id".into(),
            },
            ReclaimError::ReservationConflict("reservation".into()),
            ReclaimError::DependencyViolation("dependency".into()),
            ReclaimError::SurvivabilityViolation("copies".into()),
            ReclaimError::PinnedObject("object-id".into()),
            ReclaimError::ProtectedObject("object-id".into()),
            ReclaimError::IntegrityFailure("hash".into()),
            ReclaimError::ArchiveFailure("archive".into()),
            ReclaimError::CompressionFailure("compress".into()),
            ReclaimError::DecompressionFailure("decompress".into()),
            ReclaimError::Transport("socket".into()),
            ReclaimError::Protocol("frame".into()),
            ReclaimError::Persistence("database".into()),
            ReclaimError::Recovery("journal".into()),
            ReclaimError::Policy("threshold".into()),
            ReclaimError::Backend("backend error: nested detail".into()),
            ReclaimError::Pressure("capacity".into()),
            ReclaimError::NotFound("object-id".into()),
            ReclaimError::GenerationMismatch("generation".into()),
            ReclaimError::InvalidArgument("argument".into()),
            ReclaimError::Io("file".into()),
            ReclaimError::Dedup("reference".into()),
            ReclaimError::Internal("invariant".into()),
        ];

        for original in errors {
            let original_display = original.to_string();
            let wire = WireError::from(original.clone());
            let decoded = ReclaimError::from(wire);
            assert_eq!(decoded, original);
            assert_eq!(decoded.to_string(), original_display);
        }
    }

    #[test]
    fn malformed_structured_wire_error_fails_closed_without_fabricated_fields() {
        let decoded = ReclaimError::from(WireError {
            class: "stale_epoch".into(),
            message: "malformed peer message".into(),
        });
        assert_eq!(
            decoded,
            ReclaimError::Internal("malformed peer message".into())
        );

        let unknown = ReclaimError::from(WireError {
            class: "future_error_class".into(),
            message: "future peer message".into(),
        });
        assert_eq!(
            unknown,
            ReclaimError::Internal("future peer message".into())
        );
    }
}
