//! First-class object model for tracked machine state.
//!
//! A `ReclaimObject` is a logical unit of reusable computational state. It is
//! deliberately *not* free-form: fields that have typed meaning use typed
//! enums/structs; only application-specific metadata is allowed as JSON.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::errors::{ReclaimError, Result};
use crate::integrity::ContentHash;
use crate::lifecycle::LifecycleState;

/// Durability class: how hard the runtime must work to keep copies around.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[derive(Default)]
pub enum DurabilityClass {
    #[default]
    Ephemeral,
    Recomputable,
    Durable,
    Critical,
}

impl DurabilityClass {
    /// Minimum number of *valid* physical copies the runtime must guarantee
    /// before automatic reclamation may run for objects of this class.
    pub fn min_valid_copies(&self) -> u32 {
        match self {
            DurabilityClass::Ephemeral => 0,
            DurabilityClass::Recomputable => 0,
            DurabilityClass::Durable => 1,
            DurabilityClass::Critical => 2,
        }
    }
}

/// Survivability class semantics are policy-configurable; this is the
/// built-in vocabulary every policy may reference.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[derive(Default)]
pub enum SurvivabilityClass {
    #[default]
    Ephemeral,
    Recomputable,
    Durable,
    Critical,
}

/// Physical placement of a payload (backend + key). Describes *where bytes
/// live*, which Reclaim Fabric tracks but does not own.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PhysicalLocation {
    /// Stable backend identifier as registered with a process ("memory",
    /// "file:/data/backends", node-id + backend-id for remote payloads).
    pub backend: String,
    /// Opaque key inside the backend.
    pub key: String,
    /// Placement kind: hot (memory), durable (filesystem), archive.
    pub kind: PhysicalKind,
}

/// Placement tier of a physical payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PhysicalKind {
    Hot,
    Durable,
    Archived,
}

impl PhysicalKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            PhysicalKind::Hot => "HOT",
            PhysicalKind::Durable => "DURABLE",
            PhysicalKind::Archived => "ARCHIVED",
        }
    }
}

/// A single physical replica of an object's payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Replica {
    pub replica_id: Uuid,
    pub object_id: Uuid,
    pub generation: u64,
    pub location: PhysicalLocation,
    pub size: u64,
    pub content_hash: ContentHash,
    pub created_at_ms: i64,
    /// Timestamp of last successful integrity verification, if any.
    pub verified_at_ms: Option<i64>,
    /// Whether this replica is currently known-valid (false after corruption
    /// detection or failed verification).
    pub valid: bool,
    /// Owning node process id, if hosted by a remote node.
    pub owner_node: Option<String>,
}

/// Full metadata for a tracked state object.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReclaimObject {
    pub id: Uuid,
    pub generation: u64,
    /// Opaque application class ("checkpoint", "kv-cache", "embedding", ...).
    pub class: String,
    pub logical_size: u64,
    pub physical_size: u64,
    pub compressed_size: Option<u64>,
    pub created_at_ms: i64,
    pub last_access_ms: i64,
    pub access_count: u64,
    pub reuse_probability: f64,
    pub reuse_horizon_secs: Option<u64>,
    pub recompute_cost: Option<f64>,
    pub recompute_latency_secs: Option<f64>,
    pub transfer_cost: Option<f64>,
    pub migration_cost: Option<f64>,
    pub storage_cost_per_byte_sec: f64,
    pub memory_cost_per_byte_sec: f64,
    pub replication_count: u32,
    pub durability_class: DurabilityClass,
    pub survivability_class: SurvivabilityClass,
    pub owner: String,
    pub content_hash: Option<ContentHash>,
    pub lifecycle_state: LifecycleState,
    pub policy_version: String,
    pub decision_epoch: u64,
    pub pinned: bool,
    pub protected: bool,
    /// Earliest time at which automatic reclamation may proceed.
    pub min_retention_deadline_ms: Option<i64>,
    /// Persisted workload metadata for a desired upper retention bound. The
    /// built-in policy does not currently force reclamation at this time.
    pub max_retention_deadline_ms: Option<i64>,
    pub app_metadata: BTreeMap<String, serde_json::Value>,
}

impl ReclaimObject {
    pub fn new(
        id: Uuid,
        generation: u64,
        class: impl Into<String>,
        logical_size: u64,
        created_at_ms: i64,
    ) -> ReclaimObject {
        ReclaimObject {
            id,
            generation,
            class: class.into(),
            logical_size,
            physical_size: 0,
            compressed_size: None,
            created_at_ms,
            last_access_ms: created_at_ms,
            access_count: 0,
            reuse_probability: 0.0,
            reuse_horizon_secs: None,
            recompute_cost: None,
            recompute_latency_secs: None,
            transfer_cost: None,
            migration_cost: None,
            storage_cost_per_byte_sec: 0.0,
            memory_cost_per_byte_sec: 0.0,
            replication_count: 0,
            durability_class: DurabilityClass::Ephemeral,
            survivability_class: SurvivabilityClass::Ephemeral,
            owner: String::new(),
            content_hash: None,
            lifecycle_state: LifecycleState::Created,
            policy_version: String::new(),
            decision_epoch: 0,
            pinned: false,
            protected: false,
            min_retention_deadline_ms: None,
            max_retention_deadline_ms: None,
            app_metadata: BTreeMap::new(),
        }
    }

    pub fn is_pinned(&self) -> bool {
        self.pinned
    }

    pub fn is_protected(&self) -> bool {
        self.protected
    }

    pub fn set_app_metadata(&mut self, key: impl Into<String>, value: serde_json::Value) {
        self.app_metadata.insert(key.into(), value);
    }

    /// Validate scalar bounds so corrupt metadata cannot enter the store.
    pub fn validate(&self) -> Result<()> {
        if !self.reuse_probability.is_finite() || !(0.0..=1.0).contains(&self.reuse_probability) {
            return Err(ReclaimError::InvalidArgument(format!(
                "reuse_probability out of range: {}",
                self.reuse_probability
            )));
        }
        for (name, value) in [
            ("recompute_cost", self.recompute_cost),
            ("recompute_latency_secs", self.recompute_latency_secs),
            ("transfer_cost", self.transfer_cost),
            ("migration_cost", self.migration_cost),
        ] {
            if let Some(value) = value {
                if !value.is_finite() || value < 0.0 {
                    return Err(ReclaimError::InvalidArgument(format!(
                        "{name} must be finite and non-negative, got {value}"
                    )));
                }
            }
        }
        for (name, value) in [
            ("storage_cost_per_byte_sec", self.storage_cost_per_byte_sec),
            ("memory_cost_per_byte_sec", self.memory_cost_per_byte_sec),
        ] {
            if !value.is_finite() || value < 0.0 {
                return Err(ReclaimError::InvalidArgument(format!(
                    "{name} must be finite and non-negative, got {value}"
                )));
            }
        }
        if self.class.trim().is_empty() {
            return Err(ReclaimError::InvalidArgument(
                "class must not be empty".into(),
            ));
        }
        if self
            .min_retention_deadline_ms
            .zip(self.max_retention_deadline_ms)
            .is_some_and(|(min, max)| min > max)
        {
            return Err(ReclaimError::InvalidArgument(
                "min_retention_deadline_ms must not exceed max_retention_deadline_ms".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_object_defaults() {
        let o = ReclaimObject::new(Uuid::new_v4(), 0, "test", 1024, 1);
        assert_eq!(o.lifecycle_state, LifecycleState::Created);
        assert!(!o.pinned);
        assert!(o.app_metadata.is_empty());
        o.validate().unwrap();
    }

    #[test]
    fn validation_rejects_bad_probability() {
        let mut o = ReclaimObject::new(Uuid::new_v4(), 0, "test", 1, 1);
        o.reuse_probability = 1.5;
        assert!(o.validate().is_err());
        o.reuse_probability = f64::NAN;
        assert!(o.validate().is_err());
        o.reuse_probability = f64::INFINITY;
        assert!(o.validate().is_err());
    }

    #[test]
    fn validation_rejects_non_finite_or_negative_cost_metadata() {
        let mut o = ReclaimObject::new(Uuid::new_v4(), 0, "test", 1, 1);
        o.recompute_cost = Some(f64::INFINITY);
        assert!(o.validate().is_err());

        o.recompute_cost = Some(0.0);
        o.recompute_latency_secs = Some(-1.0);
        assert!(o.validate().is_err());

        o.recompute_latency_secs = Some(0.0);
        o.transfer_cost = Some(f64::NAN);
        assert!(o.validate().is_err());

        o.transfer_cost = Some(0.0);
        o.migration_cost = Some(f64::NEG_INFINITY);
        assert!(o.validate().is_err());

        o.migration_cost = Some(0.0);
        o.storage_cost_per_byte_sec = -0.01;
        assert!(o.validate().is_err());

        o.storage_cost_per_byte_sec = 0.0;
        o.memory_cost_per_byte_sec = f64::INFINITY;
        assert!(o.validate().is_err());
    }

    #[test]
    fn validation_rejects_inverted_retention_window() {
        let mut o = ReclaimObject::new(Uuid::new_v4(), 0, "test", 1, 1);
        o.min_retention_deadline_ms = Some(11);
        o.max_retention_deadline_ms = Some(10);
        assert!(o.validate().is_err());
    }
}
