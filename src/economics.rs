//! Explicit reclamation economics.
//!
//! The decision model is a component surface, not a single hidden scalar.
//! Every component has a name, a value, and (where applicable) a weight.
//! A decision produces both a scalar score and the full component listing so
//! the reasoning is replayable.

use serde::{Deserialize, Serialize};

use crate::errors::Result;
use crate::object::ReclaimObject;

/// Baseline horizon scale used by the simple reuse-value decay heuristic.
pub const BASELINE_REUSE_DECAY_SECS: f64 = 86_400.0;
/// Baseline compression work estimate as a fraction of recomputation cost.
pub const BASELINE_COMPRESSION_COST_FRACTION: f64 = 0.01;
/// Baseline archive work estimate as a fraction of transfer cost.
pub const BASELINE_ARCHIVE_COST_FRACTION: f64 = 0.5;
/// Baseline recovery value as a fraction of reconstruction cost.
pub const BASELINE_FAILURE_RECOVERY_VALUE_FRACTION: f64 = 0.5;

/// Raw, policy-independent economic inputs for an object at decision time.
/// Any of these may be *accepted* from the registering workload or *derived*
/// from metadata + pressure.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CostInputs {
    /// What it costs per unit time to keep this state where it is (incl.
    /// memory + storage + replication).
    pub retain_cost: f64,
    /// What it would cost to recreate this state if lost.
    pub reconstruction_cost: f64,
    /// Expected value of reuse before the reuse horizon elapses.
    pub expected_reuse_value: f64,
    pub transfer_cost: f64,
    pub migration_cost: f64,
    pub storage_cost: f64,
    /// Baseline memory residency cost before resource-pressure adjustment.
    #[serde(default)]
    pub memory_cost: f64,
    /// Additional memory cost caused by pressure. This is a surcharge, so it
    /// is zero at the NORMAL multiplier.
    pub memory_pressure_cost: f64,
    pub replication_cost: f64,
    pub compression_cost: f64,
    pub archive_cost: f64,
    pub failure_recovery_value: f64,
    /// Value contributed because dependents require this state.
    pub dependency_value: f64,
    /// Value contributed by redundancy/survivability guarantees.
    pub survivability_value: f64,
    /// Fraction of reconstruction cost saved per reuse (0..=1).
    pub reconstruction_avoidance_fraction: f64,
}

impl CostInputs {
    /// Derive a baseline input set from an object's metadata.
    ///
    /// This is the *baseline* formulation from the specification:
    /// costs scale with physical size and per-byte rates; reuse value scales
    /// with reuse probability, logical size, and reconstruction cost.
    pub fn derive(obj: &ReclaimObject, _now_ms: i64, pressure_multiplier: f64) -> CostInputs {
        let size = obj.physical_size.max(obj.logical_size) as f64;

        let storage_cost = size * obj.storage_cost_per_byte_sec;
        let memory_cost = size * obj.memory_cost_per_byte_sec;
        let replication_cost = memory_cost * (obj.replication_count as f64 - 1.0).max(0.0);
        let memory_pressure_cost = memory_cost * (pressure_multiplier - 1.0).max(0.0);

        // Reuse value: probability * (what reuse is worth to the workload).
        // Baseline: reuse is worth a fraction of reconstruction cost.
        let reconstruction_cost = obj.recompute_cost.unwrap_or(0.0);
        let expected_reuse_value = obj.reuse_probability
            * reconstruction_cost
            * obj.reuse_horizon_secs.map_or(1.0, |h| {
                // Decay: reuse far in the future is worth less. Saturating at 1.
                1.0 / (1.0 + (h as f64 / BASELINE_REUSE_DECAY_SECS))
            });

        let retain_cost = memory_cost + storage_cost + replication_cost + memory_pressure_cost;
        let reconstruction_avoidance_fraction = obj.reuse_probability;

        CostInputs {
            retain_cost,
            reconstruction_cost,
            expected_reuse_value,
            transfer_cost: obj.transfer_cost.unwrap_or(0.0),
            migration_cost: obj.migration_cost.unwrap_or(0.0),
            storage_cost,
            memory_cost,
            memory_pressure_cost,
            replication_cost,
            compression_cost: obj.recompute_cost.unwrap_or(0.0)
                * BASELINE_COMPRESSION_COST_FRACTION,
            archive_cost: obj.transfer_cost.unwrap_or(0.0) * BASELINE_ARCHIVE_COST_FRACTION,
            failure_recovery_value: reconstruction_cost * BASELINE_FAILURE_RECOVERY_VALUE_FRACTION,
            dependency_value: 0.0,    // filled in by lineage analysis
            survivability_value: 0.0, // filled in by survivability analysis
            reconstruction_avoidance_fraction,
        }
    }
}

/// Named, versioned weights applied to each cost component.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CostWeights {
    pub weight_reuse_value: f64,
    pub weight_reconstruction_avoidance: f64,
    pub weight_dependency_value: f64,
    pub weight_survivability_value: f64,
    pub weight_memory: f64,
    pub weight_storage: f64,
    pub weight_replication: f64,
    pub weight_pressure: f64,
    pub weight_transfer: f64,
    pub weight_migration: f64,
}

impl Default for CostWeights {
    fn default() -> Self {
        CostWeights {
            weight_reuse_value: 1.0,
            weight_reconstruction_avoidance: 1.0,
            weight_dependency_value: 1.0,
            weight_survivability_value: 1.0,
            weight_memory: 1.0,
            weight_storage: 1.0,
            weight_replication: 1.0,
            weight_pressure: 1.0,
            weight_transfer: 1.0,
            weight_migration: 1.0,
        }
    }
}

/// The named components produced by a decision, for replayable explanation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DecisionComponents {
    pub expected_keep_value: f64,
    pub expected_keep_cost: f64,
    pub reclaim_score: f64,
    pub retention_benefit: f64,
    pub components: Vec<(String, f64)>,
}

impl DecisionComponents {
    pub fn get(&self, name: &str) -> Option<f64> {
        self.components
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| *v)
    }
}

/// Compute the full decision surface from inputs + weights.
///
/// Baseline formulation (spec section "Reclamation Economics"):
///
/// ```text
/// ExpectedKeepValue =
///     ExpectedReuseValue
///   + ReconstructionAvoidanceValue
///   + DependencyValue
///   + SurvivabilityValue
///
/// ExpectedKeepCost =
///     MemoryCost
///   + StorageCost
///   + ReplicationCost
///   + PressureSurcharge
///   + TransferCost
///   + MigrationCost
///
/// ReclaimScore = ExpectedKeepCost - ExpectedKeepValue
/// ```
///
/// Every component is recorded individually so policies can weigh subsets
/// and explanations can name the driving factor.
pub fn evaluate(
    inputs: &CostInputs,
    weights: &CostWeights,
    dependency_value: f64,
    survivability_value: f64,
) -> DecisionComponents {
    let reconstruction_avoidance_value =
        inputs.reconstruction_cost * inputs.reconstruction_avoidance_fraction;

    let expected_keep_value = weights.weight_reuse_value * inputs.expected_reuse_value
        + weights.weight_reconstruction_avoidance * reconstruction_avoidance_value
        + weights.weight_dependency_value * dependency_value
        + weights.weight_survivability_value * survivability_value;

    let memory_cost = weights.weight_memory * inputs.memory_cost;
    let storage_cost = weights.weight_storage * inputs.storage_cost;
    let replication_cost = weights.weight_replication * inputs.replication_cost;
    let pressure_cost = weights.weight_pressure * inputs.memory_pressure_cost;
    let transfer_cost = weights.weight_transfer * inputs.transfer_cost;
    let migration_cost = weights.weight_migration * inputs.migration_cost;
    let expected_keep_cost = memory_cost
        + storage_cost
        + replication_cost
        + pressure_cost
        + transfer_cost
        + migration_cost;

    let retain_benefit = expected_keep_value - inputs.retain_cost;
    let reclaim_score = expected_keep_cost - expected_keep_value;

    DecisionComponents {
        expected_keep_value,
        expected_keep_cost,
        reclaim_score,
        retention_benefit: retain_benefit,
        components: vec![
            ("expected_reuse_value".into(), inputs.expected_reuse_value),
            ("reconstruction_cost".into(), inputs.reconstruction_cost),
            (
                "reconstruction_avoidance_value".into(),
                reconstruction_avoidance_value,
            ),
            ("dependency_value".into(), dependency_value),
            ("survivability_value".into(), survivability_value),
            ("retain_cost".into(), inputs.retain_cost),
            ("storage_cost".into(), inputs.storage_cost),
            ("memory_cost".into(), inputs.memory_cost),
            ("memory_pressure_cost".into(), inputs.memory_pressure_cost),
            ("replication_cost".into(), inputs.replication_cost),
            ("transfer_cost".into(), inputs.transfer_cost),
            ("migration_cost".into(), inputs.migration_cost),
            ("compression_cost".into(), inputs.compression_cost),
            ("archive_cost".into(), inputs.archive_cost),
            (
                "failure_recovery_value".into(),
                inputs.failure_recovery_value,
            ),
            ("expected_keep_value".into(), expected_keep_value),
            ("expected_keep_cost".into(), expected_keep_cost),
            ("reclaim_score".into(), reclaim_score),
            ("retention_benefit".into(), retain_benefit),
        ],
    }
}

/// Decision/action vocabulary. The built-in policy engine currently emits
/// `Retain` or `Reclaim`; the remaining variants are not selected by
/// [`crate::policy::decide`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ReclaimVerdict {
    Retain,
    Reclaim,
    Demote,
    Compress,
    Deduplicate,
    Archive,
    MarkRecomputable,
}

/// Final decision for an object, fully explained.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Decision {
    pub object_id: uuid::Uuid,
    pub generation: u64,
    pub verdict: ReclaimVerdict,
    pub score: f64,
    pub threshold: f64,
    pub policy_id: String,
    pub policy_version: String,
    pub epoch: u64,
    pub components: DecisionComponents,
    pub reasons: Vec<String>,
}

/// Result of a decision request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecisionReport {
    pub decision: Decision,
    pub explanation: serde_json::Value,
}

/// Convenience: build a human/machine readable explanation object.
pub fn build_explanation(d: &Decision, obj: &ReclaimObject, pressure: &str) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    map.insert("decision".into(), serde_json::json!(d.verdict));
    map.insert("score".into(), serde_json::json!(d.score));
    map.insert("threshold".into(), serde_json::json!(d.threshold));
    map.insert(
        "policy".into(),
        serde_json::json!(format!("{}-{}", d.policy_id, d.policy_version)),
    );
    map.insert(
        "reuse_probability".into(),
        serde_json::json!(obj.reuse_probability),
    );
    map.insert(
        "expected_reuse_horizon_secs".into(),
        serde_json::json!(obj.reuse_horizon_secs),
    );
    map.insert("logical_size".into(), serde_json::json!(obj.logical_size));
    map.insert(
        "recompute_cost".into(),
        serde_json::json!(obj.recompute_cost),
    );
    map.insert(
        "recomputation_latency_secs".into(),
        serde_json::json!(obj.recompute_latency_secs),
    );
    map.insert("pressure".into(), serde_json::json!(pressure));
    map.insert("epoch".into(), serde_json::json!(d.epoch));
    for (name, value) in &d.components.components {
        map.insert(format!("component_{name}"), serde_json::json!(value));
    }
    serde_json::Value::Object(map)
}

/// Validate that all values are finite, so decisions are deterministic and
/// serializable.
pub fn validate_finite(inputs: &CostInputs) -> Result<()> {
    for (name, value) in [
        ("retain_cost", inputs.retain_cost),
        ("reconstruction_cost", inputs.reconstruction_cost),
        ("expected_reuse_value", inputs.expected_reuse_value),
        ("transfer_cost", inputs.transfer_cost),
        ("migration_cost", inputs.migration_cost),
        ("storage_cost", inputs.storage_cost),
        ("memory_cost", inputs.memory_cost),
        ("memory_pressure_cost", inputs.memory_pressure_cost),
        ("replication_cost", inputs.replication_cost),
        ("compression_cost", inputs.compression_cost),
        ("archive_cost", inputs.archive_cost),
        ("failure_recovery_value", inputs.failure_recovery_value),
        ("dependency_value", inputs.dependency_value),
        ("survivability_value", inputs.survivability_value),
        (
            "reconstruction_avoidance_fraction",
            inputs.reconstruction_avoidance_fraction,
        ),
    ] {
        if value.is_nan() || value.is_infinite() || value < 0.0 {
            return Err(crate::errors::ReclaimError::InvalidArgument(format!(
                "cost input {name} must be finite and non-negative, got {value}"
            )));
        }
    }
    if !(0.0..=1.0).contains(&inputs.reconstruction_avoidance_fraction) {
        return Err(crate::errors::ReclaimError::InvalidArgument(
            "reconstruction_avoidance_fraction must be in [0,1]".into(),
        ));
    }
    Ok(())
}

/// Validate policy-owned weights before arithmetic. Negative or non-finite
/// weights can invert component meaning or produce non-serializable decisions.
pub fn validate_weights(weights: &CostWeights) -> Result<()> {
    for (name, value) in [
        ("weight_reuse_value", weights.weight_reuse_value),
        (
            "weight_reconstruction_avoidance",
            weights.weight_reconstruction_avoidance,
        ),
        ("weight_dependency_value", weights.weight_dependency_value),
        (
            "weight_survivability_value",
            weights.weight_survivability_value,
        ),
        ("weight_memory", weights.weight_memory),
        ("weight_storage", weights.weight_storage),
        ("weight_replication", weights.weight_replication),
        ("weight_pressure", weights.weight_pressure),
        ("weight_transfer", weights.weight_transfer),
        ("weight_migration", weights.weight_migration),
    ] {
        if !value.is_finite() || value < 0.0 {
            return Err(crate::errors::ReclaimError::InvalidArgument(format!(
                "cost weight {name} must be finite and non-negative, got {value}"
            )));
        }
    }
    Ok(())
}

/// Validate arithmetic output before it reaches comparisons, persistence, or
/// JSON explanation generation. Finite inputs can still overflow when
/// multiplied or summed.
pub fn validate_components(components: &DecisionComponents) -> Result<()> {
    for (name, value) in [
        ("expected_keep_value", components.expected_keep_value),
        ("expected_keep_cost", components.expected_keep_cost),
        ("reclaim_score", components.reclaim_score),
        ("retention_benefit", components.retention_benefit),
    ] {
        if !value.is_finite() {
            return Err(crate::errors::ReclaimError::InvalidArgument(format!(
                "decision component {name} must be finite, got {value}"
            )));
        }
    }
    for (name, value) in &components.components {
        if !value.is_finite() {
            return Err(crate::errors::ReclaimError::InvalidArgument(format!(
                "decision component {name} must be finite, got {value}"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn sample_obj() -> ReclaimObject {
        let mut o = ReclaimObject::new(Uuid::new_v4(), 0, "test", 1000, 0);
        o.physical_size = 1000;
        o.reuse_probability = 0.9;
        o.recompute_cost = Some(5000.0);
        o.storage_cost_per_byte_sec = 0.001;
        o.memory_cost_per_byte_sec = 0.002;
        o
    }

    #[test]
    fn cheap_reuse_expensive_recompute_retains() {
        let obj = sample_obj();
        let inputs = CostInputs::derive(&obj, 0, 1.0);
        validate_finite(&inputs).unwrap();
        let out = evaluate(&inputs, &CostWeights::default(), 0.0, 0.0);
        // Value (reuse ~ 0.9*5000 + reconstruction avoidance 4500) far exceeds costs.
        assert!(
            out.reclaim_score < 0.0,
            "score should be negative (retain), got {}",
            out.reclaim_score
        );
    }

    #[test]
    fn cheap_recompute_expensive_retain_reclaims() {
        let mut obj = sample_obj();
        obj.reuse_probability = 0.01;
        obj.recompute_cost = Some(5.0);
        obj.memory_cost_per_byte_sec = 10.0;
        let inputs = CostInputs::derive(&obj, 0, 5.0);
        let out = evaluate(&inputs, &CostWeights::default(), 0.0, 0.0);
        assert!(out.reclaim_score > 0.0);
    }

    #[test]
    fn dependency_value_blocks_reclaim_decision() {
        let obj = sample_obj();
        let inputs = CostInputs::derive(&obj, 0, 1.0);
        let no_dep = evaluate(&inputs, &CostWeights::default(), 0.0, 0.0);
        let with_dep = evaluate(&inputs, &CostWeights::default(), 1e9, 0.0);
        assert!(with_dep.reclaim_score < no_dep.reclaim_score);
    }

    #[test]
    fn deterministic_same_inputs_same_output() {
        let obj = sample_obj();
        let inputs = CostInputs::derive(&obj, 12345, 2.0);
        let a = evaluate(&inputs, &CostWeights::default(), 1.0, 2.0);
        let b = evaluate(&inputs, &CostWeights::default(), 1.0, 2.0);
        assert_eq!(a, b);
        assert_eq!(a.components, b.components);
    }

    #[test]
    fn horizon_decays_reuse_value() {
        let mut obj = sample_obj();
        obj.reuse_horizon_secs = Some(0);
        let near = CostInputs::derive(&obj, 0, 1.0).expected_reuse_value;
        obj.reuse_horizon_secs = Some(30 * 86_400);
        let far = CostInputs::derive(&obj, 0, 1.0).expected_reuse_value;
        assert!(near > far);
    }

    #[test]
    fn transfer_and_migration_weights_apply_independently() {
        let inputs = CostInputs {
            transfer_cost: 10.0,
            migration_cost: 20.0,
            ..CostInputs::default()
        };

        let baseline = evaluate(&inputs, &CostWeights::default(), 0.0, 0.0);
        assert_eq!(baseline.expected_keep_cost, 30.0);

        let weights = CostWeights {
            weight_transfer: 0.0,
            weight_migration: 2.0,
            ..CostWeights::default()
        };
        let weighted = evaluate(&inputs, &weights, 0.0, 0.0);
        assert_eq!(weighted.expected_keep_cost, 40.0);
    }

    #[test]
    fn memory_and_pressure_weights_apply_to_distinct_components() {
        let inputs = CostInputs {
            memory_cost: 10.0,
            memory_pressure_cost: 20.0,
            retain_cost: 30.0,
            ..CostInputs::default()
        };
        let baseline = evaluate(&inputs, &CostWeights::default(), 0.0, 0.0);
        assert_eq!(baseline.expected_keep_cost, 30.0);
        assert_eq!(baseline.get("memory_cost"), Some(10.0));
        assert_eq!(baseline.get("memory_pressure_cost"), Some(20.0));

        let no_memory = evaluate(
            &inputs,
            &CostWeights {
                weight_memory: 0.0,
                ..CostWeights::default()
            },
            0.0,
            0.0,
        );
        assert_eq!(no_memory.expected_keep_cost, 20.0);

        let no_pressure = evaluate(
            &inputs,
            &CostWeights {
                weight_pressure: 0.0,
                ..CostWeights::default()
            },
            0.0,
            0.0,
        );
        assert_eq!(no_pressure.expected_keep_cost, 10.0);
    }

    #[test]
    fn normal_pressure_has_no_surcharge_and_higher_levels_scale_once() {
        let mut obj = ReclaimObject::new(Uuid::new_v4(), 0, "test", 100, 0);
        obj.physical_size = 100;
        obj.memory_cost_per_byte_sec = 2.0;

        let normal = CostInputs::derive(&obj, 0, 1.0);
        assert_eq!(normal.memory_cost, 200.0);
        assert_eq!(normal.memory_pressure_cost, 0.0);
        assert_eq!(normal.retain_cost, 200.0);

        let high = CostInputs::derive(&obj, 0, 4.0);
        assert_eq!(high.memory_cost, 200.0);
        assert_eq!(high.memory_pressure_cost, 600.0);
        assert_eq!(high.retain_cost, 800.0);
    }

    #[test]
    fn invalid_weights_and_overflowed_outputs_are_rejected() {
        assert!(validate_weights(&CostWeights {
            weight_pressure: f64::NAN,
            ..CostWeights::default()
        })
        .is_err());
        assert!(validate_weights(&CostWeights {
            weight_pressure: -1.0,
            ..CostWeights::default()
        })
        .is_err());

        let inputs = CostInputs {
            storage_cost: f64::MAX,
            replication_cost: f64::MAX,
            ..CostInputs::default()
        };
        let components = evaluate(&inputs, &CostWeights::default(), 0.0, 0.0);
        assert!(validate_components(&components).is_err());
    }
}
