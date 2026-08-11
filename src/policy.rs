//! Deterministic reclamation policy engine.
//!
//! Policies are serializable, versioned documents. The engine resolves the
//! applicable policy for (object, pressure) by specificity:
//!
//! ```text
//! emergency pressure > pressure-level > object-class > owner > durability > default
//! ```
//!
//! Every decision records the exact policy id + version used. Policy changes
//! never retroactively mutate audit history.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::economics::{
    build_explanation, evaluate, validate_components, validate_finite, validate_weights,
    CostWeights, Decision, DecisionComponents, ReclaimVerdict,
};
use crate::errors::{ReclaimError, Result};
use crate::object::ReclaimObject;
use crate::pressure::PressureLevel;

/// Policy matching categories. The built-in resolver currently implements
/// default, class, owner, pressure, durability, survivability, and emergency
/// matching; registration rejects the remaining categories until a matcher is
/// implemented.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PolicyKind {
    Default,
    ObjectClass,
    Owner,
    Pressure,
    Durability,
    Survivability,
    Age,
    Reuse,
    Recomposition,
    Dependency,
    Emergency,
}

/// Recommendation the policy engine attaches to a score.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Recommendation {
    Retain,
    Reclaim,
}

/// A single versioned policy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Policy {
    /// Stable identifier shared across versions ("reclaim-default").
    pub id: String,
    /// Explicit version ("v1"). Policy id + version uniquely identify a document.
    pub version: String,
    pub kind: PolicyKind,
    /// Score at or above which the object is a reclaim candidate.
    pub reclaim_threshold: f64,
    /// Minimum reuse probability to justify retention even if score favors reclaim.
    pub min_reuse_probability: f64,
    pub weights: CostWeights,
    /// Optional match: object class (for ObjectClass policies).
    pub match_class: Option<String>,
    /// Optional match: owner (for Owner policies).
    pub match_owner: Option<String>,
    /// Optional match: pressure level (for Pressure policies).
    pub match_pressure: Option<PressureLevel>,
    /// Optional match: durability class (for Durability policies).
    pub match_durability: Option<crate::object::DurabilityClass>,
    /// Optional match: survivability class (for Survivability policies).
    pub match_survivability: Option<crate::object::SurvivabilityClass>,
    /// Emergency policies may run under CRITICAL pressure with tighter
    /// thresholds but can never override pins, protection, or survivability.
    pub emergency: bool,
    /// Human description (for `policy inspect`).
    pub description: String,
}

impl Policy {
    pub fn full_id(&self) -> String {
        format!("{}-{}", self.id, self.version)
    }
}

/// Default built-in policy, versioned v1.
pub fn default_policy() -> Policy {
    Policy {
        id: "reclaim-default".into(),
        version: "v1".into(),
        kind: PolicyKind::Default,
        reclaim_threshold: 0.0,
        min_reuse_probability: 0.05,
        weights: CostWeights::default(),
        match_class: None,
        match_owner: None,
        match_pressure: None,
        match_durability: None,
        match_survivability: None,
        emergency: false,
        description:
            "Baseline policy: reclaim when expected keep cost exceeds expected keep value.".into(),
    }
}

/// Emergency policy for CRITICAL pressure. Does not override invariants.
pub fn emergency_policy() -> Policy {
    let mut p = default_policy();
    p.id = "reclaim-emergency".into();
    p.kind = PolicyKind::Emergency;
    p.emergency = true;
    p.match_pressure = Some(PressureLevel::Critical);
    // More aggressive than the default: state with up to 50% expected reuse
    // becomes reclaimable under CRITICAL pressure.
    p.min_reuse_probability = 0.5;
    p.description =
        "Emergency policy: aggressive reclamation under CRITICAL pressure. Pins, protection, \
         and survivability invariants still apply."
            .into();
    p
}

/// Policy registry: deterministic lookup of applicable policy.
#[derive(Debug, Clone, Default)]
pub struct PolicyRegistry {
    policies: Vec<Policy>,
}

impl PolicyRegistry {
    pub fn with_defaults() -> PolicyRegistry {
        let mut r = PolicyRegistry::default();
        r.policies.push(default_policy());
        r.policies.push(emergency_policy());
        r
    }

    pub fn add(&mut self, policy: Policy) -> Result<()> {
        validate_policy(&policy)?;
        if self
            .policies
            .iter()
            .any(|p| p.id == policy.id && p.version == policy.version)
        {
            return Err(ReclaimError::Policy(format!(
                "policy {}-{} already registered",
                policy.id, policy.version
            )));
        }
        self.policies.push(policy);
        Ok(())
    }

    pub fn list(&self) -> Vec<&Policy> {
        let mut v: Vec<&Policy> = self.policies.iter().collect();
        v.sort_by_key(|a| a.full_id());
        v
    }

    pub fn get(&self, id: &str, version: &str) -> Result<&Policy> {
        self.policies
            .iter()
            .find(|p| p.id == id && p.version == version)
            .ok_or_else(|| ReclaimError::Policy(format!("policy {id}-{version} not found")))
    }

    /// Validate that the registry can make an unambiguous decision for every
    /// object. Persisted registries are checked at startup so malformed or
    /// partial policy state fails before the coordinator claims authority.
    pub fn validate_complete(&self) -> Result<()> {
        for policy in &self.policies {
            validate_policy(policy)?;
        }

        let default_count = self
            .policies
            .iter()
            .filter(|policy| policy.kind == PolicyKind::Default)
            .count();
        if default_count != 1 {
            return Err(ReclaimError::Policy(format!(
                "policy registry must contain exactly one default policy, found {default_count}"
            )));
        }

        let mut selectors: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for policy in &self.policies {
            selectors
                .entry(policy_selector_key(policy))
                .or_default()
                .push(policy.full_id());
        }
        for (selector, mut ids) in selectors {
            if ids.len() > 1 {
                ids.sort();
                return Err(ReclaimError::Policy(format!(
                    "ambiguous policy selector {selector}: {}",
                    ids.join(", ")
                )));
            }
        }
        Ok(())
    }

    /// Resolve the most specific applicable policy for (object, pressure).
    pub fn resolve(&self, obj: &ReclaimObject, pressure: PressureLevel) -> Result<&Policy> {
        // Specificity order: emergency (critical pressure) > pressure > class >
        // owner > durability > default.
        let emergency = unique_policy(
            self.policies.iter().filter(|p| {
                p.kind == PolicyKind::Emergency && p.emergency && p.match_pressure == Some(pressure)
            }),
            "emergency",
        )?;
        if let Some(p) = emergency {
            return Ok(p);
        }
        let by_pressure = unique_policy(
            self.policies.iter().filter(|p| {
                p.kind == PolicyKind::Pressure && !p.emergency && p.match_pressure == Some(pressure)
            }),
            "pressure",
        )?;
        if let Some(p) = by_pressure {
            return Ok(p);
        }
        let by_class = unique_policy(
            self.policies.iter().filter(|p| {
                p.kind == PolicyKind::ObjectClass
                    && p.match_class.as_deref() == Some(obj.class.as_str())
            }),
            "object-class",
        )?;
        if let Some(p) = by_class {
            return Ok(p);
        }
        let by_owner = unique_policy(
            self.policies.iter().filter(|p| {
                p.kind == PolicyKind::Owner && p.match_owner.as_deref() == Some(obj.owner.as_str())
            }),
            "owner",
        )?;
        if let Some(p) = by_owner {
            return Ok(p);
        }
        let by_durability = unique_policy(
            self.policies.iter().filter(|p| {
                p.kind == PolicyKind::Durability && p.match_durability == Some(obj.durability_class)
            }),
            "durability",
        )?;
        if let Some(p) = by_durability {
            return Ok(p);
        }
        let by_survivability = unique_policy(
            self.policies.iter().filter(|p| {
                p.kind == PolicyKind::Survivability
                    && p.match_survivability == Some(obj.survivability_class)
            }),
            "survivability",
        )?;
        if let Some(p) = by_survivability {
            return Ok(p);
        }
        unique_policy(
            self.policies
                .iter()
                .filter(|p| p.kind == PolicyKind::Default),
            "default",
        )?
        .ok_or_else(|| ReclaimError::Policy("no default policy registered".into()))
    }

    pub fn to_json(&self) -> Result<String> {
        Ok(serde_json::to_string_pretty(&self.policies)?)
    }
}

fn policy_selector_key(policy: &Policy) -> String {
    match policy.kind {
        PolicyKind::Default => "DEFAULT".into(),
        PolicyKind::ObjectClass => format!(
            "OBJECT_CLASS:{}",
            policy.match_class.as_deref().unwrap_or_default()
        ),
        PolicyKind::Owner => format!(
            "OWNER:{}",
            policy.match_owner.as_deref().unwrap_or_default()
        ),
        PolicyKind::Pressure => format!(
            "PRESSURE:{}",
            policy.match_pressure.map_or("", |level| level.as_str())
        ),
        PolicyKind::Durability => format!("DURABILITY:{:?}", policy.match_durability),
        PolicyKind::Survivability => {
            format!("SURVIVABILITY:{:?}", policy.match_survivability)
        }
        PolicyKind::Emergency => format!(
            "EMERGENCY:{}",
            policy.match_pressure.map_or("", |level| level.as_str())
        ),
        PolicyKind::Age
        | PolicyKind::Reuse
        | PolicyKind::Recomposition
        | PolicyKind::Dependency => format!("UNSUPPORTED:{:?}", policy.kind),
    }
}

/// Return the sole policy at a specificity tier. Silently taking the first
/// match makes decisions depend on registration or database row order.
fn unique_policy<'a>(
    matches: impl Iterator<Item = &'a Policy>,
    specificity: &str,
) -> Result<Option<&'a Policy>> {
    let mut matches: Vec<&Policy> = matches.collect();
    if matches.len() > 1 {
        matches.sort_by_key(|p| p.full_id());
        let ids: Vec<String> = matches.iter().map(|p| p.full_id()).collect();
        return Err(ReclaimError::Policy(format!(
            "ambiguous {specificity} policies: {}",
            ids.join(", ")
        )));
    }
    Ok(matches.pop())
}

/// Validate a policy document before registration.
pub fn validate_policy(p: &Policy) -> Result<()> {
    if p.id.trim().is_empty() || p.version.trim().is_empty() {
        return Err(ReclaimError::Policy(
            "policy id and version must not be empty".into(),
        ));
    }
    if !p.reclaim_threshold.is_finite() {
        return Err(ReclaimError::Policy(
            "reclaim_threshold must be finite".into(),
        ));
    }
    if !(0.0..=1.0).contains(&p.min_reuse_probability) {
        return Err(ReclaimError::Policy(
            "min_reuse_probability must be in [0,1]".into(),
        ));
    }
    validate_weights(&p.weights).map_err(|e| ReclaimError::Policy(e.to_string()))?;

    let no_class = p.match_class.is_none();
    let no_owner = p.match_owner.is_none();
    let no_pressure = p.match_pressure.is_none();
    let no_durability = p.match_durability.is_none();
    let no_survivability = p.match_survivability.is_none();
    let only_class = p
        .match_class
        .as_deref()
        .is_some_and(|value| !value.trim().is_empty())
        && no_owner
        && no_pressure
        && no_durability
        && no_survivability;
    let only_owner = p
        .match_owner
        .as_deref()
        .is_some_and(|value| !value.trim().is_empty())
        && no_class
        && no_pressure
        && no_durability
        && no_survivability;
    let only_pressure =
        p.match_pressure.is_some() && no_class && no_owner && no_durability && no_survivability;
    let only_durability =
        p.match_durability.is_some() && no_class && no_owner && no_pressure && no_survivability;
    let only_survivability =
        p.match_survivability.is_some() && no_class && no_owner && no_pressure && no_durability;
    let no_selectors = no_class && no_owner && no_pressure && no_durability && no_survivability;

    let selector_valid = match p.kind {
        PolicyKind::Default => no_selectors && !p.emergency,
        PolicyKind::ObjectClass => only_class && !p.emergency,
        PolicyKind::Owner => only_owner && !p.emergency,
        PolicyKind::Pressure => only_pressure && !p.emergency,
        PolicyKind::Durability => only_durability && !p.emergency,
        PolicyKind::Survivability => only_survivability && !p.emergency,
        PolicyKind::Emergency => {
            only_pressure && p.emergency && p.match_pressure == Some(PressureLevel::Critical)
        }
        PolicyKind::Age
        | PolicyKind::Reuse
        | PolicyKind::Recomposition
        | PolicyKind::Dependency => {
            return Err(ReclaimError::Policy(format!(
                "policy kind {:?} is not supported by the current resolver",
                p.kind
            )))
        }
    };
    if !selector_valid {
        return Err(ReclaimError::Policy(format!(
            "policy kind {:?} has inconsistent match selectors or emergency flag",
            p.kind
        )));
    }
    Ok(())
}

/// Output of the policy engine for one object.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyDecision {
    pub decision: Decision,
    /// Object state snapshot at decision time (for explanation/replay).
    pub object_snapshot: ReclaimObject,
    pub pressure: PressureLevel,
    pub explanation: serde_json::Value,
}

/// Context passed to the decision engine for lineage-aware values.
#[derive(Debug, Clone, Default)]
pub struct DecisionContext {
    /// Aggregate dependency value (how much downstream state relies on this).
    pub dependency_value: f64,
    /// Survivability value (redundancy worth preserving).
    pub survivability_value: f64,
}

/// Deterministic decision for one object under one pressure + policy.
pub fn decide(
    registry: &PolicyRegistry,
    obj: &ReclaimObject,
    pressure: PressureLevel,
    epoch: u64,
    now_ms: i64,
    context: &DecisionContext,
) -> Result<PolicyDecision> {
    obj.validate()?;
    let policy = registry.resolve(obj, pressure)?;
    validate_policy(policy)?;
    for (name, value) in [
        ("dependency_value", context.dependency_value),
        ("survivability_value", context.survivability_value),
    ] {
        if !value.is_finite() || value < 0.0 {
            return Err(ReclaimError::InvalidArgument(format!(
                "decision context {name} must be finite and non-negative, got {value}"
            )));
        }
    }
    let pressure_multiplier = pressure.multiplier();
    let inputs = crate::economics::CostInputs::derive(obj, now_ms, pressure_multiplier);
    validate_finite(&inputs)?;

    let out: DecisionComponents = evaluate(
        &inputs,
        &policy.weights,
        context.dependency_value,
        context.survivability_value,
    );
    validate_components(&out)?;

    // Pin/protection are hard invariants: even emergency policies cannot
    // recommend reclaiming pinned or protected state.
    if obj.pinned || obj.protected {
        let mut reasons = Vec::new();
        if obj.pinned {
            reasons.push("object_is_pinned".into());
        }
        if obj.protected {
            reasons.push("object_is_protected".into());
        }
        reasons.push("hard_invariant_policy_cannot_recommend_reclaim".into());
        let d = Decision {
            object_id: obj.id,
            generation: obj.generation,
            verdict: ReclaimVerdict::Retain,
            score: out.reclaim_score,
            threshold: policy.reclaim_threshold,
            policy_id: policy.id.clone(),
            policy_version: policy.version.clone(),
            epoch,
            components: out,
            reasons,
        };
        let explanation = build_explanation(&d, obj, pressure.as_str());
        return Ok(PolicyDecision {
            decision: d,
            object_snapshot: obj.clone(),
            pressure,
            explanation,
        });
    }

    let verdict = if out.reclaim_score >= policy.reclaim_threshold
        && obj.reuse_probability <= policy.min_reuse_probability
    {
        ReclaimVerdict::Reclaim
    } else {
        ReclaimVerdict::Retain
    };

    let mut reasons = vec![format!("reuse_probability={:.6}", obj.reuse_probability)];
    if let Some(h) = obj.reuse_horizon_secs {
        reasons.push(format!("expected_reuse_horizon={h}s"));
    }
    reasons.push(format!("logical_size={}", obj.logical_size));
    reasons.push(format!(
        "recompute_cost={}",
        obj.recompute_cost.unwrap_or(0.0)
    ));
    reasons.push(format!("score={:.4}", out.reclaim_score));
    reasons.push(format!("threshold={:.4}", policy.reclaim_threshold));
    reasons.push(format!("pressure={}", pressure.as_str()));
    reasons.push(format!("policy={}", policy.full_id()));

    let d = Decision {
        object_id: obj.id,
        generation: obj.generation,
        verdict,
        score: out.reclaim_score,
        threshold: policy.reclaim_threshold,
        policy_id: policy.id.clone(),
        policy_version: policy.version.clone(),
        epoch,
        components: out,
        reasons,
    };
    let explanation = build_explanation(&d, obj, pressure.as_str());
    Ok(PolicyDecision {
        decision: d,
        object_snapshot: obj.clone(),
        pressure,
        explanation,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn obj_with(reuse: f64, recompute: f64, memory_rate: f64) -> ReclaimObject {
        let mut o = ReclaimObject::new(Uuid::new_v4(), 0, "checkpoint", 100_000, 0);
        o.physical_size = 100_000;
        o.reuse_probability = reuse;
        o.recompute_cost = Some(recompute);
        o.memory_cost_per_byte_sec = memory_rate;
        o
    }

    #[test]
    fn resolve_default_policy() {
        let reg = PolicyRegistry::with_defaults();
        let o = obj_with(0.5, 1.0, 0.01);
        assert_eq!(
            reg.resolve(&o, PressureLevel::Normal).unwrap().id,
            "reclaim-default"
        );
    }

    #[test]
    fn emergency_policy_resolved_at_critical() {
        let reg = PolicyRegistry::with_defaults();
        let o = obj_with(0.5, 1.0, 0.01);
        assert!(reg.resolve(&o, PressureLevel::Critical).unwrap().emergency);
        assert!(!reg.resolve(&o, PressureLevel::Normal).unwrap().emergency);
    }

    #[test]
    fn class_policy_takes_precedence_over_default() {
        let mut reg = PolicyRegistry::with_defaults();
        reg.add(Policy {
            id: "reclaim-checkpoints".into(),
            version: "v1".into(),
            kind: PolicyKind::ObjectClass,
            reclaim_threshold: 100.0,
            min_reuse_probability: 0.9,
            weights: CostWeights::default(),
            match_class: Some("checkpoint".into()),
            match_owner: None,
            match_pressure: None,
            match_durability: None,
            match_survivability: None,
            emergency: false,
            description: "checkpoint policy".into(),
        })
        .unwrap();
        let o = obj_with(0.5, 1.0, 0.01);
        assert_eq!(
            reg.resolve(&o, PressureLevel::Normal).unwrap().id,
            "reclaim-checkpoints"
        );
    }

    #[test]
    fn duplicate_policy_rejected() {
        let mut reg = PolicyRegistry::with_defaults();
        assert!(reg.add(default_policy()).is_err());
    }

    #[test]
    fn complete_registry_requires_one_default_and_unique_selectors() {
        PolicyRegistry::with_defaults().validate_complete().unwrap();

        let mut missing_default = PolicyRegistry::default();
        let mut class_policy = default_policy();
        class_policy.id = "class-only".into();
        class_policy.kind = PolicyKind::ObjectClass;
        class_policy.match_class = Some("checkpoint".into());
        missing_default.add(class_policy.clone()).unwrap();
        let err = missing_default.validate_complete().unwrap_err();
        assert!(err.to_string().contains("exactly one default policy"));

        let mut ambiguous = PolicyRegistry::with_defaults();
        ambiguous.add(class_policy).unwrap();
        let mut second = default_policy();
        second.id = "class-second".into();
        second.kind = PolicyKind::ObjectClass;
        second.match_class = Some("checkpoint".into());
        ambiguous.add(second).unwrap();
        let err = ambiguous.validate_complete().unwrap_err();
        assert!(err.to_string().contains("class-only-v1, class-second-v1"));
    }

    #[test]
    fn invalid_emergency_policy_rejected() {
        let mut p = emergency_policy();
        p.match_pressure = Some(PressureLevel::High);
        assert!(validate_policy(&p).is_err());
    }

    #[test]
    fn incoherent_or_unsupported_policy_kinds_are_rejected() {
        let mut p = default_policy();
        p.id = "bad-selector".into();
        p.kind = PolicyKind::Owner;
        p.match_class = Some("checkpoint".into());
        assert!(validate_policy(&p).is_err());

        let mut p = default_policy();
        p.id = "unsupported".into();
        p.kind = PolicyKind::Age;
        assert!(validate_policy(&p).is_err());
    }

    #[test]
    fn non_finite_policy_weights_are_rejected() {
        let mut p = default_policy();
        p.id = "bad-weight".into();
        p.weights.weight_memory = f64::INFINITY;
        assert!(validate_policy(&p).is_err());
    }

    #[test]
    fn ambiguous_same_specificity_match_is_an_error() {
        let mut reg = PolicyRegistry::with_defaults();
        for id in ["class-a", "class-b"] {
            let mut p = default_policy();
            p.id = id.into();
            p.kind = PolicyKind::ObjectClass;
            p.match_class = Some("checkpoint".into());
            reg.add(p).unwrap();
        }
        let o = obj_with(0.5, 1.0, 0.01);
        let err = reg.resolve(&o, PressureLevel::Normal).unwrap_err();
        assert!(err.to_string().contains("class-a-v1, class-b-v1"));
    }

    #[test]
    fn pinned_object_never_recommended_for_reclaim() {
        let reg = PolicyRegistry::with_defaults();
        let mut o = obj_with(0.001, 0.001, 100.0);
        o.pinned = true;
        let pd = decide(
            &reg,
            &o,
            PressureLevel::Critical,
            1,
            0,
            &DecisionContext::default(),
        )
        .unwrap();
        assert_eq!(pd.decision.verdict, ReclaimVerdict::Retain);
        assert!(pd.decision.reasons.iter().any(|r| r == "object_is_pinned"));
    }

    #[test]
    fn all_active_hard_invariants_are_explained() {
        let reg = PolicyRegistry::with_defaults();
        let mut o = obj_with(0.001, 0.001, 100.0);
        o.pinned = true;
        o.protected = true;
        let pd = decide(
            &reg,
            &o,
            PressureLevel::Critical,
            1,
            0,
            &DecisionContext::default(),
        )
        .unwrap();
        assert!(pd.decision.reasons.iter().any(|r| r == "object_is_pinned"));
        assert!(pd
            .decision
            .reasons
            .iter()
            .any(|r| r == "object_is_protected"));
    }

    #[test]
    fn invalid_context_and_overflowed_decisions_are_rejected() {
        let reg = PolicyRegistry::with_defaults();
        let o = obj_with(0.5, 1.0, 0.01);
        assert!(decide(
            &reg,
            &o,
            PressureLevel::Normal,
            1,
            0,
            &DecisionContext {
                dependency_value: f64::NAN,
                survivability_value: 0.0,
            },
        )
        .is_err());

        assert!(decide(
            &reg,
            &o,
            PressureLevel::Normal,
            1,
            0,
            &DecisionContext {
                dependency_value: f64::MAX,
                survivability_value: f64::MAX,
            },
        )
        .is_err());
    }

    #[test]
    fn deterministic_decisions() {
        let reg = PolicyRegistry::with_defaults();
        let o = obj_with(0.01, 1.0, 1.0);
        let a = decide(
            &reg,
            &o,
            PressureLevel::High,
            7,
            1000,
            &DecisionContext::default(),
        )
        .unwrap();
        let b = decide(
            &reg,
            &o,
            PressureLevel::High,
            7,
            1000,
            &DecisionContext::default(),
        )
        .unwrap();
        assert_eq!(a.decision.score, b.decision.score);
        assert_eq!(a.decision.reasons, b.decision.reasons);
        assert_eq!(a.explanation, b.explanation);
    }

    #[test]
    fn cheap_recompute_reclaimed_expensive_retained() {
        let reg = PolicyRegistry::with_defaults();
        let cheap = obj_with(0.001, 1.0, 1.0);
        let expensive = obj_with(0.9, 1e9, 1.0);
        let d1 = decide(
            &reg,
            &cheap,
            PressureLevel::High,
            1,
            0,
            &DecisionContext::default(),
        )
        .unwrap();
        let d2 = decide(
            &reg,
            &expensive,
            PressureLevel::High,
            1,
            0,
            &DecisionContext::default(),
        )
        .unwrap();
        assert_eq!(d1.decision.verdict, ReclaimVerdict::Reclaim);
        assert_eq!(d2.decision.verdict, ReclaimVerdict::Retain);
    }
}
