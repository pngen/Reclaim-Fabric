//! Pressure model: configurable, pluggable resource pressure sources.
//!
//! The core runtime never requires real GPU APIs. Pressure providers are
//! pluggable; a synthetic provider is provided for tests and local demos.

use serde::{Deserialize, Serialize};

use crate::errors::{ReclaimError, Result};

/// Pressure levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PressureLevel {
    Normal,
    Elevated,
    High,
    Critical,
}

impl PressureLevel {
    pub fn as_str(&self) -> &'static str {
        match self {
            PressureLevel::Normal => "NORMAL",
            PressureLevel::Elevated => "ELEVATED",
            PressureLevel::High => "HIGH",
            PressureLevel::Critical => "CRITICAL",
        }
    }

    pub fn parse(s: &str) -> Result<PressureLevel> {
        match s {
            "NORMAL" => Ok(PressureLevel::Normal),
            "ELEVATED" => Ok(PressureLevel::Elevated),
            "HIGH" => Ok(PressureLevel::High),
            "CRITICAL" => Ok(PressureLevel::Critical),
            other => Err(crate::errors::ReclaimError::InvalidArgument(format!(
                "unknown pressure level: {other}"
            ))),
        }
    }

    /// Multiplier applied to pressure-sensitive cost components.
    pub fn multiplier(&self) -> f64 {
        match self {
            PressureLevel::Normal => 1.0,
            PressureLevel::Elevated => 2.0,
            PressureLevel::High => 4.0,
            PressureLevel::Critical => 8.0,
        }
    }
}

/// Individual pressure metrics (0.0 = idle, 1.0 = full).
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize, Deserialize)]
pub struct PressureMetrics {
    pub accelerator_memory: f64,
    pub host_memory: f64,
    pub local_storage: f64,
    pub remote_storage: f64,
    pub metadata: f64,
    pub queue: f64,
    pub network: f64,
}

impl PressureMetrics {
    pub fn validate(&self) -> Result<()> {
        for (name, v) in [
            ("accelerator_memory", self.accelerator_memory),
            ("host_memory", self.host_memory),
            ("local_storage", self.local_storage),
            ("remote_storage", self.remote_storage),
            ("metadata", self.metadata),
            ("queue", self.queue),
            ("network", self.network),
        ] {
            if !(0.0..=1.0).contains(&v) || v.is_nan() {
                return Err(crate::errors::ReclaimError::InvalidArgument(format!(
                    "pressure metric {name} must be in [0,1]"
                )));
            }
        }
        Ok(())
    }

    /// Overall level from the highest individual metric, with thresholds.
    pub fn level(&self) -> PressureLevel {
        let max = self
            .accelerator_memory
            .max(self.host_memory)
            .max(self.local_storage)
            .max(self.remote_storage)
            .max(self.metadata)
            .max(self.queue)
            .max(self.network);
        match max {
            m if m >= 0.90 => PressureLevel::Critical,
            m if m >= 0.70 => PressureLevel::High,
            m if m >= 0.45 => PressureLevel::Elevated,
            _ => PressureLevel::Normal,
        }
    }

    fn critical_sentinel() -> PressureMetrics {
        PressureMetrics {
            accelerator_memory: 1.0,
            host_memory: 1.0,
            local_storage: 1.0,
            remote_storage: 1.0,
            metadata: 1.0,
            queue: 1.0,
            network: 1.0,
        }
    }

    fn merge_max(&mut self, other: PressureMetrics) {
        self.accelerator_memory = self.accelerator_memory.max(other.accelerator_memory);
        self.host_memory = self.host_memory.max(other.host_memory);
        self.local_storage = self.local_storage.max(other.local_storage);
        self.remote_storage = self.remote_storage.max(other.remote_storage);
        self.metadata = self.metadata.max(other.metadata);
        self.queue = self.queue.max(other.queue);
        self.network = self.network.max(other.network);
    }
}

/// A pluggable pressure source.
pub trait PressureProvider: Send + Sync {
    /// Human-readable provider id.
    fn id(&self) -> &str;
    /// Current metrics; must always be finite and in range.
    fn metrics(&self) -> PressureMetrics;
}

/// Synthetic provider: manually settable, for tests and local demos.
#[derive(Debug, Clone)]
pub struct SyntheticPressureProvider {
    id: String,
    metrics: std::sync::Arc<std::sync::RwLock<PressureMetrics>>,
}

impl SyntheticPressureProvider {
    pub fn new(id: impl Into<String>) -> SyntheticPressureProvider {
        SyntheticPressureProvider {
            id: id.into(),
            metrics: std::sync::Arc::new(std::sync::RwLock::new(PressureMetrics::default())),
        }
    }

    fn clone_handle(&self) -> SyntheticPressureProvider {
        SyntheticPressureProvider {
            id: self.id.clone(),
            metrics: self.metrics.clone(),
        }
    }

    pub fn set(&self, metrics: PressureMetrics) -> Result<()> {
        metrics.validate()?;
        *self
            .metrics
            .write()
            .map_err(|_| ReclaimError::Pressure("synthetic pressure lock poisoned".into()))? =
            metrics;
        Ok(())
    }

    pub fn set_level(&self, level: PressureLevel) -> Result<()> {
        let m = match level {
            PressureLevel::Normal => PressureMetrics::default(),
            PressureLevel::Elevated => PressureMetrics {
                host_memory: 0.5,
                ..PressureMetrics::default()
            },
            PressureLevel::High => PressureMetrics {
                host_memory: 0.75,
                ..PressureMetrics::default()
            },
            PressureLevel::Critical => PressureMetrics {
                host_memory: 0.95,
                ..PressureMetrics::default()
            },
        };
        self.set(m)
    }
}

impl PressureProvider for SyntheticPressureProvider {
    fn id(&self) -> &str {
        &self.id
    }

    fn metrics(&self) -> PressureMetrics {
        *self
            .metrics
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Registry of pressure providers with an aggregate view.
#[derive(Default)]
pub struct PressureRegistry {
    providers: std::sync::RwLock<Vec<Box<dyn PressureProvider>>>,
    synthetic: std::sync::RwLock<std::collections::HashMap<String, SyntheticPressureProvider>>,
    node_reports: std::sync::RwLock<std::collections::HashMap<String, PressureMetrics>>,
}

impl PressureRegistry {
    pub fn new() -> PressureRegistry {
        PressureRegistry::default()
    }

    pub fn register(&self, provider: Box<dyn PressureProvider>) {
        let id = provider.id().to_string();
        // Keep the settable-synthetic index and the provider list consistent
        // under concurrent replacement. All code taking both locks uses this
        // order.
        let mut synthetic = self
            .synthetic
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut providers = self
            .providers
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        synthetic.remove(&id);
        if let Some(existing) = providers.iter().position(|p| p.id() == id) {
            providers[existing] = provider;
        } else {
            providers.push(provider);
        }
    }

    /// Register a synthetic (manually settable) provider by id.
    pub fn register_synthetic(&self, id: &str) -> SyntheticPressureProvider {
        let provider = SyntheticPressureProvider::new(id.to_string());
        let mut synthetic = self
            .synthetic
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut providers = self
            .providers
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        synthetic.insert(id.to_string(), provider.clone_handle());
        if let Some(existing) = providers.iter().position(|p| p.id() == id) {
            providers[existing] = Box::new(provider.clone_handle());
        } else {
            providers.push(Box::new(provider.clone_handle()));
        }
        provider
    }

    /// Find a registered synthetic provider by id (for manual pressure).
    pub fn find_synthetic(&self, id: &str) -> Option<SyntheticPressureProvider> {
        self.synthetic
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(id)
            .cloned()
    }

    /// Publish the latest metrics reported by a registered node. Reports are
    /// aggregated alongside local providers until the coordinator retires the
    /// node and removes its report.
    pub fn report_node(&self, node_id: &str, metrics: PressureMetrics) -> Result<()> {
        if node_id.trim().is_empty() || node_id.trim() != node_id {
            return Err(ReclaimError::InvalidArgument(
                "pressure-report node id must be non-empty and have no surrounding whitespace"
                    .into(),
            ));
        }
        metrics.validate()?;
        self.node_reports
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(node_id.to_string(), metrics);
        Ok(())
    }

    pub fn remove_node(&self, node_id: &str) {
        self.node_reports
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(node_id);
    }

    /// Aggregate registered providers, rejecting contract-violating metrics.
    pub fn try_aggregate(&self) -> Result<PressureMetrics> {
        let mut acc = PressureMetrics::default();
        let providers = self
            .providers
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for p in providers.iter() {
            let m = p.metrics();
            m.validate().map_err(|e| {
                ReclaimError::Pressure(format!(
                    "pressure provider {:?} returned invalid metrics: {e}",
                    p.id()
                ))
            })?;
            acc.merge_max(m);
        }
        drop(providers);
        for metrics in self
            .node_reports
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
        {
            metrics.validate().map_err(|e| {
                ReclaimError::Pressure(format!("stored node pressure report became invalid: {e}"))
            })?;
            acc.merge_max(*metrics);
        }
        Ok(acc)
    }

    /// Aggregate view used by compatibility callers. A provider contract
    /// violation is logged and treated as CRITICAL rather than silently
    /// disappearing into floating-point `max` behavior.
    pub fn aggregate(&self) -> PressureMetrics {
        self.try_aggregate().unwrap_or_else(|e| {
            log::error!("{e}");
            PressureMetrics::critical_sentinel()
        })
    }

    pub fn try_level(&self) -> Result<PressureLevel> {
        Ok(self.try_aggregate()?.level())
    }

    pub fn level(&self) -> PressureLevel {
        self.aggregate().level()
    }

    pub fn provider_ids(&self) -> Vec<String> {
        let mut ids: Vec<String> = self
            .providers
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(|p| p.id().to_string())
            .collect();
        ids.sort();
        ids
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn level_thresholds() {
        let base = PressureMetrics::default();
        assert_eq!(base.level(), PressureLevel::Normal);
        assert_eq!(
            PressureMetrics {
                host_memory: 0.5,
                ..base
            }
            .level(),
            PressureLevel::Elevated
        );
        assert_eq!(
            PressureMetrics {
                host_memory: 0.75,
                ..base
            }
            .level(),
            PressureLevel::High
        );
        assert_eq!(
            PressureMetrics {
                accelerator_memory: 0.95,
                ..base
            }
            .level(),
            PressureLevel::Critical
        );
    }

    #[test]
    fn validation() {
        let bad = PressureMetrics {
            host_memory: 1.5,
            ..PressureMetrics::default()
        };
        assert!(bad.validate().is_err());
        let good = PressureMetrics {
            host_memory: 1.0,
            ..PressureMetrics::default()
        };
        assert!(good.validate().is_ok());
    }

    #[test]
    fn registry_aggregates_max() {
        let reg = PressureRegistry::new();
        let p1 = SyntheticPressureProvider::new("p1");
        p1.set(PressureMetrics {
            host_memory: 0.6,
            ..PressureMetrics::default()
        })
        .unwrap();
        let p2 = SyntheticPressureProvider::new("p2");
        p2.set(PressureMetrics {
            network: 0.8,
            ..PressureMetrics::default()
        })
        .unwrap();
        reg.register(Box::new(p1));
        reg.register(Box::new(p2));
        let agg = reg.aggregate();
        assert_eq!(agg.host_memory, 0.6);
        assert_eq!(agg.network, 0.8);
        assert_eq!(reg.level(), PressureLevel::High);
    }

    #[test]
    fn node_reports_are_aggregated_and_removed_on_retirement() {
        let registry = PressureRegistry::new();
        registry
            .report_node(
                "node-a",
                PressureMetrics {
                    host_memory: 0.95,
                    ..PressureMetrics::default()
                },
            )
            .unwrap();
        assert_eq!(registry.try_level().unwrap(), PressureLevel::Critical);

        registry.remove_node("node-a");
        assert_eq!(registry.try_level().unwrap(), PressureLevel::Normal);
        assert!(registry
            .report_node(
                "node-b",
                PressureMetrics {
                    network: f64::NAN,
                    ..PressureMetrics::default()
                },
            )
            .is_err());
    }

    #[test]
    fn empty_registry_normal() {
        let reg = PressureRegistry::new();
        assert_eq!(reg.level(), PressureLevel::Normal);
    }

    #[test]
    fn invalid_synthetic_metrics_return_error_instead_of_panicking() {
        let p = SyntheticPressureProvider::new("invalid");
        assert!(p
            .set(PressureMetrics {
                host_memory: f64::NAN,
                ..PressureMetrics::default()
            })
            .is_err());
        assert_eq!(p.metrics(), PressureMetrics::default());
    }

    #[test]
    fn registering_same_id_replaces_stale_provider() {
        let reg = PressureRegistry::new();
        let stale = reg.register_synthetic("same");
        stale.set_level(PressureLevel::Critical).unwrap();
        assert_eq!(reg.level(), PressureLevel::Critical);

        let replacement = reg.register_synthetic("same");
        assert_eq!(reg.provider_ids(), vec!["same"]);
        assert_eq!(reg.level(), PressureLevel::Normal);
        replacement.set_level(PressureLevel::High).unwrap();
        assert_eq!(reg.level(), PressureLevel::High);
    }

    #[test]
    fn concurrent_synthetic_replacement_keeps_indexes_consistent() {
        for _ in 0..64 {
            let reg = std::sync::Arc::new(PressureRegistry::new());
            let barrier = std::sync::Arc::new(std::sync::Barrier::new(4));
            let threads: Vec<_> = (0..4)
                .map(|_| {
                    let reg = std::sync::Arc::clone(&reg);
                    let barrier = std::sync::Arc::clone(&barrier);
                    std::thread::spawn(move || {
                        barrier.wait();
                        reg.register_synthetic("same");
                    })
                })
                .collect();
            for thread in threads {
                thread.join().unwrap();
            }
            assert_eq!(reg.provider_ids(), vec!["same"]);
            let current = reg.find_synthetic("same").unwrap();
            current.set_level(PressureLevel::Critical).unwrap();
            assert_eq!(reg.level(), PressureLevel::Critical);
        }
    }

    #[test]
    fn generic_replacement_removes_stale_synthetic_handle() {
        struct FixedProvider;
        impl PressureProvider for FixedProvider {
            fn id(&self) -> &str {
                "same"
            }

            fn metrics(&self) -> PressureMetrics {
                PressureMetrics::default()
            }
        }

        let reg = PressureRegistry::new();
        reg.register_synthetic("same");
        reg.register(Box::new(FixedProvider));
        assert!(reg.find_synthetic("same").is_none());
        assert_eq!(reg.provider_ids(), vec!["same"]);
    }

    #[test]
    fn invalid_provider_is_reported_and_fails_closed() {
        struct InvalidProvider;
        impl PressureProvider for InvalidProvider {
            fn id(&self) -> &str {
                "invalid"
            }

            fn metrics(&self) -> PressureMetrics {
                PressureMetrics {
                    network: f64::NAN,
                    ..PressureMetrics::default()
                }
            }
        }

        let reg = PressureRegistry::new();
        reg.register(Box::new(InvalidProvider));
        assert!(reg.try_aggregate().is_err());
        assert_eq!(reg.level(), PressureLevel::Critical);
    }
}
