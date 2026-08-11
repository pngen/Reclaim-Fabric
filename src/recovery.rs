//! Crash recovery: reconciliation of the recovery journal and derived state
//! after restart.
//!
//! Invariant: physical truth and metadata truth must agree. The journal
//! records each reclaim attempt's phase so a crash between a physical
//! operation and its metadata commit can be reconciled on restart:
//!
//! - `RESERVED` / `VALIDATED`: nothing physical happened (or may not have);
//!   roll the object back to its prior state.
//! - `PHYSICAL_STARTED`: ambiguous; ask physical truth for the durable
//!   last-owner plan. An empty plan is safely rolled back.
//! - `PHYSICAL_DONE`: reconcile the durable physical plan; an empty plan is a
//!   completed shared-reference-only release and commits.
//!
//! Fail closed: anything unverifiable is never treated as reclaimed.

use serde_json::Value;
use uuid::Uuid;

use crate::errors::{ReclaimError, Result};
use crate::lifecycle::LifecycleState;
use crate::persistence::{
    AttemptStatus, AuditEntry, JournalEntry, JournalPhase, Reservation, Store,
};

/// Key under which the prior object snapshot is stored in a journal payload.
pub const PRIOR_STATE_KEY: &str = "prior_object";
/// Key under which scheduled replica deletions are described.
pub const DELETION_KEY: &str = "replica_deletions";
/// Key describing the subset of replica payloads that execution determined
/// must actually be deleted from a backend (the last dedup owner).
pub const PHYSICAL_DELETION_KEY: &str = "physical_replica_deletions";
/// Key under which scheduled archive deletions are described.
pub const ARCHIVE_DELETION_KEY: &str = "archive_deletions";

/// Strictly decoded recovery-journal payload.
///
/// All fields are required, including empty deletion arrays. Treating a
/// missing or malformed deletion list as empty can finalize a reclaim while
/// leaving metadata for physical data that is already gone.
#[derive(Debug, Clone)]
pub struct ParsedJournalPayload {
    pub prior_object: crate::object::ReclaimObject,
    pub replica_deletions: Vec<crate::object::Replica>,
    /// `None` before physical execution is atomically planned; `Some`,
    /// including an empty vector, once the exact last-owner set is durable.
    pub physical_replica_deletions: Option<Vec<crate::object::Replica>>,
    pub archive_deletions: Vec<crate::archive::ArchiveRecord>,
}

/// Decode the complete journal payload without defaults or lossy fallbacks.
pub fn parse_journal_payload(payload: &Value) -> Result<ParsedJournalPayload> {
    let object = payload
        .as_object()
        .ok_or_else(|| ReclaimError::Recovery("journal payload must be a JSON object".into()))?;
    let prior_object: crate::object::ReclaimObject = serde_json::from_value(
        object
            .get(PRIOR_STATE_KEY)
            .ok_or_else(|| {
                ReclaimError::Recovery(format!(
                    "journal payload missing required {PRIOR_STATE_KEY:?} field"
                ))
            })?
            .clone(),
    )
    .map_err(|e| ReclaimError::Recovery(format!("invalid {PRIOR_STATE_KEY}: {e}")))?;
    prior_object
        .validate()
        .map_err(|e| ReclaimError::Recovery(format!("invalid {PRIOR_STATE_KEY}: {e}")))?;
    let replica_deletions = serde_json::from_value(
        object
            .get(DELETION_KEY)
            .ok_or_else(|| {
                ReclaimError::Recovery(format!(
                    "journal payload missing required {DELETION_KEY:?} field"
                ))
            })?
            .clone(),
    )
    .map_err(|e| ReclaimError::Recovery(format!("invalid {DELETION_KEY}: {e}")))?;
    let physical_replica_deletions = match object.get(PHYSICAL_DELETION_KEY).ok_or_else(|| {
        ReclaimError::Recovery(format!(
            "journal payload missing required {PHYSICAL_DELETION_KEY:?} field"
        ))
    })? {
        Value::Null => None,
        value => Some(serde_json::from_value(value.clone()).map_err(|e| {
            ReclaimError::Recovery(format!("invalid {PHYSICAL_DELETION_KEY}: {e}"))
        })?),
    };
    let archive_deletions = serde_json::from_value(
        object
            .get(ARCHIVE_DELETION_KEY)
            .ok_or_else(|| {
                ReclaimError::Recovery(format!(
                    "journal payload missing required {ARCHIVE_DELETION_KEY:?} field"
                ))
            })?
            .clone(),
    )
    .map_err(|e| ReclaimError::Recovery(format!("invalid {ARCHIVE_DELETION_KEY}: {e}")))?;
    Ok(ParsedJournalPayload {
        prior_object,
        replica_deletions,
        physical_replica_deletions,
        archive_deletions,
    })
}

/// Decode only the physical-deletion descriptors. Coordinator recovery uses
/// this to inspect the actual backends before metadata reconciliation.
pub fn parse_journal_deletions(
    payload: &Value,
) -> Result<(
    Vec<crate::object::Replica>,
    Vec<crate::object::Replica>,
    Vec<crate::archive::ArchiveRecord>,
)> {
    let parsed = parse_journal_payload(payload)?;
    let physical = parsed.physical_replica_deletions.ok_or_else(|| {
        ReclaimError::Recovery(
            "physical journal phase has no durable physical deletion plan".into(),
        )
    })?;
    Ok((parsed.replica_deletions, physical, parsed.archive_deletions))
}

/// Add the exact, last-owner physical deletion plan to a pre-execution
/// payload. The returned value must be persisted atomically with the
/// transition to `PHYSICAL_STARTED` before metadata ownership is released.
pub fn with_physical_deletions(
    payload: &Value,
    physical_deletions: &[crate::object::Replica],
) -> Result<Value> {
    let parsed = parse_journal_payload(payload)?;
    if parsed.physical_replica_deletions.is_some() {
        return Err(ReclaimError::Recovery(
            "journal physical deletion plan is already set".into(),
        ));
    }
    validate_physical_subset(&parsed.replica_deletions, physical_deletions)?;
    let mut updated = payload
        .as_object()
        .cloned()
        .ok_or_else(|| ReclaimError::Recovery("journal payload must be a JSON object".into()))?;
    updated.insert(
        PHYSICAL_DELETION_KEY.into(),
        serde_json::to_value(physical_deletions).map_err(|e| {
            ReclaimError::Recovery(format!("serializing physical deletion plan: {e}"))
        })?,
    );
    Ok(Value::Object(updated))
}

fn same_replica_descriptor(left: &crate::object::Replica, right: &crate::object::Replica) -> bool {
    left.replica_id == right.replica_id
        && left.object_id == right.object_id
        && left.generation == right.generation
        && left.location == right.location
        && left.size == right.size
        && left.content_hash == right.content_hash
        && left.created_at_ms == right.created_at_ms
        && left.verified_at_ms == right.verified_at_ms
        && left.valid == right.valid
        && left.owner_node == right.owner_node
}

fn validate_physical_subset(
    scheduled: &[crate::object::Replica],
    physical: &[crate::object::Replica],
) -> Result<()> {
    let mut ids = std::collections::HashSet::new();
    for deletion in physical {
        if !ids.insert(deletion.replica_id) {
            return Err(ReclaimError::Recovery(format!(
                "duplicate physical replica deletion {}",
                deletion.replica_id
            )));
        }
        if !scheduled
            .iter()
            .any(|candidate| same_replica_descriptor(candidate, deletion))
        {
            return Err(ReclaimError::Recovery(format!(
                "physical replica deletion {} is not an exact scheduled descriptor",
                deletion.replica_id
            )));
        }
    }
    Ok(())
}

/// Report of a reconciliation run.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RecoveryReport {
    pub reconciled_attempts: u64,
    pub committed: Vec<Uuid>,
    pub rolled_back: Vec<Uuid>,
    pub abandoned_reservations: u64,
    pub dedup_repairs: Vec<String>,
    pub lineage_ok: bool,
    pub errors: Vec<String>,
}

/// Reconcile the store after a restart.
///
/// `physical_state` reports whether the explicitly planned last-owner payloads
/// described in the journal still exist. Implementations must query the real
/// backends (local or remote nodes) — never guess.
pub fn reconcile_store(
    store: &Store,
    now_ms: i64,
    physical_state: &dyn Fn(&Value) -> Result<bool>,
) -> Result<RecoveryReport> {
    let mut report = RecoveryReport {
        reconciled_attempts: 0,
        committed: Vec::new(),
        rolled_back: Vec::new(),
        abandoned_reservations: 0,
        dedup_repairs: Vec::new(),
        lineage_ok: true,
        errors: Vec::new(),
    };

    // --- 1. Reconcile open journal entries ---------------------------------
    let open = store.list_open_journal()?;
    for entry in open {
        report.reconciled_attempts += 1;
        match reconcile_entry(store, now_ms, &entry, physical_state) {
            Ok(ReconcileOutcome::Committed) => report.committed.push(entry.attempt_id),
            Ok(ReconcileOutcome::RolledBack) => report.rolled_back.push(entry.attempt_id),
            Ok(ReconcileOutcome::Failed) => {
                report.errors.push(format!(
                    "attempt {} could not be reconciled safely",
                    entry.attempt_id
                ));
            }
            Err(e) => report
                .errors
                .push(format!("attempt {}: {e}", entry.attempt_id)),
        }
    }

    // --- 2. Expire stale open reservations ---------------------------------
    for res in store.list_open_reservations()? {
        if res.expires_at_ms <= now_ms {
            if store
                .get_journal(&res.attempt_id)?
                .map(|entry| {
                    matches!(
                        entry.phase,
                        JournalPhase::Reserved
                            | JournalPhase::Validated
                            | JournalPhase::PhysicalStarted
                            | JournalPhase::PhysicalDone
                    )
                })
                .unwrap_or(false)
            {
                // Step 1 left this journal open because reconciliation could
                // not be proven safe. Do not overwrite that evidence or mark
                // its object/attempt terminal merely because the lease aged.
                continue;
            }
            let object = store.get_object(&res.object_id)?;
            if let Some(obj) = object {
                if obj.lifecycle_state == LifecycleState::ReclaimPending {
                    // If this attempt still has an open journal entry, it was
                    // reconciled in step 1; otherwise the reservation is
                    // abandoned without journal evidence. Fail closed: mark
                    // the object FAILED for operator repair rather than guess.
                    let mut repaired = obj.clone();
                    repaired.lifecycle_state = LifecycleState::Failed;
                    store.update_object(&repaired)?;
                    store.append_audit(&AuditEntry {
                        id: 0,
                        ts_ms: now_ms,
                        actor: "recovery".into(),
                        action: "RECOVERY_MARK_FAILED".into(),
                        object_id: Some(obj.id),
                        generation: Some(obj.generation),
                        prior_state: Some(LifecycleState::ReclaimPending.as_str().into()),
                        new_state: Some(LifecycleState::Failed.as_str().into()),
                        policy: None,
                        attempt_id: Some(res.attempt_id),
                        node: None,
                        detail: serde_json::json!({
                            "reason": "expired reservation with no reconcileable journal",
                        }),
                    })?;
                }
            }
            store.update_reservation(&res.reservation_id, "EXPIRED")?;
            store.reconcile_attempt(&res.attempt_id, AttemptStatus::Failed, now_ms)?;
            report.abandoned_reservations += 1;
        }
    }

    // --- 3. Recompute and repair dedup reference counts -------------------
    report.dedup_repairs = repair_dedup_counts(store)?;

    // --- 4. Lineage validation ---------------------------------------------
    match store.lineage_graph()?.validate() {
        Ok(()) => report.lineage_ok = true,
        Err(e) => {
            report.lineage_ok = false;
            report.errors.push(format!("lineage validation: {e}"));
        }
    }

    Ok(report)
}

enum ReconcileOutcome {
    Committed,
    RolledBack,
    Failed,
}

fn reconcile_entry(
    store: &Store,
    now_ms: i64,
    entry: &JournalEntry,
    physical_state: &dyn Fn(&Value) -> Result<bool>,
) -> Result<ReconcileOutcome> {
    let mut compatible_payload = entry.payload.clone();
    let is_pre_physical = matches!(
        entry.phase,
        JournalPhase::Reserved | JournalPhase::Validated
    );
    if is_pre_physical {
        // Older open journals predate the explicit physical plan. Their phase
        // proves physical execution never started, so adding the null marker
        // in memory is a safe, narrowly scoped compatibility path.
        if let Some(object) = compatible_payload.as_object_mut() {
            object.entry(PHYSICAL_DELETION_KEY).or_insert(Value::Null);
        }
    }
    let parsed = parse_journal_payload(&compatible_payload).map_err(|e| {
        ReclaimError::Recovery(format!("journal {} corrupt payload: {e}", entry.attempt_id))
    })?;
    validate_payload_identity(entry, &parsed)?;

    match entry.phase {
        JournalPhase::Reserved | JournalPhase::Validated
            if parsed.physical_replica_deletions.is_some() =>
        {
            return Err(ReclaimError::Recovery(format!(
                "journal {} has a physical deletion plan before PHYSICAL_STARTED",
                entry.attempt_id
            )));
        }
        JournalPhase::PhysicalStarted | JournalPhase::PhysicalDone
            if parsed.physical_replica_deletions.is_none() =>
        {
            return Err(ReclaimError::Recovery(format!(
                "journal {} physical phase has no durable physical deletion plan",
                entry.attempt_id
            )));
        }
        _ => {}
    }

    match entry.phase {
        JournalPhase::Committed | JournalPhase::RolledBack | JournalPhase::Failed => {
            // Already finalized; nothing to do.
            Ok(ReconcileOutcome::Failed)
        }
        JournalPhase::Reserved | JournalPhase::Validated => {
            // No physical operation was reported started; roll back.
            rollback(store, now_ms, entry, &parsed)
        }
        JournalPhase::PhysicalStarted | JournalPhase::PhysicalDone => {
            let no_physical_work = parsed
                .physical_replica_deletions
                .as_ref()
                .is_some_and(Vec::is_empty)
                && parsed.archive_deletions.is_empty();
            if no_physical_work {
                // Metadata-only reclaim of shared dedup references has no
                // backend truth to query. PHYSICAL_STARTED may have released
                // any subset of metadata, all of which is safely restorable;
                // PHYSICAL_DONE durably proves the release loop completed.
                return if entry.phase == JournalPhase::PhysicalDone {
                    commit_after_crash(store, now_ms, entry, &parsed)
                } else {
                    rollback(store, now_ms, entry, &parsed)
                };
            }
            // Ask physical truth: does the payload still exist?
            let exists = physical_state(&entry.payload)?;
            if exists {
                // Payload is still there: commit nothing, roll back metadata.
                rollback(store, now_ms, entry, &parsed)
            } else {
                // Payload is gone: the physical reclaim happened; commit the
                // metadata to match reality.
                commit_after_crash(store, now_ms, entry, &parsed)
            }
        }
    }
}

fn validate_payload_identity(entry: &JournalEntry, payload: &ParsedJournalPayload) -> Result<()> {
    if payload.prior_object.id != entry.object_id
        || payload.prior_object.generation != entry.generation
    {
        return Err(ReclaimError::Recovery(format!(
            "journal {} prior object identity {}/{} does not match journal {}/{}",
            entry.attempt_id,
            payload.prior_object.id,
            payload.prior_object.generation,
            entry.object_id,
            entry.generation
        )));
    }

    let mut replica_ids = std::collections::HashSet::new();
    for replica in &payload.replica_deletions {
        if replica.object_id != entry.object_id || replica.generation != entry.generation {
            return Err(ReclaimError::Recovery(format!(
                "journal {} replica {} belongs to {}/{}, expected {}/{}",
                entry.attempt_id,
                replica.replica_id,
                replica.object_id,
                replica.generation,
                entry.object_id,
                entry.generation
            )));
        }
        if !replica_ids.insert(replica.replica_id) {
            return Err(ReclaimError::Recovery(format!(
                "journal {} contains duplicate replica deletion {}",
                entry.attempt_id, replica.replica_id
            )));
        }
        if replica.location.backend.is_empty() || replica.location.key.is_empty() {
            return Err(ReclaimError::Recovery(format!(
                "journal {} replica {} has an empty backend or key",
                entry.attempt_id, replica.replica_id
            )));
        }
    }
    if let Some(physical) = &payload.physical_replica_deletions {
        validate_physical_subset(&payload.replica_deletions, physical).map_err(|e| {
            ReclaimError::Recovery(format!(
                "journal {} has invalid physical deletion plan: {e}",
                entry.attempt_id
            ))
        })?;
    }

    let mut archive_ids = std::collections::HashSet::new();
    for archive in &payload.archive_deletions {
        if archive.object_id != entry.object_id || archive.generation != entry.generation {
            return Err(ReclaimError::Recovery(format!(
                "journal {} archive {} belongs to {}/{}, expected {}/{}",
                entry.attempt_id,
                archive.archive_id,
                archive.object_id,
                archive.generation,
                entry.object_id,
                entry.generation
            )));
        }
        if !archive_ids.insert(archive.archive_id.as_str()) {
            return Err(ReclaimError::Recovery(format!(
                "journal {} contains duplicate archive deletion {}",
                entry.attempt_id, archive.archive_id
            )));
        }
        if archive.archive_id.is_empty() || archive.backend.is_empty() || archive.key.is_empty() {
            return Err(ReclaimError::Recovery(format!(
                "journal {} contains an archive deletion with an empty id, backend, or key",
                entry.attempt_id
            )));
        }
    }
    Ok(())
}

fn rollback(
    store: &Store,
    now_ms: i64,
    entry: &JournalEntry,
    payload: &ParsedJournalPayload,
) -> Result<ReconcileOutcome> {
    let prior_obj = &payload.prior_object;
    let mut audit: Option<AuditEntry> = None;
    if let Some(obj) = store.get_object(&entry.object_id)? {
        if obj.generation != entry.generation {
            return Err(ReclaimError::Recovery(format!(
                "journal {} generation {} cannot roll back current generation {}",
                entry.attempt_id, entry.generation, obj.generation
            )));
        }
        if obj.lifecycle_state != prior_obj.lifecycle_state {
            let restored = prior_obj.clone();
            store.update_object(&restored)?;
            audit = Some(AuditEntry {
                id: 0,
                ts_ms: now_ms,
                actor: "recovery".into(),
                action: "RECOVERY_ROLLBACK".into(),
                object_id: Some(entry.object_id),
                generation: Some(entry.generation),
                prior_state: Some(obj.lifecycle_state.as_str().into()),
                new_state: Some(restored.lifecycle_state.as_str().into()),
                policy: None,
                attempt_id: Some(entry.attempt_id),
                node: None,
                detail: serde_json::json!({"phase": entry.phase.as_str()}),
            });
        }
    } else {
        // Object row vanished mid-transaction; restore it from the journal.
        store.create_object(prior_obj)?;
        audit = Some(AuditEntry {
            id: 0,
            ts_ms: now_ms,
            actor: "recovery".into(),
            action: "RECOVERY_RESTORE_OBJECT".into(),
            object_id: Some(entry.object_id),
            generation: Some(entry.generation),
            prior_state: None,
            new_state: Some(prior_obj.lifecycle_state.as_str().into()),
            policy: None,
            attempt_id: Some(entry.attempt_id),
            node: None,
            detail: serde_json::json!({"phase": entry.phase.as_str()}),
        });
    }

    restore_rollback_metadata(store, entry, payload)?;
    // Recompute rather than incrementing blindly: EXECUTE may have released
    // all, some, or none of these references before the crash.
    repair_dedup_counts(store)?;
    if let Some(audit) = audit {
        store.append_audit(&audit)?;
    }
    store.update_reservation_for_attempt(&entry.attempt_id, "RELEASED")?;
    store.reconcile_attempt(&entry.attempt_id, AttemptStatus::RolledBack, now_ms)?;
    // Finalize the journal last. If recovery crashes before this write, the
    // still-open entry is retried and the preceding operations are idempotent.
    store.update_journal_phase(&entry.attempt_id, JournalPhase::RolledBack, now_ms)?;
    Ok(ReconcileOutcome::RolledBack)
}

fn restore_rollback_metadata(
    store: &Store,
    entry: &JournalEntry,
    payload: &ParsedJournalPayload,
) -> Result<()> {
    let mut replicas_by_id: std::collections::HashMap<_, _> = store
        .all_replicas()?
        .into_iter()
        .map(|replica| (replica.replica_id, replica))
        .collect();
    for expected in &payload.replica_deletions {
        if let Some(actual) = replicas_by_id.get(&expected.replica_id) {
            if actual.object_id != expected.object_id
                || actual.generation != expected.generation
                || actual.location != expected.location
                || actual.size != expected.size
                || actual.content_hash != expected.content_hash
                || actual.valid != expected.valid
            {
                return Err(ReclaimError::Recovery(format!(
                    "journal {} cannot roll back conflicting replica {}",
                    entry.attempt_id, expected.replica_id
                )));
            }
            continue;
        }
        if replicas_by_id.values().any(|actual| {
            actual.location.backend == expected.location.backend
                && actual.location.key == expected.location.key
                && (actual.content_hash != expected.content_hash || actual.size != expected.size)
        }) {
            return Err(ReclaimError::Recovery(format!(
                "journal {} cannot restore replica {}: its physical key is assigned to different content",
                entry.attempt_id, expected.replica_id
            )));
        }
        store.add_replica(expected)?;
        replicas_by_id.insert(expected.replica_id, expected.clone());
    }

    let mut archives_by_id: std::collections::HashMap<_, _> = store
        .list_archives()?
        .into_iter()
        .map(|archive| (archive.archive_id.clone(), archive))
        .collect();
    for expected in &payload.archive_deletions {
        if let Some(actual) = archives_by_id.get(&expected.archive_id) {
            if actual.object_id != expected.object_id
                || actual.generation != expected.generation
                || actual.backend != expected.backend
                || actual.key != expected.key
                || actual.size != expected.size
                || actual.content_hash != expected.content_hash
            {
                return Err(ReclaimError::Recovery(format!(
                    "journal {} cannot roll back conflicting archive {}",
                    entry.attempt_id, expected.archive_id
                )));
            }
            continue;
        }
        if archives_by_id.values().any(|actual| {
            actual.backend == expected.backend
                && actual.key == expected.key
                && (actual.content_hash != expected.content_hash || actual.size != expected.size)
        }) {
            return Err(ReclaimError::Recovery(format!(
                "journal {} cannot restore archive {}: its physical key is assigned to different content",
                entry.attempt_id, expected.archive_id
            )));
        }
        store.insert_archive(expected)?;
        archives_by_id.insert(expected.archive_id.clone(), expected.clone());
    }
    Ok(())
}

fn commit_after_crash(
    store: &Store,
    now_ms: i64,
    entry: &JournalEntry,
    payload: &ParsedJournalPayload,
) -> Result<ReconcileOutcome> {
    // Physical deletion already happened. Commit metadata: mark the object
    // RECLAIMED, drop scheduled replicas and archives, release dedup refs.
    let current = store.get_object(&entry.object_id)?.ok_or_else(|| {
        ReclaimError::Recovery(format!(
            "journal {} cannot commit: object {} is missing",
            entry.attempt_id, entry.object_id
        ))
    })?;
    if current.generation != entry.generation {
        return Err(ReclaimError::Recovery(format!(
            "journal {} generation {} cannot commit current generation {}",
            entry.attempt_id, entry.generation, current.generation
        )));
    }
    let replicas_by_id: std::collections::HashMap<_, _> = store
        .all_replicas()?
        .into_iter()
        .map(|replica| (replica.replica_id, replica))
        .collect();
    for replica in &payload.replica_deletions {
        if let Some(actual) = replicas_by_id.get(&replica.replica_id) {
            if actual.object_id != replica.object_id
                || actual.generation != replica.generation
                || actual.location != replica.location
                || actual.content_hash != replica.content_hash
            {
                return Err(ReclaimError::Recovery(format!(
                    "journal {} replica deletion {} does not match stored metadata",
                    entry.attempt_id, replica.replica_id
                )));
            }
        }
        store.delete_replica(&replica.replica_id)?;
        // Refs may already have been released during EXECUTE. The repair pass
        // below is authoritative, so an absent/already-released entry is not a
        // recovery failure.
        let _ = store.dedup_release(&replica.content_hash, &replica.location.backend);
    }
    let archives_by_id: std::collections::HashMap<_, _> = store
        .list_archives()?
        .into_iter()
        .map(|archive| (archive.archive_id.clone(), archive))
        .collect();
    for archive in &payload.archive_deletions {
        if let Some(actual) = archives_by_id.get(&archive.archive_id) {
            if actual.object_id != archive.object_id
                || actual.generation != archive.generation
                || actual.backend != archive.backend
                || actual.key != archive.key
                || actual.content_hash != archive.content_hash
            {
                return Err(ReclaimError::Recovery(format!(
                    "journal {} archive deletion {} does not match stored metadata",
                    entry.attempt_id, archive.archive_id
                )));
            }
        }
        store.delete_archive(&archive.archive_id)?;
    }
    if current.lifecycle_state != LifecycleState::Reclaimed
        || current.replication_count != 0
        || current.physical_size != 0
        || current.compressed_size.is_some()
    {
        let mut reclaimed = current.clone();
        reclaimed.lifecycle_state = LifecycleState::Reclaimed;
        reclaimed.replication_count = 0;
        reclaimed.physical_size = 0;
        reclaimed.compressed_size = None;
        store.update_object(&reclaimed)?;
        store.append_audit(&AuditEntry {
            id: 0,
            ts_ms: now_ms,
            actor: "recovery".into(),
            action: "RECOVERY_COMMIT".into(),
            object_id: Some(entry.object_id),
            generation: Some(entry.generation),
            prior_state: Some(current.lifecycle_state.as_str().into()),
            new_state: Some(LifecycleState::Reclaimed.as_str().into()),
            policy: None,
            attempt_id: Some(entry.attempt_id),
            node: None,
            detail: serde_json::json!({
                "phase": entry.phase.as_str(),
                "physical_confirmed": "payload gone",
            }),
        })?;
    }
    store.update_reservation_for_attempt(&entry.attempt_id, "COMMITTED")?;
    store.reconcile_attempt(&entry.attempt_id, AttemptStatus::Committed, now_ms)?;
    store.update_journal_phase(&entry.attempt_id, JournalPhase::Committed, now_ms)?;
    Ok(ReconcileOutcome::Committed)
}

/// Recompute dedup reference counts from live replicas and repair drift.
///
/// Safety: only counts are repaired. Physical payloads are never created or
/// destroyed during repair.
pub fn repair_dedup_counts(store: &Store) -> Result<Vec<String>> {
    let mut repairs = Vec::new();
    // Count replicas of non-RECLAIMED objects only; RECLAIMED objects must not
    // hold live references. Keyed by (content hash, backend): one physical
    // payload per backend per content identity.
    #[derive(Debug)]
    struct LiveEntry {
        ref_count: u64,
        key: String,
        payload_size: u64,
    }

    let objects: std::collections::HashMap<_, _> = store
        .list_objects()?
        .into_iter()
        .map(|object| (object.id, object))
        .collect();
    let mut live: std::collections::BTreeMap<(crate::integrity::ContentHash, String), LiveEntry> =
        std::collections::BTreeMap::new();
    for replica in store.all_replicas()? {
        let object = objects.get(&replica.object_id).ok_or_else(|| {
            ReclaimError::Recovery(format!(
                "orphan replica {} references missing object {}",
                replica.replica_id, replica.object_id
            ))
        })?;
        if object.lifecycle_state == LifecycleState::Reclaimed {
            return Err(ReclaimError::Recovery(format!(
                "reclaimed object {} still owns replica {}; refusing to drop its dedup accounting while physical metadata remains",
                object.id, replica.replica_id
            )));
        }
        let identity = (replica.content_hash, replica.location.backend.clone());
        match live.entry(identity) {
            std::collections::btree_map::Entry::Vacant(slot) => {
                slot.insert(LiveEntry {
                    ref_count: 1,
                    key: replica.location.key,
                    payload_size: replica.size,
                });
            }
            std::collections::btree_map::Entry::Occupied(mut slot) => {
                let existing = slot.get_mut();
                if existing.key != replica.location.key || existing.payload_size != replica.size {
                    return Err(ReclaimError::Recovery(format!(
                        "live replicas for {} on {} disagree on canonical key or payload size",
                        slot.key().0,
                        slot.key().1
                    )));
                }
                existing.ref_count = existing.ref_count.checked_add(1).ok_or_else(|| {
                    ReclaimError::Recovery("dedup live reference count overflow".into())
                })?;
            }
        }
    }

    for entry in store.list_dedup()? {
        let identity = (entry.content_hash, entry.backend.clone());
        match live.remove(&identity) {
            None => {
                store.delete_dedup(&entry.content_hash, &entry.backend)?;
                repairs.push(format!(
                    "removed unreferenced dedup entry for {} on {}",
                    entry.content_hash, entry.backend
                ));
            }
            Some(expected) => {
                if entry.ref_count != expected.ref_count
                    || entry.key != expected.key
                    || entry.payload_size != expected.payload_size
                {
                    store.upsert_dedup(&crate::dedup::DedupEntry {
                        content_hash: entry.content_hash,
                        backend: entry.backend.clone(),
                        key: expected.key,
                        ref_count: expected.ref_count,
                        payload_size: expected.payload_size,
                    })?;
                    repairs.push(format!(
                        "repaired dedup entry for {} on {} (refs {} -> {})",
                        entry.content_hash, entry.backend, entry.ref_count, expected.ref_count
                    ));
                }
            }
        }
    }

    // Existing code only iterated stored entries, so a completely missing row
    // was never recreated. Rebuild every remaining live identity from its
    // consistent replica metadata.
    for ((content_hash, backend), expected) in live {
        store.insert_dedup(&crate::dedup::DedupEntry {
            content_hash,
            backend: backend.clone(),
            key: expected.key,
            ref_count: expected.ref_count,
            payload_size: expected.payload_size,
        })?;
        repairs.push(format!(
            "recreated missing dedup entry for {content_hash} on {backend}"
        ));
    }
    Ok(repairs)
}

/// Cleanup helper: remove a reservation row entirely (used after successful
/// commit in the normal path; the recovery path keeps rows for audit).
pub fn close_reservation(store: &Store, reservation: &Reservation, status: &str) -> Result<()> {
    store.update_reservation(&reservation.reservation_id, status)
}

/// Serialize a prior object snapshot + deletion lists into a journal payload.
pub fn journal_payload(
    prior: &crate::object::ReclaimObject,
    deletions: &[crate::object::Replica],
    archive_deletions: &[crate::archive::ArchiveRecord],
) -> Result<Value> {
    let mut payload = serde_json::Map::new();
    payload.insert(
        PRIOR_STATE_KEY.into(),
        serde_json::to_value(prior).map_err(|e| {
            ReclaimError::Recovery(format!("serializing journal prior object: {e}"))
        })?,
    );
    payload.insert(
        DELETION_KEY.into(),
        serde_json::to_value(deletions).map_err(|e| {
            ReclaimError::Recovery(format!("serializing journal replica deletions: {e}"))
        })?,
    );
    // This marker is deliberately present from journal creation. `null`
    // means physical execution has not been planned. The exact last-owner
    // subset replaces it in the same database statement that advances the
    // journal to PHYSICAL_STARTED.
    payload.insert(PHYSICAL_DELETION_KEY.into(), Value::Null);
    payload.insert(
        ARCHIVE_DELETION_KEY.into(),
        serde_json::to_value(archive_deletions).map_err(|e| {
            ReclaimError::Recovery(format!("serializing journal archive deletions: {e}"))
        })?,
    );
    Ok(Value::Object(payload))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object::{ReclaimObject, Replica};

    fn mk_obj(now: i64) -> ReclaimObject {
        let mut o = ReclaimObject::new(Uuid::new_v4(), 0, "checkpoint", 100, now);
        o.lifecycle_state = LifecycleState::Hot;
        o
    }

    fn mk_replica(oid: Uuid) -> Replica {
        Replica {
            replica_id: Uuid::new_v4(),
            object_id: oid,
            generation: 0,
            location: crate::object::PhysicalLocation {
                backend: "memory".into(),
                key: "k".into(),
                kind: crate::object::PhysicalKind::Hot,
            },
            size: 100,
            content_hash: crate::integrity::ContentHash::of(b"x"),
            created_at_ms: 0,
            verified_at_ms: None,
            valid: true,
            owner_node: None,
        }
    }

    fn insert_test_journal(store: &Store, entry: JournalEntry) -> Result<()> {
        store.create_attempt(&crate::persistence::Attempt {
            attempt_id: entry.attempt_id,
            object_id: entry.object_id,
            generation: entry.generation,
            node: "test".into(),
            created_at_ms: entry.created_at_ms,
            updated_at_ms: entry.updated_at_ms,
            status: AttemptStatus::Open,
        })?;
        store.insert_journal(&entry)
    }

    #[test]
    fn reserved_journal_rolls_back() {
        let store = Store::open_in_memory().unwrap();
        let now = 1000;
        let obj = mk_obj(now);
        store.create_object(&obj).unwrap();
        let attempt = Uuid::new_v4();
        insert_test_journal(
            &store,
            JournalEntry {
                attempt_id: attempt,
                object_id: obj.id,
                generation: 0,
                phase: JournalPhase::Reserved,
                created_at_ms: now,
                updated_at_ms: now,
                payload: journal_payload(&obj, &[], &[]).unwrap(),
            },
        )
        .unwrap();
        // Simulate crash: object stuck in RECLAIM_PENDING.
        let mut pending = obj.clone();
        pending.lifecycle_state = LifecycleState::ReclaimPending;
        store.update_object(&pending).unwrap();

        let always_exists = |_: &Value| Ok(true);
        let report = reconcile_store(&store, now + 1, &always_exists).unwrap();
        assert_eq!(report.rolled_back.len(), 1);
        let after = store.require_object(&obj.id).unwrap();
        assert_eq!(after.lifecycle_state, LifecycleState::Hot);
    }

    #[test]
    fn physical_done_commits_when_payload_gone() {
        let store = Store::open_in_memory().unwrap();
        let now = 1000;
        let mut obj = mk_obj(now);
        obj.replication_count = 1;
        obj.physical_size = 100;
        obj.compressed_size = Some(50);
        store.create_object(&obj).unwrap();
        let replica = mk_replica(obj.id);
        store.add_replica(&replica).unwrap();
        let attempt = Uuid::new_v4();
        insert_test_journal(
            &store,
            JournalEntry {
                attempt_id: attempt,
                object_id: obj.id,
                generation: 0,
                phase: JournalPhase::PhysicalDone,
                created_at_ms: now,
                updated_at_ms: now,
                payload: with_physical_deletions(
                    &journal_payload(&obj, std::slice::from_ref(&replica), &[]).unwrap(),
                    std::slice::from_ref(&replica),
                )
                .unwrap(),
            },
        )
        .unwrap();
        let mut pending = obj.clone();
        pending.lifecycle_state = LifecycleState::ReclaimPending;
        store.update_object(&pending).unwrap();

        let gone = |_: &Value| Ok(false);
        let report = reconcile_store(&store, now + 1, &gone).unwrap();
        assert_eq!(report.committed.len(), 1);
        let after = store.require_object(&obj.id).unwrap();
        assert_eq!(after.lifecycle_state, LifecycleState::Reclaimed);
        assert_eq!(after.replication_count, 0);
        assert_eq!(after.physical_size, 0);
        assert_eq!(after.compressed_size, None);
        assert_eq!(store.replica_count(&obj.id).unwrap(), 0);
    }

    #[test]
    fn physical_started_with_payload_present_rolls_back() {
        let store = Store::open_in_memory().unwrap();
        let now = 1000;
        let obj = mk_obj(now);
        store.create_object(&obj).unwrap();
        let attempt = Uuid::new_v4();
        insert_test_journal(
            &store,
            JournalEntry {
                attempt_id: attempt,
                object_id: obj.id,
                generation: 0,
                phase: JournalPhase::PhysicalStarted,
                created_at_ms: now,
                updated_at_ms: now,
                payload: with_physical_deletions(&journal_payload(&obj, &[], &[]).unwrap(), &[])
                    .unwrap(),
            },
        )
        .unwrap();
        let mut pending = obj.clone();
        pending.lifecycle_state = LifecycleState::ReclaimPending;
        store.update_object(&pending).unwrap();

        let exists = |_: &Value| Ok(true);
        let report = reconcile_store(&store, now + 1, &exists).unwrap();
        assert_eq!(report.rolled_back.len(), 1);
        assert_eq!(
            store.require_object(&obj.id).unwrap().lifecycle_state,
            LifecycleState::Hot
        );
    }

    #[test]
    fn dedup_counts_repaired() {
        let store = Store::open_in_memory().unwrap();
        let now = 1000;
        let obj = mk_obj(now);
        let mut obj2 = mk_obj(now);
        obj2.content_hash = obj.content_hash;
        let hash = crate::integrity::ContentHash::of(b"x");
        store.create_object(&obj).unwrap();
        store.create_object(&obj2).unwrap();
        let r1 = mk_replica(obj.id);
        let mut r2 = mk_replica(obj2.id);
        r2.content_hash = hash;
        store.add_replica(&r1).unwrap();
        store.add_replica(&r2).unwrap();
        // Drift: stored ref count is 5, live is 2.
        store
            .insert_dedup(&crate::dedup::DedupEntry {
                content_hash: hash,
                backend: "memory".into(),
                key: "k".into(),
                ref_count: 5,
                payload_size: 100,
            })
            .unwrap();
        let repairs = repair_dedup_counts(&store).unwrap();
        assert_eq!(repairs.len(), 1);
        assert_eq!(
            store.get_dedup(&hash, "memory").unwrap().unwrap().ref_count,
            2
        );
    }

    #[test]
    fn expired_reservation_marks_failed() {
        let store = Store::open_in_memory().unwrap();
        let now = 1000;
        let obj = mk_obj(now);
        store.create_object(&obj).unwrap();
        let attempt = Uuid::new_v4();
        let reservation = Uuid::new_v4();
        store
            .create_attempt(&crate::persistence::Attempt {
                attempt_id: attempt,
                object_id: obj.id,
                generation: obj.generation,
                node: "n1".into(),
                created_at_ms: now,
                updated_at_ms: now,
                status: AttemptStatus::Open,
            })
            .unwrap();
        store
            .create_reservation(&Reservation {
                reservation_id: reservation,
                attempt_id: attempt,
                object_id: obj.id,
                generation: 0,
                node: "n1".into(),
                created_at_ms: now,
                expires_at_ms: now + 100,
                status: "OPEN".into(),
            })
            .unwrap();
        let mut pending = obj.clone();
        pending.lifecycle_state = LifecycleState::ReclaimPending;
        store.update_object(&pending).unwrap();
        let always_exists = |_: &Value| Ok(true);
        let report = reconcile_store(&store, now + 10_000, &always_exists).unwrap();
        assert_eq!(report.abandoned_reservations, 1);
        // Object is RECLAIM_PENDING with no journal -> marked FAILED (fail closed).
        let after = store.require_object(&obj.id).unwrap();
        assert_eq!(after.lifecycle_state, LifecycleState::Failed);
    }

    #[test]
    fn malformed_journal_deletion_list_fails_closed() {
        let store = Store::open_in_memory().unwrap();
        let now = 1000;
        let obj = mk_obj(now);
        store.create_object(&obj).unwrap();
        let attempt = Uuid::new_v4();
        let mut payload =
            with_physical_deletions(&journal_payload(&obj, &[], &[]).unwrap(), &[]).unwrap();
        payload[DELETION_KEY] = serde_json::json!("not an array");
        insert_test_journal(
            &store,
            JournalEntry {
                attempt_id: attempt,
                object_id: obj.id,
                generation: obj.generation,
                phase: JournalPhase::PhysicalDone,
                created_at_ms: now,
                updated_at_ms: now,
                payload,
            },
        )
        .unwrap();

        let report = reconcile_store(&store, now + 1, &|_| Ok(false)).unwrap();
        assert_eq!(report.errors.len(), 1);
        assert!(report.committed.is_empty());
        assert_eq!(
            store.get_journal(&attempt).unwrap().unwrap().phase,
            JournalPhase::PhysicalDone
        );
        assert_ne!(
            store.require_object(&obj.id).unwrap().lifecycle_state,
            LifecycleState::Reclaimed
        );
    }

    #[test]
    fn journal_prior_identity_mismatch_cannot_mutate_another_object() {
        let store = Store::open_in_memory().unwrap();
        let now = 1000;
        let target = mk_obj(now);
        let other = mk_obj(now);
        store.create_object(&target).unwrap();
        store.create_object(&other).unwrap();
        let attempt = Uuid::new_v4();
        insert_test_journal(
            &store,
            JournalEntry {
                attempt_id: attempt,
                object_id: target.id,
                generation: target.generation,
                phase: JournalPhase::Reserved,
                created_at_ms: now,
                updated_at_ms: now,
                payload: journal_payload(&other, &[], &[]).unwrap(),
            },
        )
        .unwrap();

        let report = reconcile_store(&store, now + 1, &|_| Ok(true)).unwrap();
        assert_eq!(report.errors.len(), 1);
        assert!(report.rolled_back.is_empty());
        assert_eq!(store.require_object(&target.id).unwrap().id, target.id);
        assert_eq!(store.require_object(&other.id).unwrap().id, other.id);
        assert_eq!(
            store.get_journal(&attempt).unwrap().unwrap().phase,
            JournalPhase::Reserved
        );
    }

    #[test]
    fn forged_replica_descriptor_cannot_delete_unrelated_metadata() {
        let store = Store::open_in_memory().unwrap();
        let now = 1000;
        let target = mk_obj(now);
        let other = mk_obj(now);
        store.create_object(&target).unwrap();
        store.create_object(&other).unwrap();
        let actual = mk_replica(other.id);
        store.add_replica(&actual).unwrap();
        let mut forged = actual.clone();
        forged.object_id = target.id;
        let attempt = Uuid::new_v4();
        insert_test_journal(
            &store,
            JournalEntry {
                attempt_id: attempt,
                object_id: target.id,
                generation: target.generation,
                phase: JournalPhase::PhysicalDone,
                created_at_ms: now,
                updated_at_ms: now,
                payload: with_physical_deletions(
                    &journal_payload(&target, std::slice::from_ref(&forged), &[]).unwrap(),
                    std::slice::from_ref(&forged),
                )
                .unwrap(),
            },
        )
        .unwrap();

        let report = reconcile_store(&store, now + 1, &|_| Ok(false)).unwrap();
        assert_eq!(report.errors.len(), 1);
        assert_eq!(store.replicas_for(&other.id).unwrap().len(), 1);
        assert_eq!(
            store.get_journal(&attempt).unwrap().unwrap().phase,
            JournalPhase::PhysicalDone
        );
    }

    #[test]
    fn missing_and_stale_dedup_rows_are_reconciled() {
        let store = Store::open_in_memory().unwrap();
        let now = 1000;
        let first = mk_obj(now);
        let second = mk_obj(now);
        store.create_object(&first).unwrap();
        store.create_object(&second).unwrap();
        let r1 = mk_replica(first.id);
        let mut r2 = mk_replica(second.id);
        r2.location.key = r1.location.key.clone();
        store.add_replica(&r1).unwrap();
        store.add_replica(&r2).unwrap();
        let stale_hash = crate::integrity::ContentHash::of(b"stale");
        store
            .insert_dedup(&crate::dedup::DedupEntry {
                content_hash: stale_hash,
                backend: "memory".into(),
                key: "stale".into(),
                ref_count: 1,
                payload_size: 5,
            })
            .unwrap();

        let repairs = repair_dedup_counts(&store).unwrap();
        assert_eq!(repairs.len(), 2);
        assert!(store.get_dedup(&stale_hash, "memory").unwrap().is_none());
        assert_eq!(
            store
                .get_dedup(&r1.content_hash, "memory")
                .unwrap()
                .unwrap()
                .ref_count,
            2
        );
    }

    #[test]
    fn rollback_restores_intermediate_recomputable_state() {
        let store = Store::open_in_memory().unwrap();
        let now = 1000;
        let obj = mk_obj(now);
        store.create_object(&obj).unwrap();
        let attempt = Uuid::new_v4();
        insert_test_journal(
            &store,
            JournalEntry {
                attempt_id: attempt,
                object_id: obj.id,
                generation: obj.generation,
                phase: JournalPhase::Reserved,
                created_at_ms: now,
                updated_at_ms: now,
                payload: journal_payload(&obj, &[], &[]).unwrap(),
            },
        )
        .unwrap();
        let mut intermediate = obj.clone();
        intermediate.lifecycle_state = LifecycleState::Recomputable;
        store.update_object(&intermediate).unwrap();

        let report = reconcile_store(&store, now + 1, &|_| Ok(true)).unwrap();
        assert!(report.errors.is_empty());
        assert_eq!(report.rolled_back, vec![attempt]);
        assert_eq!(
            store.require_object(&obj.id).unwrap().lifecycle_state,
            LifecycleState::Hot
        );
    }

    #[test]
    fn unresolved_journal_is_not_overridden_by_reservation_expiry() {
        let store = Store::open_in_memory().unwrap();
        let now = 1000;
        let obj = mk_obj(now);
        store.create_object(&obj).unwrap();
        let replica = mk_replica(obj.id);
        store.add_replica(&replica).unwrap();
        let attempt = Uuid::new_v4();
        let reservation = Uuid::new_v4();
        insert_test_journal(
            &store,
            JournalEntry {
                attempt_id: attempt,
                object_id: obj.id,
                generation: obj.generation,
                phase: JournalPhase::PhysicalStarted,
                created_at_ms: now,
                updated_at_ms: now,
                payload: with_physical_deletions(
                    &journal_payload(&obj, std::slice::from_ref(&replica), &[]).unwrap(),
                    std::slice::from_ref(&replica),
                )
                .unwrap(),
            },
        )
        .unwrap();
        store
            .create_reservation(&Reservation {
                reservation_id: reservation,
                attempt_id: attempt,
                object_id: obj.id,
                generation: obj.generation,
                node: "n1".into(),
                created_at_ms: now,
                expires_at_ms: now + 1,
                status: "OPEN".into(),
            })
            .unwrap();
        let mut pending = obj.clone();
        pending.lifecycle_state = LifecycleState::ReclaimPending;
        store.update_object(&pending).unwrap();

        let report = reconcile_store(&store, now + 10, &|_| {
            Err(ReclaimError::Recovery("backend unavailable".into()))
        })
        .unwrap();
        assert_eq!(report.errors.len(), 1);
        assert_eq!(report.abandoned_reservations, 0);
        assert_eq!(store.list_open_reservations().unwrap().len(), 1);
        assert_eq!(
            store.require_object(&obj.id).unwrap().lifecycle_state,
            LifecycleState::ReclaimPending
        );
        assert_eq!(
            store.get_journal(&attempt).unwrap().unwrap().phase,
            JournalPhase::PhysicalStarted
        );
    }

    #[test]
    fn physical_done_empty_plan_commits_shared_metadata_only_reclaim() {
        let store = Store::open_in_memory().unwrap();
        let now = 1000;
        let target = mk_obj(now);
        let survivor = mk_obj(now);
        store.create_object(&target).unwrap();
        store.create_object(&survivor).unwrap();
        let target_replica = mk_replica(target.id);
        let mut survivor_replica = mk_replica(survivor.id);
        survivor_replica.location = target_replica.location.clone();
        survivor_replica.content_hash = target_replica.content_hash;
        store.add_replica(&target_replica).unwrap();
        store.add_replica(&survivor_replica).unwrap();
        store
            .insert_dedup(&crate::dedup::DedupEntry {
                content_hash: target_replica.content_hash,
                backend: target_replica.location.backend.clone(),
                key: target_replica.location.key.clone(),
                ref_count: 2,
                payload_size: target_replica.size,
            })
            .unwrap();
        let attempt = Uuid::new_v4();
        insert_test_journal(
            &store,
            JournalEntry {
                attempt_id: attempt,
                object_id: target.id,
                generation: target.generation,
                phase: JournalPhase::PhysicalDone,
                created_at_ms: now,
                updated_at_ms: now,
                payload: with_physical_deletions(
                    &journal_payload(&target, std::slice::from_ref(&target_replica), &[]).unwrap(),
                    &[],
                )
                .unwrap(),
            },
        )
        .unwrap();
        let mut pending = target.clone();
        pending.lifecycle_state = LifecycleState::ReclaimPending;
        store.update_object(&pending).unwrap();
        // Simulate the completed metadata-only execution. The shared backend
        // bytes remain owned by `survivor` and must not influence recovery.
        store.delete_replica(&target_replica.replica_id).unwrap();
        assert!(!store
            .dedup_release(
                &target_replica.content_hash,
                &target_replica.location.backend,
            )
            .unwrap());

        let report = reconcile_store(&store, now + 1, &|_| {
            Err(ReclaimError::Recovery(
                "physical callback must not run for an empty plan".into(),
            ))
        })
        .unwrap();
        assert!(report.errors.is_empty());
        assert_eq!(report.committed, vec![attempt]);
        assert_eq!(
            store.require_object(&target.id).unwrap().lifecycle_state,
            LifecycleState::Reclaimed
        );
        assert_eq!(store.replicas_for(&survivor.id).unwrap().len(), 1);
        assert_eq!(
            store
                .get_dedup(
                    &target_replica.content_hash,
                    &target_replica.location.backend,
                )
                .unwrap()
                .unwrap()
                .ref_count,
            1
        );
    }

    #[test]
    fn rollback_restores_replica_and_dedup_after_pre_delete_metadata_crash() {
        let store = Store::open_in_memory().unwrap();
        let now = 1000;
        let obj = mk_obj(now);
        store.create_object(&obj).unwrap();
        let replica = mk_replica(obj.id);
        store.add_replica(&replica).unwrap();
        store
            .insert_dedup(&crate::dedup::DedupEntry {
                content_hash: replica.content_hash,
                backend: replica.location.backend.clone(),
                key: replica.location.key.clone(),
                ref_count: 1,
                payload_size: replica.size,
            })
            .unwrap();
        let attempt = Uuid::new_v4();
        insert_test_journal(
            &store,
            JournalEntry {
                attempt_id: attempt,
                object_id: obj.id,
                generation: obj.generation,
                phase: JournalPhase::PhysicalStarted,
                created_at_ms: now,
                updated_at_ms: now,
                payload: with_physical_deletions(
                    &journal_payload(&obj, std::slice::from_ref(&replica), &[]).unwrap(),
                    std::slice::from_ref(&replica),
                )
                .unwrap(),
            },
        )
        .unwrap();
        let mut pending = obj.clone();
        pending.lifecycle_state = LifecycleState::ReclaimPending;
        store.update_object(&pending).unwrap();

        // Exact crash point: EXECUTE removed metadata ownership, but the
        // backend deletion has not happened and physical bytes still exist.
        store.delete_replica(&replica.replica_id).unwrap();
        assert!(store
            .dedup_release(&replica.content_hash, &replica.location.backend)
            .unwrap());

        let report = reconcile_store(&store, now + 1, &|_| Ok(true)).unwrap();
        assert!(report.errors.is_empty());
        assert_eq!(report.rolled_back, vec![attempt]);
        assert_eq!(
            store.require_object(&obj.id).unwrap().lifecycle_state,
            LifecycleState::Hot
        );
        assert_eq!(store.replicas_for(&obj.id).unwrap().len(), 1);
        let dedup = store
            .get_dedup(&replica.content_hash, &replica.location.backend)
            .unwrap()
            .unwrap();
        assert_eq!(dedup.ref_count, 1);
        assert_eq!(dedup.key, replica.location.key);

        // Terminalized rollback is stable across another restart pass.
        let second = reconcile_store(&store, now + 2, &|_| Ok(true)).unwrap();
        assert_eq!(second.reconciled_attempts, 0);
        assert!(second.dedup_repairs.is_empty());
    }
}
