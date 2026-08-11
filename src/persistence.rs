//! Durable local state store (SQLite).
//!
//! The store is the single authority for all authoritative metadata:
//! objects, generations, lifecycle state, lineage, dependencies, replicas,
//! dedup references, decisions, policy versions, reservations, attempts,
//! coordinator epoch, archive records, failure records, audit trail, and the
//! recovery journal.
//!
//! Schema versioning is explicit (`PRAGMA user_version`). Incompatible state
//! is never silently discarded: opening a store with a newer schema fails
//! loudly.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use rusqlite::{Connection, OptionalExtension, TransactionBehavior};
use uuid::Uuid;

use crate::archive::ArchiveRecord;
use crate::dedup::DedupEntry;
use crate::errors::{ReclaimError, Result};
use crate::integrity::ContentHash;
use crate::lifecycle::LifecycleState;
use crate::lineage::{EdgeKind, LineageGraph};
use crate::object::{DurabilityClass, ReclaimObject, Replica, SurvivabilityClass};
use crate::policy::Policy;

pub const SCHEMA_VERSION: i64 = 1;
const MAX_QUERY_LIMIT: u64 = 100_000;

/// Phases of the recovery journal for a reclaim attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum JournalPhase {
    Reserved,
    Validated,
    PhysicalStarted,
    PhysicalDone,
    Committed,
    RolledBack,
    Failed,
}

impl JournalPhase {
    pub fn as_str(&self) -> &'static str {
        match self {
            JournalPhase::Reserved => "RESERVED",
            JournalPhase::Validated => "VALIDATED",
            JournalPhase::PhysicalStarted => "PHYSICAL_STARTED",
            JournalPhase::PhysicalDone => "PHYSICAL_DONE",
            JournalPhase::Committed => "COMMITTED",
            JournalPhase::RolledBack => "ROLLED_BACK",
            JournalPhase::Failed => "FAILED",
        }
    }
}

/// One recovery journal entry.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct JournalEntry {
    pub attempt_id: Uuid,
    pub object_id: Uuid,
    pub generation: u64,
    pub phase: JournalPhase,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    /// JSON payload: prior object state + physical operation descriptor.
    pub payload: serde_json::Value,
}

/// Attempt status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AttemptStatus {
    Open,
    Committed,
    Failed,
    RolledBack,
}

impl AttemptStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            AttemptStatus::Open => "OPEN",
            AttemptStatus::Committed => "COMMITTED",
            AttemptStatus::Failed => "FAILED",
            AttemptStatus::RolledBack => "ROLLED_BACK",
        }
    }
}

/// A reclamation attempt (transaction-like lifecycle).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Attempt {
    pub attempt_id: Uuid,
    pub object_id: Uuid,
    pub generation: u64,
    pub node: String,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub status: AttemptStatus,
}

/// A reclamation reservation granting exclusive reclamation authority.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Reservation {
    pub reservation_id: Uuid,
    pub attempt_id: Uuid,
    pub object_id: Uuid,
    pub generation: u64,
    pub node: String,
    pub created_at_ms: i64,
    pub expires_at_ms: i64,
    pub status: String, // OPEN | COMMITTED | RELEASED | EXPIRED
}

/// Audit entry (append-only).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AuditEntry {
    pub id: i64,
    pub ts_ms: i64,
    pub actor: String,
    pub action: String,
    pub object_id: Option<Uuid>,
    pub generation: Option<u64>,
    pub prior_state: Option<String>,
    pub new_state: Option<String>,
    pub policy: Option<String>,
    pub attempt_id: Option<Uuid>,
    pub node: Option<String>,
    pub detail: serde_json::Value,
}

/// Failure record.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FailureRecord {
    pub id: i64,
    pub ts_ms: i64,
    pub object_id: Option<Uuid>,
    pub attempt_id: Option<Uuid>,
    pub kind: String,
    pub message: String,
    pub recovered: bool,
}

/// Coordinator authority row.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CoordinatorState {
    pub epoch: u64,
    pub process_id: String,
    pub boot_id: Uuid,
    pub last_seen_ms: i64,
}

/// The store. All methods are internally synchronized; callers never hold
/// locks across blocking I/O. Cloneable: the connection is shared, not copied.
#[derive(Clone, Debug)]
pub struct Store {
    conn: Arc<Mutex<Connection>>,
}

fn map_sqlite(e: rusqlite::Error) -> ReclaimError {
    ReclaimError::Persistence(e.to_string())
}

fn sql_u64(field: &str, value: u64) -> Result<i64> {
    i64::try_from(value).map_err(|_| {
        ReclaimError::InvalidArgument(format!(
            "{field} value {value} exceeds SQLite INTEGER range"
        ))
    })
}

fn sql_optional_u64(field: &str, value: Option<u64>) -> Result<Option<i64>> {
    value.map(|v| sql_u64(field, v)).transpose()
}

fn decode_u64(value: i64, field: &str) -> std::result::Result<u64, rusqlite::Error> {
    u64::try_from(value).map_err(|_| {
        rusqlite::Error::InvalidParameterName(format!(
            "corrupt {field}: expected non-negative INTEGER, got {value}"
        ))
    })
}

fn decode_optional_u64(
    value: Option<i64>,
    field: &str,
) -> std::result::Result<Option<u64>, rusqlite::Error> {
    value.map(|v| decode_u64(v, field)).transpose()
}

fn decode_u32(value: i64, field: &str) -> std::result::Result<u32, rusqlite::Error> {
    u32::try_from(value).map_err(|_| {
        rusqlite::Error::InvalidParameterName(format!(
            "corrupt {field}: expected an unsigned 32-bit INTEGER, got {value}"
        ))
    })
}

fn decode_bool(value: i64, field: &str) -> std::result::Result<bool, rusqlite::Error> {
    match value {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(rusqlite::Error::InvalidParameterName(format!(
            "corrupt {field}: expected 0 or 1, got {value}"
        ))),
    }
}

fn decode_uuid(value: &str, field: &str) -> std::result::Result<Uuid, rusqlite::Error> {
    let parsed = Uuid::parse_str(value)
        .map_err(|e| rusqlite::Error::InvalidParameterName(format!("corrupt {field}: {e}")))?;
    if parsed.to_string() != value {
        return Err(rusqlite::Error::InvalidParameterName(format!(
            "corrupt {field}: UUID is not in canonical form"
        )));
    }
    Ok(parsed)
}

fn decode_optional_uuid(
    value: Option<String>,
    field: &str,
) -> std::result::Result<Option<Uuid>, rusqlite::Error> {
    value.as_deref().map(|v| decode_uuid(v, field)).transpose()
}

fn decode_hash(value: &str, field: &str) -> std::result::Result<ContentHash, rusqlite::Error> {
    let parsed = ContentHash::from_hex(value)
        .map_err(|e| rusqlite::Error::InvalidParameterName(format!("corrupt {field}: {e}")))?;
    if parsed.to_string() != value {
        return Err(rusqlite::Error::InvalidParameterName(format!(
            "corrupt {field}: content hash is not canonical lowercase hex"
        )));
    }
    Ok(parsed)
}

fn sql_limit(limit: u64) -> Result<i64> {
    if limit > MAX_QUERY_LIMIT {
        return Err(ReclaimError::InvalidArgument(format!(
            "query limit {limit} exceeds maximum {MAX_QUERY_LIMIT}"
        )));
    }
    sql_u64("query limit", limit)
}

fn validate_dedup_entry(entry: &DedupEntry) -> Result<()> {
    if entry.backend.is_empty() || entry.key.is_empty() {
        return Err(ReclaimError::InvalidArgument(
            "dedup backend and storage key must not be empty".into(),
        ));
    }
    if entry.ref_count == 0 {
        return Err(ReclaimError::Dedup(
            "dedup entries must have at least one reference".into(),
        ));
    }
    sql_u64("dedup ref_count", entry.ref_count)?;
    sql_u64("dedup payload_size", entry.payload_size)?;
    Ok(())
}

fn validate_replica_descriptor(replica: &Replica) -> Result<()> {
    if replica.location.backend.trim().is_empty() || replica.location.key.trim().is_empty() {
        return Err(ReclaimError::InvalidArgument(
            "replica backend and key must not be empty".into(),
        ));
    }
    if replica
        .owner_node
        .as_deref()
        .is_some_and(|owner| owner.trim().is_empty())
    {
        return Err(ReclaimError::InvalidArgument(
            "replica owner_node must not be empty when present".into(),
        ));
    }
    sql_u64("replica generation", replica.generation)?;
    sql_u64("replica size", replica.size)?;
    Ok(())
}

fn validate_replica_parent(conn: &Connection, replica: &Replica) -> Result<()> {
    let parent: Option<(i64, String)> = conn
        .query_row(
            "SELECT generation, lifecycle_state FROM objects WHERE id=?1",
            [replica.object_id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(map_sqlite)?;
    let (generation, state) = parent.ok_or_else(|| {
        ReclaimError::NotFound(format!("replica parent object {}", replica.object_id))
    })?;
    let generation = decode_u64(generation, "objects.generation").map_err(map_sqlite)?;
    if generation != replica.generation {
        return Err(ReclaimError::GenerationMismatch(format!(
            "replica {} generation {} does not match object {} generation {generation}",
            replica.replica_id, replica.generation, replica.object_id
        )));
    }
    let state = LifecycleState::parse(&state)
        .map_err(|e| ReclaimError::Persistence(format!("corrupt object lifecycle state: {e}")))?;
    if state == LifecycleState::Reclaimed {
        return Err(ReclaimError::InvalidArgument(format!(
            "cannot attach replica {} to reclaimed object {}",
            replica.replica_id, replica.object_id
        )));
    }
    Ok(())
}

fn validate_reservation_status(status: &str) -> Result<()> {
    match status {
        "OPEN" | "COMMITTED" | "RELEASED" | "EXPIRED" => Ok(()),
        _ => Err(ReclaimError::InvalidArgument(format!(
            "invalid reservation status {status:?}"
        ))),
    }
}

fn validate_schema_shape(conn: &Connection) -> Result<()> {
    // Preparing a zero-row projection is a cheap, non-mutating proof that
    // every authoritative table and column required by this schema version
    // exists. `quick_check` alone only validates SQLite's B-trees.
    const PROBES: &[&str] = &[
        "SELECT id,generation,class,logical_size,physical_size,compressed_size,created_at_ms,last_access_ms,access_count,reuse_probability,reuse_horizon_secs,recompute_cost,recompute_latency_secs,transfer_cost,migration_cost,storage_cost_per_byte_sec,memory_cost_per_byte_sec,replication_count,durability_class,survivability_class,owner,content_hash,lifecycle_state,policy_version,decision_epoch,pinned,protected,min_retention_deadline_ms,max_retention_deadline_ms,app_metadata FROM objects LIMIT 0",
        "SELECT replica_id,object_id,generation,backend,key,kind,size,content_hash,created_at_ms,verified_at_ms,valid,owner_node FROM replicas LIMIT 0",
        "SELECT parent_id,child_id,kind FROM lineage LIMIT 0",
        "SELECT content_hash,backend,storage_key,ref_count,payload_size FROM dedup LIMIT 0",
        "SELECT id,object_id,generation,verdict,score,threshold,policy_id,policy_version,epoch,components_json,reasons_json,created_at_ms FROM decisions LIMIT 0",
        "SELECT attempt_id,object_id,generation,node,created_at_ms,updated_at_ms,status FROM attempts LIMIT 0",
        "SELECT reservation_id,attempt_id,object_id,generation,node,created_at_ms,expires_at_ms,status FROM reservations LIMIT 0",
        "SELECT id,epoch,process_id,boot_id,last_seen_ms FROM coordinator LIMIT 0",
        "SELECT archive_id,object_id,generation,backend,key,size,content_hash,created_at_ms,valid FROM archives LIMIT 0",
        "SELECT id,ts_ms,object_id,attempt_id,kind,message,recovered FROM failures LIMIT 0",
        "SELECT id,ts_ms,actor,action,object_id,generation,prior_state,new_state,policy,attempt_id,node,detail FROM audit LIMIT 0",
        "SELECT attempt_id,object_id,generation,phase,created_at_ms,updated_at_ms,payload FROM journal LIMIT 0",
        "SELECT id,version,policy_json FROM policies LIMIT 0",
    ];
    for probe in PROBES {
        conn.prepare(probe).map_err(|e| {
            ReclaimError::Persistence(format!(
                "store schema version {SCHEMA_VERSION} is incomplete or incompatible: {e}"
            ))
        })?;
    }
    // Column presence alone does not prove uniqueness, primary keys, CHECKs,
    // AUTOINCREMENT, defaults, or required indexes. Build the authoritative
    // v1 schema in an isolated in-memory database and compare canonical
    // sqlite_schema definitions. The store owns its database file, so extra
    // application schema is rejected as well as missing/altered definitions.
    let reference = Connection::open_in_memory().map_err(map_sqlite)?;
    reference.execute_batch(SCHEMA_SQL).map_err(map_sqlite)?;
    let expected = schema_snapshot(&reference)?;
    let actual = schema_snapshot(conn)?;
    if actual != expected {
        return Err(ReclaimError::Persistence(format!(
            "store schema version {SCHEMA_VERSION} definitions do not match this binary"
        )));
    }
    Ok(())
}

fn schema_snapshot(conn: &Connection) -> Result<Vec<(String, String, String, String)>> {
    let mut statement = conn
        .prepare(
            "SELECT type, name, tbl_name, sql FROM sqlite_schema
             WHERE name NOT LIKE 'sqlite_%' AND type IN ('table','index','trigger','view')
             ORDER BY type, name",
        )
        .map_err(map_sqlite)?;
    let rows = statement
        .query_map([], |row| {
            let sql: String = row.get(3)?;
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                sql.split_whitespace()
                    .collect::<String>()
                    .to_ascii_lowercase(),
            ))
        })
        .map_err(map_sqlite)?;
    rows.collect::<std::result::Result<Vec<_>, _>>()
        .map_err(map_sqlite)
}

impl Store {
    /// Open (creating if needed) a store at `path`. `":memory:"` for tests.
    pub fn open(path: &str) -> Result<Store> {
        if path.trim().is_empty() {
            return Err(ReclaimError::InvalidArgument(
                "store path must not be empty or whitespace".into(),
            ));
        }
        let conn = Connection::open(path).map_err(map_sqlite)?;
        let store = Store {
            conn: Arc::new(Mutex::new(conn)),
        };
        store.init()?;
        Ok(store)
    }

    pub fn open_in_memory() -> Result<Store> {
        Store::open(":memory:")
    }

    fn init(&self) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        conn.busy_timeout(std::time::Duration::from_secs(5))
            .map_err(map_sqlite)?;
        conn.pragma_update(None, "journal_mode", "WAL")
            .map_err(map_sqlite)?;
        conn.pragma_update(None, "foreign_keys", "ON")
            .map_err(map_sqlite)?;
        conn.pragma_update(None, "synchronous", "FULL")
            .map_err(map_sqlite)?;

        let version: i64 = conn
            .query_row("PRAGMA user_version", (), |r| r.get(0))
            .map_err(map_sqlite)?;
        if version == 0 {
            let existing_application_schema: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'",
                    (),
                    |r| r.get(0),
                )
                .map_err(map_sqlite)?;
            if existing_application_schema != 0 {
                return Err(ReclaimError::Persistence(format!(
                    "unversioned database contains {existing_application_schema} pre-existing application schema entries; refusing to bless an unknown or partial schema"
                )));
            }
            conn.execute_batch(SCHEMA_SQL).map_err(map_sqlite)?;
            conn.pragma_update(None, "user_version", SCHEMA_VERSION)
                .map_err(map_sqlite)?;
        } else if version != SCHEMA_VERSION {
            return Err(ReclaimError::Persistence(format!(
                "store schema version {version} is not supported by this binary (expected {SCHEMA_VERSION}); refusing to open or silently migrate"
            )));
        }
        validate_schema_shape(&conn)?;
        let integrity: String = conn
            .query_row("PRAGMA quick_check(1)", (), |r| r.get(0))
            .map_err(map_sqlite)?;
        if integrity != "ok" {
            return Err(ReclaimError::Persistence(format!(
                "SQLite quick_check failed: {integrity}"
            )));
        }
        Ok(())
    }

    // ------------------------------------------------------------------
    // Objects
    // ------------------------------------------------------------------

    pub fn create_object(&self, obj: &ReclaimObject) -> Result<()> {
        obj.validate()?;
        let mut conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        let tx = conn.transaction().map_err(map_sqlite)?;
        insert_object_row(&tx, obj)?;
        tx.commit().map_err(map_sqlite)?;
        Ok(())
    }

    /// Create an object and append its audit record in one SQLite
    /// transaction. Either both authoritative facts become visible, or
    /// neither does.
    pub fn create_object_with_audit(&self, obj: &ReclaimObject, audit: &AuditEntry) -> Result<()> {
        self.create_object_with_audits(obj, std::slice::from_ref(audit))
    }

    /// Create an object and append all registration/lifecycle audit records
    /// in one transaction. An empty audit batch is rejected because callers
    /// that do not need audit should use `create_object` explicitly.
    pub fn create_object_with_audits(
        &self,
        obj: &ReclaimObject,
        audits: &[AuditEntry],
    ) -> Result<()> {
        obj.validate()?;
        if audits.is_empty() {
            return Err(ReclaimError::InvalidArgument(
                "atomic audited object creation requires at least one audit entry".into(),
            ));
        }
        for audit in audits {
            validate_object_audit(obj, audit)?;
        }
        let mut conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(map_sqlite)?;
        insert_object_row(&tx, obj)?;
        for audit in audits {
            insert_audit_row(&tx, audit)?;
        }
        tx.commit().map_err(map_sqlite)
    }

    pub fn update_object(&self, obj: &ReclaimObject) -> Result<()> {
        obj.validate()?;
        let mut conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        let tx = conn.transaction().map_err(map_sqlite)?;
        update_object_row(&tx, obj)?;
        tx.commit().map_err(map_sqlite)?;
        Ok(())
    }

    /// Update an object and append the corresponding audit entry atomically.
    pub fn update_object_with_audit(&self, obj: &ReclaimObject, audit: &AuditEntry) -> Result<()> {
        obj.validate()?;
        validate_object_audit(obj, audit)?;
        let mut conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(map_sqlite)?;
        update_object_row(&tx, obj)?;
        insert_audit_row(&tx, audit)?;
        tx.commit().map_err(map_sqlite)
    }

    pub fn get_object(&self, id: &Uuid) -> Result<Option<ReclaimObject>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        let mut stmt = conn
            .prepare_cached("SELECT * FROM objects WHERE id=?1")
            .map_err(map_sqlite)?;
        let mut rows = stmt.query([id.to_string()]).map_err(map_sqlite)?;
        let row = rows.next().map_err(map_sqlite)?;
        match row {
            Some(r) => Ok(Some(row_to_object(r)?)),
            None => Ok(None),
        }
    }

    pub fn require_object(&self, id: &Uuid) -> Result<ReclaimObject> {
        self.get_object(id)?
            .ok_or_else(|| ReclaimError::NotFound(id.to_string()))
    }

    pub fn list_objects(&self) -> Result<Vec<ReclaimObject>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        let mut stmt = conn
            .prepare_cached("SELECT * FROM objects ORDER BY id")
            .map_err(map_sqlite)?;
        let rows = stmt.query_map((), row_to_object).map_err(map_sqlite)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(map_sqlite)
    }

    pub fn object_count(&self) -> Result<u64> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        conn.query_row("SELECT COUNT(*) FROM objects", (), |r| r.get::<_, i64>(0))
            .map(|v| v as u64)
            .map_err(map_sqlite)
    }

    pub fn count_objects_in_state(&self, state: LifecycleState) -> Result<u64> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        conn.query_row(
            "SELECT COUNT(*) FROM objects WHERE lifecycle_state=?1",
            [state.as_str()],
            |r| r.get::<_, i64>(0),
        )
        .map(|v| v as u64)
        .map_err(map_sqlite)
    }

    /// Remove a failed staged object and append the rollback event atomically.
    /// Existing audit history is deliberately retained. Callers must first
    /// clean owned replicas/archives/lineage so deletion cannot orphan rows.
    pub fn delete_object_with_audit(&self, id: &Uuid, audit: &AuditEntry) -> Result<()> {
        if audit.object_id != Some(*id) || audit.new_state.is_some() {
            return Err(ReclaimError::InvalidArgument(
                "object deletion audit must identify the object and have no new_state".into(),
            ));
        }
        if let Some(prior) = &audit.prior_state {
            LifecycleState::parse(prior).map_err(|e| {
                ReclaimError::InvalidArgument(format!(
                    "deletion audit has invalid prior state: {e}"
                ))
            })?;
        }
        let mut conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(map_sqlite)?;
        let generation: Option<i64> = tx
            .query_row(
                "SELECT generation FROM objects WHERE id=?1",
                [id.to_string()],
                |row| row.get(0),
            )
            .optional()
            .map_err(map_sqlite)?;
        let generation =
            generation.ok_or_else(|| ReclaimError::NotFound(format!("object {id}")))?;
        let generation = decode_u64(generation, "objects.generation").map_err(map_sqlite)?;
        if audit.generation != Some(generation) {
            return Err(ReclaimError::InvalidArgument(format!(
                "deletion audit generation {:?} does not match object generation {generation}",
                audit.generation
            )));
        }
        let dependents: i64 = tx
            .query_row(
                "SELECT
                    (SELECT COUNT(*) FROM replicas WHERE object_id=?1) +
                    (SELECT COUNT(*) FROM archives WHERE object_id=?1) +
                    (SELECT COUNT(*) FROM lineage WHERE parent_id=?1 OR child_id=?1)",
                [id.to_string()],
                |row| row.get(0),
            )
            .map_err(map_sqlite)?;
        if dependents != 0 {
            return Err(ReclaimError::DependencyViolation(format!(
                "object {id} still has {dependents} replica/archive/lineage rows"
            )));
        }
        let changed = tx
            .execute("DELETE FROM objects WHERE id=?1", [id.to_string()])
            .map_err(map_sqlite)?;
        if changed != 1 {
            return Err(ReclaimError::NotFound(format!("object {id}")));
        }
        insert_audit_row(&tx, audit)?;
        tx.commit().map_err(map_sqlite)
    }

    // ------------------------------------------------------------------
    // Replicas
    // ------------------------------------------------------------------

    pub fn add_replica(&self, r: &Replica) -> Result<()> {
        validate_replica_descriptor(r)?;
        let generation = sql_u64("replica generation", r.generation)?;
        let size = sql_u64("replica size", r.size)?;
        let conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        conn.execute(
            "INSERT INTO replicas (replica_id, object_id, generation, backend, key, kind, size,
                content_hash, created_at_ms, verified_at_ms, valid, owner_node)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",
            rusqlite::params![
                r.replica_id.to_string(),
                r.object_id.to_string(),
                generation,
                r.location.backend,
                r.location.key,
                r.location.kind.as_str(),
                size,
                r.content_hash.to_string(),
                r.created_at_ms,
                r.verified_at_ms,
                r.valid as i64,
                r.owner_node,
            ],
        )
        .map_err(map_sqlite)?;
        Ok(())
    }

    pub fn update_replica(&self, r: &Replica) -> Result<()> {
        validate_replica_descriptor(r)?;
        let generation = sql_u64("replica generation", r.generation)?;
        let size = sql_u64("replica size", r.size)?;
        let mut conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(map_sqlite)?;
        validate_replica_parent(&tx, r)?;
        let changed = tx
            .execute(
                "UPDATE replicas SET backend=?2, key=?3, kind=?4, size=?5, content_hash=?6,
                created_at_ms=?7, verified_at_ms=?8, valid=?9, owner_node=?10, generation=?11
             WHERE replica_id=?1",
                rusqlite::params![
                    r.replica_id.to_string(),
                    r.location.backend,
                    r.location.key,
                    r.location.kind.as_str(),
                    size,
                    r.content_hash.to_string(),
                    r.created_at_ms,
                    r.verified_at_ms,
                    r.valid as i64,
                    r.owner_node,
                    generation,
                ],
            )
            .map_err(map_sqlite)?;
        if changed != 1 {
            return Err(ReclaimError::NotFound(format!("replica {}", r.replica_id)));
        }
        tx.commit().map_err(map_sqlite)
    }

    pub fn replicas_for(&self, object_id: &Uuid) -> Result<Vec<Replica>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        let mut stmt = conn
            .prepare_cached("SELECT * FROM replicas WHERE object_id=?1 ORDER BY replica_id")
            .map_err(map_sqlite)?;
        let rows = stmt
            .query_map([object_id.to_string()], row_to_replica)
            .map_err(map_sqlite)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(map_sqlite)
    }

    pub fn all_replicas(&self) -> Result<Vec<Replica>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        let mut stmt = conn
            .prepare_cached("SELECT * FROM replicas ORDER BY replica_id")
            .map_err(map_sqlite)?;
        let rows = stmt.query_map((), row_to_replica).map_err(map_sqlite)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(map_sqlite)
    }

    pub fn delete_replica(&self, replica_id: &Uuid) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        conn.execute(
            "DELETE FROM replicas WHERE replica_id=?1",
            [replica_id.to_string()],
        )
        .map_err(map_sqlite)?;
        Ok(())
    }

    pub fn replica_count(&self, object_id: &Uuid) -> Result<u64> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        conn.query_row(
            "SELECT COUNT(*) FROM replicas WHERE object_id=?1",
            [object_id.to_string()],
            |r| r.get::<_, i64>(0),
        )
        .map(|v| v as u64)
        .map_err(map_sqlite)
    }

    pub fn valid_replica_count(&self, object_id: &Uuid) -> Result<u64> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        conn.query_row(
            "SELECT COUNT(*) FROM replicas WHERE object_id=?1 AND valid=1",
            [object_id.to_string()],
            |r| r.get::<_, i64>(0),
        )
        .map(|v| v as u64)
        .map_err(map_sqlite)
    }

    /// Valid replica counts for ALL objects in one query (batch planning).
    pub fn valid_replica_counts(&self) -> Result<HashMap<Uuid, u64>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        let mut stmt = conn
            .prepare_cached(
                "SELECT object_id, COUNT(*) FROM replicas WHERE valid=1 GROUP BY object_id",
            )
            .map_err(map_sqlite)?;
        let rows = stmt
            .query_map((), |r| {
                let oid: String = r.get(0)?;
                let n: i64 = r.get(1)?;
                Ok((
                    decode_uuid(&oid, "replicas.object_id")?,
                    decode_u64(n, "replica count")?,
                ))
            })
            .map_err(map_sqlite)?;
        rows.collect::<std::result::Result<HashMap<_, _>, _>>()
            .map_err(map_sqlite)
    }

    /// Archive counts for ALL objects in one query (batch planning).
    pub fn archive_counts(&self) -> Result<HashMap<Uuid, u64>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        let mut stmt = conn
            .prepare_cached("SELECT object_id, valid FROM archives ORDER BY object_id, archive_id")
            .map_err(map_sqlite)?;
        let rows = stmt
            .query_map((), |r| {
                let oid: String = r.get(0)?;
                let valid = decode_bool(r.get(1)?, "archives.valid")?;
                Ok((decode_uuid(&oid, "archives.object_id")?, valid))
            })
            .map_err(map_sqlite)?;
        let mut counts = HashMap::new();
        for row in rows {
            let (object_id, valid) = row.map_err(map_sqlite)?;
            if valid {
                let count = counts.entry(object_id).or_insert(0u64);
                *count = count
                    .checked_add(1)
                    .ok_or_else(|| ReclaimError::Persistence("archive count exceeds u64".into()))?;
            }
        }
        Ok(counts)
    }

    /// Total replicas (for dedup ref-count validation).
    pub fn replica_count_by_hash(&self) -> Result<Vec<(ContentHash, u64)>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        let mut stmt = conn
            .prepare_cached("SELECT content_hash, COUNT(*) FROM replicas GROUP BY content_hash")
            .map_err(map_sqlite)?;
        let rows = stmt
            .query_map((), |r| {
                let s: String = r.get(0)?;
                let n: i64 = r.get(1)?;
                Ok((
                    decode_hash(&s, "replicas.content_hash")?,
                    decode_u64(n, "replica hash count")?,
                ))
            })
            .map_err(map_sqlite)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(map_sqlite)
    }

    // ------------------------------------------------------------------
    // Lineage
    // ------------------------------------------------------------------

    pub fn add_lineage_edge(&self, parent: Uuid, child: Uuid, kind: EdgeKind) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        conn.execute(
            "INSERT OR IGNORE INTO lineage (parent_id, child_id, kind) VALUES (?1,?2,?3)",
            rusqlite::params![parent.to_string(), child.to_string(), kind.as_str()],
        )
        .map_err(map_sqlite)?;
        Ok(())
    }

    pub fn remove_lineage_edge(&self, parent: Uuid, child: Uuid, kind: EdgeKind) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        conn.execute(
            "DELETE FROM lineage WHERE parent_id=?1 AND child_id=?2 AND kind=?3",
            rusqlite::params![parent.to_string(), child.to_string(), kind.as_str()],
        )
        .map_err(map_sqlite)?;
        Ok(())
    }

    pub fn remove_all_lineage_for(&self, id: Uuid) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        conn.execute(
            "DELETE FROM lineage WHERE parent_id=?1 OR child_id=?1",
            [id.to_string()],
        )
        .map_err(map_sqlite)?;
        Ok(())
    }

    pub fn lineage_graph(&self) -> Result<LineageGraph> {
        // Collect everything under a single lock acquisition, then build the
        // graph without the lock held (the lock is not reentrant).
        let (object_ids, edge_rows): (Vec<Uuid>, Vec<(Uuid, Uuid, EdgeKind)>) = {
            let conn = self
                .conn
                .lock()
                .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
            let ids = {
                let mut stmt = conn
                    .prepare_cached("SELECT id FROM objects")
                    .map_err(map_sqlite)?;
                let rows = stmt
                    .query_map((), |r| {
                        let s: String = r.get(0)?;
                        decode_uuid(&s, "objects.id")
                    })
                    .map_err(map_sqlite)?;
                rows.collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(map_sqlite)?
            };
            let edges = {
                let mut stmt = conn
                    .prepare_cached("SELECT parent_id, child_id, kind FROM lineage")
                    .map_err(map_sqlite)?;
                let rows = stmt
                    .query_map((), |r| {
                        let p: String = r.get(0)?;
                        let c: String = r.get(1)?;
                        let k: String = r.get(2)?;
                        let parent = decode_uuid(&p, "lineage.parent_id")?;
                        let child = decode_uuid(&c, "lineage.child_id")?;
                        let kind = match k.as_str() {
                            "DERIVES_FROM" => EdgeKind::DerivesFrom,
                            "DEPENDS_ON" => EdgeKind::DependsOn,
                            "SUPERSEDES" => EdgeKind::Supersedes,
                            "DUPLICATES" => EdgeKind::Duplicates,
                            _ => {
                                return Err(rusqlite::Error::InvalidParameterName(format!(
                                    "bad edge kind {k}"
                                )))
                            }
                        };
                        Ok((parent, child, kind))
                    })
                    .map_err(map_sqlite)?;
                rows.collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(map_sqlite)?
            };
            (ids, edges)
        };
        let mut g = LineageGraph::default();
        for id in object_ids {
            g.add_object(id);
        }
        for (parent, child, kind) in edge_rows {
            g.add_edge(parent, child, kind)
                .map_err(|e| ReclaimError::Persistence(format!("corrupt lineage: {e}")))?;
        }
        Ok(g)
    }

    // ------------------------------------------------------------------
    // Dedup
    // ------------------------------------------------------------------

    pub fn insert_dedup(&self, entry: &DedupEntry) -> Result<()> {
        validate_dedup_entry(entry)?;
        let conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        conn.execute(
            "INSERT INTO dedup (content_hash, backend, storage_key, ref_count, payload_size)
             VALUES (?1,?2,?3,?4,?5)",
            rusqlite::params![
                entry.content_hash.to_string(),
                entry.backend,
                entry.key,
                sql_u64("dedup ref_count", entry.ref_count)?,
                sql_u64("dedup payload_size", entry.payload_size)?,
            ],
        )
        .map_err(map_sqlite)?;
        Ok(())
    }

    /// Insert or overwrite a dedup entry (used by recovery to repair ref
    /// counts; never creates or destroys physical payloads).
    pub fn upsert_dedup(&self, entry: &DedupEntry) -> Result<()> {
        validate_dedup_entry(entry)?;
        let conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        conn.execute(
            "INSERT INTO dedup (content_hash, backend, storage_key, ref_count, payload_size)
             VALUES (?1,?2,?3,?4,?5)
             ON CONFLICT(content_hash, backend) DO UPDATE SET
                storage_key=excluded.storage_key,
                ref_count=excluded.ref_count, payload_size=excluded.payload_size",
            rusqlite::params![
                entry.content_hash.to_string(),
                entry.backend,
                entry.key,
                sql_u64("dedup ref_count", entry.ref_count)?,
                sql_u64("dedup payload_size", entry.payload_size)?,
            ],
        )
        .map_err(map_sqlite)?;
        Ok(())
    }

    pub fn get_dedup(&self, hash: &ContentHash, backend: &str) -> Result<Option<DedupEntry>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        let mut stmt = conn
            .prepare_cached("SELECT content_hash, backend, storage_key, ref_count, payload_size FROM dedup WHERE content_hash=?1 AND backend=?2")
            .map_err(map_sqlite)?;
        let mut rows = stmt
            .query(rusqlite::params![hash.to_string(), backend])
            .map_err(map_sqlite)?;
        if let Some(r) = rows.next().map_err(map_sqlite)? {
            let h: String = r.get(0)?;
            let backend: String = r.get(1)?;
            let key: String = r.get(2)?;
            let rc: i64 = r.get(3)?;
            let size: i64 = r.get(4)?;
            Ok(Some(DedupEntry {
                content_hash: decode_hash(&h, "dedup.content_hash").map_err(map_sqlite)?,
                backend,
                key,
                ref_count: decode_u64(rc, "dedup.ref_count").map_err(map_sqlite)?,
                payload_size: decode_u64(size, "dedup.payload_size").map_err(map_sqlite)?,
            }))
        } else {
            Ok(None)
        }
    }

    pub fn list_dedup(&self) -> Result<Vec<DedupEntry>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        let mut stmt = conn
            .prepare_cached("SELECT content_hash, backend, storage_key, ref_count, payload_size FROM dedup ORDER BY content_hash, backend")
            .map_err(map_sqlite)?;
        let rows = stmt
            .query_map((), |r| {
                let h: String = r.get(0)?;
                let backend: String = r.get(1)?;
                let key: String = r.get(2)?;
                let rc: i64 = r.get(3)?;
                let size: i64 = r.get(4)?;
                Ok(DedupEntry {
                    content_hash: decode_hash(&h, "dedup.content_hash")?,
                    backend,
                    key,
                    ref_count: decode_u64(rc, "dedup.ref_count")?,
                    payload_size: decode_u64(size, "dedup.payload_size")?,
                })
            })
            .map_err(map_sqlite)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(map_sqlite)
    }

    /// Atomically increment a dedup ref count; fails if the hash is absent.
    pub fn dedup_acquire(&self, hash: &ContentHash, backend: &str) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        let n = conn
            .execute(
                "UPDATE dedup SET ref_count = ref_count + 1
                 WHERE content_hash=?1 AND backend=?2 AND ref_count >= 1 AND ref_count < ?3",
                rusqlite::params![hash.to_string(), backend, i64::MAX],
            )
            .map_err(map_sqlite)?;
        if n == 0 {
            let current: Option<i64> = conn
                .query_row(
                    "SELECT ref_count FROM dedup WHERE content_hash=?1 AND backend=?2",
                    rusqlite::params![hash.to_string(), backend],
                    |r| r.get(0),
                )
                .optional()
                .map_err(map_sqlite)?;
            return Err(ReclaimError::Dedup(match current {
                None => format!("no dedup entry for {hash} on {backend}"),
                Some(v) => format!(
                    "cannot acquire dedup entry {hash} on {backend}: invalid or overflowing ref_count {v}"
                ),
            }));
        }
        Ok(())
    }

    /// Atomically decrement a dedup ref count; returns true when it hit zero
    /// (caller owns physical deletion).
    pub fn dedup_release(&self, hash: &ContentHash, backend: &str) -> Result<bool> {
        let mut conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        // IMMEDIATE obtains the database write reservation before reading the
        // count. Without this, two Store connections can both observe the
        // same count and leave a zero-ref row without granting deletion.
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(map_sqlite)?;
        let rc: Option<i64> = tx
            .query_row(
                "SELECT ref_count FROM dedup WHERE content_hash=?1 AND backend=?2",
                rusqlite::params![hash.to_string(), backend],
                |r| r.get(0),
            )
            .optional()
            .map_err(map_sqlite)?;
        let rc = rc.ok_or_else(|| {
            ReclaimError::Dedup(format!("no dedup entry for {hash} on {backend}"))
        })?;
        if rc <= 0 {
            return Err(ReclaimError::Dedup(format!(
                "release with zero refs for {hash}"
            )));
        }
        if rc == 1 {
            let changed = tx
                .execute(
                    "DELETE FROM dedup WHERE content_hash=?1 AND backend=?2",
                    rusqlite::params![hash.to_string(), backend],
                )
                .map_err(map_sqlite)?;
            if changed != 1 {
                return Err(ReclaimError::Dedup(format!(
                    "dedup entry {hash} on {backend} changed during release"
                )));
            }
            tx.commit().map_err(map_sqlite)?;
            Ok(true)
        } else {
            let changed = tx
                .execute(
                    "UPDATE dedup SET ref_count = ref_count - 1 WHERE content_hash=?1 AND backend=?2 AND ref_count=?3",
                    rusqlite::params![hash.to_string(), backend, rc],
                )
                .map_err(map_sqlite)?;
            if changed != 1 {
                return Err(ReclaimError::Dedup(format!(
                    "dedup entry {hash} on {backend} changed during release"
                )));
            }
            tx.commit().map_err(map_sqlite)?;
            Ok(false)
        }
    }

    /// Remove an unreferenced/corrupt derived dedup row during reconciliation.
    pub fn delete_dedup(&self, hash: &ContentHash, backend: &str) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        conn.execute(
            "DELETE FROM dedup WHERE content_hash=?1 AND backend=?2",
            rusqlite::params![hash.to_string(), backend],
        )
        .map_err(map_sqlite)?;
        Ok(())
    }

    pub fn dedup_count(&self) -> Result<u64> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        conn.query_row("SELECT COUNT(*) FROM dedup", (), |r| r.get::<_, i64>(0))
            .map(|v| v as u64)
            .map_err(map_sqlite)
    }

    // ------------------------------------------------------------------
    // Decisions
    // ------------------------------------------------------------------

    pub fn insert_decision(&self, d: &crate::economics::Decision) -> Result<()> {
        if !d.score.is_finite() || !d.threshold.is_finite() {
            return Err(ReclaimError::InvalidArgument(
                "decision score and threshold must be finite".into(),
            ));
        }
        let conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        conn.execute(
            "INSERT INTO decisions (object_id, generation, verdict, score, threshold, policy_id,
                policy_version, epoch, components_json, reasons_json, created_at_ms)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
            rusqlite::params![
                d.object_id.to_string(),
                sql_u64("decision generation", d.generation)?,
                format!("{:?}", d.verdict),
                d.score,
                d.threshold,
                d.policy_id,
                d.policy_version,
                sql_u64("decision epoch", d.epoch)?,
                serde_json::to_string(&d.components).map_err(ReclaimError::from)?,
                serde_json::to_string(&d.reasons).map_err(ReclaimError::from)?,
                chrono_now_ms(),
            ],
        )
        .map_err(map_sqlite)?;
        Ok(())
    }

    pub fn decision_count(&self) -> Result<u64> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        conn.query_row("SELECT COUNT(*) FROM decisions", (), |r| r.get::<_, i64>(0))
            .map(|v| v as u64)
            .map_err(map_sqlite)
    }

    pub fn list_decisions_for(&self, object_id: &Uuid) -> Result<Vec<serde_json::Value>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        let mut stmt = conn
            .prepare_cached(
                "SELECT object_id, generation, verdict, score, threshold, policy_id, policy_version,
                    epoch, components_json, reasons_json, created_at_ms
                 FROM decisions WHERE object_id=?1 ORDER BY created_at_ms, id",
            )
            .map_err(map_sqlite)?;
        let rows = stmt
            .query_map([object_id.to_string()], |r| {
                let object_id: String = r.get(0)?;
                let generation: i64 = r.get(1)?;
                let verdict: String = r.get(2)?;
                let score: f64 = r.get(3)?;
                let threshold: f64 = r.get(4)?;
                let policy_id: String = r.get(5)?;
                let policy_version: String = r.get(6)?;
                let epoch: i64 = r.get(7)?;
                let components: String = r.get(8)?;
                let reasons: String = r.get(9)?;
                let ts: i64 = r.get(10)?;
                let generation = decode_u64(generation, "decisions.generation")?;
                let epoch = decode_u64(epoch, "decisions.epoch")?;
                if !score.is_finite() || !threshold.is_finite() {
                    return Err(rusqlite::Error::InvalidParameterName(
                        "corrupt decision: score and threshold must be finite".into(),
                    ));
                }
                let components =
                    serde_json::from_str::<serde_json::Value>(&components).map_err(|e| {
                        rusqlite::Error::InvalidParameterName(format!(
                            "corrupt decision components_json: {e}"
                        ))
                    })?;
                let reasons = serde_json::from_str::<serde_json::Value>(&reasons).map_err(|e| {
                    rusqlite::Error::InvalidParameterName(format!(
                        "corrupt decision reasons_json: {e}"
                    ))
                })?;
                Ok(serde_json::json!({
                    "object_id": object_id, "generation": generation, "verdict": verdict,
                    "score": score, "threshold": threshold, "policy_id": policy_id,
                    "policy_version": policy_version, "epoch": epoch,
                    "components": components,
                    "reasons": reasons,
                    "created_at_ms": ts,
                }))
            })
            .map_err(map_sqlite)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(map_sqlite)
    }

    // ------------------------------------------------------------------
    // Attempts / reservations / journal
    // ------------------------------------------------------------------

    pub fn create_attempt(&self, attempt: &Attempt) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        conn.execute(
            "INSERT INTO attempts (attempt_id, object_id, generation, node, created_at_ms, updated_at_ms, status)
             VALUES (?1,?2,?3,?4,?5,?6,?7)",
            rusqlite::params![
                attempt.attempt_id.to_string(),
                attempt.object_id.to_string(),
                sql_u64("attempt generation", attempt.generation)?,
                attempt.node,
                attempt.created_at_ms,
                attempt.updated_at_ms,
                attempt.status.as_str(),
            ],
        )
        .map_err(map_sqlite)?;
        Ok(())
    }

    pub fn update_attempt(
        &self,
        attempt_id: &Uuid,
        status: AttemptStatus,
        now_ms: i64,
    ) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        let changed = conn
            .execute(
                "UPDATE attempts SET status=?2, updated_at_ms=?3
             WHERE attempt_id=?1 AND status='OPEN'",
                rusqlite::params![attempt_id.to_string(), status.as_str(), now_ms],
            )
            .map_err(map_sqlite)?;
        if changed != 1 {
            return Err(ReclaimError::ReservationConflict(format!(
                "attempt {attempt_id} is missing or no longer OPEN"
            )));
        }
        Ok(())
    }

    /// Recovery-only terminalization: accept an already-applied identical
    /// status after a crash, but reject missing or conflicting attempt rows.
    pub fn reconcile_attempt(
        &self,
        attempt_id: &Uuid,
        status: AttemptStatus,
        now_ms: i64,
    ) -> Result<()> {
        if status == AttemptStatus::Open {
            return Err(ReclaimError::InvalidArgument(
                "recovery cannot terminalize an attempt as OPEN".into(),
            ));
        }
        let mut conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(map_sqlite)?;
        let changed = tx
            .execute(
                "UPDATE attempts SET status=?2, updated_at_ms=?3
                 WHERE attempt_id=?1 AND status='OPEN'",
                rusqlite::params![attempt_id.to_string(), status.as_str(), now_ms],
            )
            .map_err(map_sqlite)?;
        if changed == 0 {
            let current: Option<String> = tx
                .query_row(
                    "SELECT status FROM attempts WHERE attempt_id=?1",
                    [attempt_id.to_string()],
                    |row| row.get(0),
                )
                .optional()
                .map_err(map_sqlite)?;
            if current.as_deref() != Some(status.as_str()) {
                return Err(ReclaimError::ReservationConflict(format!(
                    "attempt {attempt_id} is missing or has conflicting status {:?}",
                    current
                )));
            }
        }
        tx.commit().map_err(map_sqlite)
    }

    pub fn get_attempt(&self, attempt_id: &Uuid) -> Result<Option<Attempt>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        let mut stmt = conn
            .prepare_cached("SELECT attempt_id, object_id, generation, node, created_at_ms, updated_at_ms, status FROM attempts WHERE attempt_id=?1")
            .map_err(map_sqlite)?;
        let mut rows = stmt.query([attempt_id.to_string()]).map_err(map_sqlite)?;
        if let Some(r) = rows.next().map_err(map_sqlite)? {
            let id: String = r.get(0)?;
            let oid: String = r.get(1)?;
            let gen: i64 = r.get(2)?;
            let node: String = r.get(3)?;
            let c: i64 = r.get(4)?;
            let u: i64 = r.get(5)?;
            let s: String = r.get(6)?;
            Ok(Some(Attempt {
                attempt_id: decode_uuid(&id, "attempts.attempt_id").map_err(map_sqlite)?,
                object_id: decode_uuid(&oid, "attempts.object_id").map_err(map_sqlite)?,
                generation: decode_u64(gen, "attempt.generation").map_err(map_sqlite)?,
                node,
                created_at_ms: c,
                updated_at_ms: u,
                status: match s.as_str() {
                    "OPEN" => AttemptStatus::Open,
                    "COMMITTED" => AttemptStatus::Committed,
                    "FAILED" => AttemptStatus::Failed,
                    "ROLLED_BACK" => AttemptStatus::RolledBack,
                    _ => return Err(ReclaimError::Persistence(format!("bad attempt status {s}"))),
                },
            }))
        } else {
            Ok(None)
        }
    }

    pub fn list_open_attempts(&self) -> Result<Vec<Attempt>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        let mut stmt = conn
            .prepare_cached("SELECT attempt_id, object_id, generation, node, created_at_ms, updated_at_ms, status
                             FROM attempts
                             WHERE status NOT IN ('COMMITTED','FAILED','ROLLED_BACK')
                             ORDER BY created_at_ms, attempt_id")
            .map_err(map_sqlite)?;
        let rows = stmt
            .query_map((), |r| {
                let id: String = r.get(0)?;
                let oid: String = r.get(1)?;
                let gen: i64 = r.get(2)?;
                let node: String = r.get(3)?;
                let c: i64 = r.get(4)?;
                let u: i64 = r.get(5)?;
                let s: String = r.get(6)?;
                Ok(Attempt {
                    attempt_id: decode_uuid(&id, "attempts.attempt_id")?,
                    object_id: decode_uuid(&oid, "attempts.object_id")?,
                    generation: decode_u64(gen, "attempt.generation")?,
                    node,
                    created_at_ms: c,
                    updated_at_ms: u,
                    status: match s.as_str() {
                        "OPEN" => AttemptStatus::Open,
                        _ => {
                            return Err(rusqlite::Error::InvalidParameterName(format!(
                                "bad attempt status {s}"
                            )))
                        }
                    },
                })
            })
            .map_err(map_sqlite)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(map_sqlite)
    }

    pub fn create_reservation(&self, r: &Reservation) -> Result<()> {
        validate_reservation_status(&r.status)?;
        if r.status != "OPEN" {
            return Err(ReclaimError::InvalidArgument(
                "new reservations must start OPEN".into(),
            ));
        }
        let conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        conn.execute(
            "INSERT INTO reservations (reservation_id, attempt_id, object_id, generation, node, created_at_ms, expires_at_ms, status)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
            rusqlite::params![
                r.reservation_id.to_string(),
                r.attempt_id.to_string(),
                r.object_id.to_string(),
                sql_u64("reservation generation", r.generation)?,
                r.node,
                r.created_at_ms,
                r.expires_at_ms,
                r.status,
            ],
        )
        .map_err(map_sqlite)?;
        Ok(())
    }

    pub fn update_reservation(&self, reservation_id: &Uuid, status: &str) -> Result<()> {
        validate_reservation_status(status)?;
        let conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        let changed = conn
            .execute(
                "UPDATE reservations SET status=?2 WHERE reservation_id=?1 AND status='OPEN'",
                rusqlite::params![reservation_id.to_string(), status],
            )
            .map_err(map_sqlite)?;
        if changed != 1 {
            return Err(ReclaimError::NotFound(format!(
                "reservation {reservation_id}"
            )));
        }
        Ok(())
    }

    /// Close the open reservation tied to an attempt (if any).
    pub fn update_reservation_for_attempt(&self, attempt_id: &Uuid, status: &str) -> Result<()> {
        validate_reservation_status(status)?;
        let conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        conn.execute(
            "UPDATE reservations SET status=?2 WHERE attempt_id=?1 AND status='OPEN'",
            rusqlite::params![attempt_id.to_string(), status],
        )
        .map_err(map_sqlite)?;
        Ok(())
    }

    pub fn list_open_reservations(&self) -> Result<Vec<Reservation>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        let mut stmt = conn
            .prepare_cached("SELECT reservation_id, attempt_id, object_id, generation, node, created_at_ms, expires_at_ms, status
                             FROM reservations
                             WHERE status NOT IN ('COMMITTED','RELEASED','EXPIRED')
                             ORDER BY created_at_ms, reservation_id")
            .map_err(map_sqlite)?;
        let rows = stmt
            .query_map((), |r| {
                let rid: String = r.get(0)?;
                let aid: String = r.get(1)?;
                let oid: String = r.get(2)?;
                let gen: i64 = r.get(3)?;
                let node: String = r.get(4)?;
                let c: i64 = r.get(5)?;
                let e: i64 = r.get(6)?;
                let s: String = r.get(7)?;
                if s != "OPEN" {
                    return Err(rusqlite::Error::InvalidParameterName(format!(
                        "bad reservation status {s}"
                    )));
                }
                Ok(Reservation {
                    reservation_id: decode_uuid(&rid, "reservations.reservation_id")?,
                    attempt_id: decode_uuid(&aid, "reservations.attempt_id")?,
                    object_id: decode_uuid(&oid, "reservations.object_id")?,
                    generation: decode_u64(gen, "reservation.generation")?,
                    node,
                    created_at_ms: c,
                    expires_at_ms: e,
                    status: s,
                })
            })
            .map_err(map_sqlite)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(map_sqlite)
    }

    pub fn has_open_reservation_for(&self, object_id: &Uuid) -> Result<bool> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        let n: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM reservations WHERE object_id=?1 AND status='OPEN'",
                [object_id.to_string()],
                |r| r.get(0),
            )
            .map_err(map_sqlite)?;
        Ok(n > 0)
    }

    pub fn insert_journal(&self, e: &JournalEntry) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        conn.execute(
            "INSERT INTO journal (attempt_id, object_id, generation, phase, created_at_ms, updated_at_ms, payload)
             VALUES (?1,?2,?3,?4,?5,?6,?7)",
            rusqlite::params![
                e.attempt_id.to_string(),
                e.object_id.to_string(),
                sql_u64("journal generation", e.generation)?,
                e.phase.as_str(),
                e.created_at_ms,
                e.updated_at_ms,
                serde_json::to_string(&e.payload).map_err(ReclaimError::from)?,
            ],
        )
        .map_err(map_sqlite)?;
        Ok(())
    }

    /// Atomically establish the three durable records that grant reclaim
    /// authority. A journal without its attempt/reservation (or vice versa)
    /// is not a recoverable partial reservation.
    pub fn create_reclaim_reservation(
        &self,
        journal: &JournalEntry,
        reservation: &Reservation,
        attempt: &Attempt,
    ) -> Result<()> {
        validate_reservation_status(&reservation.status)?;
        if journal.phase != JournalPhase::Reserved
            || reservation.status != "OPEN"
            || attempt.status != AttemptStatus::Open
        {
            return Err(ReclaimError::InvalidArgument(
                "new reclaim records must start RESERVED/OPEN/OPEN".into(),
            ));
        }
        if journal.attempt_id != attempt.attempt_id
            || journal.attempt_id != reservation.attempt_id
            || journal.object_id != attempt.object_id
            || journal.object_id != reservation.object_id
            || journal.generation != attempt.generation
            || journal.generation != reservation.generation
        {
            return Err(ReclaimError::InvalidArgument(
                "reclaim journal, reservation, and attempt identities do not match".into(),
            ));
        }
        let generation = sql_u64("reclaim generation", journal.generation)?;
        let payload = serde_json::to_string(&journal.payload).map_err(ReclaimError::from)?;
        let mut conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(map_sqlite)?;
        tx.execute(
            "INSERT INTO attempts (attempt_id, object_id, generation, node, created_at_ms, updated_at_ms, status)
             VALUES (?1,?2,?3,?4,?5,?6,'OPEN')",
            rusqlite::params![
                attempt.attempt_id.to_string(),
                attempt.object_id.to_string(),
                generation,
                attempt.node,
                attempt.created_at_ms,
                attempt.updated_at_ms,
            ],
        )
        .map_err(map_sqlite)?;
        tx.execute(
            "INSERT INTO reservations (reservation_id, attempt_id, object_id, generation, node, created_at_ms, expires_at_ms, status)
             VALUES (?1,?2,?3,?4,?5,?6,?7,'OPEN')",
            rusqlite::params![
                reservation.reservation_id.to_string(),
                reservation.attempt_id.to_string(),
                reservation.object_id.to_string(),
                generation,
                reservation.node,
                reservation.created_at_ms,
                reservation.expires_at_ms,
            ],
        )
        .map_err(map_sqlite)?;
        tx.execute(
            "INSERT INTO journal (attempt_id, object_id, generation, phase, created_at_ms, updated_at_ms, payload)
             VALUES (?1,?2,?3,'RESERVED',?4,?5,?6)",
            rusqlite::params![
                journal.attempt_id.to_string(),
                journal.object_id.to_string(),
                generation,
                journal.created_at_ms,
                journal.updated_at_ms,
                payload,
            ],
        )
        .map_err(map_sqlite)?;
        tx.commit().map_err(map_sqlite)
    }

    pub fn update_journal_phase(
        &self,
        attempt_id: &Uuid,
        phase: JournalPhase,
        now_ms: i64,
    ) -> Result<()> {
        let allowed_prior = match phase {
            JournalPhase::Validated => "('RESERVED')",
            // PHYSICAL_STARTED must use `start_journal_physical`, which
            // publishes the physical plan in the same SQLite statement.
            JournalPhase::PhysicalStarted | JournalPhase::Reserved => {
                return Err(ReclaimError::InvalidArgument(format!(
                    "journal phase {} cannot be entered with the generic phase updater",
                    phase.as_str()
                )))
            }
            JournalPhase::PhysicalDone => "('PHYSICAL_STARTED')",
            // Recovery may prove deletion completed after a crash before the
            // normal PHYSICAL_DONE write.
            JournalPhase::Committed => "('PHYSICAL_STARTED','PHYSICAL_DONE')",
            JournalPhase::RolledBack | JournalPhase::Failed => {
                "('RESERVED','VALIDATED','PHYSICAL_STARTED','PHYSICAL_DONE')"
            }
        };
        let conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        let sql = format!(
            "UPDATE journal SET phase=?2, updated_at_ms=?3 WHERE attempt_id=?1 AND phase IN {allowed_prior}"
        );
        let changed = conn
            .execute(
                &sql,
                rusqlite::params![attempt_id.to_string(), phase.as_str(), now_ms],
            )
            .map_err(map_sqlite)?;
        if changed != 1 {
            return Err(ReclaimError::ReservationConflict(format!(
                "journal {attempt_id} cannot transition to {} from its current phase",
                phase.as_str()
            )));
        }
        Ok(())
    }

    /// Atomically make the exact backend-deletion plan durable and advance a
    /// validated reclaim into physical execution. Recovery must never observe
    /// `PHYSICAL_STARTED` without the plan that defines which deduplicated
    /// payloads this attempt actually owned last.
    pub fn start_journal_physical(
        &self,
        attempt_id: &Uuid,
        payload: &serde_json::Value,
        now_ms: i64,
    ) -> Result<()> {
        match payload
            .as_object()
            .and_then(|object| object.get("physical_replica_deletions"))
        {
            Some(serde_json::Value::Array(_)) => {}
            _ => {
                return Err(ReclaimError::InvalidArgument(
                    "physical journal payload requires a concrete physical_replica_deletions array"
                        .into(),
                ))
            }
        }
        let encoded = serde_json::to_string(payload).map_err(ReclaimError::from)?;
        let conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        let changed = conn
            .execute(
                "UPDATE journal SET payload=?2, phase='PHYSICAL_STARTED', updated_at_ms=?3
                 WHERE attempt_id=?1 AND phase='VALIDATED'",
                rusqlite::params![attempt_id.to_string(), encoded, now_ms],
            )
            .map_err(map_sqlite)?;
        if changed != 1 {
            return Err(ReclaimError::ReservationConflict(format!(
                "journal {attempt_id} is not in VALIDATED phase"
            )));
        }
        Ok(())
    }

    pub fn get_journal(&self, attempt_id: &Uuid) -> Result<Option<JournalEntry>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        let mut stmt = conn
            .prepare_cached("SELECT attempt_id, object_id, generation, phase, created_at_ms, updated_at_ms, payload FROM journal WHERE attempt_id=?1")
            .map_err(map_sqlite)?;
        let mut rows = stmt.query([attempt_id.to_string()]).map_err(map_sqlite)?;
        if let Some(r) = rows.next().map_err(map_sqlite)? {
            Ok(Some(row_to_journal(r)?))
        } else {
            Ok(None)
        }
    }

    pub fn list_open_journal(&self) -> Result<Vec<JournalEntry>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        let mut stmt = conn
            .prepare_cached("SELECT attempt_id, object_id, generation, phase, created_at_ms, updated_at_ms, payload
                             FROM journal
                             WHERE phase NOT IN ('COMMITTED','ROLLED_BACK','FAILED')
                             ORDER BY created_at_ms, attempt_id")
            .map_err(map_sqlite)?;
        let rows = stmt.query_map((), row_to_journal).map_err(map_sqlite)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(map_sqlite)
    }

    pub fn list_all_journal(&self) -> Result<Vec<JournalEntry>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        let mut stmt = conn
            .prepare_cached("SELECT attempt_id, object_id, generation, phase, created_at_ms, updated_at_ms, payload FROM journal ORDER BY created_at_ms, attempt_id")
            .map_err(map_sqlite)?;
        let rows = stmt.query_map((), row_to_journal).map_err(map_sqlite)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(map_sqlite)
    }

    // ------------------------------------------------------------------
    // Coordinator authority
    // ------------------------------------------------------------------

    pub fn get_coordinator_state(&self) -> Result<Option<CoordinatorState>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        let mut stmt = conn
            .prepare_cached(
                "SELECT epoch, process_id, boot_id, last_seen_ms FROM coordinator WHERE id=1",
            )
            .map_err(map_sqlite)?;
        let mut rows = stmt.query(()).map_err(map_sqlite)?;
        if let Some(r) = rows.next().map_err(map_sqlite)? {
            let epoch: i64 = r.get(0)?;
            let pid: String = r.get(1)?;
            let boot: String = r.get(2)?;
            let last: i64 = r.get(3)?;
            Ok(Some(CoordinatorState {
                epoch: decode_u64(epoch, "coordinator.epoch").map_err(map_sqlite)?,
                process_id: pid,
                boot_id: decode_uuid(&boot, "coordinator.boot_id").map_err(map_sqlite)?,
                last_seen_ms: last,
            }))
        } else {
            Ok(None)
        }
    }

    /// Atomically claim coordinator authority. Returns the new epoch (1 on
    /// first claim) or an error if another live coordinator holds it.
    pub fn claim_coordinator(
        &self,
        process_id: &str,
        boot_id: &Uuid,
        now_ms: i64,
        stale_ms: i64,
    ) -> Result<u64> {
        if process_id.is_empty() {
            return Err(ReclaimError::InvalidArgument(
                "coordinator process_id must not be empty".into(),
            ));
        }
        if stale_ms <= 0 {
            return Err(ReclaimError::InvalidArgument(
                "coordinator stale window must be positive".into(),
            ));
        }
        let mut conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(map_sqlite)?;
        let existing: Option<(i64, String, String, i64)> = tx
            .query_row(
                "SELECT epoch, process_id, boot_id, last_seen_ms FROM coordinator WHERE id=1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .optional()
            .map_err(map_sqlite)?;
        let epoch = match existing {
            None => {
                tx.execute(
                    "INSERT INTO coordinator (id, epoch, process_id, boot_id, last_seen_ms) VALUES (1,1,?1,?2,?3)",
                    rusqlite::params![process_id, boot_id.to_string(), now_ms],
                )
                .map_err(map_sqlite)?;
                1u64
            }
            Some((old_epoch, old_pid, _old_boot, last_seen)) => {
                let old_epoch = decode_u64(old_epoch, "coordinator.epoch").map_err(map_sqlite)?;
                if last_seen == 0 || now_ms.saturating_sub(last_seen) > stale_ms {
                    // Old holder is stale: take over with a new epoch.
                    let new_epoch = old_epoch.checked_add(1).ok_or_else(|| {
                        ReclaimError::Persistence("coordinator epoch exhausted".into())
                    })?;
                    tx.execute(
                        "UPDATE coordinator SET epoch=?1, process_id=?2, boot_id=?3, last_seen_ms=?4 WHERE id=1",
                        rusqlite::params![
                            sql_u64("coordinator epoch", new_epoch)?,
                            process_id,
                            boot_id.to_string(),
                            now_ms
                        ],
                    )
                    .map_err(map_sqlite)?;
                    new_epoch
                } else {
                    return Err(ReclaimError::ReservationConflict(format!(
                        "coordinator already claimed by {old_pid} (epoch {old_epoch})"
                    )));
                }
            }
        };
        tx.commit().map_err(map_sqlite)?;
        Ok(epoch)
    }

    pub fn coordinator_heartbeat(
        &self,
        process_id: &str,
        boot_id: &Uuid,
        epoch: u64,
        now_ms: i64,
    ) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        let changed = conn
            .execute(
                "UPDATE coordinator SET last_seen_ms=MAX(last_seen_ms, ?1)
             WHERE id=1 AND process_id=?2 AND boot_id=?3 AND epoch=?4",
                rusqlite::params![
                    now_ms,
                    process_id,
                    boot_id.to_string(),
                    sql_u64("coordinator epoch", epoch)?
                ],
            )
            .map_err(map_sqlite)?;
        if changed != 1 {
            return Err(ReclaimError::ReservationConflict(
                "coordinator heartbeat rejected for stale authority".into(),
            ));
        }
        Ok(())
    }

    /// Mark the coordinator released after a CLEAN shutdown: the row becomes
    /// stale immediately, so a restart with a new process id can take over
    /// without waiting out the stale window. A crash leaves the row fresh and
    /// the stale-window takeover applies as designed.
    pub fn release_coordinator(&self, process_id: &str, boot_id: &Uuid, epoch: u64) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        let changed = conn
            .execute(
                "UPDATE coordinator SET last_seen_ms=0
                 WHERE id=1 AND process_id=?1 AND boot_id=?2 AND epoch=?3",
                rusqlite::params![
                    process_id,
                    boot_id.to_string(),
                    sql_u64("coordinator epoch", epoch)?
                ],
            )
            .map_err(map_sqlite)?;
        if changed != 1 {
            return Err(ReclaimError::ReservationConflict(
                "coordinator release rejected for stale authority".into(),
            ));
        }
        Ok(())
    }

    // ------------------------------------------------------------------
    // Archives
    // ------------------------------------------------------------------

    pub fn insert_archive(&self, a: &ArchiveRecord) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        conn.execute(
            "INSERT INTO archives (archive_id, object_id, generation, backend, key, size, content_hash, created_at_ms, valid)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,1)",
            rusqlite::params![
                a.archive_id,
                a.object_id.to_string(),
                sql_u64("archive generation", a.generation)?,
                a.backend,
                a.key,
                sql_u64("archive size", a.size)?,
                a.content_hash.to_string(),
                a.created_at_ms,
            ],
        )
        .map_err(map_sqlite)?;
        Ok(())
    }

    pub fn list_archives(&self) -> Result<Vec<ArchiveRecord>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        let mut stmt = conn
            .prepare_cached("SELECT archive_id, object_id, generation, backend, key, size, content_hash, created_at_ms, valid FROM archives ORDER BY created_at_ms, archive_id")
            .map_err(map_sqlite)?;
        let rows = stmt.query_map((), row_to_archive).map_err(map_sqlite)?;
        Ok(rows
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(map_sqlite)?
            .into_iter()
            .flatten()
            .collect())
    }

    pub fn archives_for(&self, object_id: &Uuid) -> Result<Vec<ArchiveRecord>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        let mut stmt = conn
            .prepare_cached("SELECT archive_id, object_id, generation, backend, key, size, content_hash, created_at_ms, valid FROM archives WHERE object_id=?1 ORDER BY created_at_ms, archive_id")
            .map_err(map_sqlite)?;
        let rows = stmt
            .query_map([object_id.to_string()], row_to_archive)
            .map_err(map_sqlite)?;
        Ok(rows
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(map_sqlite)?
            .into_iter()
            .flatten()
            .collect())
    }

    pub fn delete_archive(&self, archive_id: &str) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        conn.execute("DELETE FROM archives WHERE archive_id=?1", [archive_id])
            .map_err(map_sqlite)?;
        Ok(())
    }

    // ------------------------------------------------------------------
    // Audit
    // ------------------------------------------------------------------

    pub fn append_audit(&self, a: &AuditEntry) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        insert_audit_row(&conn, a)?;
        Ok(())
    }

    /// Replay audit entries, optionally filtered.
    pub fn replay_audit(
        &self,
        object_id: Option<&Uuid>,
        action: Option<&str>,
        limit: u64,
    ) -> Result<Vec<AuditEntry>> {
        let limit = sql_limit(limit)?;
        let conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        let mut sql = String::from(
            "SELECT id, ts_ms, actor, action, object_id, generation, prior_state, new_state, policy, attempt_id, node, detail FROM audit",
        );
        let mut params: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
        let mut clauses: Vec<String> = Vec::new();
        if let Some(oid) = object_id {
            clauses.push("object_id=?1".into());
            params.push(Box::new(oid.to_string()));
        }
        if let Some(act) = action {
            clauses.push(format!("action=?{}", params.len() + 1));
            params.push(Box::new(act.to_string()));
        }
        if !clauses.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&clauses.join(" AND "));
        }
        sql.push_str(" ORDER BY id DESC LIMIT ?");
        params.push(Box::new(limit));
        let mut stmt = conn.prepare_cached(&sql).map_err(map_sqlite)?;
        let param_refs: Vec<&dyn rusqlite::ToSql> = params.iter().map(|p| p.as_ref()).collect();
        let rows = stmt
            .query_map(param_refs.as_slice(), row_to_audit)
            .map_err(map_sqlite)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(map_sqlite)
    }

    pub fn audit_count(&self) -> Result<u64> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        conn.query_row("SELECT COUNT(*) FROM audit", (), |r| r.get::<_, i64>(0))
            .map(|v| v as u64)
            .map_err(map_sqlite)
    }

    // ------------------------------------------------------------------
    // Failures
    // ------------------------------------------------------------------

    pub fn insert_failure(&self, f: &FailureRecord) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        conn.execute(
            "INSERT INTO failures (ts_ms, object_id, attempt_id, kind, message, recovered)
             VALUES (?1,?2,?3,?4,?5,?6)",
            rusqlite::params![
                f.ts_ms,
                f.object_id.map(|v| v.to_string()),
                f.attempt_id.map(|v| v.to_string()),
                f.kind,
                f.message,
                f.recovered as i64,
            ],
        )
        .map_err(map_sqlite)?;
        Ok(())
    }

    pub fn list_failures(&self, limit: u64) -> Result<Vec<FailureRecord>> {
        let limit = sql_limit(limit)?;
        let conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        let mut stmt = conn
            .prepare_cached("SELECT id, ts_ms, object_id, attempt_id, kind, message, recovered FROM failures ORDER BY id DESC LIMIT ?1")
            .map_err(map_sqlite)?;
        let rows = stmt
            .query_map([limit], |r| {
                let id: i64 = r.get(0)?;
                let ts: i64 = r.get(1)?;
                let oid: Option<String> = r.get(2)?;
                let aid: Option<String> = r.get(3)?;
                let kind: String = r.get(4)?;
                let msg: String = r.get(5)?;
                let rec: i64 = r.get(6)?;
                Ok(FailureRecord {
                    id,
                    ts_ms: ts,
                    object_id: decode_optional_uuid(oid, "failures.object_id")?,
                    attempt_id: decode_optional_uuid(aid, "failures.attempt_id")?,
                    kind,
                    message: msg,
                    recovered: decode_bool(rec, "failures.recovered")?,
                })
            })
            .map_err(map_sqlite)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(map_sqlite)
    }

    // ------------------------------------------------------------------
    // Policies
    // ------------------------------------------------------------------

    pub fn upsert_policy(&self, p: &Policy) -> Result<()> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        let json = serde_json::to_string(p).map_err(ReclaimError::from)?;
        conn.execute(
            "INSERT INTO policies (id, version, policy_json) VALUES (?1,?2,?3)
             ON CONFLICT(id, version) DO UPDATE SET policy_json=excluded.policy_json",
            rusqlite::params![p.id, p.version, json],
        )
        .map_err(map_sqlite)?;
        Ok(())
    }

    pub fn list_policies(&self) -> Result<Vec<Policy>> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        let mut stmt = conn
            .prepare_cached("SELECT policy_json FROM policies ORDER BY id, version")
            .map_err(map_sqlite)?;
        let rows = stmt
            .query_map((), |r| {
                let json: String = r.get(0)?;
                serde_json::from_str::<Policy>(&json)
                    .map_err(|e| rusqlite::Error::InvalidParameterName(e.to_string()))
            })
            .map_err(map_sqlite)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(map_sqlite)
    }

    // ------------------------------------------------------------------
    // Stats / misc
    // ------------------------------------------------------------------

    pub fn replica_total_bytes(&self) -> Result<u64> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        conn.query_row(
            "SELECT COALESCE(SUM(size),0) FROM replicas WHERE valid=1",
            [],
            |r| r.get::<_, i64>(0),
        )
        .and_then(|v| decode_u64(v, "replica total bytes"))
        .map_err(map_sqlite)
    }

    pub fn lineage_edge_count(&self) -> Result<u64> {
        let conn = self
            .conn
            .lock()
            .map_err(|_| ReclaimError::Internal("store lock poisoned".into()))?;
        conn.query_row("SELECT COUNT(*) FROM lineage", (), |r| r.get::<_, i64>(0))
            .map(|v| v as u64)
            .map_err(map_sqlite)
    }

    pub fn stats(&self) -> Result<serde_json::Value> {
        Ok(serde_json::json!({
            "objects": self.object_count()?,
            "replicas": self.all_replicas()?.len() as u64,
            "lineage_edges": self.lineage_edge_count()?,
            "dedup_entries": self.dedup_count()?,
            "decisions": self.decision_count()?,
            "audit_entries": self.audit_count()?,
            "open_attempts": self.list_open_attempts()?.len() as u64,
            "open_reservations": self.list_open_reservations()?.len() as u64,
            "journal_entries": self.list_all_journal()?.len() as u64,
            "archives": self.list_archives()?.len() as u64,
            "physical_bytes": self.replica_total_bytes()?,
        }))
    }
}

fn validate_object_audit(obj: &ReclaimObject, audit: &AuditEntry) -> Result<()> {
    if audit.object_id != Some(obj.id) || audit.generation != Some(obj.generation) {
        return Err(ReclaimError::InvalidArgument(format!(
            "audit identity {:?}/{:?} does not match object {}/{}",
            audit.object_id, audit.generation, obj.id, obj.generation
        )));
    }
    if let Some(prior) = &audit.prior_state {
        LifecycleState::parse(prior).map_err(|e| {
            ReclaimError::InvalidArgument(format!("audit has invalid prior state: {e}"))
        })?;
    }
    if let Some(new_state) = &audit.new_state {
        let audited = LifecycleState::parse(new_state).map_err(|e| {
            ReclaimError::InvalidArgument(format!("audit has invalid new state: {e}"))
        })?;
        if audited != obj.lifecycle_state {
            return Err(ReclaimError::InvalidArgument(format!(
                "audit new state {} does not match persisted object state {}",
                audited.as_str(),
                obj.lifecycle_state.as_str()
            )));
        }
    }
    Ok(())
}

fn insert_object_row(conn: &Connection, obj: &ReclaimObject) -> Result<()> {
    conn.execute(
        "INSERT INTO objects (
            id, generation, class, logical_size, physical_size, compressed_size,
            created_at_ms, last_access_ms, access_count, reuse_probability,
            reuse_horizon_secs, recompute_cost, recompute_latency_secs,
            transfer_cost, migration_cost, storage_cost_per_byte_sec,
            memory_cost_per_byte_sec, replication_count, durability_class,
            survivability_class, owner, content_hash, lifecycle_state,
            policy_version, decision_epoch, pinned, protected,
            min_retention_deadline_ms, max_retention_deadline_ms, app_metadata
        ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?24,?25,?26,?27,?28,?29,?30)",
        rusqlite::params![
            obj.id.to_string(),
            sql_u64("object generation", obj.generation)?, obj.class,
            sql_u64("object logical_size", obj.logical_size)?,
            sql_u64("object physical_size", obj.physical_size)?,
            sql_optional_u64("object compressed_size", obj.compressed_size)?,
            obj.created_at_ms, obj.last_access_ms,
            sql_u64("object access_count", obj.access_count)?, obj.reuse_probability,
            sql_optional_u64("object reuse_horizon_secs", obj.reuse_horizon_secs)?,
            obj.recompute_cost, obj.recompute_latency_secs, obj.transfer_cost, obj.migration_cost,
            obj.storage_cost_per_byte_sec, obj.memory_cost_per_byte_sec,
            i64::from(obj.replication_count), dura_str(obj.durability_class),
            surv_str(obj.survivability_class), obj.owner,
            obj.content_hash.map(|h| h.to_string()), obj.lifecycle_state.as_str(),
            obj.policy_version, sql_u64("object decision_epoch", obj.decision_epoch)?,
            obj.pinned as i64, obj.protected as i64,
            obj.min_retention_deadline_ms, obj.max_retention_deadline_ms,
            serde_json::to_string(&obj.app_metadata).map_err(ReclaimError::from)?,
        ],
    )
    .map_err(map_sqlite)?;
    Ok(())
}

fn update_object_row(conn: &Connection, obj: &ReclaimObject) -> Result<()> {
    let changed = conn
        .execute(
            "UPDATE objects SET
            generation=?2, class=?3, logical_size=?4, physical_size=?5, compressed_size=?6,
            created_at_ms=?7, last_access_ms=?8, access_count=?9, reuse_probability=?10,
            reuse_horizon_secs=?11, recompute_cost=?12, recompute_latency_secs=?13,
            transfer_cost=?14, migration_cost=?15, storage_cost_per_byte_sec=?16,
            memory_cost_per_byte_sec=?17, replication_count=?18, durability_class=?19,
            survivability_class=?20, owner=?21, content_hash=?22, lifecycle_state=?23,
            policy_version=?24, decision_epoch=?25, pinned=?26, protected=?27,
            min_retention_deadline_ms=?28, max_retention_deadline_ms=?29, app_metadata=?30
         WHERE id=?1",
            rusqlite::params![
                obj.id.to_string(),
                sql_u64("object generation", obj.generation)?,
                obj.class,
                sql_u64("object logical_size", obj.logical_size)?,
                sql_u64("object physical_size", obj.physical_size)?,
                sql_optional_u64("object compressed_size", obj.compressed_size)?,
                obj.created_at_ms,
                obj.last_access_ms,
                sql_u64("object access_count", obj.access_count)?,
                obj.reuse_probability,
                sql_optional_u64("object reuse_horizon_secs", obj.reuse_horizon_secs)?,
                obj.recompute_cost,
                obj.recompute_latency_secs,
                obj.transfer_cost,
                obj.migration_cost,
                obj.storage_cost_per_byte_sec,
                obj.memory_cost_per_byte_sec,
                i64::from(obj.replication_count),
                dura_str(obj.durability_class),
                surv_str(obj.survivability_class),
                obj.owner,
                obj.content_hash.map(|h| h.to_string()),
                obj.lifecycle_state.as_str(),
                obj.policy_version,
                sql_u64("object decision_epoch", obj.decision_epoch)?,
                obj.pinned as i64,
                obj.protected as i64,
                obj.min_retention_deadline_ms,
                obj.max_retention_deadline_ms,
                serde_json::to_string(&obj.app_metadata).map_err(ReclaimError::from)?,
            ],
        )
        .map_err(map_sqlite)?;
    if changed != 1 {
        return Err(ReclaimError::NotFound(format!("object {}", obj.id)));
    }
    Ok(())
}

fn insert_audit_row(conn: &Connection, audit: &AuditEntry) -> Result<()> {
    conn.execute(
        "INSERT INTO audit (ts_ms, actor, action, object_id, generation, prior_state, new_state, policy, attempt_id, node, detail)
         VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
        rusqlite::params![
            audit.ts_ms, audit.actor, audit.action,
            audit.object_id.map(|v| v.to_string()),
            sql_optional_u64("audit generation", audit.generation)?,
            audit.prior_state, audit.new_state, audit.policy,
            audit.attempt_id.map(|v| v.to_string()), audit.node,
            serde_json::to_string(&audit.detail).map_err(ReclaimError::from)?,
        ],
    ).map_err(map_sqlite)?;
    Ok(())
}

// ----------------------------------------------------------------------
// Row mapping helpers
// ----------------------------------------------------------------------

fn row_to_object(r: &rusqlite::Row<'_>) -> std::result::Result<ReclaimObject, rusqlite::Error> {
    let id: String = r.get("id")?;
    let class: String = r.get("class")?;
    let content_hash: Option<String> = r.get("content_hash")?;
    let lifecycle_state: String = r.get("lifecycle_state")?;
    let durability: String = r.get("durability_class")?;
    let survivability: String = r.get("survivability_class")?;
    let app_meta: String = r.get("app_metadata")?;
    let app_metadata =
        serde_json::from_str::<std::collections::BTreeMap<String, serde_json::Value>>(&app_meta)
            .map_err(|e| rusqlite::Error::InvalidParameterName(e.to_string()))?;
    let object = ReclaimObject {
        id: decode_uuid(&id, "objects.id")?,
        generation: decode_u64(r.get::<_, i64>("generation")?, "objects.generation")?,
        class,
        logical_size: decode_u64(r.get::<_, i64>("logical_size")?, "objects.logical_size")?,
        physical_size: decode_u64(r.get::<_, i64>("physical_size")?, "objects.physical_size")?,
        compressed_size: decode_optional_u64(
            r.get::<_, Option<i64>>("compressed_size")?,
            "objects.compressed_size",
        )?,
        created_at_ms: r.get("created_at_ms")?,
        last_access_ms: r.get("last_access_ms")?,
        access_count: decode_u64(r.get::<_, i64>("access_count")?, "objects.access_count")?,
        reuse_probability: r.get("reuse_probability")?,
        reuse_horizon_secs: decode_optional_u64(
            r.get::<_, Option<i64>>("reuse_horizon_secs")?,
            "objects.reuse_horizon_secs",
        )?,
        recompute_cost: r.get("recompute_cost")?,
        recompute_latency_secs: r.get("recompute_latency_secs")?,
        transfer_cost: r.get("transfer_cost")?,
        migration_cost: r.get("migration_cost")?,
        storage_cost_per_byte_sec: r.get("storage_cost_per_byte_sec")?,
        memory_cost_per_byte_sec: r.get("memory_cost_per_byte_sec")?,
        replication_count: decode_u32(
            r.get::<_, i64>("replication_count")?,
            "objects.replication_count",
        )?,
        durability_class: match durability.as_str() {
            "EPHEMERAL" => DurabilityClass::Ephemeral,
            "RECOMPUTABLE" => DurabilityClass::Recomputable,
            "DURABLE" => DurabilityClass::Durable,
            "CRITICAL" => DurabilityClass::Critical,
            _ => {
                return Err(rusqlite::Error::InvalidParameterName(format!(
                    "bad durability {durability}"
                )))
            }
        },
        survivability_class: match survivability.as_str() {
            "EPHEMERAL" => SurvivabilityClass::Ephemeral,
            "RECOMPUTABLE" => SurvivabilityClass::Recomputable,
            "DURABLE" => SurvivabilityClass::Durable,
            "CRITICAL" => SurvivabilityClass::Critical,
            _ => {
                return Err(rusqlite::Error::InvalidParameterName(format!(
                    "bad survivability {survivability}"
                )))
            }
        },
        owner: r.get("owner")?,
        content_hash: content_hash
            .as_deref()
            .map(|h| decode_hash(h, "objects.content_hash"))
            .transpose()?,
        lifecycle_state: LifecycleState::parse(&lifecycle_state)
            .map_err(|e| rusqlite::Error::InvalidParameterName(e.to_string()))?,
        policy_version: r.get("policy_version")?,
        decision_epoch: decode_u64(r.get::<_, i64>("decision_epoch")?, "objects.decision_epoch")?,
        pinned: decode_bool(r.get::<_, i64>("pinned")?, "objects.pinned")?,
        protected: decode_bool(r.get::<_, i64>("protected")?, "objects.protected")?,
        min_retention_deadline_ms: r.get("min_retention_deadline_ms")?,
        max_retention_deadline_ms: r.get("max_retention_deadline_ms")?,
        app_metadata,
    };
    object.validate().map_err(|e| {
        rusqlite::Error::InvalidParameterName(format!("corrupt object {}: {e}", object.id))
    })?;
    Ok(object)
}

fn row_to_replica(r: &rusqlite::Row<'_>) -> std::result::Result<Replica, rusqlite::Error> {
    let rid: String = r.get("replica_id")?;
    let oid: String = r.get("object_id")?;
    let backend: String = r.get("backend")?;
    let key: String = r.get("key")?;
    let kind: String = r.get("kind")?;
    let hash: String = r.get("content_hash")?;
    let replica = Replica {
        replica_id: decode_uuid(&rid, "replicas.replica_id")?,
        object_id: decode_uuid(&oid, "replicas.object_id")?,
        generation: decode_u64(r.get::<_, i64>("generation")?, "replicas.generation")?,
        location: crate::object::PhysicalLocation {
            backend,
            key,
            kind: match kind.as_str() {
                "HOT" => crate::object::PhysicalKind::Hot,
                "DURABLE" => crate::object::PhysicalKind::Durable,
                "ARCHIVED" => crate::object::PhysicalKind::Archived,
                _ => {
                    return Err(rusqlite::Error::InvalidParameterName(format!(
                        "bad kind {kind}"
                    )))
                }
            },
        },
        size: decode_u64(r.get::<_, i64>("size")?, "replicas.size")?,
        content_hash: decode_hash(&hash, "replicas.content_hash")?,
        created_at_ms: r.get("created_at_ms")?,
        verified_at_ms: r.get("verified_at_ms")?,
        valid: decode_bool(r.get::<_, i64>("valid")?, "replicas.valid")?,
        owner_node: r.get("owner_node")?,
    };
    validate_replica_descriptor(&replica).map_err(|e| {
        rusqlite::Error::InvalidParameterName(format!(
            "corrupt replica {}: {e}",
            replica.replica_id
        ))
    })?;
    Ok(replica)
}

fn row_to_archive(
    row: &rusqlite::Row<'_>,
) -> std::result::Result<Option<ArchiveRecord>, rusqlite::Error> {
    let valid = decode_bool(row.get(8)?, "archives.valid")?;
    if !valid {
        return Ok(None);
    }
    let archive_id: String = row.get(0)?;
    let object_id: String = row.get(1)?;
    let generation: i64 = row.get(2)?;
    let backend: String = row.get(3)?;
    let key: String = row.get(4)?;
    let size: i64 = row.get(5)?;
    let hash: String = row.get(6)?;
    let created_at_ms: i64 = row.get(7)?;
    Ok(Some(ArchiveRecord {
        archive_id,
        object_id: decode_uuid(&object_id, "archives.object_id")?,
        generation: decode_u64(generation, "archives.generation")?,
        backend,
        key,
        size: decode_u64(size, "archives.size")?,
        content_hash: decode_hash(&hash, "archives.content_hash")?,
        created_at_ms,
    }))
}

fn row_to_journal(r: &rusqlite::Row<'_>) -> std::result::Result<JournalEntry, rusqlite::Error> {
    let aid: String = r.get("attempt_id")?;
    let oid: String = r.get("object_id")?;
    let phase: String = r.get("phase")?;
    let payload: String = r.get("payload")?;
    let phase_enum = match phase.as_str() {
        "RESERVED" => JournalPhase::Reserved,
        "VALIDATED" => JournalPhase::Validated,
        "PHYSICAL_STARTED" => JournalPhase::PhysicalStarted,
        "PHYSICAL_DONE" => JournalPhase::PhysicalDone,
        "COMMITTED" => JournalPhase::Committed,
        "ROLLED_BACK" => JournalPhase::RolledBack,
        "FAILED" => JournalPhase::Failed,
        _ => {
            return Err(rusqlite::Error::InvalidParameterName(format!(
                "bad phase {phase}"
            )))
        }
    };
    Ok(JournalEntry {
        attempt_id: decode_uuid(&aid, "journal.attempt_id")?,
        object_id: decode_uuid(&oid, "journal.object_id")?,
        generation: decode_u64(r.get::<_, i64>("generation")?, "journal.generation")?,
        phase: phase_enum,
        created_at_ms: r.get("created_at_ms")?,
        updated_at_ms: r.get("updated_at_ms")?,
        payload: serde_json::from_str(&payload)
            .map_err(|e| rusqlite::Error::InvalidParameterName(e.to_string()))?,
    })
}

fn row_to_audit(r: &rusqlite::Row<'_>) -> std::result::Result<AuditEntry, rusqlite::Error> {
    let id: i64 = r.get("id")?;
    let oid: Option<String> = r.get("object_id")?;
    let gen: Option<i64> = r.get("generation")?;
    let aid: Option<String> = r.get("attempt_id")?;
    let detail: String = r.get("detail")?;
    Ok(AuditEntry {
        id,
        ts_ms: r.get("ts_ms")?,
        actor: r.get("actor")?,
        action: r.get("action")?,
        object_id: decode_optional_uuid(oid, "audit.object_id")?,
        generation: decode_optional_u64(gen, "audit.generation")?,
        prior_state: r.get("prior_state")?,
        new_state: r.get("new_state")?,
        policy: r.get("policy")?,
        attempt_id: decode_optional_uuid(aid, "audit.attempt_id")?,
        node: r.get("node")?,
        detail: serde_json::from_str(&detail)
            .map_err(|e| rusqlite::Error::InvalidParameterName(e.to_string()))?,
    })
}

fn dura_str(d: DurabilityClass) -> &'static str {
    match d {
        DurabilityClass::Ephemeral => "EPHEMERAL",
        DurabilityClass::Recomputable => "RECOMPUTABLE",
        DurabilityClass::Durable => "DURABLE",
        DurabilityClass::Critical => "CRITICAL",
    }
}

fn surv_str(s: SurvivabilityClass) -> &'static str {
    match s {
        SurvivabilityClass::Ephemeral => "EPHEMERAL",
        SurvivabilityClass::Recomputable => "RECOMPUTABLE",
        SurvivabilityClass::Durable => "DURABLE",
        SurvivabilityClass::Critical => "CRITICAL",
    }
}

fn chrono_now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

const SCHEMA_SQL: &str = "
CREATE TABLE IF NOT EXISTS objects (
    id TEXT PRIMARY KEY,
    generation INTEGER NOT NULL DEFAULT 0,
    class TEXT NOT NULL,
    logical_size INTEGER NOT NULL,
    physical_size INTEGER NOT NULL DEFAULT 0,
    compressed_size INTEGER,
    created_at_ms INTEGER NOT NULL,
    last_access_ms INTEGER NOT NULL,
    access_count INTEGER NOT NULL DEFAULT 0,
    reuse_probability REAL NOT NULL DEFAULT 0.0,
    reuse_horizon_secs INTEGER,
    recompute_cost REAL,
    recompute_latency_secs REAL,
    transfer_cost REAL,
    migration_cost REAL,
    storage_cost_per_byte_sec REAL NOT NULL DEFAULT 0.0,
    memory_cost_per_byte_sec REAL NOT NULL DEFAULT 0.0,
    replication_count INTEGER NOT NULL DEFAULT 0,
    durability_class TEXT NOT NULL DEFAULT 'EPHEMERAL',
    survivability_class TEXT NOT NULL DEFAULT 'EPHEMERAL',
    owner TEXT NOT NULL DEFAULT '',
    content_hash TEXT,
    lifecycle_state TEXT NOT NULL,
    policy_version TEXT NOT NULL DEFAULT '',
    decision_epoch INTEGER NOT NULL DEFAULT 0,
    pinned INTEGER NOT NULL DEFAULT 0,
    protected INTEGER NOT NULL DEFAULT 0,
    min_retention_deadline_ms INTEGER,
    max_retention_deadline_ms INTEGER,
    app_metadata TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS replicas (
    replica_id TEXT PRIMARY KEY,
    object_id TEXT NOT NULL,
    generation INTEGER NOT NULL,
    backend TEXT NOT NULL,
    key TEXT NOT NULL,
    kind TEXT NOT NULL,
    size INTEGER NOT NULL,
    content_hash TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    verified_at_ms INTEGER,
    valid INTEGER NOT NULL DEFAULT 1,
    owner_node TEXT
);
CREATE INDEX IF NOT EXISTS idx_replicas_object ON replicas(object_id);

CREATE TABLE IF NOT EXISTS lineage (
    parent_id TEXT NOT NULL,
    child_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    PRIMARY KEY (parent_id, child_id, kind)
);
CREATE INDEX IF NOT EXISTS idx_lineage_child ON lineage(child_id);

CREATE TABLE IF NOT EXISTS dedup (
    content_hash TEXT NOT NULL,
    backend TEXT NOT NULL,
    storage_key TEXT NOT NULL,
    ref_count INTEGER NOT NULL,
    payload_size INTEGER NOT NULL,
    PRIMARY KEY (content_hash, backend)
);

CREATE TABLE IF NOT EXISTS decisions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    object_id TEXT NOT NULL,
    generation INTEGER NOT NULL,
    verdict TEXT NOT NULL,
    score REAL NOT NULL,
    threshold REAL NOT NULL,
    policy_id TEXT NOT NULL,
    policy_version TEXT NOT NULL,
    epoch INTEGER NOT NULL,
    components_json TEXT NOT NULL,
    reasons_json TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_decisions_object ON decisions(object_id);

CREATE TABLE IF NOT EXISTS attempts (
    attempt_id TEXT PRIMARY KEY,
    object_id TEXT NOT NULL,
    generation INTEGER NOT NULL,
    node TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    status TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_attempts_object ON attempts(object_id);

CREATE TABLE IF NOT EXISTS reservations (
    reservation_id TEXT PRIMARY KEY,
    attempt_id TEXT NOT NULL,
    object_id TEXT NOT NULL,
    generation INTEGER NOT NULL,
    node TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    expires_at_ms INTEGER NOT NULL,
    status TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_reservations_object ON reservations(object_id);
CREATE INDEX IF NOT EXISTS idx_reservations_status ON reservations(status);

CREATE TABLE IF NOT EXISTS coordinator (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    epoch INTEGER NOT NULL,
    process_id TEXT NOT NULL,
    boot_id TEXT NOT NULL,
    last_seen_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS archives (
    archive_id TEXT PRIMARY KEY,
    object_id TEXT NOT NULL,
    generation INTEGER NOT NULL,
    backend TEXT NOT NULL,
    key TEXT NOT NULL,
    size INTEGER NOT NULL,
    content_hash TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    valid INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS idx_archives_object ON archives(object_id);

CREATE TABLE IF NOT EXISTS failures (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_ms INTEGER NOT NULL,
    object_id TEXT,
    attempt_id TEXT,
    kind TEXT NOT NULL,
    message TEXT NOT NULL,
    recovered INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS audit (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_ms INTEGER NOT NULL,
    actor TEXT NOT NULL,
    action TEXT NOT NULL,
    object_id TEXT,
    generation INTEGER,
    prior_state TEXT,
    new_state TEXT,
    policy TEXT,
    attempt_id TEXT,
    node TEXT,
    detail TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_audit_object ON audit(object_id);
CREATE INDEX IF NOT EXISTS idx_audit_action ON audit(action);

CREATE TABLE IF NOT EXISTS journal (
    attempt_id TEXT PRIMARY KEY,
    object_id TEXT NOT NULL,
    generation INTEGER NOT NULL,
    phase TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    payload TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_journal_phase ON journal(phase);

CREATE TABLE IF NOT EXISTS policies (
    id TEXT NOT NULL,
    version TEXT NOT NULL,
    policy_json TEXT NOT NULL,
    PRIMARY KEY (id, version)
);
";

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object::PhysicalKind;

    fn obj(id: Uuid) -> ReclaimObject {
        let mut o = ReclaimObject::new(id, 3, "checkpoint", 4096, 1000);
        o.lifecycle_state = LifecycleState::Hot;
        o.content_hash = Some(ContentHash::of(b"payload"));
        o
    }

    fn replica(_id: Uuid, oid: Uuid) -> Replica {
        Replica {
            replica_id: Uuid::new_v4(),
            object_id: oid,
            generation: 3,
            location: crate::object::PhysicalLocation {
                backend: "memory".into(),
                key: "k1".into(),
                kind: PhysicalKind::Hot,
            },
            size: 4096,
            content_hash: ContentHash::of(b"payload"),
            created_at_ms: 1000,
            verified_at_ms: None,
            valid: true,
            owner_node: None,
        }
    }

    #[test]
    fn object_roundtrip() {
        let store = Store::open_in_memory().unwrap();
        let id = Uuid::new_v4();
        store.create_object(&obj(id)).unwrap();
        let back = store.require_object(&id).unwrap();
        assert_eq!(back.id, id);
        assert_eq!(back.generation, 3);
        assert_eq!(back.lifecycle_state, LifecycleState::Hot);
        assert_eq!(back.content_hash, Some(ContentHash::of(b"payload")));
        assert!(store.get_object(&Uuid::new_v4()).unwrap().is_none());
    }

    #[test]
    fn update_and_count() {
        let store = Store::open_in_memory().unwrap();
        let id = Uuid::new_v4();
        let mut o = obj(id);
        store.create_object(&o).unwrap();
        o.access_count = 42;
        o.lifecycle_state = LifecycleState::Warm;
        store.update_object(&o).unwrap();
        let back = store.require_object(&id).unwrap();
        assert_eq!(back.access_count, 42);
        assert_eq!(back.lifecycle_state, LifecycleState::Warm);
        assert_eq!(store.object_count().unwrap(), 1);
    }

    #[test]
    fn replicas_roundtrip() {
        let store = Store::open_in_memory().unwrap();
        let id = Uuid::new_v4();
        store.create_object(&obj(id)).unwrap();
        store.add_replica(&replica(Uuid::new_v4(), id)).unwrap();
        store.add_replica(&replica(Uuid::new_v4(), id)).unwrap();
        assert_eq!(store.replica_count(&id).unwrap(), 2);
        assert_eq!(store.valid_replica_count(&id).unwrap(), 2);
        assert_eq!(store.replicas_for(&id).unwrap().len(), 2);
        let r = store.replicas_for(&id).unwrap().remove(0);
        store.delete_replica(&r.replica_id).unwrap();
        assert_eq!(store.replica_count(&id).unwrap(), 1);
    }

    #[test]
    fn lineage_roundtrip() {
        let store = Store::open_in_memory().unwrap();
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        store.create_object(&obj(a)).unwrap();
        store.create_object(&obj(b)).unwrap();
        store.add_lineage_edge(a, b, EdgeKind::DependsOn).unwrap();
        let g = store.lineage_graph().unwrap();
        assert!(g
            .dependency_safe(a, &|_| true, &std::collections::HashSet::new())
            .is_ok());
        assert!(g
            .dependency_safe(a, &|_| false, &std::collections::HashSet::new())
            .is_err());
        store
            .remove_lineage_edge(a, b, EdgeKind::DependsOn)
            .unwrap();
        assert_eq!(store.lineage_graph().unwrap().edge_count(), 0);
    }

    #[test]
    fn dedup_roundtrip() {
        let store = Store::open_in_memory().unwrap();
        let h = ContentHash::of(b"shared");
        store
            .insert_dedup(&DedupEntry {
                content_hash: h,
                backend: "mem".into(),
                key: "k".into(),
                ref_count: 2,
                payload_size: 6,
            })
            .unwrap();
        store.dedup_acquire(&h, "mem").unwrap();
        assert_eq!(store.get_dedup(&h, "mem").unwrap().unwrap().ref_count, 3);
        assert!(!store.dedup_release(&h, "mem").unwrap());
        assert!(!store.dedup_release(&h, "mem").unwrap());
        assert!(store.dedup_release(&h, "mem").unwrap());
        assert!(store.get_dedup(&h, "mem").unwrap().is_none());
    }

    #[test]
    fn audit_append_replay() {
        let store = Store::open_in_memory().unwrap();
        let id = Uuid::new_v4();
        store.create_object(&obj(id)).unwrap();
        let e = AuditEntry {
            id: 0,
            ts_ms: 100,
            actor: "test".into(),
            action: "RECLAIM".into(),
            object_id: Some(id),
            generation: Some(3),
            prior_state: Some("HOT".into()),
            new_state: Some("RECLAIMED".into()),
            policy: Some("reclaim-default-v1".into()),
            attempt_id: None,
            node: Some("n1".into()),
            detail: serde_json::json!({"score": 1.5}),
        };
        store.append_audit(&e).unwrap();
        let all = store.replay_audit(None, None, 10).unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].action, "RECLAIM");
        assert_eq!(all[0].detail["score"], 1.5);
        let filtered = store.replay_audit(Some(&id), Some("RECLAIM"), 10).unwrap();
        assert_eq!(filtered.len(), 1);
        let none = store.replay_audit(Some(&Uuid::new_v4()), None, 10).unwrap();
        assert_eq!(none.len(), 0);
    }

    #[test]
    fn atomic_object_audit_rejects_mismatched_state_without_partial_write() {
        let store = Store::open_in_memory().unwrap();
        let object = obj(Uuid::new_v4());
        let misleading = AuditEntry {
            id: 0,
            ts_ms: 1,
            actor: "test".into(),
            action: "OBJECT_CREATED".into(),
            object_id: Some(object.id),
            generation: Some(object.generation),
            prior_state: None,
            new_state: Some(LifecycleState::Reclaimed.as_str().into()),
            policy: None,
            attempt_id: None,
            node: None,
            detail: serde_json::json!({}),
        };
        assert!(matches!(
            store.create_object_with_audit(&object, &misleading),
            Err(ReclaimError::InvalidArgument(_))
        ));
        assert!(store.get_object(&object.id).unwrap().is_none());
        assert_eq!(store.audit_count().unwrap(), 0);
    }

    #[test]
    fn staged_object_rollback_deletes_row_but_preserves_audit_history() {
        let store = Store::open_in_memory().unwrap();
        let mut object = obj(Uuid::new_v4());
        object.lifecycle_state = LifecycleState::Created;
        let created = AuditEntry {
            id: 0,
            ts_ms: 1,
            actor: "test".into(),
            action: "OBJECT_CREATED".into(),
            object_id: Some(object.id),
            generation: Some(object.generation),
            prior_state: None,
            new_state: Some(LifecycleState::Created.as_str().into()),
            policy: None,
            attempt_id: None,
            node: None,
            detail: serde_json::json!({"creation_pending": true}),
        };
        store.create_object_with_audit(&object, &created).unwrap();
        let rolled_back = AuditEntry {
            id: 0,
            ts_ms: 2,
            actor: "test".into(),
            action: "OBJECT_CREATION_ROLLED_BACK".into(),
            object_id: Some(object.id),
            generation: Some(object.generation),
            prior_state: Some(LifecycleState::Created.as_str().into()),
            new_state: None,
            policy: None,
            attempt_id: None,
            node: None,
            detail: serde_json::json!({"reason": "injected failure"}),
        };
        store
            .delete_object_with_audit(&object.id, &rolled_back)
            .unwrap();
        assert!(store.get_object(&object.id).unwrap().is_none());
        let audit = store.replay_audit(Some(&object.id), None, 10).unwrap();
        assert_eq!(audit.len(), 2);
        assert_eq!(audit[0].action, "OBJECT_CREATION_ROLLED_BACK");
        assert_eq!(audit[1].action, "OBJECT_CREATED");
    }

    #[test]
    fn attempt_updates_reject_missing_stale_and_conflicting_rows() {
        let store = Store::open_in_memory().unwrap();
        let attempt_id = Uuid::new_v4();
        assert!(matches!(
            store.update_attempt(&attempt_id, AttemptStatus::Committed, 2),
            Err(ReclaimError::ReservationConflict(_))
        ));
        store
            .create_attempt(&Attempt {
                attempt_id,
                object_id: Uuid::new_v4(),
                generation: 0,
                node: "test".into(),
                created_at_ms: 1,
                updated_at_ms: 1,
                status: AttemptStatus::Open,
            })
            .unwrap();
        store
            .update_attempt(&attempt_id, AttemptStatus::Committed, 2)
            .unwrap();
        assert!(matches!(
            store.update_attempt(&attempt_id, AttemptStatus::Failed, 3),
            Err(ReclaimError::ReservationConflict(_))
        ));
        store
            .reconcile_attempt(&attempt_id, AttemptStatus::Committed, 4)
            .unwrap();
        assert!(matches!(
            store.reconcile_attempt(&attempt_id, AttemptStatus::Failed, 5),
            Err(ReclaimError::ReservationConflict(_))
        ));
    }

    #[test]
    fn coordinator_claim_epochs() {
        let store = Store::open_in_memory().unwrap();
        let boot = Uuid::new_v4();
        let e1 = store
            .claim_coordinator("proc-a", &boot, 1000, 10_000)
            .unwrap();
        assert_eq!(e1, 1);
        store
            .coordinator_heartbeat("proc-a", &boot, e1, 2000)
            .unwrap();
        // A wall-clock rollback must not shorten the live lease.
        store
            .coordinator_heartbeat("proc-a", &boot, e1, 500)
            .unwrap();
        assert!(store
            .claim_coordinator("rollback-thief", &Uuid::new_v4(), 10_501, 10_000)
            .is_err());
        // A fresh lease cannot be stolen even by a caller reusing the same
        // process id. Boot identity and lease freshness both matter.
        assert!(store
            .claim_coordinator("proc-a", &boot, 2000, 10_000)
            .is_err());
        assert!(store
            .coordinator_heartbeat("proc-a", &Uuid::new_v4(), e1, 2001)
            .is_err());
        // Different live process rejected.
        assert!(store
            .claim_coordinator("proc-b", &Uuid::new_v4(), 3000, 10_000)
            .is_err());
        // Stale holder can be taken over.
        let e2 = store
            .claim_coordinator("proc-b", &Uuid::new_v4(), 100_000, 10_000)
            .unwrap();
        assert_eq!(e2, 2);
        // The superseded identity cannot heartbeat or release the new lease.
        assert!(store
            .coordinator_heartbeat("proc-a", &boot, e1, 100_001)
            .is_err());
        assert!(store.release_coordinator("proc-a", &boot, e1).is_err());
    }

    #[test]
    fn journal_phases() {
        let store = Store::open_in_memory().unwrap();
        let aid = Uuid::new_v4();
        let oid = Uuid::new_v4();
        let entry = JournalEntry {
            attempt_id: aid,
            object_id: oid,
            generation: 0,
            phase: JournalPhase::Reserved,
            created_at_ms: 1,
            updated_at_ms: 1,
            payload: serde_json::json!({"physical_replica_deletions": null}),
        };
        store.insert_journal(&entry).unwrap();
        assert!(store
            .update_journal_phase(&aid, JournalPhase::PhysicalDone, 2)
            .is_err());
        store
            .update_journal_phase(&aid, JournalPhase::Validated, 2)
            .unwrap();
        let planned = serde_json::json!({"physical_replica_deletions": []});
        store.start_journal_physical(&aid, &planned, 3).unwrap();
        assert!(store
            .update_journal_phase(&aid, JournalPhase::Validated, 4)
            .is_err());
        store
            .update_journal_phase(&aid, JournalPhase::PhysicalDone, 5)
            .unwrap();
        let open = store.list_open_journal().unwrap();
        assert_eq!(open.len(), 1);
        assert_eq!(open[0].phase, JournalPhase::PhysicalDone);
        store
            .update_journal_phase(&aid, JournalPhase::Committed, 6)
            .unwrap();
        assert!(store.list_open_journal().unwrap().is_empty());
        assert!(store
            .update_journal_phase(&aid, JournalPhase::RolledBack, 7)
            .is_err());
        assert_eq!(
            store.get_journal(&aid).unwrap().unwrap().phase,
            JournalPhase::Committed
        );
    }

    #[test]
    fn physical_plan_and_phase_are_published_together_once() {
        let store = Store::open_in_memory().unwrap();
        let aid = Uuid::new_v4();
        let oid = Uuid::new_v4();
        let initial = serde_json::json!({
            "prior_object": {"id": oid},
            "replica_deletions": [],
            "physical_replica_deletions": null,
            "archive_deletions": [],
        });
        store
            .insert_journal(&JournalEntry {
                attempt_id: aid,
                object_id: oid,
                generation: 0,
                phase: JournalPhase::Validated,
                created_at_ms: 1,
                updated_at_ms: 1,
                payload: initial.clone(),
            })
            .unwrap();

        let invalid = store.start_journal_physical(&aid, &initial, 2);
        assert!(matches!(invalid, Err(ReclaimError::InvalidArgument(_))));
        let unchanged = store.get_journal(&aid).unwrap().unwrap();
        assert_eq!(unchanged.phase, JournalPhase::Validated);
        assert!(unchanged.payload["physical_replica_deletions"].is_null());

        let mut planned = initial;
        planned["physical_replica_deletions"] = serde_json::json!([]);
        store.start_journal_physical(&aid, &planned, 3).unwrap();
        let started = store.get_journal(&aid).unwrap().unwrap();
        assert_eq!(started.phase, JournalPhase::PhysicalStarted);
        assert_eq!(started.updated_at_ms, 3);
        assert_eq!(started.payload, planned);

        assert!(matches!(
            store.start_journal_physical(&aid, &planned, 4),
            Err(ReclaimError::ReservationConflict(_))
        ));
        let still_started = store.get_journal(&aid).unwrap().unwrap();
        assert_eq!(still_started.phase, JournalPhase::PhysicalStarted);
        assert_eq!(still_started.updated_at_ms, 3);
    }

    #[test]
    fn reclaim_reservation_records_are_all_or_none_on_constraint_failure() {
        let store = Store::open_in_memory().unwrap();
        let attempt_id = Uuid::new_v4();
        let object_id = Uuid::new_v4();
        // Force the final insert in the batch to collide, after the attempt
        // and reservation INSERT statements have executed in the transaction.
        store
            .insert_journal(&JournalEntry {
                attempt_id,
                object_id,
                generation: 0,
                phase: JournalPhase::Reserved,
                created_at_ms: 1,
                updated_at_ms: 1,
                payload: serde_json::json!({"physical_replica_deletions": null}),
            })
            .unwrap();
        let attempt = Attempt {
            attempt_id,
            object_id,
            generation: 0,
            node: "test".into(),
            created_at_ms: 2,
            updated_at_ms: 2,
            status: AttemptStatus::Open,
        };
        let reservation = Reservation {
            reservation_id: Uuid::new_v4(),
            attempt_id,
            object_id,
            generation: 0,
            node: "test".into(),
            created_at_ms: 2,
            expires_at_ms: 100,
            status: "OPEN".into(),
        };
        let colliding = JournalEntry {
            attempt_id,
            object_id,
            generation: 0,
            phase: JournalPhase::Reserved,
            created_at_ms: 2,
            updated_at_ms: 2,
            payload: serde_json::json!({"physical_replica_deletions": null}),
        };
        assert!(store
            .create_reclaim_reservation(&colliding, &reservation, &attempt)
            .is_err());
        assert!(store.get_attempt(&attempt_id).unwrap().is_none());
        assert!(store.list_open_reservations().unwrap().is_empty());
        let original = store.get_journal(&attempt_id).unwrap().unwrap();
        assert_eq!(original.created_at_ms, 1);
    }

    #[test]
    fn schema_version_mismatch_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store.db");
        {
            let store = Store::open(path.to_str().unwrap()).unwrap();
            store.create_object(&obj(Uuid::new_v4())).unwrap();
        }
        // Bump the version to simulate a future binary.
        let conn = Connection::open(&path).unwrap();
        conn.pragma_update(None, "user_version", 99).unwrap();
        drop(conn);
        let err = Store::open(path.to_str().unwrap()).unwrap_err();
        assert!(matches!(err, ReclaimError::Persistence(_)));
    }

    #[test]
    fn empty_store_path_hidden_temporary_fallback_is_rejected() {
        assert!(matches!(
            Store::open(""),
            Err(ReclaimError::InvalidArgument(_))
        ));
        assert!(matches!(
            Store::open("   "),
            Err(ReclaimError::InvalidArgument(_))
        ));
        Store::open(":memory:").unwrap();
    }

    #[test]
    fn versioned_partial_schema_is_rejected_at_open() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("forged-v1.db");
        let conn = Connection::open(&path).unwrap();
        conn.execute("CREATE TABLE objects (id TEXT PRIMARY KEY)", [])
            .unwrap();
        conn.pragma_update(None, "user_version", SCHEMA_VERSION)
            .unwrap();
        drop(conn);
        assert!(matches!(
            Store::open(path.to_str().unwrap()),
            Err(ReclaimError::Persistence(_))
        ));
    }

    #[test]
    fn versioned_schema_with_missing_primary_key_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("forged-constraint.db");
        drop(Store::open(path.to_str().unwrap()).unwrap());
        let conn = Connection::open(&path).unwrap();
        conn.pragma_update(None, "writable_schema", true).unwrap();
        let changed = conn
            .execute(
                "UPDATE sqlite_schema
                 SET sql=replace(sql, 'id TEXT PRIMARY KEY', 'id TEXT')
                 WHERE type='table' AND name='objects'",
                [],
            )
            .unwrap();
        assert_eq!(changed, 1);
        conn.pragma_update(None, "writable_schema", false).unwrap();
        drop(conn);
        assert!(matches!(
            Store::open(path.to_str().unwrap()),
            Err(ReclaimError::Persistence(_))
        ));
    }

    #[test]
    fn unversioned_preexisting_schema_is_not_silently_blessed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("partial.db");
        let conn = Connection::open(&path).unwrap();
        conn.execute(
            "CREATE TABLE objects (id TEXT PRIMARY KEY, incompatible TEXT NOT NULL)",
            [],
        )
        .unwrap();
        drop(conn);

        let err = Store::open(path.to_str().unwrap()).unwrap_err();
        assert!(matches!(err, ReclaimError::Persistence(_)));
        let conn = Connection::open(&path).unwrap();
        assert_eq!(
            conn.query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            0
        );
        let columns: Vec<String> = conn
            .prepare("PRAGMA table_info(objects)")
            .unwrap()
            .query_map([], |r| r.get(1))
            .unwrap()
            .collect::<std::result::Result<_, _>>()
            .unwrap();
        assert_eq!(columns, vec!["id", "incompatible"]);
    }

    #[test]
    fn store_persists_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store.db");
        let id = Uuid::new_v4();
        {
            let store = Store::open(path.to_str().unwrap()).unwrap();
            store.create_object(&obj(id)).unwrap();
        }
        let store = Store::open(path.to_str().unwrap()).unwrap();
        assert!(store.require_object(&id).is_ok());
        assert_eq!(store.audit_count().unwrap(), 0);
    }

    #[test]
    fn policy_persistence() {
        let store = Store::open_in_memory().unwrap();
        store
            .upsert_policy(&crate::policy::default_policy())
            .unwrap();
        let policies = store.list_policies().unwrap();
        assert_eq!(policies.len(), 1);
        assert_eq!(policies[0].id, "reclaim-default");
        // Upsert is idempotent by (id, version).
        store
            .upsert_policy(&crate::policy::default_policy())
            .unwrap();
        assert_eq!(store.list_policies().unwrap().len(), 1);
    }

    #[test]
    fn failures_recorded() {
        let store = Store::open_in_memory().unwrap();
        let oid = Uuid::new_v4();
        store
            .insert_failure(&FailureRecord {
                id: 0,
                ts_ms: 1,
                object_id: Some(oid),
                attempt_id: None,
                kind: "INTEGRITY_FAILURE".into(),
                message: "hash mismatch".into(),
                recovered: false,
            })
            .unwrap();
        let failures = store.list_failures(10).unwrap();
        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].object_id, Some(oid));
    }

    #[test]
    fn unsigned_values_outside_sqlite_range_are_rejected() {
        let store = Store::open_in_memory().unwrap();
        let id = Uuid::new_v4();
        let mut invalid = obj(id);
        invalid.generation = u64::MAX;
        assert!(matches!(
            store.create_object(&invalid),
            Err(ReclaimError::InvalidArgument(_))
        ));
        assert_eq!(store.object_count().unwrap(), 0);

        let hash = ContentHash::of(b"too-many-refs");
        assert!(store
            .insert_dedup(&DedupEntry {
                content_hash: hash,
                backend: "memory".into(),
                key: "k".into(),
                ref_count: u64::MAX,
                payload_size: 1,
            })
            .is_err());
        assert!(store.get_dedup(&hash, "memory").unwrap().is_none());
    }

    #[test]
    fn corrupt_negative_and_noncanonical_values_fail_closed() {
        let store = Store::open_in_memory().unwrap();
        let id = Uuid::new_v4();
        store.create_object(&obj(id)).unwrap();
        {
            let conn = store.conn.lock().unwrap();
            conn.execute(
                "UPDATE objects SET logical_size=-1, content_hash=?2 WHERE id=?1",
                rusqlite::params![id.to_string(), "AA".repeat(32)],
            )
            .unwrap();
        }
        let error = store.require_object(&id).unwrap_err();
        assert!(matches!(error, ReclaimError::Persistence(_)));
    }

    #[test]
    fn oversized_query_limits_are_rejected_not_treated_as_unbounded() {
        let store = Store::open_in_memory().unwrap();
        assert!(matches!(
            store.replay_audit(None, None, u64::MAX),
            Err(ReclaimError::InvalidArgument(_))
        ));
        assert!(matches!(
            store.list_failures(u64::MAX),
            Err(ReclaimError::InvalidArgument(_))
        ));
    }

    #[test]
    fn corrupt_optional_ids_and_booleans_are_not_silently_coerced() {
        let store = Store::open_in_memory().unwrap();
        {
            let conn = store.conn.lock().unwrap();
            conn.execute(
                "INSERT INTO failures (ts_ms, object_id, attempt_id, kind, message, recovered)
                 VALUES (1, 'not-a-uuid', NULL, 'TEST', 'bad', 2)",
                [],
            )
            .unwrap();
        }
        assert!(matches!(
            store.list_failures(1),
            Err(ReclaimError::Persistence(_))
        ));
    }

    #[test]
    fn concurrent_dedup_release_across_connections_never_leaves_zero_row() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dedup-race.db");
        let path = path.to_str().unwrap();
        let first = Store::open(path).unwrap();
        let second = Store::open(path).unwrap();

        for iteration in 0..32u64 {
            let hash = ContentHash::of(&iteration.to_le_bytes());
            first
                .insert_dedup(&DedupEntry {
                    content_hash: hash,
                    backend: "memory".into(),
                    key: format!("k-{iteration}"),
                    ref_count: 2,
                    payload_size: 8,
                })
                .unwrap();
            let (left, right) = std::thread::scope(|scope| {
                let left = scope.spawn(|| first.dedup_release(&hash, "memory"));
                let right = scope.spawn(|| second.dedup_release(&hash, "memory"));
                (
                    left.join().unwrap().unwrap(),
                    right.join().unwrap().unwrap(),
                )
            });
            assert_ne!(left, right, "exactly one releaser owns deletion");
            assert!(first.get_dedup(&hash, "memory").unwrap().is_none());
        }
    }

    #[test]
    fn archive_order_has_stable_tie_breaker() {
        let store = Store::open_in_memory().unwrap();
        let object_id = Uuid::new_v4();
        store.create_object(&obj(object_id)).unwrap();
        for archive_id in ["archive-b", "archive-a"] {
            store
                .insert_archive(&ArchiveRecord {
                    archive_id: archive_id.into(),
                    object_id,
                    generation: 3,
                    backend: "local".into(),
                    key: archive_id.into(),
                    size: 1,
                    content_hash: ContentHash::of(archive_id.as_bytes()),
                    created_at_ms: 100,
                })
                .unwrap();
        }
        let ids: Vec<_> = store
            .archives_for(&object_id)
            .unwrap()
            .into_iter()
            .map(|archive| archive.archive_id)
            .collect();
        assert_eq!(ids, ["archive-a", "archive-b"]);
    }
}
