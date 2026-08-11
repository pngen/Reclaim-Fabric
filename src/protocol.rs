//! Wire protocol method names and shared payload structures between
//! coordinator, nodes, and CLI clients.

use serde::{Deserialize, Serialize};

/// Transport method names (control plane, JSON envelopes inside frames).
pub mod method {
    // Coordinator <-> CLI
    pub const CREATE_OBJECT: &str = "object.create";
    pub const INSPECT_OBJECT: &str = "object.inspect";
    pub const TOUCH_OBJECT: &str = "object.touch";
    pub const PIN_OBJECT: &str = "object.pin";
    pub const UNPIN_OBJECT: &str = "object.unpin";
    pub const SET_PROTECTED: &str = "object.protect";
    pub const OBJECT_LINEAGE: &str = "object.lineage";
    pub const OBJECT_DEPENDENCIES: &str = "object.dependencies";
    pub const ADD_LINEAGE_EDGE: &str = "lineage.add";
    pub const REMOVE_LINEAGE_EDGE: &str = "lineage.remove";
    pub const PLAN_OBJECT: &str = "plan.object";
    pub const PLAN_CANDIDATES: &str = "plan.candidates";
    pub const RECLAIM_OBJECT: &str = "reclaim.object";
    pub const COMPRESS_OBJECT: &str = "compress.object";
    pub const ARCHIVE_OBJECT: &str = "archive.object";
    pub const RESTORE_OBJECT: &str = "restore.object";
    pub const VERIFY_OBJECT: &str = "verify.object";
    pub const GET_PRESSURE: &str = "pressure.get";
    pub const SET_PRESSURE: &str = "pressure.set";
    pub const POLICY_LIST: &str = "policy.list";
    pub const POLICY_GET: &str = "policy.get";
    pub const POLICY_ADD: &str = "policy.add";
    pub const AUDIT_QUERY: &str = "audit.query";
    pub const FAILURES_LIST: &str = "failures.list";
    pub const STATS: &str = "stats";
    pub const RECOVER: &str = "recover";
    pub const SHUTDOWN: &str = "shutdown";

    // Node <-> Coordinator
    pub const NODE_REGISTER: &str = "node.register";
    pub const NODE_HEARTBEAT: &str = "node.heartbeat";
    pub const NODE_LIST: &str = "node.list";
    pub const NODE_EXECUTE_RECLAIM: &str = "node.execute.reclaim";
    pub const NODE_EXECUTE_COMPRESS: &str = "node.execute.compress";
    pub const NODE_EXECUTE_ARCHIVE: &str = "node.execute.archive";
    pub const NODE_EXECUTE_RESTORE: &str = "node.execute.restore";
    pub const NODE_EXECUTE_VERIFY: &str = "node.execute.verify";
    pub const NODE_EXECUTE_STORE: &str = "node.execute.store";
    pub const NODE_EXECUTE_DELETE: &str = "node.execute.delete";
    pub const NODE_EXECUTE_READ: &str = "node.execute.read";
    pub const NODE_EXECUTE_EXISTS: &str = "node.execute.exists";
    pub const NODE_REPORT_PRESSURE: &str = "node.report.pressure";
}

/// Replication/tier kind strings shared over the wire.
pub mod kind {
    pub const HOT: &str = "HOT";
    pub const DURABLE: &str = "DURABLE";
    pub const ARCHIVED: &str = "ARCHIVED";
}

/// Node registration request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeRegisterRequest {
    pub name: String,
    pub process_id: String,
    pub boot_id: uuid::Uuid,
    /// Node's own listen address for coordinator-initiated operations.
    pub addr: String,
    pub backends: Vec<BackendDescriptor>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackendDescriptor {
    pub id: String,
    pub kind: String,
    /// Optional keying info (e.g. backend file path) for diagnostics.
    pub location: Option<String>,
}

/// Node registration reply.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeRegisterReply {
    pub node_id: String,
    pub coordinator_epoch: u64,
    pub heartbeat_interval_ms: u64,
    pub heartbeat_timeout_ms: u64,
}

/// Create object request (CLI/client or node).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateObjectRequest {
    pub object: crate::object::ReclaimObject,
    /// Base64 payload to store on the target backend (optional).
    #[serde(default)]
    pub payload_b64: Option<String>,
    /// Backend id on which to store the payload (required with payload).
    #[serde(default)]
    pub target_backend: Option<String>,
    /// Optional: replicate the payload to an additional backend.
    #[serde(default)]
    pub replicate_to: Vec<String>,
}

/// Reclaim request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReclaimRequest {
    pub object_id: uuid::Uuid,
    pub actor: String,
    #[serde(default)]
    pub force: bool,
}

/// Object operation request used by node execute commands.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeOperationRequest {
    pub object_id: uuid::Uuid,
    pub generation: u64,
    pub replica_id: uuid::Uuid,
    pub attempt_id: Option<uuid::Uuid>,
    pub coordinator_epoch: u64,
    pub backend: String,
    pub key: String,
    /// Payload for store operations (base64).
    #[serde(default)]
    pub payload_b64: Option<String>,
    /// Expected content hash for verify/store.
    #[serde(default)]
    pub expected_hash: Option<String>,
    /// Compression codec for compress operations.
    #[serde(default)]
    pub codec: Option<String>,
}

/// Result of a node physical operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeOperationReply {
    pub ok: bool,
    /// Physical size after the operation (e.g. compressed size).
    #[serde(default)]
    pub size: Option<u64>,
    /// Content hash of resulting payload (base64 of raw hash bytes).
    #[serde(default)]
    pub result_hash: Option<String>,
    /// True if the payload existed (for exists/verify ops).
    #[serde(default)]
    pub existed: bool,
    /// Error details when !ok.
    #[serde(default)]
    pub error: Option<crate::errors::WireError>,
}

/// Pressure report from a node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PressureReportRequest {
    pub node_id: String,
    pub metrics: crate::pressure::PressureMetrics,
}

/// Basic ID+state query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectIdRequest {
    pub object_id: uuid::Uuid,
    pub actor: String,
}

/// Edge manipulation request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineageRequest {
    pub parent: uuid::Uuid,
    pub child: uuid::Uuid,
    pub kind: crate::lineage::EdgeKind,
    pub actor: String,
}

/// Plan request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanRequest {
    pub object_id: uuid::Uuid,
    pub actor: String,
}

/// Candidate listing request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CandidatesRequest {
    #[serde(default = "default_limit")]
    pub limit: u64,
    pub actor: String,
}

fn default_limit() -> u64 {
    100
}

/// Shutdown request. `actor` is audit attribution, not authentication.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShutdownRequest {
    pub actor: String,
    pub reason: String,
}
