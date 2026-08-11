//! Node runtime: registers with the coordinator, reports resources and
//! pressure, hosts physical objects, and executes coordinator-authorized
//! lifecycle operations.
//!
//! Nodes have stable process identity: `name@process-id@boot-id`. Restart
//! produces a new boot id, so restart can never create a phantom node with a
//! stale identity. Connection retirement is handled by coordinator-side
//! heartbeat timeouts.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::json;
use uuid::Uuid;

use crate::backends::BackendRegistry;
use crate::errors::{ReclaimError, Result};
use crate::integrity::ContentHash;
use crate::pressure::PressureMetrics;
use crate::protocol::{
    BackendDescriptor, NodeOperationReply, NodeOperationRequest, NodeRegisterReply,
    NodeRegisterRequest,
};
use crate::transport::{Client, Reply, Request, RequestHandler, Server, ServerConfig};

const COORDINATOR_IO_TIMEOUT_MS: u64 = 2_000;
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(20);

/// Node configuration.
#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub name: String,
    pub process_id: String,
    pub coordinator_addr: String,
    /// Address this node listens on for coordinator operations.
    pub bind_addr: String,
    pub heartbeat_interval_ms: u64,
}

impl Default for NodeConfig {
    fn default() -> Self {
        NodeConfig {
            name: format!("node-{}", std::process::id()),
            process_id: format!("node-proc-{}", std::process::id()),
            coordinator_addr: "127.0.0.1:7910".into(),
            bind_addr: "127.0.0.1:0".into(),
            heartbeat_interval_ms: 2_000,
        }
    }
}

/// A running node.
pub struct Node {
    pub node_id: String,
    config: NodeConfig,
    backends: BackendRegistry,
    /// Last known coordinator epoch (stale coordinator ops are rejected).
    coordinator_epoch: Arc<AtomicU64>,
    server: Arc<Server>,
    shutdown: Arc<AtomicBool>,
    handles: Mutex<Vec<std::thread::JoinHandle<()>>>,
}

impl Node {
    /// Start a node: register with the coordinator and begin serving.
    pub fn start(mut config: NodeConfig, backends: BackendRegistry) -> Result<Node> {
        if config.name.trim().is_empty()
            || config.name.trim() != config.name
            || config.process_id.trim().is_empty()
            || config.process_id.trim() != config.process_id
        {
            return Err(ReclaimError::InvalidArgument(
                "node name and process id must be non-empty and have no surrounding whitespace"
                    .into(),
            ));
        }
        if config.heartbeat_interval_ms == 0 {
            return Err(ReclaimError::InvalidArgument(
                "node heartbeat interval must be greater than zero".into(),
            ));
        }
        let boot_id = Uuid::new_v4();
        let node_id = format!("{}@{}@{}", config.name, config.process_id, boot_id);

        // Namespace backend ids per node so node-hosted backends can never
        // collide with coordinator-local or other nodes' backends.
        let prefixed = BackendRegistry::new();
        for id in backends.ids()? {
            let backend = backends.get(&id)?;
            prefixed.register_as(&format!("{}/{}", config.name, id), backend)?;
        }

        // Bind the operation listener first so registration can advertise a
        // real address.
        let shutdown = Arc::new(AtomicBool::new(false));
        let handler_epoch = Arc::new(AtomicU64::new(u64::MAX));
        let handler = Self::build_handler(prefixed.clone(), handler_epoch.clone());
        let server_config = ServerConfig {
            bind_addr: config.bind_addr.clone(),
            max_connections: 32,
            timeout_ms: 15_000,
            shutdown_poll_ms: 100,
        };
        let server = Arc::new(Server::new(server_config, handler)?);
        let addr = server.local_addr()?;

        // Register with the coordinator.
        let mut descriptors = Vec::new();
        for id in prefixed.ids()? {
            descriptors.push(BackendDescriptor {
                kind: prefixed.get(&id)?.kind().to_string(),
                id,
                location: None,
            });
        }
        let mut client = Client::connect(&config.coordinator_addr, 15_000)?;
        // Resolve once on the synchronous startup path. Background heartbeat
        // reconnects use this numeric peer address, so shutdown is bounded by
        // connect/read/write timeouts rather than an uninterruptible DNS call.
        let coordinator_peer = client.peer_addr()?;
        let reply = client.call(
            crate::protocol::method::NODE_REGISTER,
            serde_json::to_value(NodeRegisterRequest {
                name: config.name.clone(),
                process_id: config.process_id.clone(),
                boot_id,
                addr: addr.clone(),
                backends: descriptors,
            })?,
        )?;
        let id = reply.id;
        let value = reply.into_result(id)?;
        let reg: NodeRegisterReply = serde_json::from_value(value)?;
        if reg.node_id != node_id {
            return Err(ReclaimError::Protocol(format!(
                "coordinator assigned node id {} but expected {node_id}",
                reg.node_id
            )));
        }
        config.coordinator_addr = coordinator_peer;

        let node = Node {
            node_id,
            config,
            backends: prefixed,
            coordinator_epoch: handler_epoch.clone(),
            server,
            shutdown,
            handles: Mutex::new(Vec::new()),
        };
        // Publish the registration epoch so the operation handler can enforce
        // stale-authority rejection from the start.
        handler_epoch.store(reg.coordinator_epoch, Ordering::SeqCst);
        log::info!(
            "node {} listening on {addr}, epoch {}",
            node.node_id,
            reg.coordinator_epoch
        );

        // Heartbeat thread: keep-alive + pressure reporting + re-registration.
        let hb_config = node.config.clone();
        let hb_identity = NodeIdentity {
            node_id: node.node_id.clone(),
            boot_id,
            addr: addr.clone(),
        };
        let hb_shutdown = node.shutdown.clone();
        // The heartbeat path and operation handler must share one fencing
        // epoch. A coordinator restart advances it through heartbeat replies;
        // using a second atomic here would leave the handler permanently
        // rejecting the new coordinator.
        let hb_epoch = node.coordinator_epoch.clone();
        let hb_backends = node.backends.clone();
        let hb_interval = Duration::from_millis(node.config.heartbeat_interval_ms);
        let handle = std::thread::spawn(move || {
            heartbeat_loop(
                hb_identity,
                hb_config,
                hb_backends,
                hb_epoch,
                hb_shutdown,
                hb_interval,
            );
        });
        node.handles
            .lock()
            .map_err(|_| ReclaimError::Internal("node handles poisoned".into()))?
            .push(handle);

        let serve_shutdown = node.shutdown.clone();
        let serve_server = node.server.clone();
        let handle = std::thread::spawn(move || {
            if let Err(e) = serve_server.serve() {
                log::error!("node operation server stopped unexpectedly: {e}");
                serve_shutdown.store(true, Ordering::SeqCst);
            }
        });
        node.handles
            .lock()
            .map_err(|_| ReclaimError::Internal("node handles poisoned".into()))?
            .push(handle);
        Ok(node)
    }

    pub fn coordinator_epoch(&self) -> u64 {
        self.coordinator_epoch.load(Ordering::SeqCst)
    }

    pub fn shutdown_requested(&self) -> bool {
        self.shutdown.load(Ordering::SeqCst)
    }

    /// Address of the node operation listener.
    pub fn local_addr(&self) -> Result<String> {
        self.server.local_addr()
    }

    /// Build the request handler executing coordinator-authorized operations.
    /// `handler_epoch` tracks the last known coordinator epoch (updated by
    /// registration and heartbeats).
    fn build_handler(backends: BackendRegistry, handler_epoch: Arc<AtomicU64>) -> RequestHandler {
        Arc::new(move |req: Request| {
            let op: NodeOperationRequest = match serde_json::from_value(req.payload.clone()) {
                Ok(op) => op,
                Err(e) => {
                    return Reply::err(
                        req.id,
                        ReclaimError::Protocol(format!("bad node operation: {e}")),
                    )
                }
            };
            // Stale-authority rejection: coordinator must speak the current epoch.
            if op.coordinator_epoch != handler_epoch.load(Ordering::SeqCst) {
                return Reply::err(
                    req.id,
                    ReclaimError::StaleEpoch {
                        expected: handler_epoch.load(Ordering::SeqCst),
                        got: op.coordinator_epoch,
                    },
                );
            }
            let kind = match req.method.as_str() {
                crate::protocol::method::NODE_EXECUTE_STORE => OpKind::Store,
                crate::protocol::method::NODE_EXECUTE_READ => OpKind::Read,
                crate::protocol::method::NODE_EXECUTE_DELETE
                | crate::protocol::method::NODE_EXECUTE_RECLAIM => OpKind::Delete,
                crate::protocol::method::NODE_EXECUTE_EXISTS => OpKind::Exists,
                crate::protocol::method::NODE_EXECUTE_VERIFY => OpKind::Verify,
                other => {
                    return Reply::err(
                        req.id,
                        ReclaimError::Protocol(format!("unknown node operation {other}")),
                    )
                }
            };
            let reply = execute_operation(&backends, &op, kind);
            match serde_json::to_value(&reply) {
                Ok(value) => Reply::ok(req.id, value),
                Err(e) => Reply::err(
                    req.id,
                    ReclaimError::Internal(format!(
                        "failed to serialize node operation reply: {e}"
                    )),
                ),
            }
        })
    }

    /// Stop the node cleanly: stop heartbeats, drain the server, join threads.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
        self.server.request_shutdown();
        let deadline = std::time::Instant::now() + SHUTDOWN_TIMEOUT;
        loop {
            let finished = self
                .handles
                .lock()
                .map(|guard| guard.iter().all(|h| h.is_finished()))
                .unwrap_or_else(|poisoned| poisoned.into_inner().iter().all(|h| h.is_finished()));
            if finished || std::time::Instant::now() >= deadline {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let all_finished = self
            .handles
            .lock()
            .map(|guard| guard.iter().all(|h| h.is_finished()))
            .unwrap_or_else(|poisoned| poisoned.into_inner().iter().all(|h| h.is_finished()));
        if !all_finished {
            log::error!("node threads did not stop before the shutdown deadline");
            return;
        }
        let handles = self
            .handles
            .lock()
            .map(|mut guard| std::mem::take(&mut *guard))
            .unwrap_or_else(|poisoned| {
                let mut guard = poisoned.into_inner();
                std::mem::take(&mut *guard)
            });
        for handle in handles {
            let _ = handle.join();
        }
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Execute one coordinator-authorized physical operation against a local
/// backend. Returns a `NodeOperationReply` (never panics on normal failures).
fn execute_operation(
    backends: &BackendRegistry,
    op: &NodeOperationRequest,
    kind: OpKind,
) -> NodeOperationReply {
    let backend = match backends.get(&op.backend) {
        Ok(b) => b,
        Err(e) => {
            return NodeOperationReply {
                ok: false,
                size: None,
                result_hash: None,
                existed: false,
                error: Some(e.into()),
            }
        }
    };
    match kind {
        OpKind::Store => {
            let data = match op.payload_b64.as_deref().map(decode_b64) {
                Some(Ok(d)) => d,
                Some(Err(e)) => {
                    return NodeOperationReply {
                        ok: false,
                        size: None,
                        result_hash: None,
                        existed: false,
                        error: Some(e.into()),
                    }
                }
                None => {
                    return NodeOperationReply {
                        ok: false,
                        size: None,
                        result_hash: None,
                        existed: false,
                        error: Some(ReclaimError::Protocol("store without payload".into()).into()),
                    }
                }
            };
            let store_result = (|| {
                let expected = op
                    .expected_hash
                    .as_deref()
                    .ok_or_else(|| ReclaimError::Protocol("store without expected hash".into()))
                    .and_then(ContentHash::from_hex)?;
                crate::integrity::verify_sha256(&data, &expected)?;
                let size = backend.put(&op.key, &data)?;
                if size != data.len() as u64 {
                    return Err(ReclaimError::Backend(format!(
                        "backend reported storing {size} bytes, expected {}",
                        data.len()
                    )));
                }
                let stored = backend.get(&op.key)?;
                crate::integrity::verify_sha256(&stored, &expected)?;
                Ok((size, encode_b64(&expected.0)))
            })();
            match store_result {
                Ok((size, result_hash)) => NodeOperationReply {
                    ok: true,
                    size: Some(size),
                    result_hash: Some(result_hash),
                    existed: false,
                    error: None,
                },
                Err(e) => {
                    let error = match backend.delete(&op.key) {
                        Ok(()) => e,
                        Err(cleanup) => ReclaimError::Internal(format!(
                            "node store failed ({e}); cleanup also failed ({cleanup})"
                        )),
                    };
                    NodeOperationReply {
                        ok: false,
                        size: None,
                        result_hash: None,
                        existed: false,
                        error: Some(error.into()),
                    }
                }
            }
        }
        OpKind::Read => match backend.get(&op.key) {
            Ok(data) => NodeOperationReply {
                ok: true,
                size: Some(data.len() as u64),
                result_hash: Some(encode_b64(&data)),
                existed: true,
                error: None,
            },
            Err(e) => NodeOperationReply {
                ok: false,
                size: None,
                result_hash: None,
                existed: false,
                error: Some(e.into()),
            },
        },
        OpKind::Delete => match backend.delete(&op.key) {
            Ok(()) => NodeOperationReply {
                ok: true,
                size: None,
                result_hash: None,
                existed: false,
                error: None,
            },
            Err(e) => NodeOperationReply {
                ok: false,
                size: None,
                result_hash: None,
                existed: false,
                error: Some(e.into()),
            },
        },
        OpKind::Exists => match backend.exists(&op.key) {
            Ok(existed) => NodeOperationReply {
                ok: true,
                size: None,
                result_hash: None,
                existed,
                error: None,
            },
            Err(error) => NodeOperationReply {
                ok: false,
                size: None,
                result_hash: None,
                existed: false,
                error: Some(error.into()),
            },
        },
        OpKind::Verify => {
            let expected = op
                .expected_hash
                .as_deref()
                .and_then(|h| ContentHash::from_hex(h).ok());
            let result = match expected {
                Some(hash) => backend.verify(&op.key, &hash),
                None => Err(ReclaimError::Protocol(
                    "verify without expected hash".into(),
                )),
            };
            match result {
                Ok(()) => NodeOperationReply {
                    ok: true,
                    size: None,
                    result_hash: None,
                    existed: true,
                    error: None,
                },
                Err(e) => NodeOperationReply {
                    ok: false,
                    size: None,
                    result_hash: None,
                    existed: true,
                    error: Some(e.into()),
                },
            }
        }
    }
}

enum OpKind {
    Store,
    Read,
    Delete,
    Exists,
    Verify,
}

/// Heartbeat loop: register (if needed), heartbeat, report pressure, sleep.
/// Identity a node carries through heartbeats and re-registration.
#[derive(Debug, Clone)]
struct NodeIdentity {
    node_id: String,
    boot_id: Uuid,
    addr: String,
}

fn heartbeat_loop(
    identity: NodeIdentity,
    config: NodeConfig,
    backends: BackendRegistry,
    epoch: Arc<AtomicU64>,
    shutdown: Arc<AtomicBool>,
    interval: Duration,
) {
    // `Node::start` completed an initial registration before spawning us.
    let mut registered = true;
    while !shutdown.load(Ordering::SeqCst) {
        match run_heartbeat_cycle(&identity, &config, &backends, &epoch, &mut registered) {
            Ok(()) => {}
            Err(e) => {
                log::warn!("node heartbeat failed: {e}");
                // The coordinator may have restarted and forgotten its
                // in-memory node registry. Re-register on the next bounded
                // cycle instead of heartbeating a permanently unknown id.
                registered = false;
            }
        }
        let mut slept = 0u64;
        while slept < interval.as_millis() as u64 && !shutdown.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(100));
            slept += 100;
        }
    }
}

fn run_heartbeat_cycle(
    identity: &NodeIdentity,
    config: &NodeConfig,
    backends: &BackendRegistry,
    epoch: &Arc<AtomicU64>,
    registered: &mut bool,
) -> Result<()> {
    let mut client = Client::connect(&config.coordinator_addr, COORDINATOR_IO_TIMEOUT_MS)?;
    if !*registered {
        let mut descriptors = Vec::new();
        for id in backends.ids()? {
            descriptors.push(BackendDescriptor {
                kind: backends.get(&id)?.kind().to_string(),
                id,
                location: None,
            });
        }
        let reply = client.call(
            crate::protocol::method::NODE_REGISTER,
            serde_json::to_value(NodeRegisterRequest {
                name: config.name.clone(),
                process_id: config.process_id.clone(),
                boot_id: identity.boot_id,
                addr: identity.addr.clone(),
                backends: descriptors,
            })?,
        )?;
        let id = reply.id;
        let value = reply.into_result(id)?;
        let reg: NodeRegisterReply = serde_json::from_value(value)?;
        if reg.node_id != identity.node_id {
            return Err(ReclaimError::Protocol(format!(
                "coordinator assigned node id {} but expected {}",
                reg.node_id, identity.node_id
            )));
        }
        let current_epoch = epoch.load(Ordering::SeqCst);
        if reg.coordinator_epoch < current_epoch {
            return Err(ReclaimError::StaleEpoch {
                expected: current_epoch,
                got: reg.coordinator_epoch,
            });
        }
        epoch.store(reg.coordinator_epoch, Ordering::SeqCst);
        *registered = true;
        log::debug!(
            "node {} (re)registered, epoch {}",
            identity.node_id,
            reg.coordinator_epoch
        );
    }
    let reply = client.call(
        crate::protocol::method::NODE_HEARTBEAT,
        json!({"node_id": identity.node_id, "epoch": epoch.load(Ordering::SeqCst)}),
    )?;
    let id = reply.id;
    let value = reply.into_result(id)?;
    if let Some(e) = value.get("epoch").and_then(|e| e.as_u64()) {
        let current_epoch = epoch.load(Ordering::SeqCst);
        if e < current_epoch {
            return Err(ReclaimError::StaleEpoch {
                expected: current_epoch,
                got: e,
            });
        }
        epoch.store(e, Ordering::SeqCst);
    } else {
        return Err(ReclaimError::Protocol(
            "heartbeat reply did not contain a coordinator epoch".into(),
        ));
    }
    // Report pressure (synthetic: NORMAL by default; real providers plug in).
    let metrics = PressureMetrics::default();
    let _ = client.call(
        crate::protocol::method::NODE_REPORT_PRESSURE,
        json!({"node_id": identity.node_id, "metrics": metrics}),
    )?;
    Ok(())
}

fn decode_b64(s: &str) -> Result<Vec<u8>> {
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine;
    STANDARD
        .decode(s)
        .map_err(|e| ReclaimError::Protocol(format!("bad base64: {e}")))
}

fn encode_b64(data: &[u8]) -> String {
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine;
    STANDARD.encode(data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::MemoryBackend;

    #[test]
    fn inbound_heartbeat_cannot_rewrite_node_authority_epoch() {
        let backends = BackendRegistry::new();
        backends
            .register(Arc::new(MemoryBackend::new("node/memory")))
            .unwrap();
        let epoch = Arc::new(AtomicU64::new(17));
        let handler = Node::build_handler(backends, epoch.clone());

        let reply = handler(Request {
            id: 1,
            method: crate::protocol::method::NODE_HEARTBEAT.into(),
            payload: json!({"epoch": 999}),
        });

        assert!(!reply.ok);
        assert_eq!(reply.error.unwrap().class, "protocol");
        assert_eq!(epoch.load(Ordering::SeqCst), 17);
    }

    #[test]
    fn stale_epoch_is_rejected_before_backend_access() {
        let backends = BackendRegistry::new();
        backends
            .register(Arc::new(MemoryBackend::new("node/memory")))
            .unwrap();
        let epoch = Arc::new(AtomicU64::new(7));
        let handler = Node::build_handler(backends, epoch);
        let op = NodeOperationRequest {
            object_id: Uuid::nil(),
            generation: 0,
            replica_id: Uuid::nil(),
            attempt_id: None,
            coordinator_epoch: 6,
            backend: "node/memory".into(),
            key: "missing".into(),
            payload_b64: None,
            expected_hash: None,
            codec: None,
        };
        let reply = handler(Request {
            id: 2,
            method: crate::protocol::method::NODE_EXECUTE_EXISTS.into(),
            payload: serde_json::to_value(op).unwrap(),
        });
        assert!(!reply.ok);
        assert_eq!(reply.error.unwrap().class, "stale_epoch");
    }
}
