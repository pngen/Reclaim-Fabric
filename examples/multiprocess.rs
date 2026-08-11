//! Example 10: multi-process coordinator + nodes.
//!
//! A coordinator process and a node process run over framed TCP. The node
//! hosts physical payloads; the coordinator instructs the node to delete
//! them during reclamation; node operations reject stale coordinator epochs.
//!
//! Run with: cargo run --example multiprocess
//! (cargo must have built the binary: cargo build first)

use std::ffi::OsString;
use std::io::{self, BufRead};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use reclaim_fabric::errors::{ReclaimError, Result};
use reclaim_fabric::protocol::{CreateObjectRequest, NodeOperationRequest, ReclaimRequest};
use reclaim_fabric::transport::Client;

fn bin_path() -> io::Result<std::path::PathBuf> {
    // `cargo run --example` puts this example in target/<profile>/examples/;
    // the runtime binary lives one directory up.
    let exe = std::env::current_exe()?;
    let profile_dir = exe
        .parent()
        .ok_or_else(|| io::Error::other("example executable has no parent directory"))?;
    let target_dir = profile_dir
        .parent()
        .ok_or_else(|| io::Error::other("example executable is outside a Cargo target tree"))?;
    let name = if cfg!(windows) {
        "reclaim-fabric.exe"
    } else {
        "reclaim-fabric"
    };
    Ok(target_dir.join(name))
}

struct ChildProcess {
    child: Child,
}

impl ChildProcess {
    fn terminate(&mut self) -> io::Result<ExitStatus> {
        if let Some(status) = self.child.try_wait()? {
            return Ok(status);
        }
        if let Err(e) = self.child.kill() {
            if e.kind() != io::ErrorKind::InvalidInput {
                return Err(e);
            }
        }
        self.child.wait()
    }

    fn wait_exit(&mut self, timeout: Duration) -> io::Result<ExitStatus> {
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "timeout too large"))?;
        loop {
            if let Some(status) = self.child.try_wait()? {
                return Ok(status);
            }
            if Instant::now() >= deadline {
                let _ = self.terminate();
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("process {} did not exit before deadline", self.child.id()),
                ));
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

impl Drop for ChildProcess {
    fn drop(&mut self) {
        let _ = self.terminate();
    }
}

fn spawn_until_ready(args: &[OsString]) -> Result<(ChildProcess, String)> {
    let child = Command::new(bin_path()?)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()?;
    let mut owned = ChildProcess { child };
    let stdout = owned
        .child
        .stdout
        .take()
        .ok_or_else(|| io::Error::other("child stdout was not piped"))?;
    let (ready_tx, ready_rx) = mpsc::sync_channel(1);
    let reader = std::thread::spawn(move || {
        let mut reader = std::io::BufReader::new(stdout);
        let result = loop {
            let mut line = String::new();
            match reader.read_line(&mut line) {
                Ok(0) => {
                    break Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "child exited before READY",
                    ))
                }
                Ok(_) => {
                    if let Some(value) = line.trim().strip_prefix("READY ") {
                        break Ok(value.to_owned());
                    }
                }
                Err(e) => break Err(e),
            }
        };
        let _ = ready_tx.send(result);
    });
    let deadline = Instant::now()
        .checked_add(Duration::from_secs(10))
        .ok_or_else(|| io::Error::other("startup timeout overflow"))?;
    loop {
        match ready_rx.recv_timeout(Duration::from_millis(20)) {
            Ok(result) => {
                reader
                    .join()
                    .map_err(|_| io::Error::other("READY reader thread panicked"))?;
                return Ok((owned, result?));
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                reader
                    .join()
                    .map_err(|_| io::Error::other("READY reader thread panicked"))?;
                return Err(io::Error::other("READY reader stopped without a result").into());
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
        }
        if let Some(status) = owned.child.try_wait()? {
            reader
                .join()
                .map_err(|_| io::Error::other("READY reader thread panicked"))?;
            return Err(
                io::Error::other(format!("child exited with {status} before READY")).into(),
            );
        }
        if Instant::now() >= deadline {
            let _ = owned.terminate();
            reader
                .join()
                .map_err(|_| io::Error::other("READY reader thread panicked"))?;
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("timed out waiting for READY: {args:?}"),
            )
            .into());
        }
    }
}

fn main() -> Result<()> {
    let work = tempfile::tempdir()?;
    let store = work.path().join("store.db");
    let data_dir = work.path().join("data");
    let node_dir = work.path().join("node-data");
    std::fs::create_dir_all(&data_dir)?;
    std::fs::create_dir_all(&node_dir)?;

    // 1. Start the coordinator process.
    let coordinator_args = vec![
        OsString::from("coordinator"),
        OsString::from("start"),
        OsString::from("--store"),
        store.as_os_str().to_owned(),
        OsString::from("--bind"),
        OsString::from("127.0.0.1:0"),
        OsString::from("--data-dir"),
        data_dir.as_os_str().to_owned(),
    ];
    let (mut coord_child, coord_addr) = spawn_until_ready(&coordinator_args)?;
    println!("coordinator listening on {coord_addr}");

    // 2. Start a node process that registers with the coordinator.
    let node_args = vec![
        OsString::from("node"),
        OsString::from("start"),
        OsString::from("--coordinator"),
        OsString::from(&coord_addr),
        OsString::from("--name"),
        OsString::from("demo-node"),
        OsString::from("--bind"),
        OsString::from("127.0.0.1:0"),
        OsString::from("--data-dir"),
        node_dir.as_os_str().to_owned(),
    ];
    let (mut node_child, node_addr) = spawn_until_ready(&node_args)?;
    println!("node listening on {node_addr}");

    // 3. Create a node-hosted object through the coordinator control plane.
    let mut client = Client::connect(&coord_addr, 10_000)?;
    let now =
        <reclaim_fabric::coordinator::SystemClock as reclaim_fabric::coordinator::Clock>::now_ms(
            &reclaim_fabric::coordinator::SystemClock,
        );
    let mut obj = reclaim_fabric::object::ReclaimObject::new(
        uuid::Uuid::new_v4(),
        0,
        "node-checkpoint",
        1024,
        now,
    );
    obj.reuse_probability = 0.01;
    obj.recompute_cost = Some(1.0);
    obj.memory_cost_per_byte_sec = 1.0;
    let req = CreateObjectRequest {
        object: obj,
        payload_b64: Some(reclaim_fabric::base64_payload(b"node-hosted-state")),
        target_backend: Some(format!("demo-node/file:{}", node_dir.display())),
        replicate_to: vec![],
    };
    let reply = client.call(
        reclaim_fabric::protocol::method::CREATE_OBJECT,
        serde_json::to_value(req)?,
    )?;
    let id = reply.id;
    let value = reply.into_result(id)?;
    let created: reclaim_fabric::object::ReclaimObject = serde_json::from_value(value)?;
    println!("created {} on node backend", created.id);

    // 4. Reclaim it: the coordinator sends a DELETE to the node, which
    // verifies the coordinator epoch before executing.
    let reply = client.call(
        reclaim_fabric::protocol::method::RECLAIM_OBJECT,
        serde_json::to_value(ReclaimRequest {
            object_id: created.id,
            actor: "example".into(),
            force: true,
        })?,
    )?;
    let id = reply.id;
    let value = reply.into_result(id)?;
    let report: reclaim_fabric::coordinator::ReclaimReport = serde_json::from_value(value)?;
    println!(
        "reclaimed: {} ({} deleted replicas)",
        report.reclaimed,
        report.deleted_replicas.len()
    );

    // 5. A stale-epoch node operation is rejected by the node itself
    // (authority check over the real node listener).
    let stale = NodeOperationRequest {
        object_id: created.id,
        generation: 0,
        replica_id: uuid::Uuid::nil(),
        attempt_id: None,
        coordinator_epoch: 999_999,
        backend: "demo-node/file:x".into(),
        key: "k".into(),
        payload_b64: None,
        expected_hash: None,
        codec: None,
    };
    let mut node_client = Client::connect(&node_addr, 5_000)?;
    let reply = node_client.call(
        reclaim_fabric::protocol::method::NODE_EXECUTE_EXISTS,
        serde_json::to_value(stale)?,
    )?;
    let request_id = reply.id;
    match reply.into_result(request_id) {
        Err(reclaim_fabric::errors::ReclaimError::StaleEpoch { got, .. }) => {
            assert_eq!(got, 999_999);
        }
        Err(other) => {
            return Err(ReclaimError::Protocol(format!(
                "expected stale epoch rejection, got {other}"
            )))
        }
        Ok(_) => {
            return Err(ReclaimError::Protocol(
                "stale epoch operation unexpectedly succeeded".into(),
            ))
        }
    }

    // 6. Stop the node deterministically, then shut the coordinator down via
    // its graceful control-plane operation. The CLI has no remote node-stop
    // operation, so this owned demo child is explicitly terminated and reaped.
    let _ = node_child.terminate()?;
    let reply = client.call(
        reclaim_fabric::protocol::method::SHUTDOWN,
        serde_json::json!({"actor": "example", "reason": "example complete"}),
    )?;
    let request_id = reply.id;
    reply.into_result(request_id)?;
    drop(client);
    let status = coord_child.wait_exit(Duration::from_secs(10))?;
    if !status.success() {
        return Err(ReclaimError::Internal(format!(
            "coordinator exited unsuccessfully: {status}"
        )));
    }
    println!("node reaped; coordinator shut down cleanly");
    Ok(())
}
