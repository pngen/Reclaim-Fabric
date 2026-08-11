//! Multi-process integration test: a real coordinator process, a real node
//! process, and CLI clients over framed TCP.

use std::io::Read;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use serde_json::Value;

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_reclaim-fabric")
}

struct ChildProcess {
    child: Option<Child>,
    stdout_reader: Option<JoinHandle<()>>,
    stderr_reader: Option<JoinHandle<()>>,
    stdout_tail: Arc<Mutex<Vec<u8>>>,
    stderr_tail: Arc<Mutex<Vec<u8>>>,
}

impl ChildProcess {
    fn wait_exit(&mut self, timeout: Duration) -> Option<i32> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = self
                .child
                .as_mut()
                .and_then(|child| child.try_wait().ok().flatten())
            {
                self.child.take();
                self.join_readers(Duration::from_secs(2));
                return status.code();
            }
            if Instant::now() >= deadline {
                self.terminate_and_reap(Duration::from_secs(5));
                return None;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn terminate_and_reap(&mut self, timeout: Duration) {
        let Some(child) = self.child.as_mut() else {
            self.join_readers(Duration::from_secs(2));
            return;
        };
        let _ = child.kill();
        let deadline = Instant::now() + timeout;
        loop {
            match child.try_wait() {
                Ok(Some(_)) => {
                    self.child.take();
                    self.join_readers(Duration::from_secs(2));
                    return;
                }
                Ok(None) | Err(_) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(20));
                }
                Ok(None) | Err(_) => {
                    // A second kill is harmless and covers a transient first
                    // failure. These test children do not spawn subprocesses.
                    let _ = child.kill();
                    return;
                }
            }
        }
    }

    fn join_readers(&mut self, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        for slot in [&mut self.stdout_reader, &mut self.stderr_reader] {
            while slot.as_ref().is_some_and(|handle| !handle.is_finished())
                && Instant::now() < deadline
            {
                std::thread::sleep(Duration::from_millis(10));
            }
            if slot.as_ref().is_some_and(JoinHandle::is_finished) {
                if let Some(handle) = slot.take() {
                    let _ = handle.join();
                }
            }
        }
    }

    fn stderr(&self) -> String {
        capture_text(&self.stderr_tail)
    }

    fn stdout(&self) -> String {
        capture_text(&self.stdout_tail)
    }
}

impl Drop for ChildProcess {
    fn drop(&mut self) {
        self.terminate_and_reap(Duration::from_secs(5));
    }
}

const CAPTURE_LIMIT: usize = 20 * 1024 * 1024;

fn capture_text(capture: &Arc<Mutex<Vec<u8>>>) -> String {
    capture
        .lock()
        .map(|bytes| String::from_utf8_lossy(&bytes).into_owned())
        .unwrap_or_else(|_| "<output capture poisoned>".into())
}

fn append_capped(capture: &Arc<Mutex<Vec<u8>>>, chunk: &[u8]) {
    let Ok(mut bytes) = capture.lock() else {
        return;
    };
    if chunk.len() >= CAPTURE_LIMIT {
        bytes.clear();
        bytes.extend_from_slice(&chunk[chunk.len() - CAPTURE_LIMIT..]);
        return;
    }
    let overflow = bytes
        .len()
        .saturating_add(chunk.len())
        .saturating_sub(CAPTURE_LIMIT);
    if overflow > 0 {
        let remove = overflow.min(bytes.len());
        bytes.drain(..remove);
    }
    bytes.extend_from_slice(chunk);
}

fn spawn_reader(
    mut reader: impl Read + Send + 'static,
    capture: Arc<Mutex<Vec<u8>>>,
    startup: Option<mpsc::SyncSender<Vec<u8>>>,
) -> JoinHandle<()> {
    std::thread::spawn(move || {
        let mut buf = [0u8; 4096];
        loop {
            match reader.read(&mut buf) {
                Ok(0) => return,
                Ok(n) => {
                    append_capped(&capture, &buf[..n]);
                    if let Some(tx) = &startup {
                        if tx.send(buf[..n].to_vec()).is_err() {
                            // Startup receiver is gone; continue draining so
                            // the child can never block on a full pipe.
                        }
                    }
                }
                Err(_) => return,
            }
        }
    })
}

// The returned child is owned by the caller, which always waits (ChildProcess
// guard or explicit wait_exit); the lint cannot see across the boundary.
#[allow(clippy::zombie_processes)]
fn spawn_until_ready(args: &[&str]) -> (ChildProcess, String) {
    let mut child = Command::new(bin())
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn reclaim-fabric");
    let pid = child.id();
    let stdout = child.stdout.take().expect("piped stdout");
    let stderr = child.stderr.take().expect("piped stderr");
    let stdout_tail = Arc::new(Mutex::new(Vec::new()));
    let stderr_tail = Arc::new(Mutex::new(Vec::new()));
    let (tx, rx) = mpsc::sync_channel(64);
    let stdout_reader = spawn_reader(stdout, stdout_tail.clone(), Some(tx));
    let stderr_reader = spawn_reader(stderr, stderr_tail.clone(), None);
    let mut process = ChildProcess {
        child: Some(child),
        stdout_reader: Some(stdout_reader),
        stderr_reader: Some(stderr_reader),
        stdout_tail,
        stderr_tail,
    };
    let mut pending = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = process
            .child
            .as_mut()
            .and_then(|child| child.try_wait().ok().flatten())
        {
            process.child.take();
            process.join_readers(Duration::from_secs(2));
            panic!(
                "child (pid {pid}) exited ({status}) before READY; args: {args:?}\nstdout: {}\nstderr: {}",
                process.stdout(),
                process.stderr()
            );
        }
        let now = Instant::now();
        if now >= deadline {
            process.terminate_and_reap(Duration::from_secs(5));
            panic!(
                "timed out waiting for READY from child {pid}; args: {args:?}\nstdout: {}\nstderr: {}",
                process.stdout(),
                process.stderr()
            );
        }
        match rx.recv_timeout((deadline - now).min(Duration::from_millis(100))) {
            Ok(chunk) => {
                pending.extend_from_slice(&chunk);
                if pending.len() > 64 * 1024 {
                    process.terminate_and_reap(Duration::from_secs(5));
                    panic!("child {pid} emitted an overlong startup line; args: {args:?}");
                }
                while let Some(newline) = pending.iter().position(|byte| *byte == b'\n') {
                    let line: Vec<u8> = pending.drain(..=newline).collect();
                    let trimmed = String::from_utf8_lossy(&line).trim().to_string();
                    if let Some(addr) = trimmed.strip_prefix("READY ") {
                        return (process, addr.to_string());
                    }
                    if trimmed.starts_with("error:") {
                        process.terminate_and_reap(Duration::from_secs(5));
                        panic!(
                            "child startup error: {trimmed}\nstdout: {}\nstderr: {}",
                            process.stdout(),
                            process.stderr()
                        );
                    }
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                process.terminate_and_reap(Duration::from_secs(5));
                panic!(
                    "child {pid} closed stdout before READY; args: {args:?}\nstdout: {}\nstderr: {}",
                    process.stdout(),
                    process.stderr()
                );
            }
        }
    }
}

struct BoundedOutput {
    status: ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

#[allow(clippy::zombie_processes)]
fn bounded_output(mut command: Command, timeout: Duration) -> BoundedOutput {
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = command.spawn().expect("spawn bounded command");
    let stdout = child.stdout.take().expect("piped stdout");
    let stderr = child.stderr.take().expect("piped stderr");
    let stdout_reader = std::thread::spawn(move || {
        let mut out = Vec::new();
        stdout
            .take((CAPTURE_LIMIT + 1) as u64)
            .read_to_end(&mut out)
            .expect("read command stdout");
        out
    });
    let stderr_reader = std::thread::spawn(move || {
        let mut out = Vec::new();
        stderr
            .take((CAPTURE_LIMIT + 1) as u64)
            .read_to_end(&mut out)
            .expect("read command stderr");
        out
    });
    let deadline = Instant::now() + timeout;
    let status = loop {
        if let Some(status) = child.try_wait().expect("poll bounded command") {
            break status;
        }
        if Instant::now() >= deadline {
            let pid = child.id();
            let _ = child.kill();
            let reap_deadline = Instant::now() + Duration::from_secs(5);
            loop {
                if child.try_wait().expect("reap timed-out command").is_some() {
                    break;
                }
                assert!(
                    Instant::now() < reap_deadline,
                    "timed-out CLI child {pid} could not be reaped"
                );
                std::thread::sleep(Duration::from_millis(20));
            }
            panic!("CLI command {pid} exceeded {timeout:?}");
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    let stdout = stdout_reader.join().expect("stdout reader panicked");
    let stderr = stderr_reader.join().expect("stderr reader panicked");
    assert!(
        stdout.len() <= CAPTURE_LIMIT && stderr.len() <= CAPTURE_LIMIT,
        "CLI output exceeded capture limit"
    );
    BoundedOutput {
        status,
        stdout,
        stderr,
    }
}

fn cli(coordinator: &str, args: &[&str]) -> Value {
    let mut command = Command::new(bin());
    command
        .args(["--coordinator", coordinator, "--json"])
        .args(args);
    let output = bounded_output(command, Duration::from_secs(10));
    assert!(
        output.status.success(),
        "CLI failed: {args:?}\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let text = String::from_utf8_lossy(&output.stdout);
    serde_json::from_str(text.trim())
        .unwrap_or_else(|e| panic!("CLI output not JSON ({e}): {text}"))
}

fn cli_err(coordinator: &str, args: &[&str]) -> Value {
    let mut command = Command::new(bin());
    command
        .args(["--coordinator", coordinator, "--json"])
        .args(args);
    let output = bounded_output(command, Duration::from_secs(10));
    assert!(
        !output.status.success(),
        "CLI unexpectedly succeeded: {args:?}"
    );
    let text = String::from_utf8_lossy(&output.stderr);
    serde_json::from_str(text.trim())
        .unwrap_or_else(|e| panic!("CLI error output not JSON ({e}): {text}"))
}

fn temp_dir(tag: &str) -> tempfile::TempDir {
    let dir = tempfile::Builder::new().prefix(tag).tempdir().unwrap();
    dir
}

#[test]
fn coordinator_node_and_cli_over_tcp() {
    let work = temp_dir("rf-multi");
    let store = work.path().join("store.db");
    let data_dir = work.path().join("data");
    let archive_dir = work.path().join("archive");
    let node_dir = work.path().join("node-data");
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::create_dir_all(&archive_dir).unwrap();
    std::fs::create_dir_all(&node_dir).unwrap();

    // 1. Start the coordinator.
    let (mut coordinator_proc, coord_addr) = spawn_until_ready(&[
        "coordinator",
        "start",
        "--store",
        store.to_str().unwrap(),
        "--bind",
        "127.0.0.1:0",
        "--data-dir",
        data_dir.to_str().unwrap(),
        "--archive-dir",
        archive_dir.to_str().unwrap(),
    ]);

    // 2. Start a node that registers with the coordinator.
    let (node_proc, _node_ready) = spawn_until_ready(&[
        "node",
        "start",
        "--coordinator",
        &coord_addr,
        "--name",
        "mp-node",
        "--bind",
        "127.0.0.1:0",
        "--data-dir",
        node_dir.to_str().unwrap(),
    ]);

    // 3. Give the node a moment to register + heartbeat.
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let stats = cli(&coord_addr, &["stats"]);
        let nodes = stats["nodes"].as_u64().unwrap_or(0);
        if nodes >= 1 {
            break;
        }
        assert!(Instant::now() < deadline, "node never registered");
        std::thread::sleep(Duration::from_millis(200));
    }

    // 4. Create a local payload object and store a node-hosted payload object.
    let payload_file = work.path().join("payload.bin");
    let payload = vec![7u8; 4096];
    std::fs::write(&payload_file, &payload).unwrap();
    let created = cli(
        &coord_addr,
        &[
            "object",
            "create",
            "--class",
            "checkpoint",
            "--data-file",
            payload_file.to_str().unwrap(),
            "--backend",
            "memory",
            "--reuse-probability",
            "0.01",
            "--recompute-cost",
            "1",
            "--memory-cost-per-byte-sec",
            "1",
        ],
    );
    let local_id = created["id"].as_str().unwrap().to_string();
    assert_eq!(created["lifecycle_state"], "HOT");

    // Node-hosted payload: store it on the node's file backend. Node backend
    // ids are namespaced by the node name.
    let node_backend = format!("mp-node/file:{}", node_dir.display());
    let node_obj = cli(
        &coord_addr,
        &[
            "object",
            "create",
            "--class",
            "node-checkpoint",
            "--data-file",
            payload_file.to_str().unwrap(),
            "--backend",
            &node_backend,
            "--reuse-probability",
            "0.01",
            "--recompute-cost",
            "1",
            "--memory-cost-per-byte-sec",
            "1",
        ],
    );
    let node_obj_id = node_obj["id"].as_str().unwrap().to_string();

    // 5. Plan + reclaim the local object via the CLI.
    let plan = cli(&coord_addr, &["plan", "--object", &local_id]);
    assert_eq!(plan["decision"]["verdict"], "RECLAIM");
    let reclaim = cli(&coord_addr, &["reclaim", &local_id, "--force"]);
    assert_eq!(reclaim["reclaimed"], true);

    // 6. Reclaim the node-hosted object: the coordinator instructs the node
    // to delete the physical payload over TCP.
    let reclaim = cli(&coord_addr, &["reclaim", &node_obj_id, "--force"]);
    assert_eq!(reclaim["reclaimed"], true);

    // 7. Stats reflect the state; audit contains the committed reclaims.
    let stats = cli(&coord_addr, &["stats"]);
    assert_eq!(stats["objects"], 2);
    let audit = cli(&coord_addr, &["audit", "--limit", "50"]);
    let entries = audit["entries"].as_array().unwrap();
    assert!(entries.iter().any(|e| e["action"] == "RECLAIM_COMMITTED"));

    // 8. Shut the node down, then the coordinator (graceful via transport).
    drop(node_proc);
    let shutdown = cli(&coord_addr, &["shutdown", "--reason", "test complete"]);
    assert_eq!(shutdown["ok"], true);
    assert_eq!(coordinator_proc.wait_exit(Duration::from_secs(20)), Some(0));

    // 9. Restart durability: reopen the same store, verify state persisted.
    let (mut coordinator2, coord_addr2) = spawn_until_ready(&[
        "coordinator",
        "start",
        "--store",
        store.to_str().unwrap(),
        "--bind",
        "127.0.0.1:0",
        "--data-dir",
        data_dir.to_str().unwrap(),
        "--archive-dir",
        archive_dir.to_str().unwrap(),
    ]);
    let inspect = cli(&coord_addr2, &["object", "inspect", &local_id]);
    assert_eq!(inspect["lifecycle_state"], "RECLAIMED");
    let inspect = cli(&coord_addr2, &["object", "inspect", &node_obj_id]);
    assert_eq!(inspect["lifecycle_state"], "RECLAIMED");
    let shutdown = cli(&coord_addr2, &["shutdown", "--reason", "test complete"]);
    assert_eq!(shutdown["ok"], true);
    assert_eq!(coordinator2.wait_exit(Duration::from_secs(20)), Some(0));
}

#[test]
fn coordinator_rejects_malformed_clients() {
    let work = temp_dir("rf-mal");
    let store = work.path().join("store.db");
    let data_dir = work.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let (mut coordinator_proc, coord_addr) = spawn_until_ready(&[
        "coordinator",
        "start",
        "--store",
        store.to_str().unwrap(),
        "--bind",
        "127.0.0.1:0",
        "--data-dir",
        data_dir.to_str().unwrap(),
    ]);

    // A raw TCP client sending garbage must be rejected without harming the
    // server.
    use std::io::Write;
    use std::net::TcpStream;
    let mut raw = TcpStream::connect(&coord_addr).unwrap();
    raw.write_all(b"this is not a reclaim-fabric frame at all")
        .unwrap();
    let _ = raw.shutdown(std::net::Shutdown::Both);

    // Oversized length field.
    let mut raw = TcpStream::connect(&coord_addr).unwrap();
    let mut junk = vec![0u8; 16];
    junk[0..4].copy_from_slice(&0x5246_3100u32.to_be_bytes());
    junk[4..6].copy_from_slice(&1u16.to_be_bytes());
    junk[6] = 1;
    junk[8..12].copy_from_slice(&u32::MAX.to_be_bytes());
    raw.write_all(&junk).unwrap();
    drop(raw);

    // Server must still be healthy.
    let stats = cli(&coord_addr, &["stats"]);
    assert!(stats["objects"].as_u64().unwrap() == 0);
    let shutdown = cli(&coord_addr, &["shutdown", "--reason", "malformed test"]);
    assert_eq!(shutdown["ok"], true);
    assert_eq!(coordinator_proc.wait_exit(Duration::from_secs(20)), Some(0));
}

#[test]
fn errors_are_machine_readable() {
    let work = temp_dir("rf-err");
    let store = work.path().join("store.db");
    let data_dir = work.path().join("data");
    std::fs::create_dir_all(&data_dir).unwrap();
    let (mut coordinator_proc, coord_addr) = spawn_until_ready(&[
        "coordinator",
        "start",
        "--store",
        store.to_str().unwrap(),
        "--bind",
        "127.0.0.1:0",
        "--data-dir",
        data_dir.to_str().unwrap(),
    ]);
    let err = cli_err(
        &coord_addr,
        &["object", "inspect", "00000000-0000-0000-0000-000000000000"],
    );
    assert_eq!(err["ok"], false);
    assert_eq!(err["error"]["class"], "not_found");
    assert!(
        err["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains("not found")),
        "got: {err}"
    );
    let shutdown = cli(&coord_addr, &["shutdown", "--reason", "err test"]);
    assert_eq!(shutdown["ok"], true);
    assert_eq!(coordinator_proc.wait_exit(Duration::from_secs(20)), Some(0));
}
