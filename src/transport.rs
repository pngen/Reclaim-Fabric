//! Framed TCP transport for the multi-process fabric.
//!
//! Wire format (all integers big-endian, checked):
//!
//! ```text
//! [magic u32 = 0x52463100 ("RF1\0")]
//! [protocol_version u16 = 1]
//! [message_type u8]
//! [flags u8]
//! [payload_len u32]      (bounded by MAX_FRAME_SIZE)
//! [crc32c u32]           (over the 12 header bytes + payload)
//! [payload ...]
//! ```
//!
//! Rules:
//! - Peer-provided lengths are never trusted beyond `MAX_FRAME_SIZE`.
//! - Malformed frames reject the connection (fail closed).
//! - Reads are bounded; no unbounded allocation.
//! - Every request carries a request id in the typed envelope; replies echo it.
//! - Timeouts apply to connects, reads, and writes.
//! - Graceful server shutdown drains and closes connections.

use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::errors::{ReclaimError, Result, WireError};
use crate::integrity::crc32c;

pub const MAGIC: u32 = 0x5246_3100;
pub const PROTOCOL_VERSION: u16 = 1;
pub const MAX_FRAME_SIZE: u32 = 16 * 1024 * 1024; // 16 MiB
pub const DEFAULT_PORT: u16 = 7910;
pub const DEFAULT_TIMEOUT_MS: u64 = 30_000;
const HEADER_LEN: usize = 12;

/// Message types on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[repr(u8)]
pub enum MessageType {
    Request = 1,
    Reply = 2,
    Heartbeat = 3,
    HeartbeatAck = 4,
}

impl MessageType {
    pub fn from_u8(v: u8) -> Result<MessageType> {
        match v {
            1 => Ok(MessageType::Request),
            2 => Ok(MessageType::Reply),
            3 => Ok(MessageType::Heartbeat),
            4 => Ok(MessageType::HeartbeatAck),
            other => Err(ReclaimError::Protocol(format!(
                "unknown message type {other}"
            ))),
        }
    }
}

/// One framed message on the wire. Request/reply ids live in the typed JSON
/// envelope; the frame itself is id-agnostic.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireMessage {
    pub msg_type: MessageType,
    pub payload: Vec<u8>,
}

impl WireMessage {
    pub fn request(payload: Vec<u8>) -> WireMessage {
        WireMessage {
            msg_type: MessageType::Request,
            payload,
        }
    }

    pub fn reply(payload: Vec<u8>) -> WireMessage {
        WireMessage {
            msg_type: MessageType::Reply,
            payload,
        }
    }

    pub fn heartbeat() -> WireMessage {
        WireMessage {
            msg_type: MessageType::Heartbeat,
            payload: Vec::new(),
        }
    }

    pub fn heartbeat_ack() -> WireMessage {
        WireMessage {
            msg_type: MessageType::HeartbeatAck,
            payload: Vec::new(),
        }
    }

    /// Encode into the framed wire format.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(HEADER_LEN + 4 + self.payload.len());
        buf.extend_from_slice(&MAGIC.to_be_bytes());
        buf.extend_from_slice(&PROTOCOL_VERSION.to_be_bytes());
        buf.push(self.msg_type as u8);
        buf.push(0); // flags (reserved)
        buf.extend_from_slice(&(self.payload.len() as u32).to_be_bytes());
        let crc = ::crc32c::crc32c_append(crc32c(&buf), &self.payload);
        buf.extend_from_slice(&crc.to_be_bytes());
        buf.extend_from_slice(&self.payload);
        buf
    }

    /// Decode one frame from a reader. Returns None on clean EOF at a frame
    /// boundary (no partial data). Any corruption rejects with a protocol
    /// error and the connection must be dropped (fail closed).
    pub fn decode(reader: &mut impl Read) -> Result<Option<WireMessage>> {
        let mut header = [0u8; HEADER_LEN];
        let mut filled = 0usize;
        while filled < HEADER_LEN {
            let n = reader.read(&mut header[filled..]).map_err(|e| {
                if is_timeout(&e) && filled > 0 {
                    ReclaimError::Protocol(format!(
                        "timeout in frame header after {filled} of {HEADER_LEN} bytes"
                    ))
                } else if is_timeout(&e) {
                    ReclaimError::Transport("idle read timed out".into())
                } else {
                    io_err(e)
                }
            })?;
            if n == 0 {
                if filled == 0 {
                    return Ok(None); // clean EOF at boundary
                }
                return Err(ReclaimError::Protocol(
                    "EOF in the middle of a frame header".into(),
                ));
            }
            filled += n;
        }
        let magic = u32::from_be_bytes(header[0..4].try_into().expect("4 bytes"));
        if magic != MAGIC {
            return Err(ReclaimError::Protocol(format!(
                "bad magic 0x{magic:08x}; not a reclaim-fabric peer"
            )));
        }
        let version = u16::from_be_bytes(header[4..6].try_into().expect("2 bytes"));
        if version != PROTOCOL_VERSION {
            return Err(ReclaimError::Protocol(format!(
                "protocol version mismatch: got {version}, expected {PROTOCOL_VERSION}"
            )));
        }
        let msg_type = MessageType::from_u8(header[6])?;
        if header[7] != 0 {
            return Err(ReclaimError::Protocol(format!(
                "unsupported frame flags 0x{:02x}",
                header[7]
            )));
        }
        let payload_len = u32::from_be_bytes(header[8..12].try_into().expect("4 bytes"));
        if payload_len > MAX_FRAME_SIZE {
            return Err(ReclaimError::Protocol(format!(
                "frame size {payload_len} exceeds maximum {MAX_FRAME_SIZE}"
            )));
        }
        let mut crc_buf = [0u8; 4];
        reader.read_exact(&mut crc_buf).map_err(|e| {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                ReclaimError::Protocol("EOF in frame checksum".into())
            } else if is_timeout(&e) {
                ReclaimError::Protocol("timeout in frame checksum".into())
            } else {
                io_err(e)
            }
        })?;
        let frame_crc = u32::from_be_bytes(crc_buf);

        let mut payload = vec![0u8; payload_len as usize];
        if !payload.is_empty() {
            reader.read_exact(&mut payload).map_err(|e| {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    ReclaimError::Protocol(format!(
                        "truncated frame: expected {payload_len} payload bytes"
                    ))
                } else if is_timeout(&e) {
                    ReclaimError::Protocol(format!(
                        "timeout in frame payload: expected {payload_len} bytes"
                    ))
                } else {
                    io_err(e)
                }
            })?;
        }
        let computed_crc = ::crc32c::crc32c_append(crc32c(&header), &payload);
        if computed_crc != frame_crc {
            return Err(ReclaimError::Protocol(format!(
                "frame checksum mismatch: got {frame_crc:08x}, computed {computed_crc:08x}"
            )));
        }
        Ok(Some(WireMessage { msg_type, payload }))
    }
}

fn io_err(e: std::io::Error) -> ReclaimError {
    ReclaimError::Transport(e.to_string())
}

fn is_timeout(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
    )
}

/// Typed request envelope (JSON payload inside the frame).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Request {
    pub id: u64,
    pub method: String,
    pub payload: serde_json::Value,
}

/// Typed reply envelope.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Reply {
    pub id: u64,
    pub ok: bool,
    pub error: Option<WireError>,
    pub payload: serde_json::Value,
}

impl Reply {
    pub fn ok(id: u64, payload: serde_json::Value) -> Reply {
        Reply {
            id,
            ok: true,
            error: None,
            payload,
        }
    }

    pub fn err(id: u64, e: ReclaimError) -> Reply {
        Reply {
            id,
            ok: false,
            error: Some(e.into()),
            payload: serde_json::Value::Null,
        }
    }

    /// Convert into a payload value, mapping the wire error back.
    pub fn into_result(self, expected_id: u64) -> Result<serde_json::Value> {
        if self.id != expected_id {
            return Err(ReclaimError::Protocol(format!(
                "reply id mismatch: expected {expected_id}, got {}",
                self.id
            )));
        }
        if self.ok {
            Ok(self.payload)
        } else if let Some(e) = self.error {
            Err(ReclaimError::from(e))
        } else {
            Err(ReclaimError::Protocol(
                "error reply without error details".into(),
            ))
        }
    }
}

/// Decode a payload field into a typed value with protocol-level validation.
pub fn decode_payload<T: DeserializeOwned>(value: &serde_json::Value) -> Result<T> {
    serde_json::from_value(value.clone())
        .map_err(|e| ReclaimError::Protocol(format!("invalid payload: {e}")))
}

/// Client connection with strict framing.
pub struct Client {
    stream: TcpStream,
    next_request_id: std::sync::atomic::AtomicU64,
    timeout: Duration,
}

impl Client {
    pub fn connect(addr: &str, timeout_ms: u64) -> Result<Client> {
        if timeout_ms == 0 {
            return Err(ReclaimError::InvalidArgument(
                "transport timeout must be greater than zero".into(),
            ));
        }
        let timeout = Duration::from_millis(timeout_ms);
        let deadline = Instant::now().checked_add(timeout).ok_or_else(|| {
            ReclaimError::InvalidArgument("transport timeout is too large".into())
        })?;
        let socket_addr: SocketAddr = addr.parse().map_err(|e| {
            ReclaimError::InvalidArgument(format!(
                "transport address must be a numeric socket address ({addr}): {e}"
            ))
        })?;
        let remaining = deadline.saturating_duration_since(Instant::now());
        let stream = TcpStream::connect_timeout(&socket_addr, remaining)
            .map_err(|e| ReclaimError::Transport(format!("connect to {addr}: {e}")))?;
        stream.set_read_timeout(Some(timeout)).map_err(io_err)?;
        stream.set_write_timeout(Some(timeout)).map_err(io_err)?;
        stream.set_nodelay(true).map_err(io_err)?;
        Ok(Client {
            stream,
            next_request_id: std::sync::atomic::AtomicU64::new(1),
            timeout,
        })
    }

    /// Numeric address of the connected peer. Persisting this after the
    /// initial connection keeps background reconnects free of DNS stalls.
    pub fn peer_addr(&self) -> Result<String> {
        self.stream
            .peer_addr()
            .map(|addr| addr.to_string())
            .map_err(io_err)
    }

    /// Send a typed request and await its matching reply.
    pub fn call(&mut self, method: &str, payload: serde_json::Value) -> Result<Reply> {
        self.stream
            .set_write_timeout(Some(self.timeout))
            .map_err(io_err)?;
        let id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        self.send_request(id, method, payload)?;
        let deadline = Instant::now().checked_add(self.timeout).ok_or_else(|| {
            ReclaimError::InvalidArgument("transport timeout is too large".into())
        })?;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(ReclaimError::Transport(format!("request {id} timed out")));
            }
            self.stream
                .set_read_timeout(Some(remaining))
                .map_err(io_err)?;
            let msg = self.recv_frame()?;
            match msg.msg_type {
                MessageType::Heartbeat => {
                    if !msg.payload.is_empty() {
                        return Err(ReclaimError::Protocol(
                            "heartbeat frame carried a payload".into(),
                        ));
                    }
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        return Err(ReclaimError::Transport(format!("request {id} timed out")));
                    }
                    self.stream
                        .set_write_timeout(Some(remaining))
                        .map_err(io_err)?;
                    self.send_raw(&WireMessage::heartbeat_ack())?;
                }
                MessageType::HeartbeatAck => {
                    if !msg.payload.is_empty() {
                        return Err(ReclaimError::Protocol(
                            "heartbeat acknowledgement carried a payload".into(),
                        ));
                    }
                    // keep-alive noise; continue waiting for the reply
                }
                MessageType::Reply => {
                    let reply: Reply = serde_json::from_slice(&msg.payload)
                        .map_err(|e| ReclaimError::Protocol(format!("bad reply json: {e}")))?;
                    if reply.id != id {
                        return Err(ReclaimError::Protocol(format!(
                            "reply id mismatch: expected {id}, got {}",
                            reply.id
                        )));
                    }
                    return Ok(reply);
                }
                MessageType::Request => {
                    return Err(ReclaimError::Protocol(
                        "server sent unsolicited request".into(),
                    ));
                }
            }
        }
    }

    fn send_request(&mut self, id: u64, method: &str, payload: serde_json::Value) -> Result<()> {
        let req = Request {
            id,
            method: method.to_string(),
            payload,
        };
        let bytes = serde_json::to_vec(&req).map_err(ReclaimError::from)?;
        if bytes.len() > MAX_FRAME_SIZE as usize {
            return Err(ReclaimError::Protocol("request payload too large".into()));
        }
        self.send_raw(&WireMessage::request(bytes))
    }

    fn send_raw(&mut self, msg: &WireMessage) -> Result<()> {
        if msg.payload.len() > MAX_FRAME_SIZE as usize {
            return Err(ReclaimError::Protocol(format!(
                "frame payload exceeds maximum {MAX_FRAME_SIZE}"
            )));
        }
        self.stream.write_all(&msg.encode()).map_err(io_err)?;
        self.stream.flush().map_err(io_err)?;
        Ok(())
    }

    fn recv_frame(&mut self) -> Result<WireMessage> {
        WireMessage::decode(&mut self.stream)?
            .ok_or_else(|| ReclaimError::Transport("connection closed by peer".into()))
    }
}

/// Server configuration.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub bind_addr: String,
    pub max_connections: usize,
    pub timeout_ms: u64,
    pub shutdown_poll_ms: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            bind_addr: format!("127.0.0.1:{DEFAULT_PORT}"),
            max_connections: 64,
            timeout_ms: DEFAULT_TIMEOUT_MS,
            shutdown_poll_ms: 100,
        }
    }
}

/// Callback receiving one decoded request; returns a reply.
pub type RequestHandler = Arc<dyn Fn(Request) -> Reply + Send + Sync>;

struct ConnectionThread {
    handle: std::thread::JoinHandle<()>,
    control: TcpStream,
}

/// Framed TCP server: bounded connections, graceful shutdown.
pub struct Server {
    listener: TcpListener,
    config: ServerConfig,
    handler: RequestHandler,
    shutdown: Arc<AtomicBool>,
    /// Join handles of connection threads (retained for graceful drain).
    threads: Arc<std::sync::Mutex<Vec<ConnectionThread>>>,
}

impl Server {
    pub fn new(config: ServerConfig, handler: RequestHandler) -> Result<Server> {
        if config.max_connections == 0 {
            return Err(ReclaimError::InvalidArgument(
                "max_connections must be greater than zero".into(),
            ));
        }
        if config.timeout_ms == 0 || config.shutdown_poll_ms == 0 {
            return Err(ReclaimError::InvalidArgument(
                "server timeouts must be greater than zero".into(),
            ));
        }
        let bind_addr: SocketAddr = config.bind_addr.parse().map_err(|e| {
            ReclaimError::InvalidArgument(format!(
                "server bind address must be numeric ({}): {e}",
                config.bind_addr
            ))
        })?;
        let listener = TcpListener::bind(bind_addr)
            .map_err(|e| ReclaimError::Transport(format!("bind {}: {e}", config.bind_addr)))?;
        listener.set_nonblocking(true).map_err(io_err)?;
        Ok(Server {
            listener,
            config,
            handler,
            shutdown: Arc::new(AtomicBool::new(false)),
            threads: Arc::new(std::sync::Mutex::new(Vec::new())),
        })
    }

    pub fn local_addr(&self) -> Result<String> {
        self.listener
            .local_addr()
            .map(|a| a.to_string())
            .map_err(io_err)
    }

    pub fn request_shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
        if let Ok(threads) = self.threads.lock() {
            for thread in threads.iter() {
                let _ = thread.control.shutdown(Shutdown::Both);
            }
        }
    }

    /// Serve until shutdown is requested. Connection concurrency is bounded.
    pub fn serve(&self) -> Result<()> {
        loop {
            if self.shutdown.load(Ordering::SeqCst) {
                break;
            }
            match self.listener.accept() {
                Ok((stream, _peer)) => {
                    let finished = {
                        let mut guard = self.threads.lock().map_err(|_| {
                            ReclaimError::Internal("server thread list poisoned".into())
                        })?;
                        let mut finished = Vec::new();
                        let mut active = Vec::new();
                        for thread in guard.drain(..) {
                            if thread.handle.is_finished() {
                                finished.push(thread);
                            } else {
                                active.push(thread);
                            }
                        }
                        *guard = active;
                        finished
                    };
                    for thread in finished {
                        thread.handle.join().map_err(|_| {
                            ReclaimError::Internal("server connection thread panicked".into())
                        })?;
                    }
                    let active = self
                        .threads
                        .lock()
                        .map_err(|_| ReclaimError::Internal("server thread list poisoned".into()))?
                        .len();
                    if active >= self.config.max_connections {
                        log::warn!(
                            "rejecting connection: at capacity {}",
                            self.config.max_connections
                        );
                        drop(stream);
                        continue;
                    }
                    // Short read timeout: idle connections poll the shutdown
                    // flag instead of blocking the drain for the full timeout.
                    let read_timeout_ms = self.config.timeout_ms.min(1_000);
                    stream
                        .set_read_timeout(Some(Duration::from_millis(read_timeout_ms)))
                        .map_err(io_err)?;
                    stream
                        .set_write_timeout(Some(Duration::from_millis(self.config.timeout_ms)))
                        .map_err(io_err)?;
                    // Accepted sockets inherit the listener's non-blocking
                    // mode (Windows and Unix); connection threads use blocking
                    // I/O with timeouts.
                    stream.set_nonblocking(false).map_err(io_err)?;
                    stream.set_nodelay(true).map_err(io_err)?;
                    let control = stream.try_clone().map_err(io_err)?;
                    let handler = self.handler.clone();
                    let shutdown = self.shutdown.clone();
                    let idle_timeout = Duration::from_millis(self.config.timeout_ms);
                    let handle = std::thread::spawn(move || {
                        let _ = handle_connection(stream, handler, shutdown, idle_timeout);
                    });
                    self.threads
                        .lock()
                        .map_err(|_| ReclaimError::Internal("server thread list poisoned".into()))?
                        .push(ConnectionThread { handle, control });
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(self.config.shutdown_poll_ms));
                }
                Err(e) => {
                    if self.shutdown.load(Ordering::SeqCst) {
                        break;
                    }
                    return Err(ReclaimError::Transport(format!(
                        "listener accept failed: {e}"
                    )));
                }
            }
        }
        // Graceful drain: wait for in-flight connections (bounded time).
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let done = {
                let guard = self
                    .threads
                    .lock()
                    .map_err(|_| ReclaimError::Internal("server thread list poisoned".into()))?;
                guard.iter().all(|thread| thread.handle.is_finished())
            };
            if done || std::time::Instant::now() >= deadline {
                break;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        let all_finished = self
            .threads
            .lock()
            .map_err(|_| ReclaimError::Internal("server thread list poisoned".into()))?
            .iter()
            .all(|thread| thread.handle.is_finished());
        if !all_finished {
            return Err(ReclaimError::Internal(
                "server connection threads did not drain before shutdown deadline".into(),
            ));
        }
        let handles: Vec<_> = {
            let mut guard = self
                .threads
                .lock()
                .map_err(|_| ReclaimError::Internal("server thread list poisoned".into()))?;
            std::mem::take(&mut *guard)
        };
        let mut panicked = false;
        for thread in handles {
            if thread.handle.join().is_err() {
                panicked = true;
            }
        }
        if panicked {
            return Err(ReclaimError::Internal(
                "one or more server connection threads panicked".into(),
            ));
        }
        Ok(())
    }
}

fn handle_connection(
    mut stream: TcpStream,
    handler: RequestHandler,
    shutdown: Arc<AtomicBool>,
    idle_timeout: Duration,
) -> Result<()> {
    let mut last_activity = Instant::now();
    loop {
        if shutdown.load(Ordering::SeqCst) {
            return Ok(());
        }
        let msg = match WireMessage::decode(&mut stream) {
            Ok(Some(m)) => m,
            Ok(None) => return Ok(()), // peer closed cleanly
            Err(e) => {
                let timed_out =
                    matches!(&e, ReclaimError::Transport(msg) if msg == "idle read timed out");
                if timed_out {
                    // Idle connections have a total lifetime bound; periodic
                    // socket timeouts are only polling intervals.
                    if shutdown.load(Ordering::SeqCst) {
                        return Ok(());
                    }
                    if last_activity.elapsed() >= idle_timeout {
                        return Ok(());
                    }
                    continue;
                }
                // Malformed frames: fail closed, log, and drop the connection.
                log::warn!("dropping connection: {e}");
                let reply = Reply::err(0, e);
                if let Ok(bytes) = serde_json::to_vec(&reply) {
                    let _ = stream.write_all(&WireMessage::reply(bytes).encode());
                }
                return Ok(());
            }
        };
        last_activity = Instant::now();
        match msg.msg_type {
            MessageType::Request => {
                let req: Request = match serde_json::from_slice(&msg.payload) {
                    Ok(r) => r,
                    Err(e) => {
                        let reply =
                            Reply::err(0, ReclaimError::Protocol(format!("bad request json: {e}")));
                        if let Ok(bytes) = serde_json::to_vec(&reply) {
                            let _ = stream.write_all(&WireMessage::reply(bytes).encode());
                        }
                        continue;
                    }
                };
                let reply = handler(req);
                let mut bytes = match serde_json::to_vec(&reply) {
                    Ok(b) => b,
                    Err(e) => {
                        log::error!("failed to serialize reply: {e}");
                        continue;
                    }
                };
                if bytes.len() > MAX_FRAME_SIZE as usize {
                    bytes = serde_json::to_vec(&Reply::err(
                        reply.id,
                        ReclaimError::Protocol("reply payload too large".into()),
                    ))
                    .map_err(|e| {
                        ReclaimError::Internal(format!("serialize oversized-reply error: {e}"))
                    })?;
                }
                if stream
                    .write_all(&WireMessage::reply(bytes).encode())
                    .is_err()
                {
                    log::debug!("connection closed while writing reply");
                    return Ok(());
                }
                let _ = stream.flush();
            }
            MessageType::Heartbeat => {
                if !msg.payload.is_empty() {
                    return Err(ReclaimError::Protocol(
                        "heartbeat frame carried a payload".into(),
                    ));
                }
                let _ = stream.write_all(&WireMessage::heartbeat_ack().encode());
            }
            MessageType::HeartbeatAck | MessageType::Reply => {
                return Err(ReclaimError::Protocol(
                    "client sent unsolicited reply/heartbeat-ack".into(),
                ));
            }
        }
    }
}

/// Default handler that answers heartbeats and rejects requests (client-side
/// stub where a server-side handler is not needed).
pub fn stub_handler() -> RequestHandler {
    Arc::new(|req: Request| Reply::err(req.id, ReclaimError::Protocol("not handled".into())))
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TimeoutAfterPrefix {
        prefix: std::io::Cursor<Vec<u8>>,
    }

    impl Read for TimeoutAfterPrefix {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            if self.prefix.position() < self.prefix.get_ref().len() as u64 {
                self.prefix.read(buf)
            } else {
                Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "injected timeout",
                ))
            }
        }
    }

    #[test]
    fn frame_roundtrip() {
        let msg = WireMessage::request(b"hello world".to_vec());
        let encoded = msg.encode();
        let mut cursor = std::io::Cursor::new(encoded);
        let decoded = WireMessage::decode(&mut cursor).unwrap().unwrap();
        assert_eq!(decoded.msg_type, MessageType::Request);
        assert_eq!(decoded.payload, b"hello world");
    }

    #[test]
    fn empty_frame_roundtrip() {
        let msg = WireMessage::heartbeat();
        let encoded = msg.encode();
        let mut cursor = std::io::Cursor::new(encoded);
        let decoded = WireMessage::decode(&mut cursor).unwrap().unwrap();
        assert_eq!(decoded.msg_type, MessageType::Heartbeat);
        assert!(decoded.payload.is_empty());
    }

    #[test]
    fn clean_eof_returns_none() {
        let mut cursor = std::io::Cursor::new(Vec::<u8>::new());
        assert!(WireMessage::decode(&mut cursor).unwrap().is_none());
    }

    #[test]
    fn bad_magic_rejected() {
        let mut buf = vec![0u8; 20];
        buf[0] = 0xDE;
        buf[1] = 0xAD;
        let mut cursor = std::io::Cursor::new(buf);
        assert!(WireMessage::decode(&mut cursor).is_err());
    }

    #[test]
    fn bad_version_rejected() {
        let mut msg = WireMessage::heartbeat().encode();
        msg[4] = 0;
        msg[5] = 99;
        let mut cursor = std::io::Cursor::new(msg);
        assert!(WireMessage::decode(&mut cursor).is_err());
    }

    #[test]
    fn oversized_frame_rejected() {
        let mut msg = WireMessage::heartbeat().encode();
        msg[8] = 0xFF;
        msg[9] = 0xFF;
        msg[10] = 0xFF;
        msg[11] = 0xFF;
        let mut cursor = std::io::Cursor::new(msg);
        assert!(WireMessage::decode(&mut cursor).is_err());
    }

    #[test]
    fn truncated_frame_rejected() {
        let msg = WireMessage::request(vec![0u8; 100]).encode();
        let truncated = &msg[..msg.len() - 50];
        let mut cursor = std::io::Cursor::new(truncated.to_vec());
        assert!(WireMessage::decode(&mut cursor).is_err());
    }

    #[test]
    fn corrupted_payload_crc_rejected() {
        let mut msg = WireMessage::request(vec![1u8; 64]).encode();
        let last = msg.len() - 1;
        msg[last] ^= 0xFF;
        let mut cursor = std::io::Cursor::new(msg);
        assert!(WireMessage::decode(&mut cursor).is_err());
    }

    #[test]
    fn unknown_message_type_rejected() {
        let mut msg = WireMessage::heartbeat().encode();
        msg[6] = 42;
        let mut cursor = std::io::Cursor::new(msg);
        assert!(WireMessage::decode(&mut cursor).is_err());
    }

    #[test]
    fn reserved_frame_flags_are_rejected() {
        let mut msg = WireMessage::heartbeat().encode();
        msg[7] = 1;
        let mut cursor = std::io::Cursor::new(msg);
        assert!(matches!(
            WireMessage::decode(&mut cursor),
            Err(ReclaimError::Protocol(message)) if message.contains("unsupported frame flags")
        ));
    }

    #[test]
    fn timeout_after_partial_header_is_fatal_not_treated_as_idle() {
        let encoded = WireMessage::heartbeat().encode();
        let mut reader = TimeoutAfterPrefix {
            prefix: std::io::Cursor::new(encoded[..5].to_vec()),
        };
        assert!(matches!(
            WireMessage::decode(&mut reader),
            Err(ReclaimError::Protocol(message)) if message.contains("timeout in frame header")
        ));
    }

    #[test]
    fn multiple_frames_back_to_back() {
        let mut encoded = Vec::new();
        let a = WireMessage::request(vec![0u8; 10]);
        let b = WireMessage::request(vec![0u8; 20]);
        encoded.extend_from_slice(&a.encode());
        encoded.extend_from_slice(&b.encode());
        let mut cursor = std::io::Cursor::new(encoded);
        assert_eq!(
            WireMessage::decode(&mut cursor)
                .unwrap()
                .unwrap()
                .payload
                .len(),
            10
        );
        assert_eq!(
            WireMessage::decode(&mut cursor)
                .unwrap()
                .unwrap()
                .payload
                .len(),
            20
        );
        assert!(WireMessage::decode(&mut cursor).unwrap().is_none());
    }

    #[test]
    fn concurrent_clients_roundtrip() {
        let handler: RequestHandler =
            Arc::new(|req: Request| Reply::ok(req.id, serde_json::json!({"echo": req.method})));
        let config = ServerConfig {
            bind_addr: "127.0.0.1:0".into(),
            ..Default::default()
        };
        let server = Server::new(config, handler).unwrap();
        let addr = server.local_addr().unwrap();
        let s = Arc::new(server);
        let s2 = s.clone();
        let server_thread = std::thread::spawn(move || s2.serve().unwrap());

        let mut clients = Vec::new();
        for _ in 0..8 {
            let mut c = Client::connect(&addr, 5000).unwrap();
            let reply = c.call("ping", serde_json::json!({})).unwrap();
            assert!(reply.ok);
            assert_eq!(reply.payload["echo"], "ping");
            clients.push(c);
        }
        // Second round on same connections (sequential reuse).
        for c in clients.iter_mut() {
            let reply = c.call("pong", serde_json::json!({})).unwrap();
            assert!(reply.ok);
        }
        s.request_shutdown();
        server_thread.join().unwrap();
    }

    #[test]
    fn error_replies_roundtrip() {
        let handler: RequestHandler = Arc::new(|req: Request| {
            Reply::err(req.id, ReclaimError::PinnedObject("cannot".into()))
        });
        let config = ServerConfig {
            bind_addr: "127.0.0.1:0".into(),
            ..Default::default()
        };
        let server = Server::new(config, handler).unwrap();
        let addr = server.local_addr().unwrap();
        let s = Arc::new(server);
        let s2 = s.clone();
        let server_thread = std::thread::spawn(move || s2.serve().unwrap());
        let mut c = Client::connect(&addr, 5000).unwrap();
        let reply = c.call("x", serde_json::json!({})).unwrap();
        assert!(!reply.ok);
        assert_eq!(reply.error.unwrap().class, "pinned_object");
        s.request_shutdown();
        server_thread.join().unwrap();
    }

    #[test]
    fn graceful_shutdown_terminates_server() {
        let handler = stub_handler();
        let config = ServerConfig {
            bind_addr: "127.0.0.1:0".into(),
            ..Default::default()
        };
        let server = Server::new(config, handler).unwrap();
        let s = Arc::new(server);
        let s2 = s.clone();
        let server_thread = std::thread::spawn(move || s2.serve().unwrap());
        std::thread::sleep(Duration::from_millis(50));
        s.request_shutdown();
        server_thread.join().unwrap();
    }

    #[test]
    fn graceful_shutdown_interrupts_partial_frame_reader() {
        let config = ServerConfig {
            bind_addr: "127.0.0.1:0".into(),
            ..Default::default()
        };
        let server = Arc::new(Server::new(config, stub_handler()).unwrap());
        let addr = server.local_addr().unwrap();
        let running = server.clone();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let server_thread = std::thread::spawn(move || {
            let result = running.serve();
            let _ = done_tx.send(result);
        });
        let mut raw = TcpStream::connect(addr).unwrap();
        raw.write_all(&MAGIC.to_be_bytes()[..2]).unwrap();
        std::thread::sleep(Duration::from_millis(50));
        server.request_shutdown();
        assert!(done_rx
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .is_ok());
        server_thread.join().unwrap();
    }

    #[test]
    fn heartbeat_noise_cannot_extend_client_call_deadline() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let peer = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let _ = WireMessage::decode(&mut stream).unwrap().unwrap();
            loop {
                if stream
                    .write_all(&WireMessage::heartbeat().encode())
                    .is_err()
                {
                    break;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
        });
        let mut client = Client::connect(&addr.to_string(), 100).unwrap();
        let started = Instant::now();
        assert!(matches!(
            client.call("never-reply", serde_json::json!({})),
            Err(ReclaimError::Transport(_))
        ));
        assert!(started.elapsed() < Duration::from_secs(1));
        drop(client);
        peer.join().unwrap();
    }
}
