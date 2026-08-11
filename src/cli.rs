//! CLI for Reclaim Fabric.
//!
//! Control-plane commands speak the framed TCP protocol to a running
//! coordinator. `coordinator start` and `node start` run the runtime in the
//! foreground.
//!
//! Client control commands support `--json`; foreground runtimes emit a
//! readiness line for process supervisors.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use clap::{Args, Parser, Subcommand};
use serde::Deserialize;
use serde_json::json;
use uuid::Uuid;

use crate::errors::{ReclaimError, Result};
use crate::protocol::method;
use crate::transport::{Client, Reply};

const DEFAULT_COORDINATOR: &str = "127.0.0.1:7910";
const MAX_QUERY_LIMIT: u64 = 10_000;
const MAX_INLINE_PAYLOAD_SIZE: usize = 8 * 1024 * 1024;

fn read_bounded_file(path: &Path, max_size: usize, description: &str) -> Result<Vec<u8>> {
    let file = std::fs::File::open(path).map_err(|e| {
        ReclaimError::Io(format!("opening {} {}: {e}", path.display(), description))
    })?;
    let read_limit = u64::try_from(max_size)
        .map_err(|_| ReclaimError::InvalidArgument(format!("{description} size limit is invalid")))?
        .checked_add(1)
        .ok_or_else(|| {
            ReclaimError::InvalidArgument(format!("{description} size limit is too large"))
        })?;
    let mut bytes = Vec::new();
    file.take(read_limit).read_to_end(&mut bytes).map_err(|e| {
        ReclaimError::Io(format!("reading {} {}: {e}", path.display(), description))
    })?;
    if bytes.len() > max_size {
        return Err(ReclaimError::InvalidArgument(format!(
            "{description} exceeds the {max_size}-byte limit"
        )));
    }
    Ok(bytes)
}

#[derive(Deserialize)]
struct PressureSetRequest {
    level: String,
}

#[derive(Deserialize)]
struct PolicyGetRequest {
    id: String,
    version: String,
}

#[derive(Deserialize)]
struct AuditQueryRequest {
    #[serde(default)]
    object_id: Option<Uuid>,
    #[serde(default)]
    action: Option<String>,
    #[serde(default = "default_audit_limit")]
    limit: u64,
}

#[derive(Deserialize)]
struct FailureListRequest {
    #[serde(default = "default_audit_limit")]
    limit: u64,
}

#[derive(Deserialize)]
struct SetProtectedRequest {
    object_id: Uuid,
    protected: bool,
    actor: String,
}

fn default_audit_limit() -> u64 {
    50
}

fn validate_query_limit(limit: u64) -> Result<u64> {
    if limit > MAX_QUERY_LIMIT {
        return Err(ReclaimError::InvalidArgument(format!(
            "query limit {limit} exceeds maximum {MAX_QUERY_LIMIT}"
        )));
    }
    Ok(limit)
}

#[derive(Parser)]
#[command(
    name = "reclaim-fabric",
    version,
    about = "Reclaim Fabric: a vendor-neutral machine-state reclamation runtime for AI infrastructure.\nWhat state is still worth keeping?",
    disable_help_subcommand = true
)]
pub struct Cli {
    /// Coordinator address for control-plane commands.
    #[arg(long, global = true, default_value = DEFAULT_COORDINATOR)]
    pub coordinator: String,

    /// Machine-readable JSON output.
    #[arg(long, global = true)]
    pub json: bool,

    /// Actor name recorded in the audit trail.
    #[arg(long, global = true, default_value = "cli")]
    pub actor: String,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Run the coordinator runtime (foreground).
    Coordinator {
        #[command(subcommand)]
        action: CoordinatorAction,
    },
    /// Run a node runtime (foreground).
    Node {
        #[command(subcommand)]
        action: NodeAction,
    },
    /// Create a tracked state object.
    Object {
        #[command(subcommand)]
        action: ObjectAction,
    },
    /// Manipulate lineage edges.
    Lineage {
        #[command(subcommand)]
        action: LineageAction,
    },
    /// Produce a deterministic reclamation decision.
    Plan {
        /// Object id to plan for.
        #[arg(long, conflicts_with = "candidates")]
        object: Option<Uuid>,
        /// List reclaim candidates instead.
        #[arg(long, conflicts_with = "object")]
        candidates: bool,
        /// Maximum candidates.
        #[arg(long, default_value_t = 100)]
        limit: u64,
    },
    /// Transactionally reclaim an object.
    Reclaim {
        /// Object id.
        id: Uuid,
        /// Execute the reclaim even if the policy recommends retain.
        #[arg(long)]
        force: bool,
    },
    /// Compress an object's payload.
    Compress {
        /// Object id.
        id: Uuid,
    },
    /// Archive an object's payload.
    Archive {
        /// Object id.
        id: Uuid,
    },
    /// Restore an archived object to a hot backend.
    Restore {
        /// Object id.
        id: Uuid,
    },
    /// Verify integrity of all replicas of an object.
    Verify {
        /// Object id.
        id: Uuid,
    },
    /// Show current pressure.
    Pressure {
        #[command(subcommand)]
        action: PressureAction,
    },
    /// List policies.
    Policy {
        #[command(subcommand)]
        action: PolicyAction,
    },
    /// Replay the audit trail.
    Audit {
        /// Filter by object id.
        #[arg(long)]
        object: Option<Uuid>,
        /// Filter by action.
        #[arg(long)]
        action: Option<String>,
        /// Maximum entries (newest first).
        #[arg(long, default_value_t = 50)]
        limit: u64,
    },
    /// List recorded failures.
    Failures {
        /// Maximum entries.
        #[arg(long, default_value_t = 50)]
        limit: u64,
    },
    /// Runtime statistics.
    Stats,
    /// Run a recovery scan (reconcile journal against physical truth).
    Recover,
    /// Shut the coordinator down gracefully.
    Shutdown {
        /// Reason recorded in the audit trail.
        #[arg(long, default_value = "cli shutdown")]
        reason: String,
    },
}

#[derive(Subcommand)]
pub enum CoordinatorAction {
    /// Start the coordinator (foreground; Ctrl+C to stop).
    Start(CoordinatorStartArgs),
}

#[derive(Args)]
pub struct CoordinatorStartArgs {
    /// SQLite store path.
    #[arg(long, default_value = "reclaim-fabric.db")]
    pub store: String,
    /// Bind address for the control plane.
    #[arg(long, default_value = "127.0.0.1:7910")]
    pub bind: String,
    /// Directory for durable payload backends.
    #[arg(long)]
    pub data_dir: Option<PathBuf>,
    /// Directory for the durable archive backend.
    #[arg(long)]
    pub archive_dir: Option<PathBuf>,
}

#[derive(Subcommand)]
pub enum NodeAction {
    /// Start a node (foreground; Ctrl+C to stop).
    Start(NodeStartArgs),
}

#[derive(Args)]
pub struct NodeStartArgs {
    /// Coordinator address.
    #[arg(long, default_value = DEFAULT_COORDINATOR)]
    pub coordinator: String,
    /// Node name.
    #[arg(long, default_value = "node")]
    pub name: String,
    /// Bind address for coordinator operations.
    #[arg(long, default_value = "127.0.0.1:0")]
    pub bind: String,
    /// Directory for the node's durable file backend.
    #[arg(long)]
    pub data_dir: PathBuf,
}

#[derive(Subcommand)]
pub enum ObjectAction {
    /// Register a new state object.
    Create(Box<ObjectCreateArgs>),
    /// Inspect an object's metadata.
    Inspect { id: Uuid },
    /// Record an access.
    Touch { id: Uuid },
    /// Pin an object (cannot be reclaimed).
    Pin { id: Uuid },
    /// Unpin an object.
    Unpin { id: Uuid },
    /// Show lineage neighbors.
    Lineage { id: Uuid },
    /// Show dependency-related neighbors.
    Dependencies { id: Uuid },
}

#[derive(Args)]
pub struct ObjectCreateArgs {
    /// Application class (opaque).
    #[arg(long, default_value = "default")]
    pub class: String,
    /// Explicit object id (default: random).
    #[arg(long)]
    pub id: Option<Uuid>,
    /// Logical size in bytes (defaults to payload length, or 1024 without a payload).
    #[arg(long)]
    pub size: Option<u64>,
    /// Payload file to store with the object.
    #[arg(long)]
    pub data_file: Option<PathBuf>,
    /// Backend to store the payload on.
    #[arg(long, default_value = "memory")]
    pub backend: String,
    /// Estimated future reuse probability (0..1).
    #[arg(long, default_value_t = 0.5)]
    pub reuse_probability: f64,
    /// Expected reuse horizon in seconds.
    #[arg(long)]
    pub reuse_horizon_secs: Option<u64>,
    /// Recomputation cost (arbitrary cost units).
    #[arg(long)]
    pub recompute_cost: Option<f64>,
    /// Recomputation latency in seconds.
    #[arg(long)]
    pub recompute_latency_secs: Option<f64>,
    /// Durability class.
    #[arg(long, value_enum)]
    pub durability: Option<DurabilityArg>,
    /// Survivability class.
    #[arg(long, value_enum)]
    pub survivability: Option<SurvivabilityArg>,
    /// Owner identity.
    #[arg(long, default_value = "default-owner")]
    pub owner: String,
    /// Pin the object on creation.
    #[arg(long)]
    pub pin: bool,
    /// Storage cost per byte-second.
    #[arg(long, default_value_t = 0.0)]
    pub storage_cost_per_byte_sec: f64,
    /// Memory cost per byte-second.
    #[arg(long, default_value_t = 0.0)]
    pub memory_cost_per_byte_sec: f64,
    /// Generation number.
    #[arg(long, default_value_t = 0)]
    pub generation: u64,
    /// Minimum retention duration from creation (seconds).
    #[arg(long)]
    pub min_retention_secs: Option<i64>,
}

#[derive(clap::ValueEnum, Clone, Copy)]
pub enum DurabilityArg {
    Ephemeral,
    Recomputable,
    Durable,
    Critical,
}

#[derive(clap::ValueEnum, Clone, Copy)]
pub enum SurvivabilityArg {
    Ephemeral,
    Recomputable,
    Durable,
    Critical,
}

#[derive(Subcommand)]
pub enum LineageAction {
    /// Add a lineage edge.
    Add {
        #[arg(long)]
        parent: Uuid,
        #[arg(long)]
        child: Uuid,
        #[arg(long, value_enum)]
        kind: EdgeKindArg,
    },
    /// Remove a lineage edge.
    Remove {
        #[arg(long)]
        parent: Uuid,
        #[arg(long)]
        child: Uuid,
        #[arg(long, value_enum)]
        kind: EdgeKindArg,
    },
}

#[derive(clap::ValueEnum, Clone, Copy)]
pub enum EdgeKindArg {
    DerivesFrom,
    DependsOn,
    Supersedes,
    Duplicates,
}

impl EdgeKindArg {
    fn kind(&self) -> crate::lineage::EdgeKind {
        match self {
            EdgeKindArg::DerivesFrom => crate::lineage::EdgeKind::DerivesFrom,
            EdgeKindArg::DependsOn => crate::lineage::EdgeKind::DependsOn,
            EdgeKindArg::Supersedes => crate::lineage::EdgeKind::Supersedes,
            EdgeKindArg::Duplicates => crate::lineage::EdgeKind::Duplicates,
        }
    }
}

#[derive(Subcommand)]
pub enum PressureAction {
    /// Show current aggregate pressure.
    Get,
    /// Set pressure on the synthetic provider (demo/test).
    Set {
        #[arg(long, value_enum)]
        level: PressureArg,
    },
}

#[derive(clap::ValueEnum, Clone, Copy)]
pub enum PressureArg {
    Normal,
    Elevated,
    High,
    Critical,
}

#[derive(Subcommand)]
pub enum PolicyAction {
    /// List registered policies.
    List,
    /// Inspect a policy by id and version.
    Inspect {
        #[arg(long)]
        id: String,
        #[arg(long)]
        version: String,
    },
    /// Add a policy from a JSON file.
    Add {
        #[arg(long)]
        file: PathBuf,
    },
}

/// Entry point: parse args and dispatch.
pub fn run() -> Result<()> {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(error)
            if matches!(
                error.kind(),
                clap::error::ErrorKind::DisplayHelp | clap::error::ErrorKind::DisplayVersion
            ) =>
        {
            error
                .print()
                .map_err(|e| ReclaimError::Io(format!("writing command help: {e}")))?;
            return Ok(());
        }
        Err(error) => return Err(ReclaimError::InvalidArgument(error.to_string())),
    };
    match &cli.command {
        Command::Coordinator { action } => match action {
            CoordinatorAction::Start(args) => run_coordinator(args),
        },
        Command::Node { action } => match action {
            NodeAction::Start(args) => run_node(args),
        },
        _ => run_client_command(&cli),
    }
}

fn output(cli: &Cli, value: serde_json::Value) {
    if cli.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&value).unwrap_or_else(|_| "{}".into())
        );
    } else {
        match value {
            serde_json::Value::String(s) => println!("{s}"),
            other => println!(
                "{}",
                serde_json::to_string_pretty(&other).unwrap_or_else(|_| "{}".into())
            ),
        }
    }
}

fn call(cli: &Cli, method_name: &str, payload: serde_json::Value) -> Result<serde_json::Value> {
    let mut client = Client::connect(&cli.coordinator, 30_000)?;
    let reply: Reply = client.call(method_name, payload)?;
    let id = reply.id;
    reply.into_result(id)
}

fn run_client_command(cli: &Cli) -> Result<()> {
    let actor = cli.actor.clone();
    match &cli.command {
        Command::Plan {
            object,
            candidates,
            limit,
        } => {
            if *candidates {
                let value = call(
                    cli,
                    method::PLAN_CANDIDATES,
                    json!({"limit": limit, "actor": actor}),
                )?;
                output(cli, value);
            } else {
                let id = object.ok_or_else(|| {
                    ReclaimError::InvalidArgument("plan requires --object or --candidates".into())
                })?;
                let value = call(
                    cli,
                    method::PLAN_OBJECT,
                    json!({"object_id": id, "actor": actor}),
                )?;
                output(cli, value);
            }
        }
        Command::Reclaim { id, force } => {
            let value = call(
                cli,
                method::RECLAIM_OBJECT,
                json!({"object_id": id, "actor": actor, "force": force}),
            )?;
            output(cli, value);
        }
        Command::Compress { id } => {
            let value = call(
                cli,
                method::COMPRESS_OBJECT,
                json!({"object_id": id, "actor": actor}),
            )?;
            output(cli, value);
        }
        Command::Archive { id } => {
            let value = call(
                cli,
                method::ARCHIVE_OBJECT,
                json!({"object_id": id, "actor": actor}),
            )?;
            output(cli, value);
        }
        Command::Restore { id } => {
            let value = call(
                cli,
                method::RESTORE_OBJECT,
                json!({"object_id": id, "actor": actor}),
            )?;
            output(cli, value);
        }
        Command::Verify { id } => {
            let value = call(
                cli,
                method::VERIFY_OBJECT,
                json!({"object_id": id, "actor": actor}),
            )?;
            output(cli, value);
        }
        Command::Object { action } => match action {
            ObjectAction::Create(args) => {
                let value = object_create(cli, args)?;
                output(cli, value);
            }
            ObjectAction::Inspect { id } => {
                let value = call(
                    cli,
                    method::INSPECT_OBJECT,
                    json!({"object_id": id, "actor": actor}),
                )?;
                output(cli, value);
            }
            ObjectAction::Touch { id } => {
                let value = call(
                    cli,
                    method::TOUCH_OBJECT,
                    json!({"object_id": id, "actor": actor}),
                )?;
                output(cli, value);
            }
            ObjectAction::Pin { id } => {
                let value = call(
                    cli,
                    method::PIN_OBJECT,
                    json!({"object_id": id, "actor": actor}),
                )?;
                output(cli, value);
            }
            ObjectAction::Unpin { id } => {
                let value = call(
                    cli,
                    method::UNPIN_OBJECT,
                    json!({"object_id": id, "actor": actor}),
                )?;
                output(cli, value);
            }
            ObjectAction::Lineage { id } => {
                let value = call(
                    cli,
                    method::OBJECT_LINEAGE,
                    json!({"object_id": id, "actor": actor}),
                )?;
                output(cli, value);
            }
            ObjectAction::Dependencies { id } => {
                let value = call(
                    cli,
                    method::OBJECT_DEPENDENCIES,
                    json!({"object_id": id, "actor": actor}),
                )?;
                output(cli, value);
            }
        },
        Command::Lineage { action } => match action {
            LineageAction::Add {
                parent,
                child,
                kind,
            } => {
                let value = call(
                    cli,
                    method::ADD_LINEAGE_EDGE,
                    json!({"parent": parent, "child": child, "kind": kind.kind(), "actor": actor}),
                )?;
                output(cli, value);
            }
            LineageAction::Remove {
                parent,
                child,
                kind,
            } => {
                let value = call(
                    cli,
                    method::REMOVE_LINEAGE_EDGE,
                    json!({"parent": parent, "child": child, "kind": kind.kind(), "actor": actor}),
                )?;
                output(cli, value);
            }
        },
        Command::Pressure { action } => match action {
            PressureAction::Get => {
                let value = call(cli, method::GET_PRESSURE, json!({}))?;
                output(cli, value);
            }
            PressureAction::Set { level } => {
                let value = call(
                    cli,
                    method::SET_PRESSURE,
                    json!({"level": level_name(*level), "actor": actor}),
                )?;
                output(cli, value);
            }
        },
        Command::Policy { action } => match action {
            PolicyAction::List => {
                let value = call(cli, method::POLICY_LIST, json!({}))?;
                output(cli, value);
            }
            PolicyAction::Inspect { id, version } => {
                let value = call(
                    cli,
                    method::POLICY_GET,
                    json!({"id": id, "version": version}),
                )?;
                output(cli, value);
            }
            PolicyAction::Add { file } => {
                let raw = read_bounded_file(
                    file,
                    crate::transport::MAX_FRAME_SIZE as usize,
                    "policy file",
                )?;
                let policy: crate::policy::Policy = serde_json::from_slice(&raw).map_err(|e| {
                    ReclaimError::Policy(format!("invalid policy file {}: {e}", file.display()))
                })?;
                let value = call(cli, method::POLICY_ADD, serde_json::to_value(policy)?)?;
                output(cli, value);
            }
        },
        Command::Audit {
            object,
            action,
            limit,
        } => {
            let value = call(
                cli,
                method::AUDIT_QUERY,
                json!({"object_id": object, "action": action, "limit": limit}),
            )?;
            output(cli, value);
        }
        Command::Failures { limit } => {
            let value = call(cli, method::FAILURES_LIST, json!({"limit": limit}))?;
            output(cli, value);
        }
        Command::Stats => {
            let value = call(cli, method::STATS, json!({}))?;
            output(cli, value);
        }
        Command::Recover => {
            let value = call(cli, method::RECOVER, json!({"actor": actor}))?;
            output(cli, value);
        }
        Command::Shutdown { reason } => {
            let value = call(
                cli,
                method::SHUTDOWN,
                json!({"actor": actor, "reason": reason}),
            )?;
            output(cli, value);
        }
        Command::Coordinator { .. } | Command::Node { .. } => unreachable!(),
    }
    Ok(())
}

fn object_create(cli: &Cli, args: &ObjectCreateArgs) -> Result<serde_json::Value> {
    if !(0.0..=1.0).contains(&args.reuse_probability) {
        return Err(ReclaimError::InvalidArgument(
            "--reuse-probability must be in [0,1]".into(),
        ));
    }
    let id = args.id.unwrap_or_else(Uuid::new_v4);
    let clock = crate::coordinator::SystemClock;
    let now = crate::coordinator::Clock::now_ms(&clock);
    let mut obj = crate::object::ReclaimObject::new(
        id,
        args.generation,
        &args.class,
        args.size.unwrap_or(1024),
        now,
    );
    obj.reuse_probability = args.reuse_probability;
    obj.reuse_horizon_secs = args.reuse_horizon_secs;
    obj.recompute_cost = args.recompute_cost;
    obj.recompute_latency_secs = args.recompute_latency_secs;
    obj.owner = args.owner.clone();
    obj.storage_cost_per_byte_sec = args.storage_cost_per_byte_sec;
    obj.memory_cost_per_byte_sec = args.memory_cost_per_byte_sec;
    obj.pinned = args.pin;
    obj.durability_class = match args.durability {
        Some(DurabilityArg::Ephemeral) => crate::object::DurabilityClass::Ephemeral,
        Some(DurabilityArg::Recomputable) => crate::object::DurabilityClass::Recomputable,
        Some(DurabilityArg::Durable) => crate::object::DurabilityClass::Durable,
        Some(DurabilityArg::Critical) => crate::object::DurabilityClass::Critical,
        None => crate::object::DurabilityClass::Ephemeral,
    };
    obj.survivability_class = match args.survivability {
        Some(SurvivabilityArg::Ephemeral) => crate::object::SurvivabilityClass::Ephemeral,
        Some(SurvivabilityArg::Recomputable) => crate::object::SurvivabilityClass::Recomputable,
        Some(SurvivabilityArg::Durable) => crate::object::SurvivabilityClass::Durable,
        Some(SurvivabilityArg::Critical) => crate::object::SurvivabilityClass::Critical,
        None => crate::object::SurvivabilityClass::Ephemeral,
    };
    if let Some(secs) = args.min_retention_secs {
        obj.min_retention_deadline_ms = Some(retention_deadline_ms(now, secs)?);
    }

    let (payload_b64, backend) = match &args.data_file {
        Some(path) => {
            let data = read_bounded_file(path, MAX_INLINE_PAYLOAD_SIZE, "payload file")?;
            if args.size.is_none() {
                obj.logical_size = u64::try_from(data.len()).map_err(|_| {
                    ReclaimError::InvalidArgument("payload length exceeds u64".into())
                })?;
            }
            (
                Some(crate::base64_payload(&data)),
                Some(args.backend.clone()),
            )
        }
        None => (None, None),
    };
    let req_has_payload = payload_b64.is_some();

    let req = crate::protocol::CreateObjectRequest {
        object: obj,
        payload_b64,
        target_backend: backend.filter(|_| req_has_payload),
        replicate_to: vec![],
    };
    let value = call(
        cli,
        method::CREATE_OBJECT,
        serde_json::to_value(req).map_err(|e| ReclaimError::Internal(format!("json: {e}")))?,
    )?;
    Ok(value)
}

fn retention_deadline_ms(now_ms: i64, duration_secs: i64) -> Result<i64> {
    if duration_secs < 0 {
        return Err(ReclaimError::InvalidArgument(
            "--min-retention-secs must be non-negative".into(),
        ));
    }
    let duration_ms = duration_secs
        .checked_mul(1_000)
        .ok_or_else(|| ReclaimError::InvalidArgument("--min-retention-secs is too large".into()))?;
    now_ms
        .checked_add(duration_ms)
        .ok_or_else(|| ReclaimError::InvalidArgument("--min-retention-secs is too large".into()))
}

fn level_name(level: PressureArg) -> &'static str {
    match level {
        PressureArg::Normal => "NORMAL",
        PressureArg::Elevated => "ELEVATED",
        PressureArg::High => "HIGH",
        PressureArg::Critical => "CRITICAL",
    }
}

// ----------------------------------------------------------------------
// Runtime entry points (foreground)
// ----------------------------------------------------------------------

fn run_coordinator(args: &CoordinatorStartArgs) -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let backends = crate::backends::BackendRegistry::new();
    backends.register(Arc::new(crate::backends::MemoryBackend::new("memory")))?;
    if let Some(dir) = &args.data_dir {
        let id = format!("file:{}", dir.display());
        let backend = crate::backends::FileBackend::new(&id, dir)?;
        backends.register(Arc::new(backend))?;
    }
    let pressure = crate::pressure::PressureRegistry::new();
    pressure.register_synthetic("synthetic");

    let archives: Vec<Arc<dyn crate::archive::ArchiveBackend>> = match &args.archive_dir {
        Some(dir) => {
            let id = format!("local-fs:{}", dir.display());
            vec![Arc::new(crate::archive::LocalFsArchive::new(&id, dir)?)]
        }
        None => Vec::new(),
    };

    let config = crate::coordinator::CoordinatorConfig {
        store_path: args.store.clone(),
        process_id: format!("coordinator-{}", std::process::id()),
        reservation_ttl_ms: 60_000,
        node_heartbeat_timeout_ms: 30_000,
        node_addr: Some(args.bind.clone()),
    };
    let coordinator = Arc::new(crate::coordinator::Coordinator::open(
        config,
        backends,
        pressure,
        archives,
        Arc::new(crate::coordinator::SystemClock),
    )?);

    log::info!(
        "Reclaim Fabric {} coordinator: store={} epoch={}",
        env!("CARGO_PKG_VERSION"),
        args.store,
        coordinator.epoch()
    );

    let handler = build_handler(coordinator.clone());
    let server = crate::transport::Server::new(
        crate::transport::ServerConfig {
            bind_addr: args.bind.clone(),
            max_connections: 64,
            timeout_ms: 30_000,
            shutdown_poll_ms: 100,
        },
        handler,
    )?;
    let listen_addr = server.local_addr()?;
    log::info!("coordinator listening on {listen_addr}");

    let (stop_tx, stop_rx) = std::sync::mpsc::channel::<()>();
    let server_arc = Arc::new(server);
    {
        let server = server_arc.clone();
        ctrlc::set_handler(move || {
            log::info!("shutdown requested (signal)");
            server.request_shutdown();
            let _ = stop_tx.send(());
        })
        .map_err(|e| ReclaimError::Io(format!("installing ctrl-c handler: {e}")))?;
    }
    let serve_server = server_arc.clone();
    let serve_handle = std::thread::spawn(move || serve_server.serve());

    // Signal readiness to tests/parents on stdout.
    {
        let stdout = std::io::stdout();
        let mut stdout = stdout.lock();
        writeln!(stdout, "READY {listen_addr}")?;
        stdout.flush()?;
    }

    // Block until signal/transport shutdown, but also notice an unexpected
    // server exit so a listener failure cannot strand the foreground process.
    loop {
        match stop_rx.recv_timeout(std::time::Duration::from_millis(100)) {
            Ok(()) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
        }
        if coordinator.shutdown_requested() || serve_handle.is_finished() {
            break;
        }
    }
    server_arc.request_shutdown();

    let serve_result = serve_handle
        .join()
        .map_err(|_| ReclaimError::Internal("coordinator server thread panicked".into()))
        .and_then(|result| result);
    // Perform both cleanup operations even when serving failed.  In
    // particular, a cleanly-owned store claim must not be left live because a
    // listener returned an error.
    let retire_result = coordinator.retire_stale_nodes();
    let release_result = coordinator.release();
    serve_result?;
    retire_result?;
    release_result?;
    log::info!("coordinator stopped cleanly");
    Ok(())
}

fn run_node(args: &NodeStartArgs) -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let backends = crate::backends::BackendRegistry::new();
    backends.register(Arc::new(crate::backends::MemoryBackend::new("memory")))?;
    let file_id = format!("file:{}", args.data_dir.display());
    let file_backend = crate::backends::FileBackend::new(&file_id, &args.data_dir)?;
    backends.register(Arc::new(file_backend))?;

    let config = crate::node::NodeConfig {
        name: args.name.clone(),
        process_id: format!("node-proc-{}", std::process::id()),
        coordinator_addr: args.coordinator.clone(),
        bind_addr: args.bind.clone(),
        heartbeat_interval_ms: 2_000,
    };
    let node = crate::node::Node::start(config, backends)?;
    log::info!("Reclaim Fabric node {} started", node.node_id);

    let (stop_tx, stop_rx) = std::sync::mpsc::channel::<()>();
    ctrlc::set_handler(move || {
        log::info!("shutdown requested");
        let _ = stop_tx.send(());
    })
    .map_err(|e| ReclaimError::Io(format!("installing ctrl-c handler: {e}")))?;

    let listen_addr = node.local_addr()?;
    {
        let stdout = std::io::stdout();
        let mut stdout = stdout.lock();
        writeln!(stdout, "READY {listen_addr}")?;
        stdout.flush()?;
    }

    let mut runtime_failed = false;
    loop {
        match stop_rx.recv_timeout(std::time::Duration::from_millis(100)) {
            Ok(()) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
        }
        if node.shutdown_requested() {
            runtime_failed = true;
            break;
        }
    }
    node.shutdown();
    if runtime_failed {
        return Err(ReclaimError::Internal(
            "node operation server stopped unexpectedly".into(),
        ));
    }
    log::info!("node stopped cleanly");
    Ok(())
}

/// Build the coordinator's transport request handler.
pub fn build_handler(
    coordinator: Arc<crate::coordinator::Coordinator>,
) -> crate::transport::RequestHandler {
    use crate::transport::Request;

    Arc::new(move |req: Request| {
        let (id, result) = dispatch(&coordinator, req);
        match result {
            Ok(payload) => Reply::ok(id, payload),
            Err(e) => Reply::err(id, e),
        }
    })
}

/// Dispatch a request to the coordinator, returning (request id, result).
fn dispatch(
    coordinator: &crate::coordinator::Coordinator,
    req: crate::transport::Request,
) -> (u64, Result<serde_json::Value>) {
    let id = req.id;
    (id, dispatch_one(coordinator, req))
}

fn dispatch_one(
    coordinator: &crate::coordinator::Coordinator,
    req: crate::transport::Request,
) -> Result<serde_json::Value> {
    use crate::protocol::method::*;
    let result: Result<serde_json::Value> = match req.method.as_str() {
        CREATE_OBJECT => {
            let payload: crate::protocol::CreateObjectRequest = serde_json::from_value(req.payload)
                .map_err(|e| ReclaimError::Protocol(format!("bad create payload: {e}")))?;
            let obj = coordinator.create_object(&payload)?;
            Ok(json!(obj))
        }
        INSPECT_OBJECT => {
            let p: crate::protocol::ObjectIdRequest = serde_json::from_value(req.payload)
                .map_err(|e| ReclaimError::Protocol(format!("bad payload: {e}")))?;
            let obj = coordinator.store()?.require_object(&p.object_id)?;
            Ok(json!(obj))
        }
        TOUCH_OBJECT => {
            let p: crate::protocol::ObjectIdRequest = serde_json::from_value(req.payload)
                .map_err(|e| ReclaimError::Protocol(format!("bad payload: {e}")))?;
            let obj = coordinator.touch(&p.object_id, &p.actor)?;
            Ok(json!(obj))
        }
        PIN_OBJECT => {
            let p: crate::protocol::ObjectIdRequest = serde_json::from_value(req.payload)
                .map_err(|e| ReclaimError::Protocol(format!("bad payload: {e}")))?;
            let obj = coordinator.pin(&p.object_id, &p.actor)?;
            Ok(json!(obj))
        }
        UNPIN_OBJECT => {
            let p: crate::protocol::ObjectIdRequest = serde_json::from_value(req.payload)
                .map_err(|e| ReclaimError::Protocol(format!("bad payload: {e}")))?;
            let obj = coordinator.unpin(&p.object_id, &p.actor)?;
            Ok(json!(obj))
        }
        SET_PROTECTED => {
            let p: SetProtectedRequest = serde_json::from_value(req.payload)
                .map_err(|e| ReclaimError::Protocol(format!("bad payload: {e}")))?;
            let obj = coordinator.set_protected(&p.object_id, p.protected, &p.actor)?;
            Ok(json!(obj))
        }
        OBJECT_LINEAGE | OBJECT_DEPENDENCIES => {
            let p: crate::protocol::ObjectIdRequest = serde_json::from_value(req.payload)
                .map_err(|e| ReclaimError::Protocol(format!("bad payload: {e}")))?;
            let graph = coordinator.lineage()?;
            let mut neighbors = graph.neighbors(p.object_id);
            if req.method == OBJECT_DEPENDENCIES {
                neighbors.retain(|edge| edge.kind == crate::lineage::EdgeKind::DependsOn);
            }
            Ok(json!({
                "object_id": p.object_id.to_string(),
                "edges": neighbors,
            }))
        }
        ADD_LINEAGE_EDGE | REMOVE_LINEAGE_EDGE => {
            let p: crate::protocol::LineageRequest = serde_json::from_value(req.payload)
                .map_err(|e| ReclaimError::Protocol(format!("bad payload: {e}")))?;
            if req.method == ADD_LINEAGE_EDGE {
                coordinator.add_lineage(p.parent, p.child, p.kind, &p.actor)?;
            } else {
                coordinator.remove_lineage(p.parent, p.child, p.kind, &p.actor)?;
            }
            Ok(json!({"ok": true}))
        }
        PLAN_OBJECT => {
            let p: crate::protocol::PlanRequest = serde_json::from_value(req.payload)
                .map_err(|e| ReclaimError::Protocol(format!("bad payload: {e}")))?;
            let decision = coordinator.plan(&p.object_id, &p.actor)?;
            Ok(json!(decision))
        }
        PLAN_CANDIDATES => {
            let p: crate::protocol::CandidatesRequest = serde_json::from_value(req.payload)
                .map_err(|e| ReclaimError::Protocol(format!("bad payload: {e}")))?;
            let candidates = coordinator.candidates(validate_query_limit(p.limit)?, &p.actor)?;
            Ok(json!(candidates))
        }
        RECLAIM_OBJECT => {
            let p: crate::protocol::ReclaimRequest = serde_json::from_value(req.payload)
                .map_err(|e| ReclaimError::Protocol(format!("bad payload: {e}")))?;
            let report = coordinator.reclaim(&p)?;
            Ok(json!(report))
        }
        COMPRESS_OBJECT => {
            let p: crate::protocol::ObjectIdRequest = serde_json::from_value(req.payload)
                .map_err(|e| ReclaimError::Protocol(format!("bad payload: {e}")))?;
            let result = coordinator.compress(&p.object_id, &p.actor)?;
            Ok(json!(result))
        }
        ARCHIVE_OBJECT => {
            let p: crate::protocol::ObjectIdRequest = serde_json::from_value(req.payload)
                .map_err(|e| ReclaimError::Protocol(format!("bad payload: {e}")))?;
            let record = coordinator.archive(&p.object_id, &p.actor)?;
            Ok(json!(record))
        }
        RESTORE_OBJECT => {
            let p: crate::protocol::ObjectIdRequest = serde_json::from_value(req.payload)
                .map_err(|e| ReclaimError::Protocol(format!("bad payload: {e}")))?;
            let obj = coordinator.restore(&p.object_id, &p.actor)?;
            Ok(json!(obj))
        }
        VERIFY_OBJECT => {
            let p: crate::protocol::ObjectIdRequest = serde_json::from_value(req.payload)
                .map_err(|e| ReclaimError::Protocol(format!("bad payload: {e}")))?;
            let result = coordinator.verify(&p.object_id, &p.actor)?;
            Ok(result)
        }
        GET_PRESSURE => {
            let metrics = coordinator.pressure()?;
            let level = metrics.level();
            Ok(json!({
                "level": level.as_str(),
                "metrics": metrics,
            }))
        }
        SET_PRESSURE => {
            let p: PressureSetRequest = serde_json::from_value(req.payload)
                .map_err(|e| ReclaimError::Protocol(format!("bad payload: {e}")))?;
            let level = crate::pressure::PressureLevel::parse(&p.level)?;
            coordinator.set_pressure("synthetic", level)?;
            Ok(json!({"ok": true, "level": level.as_str()}))
        }
        POLICY_LIST => {
            let registry = coordinator.policy_registry()?;
            Ok(json!(registry.list()))
        }
        POLICY_GET => {
            let p: PolicyGetRequest = serde_json::from_value(req.payload)
                .map_err(|e| ReclaimError::Protocol(format!("bad payload: {e}")))?;
            let registry = coordinator.policy_registry()?;
            let policy = registry.get(&p.id, &p.version)?;
            Ok(json!(policy))
        }
        POLICY_ADD => {
            let policy: crate::policy::Policy = serde_json::from_value(req.payload)
                .map_err(|e| ReclaimError::Policy(format!("bad policy json: {e}")))?;
            coordinator.add_policy(policy)?;
            Ok(json!({"ok": true}))
        }
        AUDIT_QUERY => {
            let p: AuditQueryRequest = serde_json::from_value(req.payload)
                .map_err(|e| ReclaimError::Protocol(format!("bad payload: {e}")))?;
            let entries = coordinator.audit(
                p.object_id.as_ref(),
                p.action.as_deref(),
                validate_query_limit(p.limit)?,
            )?;
            Ok(json!({"entries": entries, "count": entries.len()}))
        }
        FAILURES_LIST => {
            let p: FailureListRequest = serde_json::from_value(req.payload)
                .map_err(|e| ReclaimError::Protocol(format!("bad payload: {e}")))?;
            let failures = coordinator.failures(validate_query_limit(p.limit)?)?;
            Ok(json!(failures))
        }
        STATS => coordinator.stats(),
        RECOVER => {
            let report = coordinator.recover()?;
            Ok(json!(report))
        }
        SHUTDOWN => {
            let p: crate::protocol::ShutdownRequest = serde_json::from_value(req.payload)
                .map_err(|e| ReclaimError::Protocol(format!("bad payload: {e}")))?;
            log::info!(
                "shutdown requested via transport by {}: {}",
                p.actor,
                p.reason
            );
            coordinator.request_shutdown();
            Ok(json!({"ok": true}))
        }
        NODE_REGISTER => {
            let p: crate::protocol::NodeRegisterRequest = serde_json::from_value(req.payload)
                .map_err(|e| ReclaimError::Protocol(format!("bad payload: {e}")))?;
            let reply = coordinator.node_register(&p)?;
            Ok(json!(reply))
        }
        NODE_HEARTBEAT => {
            let node_id: String =
                serde_json::from_value(req.payload.get("node_id").cloned().unwrap_or_default())
                    .map_err(|e| ReclaimError::Protocol(format!("bad payload: {e}")))?;
            let epoch = coordinator.node_heartbeat(&node_id)?;
            Ok(json!({"epoch": epoch}))
        }
        NODE_LIST => Ok(json!(coordinator.nodes()?)),
        NODE_REPORT_PRESSURE => {
            let node_id: String =
                serde_json::from_value(req.payload.get("node_id").cloned().unwrap_or_default())
                    .map_err(|e| ReclaimError::Protocol(format!("bad payload: {e}")))?;
            let metrics: crate::pressure::PressureMetrics =
                serde_json::from_value(req.payload.get("metrics").cloned().unwrap_or_default())
                    .map_err(|e| ReclaimError::Protocol(format!("bad payload: {e}")))?;
            coordinator.node_report_pressure(&node_id, metrics)?;
            Ok(json!({"ok": true}))
        }
        other => Err(ReclaimError::Protocol(format!("unknown method {other}"))),
    };
    result
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::json;

    use super::{dispatch_one, retention_deadline_ms, MAX_QUERY_LIMIT};
    use crate::backends::{BackendRegistry, MemoryBackend};
    use crate::coordinator::{Coordinator, CoordinatorConfig, SystemClock};
    use crate::errors::ReclaimError;
    use crate::pressure::{PressureLevel, PressureMetrics, PressureRegistry};
    use crate::protocol::method;
    use crate::transport::Request;

    fn coordinator() -> Coordinator {
        let backends = BackendRegistry::new();
        backends
            .register(Arc::new(MemoryBackend::new("memory")))
            .unwrap();
        let config = CoordinatorConfig {
            store_path: ":memory:".into(),
            process_id: "cli-test".into(),
            ..CoordinatorConfig::default()
        };
        Coordinator::open(
            config,
            backends,
            PressureRegistry::new(),
            vec![],
            Arc::new(SystemClock),
        )
        .expect("test coordinator")
    }

    fn request(method: &str, payload: serde_json::Value) -> Request {
        Request {
            id: 1,
            method: method.into(),
            payload,
        }
    }

    #[test]
    fn retention_duration_rejects_negative_and_overflowing_values() {
        assert_eq!(retention_deadline_ms(1_000, 2).unwrap(), 3_000);
        assert!(matches!(
            retention_deadline_ms(1_000, -1),
            Err(ReclaimError::InvalidArgument(_))
        ));
        assert!(matches!(
            retention_deadline_ms(i64::MAX, 1),
            Err(ReclaimError::InvalidArgument(_))
        ));
        assert!(matches!(
            retention_deadline_ms(0, i64::MAX),
            Err(ReclaimError::InvalidArgument(_))
        ));
    }

    #[test]
    fn malformed_query_fields_are_rejected_instead_of_silently_defaulted() {
        let coordinator = coordinator();
        for req in [
            request(
                method::AUDIT_QUERY,
                json!({"object_id": "not-a-uuid", "limit": 50}),
            ),
            request(method::AUDIT_QUERY, json!({"limit": "50"})),
            request(method::FAILURES_LIST, json!({"limit": "50"})),
        ] {
            assert!(matches!(
                dispatch_one(&coordinator, req),
                Err(ReclaimError::Protocol(_))
            ));
        }
    }

    #[test]
    fn pathological_query_limits_are_bounded() {
        let coordinator = coordinator();
        for req in [
            request(method::AUDIT_QUERY, json!({"limit": MAX_QUERY_LIMIT + 1})),
            request(method::FAILURES_LIST, json!({"limit": MAX_QUERY_LIMIT + 1})),
            request(
                method::PLAN_CANDIDATES,
                json!({"limit": MAX_QUERY_LIMIT + 1, "actor": "test"}),
            ),
        ] {
            assert!(matches!(
                dispatch_one(&coordinator, req),
                Err(ReclaimError::InvalidArgument(_))
            ));
        }
    }

    #[test]
    fn pressure_reply_level_is_derived_from_the_returned_metrics_snapshot() {
        let coordinator = coordinator();
        coordinator
            .register_synthetic_pressure("synthetic")
            .unwrap();
        coordinator
            .set_pressure("synthetic", PressureLevel::High)
            .unwrap();
        let reply = dispatch_one(&coordinator, request(method::GET_PRESSURE, json!({}))).unwrap();
        let metrics: PressureMetrics = serde_json::from_value(reply["metrics"].clone()).unwrap();
        assert_eq!(reply["level"], metrics.level().as_str());
    }
}
