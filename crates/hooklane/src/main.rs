//! The `hooklane` binary.
//!
//! ```text
//! hooklane --role api | relay | scheduler | worker
//! hooklane migrate
//! hooklane admin create-tenant --name <name>
//! hooklane admin gen-key            # print a fresh SECRET_ENCRYPTION_KEY
//! ```
//!
//! One binary, four roles: every role ships in the same Docker image and
//! Kubernetes just runs it with a different `--role`. That keeps builds,
//! versions and deploys in lockstep.

use std::sync::Arc;

use clap::{Parser, Subcommand, ValueEnum};

use hooklane::config::Config;
use hooklane::{admin, api, crypto, db, relay, scheduler, shutdown, telemetry, worker};

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Role {
    Api,
    Relay,
    Scheduler,
    Worker,
}

impl Role {
    fn as_str(&self) -> &'static str {
        match self {
            Role::Api => "api",
            Role::Relay => "relay",
            Role::Scheduler => "scheduler",
            Role::Worker => "worker",
        }
    }
}

#[derive(Debug, Parser)]
#[command(
    name = "hooklane",
    version,
    about = "Outbound webhook delivery service"
)]
struct Cli {
    /// Which long-running role this process plays.
    #[arg(long, env = "HOOKLANE_ROLE", value_enum)]
    role: Option<Role>,

    #[command(subcommand)]
    command: Option<Command>,

    #[command(flatten)]
    config: Config,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run database migrations and create the Kafka topic if missing.
    Migrate {
        /// Kafka topic replication factor (1 for a single-node broker).
        #[arg(long, env = "KAFKA_REPLICATION", default_value_t = 1)]
        replication: i32,
    },
    /// Administrative commands.
    Admin {
        #[command(subcommand)]
        command: AdminCommand,
    },
}

#[derive(Debug, Subcommand)]
enum AdminCommand {
    /// Create a tenant; prints its id and an API key (shown only once).
    CreateTenant {
        #[arg(long)]
        name: String,
        /// Print `TENANT_ID=... API_KEY=...` lines for scripts.
        #[arg(long)]
        env_format: bool,
    },
    /// Print a new random SECRET_ENCRYPTION_KEY.
    GenKey,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let config = Arc::new(cli.config);

    if let Some(Command::Admin {
        command: AdminCommand::GenKey,
    }) = &cli.command
    {
        println!("{}", crypto::generate_encryption_key());
        return Ok(());
    }

    let role_name = match (&cli.command, cli.role) {
        (Some(Command::Migrate { .. }), _) => "migrate",
        (Some(Command::Admin { .. }), _) => "admin",
        (None, Some(role)) => role.as_str(),
        (None, None) => {
            anyhow::bail!("specify --role api|relay|scheduler|worker, or a subcommand (see --help)")
        }
    };

    // Telemetry is set up before the async runtime starts (see telemetry.rs).
    let telemetry = telemetry::init(&config, role_name)?;
    if config.allow_private_endpoints {
        tracing::warn!(
            "ALLOW_PRIVATE_ENDPOINTS=true: SSRF protection for private IP ranges is DISABLED. \
             This is only acceptable on an isolated local network. Never use it in production."
        );
    }

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let result = runtime.block_on(run(cli.command, cli.role, config));
    // Let background tasks wind down, then flush telemetry.
    runtime.shutdown_timeout(std::time::Duration::from_secs(2));
    telemetry.shutdown();
    if let Err(e) = &result {
        eprintln!("error: {e:#}");
    }
    result
}

async fn run(
    command: Option<Command>,
    role: Option<Role>,
    config: Arc<Config>,
) -> anyhow::Result<()> {
    let pool = db::connect_with_retry(&config.database_url, config.database_max_connections).await;

    match command {
        Some(Command::Migrate { replication }) => {
            return admin::migrate(&config, &pool, replication).await;
        }
        Some(Command::Admin {
            command: AdminCommand::CreateTenant { name, env_format },
        }) => {
            let (tenant_id, key) = admin::create_tenant(&pool, &name).await?;
            if env_format {
                println!("TENANT_ID={tenant_id}");
                println!("API_KEY={key}");
            } else {
                println!("tenant id: {tenant_id}");
                println!("api key:   {key}");
                println!("(the API key is shown only once; store it now)");
            }
            return Ok(());
        }
        Some(Command::Admin {
            command: AdminCommand::GenKey,
        }) => return Ok(()),
        None => {}
    }

    let shutdown = shutdown::on_signal();
    // Non-API roles get a tiny health endpoint for Kubernetes probes. It
    // answers as long as the process (and its Tokio runtime) is alive.
    if !matches!(role, Some(Role::Api)) && !config.health_addr.is_empty() {
        tokio::spawn(serve_health(config.health_addr.clone(), shutdown.clone()));
    }
    match role {
        Some(Role::Api) => api::run(config, pool, shutdown).await,
        Some(Role::Relay) => relay::run(config, pool, shutdown).await,
        Some(Role::Scheduler) => scheduler::run(config, pool, shutdown).await,
        Some(Role::Worker) => worker::run(config, pool, shutdown).await,
        None => Ok(()),
    }
}

async fn serve_health(addr: String, shutdown: tokio_util::sync::CancellationToken) {
    let app = axum::Router::new().route("/healthz", axum::routing::get(|| async { "ok" }));
    match tokio::net::TcpListener::bind(&addr).await {
        Ok(listener) => {
            let _ = axum::serve(listener, app)
                .with_graceful_shutdown(async move { shutdown.cancelled().await })
                .await;
        }
        Err(e) => tracing::warn!(%addr, error = %e, "could not start health endpoint"),
    }
}
