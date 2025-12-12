mod command;
mod config;
mod pool;
mod protocol;
mod proxy;
mod stats;

use anyhow::{Context, Result};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::signal;
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use config::Config;
use pool::RedisCluster;
use proxy::ProxyHandler;
use stats::Statistics;

#[tokio::main]
async fn main() -> Result<()> {
    let config = load_config()?;

    init_logging(&config.proxy.log_level)?;

    config.validate().context("invalid configuration")?;

    info!("========================================");
    info!("Starting Redis Proxy Server");
    info!("========================================");
    info!("Proxy Configuration:");
    info!("  - Listening on: {}", config.proxy.bind_addr);
    info!("  - Connection pool size: {}", config.proxy.pool_size);
    info!("  - Log level: {}", config.proxy.log_level);
    info!("");
    info!("Redis Cluster Configuration:");
    info!("  - Master: {}", config.redis.master);

    if !config.redis.slaves.is_empty() {
        info!("  - Slaves ({} nodes):", config.redis.slaves.len());
        for (idx, slave) in config.redis.slaves.iter().enumerate() {
            info!("    [{}] {}", idx + 1, slave);
        }
        info!("  - Read/Write splitting: ENABLED");
        info!("    * Write operations → Master");
        info!("    * Read operations → Slaves (round-robin)");
    } else {
        info!("  - Slaves: None configured");
        info!("  - Read/Write splitting: DISABLED");
        info!("    * All operations → Master");
    }

    let cluster = Arc::new(RedisCluster::new(
        config.redis.master.clone(),
        config.redis.slaves.clone(),
        config.proxy.pool_size,
    ));

    let stats = Statistics::new();
    let handler = Arc::new(ProxyHandler::new(cluster, stats.clone()));

    let listener = TcpListener::bind(&config.proxy.bind_addr)
        .await
        .context("failed to bind to address")?;

    info!("");
    info!("========================================");
    info!("Redis Proxy Server Started Successfully!");
    info!("========================================");
    info!("Ready to accept connections on {}", config.proxy.bind_addr);

    let stats_task = tokio::spawn(stats_reporter(stats.clone()));

    loop {
        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((stream, addr)) => {
                        let handler = handler.clone();
                        let client_addr = addr.to_string();
                        tokio::spawn(async move {
                            handler.handle_client(stream, client_addr).await;
                        });
                    }
                    Err(e) => {
                        error!("failed to accept connection: {}", e);
                    }
                }
            }
            _ = signal::ctrl_c() => {
                info!("received shutdown signal");
                break;
            }
        }
    }

    stats_task.abort();
    stats.print_stats();
    info!("Redis proxy server stopped");

    Ok(())
}

fn load_config() -> Result<Config> {
    let config_path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "config.toml".to_string());

    if std::path::Path::new(&config_path).exists() {
        info!("loading configuration from: {}", config_path);
        Config::from_file(&config_path)
    } else {
        info!("configuration file not found, using default configuration");
        info!("to create a config file, run: redis-proxy --generate-config");

        if std::env::args().any(|arg| arg == "--generate-config") {
            let config = Config::default();
            config.save_to_file("config.toml")?;
            info!("generated default configuration file: config.toml");
            std::process::exit(0);
        }

        Ok(Config::default())
    }
}

fn init_logging(log_level: &str) -> Result<()> {
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(log_level));

    tracing_subscriber::registry()
        .with(env_filter)
        .with(tracing_subscriber::fmt::layer())
        .init();

    Ok(())
}

async fn stats_reporter(stats: Arc<Statistics>) {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(60));
    loop {
        interval.tick().await;
        let global = stats.get_global_stats();
        info!(
            "stats: commands={} (read={}, write={}), errors={}, connections={}/{}",
            global.total_commands,
            global.total_read_commands,
            global.total_write_commands,
            global.total_errors,
            global.connections_active,
            global.connections_received
        );
    }
}
