use alloy::providers::{Provider, ProviderBuilder, WsConnect};
use anyhow::{Context, Result, bail};
use liquidation_engine::{cache, chain, config, store};
use sqlx::postgres::PgPoolOptions;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    if std::path::Path::new(".env").exists() {
        dotenvy::dotenv().context("load .env")?;
    }
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .json()
        .init();

    let (config_path, observe_only, end_block) = parse_args()?;
    if !observe_only {
        bail!(
            "only observe-only mode is supported; transaction signing/broadcast is intentionally unavailable"
        );
    }
    let config = config::Config::load(&config_path).context("invalid searcher configuration")?;
    let provider = tokio::time::timeout(
        config.rpc_timeout,
        ProviderBuilder::new().connect_ws(WsConnect::new(config.rpc_ws_url.clone())),
    )
    .await
    .context("RPC WebSocket connection timed out")?
    .context("connect to RPC WebSocket")?;
    let chain_id = tokio::time::timeout(config.rpc_timeout, provider.get_chain_id())
        .await
        .context("chain ID request timed out")?
        .context("read RPC chain ID")?;
    if chain_id != config.deployment.chain_id {
        bail!(
            "RPC chain ID {chain_id} differs from configured {}",
            config.deployment.chain_id
        );
    }
    let mut contracts = vec![
        config.deployment.pool,
        config.pool_configurator,
        config.oracle,
        config.protocol_data_provider,
    ];
    contracts.extend(config.oracle_feeds.iter().map(|feed| feed.feed));
    contracts.sort();
    contracts.dedup();
    for address in contracts {
        let code = tokio::time::timeout(config.rpc_timeout, provider.get_code_at(address))
            .await
            .context("contract code lookup timed out")?
            .context("read configured contract code")?;
        if code.is_empty() {
            bail!("configured Aave contract {address:#x} has no code on chain {chain_id}");
        }
    }

    let database = PgPoolOptions::new()
        .max_connections(10)
        .acquire_timeout(config.database_timeout)
        .connect(&config.database_url)
        .await
        .context("connect to PostgreSQL")?;
    sqlx::migrate!()
        .run(&database)
        .await
        .context("apply database migrations")?;
    let store = store::Store::new(database);
    store.ensure_deployment(&config.deployment).await?;

    let redis_client = redis::Client::open(config.redis_url.as_str()).context("parse Redis URL")?;
    let redis = redis_client
        .get_multiplexed_async_connection()
        .await
        .context("connect to Redis")?;
    let mut cache = cache::CandidateCache::new(redis);
    cache
        .rebuild(&store)
        .await
        .context("rebuild Redis risk indexes")?;
    info!(chain_id, pool = %format!("{:#x}",config.deployment.pool), protocol_revision = %config.deployment.protocol_revision, start_block = config.deployment.start_block, observe_only, "Aave event indexer started");
    chain::run(provider, config, store, cache, end_block).await
}

fn parse_args() -> Result<(String, bool, Option<u64>)> {
    let mut args = std::env::args().skip(1);
    let mut config = None;
    let mut observe_only = true;
    let mut end_block = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => config = Some(args.next().context("--config requires a path")?),
            "--observe-only" => observe_only = true,
            "--end-block" => {
                let value = args.next().context("--end-block requires a block number")?;
                end_block = Some(
                    value
                        .parse::<u64>()
                        .context("--end-block must be a decimal block number")?,
                );
            }
            "--help" | "-h" => {
                println!(
                    "Usage: liquidation-engine --config config/arbitrum.toml [--observe-only] [--end-block BLOCK]\n--end-block performs a bounded canonical sync and exits.\nObserve-only is the default; transaction submission is not implemented."
                );
                std::process::exit(0);
            }
            other => bail!("unknown argument: {other}"),
        }
    }
    Ok((
        config.context("--config is required")?,
        observe_only,
        end_block,
    ))
}
