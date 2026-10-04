use std::{collections::HashMap, env, fs, path::Path, str::FromStr, time::Duration};

use alloy::primitives::Address;
use anyhow::{Context, Result, bail};
use bigdecimal::BigDecimal;
use serde::Deserialize;

#[derive(Clone, Debug)]
pub struct DeploymentMetadata {
    pub chain_id: u64,
    pub pool: Address,
    pub protocol_revision: String,
    pub start_block: u64,
}

#[derive(Clone, Debug)]
pub struct OracleFeed {
    pub asset: Address,
    pub feed: Address,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub rpc_ws_url: String,
    pub deployment: DeploymentMetadata,
    pub pool_configurator: Address,
    pub oracle: Address,
    pub protocol_data_provider: Address,
    pub simulation_caller: Address,
    pub native_asset: Address,
    pub gas_price_gwei: BigDecimal,
    pub min_profit_usd: BigDecimal,
    pub estimated_gas_units: u64,
    pub dex_slippage_bps: u16,
    pub flashloan_fee_bps: u16,
    pub execution_risk_bps: u16,
    pub oracle_feeds: Vec<OracleFeed>,
    pub database_url: String,
    pub redis_url: String,
    pub rpc_timeout: Duration,
    pub database_timeout: Duration,
    pub max_candidate_refresh_per_block: usize,
}

#[derive(Deserialize)]
struct FileConfig {
    chain_id: u64,
    pool: String,
    pool_configurator: String,
    oracle: String,
    protocol_data_provider: String,
    protocol_revision: String,
    start_block: u64,
    oracle_feeds: Option<Vec<FileOracleFeed>>,
    rpc_timeout_seconds: Option<u64>,
    database_timeout_seconds: Option<u64>,
    native_asset: String,
    gas_price_gwei: Option<String>,
    min_profit_usd: Option<String>,
    estimated_gas_units: Option<u64>,
    dex_slippage_bps: Option<u16>,
    flashloan_fee_bps: Option<u16>,
    execution_risk_bps: Option<u16>,
    max_candidate_refresh_per_block: Option<usize>,
}

#[derive(Deserialize)]
struct FileOracleFeed {
    asset: String,
    feed: String,
}

impl Config {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let file = read_file_config(path)?;
        let pool = parse_address("pool", &file.pool)?;
        let pool_configurator = parse_address("pool_configurator", &file.pool_configurator)?;
        let oracle = parse_address("oracle", &file.oracle)?;
        let protocol_data_provider =
            parse_address("protocol_data_provider", &file.protocol_data_provider)?;
        let native_asset = parse_address("native_asset", &file.native_asset)?;
        let oracle_feeds = file
            .oracle_feeds
            .unwrap_or_default()
            .into_iter()
            .map(|entry| {
                Ok(OracleFeed {
                    asset: parse_address("oracle_feeds.asset", &entry.asset)?,
                    feed: parse_address("oracle_feeds.feed", &entry.feed)?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut seen = HashMap::new();
        for entry in &oracle_feeds {
            if seen.insert(entry.feed, entry.asset).is_some() {
                bail!("oracle feed address is configured more than once");
            }
        }
        let rpc_timeout = file.rpc_timeout_seconds.unwrap_or(10);
        let database_timeout = file.database_timeout_seconds.unwrap_or(5);
        if rpc_timeout == 0 || database_timeout == 0 {
            bail!("RPC and database timeouts must be greater than zero");
        }
        let gas_price_gwei = parse_nonnegative_decimal(
            "gas_price_gwei",
            file.gas_price_gwei.as_deref().unwrap_or("0.1"),
        )?;
        let min_profit_usd = parse_nonnegative_decimal(
            "min_profit_usd",
            file.min_profit_usd.as_deref().unwrap_or("1"),
        )?;
        let estimated_gas_units = file.estimated_gas_units.unwrap_or(500_000);
        if estimated_gas_units == 0 {
            bail!("estimated_gas_units must be greater than zero");
        }
        let dex_slippage_bps = file.dex_slippage_bps.unwrap_or(30);
        let flashloan_fee_bps = file.flashloan_fee_bps.unwrap_or(9);
        let execution_risk_bps = file.execution_risk_bps.unwrap_or(50);
        if [dex_slippage_bps, flashloan_fee_bps, execution_risk_bps]
            .into_iter()
            .any(|value| value > 10_000)
        {
            bail!("profit model basis points must be between 0 and 10000");
        }
        let max_candidate_refresh_per_block = file.max_candidate_refresh_per_block.unwrap_or(200);
        if max_candidate_refresh_per_block == 0 || max_candidate_refresh_per_block > 10_000 {
            bail!("max_candidate_refresh_per_block must be between 1 and 10000");
        }

        Ok(Self {
            rpc_ws_url: env::var("RPC_WS_URL").context("RPC_WS_URL is required")?,
            deployment: DeploymentMetadata {
                chain_id: file.chain_id,
                pool,
                protocol_revision: file.protocol_revision,
                start_block: file.start_block,
            },
            pool_configurator,
            oracle,
            protocol_data_provider,
            simulation_caller: parse_address(
                "SIMULATION_CALLER",
                &env::var("SIMULATION_CALLER")
                    .context("SIMULATION_CALLER is required; it is only the eth_call sender")?,
            )?,
            native_asset,
            gas_price_gwei,
            min_profit_usd,
            estimated_gas_units,
            dex_slippage_bps,
            flashloan_fee_bps,
            execution_risk_bps,
            oracle_feeds,
            database_url: env::var("DATABASE_URL").context("DATABASE_URL is required")?,
            redis_url: env::var("REDIS_URL").context("REDIS_URL is required")?,
            rpc_timeout: Duration::from_secs(rpc_timeout),
            database_timeout: Duration::from_secs(database_timeout),
            max_candidate_refresh_per_block,
        })
    }
}

pub fn load_deployment(path: impl AsRef<Path>) -> Result<DeploymentMetadata> {
    let path = path.as_ref();
    let file = read_file_config(path)?;
    Ok(DeploymentMetadata {
        chain_id: file.chain_id,
        pool: parse_address("pool", &file.pool)?,
        protocol_revision: file.protocol_revision,
        start_block: file.start_block,
    })
}

fn read_file_config(path: &Path) -> Result<FileConfig> {
    let source =
        fs::read_to_string(path).with_context(|| format!("read config {}", path.display()))?;
    toml::from_str(&source).with_context(|| format!("parse config {}", path.display()))
}

pub fn oracle_feed_map(feeds: &[OracleFeed]) -> HashMap<Address, Address> {
    feeds
        .iter()
        .map(|entry| (entry.feed, entry.asset))
        .collect()
}

fn parse_address(name: &str, value: &str) -> Result<Address> {
    Address::from_str(value).with_context(|| format!("invalid address in {name}"))
}

fn parse_nonnegative_decimal(name: &str, value: &str) -> Result<BigDecimal> {
    let parsed =
        BigDecimal::from_str(value).with_context(|| format!("invalid decimal in {name}"))?;
    if parsed < 0 {
        bail!("{name} cannot be negative");
    }
    Ok(parsed)
}
