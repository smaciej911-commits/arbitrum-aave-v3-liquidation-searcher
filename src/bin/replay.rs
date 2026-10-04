use std::{
    fs::File,
    io::{BufRead, BufReader},
    str::FromStr,
};

use alloy::primitives::B256;
use anyhow::{Context, Result, bail};
use serde::Deserialize;
use serde_json::{Value, json};
use sqlx::postgres::PgPoolOptions;
use tracing_subscriber::EnvFilter;

use liquidation_engine::{
    config,
    domain::{AccountSnapshot, ProtocolDelta},
    store::{BlockHeader, IndexedDelta, Store},
};

#[derive(Deserialize)]
struct FixtureRecord {
    block: u64,
    hash: String,
    parent: String,
    #[serde(default)]
    tx_index: u64,
    #[serde(default)]
    log_index: u64,
    #[serde(default)]
    transaction_hash: Option<String>,
    #[serde(default)]
    contract_address: Option<String>,
    #[serde(default)]
    delta: Option<ProtocolDelta>,
    #[serde(default)]
    account_snapshot: Option<AccountSnapshot>,
    #[serde(default)]
    raw_log: Option<Value>,
    #[serde(default)]
    expected_final_state_hash: Option<String>,
}

struct PendingBlock {
    header: BlockHeader,
    logs: Vec<IndexedDelta>,
    account_snapshots: Vec<AccountSnapshot>,
}

struct ReplayOptions {
    fixture: Option<std::path::PathBuf>,
    config: std::path::PathBuf,
    verify_final_state: bool,
    expected_state_hash: Option<String>,
    state_hash_only: bool,
}

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
    let ReplayOptions {
        fixture,
        config: config_path,
        verify_final_state,
        expected_state_hash,
        state_hash_only,
    } = parse_args()?;
    let deployment = config::load_deployment(config_path)?;
    let database_url =
        std::env::var("DATABASE_URL").context("DATABASE_URL is required for replay")?;
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&database_url)
        .await
        .context("connect replay PostgreSQL")?;
    sqlx::migrate!()
        .run(&pool)
        .await
        .context("apply replay migrations")?;
    let store = Store::new(pool);
    store.ensure_deployment(&deployment).await?;
    if state_hash_only {
        let state_hash = store.state_hash().await?;
        let (blocks, logs, accounts) = store.counts().await?;
        println!("canonical DB blocks:     {blocks}");
        println!("canonical DB logs:       {logs}");
        println!("account snapshots:       {accounts}");
        println!("final state hash:        {state_hash}");
        return Ok(());
    }
    if store.head().await?.is_some() {
        bail!("replay requires an empty canonical-block database; use a dedicated database");
    }
    let fixture = fixture.context("--fixture is required unless --state-hash-only is used")?;

    let reader = BufReader::new(
        File::open(&fixture).with_context(|| format!("open fixture {}", fixture.display()))?,
    );
    let mut pending: Option<PendingBlock> = None;
    let mut block_count = 0_u64;
    let mut log_count = 0_u64;
    let mut updates = 0_u64;
    let mut snapshot_updates = 0_u64;
    let mut expected = expected_state_hash;
    for (line_number, line) in reader.lines().enumerate() {
        let line = line.with_context(|| format!("read fixture line {}", line_number + 1))?;
        if line.trim().is_empty() {
            continue;
        }
        let record: FixtureRecord = serde_json::from_str(&line)
            .with_context(|| format!("parse fixture line {}", line_number + 1))?;
        if let Some(hash) = record.expected_final_state_hash {
            expected = Some(hash);
        }
        let header = BlockHeader {
            number: record.block,
            hash: B256::from_str(&record.hash).with_context(|| {
                format!("invalid block hash at fixture line {}", line_number + 1)
            })?,
            parent_hash: B256::from_str(&record.parent).with_context(|| {
                format!("invalid parent hash at fixture line {}", line_number + 1)
            })?,
        };
        if pending
            .as_ref()
            .is_some_and(|current| current.header.number != header.number)
        {
            let completed = pending
                .take()
                .context("pending fixture block disappeared")?;
            apply_pending_block(&store, completed)
                .await
                .context("apply replay fixture block")?;
            block_count += 1;
            pending = Some(PendingBlock {
                header,
                logs: Vec::new(),
                account_snapshots: Vec::new(),
            });
        } else if pending.is_none() {
            pending = Some(PendingBlock {
                header,
                logs: Vec::new(),
                account_snapshots: Vec::new(),
            });
        } else if let Some(current) = pending.as_ref()
            && (current.header.hash != header.hash
                || current.header.parent_hash != header.parent_hash)
        {
            bail!(
                "fixture has inconsistent header data for block {}",
                header.number
            );
        }
        let pending = pending.as_mut().context("pending block not initialized")?;
        if let Some(delta) = record.delta {
            if matches!(
                &delta,
                ProtocolDelta::Supply { .. }
                    | ProtocolDelta::Withdraw { .. }
                    | ProtocolDelta::Borrow { .. }
                    | ProtocolDelta::Repay { .. }
                    | ProtocolDelta::Liquidation { .. }
                    | ProtocolDelta::CollateralFlagChanged { .. }
                    | ProtocolDelta::UserEModeChanged { .. }
            ) {
                updates += 1;
            }
            pending.logs.push(IndexedDelta {
                log_index: record.log_index,
                transaction_index: record.tx_index,
                transaction_hash: record
                    .transaction_hash
                    .unwrap_or_else(|| format!("fixture-tx-{}-{}", record.block, record.tx_index)),
                contract_address: record
                    .contract_address
                    .unwrap_or_else(|| format!("{:#x}", deployment.pool).to_ascii_lowercase()),
                delta,
                raw_log: record
                    .raw_log
                    .unwrap_or_else(|| json!({"fixture_line":line_number+1})),
            });
            log_count += 1;
        }
        if let Some(snapshot) = record.account_snapshot {
            pending.account_snapshots.push(snapshot);
            snapshot_updates += 1;
        }
    }
    if let Some(last) = pending {
        apply_pending_block(&store, last)
            .await
            .context("apply final replay fixture block")?;
        block_count += 1;
    }
    if block_count == 0 {
        bail!("fixture contains no protocol delta records");
    }

    let state_hash = store.state_hash().await?;
    let (db_blocks, db_logs, accounts) = store.counts().await?;
    println!("blocks:                 {block_count}");
    println!("logs:                   {log_count}");
    println!("position updates:        {updates}");
    println!("account snapshot updates: {snapshot_updates}");
    println!("account snapshots:       {accounts}");
    println!("canonical DB blocks:     {db_blocks}");
    println!("canonical DB logs:       {db_logs}");
    println!("final state hash:        {state_hash}");
    if verify_final_state {
        let expected=expected.context("--verify-final-state requires expected_final_state_hash in fixture or --expected-state-hash")?;
        println!("expected state hash:     {expected}");
        if !state_hash.eq_ignore_ascii_case(&expected) {
            bail!("canonical state hash mismatch");
        }
        println!("MATCH");
    }
    Ok(())
}

async fn apply_pending_block(store: &Store, pending: PendingBlock) -> Result<()> {
    let block_hash = format!("{:#x}", pending.header.hash).to_ascii_lowercase();
    let block_number = pending.header.number;
    store.apply_block(&pending.header, pending.logs).await?;
    for snapshot in pending.account_snapshots {
        store
            .save_account_snapshot(&block_hash, block_number, &snapshot)
            .await?;
    }
    Ok(())
}

fn parse_args() -> Result<ReplayOptions> {
    let mut args = std::env::args().skip(1);
    let mut fixture = None;
    let mut config = std::path::PathBuf::from("config/arbitrum.toml");
    let mut verify = false;
    let mut expected = None;
    let mut state_hash_only = false;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--fixture" => {
                fixture = Some(std::path::PathBuf::from(
                    args.next().context("--fixture requires a path")?,
                ))
            }
            "--config" => {
                config = std::path::PathBuf::from(args.next().context("--config requires a path")?)
            }
            "--expected-state-hash" => {
                expected = Some(
                    args.next()
                        .context("--expected-state-hash requires a hash")?,
                )
            }
            "--verify-final-state" => verify = true,
            "--state-hash-only" => state_hash_only = true,
            "--help" | "-h" => {
                println!(
                    "Usage: replay --fixture FILE [--config config/arbitrum.toml] [--verify-final-state --expected-state-hash 0x…]\n       replay --state-hash-only [--config config/arbitrum.toml]"
                );
                std::process::exit(0);
            }
            other => bail!("unknown argument: {other}"),
        }
    }
    if state_hash_only && fixture.is_some() {
        bail!("--state-hash-only cannot be combined with --fixture");
    }
    if verify && state_hash_only {
        bail!("--verify-final-state cannot be combined with --state-hash-only");
    }
    Ok(ReplayOptions {
        fixture,
        config,
        verify_final_state: verify,
        expected_state_hash: expected,
        state_hash_only,
    })
}
