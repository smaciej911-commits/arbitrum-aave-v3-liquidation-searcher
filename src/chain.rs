use std::{collections::BTreeSet, str::FromStr, time::Instant};

use alloy::{
    primitives::Address,
    providers::Provider,
    rpc::types::{BlockNumberOrTag, Filter},
};
use anyhow::{Context, Result, bail};
use bigdecimal::BigDecimal;
use futures_util::StreamExt;
use serde_json::json;
use tracing::{error, info, warn};

use crate::{
    cache::CandidateCache,
    config::{Config, oracle_feed_map},
    domain::{AssetValue, ProfitAssumptions, ProtocolDelta},
    events::{decode, supported_signatures},
    rpc::{aave_asset_price, user_account_data},
    simulator,
    store::{BlockHeader, CandidateDecision, IndexedDelta, Store},
};

pub async fn run<P: Provider>(
    provider: P,
    config: Config,
    store: Store,
    mut cache: CandidateCache,
    end_block: Option<u64>,
) -> Result<()> {
    if let Some(target) = end_block {
        let latest = tokio::time::timeout(config.rpc_timeout, provider.get_block_number())
            .await
            .context("initial block-height request timed out")?
            .context("read initial block height")?;
        if target < config.deployment.start_block {
            bail!(
                "end block {target} is earlier than configured start block {}",
                config.deployment.start_block
            );
        }
        if target > latest {
            bail!("end block {target} is above RPC head {latest}");
        }
        sync_to(&provider, &config, &store, &mut cache, target).await?;
        if let Some(head) = store.head().await? {
            refresh_users(
                &provider,
                &config,
                &store,
                &mut cache,
                &format!("{:#x}", head.hash),
                head.number,
                store.users().await?,
            )
            .await?;
        }
        info!(target_block = target, "bounded canonical sync completed");
        return Ok(());
    }

    let subscription = tokio::time::timeout(config.rpc_timeout, provider.subscribe_blocks())
        .await
        .context("newHeads subscription timed out")?
        .context("subscribe to newHeads")?;
    let mut headers = subscription.into_stream();
    // Subscribe before catch-up so a block produced during startup is not missed.
    let latest = tokio::time::timeout(config.rpc_timeout, provider.get_block_number())
        .await
        .context("initial block-height request timed out")?
        .context("read initial block height")?;
    sync_to(&provider, &config, &store, &mut cache, latest).await?;
    if let Some(head) = store.head().await? {
        refresh_users(
            &provider,
            &config,
            &store,
            &mut cache,
            &format!("{:#x}", head.hash),
            head.number,
            store.users().await?,
        )
        .await?;
    }
    info!(latest_block = latest, "initial canonical catch-up finished");

    while let Some(header) = headers.next().await {
        let latest = tokio::time::timeout(config.rpc_timeout, provider.get_block_number())
            .await
            .context("block-height request timed out")?
            .context("refresh RPC block height")?;
        let target = latest.max(header.number);
        if let Err(error) = sync_to(&provider, &config, &store, &mut cache, target).await {
            error!(block_number=target,error=%error,"canonical block synchronization failed");
            return Err(error);
        }
    }
    bail!("RPC newHeads subscription ended unexpectedly")
}

async fn sync_to<P: Provider>(
    provider: &P,
    config: &Config,
    store: &Store,
    cache: &mut CandidateCache,
    target: u64,
) -> Result<()> {
    if target < config.deployment.start_block {
        return Ok(());
    }
    let first = match store.head().await? {
        None => config.deployment.start_block,
        Some(head) => {
            let ancestor = find_common_ancestor(
                provider,
                store,
                config.deployment.start_block,
                head.number,
                target,
                config.rpc_timeout,
            )
            .await?;
            if ancestor < head.number {
                warn!(
                    old_head = head.number,
                    common_ancestor = ancestor,
                    new_head = target,
                    "reorg detected; reverting projection and replaying canonical blocks"
                );
                store.rollback_after(ancestor).await?;
                cache
                    .rebuild(store)
                    .await
                    .context("rebuild Redis after rollback")?;
            }
            ancestor.saturating_add(1)
        }
    };
    if first > target {
        return Ok(());
    }

    for number in first..=target {
        let block_started = Instant::now();
        let rpc_started = Instant::now();
        let block = tokio::time::timeout(
            config.rpc_timeout,
            provider.get_block_by_number(BlockNumberOrTag::Number(number)),
        )
        .await
        .with_context(|| format!("block {number} request timed out"))?
        .with_context(|| format!("read block {number}"))?
        .with_context(|| format!("RPC returned no block {number}"))?;
        if block.header.number != number {
            bail!(
                "RPC returned block {} for requested height {number}",
                block.header.number
            );
        }
        let block_fetch_ms = rpc_started.elapsed().as_millis() as u64;
        let mut addresses = vec![config.deployment.pool, config.pool_configurator];
        addresses.extend(config.oracle_feeds.iter().map(|feed| feed.feed));
        addresses.sort();
        addresses.dedup();
        let filter = Filter::new()
            .address(addresses)
            .event_signature(supported_signatures())
            .from_block(BlockNumberOrTag::Number(number))
            .to_block(BlockNumberOrTag::Number(number));
        let logs_started = Instant::now();
        let logs = tokio::time::timeout(config.rpc_timeout, provider.get_logs(&filter))
            .await
            .with_context(|| format!("logs for block {number} request timed out"))?
            .with_context(|| format!("read logs for block {number}"))?;
        let log_fetch_ms = logs_started.elapsed().as_millis() as u64;

        let feeds = oracle_feed_map(&config.oracle_feeds);
        let decode_started = Instant::now();
        let mut indexed = Vec::with_capacity(logs.len());
        let mut users = BTreeSet::new();
        let mut assets = BTreeSet::new();
        for log in logs {
            if log.block_hash != Some(block.hash()) {
                bail!("RPC returned a log from a non-canonical block {number}");
            }
            let Some(mut decoded) = decode(
                &log,
                &feeds,
                config.deployment.pool,
                config.pool_configurator,
            )
            .with_context(|| format!("decode log at block {number}, index {:?}", log.log_index))?
            else {
                continue;
            };
            if let ProtocolDelta::PriceObserved {
                asset, price_raw, ..
            } = &mut decoded.delta
            {
                let asset_address =
                    Address::from_str(asset).context("invalid oracle asset in config")?;
                // Chainlink's AnswerUpdated identifies a changed feed. Aave's own oracle is
                // authoritative for risk calculations, so snapshot its value at this block.
                *price_raw = aave_asset_price(
                    provider,
                    config.oracle,
                    asset_address,
                    number,
                    config.rpc_timeout,
                )
                .await?;
            }
            if let Some(user) = decoded.user {
                users.insert(user);
            }
            assets.extend(decoded.assets);
            let log_index = log.log_index.context("RPC log is missing logIndex")?;
            let tx_hash = log
                .transaction_hash
                .map(|hash| format!("{hash:#x}").to_ascii_lowercase())
                .context("RPC log is missing transactionHash")?;
            let tx_index = log.transaction_index.unwrap_or(0);
            let contract = format!("{:#x}", log.address()).to_ascii_lowercase();
            let raw_log = serde_json::to_value(&log).context("serialize raw RPC log")?;
            indexed.push(IndexedDelta {
                log_index,
                transaction_index: tx_index,
                transaction_hash: tx_hash,
                contract_address: contract,
                delta: decoded.delta,
                raw_log,
            });
        }
        let decode_us = decode_started.elapsed().as_micros() as u64;
        let already_observed = indexed
            .iter()
            .filter_map(|event| match &event.delta {
                ProtocolDelta::PriceObserved { asset, .. } => Some(asset.clone()),
                _ => None,
            })
            .collect::<BTreeSet<_>>();
        let mut next_log_index = indexed
            .iter()
            .map(|event| event.log_index)
            .max()
            .map(|index| index.saturating_add(1))
            .unwrap_or(0);
        let oracle_started = Instant::now();
        for feed in &config.oracle_feeds {
            let asset = format!("{:#x}", feed.asset).to_ascii_lowercase();
            let price = aave_asset_price(
                provider,
                config.oracle,
                feed.asset,
                number,
                config.rpc_timeout,
            )
            .await
            .with_context(|| format!("poll Aave oracle price for {asset} at block {number}"))?;
            let stored = store.oracle_price(&asset).await?;
            if stored.as_deref() == Some(price.as_str()) || already_observed.contains(&asset) {
                continue;
            }
            let event_index = next_log_index;
            next_log_index = next_log_index
                .checked_add(1)
                .context("oracle poll log index overflow")?;
            indexed.push(IndexedDelta {
                log_index: event_index,
                // Keep synthetic oracle snapshots after on-chain logs while
                // remaining representable by PostgreSQL BIGINT.
                transaction_index: i64::MAX as u64,
                transaction_hash: format!("oracle-poll:{number}:{asset}"),
                contract_address: format!("{:#x}", config.oracle).to_ascii_lowercase(),
                delta: ProtocolDelta::PriceObserved {
                    asset: asset.clone(),
                    price_raw: price,
                    oracle: format!("{:#x}", config.oracle).to_ascii_lowercase(),
                },
                raw_log: json!({"source":"aave_oracle_block_poll","block":number,"asset":asset}),
            });
            assets.insert(asset);
        }
        let oracle_poll_ms = oracle_started.elapsed().as_millis() as u64;
        let indexed_count = indexed.len();
        let header = BlockHeader {
            number,
            hash: block.hash(),
            parent_hash: block.header.parent_hash,
        };
        let block_hash = format!("{:#x}", header.hash).to_ascii_lowercase();
        let projection_started = Instant::now();
        store
            .apply_block(&header, indexed)
            .await
            .with_context(|| format!("apply block {number}"))?;
        let postgres_projection_ms = projection_started.elapsed().as_millis() as u64;
        for asset in assets {
            users.extend(store.users_for_asset(&asset).await?);
        }
        // Debt accrues between protocol events. Refresh the most vulnerable
        // known accounts each block so interest alone can cross HF < 1.
        users.extend(
            cache
                .most_at_risk(config.max_candidate_refresh_per_block)
                .await?,
        );
        let refresh_started = Instant::now();
        refresh_users(
            provider,
            config,
            store,
            cache,
            &block_hash,
            number,
            users.into_iter().collect(),
        )
        .await?;
        let account_refresh_ms = refresh_started.elapsed().as_millis() as u64;
        info!(block_number=number,block_hash=%block_hash,decoded_logs=indexed_count,block_fetch_rpc_ms=block_fetch_ms,log_fetch_rpc_ms=log_fetch_ms,log_decode_us=decode_us,oracle_rpc_ms=oracle_poll_ms,postgres_projection_ms,account_refresh_ms,block_to_candidate_ms=block_started.elapsed().as_millis() as u64,"indexed Aave canonical block");
    }
    Ok(())
}

async fn find_common_ancestor<P: Provider>(
    provider: &P,
    store: &Store,
    start: u64,
    local_head: u64,
    remote_head: u64,
    timeout: std::time::Duration,
) -> Result<u64> {
    let mut number = local_head.min(remote_head);
    loop {
        let local = store.canonical_hash(number).await?;
        let remote = tokio::time::timeout(
            timeout,
            provider.get_block_by_number(BlockNumberOrTag::Number(number)),
        )
        .await
        .with_context(|| format!("common ancestor block {number} request timed out"))?
        .with_context(|| format!("read remote block {number}"))?
        .with_context(|| format!("RPC returned no common-ancestor candidate at {number}"))?;
        if local == Some(remote.hash()) {
            return Ok(number);
        }
        if number <= start {
            bail!("no common ancestor at or after configured start block {start}");
        }
        number -= 1;
    }
}

async fn refresh_users<P: Provider>(
    provider: &P,
    config: &Config,
    store: &Store,
    cache: &mut CandidateCache,
    block_hash: &str,
    block_number: u64,
    users: Vec<String>,
) -> Result<()> {
    for user in users {
        let snapshot = user_account_data(
            provider,
            config.deployment.pool,
            &user,
            block_number,
            config.rpc_timeout,
        )
        .await
        .with_context(|| format!("read pinned Aave account data for {user}"))?;
        store
            .save_account_snapshot(block_hash, block_number, &snapshot)
            .await?;
        let health_factor =
            BigDecimal::from_str(&snapshot.health_factor).context("parse account health factor")?;
        if snapshot.total_debt_base == "0" || health_factor >= 1 {
            cache.remove(&user).await?;
            if snapshot.total_debt_base == "0" {
                continue;
            }
            store
                .save_candidate(CandidateDecision {
                    block_hash,
                    user: &user,
                    health_factor: &snapshot.health_factor,
                    decision: "HEALTHY",
                    expected_profit: None,
                    gas_cost: None,
                    collateral: None,
                    debt: None,
                    error: None,
                })
                .await?;
            continue;
        }
        cache.update(&user, &health_factor).await?;
        let mut simulation_error = None;
        let mut expected_profit = None;
        let mut gas_cost = None;
        let mut selected_pair = None;
        let mut decision = "NO_RESERVE_PAIR";
        let pairs = store.liquidation_pairs(&user).await?;
        for (collateral, debt) in pairs {
            let model = match prepare_candidate_model(
                provider,
                config,
                &snapshot,
                &user,
                &collateral,
                &debt,
                block_number,
            )
            .await
            {
                Ok(Some(model)) => model,
                Ok(None) => continue,
                Err(error) => {
                    simulation_error = Some(error.to_string());
                    continue;
                }
            };
            decision = "UNPROFITABLE_MODEL";
            expected_profit = Some(model.estimate.expected_profit_usd.clone());
            gas_cost = Some(model.estimate.gas_cost_usd.clone());
            selected_pair = Some((collateral.clone(), debt.clone()));
            simulation_error = None;
            if model.estimate.expected_profit_usd <= config.min_profit_usd {
                continue;
            }

            let collateral_address =
                Address::from_str(&collateral).context("invalid collateral reserve")?;
            let debt_address = Address::from_str(&debt).context("invalid debt reserve")?;
            let candidate = Address::from_str(&user).context("invalid candidate user")?;
            let simulation_started = Instant::now();
            match simulator::simulate_liquidation(
                provider,
                simulator::LiquidationRequest {
                    pool: config.deployment.pool,
                    caller: config.simulation_caller,
                    collateral_asset: collateral_address,
                    debt_asset: debt_address,
                    user: candidate,
                    block_number,
                },
                config.rpc_timeout,
            )
            .await
            {
                Ok(simulation) => {
                    let actual = crate::domain::estimate_profit(
                        &snapshot,
                        AssetValue {
                            amount_raw: &model.collateral_state.collateral_amount_raw,
                            decimals: model.collateral_state.decimals,
                            price_raw: &model.collateral_price,
                        },
                        AssetValue {
                            amount_raw: &model.debt_state.debt_amount_raw,
                            decimals: model.debt_state.decimals,
                            price_raw: &model.debt_price,
                        },
                        ProfitAssumptions {
                            liquidation_bonus_bps: model.collateral_state.liquidation_bonus_bps,
                            liquidation_protocol_fee_bps: model
                                .collateral_state
                                .liquidation_protocol_fee_bps,
                            native_price_raw: &model.native_price,
                            gas_price_gwei: &config.gas_price_gwei,
                            gas_units: simulation.gas_used,
                            slippage_bps: config.dex_slippage_bps,
                            flashloan_fee_bps: config.flashloan_fee_bps,
                            execution_risk_bps: config.execution_risk_bps,
                        },
                    )?;
                    expected_profit = Some(actual.expected_profit_usd.clone());
                    gas_cost = Some(actual.gas_cost_usd.clone());
                    decision = if actual.expected_profit_usd > config.min_profit_usd {
                        "DRY_RUN_PROFITABLE_CALL_PASSED"
                    } else {
                        "UNPROFITABLE_AFTER_GAS"
                    };
                    info!(user=%user,health_factor=%health_factor,expected_profit_usd=%actual.expected_profit_usd,gas_cost_usd=%actual.gas_cost_usd,gas_estimate=simulation.gas_used,simulation_rpc_ms=simulation_started.elapsed().as_millis() as u64,decision,"read-only Aave liquidation simulation completed; no transaction submitted");
                    simulation_error = None;
                    if decision == "DRY_RUN_PROFITABLE_CALL_PASSED" {
                        break;
                    }
                }
                Err(error) => {
                    decision = "LIQUIDATION_CALL_SIMULATION_FAILED";
                    simulation_error = Some(error.to_string());
                    warn!(user=%user,simulation_rpc_ms=simulation_started.elapsed().as_millis() as u64,error=%simulation_error.as_deref().unwrap_or_default(),"read-only liquidation simulation rejected candidate");
                }
            }
        }
        if selected_pair.is_none() && simulation_error.is_some() {
            decision = "RESERVE_READ_FAILED";
        }
        let (collateral, debt) = selected_pair
            .as_ref()
            .map(|(c, d)| (Some(c.as_str()), Some(d.as_str())))
            .unwrap_or((None, None));
        store
            .save_candidate(CandidateDecision {
                block_hash,
                user: &user,
                health_factor: &snapshot.health_factor,
                decision,
                expected_profit: expected_profit.as_ref(),
                gas_cost: gas_cost.as_ref(),
                collateral,
                debt,
                error: simulation_error.as_deref(),
            })
            .await?;
        if decision != "DRY_RUN_PROFITABLE_CALL_PASSED" {
            warn!(user=%user,health_factor=%health_factor,decision,expected_profit_usd=?expected_profit,error=?simulation_error,"candidate retained for observe-only review");
        }
    }
    Ok(())
}

struct CandidateModel {
    collateral_state: crate::rpc::ReserveSnapshot,
    debt_state: crate::rpc::ReserveSnapshot,
    collateral_price: String,
    debt_price: String,
    native_price: String,
    estimate: crate::domain::ProfitEstimate,
}

async fn prepare_candidate_model<P: Provider>(
    provider: &P,
    config: &Config,
    account: &crate::domain::AccountSnapshot,
    user: &str,
    collateral: &str,
    debt: &str,
    block_number: u64,
) -> Result<Option<CandidateModel>> {
    let mut collateral_state = crate::rpc::user_reserve_data(
        provider,
        config.protocol_data_provider,
        config.deployment.pool,
        user,
        collateral,
        block_number,
        config.rpc_timeout,
    )
    .await?;
    let debt_state = crate::rpc::user_reserve_data(
        provider,
        config.protocol_data_provider,
        config.deployment.pool,
        user,
        debt,
        block_number,
        config.rpc_timeout,
    )
    .await?;
    if !collateral_state.collateral_enabled
        || collateral_state.collateral_amount_raw == "0"
        || debt_state.debt_amount_raw == "0"
    {
        return Ok(None);
    }
    collateral_state.liquidation_bonus_bps = crate::rpc::effective_liquidation_bonus(
        provider,
        config.deployment.pool,
        user,
        collateral,
        collateral_state.liquidation_bonus_bps,
        block_number,
        config.rpc_timeout,
    )
    .await?;
    let collateral_price = aave_asset_price(
        provider,
        config.oracle,
        Address::from_str(collateral).context("invalid collateral reserve")?,
        block_number,
        config.rpc_timeout,
    )
    .await?;
    let debt_price = aave_asset_price(
        provider,
        config.oracle,
        Address::from_str(debt).context("invalid debt reserve")?,
        block_number,
        config.rpc_timeout,
    )
    .await?;
    let native_price = aave_asset_price(
        provider,
        config.oracle,
        config.native_asset,
        block_number,
        config.rpc_timeout,
    )
    .await?;
    let estimate = crate::domain::estimate_profit(
        account,
        AssetValue {
            amount_raw: &collateral_state.collateral_amount_raw,
            decimals: collateral_state.decimals,
            price_raw: &collateral_price,
        },
        AssetValue {
            amount_raw: &debt_state.debt_amount_raw,
            decimals: debt_state.decimals,
            price_raw: &debt_price,
        },
        ProfitAssumptions {
            liquidation_bonus_bps: collateral_state.liquidation_bonus_bps,
            liquidation_protocol_fee_bps: collateral_state.liquidation_protocol_fee_bps,
            native_price_raw: &native_price,
            gas_price_gwei: &config.gas_price_gwei,
            gas_units: config.estimated_gas_units,
            slippage_bps: config.dex_slippage_bps,
            flashloan_fee_bps: config.flashloan_fee_bps,
            execution_risk_bps: config.execution_risk_bps,
        },
    )?;
    Ok(Some(CandidateModel {
        collateral_state,
        debt_state,
        collateral_price,
        debt_price,
        native_price,
        estimate,
    }))
}
