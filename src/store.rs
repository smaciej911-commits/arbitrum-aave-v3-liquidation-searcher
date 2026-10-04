use std::str::FromStr;

use alloy::primitives::B256;
use anyhow::{Context, Result, bail};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{FromRow, PgPool, Postgres, Transaction, types::Json};

use crate::{
    config::DeploymentMetadata,
    domain::{AccountSnapshot, ProtocolDelta},
};

#[derive(Clone, Debug)]
pub struct BlockHeader {
    pub number: u64,
    pub hash: B256,
    pub parent_hash: B256,
}

#[derive(Clone, Debug)]
pub struct IndexedDelta {
    pub log_index: u64,
    pub transaction_index: u64,
    pub transaction_hash: String,
    pub contract_address: String,
    pub delta: ProtocolDelta,
    pub raw_log: Value,
}

#[derive(Clone, Debug)]
pub struct StoredHead {
    pub number: u64,
    pub hash: B256,
}

pub struct CandidateDecision<'a> {
    pub block_hash: &'a str,
    pub user: &'a str,
    pub health_factor: &'a str,
    pub decision: &'a str,
    pub expected_profit: Option<&'a BigDecimal>,
    pub gas_cost: Option<&'a BigDecimal>,
    pub collateral: Option<&'a str>,
    pub debt: Option<&'a str>,
    pub error: Option<&'a str>,
}

struct ReserveChange<'a> {
    block_hash: &'a str,
    block_number: i64,
    sequence: i64,
    user: &'a str,
    reserve: &'a str,
    collateral_delta: BigDecimal,
    debt_delta: BigDecimal,
    enabled: Option<bool>,
}

#[derive(Clone, Debug, Serialize, Deserialize, FromRow)]
struct ReserveRow {
    user_address: String,
    reserve_address: String,
    collateral_amount_raw: BigDecimal,
    debt_amount_raw: BigDecimal,
    collateral_enabled: bool,
    source_block_number: i64,
    source_block_hash: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, FromRow)]
struct AccountRow {
    user_address: String,
    total_collateral_base: BigDecimal,
    total_debt_base: BigDecimal,
    liquidation_threshold_bps: i16,
    health_factor: BigDecimal,
    source_block_number: i64,
    source_block_hash: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, FromRow)]
struct OracleRow {
    asset_address: String,
    price_raw: BigDecimal,
    oracle_address: String,
    source_block_number: i64,
    source_block_hash: String,
}

pub struct Store {
    pool: PgPool,
}

impl Store {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn ensure_deployment(&self, deployment: &DeploymentMetadata) -> Result<()> {
        let chain_id =
            i64::try_from(deployment.chain_id).context("chain ID exceeds PostgreSQL range")?;
        let start_block = i64::try_from(deployment.start_block)
            .context("start block exceeds PostgreSQL range")?;
        let pool = format!("{:#x}", deployment.pool).to_ascii_lowercase();
        sqlx::query("INSERT INTO deployment_metadata(singleton,chain_id,pool_address,protocol_revision,start_block) VALUES(TRUE,$1,$2,$3,$4) ON CONFLICT(singleton) DO NOTHING")
            .bind(chain_id).bind(&pool).bind(&deployment.protocol_revision).bind(start_block)
            .execute(&self.pool).await.context("bind database to Aave deployment")?;
        let existing = sqlx::query_as::<_, (i64, String, String, i64)>("SELECT chain_id,pool_address,protocol_revision,start_block FROM deployment_metadata WHERE singleton=TRUE")
            .fetch_one(&self.pool).await.context("read Aave deployment binding")?;
        if existing
            != (
                chain_id,
                pool,
                deployment.protocol_revision.clone(),
                start_block,
            )
        {
            bail!(
                "database deployment metadata does not match configured chain, Pool, revision, or start block"
            );
        }
        Ok(())
    }

    pub async fn head(&self) -> Result<Option<StoredHead>> {
        let row = sqlx::query_as::<_, (i64, String)>(
            "SELECT number,hash FROM canonical_blocks ORDER BY number DESC LIMIT 1",
        )
        .fetch_optional(&self.pool)
        .await
        .context("read canonical head")?;
        row.map(|(number, hash)| {
            Ok(StoredHead {
                number: u64::try_from(number).context("negative canonical block")?,
                hash: B256::from_str(&hash).context("invalid stored block hash")?,
            })
        })
        .transpose()
    }

    pub async fn canonical_hash(&self, number: u64) -> Result<Option<B256>> {
        let number = i64::try_from(number).context("block number exceeds PostgreSQL range")?;
        let hash =
            sqlx::query_scalar::<_, String>("SELECT hash FROM canonical_blocks WHERE number=$1")
                .bind(number)
                .fetch_optional(&self.pool)
                .await
                .context("read canonical block hash")?;
        hash.map(|value| B256::from_str(&value).context("invalid stored block hash"))
            .transpose()
    }

    pub async fn apply_block(
        &self,
        header: &BlockHeader,
        mut events: Vec<IndexedDelta>,
    ) -> Result<()> {
        events.sort_by_key(|event| (event.transaction_index, event.log_index));
        let number =
            i64::try_from(header.number).context("block number exceeds PostgreSQL range")?;
        let hash = format!("{:#x}", header.hash).to_ascii_lowercase();
        let parent_hash = format!("{:#x}", header.parent_hash).to_ascii_lowercase();
        let mut tx = self
            .pool
            .begin()
            .await
            .context("begin canonical block transaction")?;
        sqlx::query("SELECT pg_advisory_xact_lock(7162381)")
            .execute(&mut *tx)
            .await
            .context("lock canonical chain")?;
        if let Some((head_number, head_hash)) = sqlx::query_as::<_, (i64, String)>(
            "SELECT number,hash FROM canonical_blocks ORDER BY number DESC LIMIT 1",
        )
        .fetch_optional(&mut *tx)
        .await
        .context("read head before applying block")?
        {
            if number == head_number && hash == head_hash {
                tx.rollback()
                    .await
                    .context("finish idempotent block apply")?;
                return Ok(());
            }
            if !block_extends(head_number, &head_hash, number, &parent_hash) {
                bail!("block {number} does not extend canonical head {head_number} ({head_hash})");
            }
        }
        sqlx::query("INSERT INTO canonical_blocks(number,hash,parent_hash) VALUES($1,$2,$3)")
            .bind(number)
            .bind(&hash)
            .bind(&parent_hash)
            .execute(&mut *tx)
            .await
            .context("insert canonical block")?;

        for (sequence, event) in events.into_iter().enumerate() {
            let sequence = i64::try_from(sequence)
                .context("too many logs in a block")?
                .checked_mul(10)
                .context("block journal sequence overflow")?;
            let log_index =
                i64::try_from(event.log_index).context("log index exceeds PostgreSQL range")?;
            let transaction_index = i64::try_from(event.transaction_index)
                .context("transaction index exceeds PostgreSQL range")?;
            let kind = delta_kind(&event.delta);
            apply_delta(&mut tx, &hash, number, sequence, &event.delta).await?;
            sqlx::query("INSERT INTO protocol_logs(block_hash,log_index,transaction_index,transaction_hash,contract_address,delta_kind,delta,raw_log) VALUES($1,$2,$3,$4,$5,$6,$7,$8)")
                .bind(&hash).bind(log_index).bind(transaction_index).bind(&event.transaction_hash).bind(&event.contract_address).bind(kind)
                .bind(Json(serde_json::to_value(&event.delta).context("serialize normalized protocol delta")?))
                .bind(Json(event.raw_log)).execute(&mut *tx).await.context("persist raw log and normalized delta")?;
        }
        tx.commit().await.context("commit canonical block")
    }

    pub async fn users_for_asset(&self, asset: &str) -> Result<Vec<String>> {
        sqlx::query_scalar("SELECT user_address FROM user_reserves WHERE reserve_address=$1 AND (collateral_amount_raw>0 OR debt_amount_raw>0) ORDER BY user_address")
            .bind(asset).fetch_all(&self.pool).await.context("find users exposed to oracle asset")
    }

    pub async fn oracle_price(&self, asset: &str) -> Result<Option<String>> {
        let price = sqlx::query_scalar::<_, BigDecimal>(
            "SELECT price_raw FROM oracle_prices WHERE asset_address=$1",
        )
        .bind(asset)
        .fetch_optional(&self.pool)
        .await
        .context("read persisted oracle price")?;
        Ok(price.map(|value| value.to_string()))
    }

    pub async fn users(&self) -> Result<Vec<String>> {
        sqlx::query_scalar("SELECT DISTINCT user_address FROM user_reserves ORDER BY user_address")
            .fetch_all(&self.pool)
            .await
            .context("list account snapshots")
    }

    pub async fn liquidation_pairs(&self, user: &str) -> Result<Vec<(String, String)>> {
        sqlx::query_as("SELECT c.reserve_address,d.reserve_address FROM user_reserves c JOIN user_reserves d ON d.user_address=c.user_address WHERE c.user_address=$1 AND c.collateral_amount_raw>0 AND c.collateral_enabled AND d.debt_amount_raw>0 ORDER BY c.collateral_amount_raw DESC,d.debt_amount_raw DESC LIMIT 4")
            .bind(user).fetch_all(&self.pool).await.context("select candidate collateral/debt reserve pairs")
    }

    pub async fn save_account_snapshot(
        &self,
        block_hash: &str,
        block_number: u64,
        snapshot: &AccountSnapshot,
    ) -> Result<()> {
        let number =
            i64::try_from(block_number).context("block number exceeds PostgreSQL range")?;
        let previous = sqlx::query_as::<_, AccountRow>("SELECT user_address,total_collateral_base,total_debt_base,liquidation_threshold_bps,health_factor,source_block_number,source_block_hash FROM account_snapshots WHERE user_address=$1")
            .bind(&snapshot.user).fetch_optional(&self.pool).await.context("read prior account snapshot")?;
        let mut tx = self
            .pool
            .begin()
            .await
            .context("begin account snapshot update")?;
        let sequence = sqlx::query_scalar::<_, i64>(
            "SELECT COALESCE(MAX(sequence),-1)+1 FROM state_journal WHERE block_hash=$1",
        )
        .bind(block_hash)
        .fetch_one(&mut *tx)
        .await
        .context("allocate account journal sequence")?;
        journal(
            &mut tx,
            block_hash,
            sequence,
            "account_snapshot",
            &snapshot.user,
            previous
                .map(serde_json::to_value)
                .transpose()
                .context("serialize prior account snapshot")?,
        )
        .await?;
        sqlx::query("INSERT INTO account_snapshots(user_address,total_collateral_base,total_debt_base,liquidation_threshold_bps,health_factor,source_block_number,source_block_hash) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(user_address) DO UPDATE SET total_collateral_base=EXCLUDED.total_collateral_base,total_debt_base=EXCLUDED.total_debt_base,liquidation_threshold_bps=EXCLUDED.liquidation_threshold_bps,health_factor=EXCLUDED.health_factor,source_block_number=EXCLUDED.source_block_number,source_block_hash=EXCLUDED.source_block_hash,updated_at=NOW()")
            .bind(&snapshot.user).bind(decimal_raw(&snapshot.total_collateral_base)?).bind(decimal_raw(&snapshot.total_debt_base)?)
            .bind(i16::try_from(snapshot.liquidation_threshold_bps).context("liquidation threshold exceeds SMALLINT")?)
            .bind(BigDecimal::from_str(&snapshot.health_factor).context("invalid health factor")?)
            .bind(number).bind(block_hash).execute(&mut *tx).await.context("save pinned Aave account snapshot")?;
        tx.commit().await.context("commit account snapshot")
    }

    pub async fn save_candidate(&self, candidate: CandidateDecision<'_>) -> Result<()> {
        sqlx::query("INSERT INTO candidate_decisions(block_hash,user_address,health_factor,decision,expected_profit_usd,gas_cost_usd,collateral_asset,debt_asset,simulation_error) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT(block_hash,user_address) DO UPDATE SET health_factor=EXCLUDED.health_factor,decision=EXCLUDED.decision,expected_profit_usd=EXCLUDED.expected_profit_usd,gas_cost_usd=EXCLUDED.gas_cost_usd,collateral_asset=EXCLUDED.collateral_asset,debt_asset=EXCLUDED.debt_asset,simulation_error=EXCLUDED.simulation_error,created_at=NOW()")
            .bind(candidate.block_hash).bind(candidate.user).bind(BigDecimal::from_str(candidate.health_factor).context("invalid health factor")?).bind(candidate.decision).bind(candidate.expected_profit).bind(candidate.gas_cost).bind(candidate.collateral).bind(candidate.debt).bind(candidate.error)
            .execute(&self.pool).await.context("persist observe-only candidate decision")?;
        Ok(())
    }

    pub async fn rollback_after(&self, common_ancestor: u64) -> Result<()> {
        let common_ancestor =
            i64::try_from(common_ancestor).context("block number exceeds PostgreSQL range")?;
        let mut tx = self.pool.begin().await.context("begin reorg rollback")?;
        sqlx::query("SELECT pg_advisory_xact_lock(7162381)")
            .execute(&mut *tx)
            .await
            .context("lock chain for reorg rollback")?;
        let entries = sqlx::query_as::<_, (String,String,Option<Json<Value>>)>("SELECT j.entity_kind,j.entity_key,j.before_state FROM state_journal j JOIN canonical_blocks b ON b.hash=j.block_hash WHERE b.number>$1 ORDER BY b.number DESC,j.sequence DESC")
            .bind(common_ancestor).fetch_all(&mut *tx).await.context("read state undo journal")?;
        for (kind, key, before) in entries {
            restore(&mut tx, &kind, &key, before.map(|v| v.0)).await?;
        }
        sqlx::query("DELETE FROM candidate_decisions WHERE block_hash IN (SELECT hash FROM canonical_blocks WHERE number>$1)").bind(common_ancestor).execute(&mut *tx).await.context("remove orphaned candidate decisions")?;
        sqlx::query("DELETE FROM protocol_logs WHERE block_hash IN (SELECT hash FROM canonical_blocks WHERE number>$1)").bind(common_ancestor).execute(&mut *tx).await.context("remove orphaned logs")?;
        sqlx::query("DELETE FROM state_journal WHERE block_hash IN (SELECT hash FROM canonical_blocks WHERE number>$1)").bind(common_ancestor).execute(&mut *tx).await.context("remove orphaned undo rows")?;
        sqlx::query("DELETE FROM canonical_blocks WHERE number>$1")
            .bind(common_ancestor)
            .execute(&mut *tx)
            .await
            .context("remove orphaned blocks")?;
        tx.commit().await.context("commit reorg rollback")
    }

    pub async fn account_snapshots(&self) -> Result<Vec<(String, BigDecimal)>> {
        sqlx::query_as(
            "SELECT user_address,health_factor FROM account_snapshots ORDER BY user_address",
        )
        .fetch_all(&self.pool)
        .await
        .context("read account snapshots")
    }

    pub async fn state_hash(&self) -> Result<String> {
        let reserves = sqlx::query_as::<_, ReserveRow>("SELECT user_address,reserve_address,collateral_amount_raw,debt_amount_raw,collateral_enabled,source_block_number,source_block_hash FROM user_reserves ORDER BY user_address,reserve_address")
            .fetch_all(&self.pool).await.context("read reserve projection for state hash")?;
        let accounts = sqlx::query_as::<_, AccountRow>("SELECT user_address,total_collateral_base,total_debt_base,liquidation_threshold_bps,health_factor,source_block_number,source_block_hash FROM account_snapshots ORDER BY user_address")
            .fetch_all(&self.pool).await.context("read account projection for state hash")?;
        let prices = sqlx::query_as::<_, OracleRow>("SELECT asset_address,price_raw,oracle_address,source_block_number,source_block_hash FROM oracle_prices ORDER BY asset_address")
            .fetch_all(&self.pool).await.context("read oracle projection for state hash")?;
        let encoded = serde_json::to_vec(&(reserves, accounts, prices))
            .context("serialize canonical state for hash")?;
        Ok(format!("{:#x}", alloy::primitives::keccak256(encoded)))
    }

    pub async fn counts(&self) -> Result<(i64, i64, i64)> {
        let blocks = sqlx::query_scalar("SELECT COUNT(*) FROM canonical_blocks")
            .fetch_one(&self.pool)
            .await
            .context("count indexed blocks")?;
        let logs = sqlx::query_scalar("SELECT COUNT(*) FROM protocol_logs")
            .fetch_one(&self.pool)
            .await
            .context("count indexed protocol logs")?;
        let accounts = sqlx::query_scalar("SELECT COUNT(*) FROM account_snapshots")
            .fetch_one(&self.pool)
            .await
            .context("count projected accounts")?;
        Ok((blocks, logs, accounts))
    }
}

async fn apply_delta(
    tx: &mut Transaction<'_, Postgres>,
    block_hash: &str,
    block_number: i64,
    sequence: i64,
    delta: &ProtocolDelta,
) -> Result<()> {
    match delta {
        ProtocolDelta::Supply {
            user,
            reserve,
            amount_raw,
        } => {
            change_reserve(
                tx,
                ReserveChange {
                    block_hash,
                    block_number,
                    sequence,
                    user,
                    reserve,
                    collateral_delta: decimal_raw(amount_raw)?,
                    debt_delta: BigDecimal::from(0),
                    enabled: None,
                },
            )
            .await?
        }
        ProtocolDelta::Withdraw {
            user,
            reserve,
            amount_raw,
        } => {
            change_reserve(
                tx,
                ReserveChange {
                    block_hash,
                    block_number,
                    sequence,
                    user,
                    reserve,
                    collateral_delta: -decimal_raw(amount_raw)?,
                    debt_delta: BigDecimal::from(0),
                    enabled: None,
                },
            )
            .await?
        }
        ProtocolDelta::Borrow {
            user,
            reserve,
            amount_raw,
            ..
        } => {
            change_reserve(
                tx,
                ReserveChange {
                    block_hash,
                    block_number,
                    sequence,
                    user,
                    reserve,
                    collateral_delta: BigDecimal::from(0),
                    debt_delta: decimal_raw(amount_raw)?,
                    enabled: None,
                },
            )
            .await?
        }
        ProtocolDelta::Repay {
            user,
            reserve,
            amount_raw,
        } => {
            change_reserve(
                tx,
                ReserveChange {
                    block_hash,
                    block_number,
                    sequence,
                    user,
                    reserve,
                    collateral_delta: BigDecimal::from(0),
                    debt_delta: -decimal_raw(amount_raw)?,
                    enabled: None,
                },
            )
            .await?
        }
        ProtocolDelta::CollateralFlagChanged {
            user,
            reserve,
            enabled,
        } => {
            change_reserve(
                tx,
                ReserveChange {
                    block_hash,
                    block_number,
                    sequence,
                    user,
                    reserve,
                    collateral_delta: BigDecimal::from(0),
                    debt_delta: BigDecimal::from(0),
                    enabled: Some(*enabled),
                },
            )
            .await?
        }
        ProtocolDelta::Liquidation {
            user,
            collateral_asset,
            debt_asset,
            debt_to_cover_raw,
            liquidated_collateral_raw,
        } => {
            change_reserve(
                tx,
                ReserveChange {
                    block_hash,
                    block_number,
                    sequence,
                    user,
                    reserve: collateral_asset,
                    collateral_delta: -decimal_raw(liquidated_collateral_raw)?,
                    debt_delta: BigDecimal::from(0),
                    enabled: None,
                },
            )
            .await?;
            change_reserve(
                tx,
                ReserveChange {
                    block_hash,
                    block_number,
                    sequence: sequence + 1,
                    user,
                    reserve: debt_asset,
                    collateral_delta: BigDecimal::from(0),
                    debt_delta: -decimal_raw(debt_to_cover_raw)?,
                    enabled: None,
                },
            )
            .await?;
        }
        ProtocolDelta::UserEModeChanged { .. } => {}
        ProtocolDelta::ReserveRiskChanged { .. } => {}
        ProtocolDelta::PriceObserved {
            asset,
            price_raw,
            oracle,
        } => {
            let previous=sqlx::query_as::<_,OracleRow>("SELECT asset_address,price_raw,oracle_address,source_block_number,source_block_hash FROM oracle_prices WHERE asset_address=$1")
                .bind(asset).fetch_optional(&mut **tx).await.context("read previous oracle price")?;
            journal(
                tx,
                block_hash,
                sequence + 1,
                "oracle_price",
                asset,
                previous
                    .map(serde_json::to_value)
                    .transpose()
                    .context("serialize prior oracle price")?,
            )
            .await?;
            sqlx::query("INSERT INTO oracle_prices(asset_address,price_raw,oracle_address,source_block_number,source_block_hash) VALUES($1,$2,$3,$4,$5) ON CONFLICT(asset_address) DO UPDATE SET price_raw=EXCLUDED.price_raw,oracle_address=EXCLUDED.oracle_address,source_block_number=EXCLUDED.source_block_number,source_block_hash=EXCLUDED.source_block_hash,updated_at=NOW()")
                .bind(asset).bind(decimal_raw(price_raw)?).bind(oracle).bind(block_number).bind(block_hash)
                .execute(&mut **tx).await.context("project Aave oracle price")?;
        }
    }
    Ok(())
}

async fn change_reserve(
    tx: &mut Transaction<'_, Postgres>,
    change: ReserveChange<'_>,
) -> Result<()> {
    let ReserveChange {
        block_hash,
        block_number,
        sequence,
        user,
        reserve,
        collateral_delta,
        debt_delta,
        enabled,
    } = change;
    let previous = sqlx::query_as::<_,ReserveRow>("SELECT user_address,reserve_address,collateral_amount_raw,debt_amount_raw,collateral_enabled,source_block_number,source_block_hash FROM user_reserves WHERE user_address=$1 AND reserve_address=$2")
        .bind(user).bind(reserve).fetch_optional(&mut **tx).await.context("read previous reserve projection")?;
    journal(
        tx,
        block_hash,
        sequence,
        "user_reserve",
        &format!("{user}:{reserve}"),
        previous
            .map(serde_json::to_value)
            .transpose()
            .context("serialize prior reserve projection")?,
    )
    .await?;
    sqlx::query("INSERT INTO user_reserves(user_address,reserve_address,collateral_amount_raw,debt_amount_raw,collateral_enabled,source_block_number,source_block_hash) VALUES($1,$2,GREATEST($3,0),GREATEST($4,0),COALESCE($5,FALSE),$6,$7) ON CONFLICT(user_address,reserve_address) DO UPDATE SET collateral_amount_raw=GREATEST(user_reserves.collateral_amount_raw+$8,0),debt_amount_raw=GREATEST(user_reserves.debt_amount_raw+$9,0),collateral_enabled=COALESCE($10,user_reserves.collateral_enabled),source_block_number=$6,source_block_hash=$7,updated_at=NOW()")
        .bind(user).bind(reserve).bind(collateral_delta.clone()).bind(debt_delta.clone()).bind(enabled).bind(block_number).bind(block_hash).bind(collateral_delta).bind(debt_delta).bind(enabled)
        .execute(&mut **tx).await.context("apply normalized reserve delta")?;
    Ok(())
}

async fn journal(
    tx: &mut Transaction<'_, Postgres>,
    block_hash: &str,
    sequence: i64,
    kind: &str,
    key: &str,
    before: Option<Value>,
) -> Result<()> {
    sqlx::query("INSERT INTO state_journal(block_hash,sequence,entity_kind,entity_key,before_state) VALUES($1,$2,$3,$4,$5)")
        .bind(block_hash).bind(sequence).bind(kind).bind(key).bind(before.map(Json))
        .execute(&mut **tx).await.context("write state undo journal")?;
    Ok(())
}

async fn restore(
    tx: &mut Transaction<'_, Postgres>,
    kind: &str,
    key: &str,
    before: Option<Value>,
) -> Result<()> {
    match kind {
        "user_reserve" => {
            let (user, reserve) = key.split_once(':').context("invalid reserve journal key")?;
            if let Some(value) = before {
                let row: ReserveRow =
                    serde_json::from_value(value).context("decode previous reserve projection")?;
                sqlx::query("INSERT INTO user_reserves(user_address,reserve_address,collateral_amount_raw,debt_amount_raw,collateral_enabled,source_block_number,source_block_hash) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(user_address,reserve_address) DO UPDATE SET collateral_amount_raw=EXCLUDED.collateral_amount_raw,debt_amount_raw=EXCLUDED.debt_amount_raw,collateral_enabled=EXCLUDED.collateral_enabled,source_block_number=EXCLUDED.source_block_number,source_block_hash=EXCLUDED.source_block_hash,updated_at=NOW()")
                    .bind(row.user_address).bind(row.reserve_address).bind(row.collateral_amount_raw).bind(row.debt_amount_raw).bind(row.collateral_enabled).bind(row.source_block_number).bind(row.source_block_hash)
                    .execute(&mut **tx).await.context("restore reserve projection")?;
            } else {
                sqlx::query(
                    "DELETE FROM user_reserves WHERE user_address=$1 AND reserve_address=$2",
                )
                .bind(user)
                .bind(reserve)
                .execute(&mut **tx)
                .await
                .context("remove orphaned reserve projection")?;
            }
        }
        "account_snapshot" => {
            if let Some(value) = before {
                let row: AccountRow =
                    serde_json::from_value(value).context("decode previous account snapshot")?;
                sqlx::query("INSERT INTO account_snapshots(user_address,total_collateral_base,total_debt_base,liquidation_threshold_bps,health_factor,source_block_number,source_block_hash) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(user_address) DO UPDATE SET total_collateral_base=EXCLUDED.total_collateral_base,total_debt_base=EXCLUDED.total_debt_base,liquidation_threshold_bps=EXCLUDED.liquidation_threshold_bps,health_factor=EXCLUDED.health_factor,source_block_number=EXCLUDED.source_block_number,source_block_hash=EXCLUDED.source_block_hash,updated_at=NOW()")
                    .bind(row.user_address).bind(row.total_collateral_base).bind(row.total_debt_base).bind(row.liquidation_threshold_bps).bind(row.health_factor).bind(row.source_block_number).bind(row.source_block_hash)
                    .execute(&mut **tx).await.context("restore account snapshot")?;
            } else {
                sqlx::query("DELETE FROM account_snapshots WHERE user_address=$1")
                    .bind(key)
                    .execute(&mut **tx)
                    .await
                    .context("remove orphaned account snapshot")?;
            }
        }
        "oracle_price" => {
            if let Some(value) = before {
                let row: OracleRow =
                    serde_json::from_value(value).context("decode previous oracle price")?;
                sqlx::query("INSERT INTO oracle_prices(asset_address,price_raw,oracle_address,source_block_number,source_block_hash) VALUES($1,$2,$3,$4,$5) ON CONFLICT(asset_address) DO UPDATE SET price_raw=EXCLUDED.price_raw,oracle_address=EXCLUDED.oracle_address,source_block_number=EXCLUDED.source_block_number,source_block_hash=EXCLUDED.source_block_hash,updated_at=NOW()")
                    .bind(row.asset_address).bind(row.price_raw).bind(row.oracle_address).bind(row.source_block_number).bind(row.source_block_hash)
                    .execute(&mut **tx).await.context("restore oracle price")?;
            } else {
                sqlx::query("DELETE FROM oracle_prices WHERE asset_address=$1")
                    .bind(key)
                    .execute(&mut **tx)
                    .await
                    .context("remove orphaned oracle price")?;
            }
        }
        _ => bail!("unknown journal entity kind: {kind}"),
    }
    Ok(())
}

fn delta_kind(delta: &ProtocolDelta) -> &'static str {
    match delta {
        ProtocolDelta::Supply { .. } => "supply",
        ProtocolDelta::Withdraw { .. } => "withdraw",
        ProtocolDelta::Borrow { .. } => "borrow",
        ProtocolDelta::Repay { .. } => "repay",
        ProtocolDelta::Liquidation { .. } => "liquidation",
        ProtocolDelta::CollateralFlagChanged { .. } => "collateral_flag_changed",
        ProtocolDelta::UserEModeChanged { .. } => "user_emode_changed",
        ProtocolDelta::ReserveRiskChanged { .. } => "reserve_risk_changed",
        ProtocolDelta::PriceObserved { .. } => "price_observed",
    }
}

fn decimal_raw(value: &str) -> Result<BigDecimal> {
    BigDecimal::from_str(value).context("invalid raw integer amount")
}

fn block_extends(head_number: i64, head_hash: &str, next_number: i64, parent_hash: &str) -> bool {
    next_number == head_number.saturating_add(1) && parent_hash.eq_ignore_ascii_case(head_hash)
}

#[cfg(test)]
mod tests {
    use std::{
        str::FromStr,
        time::{SystemTime, UNIX_EPOCH},
    };

    use alloy::primitives::B256;
    use bigdecimal::BigDecimal;
    use sqlx::{
        Connection,
        postgres::{PgConnectOptions, PgPoolOptions},
    };

    use crate::{
        config::DeploymentMetadata,
        domain::{AccountSnapshot, ProtocolDelta},
    };

    use super::{BlockHeader, IndexedDelta, Store, block_extends};

    #[test]
    fn block_order_requires_next_height_and_matching_parent() {
        assert!(block_extends(100, "0xabc", 101, "0xAbC"));
        assert!(!block_extends(100, "0xabc", 102, "0xabc"));
        assert!(!block_extends(100, "0xabc", 101, "0xdef"));
    }

    #[tokio::test]
    async fn postgres_projection_rolls_back_and_replays_a_reorg_branch() -> anyhow::Result<()> {
        let Ok(database_url) = std::env::var("DATABASE_URL") else {
            eprintln!("skipping PostgreSQL reorg integration test: DATABASE_URL is not set");
            return Ok(());
        };

        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let schema = format!("liq_reorg_test_{}_{}", std::process::id(), suffix);
        let mut admin = sqlx::PgConnection::connect(&database_url).await?;
        // Generated from a fixed prefix, process ID, and nanosecond timestamp.
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!("CREATE SCHEMA \"{schema}\"")))
            .execute(&mut admin)
            .await?;

        let options =
            PgConnectOptions::from_str(&database_url)?.options([("search_path", schema.as_str())]);
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect_with(options)
            .await?;
        sqlx::migrate!().run(&pool).await?;
        let store = Store::new(pool.clone());
        store
            .ensure_deployment(&DeploymentMetadata {
                chain_id: 42161,
                pool: alloy::primitives::Address::from_str(
                    "0x794a61358d6845594f94dc1db02a252b5b4814ad",
                )?,
                protocol_revision: "integration-test".to_owned(),
                start_block: 100,
            })
            .await?;

        let user = "0x0000000000000000000000000000000000000001";
        let collateral = "0x0000000000000000000000000000000000000010";
        let debt = "0x0000000000000000000000000000000000000020";
        let oracle = "0x0000000000000000000000000000000000000030";

        let block_100 = header(100, 100, 99);
        store
            .apply_block(
                &block_100,
                vec![
                    delta(
                        0,
                        ProtocolDelta::Supply {
                            user: user.into(),
                            reserve: collateral.into(),
                            amount_raw: "10".into(),
                        },
                    ),
                    delta(
                        1,
                        ProtocolDelta::Borrow {
                            user: user.into(),
                            reserve: debt.into(),
                            amount_raw: "5".into(),
                            rate_mode: 2,
                        },
                    ),
                    delta(
                        2,
                        ProtocolDelta::PriceObserved {
                            asset: collateral.into(),
                            price_raw: "2000".into(),
                            oracle: oracle.into(),
                        },
                    ),
                ],
            )
            .await?;
        store
            .save_account_snapshot(&hash(100), 100, &snapshot(user, "1.2"))
            .await?;

        let block_101 = header(101, 101, 100);
        store
            .apply_block(
                &block_101,
                vec![delta(
                    0,
                    ProtocolDelta::Supply {
                        user: user.into(),
                        reserve: collateral.into(),
                        amount_raw: "2".into(),
                    },
                )],
            )
            .await?;
        store
            .save_account_snapshot(&hash(101), 101, &snapshot(user, "1.1"))
            .await?;

        let block_102a = header(102, 1021, 101);
        store
            .apply_block(
                &block_102a,
                vec![
                    delta(
                        0,
                        ProtocolDelta::Borrow {
                            user: user.into(),
                            reserve: debt.into(),
                            amount_raw: "2".into(),
                            rate_mode: 2,
                        },
                    ),
                    delta(
                        1,
                        ProtocolDelta::PriceObserved {
                            asset: collateral.into(),
                            price_raw: "1900".into(),
                            oracle: oracle.into(),
                        },
                    ),
                ],
            )
            .await?;
        store
            .save_account_snapshot(&hash(1021), 102, &snapshot(user, "0.8"))
            .await?;
        let block_103a = header(103, 1031, 1021);
        store.apply_block(&block_103a, vec![]).await?;

        store.rollback_after(101).await?;
        assert_eq!(
            store.head().await?.map(|head| (head.number, head.hash)),
            Some((101, b256(101)))
        );
        assert_eq!(
            store.oracle_price(collateral).await?.as_deref(),
            Some("2000")
        );
        let restored_hf: BigDecimal =
            sqlx::query_scalar("SELECT health_factor FROM account_snapshots WHERE user_address=$1")
                .bind(user)
                .fetch_one(&pool)
                .await?;
        assert_eq!(restored_hf, BigDecimal::from_str("1.1")?);

        let block_102b = header(102, 1022, 101);
        store
            .apply_block(
                &block_102b,
                vec![
                    delta(
                        0,
                        ProtocolDelta::Supply {
                            user: user.into(),
                            reserve: collateral.into(),
                            amount_raw: "3".into(),
                        },
                    ),
                    delta(
                        1,
                        ProtocolDelta::PriceObserved {
                            asset: collateral.into(),
                            price_raw: "1800".into(),
                            oracle: oracle.into(),
                        },
                    ),
                ],
            )
            .await?;
        store
            .save_account_snapshot(&hash(1022), 102, &snapshot(user, "1.0"))
            .await?;
        let block_103b = header(103, 1032, 1022);
        store
            .apply_block(
                &block_103b,
                vec![delta(
                    0,
                    ProtocolDelta::Repay {
                        user: user.into(),
                        reserve: debt.into(),
                        amount_raw: "2".into(),
                    },
                )],
            )
            .await?;
        store
            .save_account_snapshot(&hash(1032), 103, &snapshot(user, "1.3"))
            .await?;
        let block_104b = header(104, 1042, 1032);
        store.apply_block(&block_104b, vec![]).await?;

        let reserve_amounts: (BigDecimal, BigDecimal) = sqlx::query_as(
            "SELECT collateral_amount_raw,debt_amount_raw FROM user_reserves WHERE user_address=$1 AND reserve_address=$2",
        ).bind(user).bind(collateral).fetch_one(&pool).await?;
        assert_eq!(reserve_amounts.0, BigDecimal::from(15));
        let debt_amount: BigDecimal = sqlx::query_scalar(
            "SELECT debt_amount_raw FROM user_reserves WHERE user_address=$1 AND reserve_address=$2",
        ).bind(user).bind(debt).fetch_one(&pool).await?;
        assert_eq!(debt_amount, BigDecimal::from(3));
        assert_eq!(
            store.oracle_price(collateral).await?.as_deref(),
            Some("1800")
        );
        assert_eq!(
            store.head().await?.map(|head| (head.number, head.hash)),
            Some((104, b256(1042)))
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM canonical_blocks WHERE number>101")
                .fetch_one(&pool)
                .await?,
            3
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM protocol_logs WHERE block_hash=$1")
                .bind(hash(1021))
                .fetch_one(&pool)
                .await?,
            0
        );

        pool.close().await;
        sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
            "DROP SCHEMA \"{schema}\" CASCADE"
        )))
        .execute(&mut admin)
        .await?;
        admin.close().await?;
        Ok(())
    }

    fn delta(log_index: u64, delta: ProtocolDelta) -> IndexedDelta {
        IndexedDelta {
            log_index,
            transaction_index: 0,
            transaction_hash: format!("fixture-{log_index}"),
            contract_address: "0x0000000000000000000000000000000000000009".into(),
            delta,
            raw_log: serde_json::json!({"fixture": true}),
        }
    }

    fn snapshot(user: &str, health_factor: &str) -> AccountSnapshot {
        AccountSnapshot {
            user: user.into(),
            total_collateral_base: "100000000".into(),
            total_debt_base: "50000000".into(),
            liquidation_threshold_bps: 8000,
            health_factor: health_factor.into(),
        }
    }

    fn header(number: u64, code: u64, parent_code: u64) -> BlockHeader {
        BlockHeader {
            number,
            hash: b256(code),
            parent_hash: b256(parent_code),
        }
    }

    fn b256(code: u64) -> B256 {
        B256::from_str(&format!("0x{code:064x}")).expect("valid test hash")
    }

    fn hash(code: u64) -> String {
        format!("{:#x}", b256(code))
    }
}
