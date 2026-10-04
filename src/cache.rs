use std::collections::HashMap;

use anyhow::{Context, Result};
use bigdecimal::BigDecimal;
use redis::{AsyncCommands, aio::MultiplexedConnection};

use crate::store::Store;

const RISK_KEYS: [&str; 3] = [
    "liquidation-engine:hf:1.20",
    "liquidation-engine:hf:1.10",
    "liquidation-engine:hf:1.05",
];

pub struct CandidateCache {
    connection: MultiplexedConnection,
}

impl CandidateCache {
    pub fn new(connection: MultiplexedConnection) -> Self {
        Self { connection }
    }

    pub async fn rebuild(&mut self, store: &Store) -> Result<()> {
        let _: () = self
            .connection
            .del(&RISK_KEYS[..])
            .await
            .context("clear Redis risk indexes")?;
        for (user, health_factor) in store.account_snapshots().await? {
            self.update(&user, &health_factor).await?;
        }
        Ok(())
    }

    pub async fn update(&mut self, user: &str, health_factor: &BigDecimal) -> Result<()> {
        let score = health_factor
            .to_string()
            .parse::<f64>()
            .context("convert health factor for Redis score")?;
        let thresholds = [
            BigDecimal::from(120_u32) / BigDecimal::from(100_u32),
            BigDecimal::from(110_u32) / BigDecimal::from(100_u32),
            BigDecimal::from(105_u32) / BigDecimal::from(100_u32),
        ];
        let mut pipe = redis::pipe();
        for key in RISK_KEYS {
            pipe.cmd("ZREM").arg(key).arg(user).ignore();
        }
        for (key, threshold) in RISK_KEYS.into_iter().zip(thresholds) {
            if health_factor < &threshold {
                pipe.cmd("ZADD").arg(key).arg(score).arg(user).ignore();
            }
        }
        pipe.query_async(&mut self.connection)
            .await
            .context("update Redis candidate rank")
    }

    pub async fn remove(&mut self, user: &str) -> Result<()> {
        let mut pipe = redis::pipe();
        for key in RISK_KEYS {
            pipe.cmd("ZREM").arg(key).arg(user).ignore();
        }
        pipe.query_async(&mut self.connection)
            .await
            .context("remove Redis candidate")
    }

    pub async fn most_at_risk(&mut self, limit: usize) -> Result<Vec<String>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let end =
            isize::try_from(limit - 1).context("candidate refresh limit exceeds Redis range")?;
        let mut scores = HashMap::<String, f64>::new();
        for key in RISK_KEYS {
            let ranked: Vec<(String, f64)> =
                self.connection
                    .zrange_withscores(key, 0, end)
                    .await
                    .with_context(|| format!("read ranked Redis risk index {key}"))?;
            for (user, score) in ranked {
                scores
                    .entry(user)
                    .and_modify(|current| *current = current.min(score))
                    .or_insert(score);
            }
        }
        let mut ranked: Vec<_> = scores.into_iter().collect();
        ranked.sort_by(|a, b| a.1.total_cmp(&b.1));
        ranked.truncate(limit);
        Ok(ranked.into_iter().map(|(user, _)| user).collect())
    }
}
