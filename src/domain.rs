use std::str::FromStr;

use anyhow::{Context, Result};
use bigdecimal::BigDecimal;
use serde::{Deserialize, Serialize};

/// Protocol-independent state transitions emitted by protocol adapters.
/// Amounts remain raw integer strings so no precision is lost during replay.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ProtocolDelta {
    Supply {
        user: String,
        reserve: String,
        amount_raw: String,
    },
    Withdraw {
        user: String,
        reserve: String,
        amount_raw: String,
    },
    Borrow {
        user: String,
        reserve: String,
        amount_raw: String,
        rate_mode: u8,
    },
    Repay {
        user: String,
        reserve: String,
        amount_raw: String,
    },
    Liquidation {
        user: String,
        collateral_asset: String,
        debt_asset: String,
        debt_to_cover_raw: String,
        liquidated_collateral_raw: String,
    },
    CollateralFlagChanged {
        user: String,
        reserve: String,
        enabled: bool,
    },
    UserEModeChanged {
        user: String,
        category_id: u8,
    },
    ReserveRiskChanged {
        reserve: String,
        liquidation_threshold_bps: u16,
        liquidation_bonus_bps: u16,
    },
    PriceObserved {
        asset: String,
        price_raw: String,
        oracle: String,
    },
}

impl ProtocolDelta {
    pub fn user(&self) -> Option<&str> {
        match self {
            Self::Supply { user, .. }
            | Self::Withdraw { user, .. }
            | Self::Borrow { user, .. }
            | Self::Repay { user, .. }
            | Self::Liquidation { user, .. }
            | Self::CollateralFlagChanged { user, .. }
            | Self::UserEModeChanged { user, .. } => Some(user),
            Self::PriceObserved { .. } | Self::ReserveRiskChanged { .. } => None,
        }
    }

    pub fn assets(&self) -> Vec<&str> {
        match self {
            Self::Supply { reserve, .. }
            | Self::Withdraw { reserve, .. }
            | Self::Borrow { reserve, .. }
            | Self::Repay { reserve, .. }
            | Self::CollateralFlagChanged { reserve, .. } => vec![reserve],
            Self::UserEModeChanged { .. } => vec![],
            Self::ReserveRiskChanged { reserve, .. } => vec![reserve],
            Self::Liquidation {
                collateral_asset,
                debt_asset,
                ..
            } => {
                vec![collateral_asset, debt_asset]
            }
            Self::PriceObserved { asset, .. } => vec![asset],
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct AccountSnapshot {
    pub user: String,
    pub total_collateral_base: String,
    pub total_debt_base: String,
    pub liquidation_threshold_bps: u16,
    pub health_factor: String,
}

#[derive(Clone, Debug)]
pub struct ProfitEstimate {
    pub expected_profit_usd: BigDecimal,
    pub gas_cost_usd: BigDecimal,
}

pub struct AssetValue<'a> {
    pub amount_raw: &'a str,
    pub decimals: u8,
    pub price_raw: &'a str,
}

pub struct ProfitAssumptions<'a> {
    pub liquidation_bonus_bps: u16,
    pub liquidation_protocol_fee_bps: u16,
    pub native_price_raw: &'a str,
    pub gas_price_gwei: &'a BigDecimal,
    pub gas_units: u64,
    pub slippage_bps: u16,
    pub flashloan_fee_bps: u16,
    pub execution_risk_bps: u16,
}

pub fn estimate_profit(
    account: &AccountSnapshot,
    collateral: AssetValue<'_>,
    debt: AssetValue<'_>,
    assumptions: ProfitAssumptions<'_>,
) -> Result<ProfitEstimate> {
    let ten_thousand = BigDecimal::from(10_000_u32);
    let scale = BigDecimal::from(10_u8).powi(8);
    let collateral = amount_value(
        collateral.amount_raw,
        collateral.decimals,
        collateral.price_raw,
    )?;
    let debt = amount_value(debt.amount_raw, debt.decimals, debt.price_raw)?;
    let total_debt_base =
        BigDecimal::from_str(&account.total_debt_base).context("invalid aggregate debt value")?;
    let total_debt = total_debt_base / scale.clone();
    let hf = BigDecimal::from_str(&account.health_factor).context("invalid health factor")?;
    let reserve_is_small = debt < 2_000_u32 || collateral < 2_000_u32;
    let full_close_factor_hf = BigDecimal::from(95_u32) / BigDecimal::from(100_u32);
    let close_factor = if hf <= full_close_factor_hf || reserve_is_small {
        BigDecimal::from(1)
    } else {
        BigDecimal::from(50_u32) / BigDecimal::from(100_u32)
    };
    let max_repay = if reserve_is_small || hf <= full_close_factor_hf {
        debt.clone()
    } else {
        std::cmp::min(debt.clone(), total_debt * close_factor)
    };
    let bonus = BigDecimal::from(assumptions.liquidation_bonus_bps);
    let debt_capped_by_collateral = if bonus > 0 {
        collateral.clone() * ten_thousand.clone() / bonus.clone()
    } else {
        BigDecimal::from(0)
    };
    let debt_cover = std::cmp::min(max_repay, debt_capped_by_collateral);
    let gross_bonus = if bonus > ten_thousand {
        debt_cover.clone() * (bonus - ten_thousand.clone()) / ten_thousand.clone()
    } else {
        BigDecimal::from(0)
    };
    let protocol_fee = gross_bonus.clone()
        * BigDecimal::from(assumptions.liquidation_protocol_fee_bps)
        / ten_thousand.clone();
    let liquidator_bonus = gross_bonus.clone() - protocol_fee;
    let slippage = (debt_cover.clone() + liquidator_bonus.clone())
        * BigDecimal::from(assumptions.slippage_bps)
        / ten_thousand.clone();
    let flashloan =
        debt_cover * BigDecimal::from(assumptions.flashloan_fee_bps) / ten_thousand.clone();
    let execution_risk =
        liquidator_bonus.clone() * BigDecimal::from(assumptions.execution_risk_bps) / ten_thousand;
    let native_price = BigDecimal::from_str(assumptions.native_price_raw)
        .context("invalid native asset price")?
        / scale;
    let gas_cost =
        BigDecimal::from(assumptions.gas_units) * assumptions.gas_price_gwei.clone() * native_price
            / BigDecimal::from(1_000_000_000_u64);
    Ok(ProfitEstimate {
        expected_profit_usd: liquidator_bonus
            - slippage
            - flashloan
            - execution_risk
            - gas_cost.clone(),
        gas_cost_usd: gas_cost,
    })
}

pub fn is_liquidatable(health_factor: &BigDecimal) -> bool {
    health_factor < &BigDecimal::from(1)
}

fn amount_value(amount_raw: &str, decimals: u8, price_raw: &str) -> Result<BigDecimal> {
    let amount = BigDecimal::from_str(amount_raw).context("invalid raw token amount")?;
    let price = BigDecimal::from_str(price_raw).context("invalid raw oracle price")?;
    Ok(amount * price
        / BigDecimal::from(10_u8).powi(i64::from(decimals))
        / BigDecimal::from(10_u8).powi(8))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn liquidation_threshold_is_strictly_below_one() {
        assert!(is_liquidatable(&BigDecimal::from_str("0.999999").unwrap()));
        assert!(!is_liquidatable(&BigDecimal::from(1)));
        assert!(!is_liquidatable(&BigDecimal::from_str("1.0001").unwrap()));
    }

    #[test]
    fn token_amount_and_usd_price_respect_token_decimals() {
        let value = amount_value("1250000", 6, "100000000").unwrap();
        assert_eq!(value, BigDecimal::from_str("1.25").unwrap());
    }

    #[test]
    fn profitability_deducts_execution_costs_from_bonus() {
        let account = AccountSnapshot {
            user: "0xabc".to_owned(),
            total_collateral_base: "200000000000".to_owned(),
            total_debt_base: "100000000000".to_owned(),
            liquidation_threshold_bps: 8000,
            health_factor: "0.8".to_owned(),
        };
        let gas_price = BigDecimal::from_str("0.1").unwrap();
        let estimate = estimate_profit(
            &account,
            AssetValue {
                amount_raw: "1000000000000000000",
                decimals: 18,
                price_raw: "200000000000",
            },
            AssetValue {
                amount_raw: "1000000000",
                decimals: 6,
                price_raw: "100000000",
            },
            ProfitAssumptions {
                liquidation_bonus_bps: 10_500,
                liquidation_protocol_fee_bps: 0,
                native_price_raw: "200000000000",
                gas_price_gwei: &gas_price,
                gas_units: 100_000,
                slippage_bps: 30,
                flashloan_fee_bps: 9,
                execution_risk_bps: 50,
            },
        )
        .unwrap();
        assert!(estimate.expected_profit_usd > 0);
        assert!(estimate.expected_profit_usd < 50);
        assert!(estimate.gas_cost_usd > 0);
    }
}
