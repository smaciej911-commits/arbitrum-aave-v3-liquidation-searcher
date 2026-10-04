use std::collections::HashMap;

use crate::domain::ProtocolDelta;
use alloy::{
    primitives::{Address, B256, U256},
    sol,
    sol_types::SolEvent,
};
use anyhow::{Context, Result};

sol! {
    event Supply(address indexed reserve, address user, address indexed onBehalfOf, uint256 amount, uint16 indexed referralCode);
    event Withdraw(address indexed reserve, address indexed user, address indexed to, uint256 amount);
    event Borrow(address indexed reserve, address user, address indexed onBehalfOf, uint256 amount, uint8 interestRateMode, uint256 borrowRate, uint16 indexed referralCode);
    event Repay(address indexed reserve, address indexed user, address indexed repayer, uint256 amount, bool useATokens);
    event LiquidationCall(address indexed collateralAsset, address indexed debtAsset, address indexed user, uint256 debtToCover, uint256 liquidatedCollateralAmount, address liquidator, bool receiveAToken);
    event ReserveUsedAsCollateralEnabled(address indexed reserve, address indexed user);
    event ReserveUsedAsCollateralDisabled(address indexed reserve, address indexed user);
    event UserEModeSet(address indexed user, uint8 categoryId);
    event CollateralConfigurationChanged(address indexed asset, uint256 ltv, uint256 liquidationThreshold, uint256 liquidationBonus);
    event AnswerUpdated(int256 indexed current, uint256 indexed roundId, uint256 updatedAt);
}

#[derive(Clone, Debug)]
pub struct DecodedLog {
    pub delta: ProtocolDelta,
    pub user: Option<String>,
    pub assets: Vec<String>,
}

pub fn supported_signatures() -> Vec<B256> {
    vec![
        Supply::SIGNATURE_HASH,
        Withdraw::SIGNATURE_HASH,
        Borrow::SIGNATURE_HASH,
        Repay::SIGNATURE_HASH,
        LiquidationCall::SIGNATURE_HASH,
        ReserveUsedAsCollateralEnabled::SIGNATURE_HASH,
        ReserveUsedAsCollateralDisabled::SIGNATURE_HASH,
        UserEModeSet::SIGNATURE_HASH,
        CollateralConfigurationChanged::SIGNATURE_HASH,
        AnswerUpdated::SIGNATURE_HASH,
    ]
}

/// Decode only protocol events we understand. The upstream signature filter
/// excludes unknown events; decoded supported logs retain their raw RPC data.
pub fn decode(
    log: &alloy::rpc::types::Log,
    oracle_feeds: &HashMap<Address, Address>,
    pool: Address,
    pool_configurator: Address,
) -> Result<Option<DecodedLog>> {
    let Some(topic0) = log.topic0() else {
        return Ok(None);
    };
    let address = |value: Address| format!("{value:#x}").to_ascii_lowercase();
    let mut decoded = if *topic0 == Supply::SIGNATURE_HASH && log.address() == pool {
        let e = log
            .log_decode_validate::<Supply>()
            .context("decode Aave Supply")?
            .inner
            .data;
        let user = address(e.onBehalfOf);
        let reserve = address(e.reserve);
        DecodedLog {
            delta: ProtocolDelta::Supply {
                user: user.clone(),
                reserve: reserve.clone(),
                amount_raw: uint(e.amount),
            },
            user: Some(user),
            assets: vec![reserve],
        }
    } else if *topic0 == Withdraw::SIGNATURE_HASH && log.address() == pool {
        let e = log
            .log_decode_validate::<Withdraw>()
            .context("decode Aave Withdraw")?
            .inner
            .data;
        let user = address(e.user);
        let reserve = address(e.reserve);
        DecodedLog {
            delta: ProtocolDelta::Withdraw {
                user: user.clone(),
                reserve: reserve.clone(),
                amount_raw: uint(e.amount),
            },
            user: Some(user),
            assets: vec![reserve],
        }
    } else if *topic0 == Borrow::SIGNATURE_HASH && log.address() == pool {
        let e = log
            .log_decode_validate::<Borrow>()
            .context("decode Aave Borrow")?
            .inner
            .data;
        let user = address(e.onBehalfOf);
        let reserve = address(e.reserve);
        DecodedLog {
            delta: ProtocolDelta::Borrow {
                user: user.clone(),
                reserve: reserve.clone(),
                amount_raw: uint(e.amount),
                rate_mode: e.interestRateMode,
            },
            user: Some(user),
            assets: vec![reserve],
        }
    } else if *topic0 == Repay::SIGNATURE_HASH && log.address() == pool {
        let e = log
            .log_decode_validate::<Repay>()
            .context("decode Aave Repay")?
            .inner
            .data;
        let user = address(e.user);
        let reserve = address(e.reserve);
        DecodedLog {
            delta: ProtocolDelta::Repay {
                user: user.clone(),
                reserve: reserve.clone(),
                amount_raw: uint(e.amount),
            },
            user: Some(user),
            assets: vec![reserve],
        }
    } else if *topic0 == LiquidationCall::SIGNATURE_HASH && log.address() == pool {
        let e = log
            .log_decode_validate::<LiquidationCall>()
            .context("decode Aave LiquidationCall")?
            .inner
            .data;
        let user = address(e.user);
        let collateral_asset = address(e.collateralAsset);
        let debt_asset = address(e.debtAsset);
        DecodedLog {
            delta: ProtocolDelta::Liquidation {
                user: user.clone(),
                collateral_asset: collateral_asset.clone(),
                debt_asset: debt_asset.clone(),
                debt_to_cover_raw: uint(e.debtToCover),
                liquidated_collateral_raw: uint(e.liquidatedCollateralAmount),
            },
            user: Some(user),
            assets: vec![collateral_asset, debt_asset],
        }
    } else if *topic0 == ReserveUsedAsCollateralEnabled::SIGNATURE_HASH && log.address() == pool {
        let e = log
            .log_decode_validate::<ReserveUsedAsCollateralEnabled>()
            .context("decode Aave collateral enabled")?
            .inner
            .data;
        let user = address(e.user);
        let reserve = address(e.reserve);
        DecodedLog {
            delta: ProtocolDelta::CollateralFlagChanged {
                user: user.clone(),
                reserve: reserve.clone(),
                enabled: true,
            },
            user: Some(user),
            assets: vec![reserve],
        }
    } else if *topic0 == ReserveUsedAsCollateralDisabled::SIGNATURE_HASH && log.address() == pool {
        let e = log
            .log_decode_validate::<ReserveUsedAsCollateralDisabled>()
            .context("decode Aave collateral disabled")?
            .inner
            .data;
        let user = address(e.user);
        let reserve = address(e.reserve);
        DecodedLog {
            delta: ProtocolDelta::CollateralFlagChanged {
                user: user.clone(),
                reserve: reserve.clone(),
                enabled: false,
            },
            user: Some(user),
            assets: vec![reserve],
        }
    } else if *topic0 == UserEModeSet::SIGNATURE_HASH && log.address() == pool {
        let e = log
            .log_decode_validate::<UserEModeSet>()
            .context("decode Aave UserEModeSet")?
            .inner
            .data;
        let user = address(e.user);
        DecodedLog {
            delta: ProtocolDelta::UserEModeChanged {
                user: user.clone(),
                category_id: e.categoryId,
            },
            user: Some(user),
            assets: vec![],
        }
    } else if *topic0 == CollateralConfigurationChanged::SIGNATURE_HASH
        && log.address() == pool_configurator
    {
        let e = log
            .log_decode_validate::<CollateralConfigurationChanged>()
            .context("decode Aave CollateralConfigurationChanged")?
            .inner
            .data;
        let threshold = u16::try_from(e.liquidationThreshold)
            .context("Aave liquidation threshold exceeds u16")?;
        let bonus =
            u16::try_from(e.liquidationBonus).context("Aave liquidation bonus exceeds u16")?;
        if threshold > 10_000 || bonus > 20_000 {
            anyhow::bail!("Aave reserve risk parameters outside supported bounds");
        }
        let reserve = address(e.asset);
        DecodedLog {
            delta: ProtocolDelta::ReserveRiskChanged {
                reserve: reserve.clone(),
                liquidation_threshold_bps: threshold,
                liquidation_bonus_bps: bonus,
            },
            user: None,
            assets: vec![reserve],
        }
    } else if *topic0 == AnswerUpdated::SIGNATURE_HASH {
        let Some(asset) = oracle_feeds.get(&log.address()).copied() else {
            return Ok(None);
        };
        let e = log
            .log_decode_validate::<AnswerUpdated>()
            .context("decode Chainlink AnswerUpdated")?
            .inner
            .data;
        let feed = address(log.address());
        let asset = address(asset);
        let answer = e.current.to_string();
        if answer.starts_with('-') || answer == "0" {
            return Ok(None);
        }
        DecodedLog {
            delta: ProtocolDelta::PriceObserved {
                asset: asset.clone(),
                price_raw: answer,
                oracle: feed,
            },
            user: None,
            assets: vec![asset],
        }
    } else {
        return Ok(None);
    };

    decoded.assets.sort();
    decoded.assets.dedup();
    Ok(Some(decoded))
}

fn uint(value: U256) -> String {
    value.to_string()
}
