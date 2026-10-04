use std::{str::FromStr, time::Duration};

use crate::domain::AccountSnapshot;
use alloy::{
    network::TransactionBuilder,
    primitives::{Address, Bytes, U256, keccak256},
    providers::Provider,
    rpc::types::{BlockId, BlockNumberOrTag, TransactionRequest},
};
use anyhow::{Context, Result, bail};

#[derive(Clone, Debug)]
pub struct ReserveSnapshot {
    pub decimals: u8,
    pub liquidation_bonus_bps: u16,
    pub liquidation_protocol_fee_bps: u16,
    pub collateral_amount_raw: String,
    pub debt_amount_raw: String,
    pub collateral_enabled: bool,
}

pub async fn user_account_data<P: Provider>(
    provider: &P,
    pool: Address,
    user: &str,
    block_number: u64,
    timeout: Duration,
) -> Result<AccountSnapshot> {
    let user_address = Address::from_str(user).context("invalid Aave account address")?;
    let calldata = encode_call("getUserAccountData(address)", &[user_address]);
    let response = call_at(
        provider,
        pool,
        calldata,
        block_number,
        timeout,
        "getUserAccountData",
    )
    .await?;
    if response.len() < 6 * 32 {
        bail!("getUserAccountData returned fewer than six ABI words");
    }
    let collateral = word(&response, 0).to_string();
    let debt = word(&response, 1).to_string();
    let threshold =
        u16::try_from(word(&response, 3)).context("Aave liquidation threshold exceeds u16")?;
    let hf_raw = word(&response, 5);
    let raw = hf_raw
        .to_string()
        .parse::<bigdecimal::BigDecimal>()
        .context("parse health factor")?;
    let health_factor = (raw / bigdecimal::BigDecimal::from(10_u8).powi(18))
        .normalized()
        .to_string();
    Ok(AccountSnapshot {
        user: user.to_ascii_lowercase(),
        total_collateral_base: collateral,
        total_debt_base: debt,
        liquidation_threshold_bps: threshold,
        health_factor,
    })
}

pub async fn aave_asset_price<P: Provider>(
    provider: &P,
    oracle: Address,
    asset: Address,
    block_number: u64,
    timeout: Duration,
) -> Result<String> {
    let calldata = encode_call("getAssetPrice(address)", &[asset]);
    let response = call_at(
        provider,
        oracle,
        calldata,
        block_number,
        timeout,
        "AaveOracle.getAssetPrice",
    )
    .await?;
    if response.len() < 32 {
        bail!("AaveOracle.getAssetPrice returned no ABI word");
    }
    let price = word(&response, 0);
    if price == U256::ZERO {
        bail!("Aave oracle returned a zero price for {asset:#x}");
    }
    Ok(price.to_string())
}

pub async fn user_reserve_data<P: Provider>(
    provider: &P,
    data_provider: Address,
    pool: Address,
    user: &str,
    asset: &str,
    block_number: u64,
    timeout: Duration,
) -> Result<ReserveSnapshot> {
    let user_address = Address::from_str(user).context("invalid user address")?;
    let asset_address = Address::from_str(asset).context("invalid reserve address")?;
    let configuration = call_at(
        provider,
        data_provider,
        encode_call("getReserveConfigurationData(address)", &[asset_address]),
        block_number,
        timeout,
        "AaveProtocolDataProvider.getReserveConfigurationData",
    )
    .await?;
    let user_state = call_at(
        provider,
        data_provider,
        encode_call(
            "getUserReserveData(address,address)",
            &[asset_address, user_address],
        ),
        block_number,
        timeout,
        "AaveProtocolDataProvider.getUserReserveData",
    )
    .await?;
    let packed_configuration = call_at(
        provider,
        pool,
        encode_call("getConfiguration(address)", &[asset_address]),
        block_number,
        timeout,
        "Pool.getConfiguration",
    )
    .await?;
    if configuration.len() < 10 * 32 || user_state.len() < 9 * 32 || packed_configuration.len() < 32
    {
        bail!("Aave reserve views returned a truncated response");
    }
    let decimals = u8::try_from(word(&configuration, 0)).context("reserve decimals exceed u8")?;
    let bonus = u16::try_from(word(&configuration, 3)).context("liquidation bonus exceeds u16")?;
    let protocol_fee =
        u16::try_from((word(&packed_configuration, 0) >> 152) & U256::from(u16::MAX))
            .context("liquidation protocol fee exceeds u16")?;
    if decimals > 36 || bonus > 20_000 || protocol_fee > 10_000 {
        bail!("Aave reserve configuration outside supported bounds");
    }
    let collateral = word(&user_state, 0);
    let stable = word(&user_state, 1);
    let variable = word(&user_state, 2);
    let enabled = word(&user_state, 8) != U256::ZERO;
    Ok(ReserveSnapshot {
        decimals,
        liquidation_bonus_bps: bonus,
        liquidation_protocol_fee_bps: protocol_fee,
        collateral_amount_raw: collateral.to_string(),
        debt_amount_raw: (stable + variable).to_string(),
        collateral_enabled: enabled,
    })
}

pub async fn effective_liquidation_bonus<P: Provider>(
    provider: &P,
    pool: Address,
    user: &str,
    asset: &str,
    reserve_bonus_bps: u16,
    block_number: u64,
    timeout: Duration,
) -> Result<u16> {
    let user = Address::from_str(user).context("invalid user address")?;
    let asset = Address::from_str(asset).context("invalid reserve address")?;
    let mode = call_at(
        provider,
        pool,
        encode_call("getUserEMode(address)", &[user]),
        block_number,
        timeout,
        "Pool.getUserEMode",
    )
    .await?;
    if mode.len() < 32 {
        bail!("Pool.getUserEMode returned no ABI word");
    }
    let category = u8::try_from(word(&mode, 0)).context("eMode category exceeds u8")?;
    if category == 0 {
        return Ok(reserve_bonus_bps);
    }

    let reserve = call_at(
        provider,
        pool,
        encode_call("getReserveData(address)", &[asset]),
        block_number,
        timeout,
        "Pool.getReserveData",
    )
    .await?;
    if reserve.len() < 8 * 32 {
        bail!("Pool.getReserveData returned a truncated reserve response");
    }
    let reserve_id = u32::try_from(word(&reserve, 7)).context("reserve id exceeds u32")?;
    let bitmap = call_at(
        provider,
        pool,
        encode_call_u256(
            "getEModeCategoryCollateralBitmap(uint8)",
            &[U256::from(category)],
        ),
        block_number,
        timeout,
        "Pool.getEModeCategoryCollateralBitmap",
    )
    .await?;
    if bitmap.len() < 32 {
        bail!("Pool eMode collateral bitmap response is truncated");
    }
    if ((word(&bitmap, 0) >> reserve_id) & U256::from(1)) == U256::ZERO {
        return Ok(reserve_bonus_bps);
    }
    let config = call_at(
        provider,
        pool,
        encode_call_u256(
            "getEModeCategoryCollateralConfig(uint8)",
            &[U256::from(category)],
        ),
        block_number,
        timeout,
        "Pool.getEModeCategoryCollateralConfig",
    )
    .await?;
    if config.len() < 3 * 32 {
        bail!("Pool eMode collateral config response is truncated");
    }
    let bonus = u16::try_from(word(&config, 2)).context("eMode liquidation bonus exceeds u16")?;
    if bonus > 20_000 {
        bail!("eMode liquidation bonus outside supported bounds");
    }
    Ok(bonus)
}

async fn call_at<P: Provider>(
    provider: &P,
    to: Address,
    calldata: Bytes,
    block_number: u64,
    timeout: Duration,
    label: &str,
) -> Result<Bytes> {
    let block = BlockId::Number(BlockNumberOrTag::Number(block_number));
    tokio::time::timeout(
        timeout,
        provider
            .call(TransactionRequest::default().to(to).with_input(calldata))
            .block(block),
    )
    .await
    .with_context(|| format!("{label} timed out at block {block_number}"))?
    .with_context(|| format!("call {label} at block {block_number}"))
}

fn encode_call(signature: &str, addresses: &[Address]) -> Bytes {
    let selector = keccak256(signature.as_bytes());
    let mut bytes = Vec::with_capacity(4 + addresses.len() * 32);
    bytes.extend_from_slice(&selector[..4]);
    for address in addresses {
        bytes.extend_from_slice(&[0_u8; 12]);
        bytes.extend_from_slice(address.as_slice());
    }
    Bytes::from(bytes)
}

fn encode_call_u256(signature: &str, values: &[U256]) -> Bytes {
    let selector = keccak256(signature.as_bytes());
    let mut bytes = Vec::with_capacity(4 + values.len() * 32);
    bytes.extend_from_slice(&selector[..4]);
    for value in values {
        bytes.extend_from_slice(&value.to_be_bytes::<32>());
    }
    Bytes::from(bytes)
}

fn word(data: &[u8], index: usize) -> U256 {
    U256::from_be_slice(&data[index * 32..(index + 1) * 32])
}
