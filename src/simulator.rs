use alloy::{
    network::TransactionBuilder,
    primitives::{Address, Bytes, U256, keccak256},
    providers::Provider,
    rpc::types::{BlockId, BlockNumberOrTag, TransactionRequest},
};
use anyhow::{Context, Result};
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct SimulationResult {
    pub gas_used: u64,
}

pub struct LiquidationRequest {
    pub pool: Address,
    pub caller: Address,
    pub collateral_asset: Address,
    pub debt_asset: Address,
    pub user: Address,
    pub block_number: u64,
}

pub async fn simulate_liquidation<P: Provider>(
    provider: &P,
    request: LiquidationRequest,
    rpc_timeout: Duration,
) -> Result<SimulationResult> {
    let block = BlockId::Number(BlockNumberOrTag::Number(request.block_number));
    let calldata =
        liquidation_call_calldata(request.collateral_asset, request.debt_asset, request.user);
    let transaction = TransactionRequest::default()
        .from(request.caller)
        .to(request.pool)
        .with_input(calldata);

    tokio::time::timeout(rpc_timeout, provider.call(transaction.clone()).block(block))
        .await
        .context("Aave liquidationCall eth_call timed out")?
        .context("Aave liquidationCall eth_call reverted")?;
    let gas_used =
        tokio::time::timeout(rpc_timeout, provider.estimate_gas(transaction).block(block))
            .await
            .context("Aave liquidationCall gas estimation timed out")?
            .context("estimate Aave liquidationCall gas")?;

    Ok(SimulationResult { gas_used })
}

fn liquidation_call_calldata(
    collateral_asset: Address,
    debt_asset: Address,
    user: Address,
) -> Bytes {
    let selector = keccak256("liquidationCall(address,address,address,uint256,bool)".as_bytes());
    let mut data = Vec::with_capacity(4 + 32 * 5);
    data.extend_from_slice(&selector[..4]);
    push_address(&mut data, collateral_asset);
    push_address(&mut data, debt_asset);
    push_address(&mut data, user);
    data.extend_from_slice(&U256::MAX.to_be_bytes::<32>());
    data.extend_from_slice(&U256::ZERO.to_be_bytes::<32>());
    Bytes::from(data)
}

fn push_address(data: &mut Vec<u8>, address: Address) {
    data.extend_from_slice(&[0_u8; 12]);
    data.extend_from_slice(address.as_slice());
}
