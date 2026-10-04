CREATE TABLE deployment_metadata (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    chain_id BIGINT NOT NULL,
    pool_address TEXT NOT NULL,
    protocol_revision TEXT NOT NULL,
    start_block BIGINT NOT NULL,
    initialized_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE canonical_blocks (
    number BIGINT PRIMARY KEY,
    hash TEXT NOT NULL UNIQUE,
    parent_hash TEXT NOT NULL,
    canonical BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX canonical_blocks_parent_idx ON canonical_blocks(parent_hash);

CREATE TABLE protocol_logs (
    block_hash TEXT NOT NULL REFERENCES canonical_blocks(hash),
    log_index BIGINT NOT NULL,
    transaction_index BIGINT NOT NULL,
    transaction_hash TEXT NOT NULL,
    contract_address TEXT NOT NULL,
    delta_kind TEXT NOT NULL,
    delta JSONB NOT NULL,
    raw_log JSONB NOT NULL,
    PRIMARY KEY (block_hash, log_index)
);
CREATE INDEX protocol_logs_kind_idx ON protocol_logs(delta_kind);

CREATE TABLE user_reserves (
    user_address TEXT NOT NULL,
    reserve_address TEXT NOT NULL,
    collateral_amount_raw NUMERIC NOT NULL DEFAULT 0 CHECK (collateral_amount_raw >= 0),
    debt_amount_raw NUMERIC NOT NULL DEFAULT 0 CHECK (debt_amount_raw >= 0),
    collateral_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    source_block_number BIGINT NOT NULL,
    source_block_hash TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_address, reserve_address)
);
CREATE INDEX user_reserves_collateral_idx ON user_reserves(reserve_address, user_address) WHERE collateral_amount_raw > 0 AND collateral_enabled;
CREATE INDEX user_reserves_debt_idx ON user_reserves(reserve_address, user_address) WHERE debt_amount_raw > 0;

CREATE TABLE account_snapshots (
    user_address TEXT PRIMARY KEY,
    total_collateral_base NUMERIC NOT NULL,
    total_debt_base NUMERIC NOT NULL,
    liquidation_threshold_bps SMALLINT NOT NULL,
    health_factor NUMERIC NOT NULL,
    source_block_number BIGINT NOT NULL,
    source_block_hash TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX account_snapshots_hf_idx ON account_snapshots(health_factor);

CREATE TABLE oracle_prices (
    asset_address TEXT PRIMARY KEY,
    price_raw NUMERIC NOT NULL CHECK (price_raw > 0),
    oracle_address TEXT NOT NULL,
    source_block_number BIGINT NOT NULL,
    source_block_hash TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE state_journal (
    block_hash TEXT NOT NULL REFERENCES canonical_blocks(hash),
    sequence BIGINT NOT NULL,
    entity_kind TEXT NOT NULL CHECK (entity_kind IN ('user_reserve', 'account_snapshot', 'oracle_price')),
    entity_key TEXT NOT NULL,
    before_state JSONB,
    PRIMARY KEY (block_hash, sequence)
);

CREATE TABLE candidate_decisions (
    block_hash TEXT NOT NULL REFERENCES canonical_blocks(hash),
    user_address TEXT NOT NULL,
    health_factor NUMERIC NOT NULL,
    decision TEXT NOT NULL,
    expected_profit_usd NUMERIC,
    gas_cost_usd NUMERIC,
    collateral_asset TEXT,
    debt_asset TEXT,
    simulation_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (block_hash, user_address)
);
