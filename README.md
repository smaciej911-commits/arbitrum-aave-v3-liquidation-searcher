# Real-Time Liquidation Searcher

Rust observe-only indexer/searcher for Aave V3 on Arbitrum One. It consumes Pool and configured oracle logs, writes a fork-aware PostgreSQL projection, gets authoritative account health at a pinned block, ranks accounts in Redis, estimates liquidation economics, and tries a read-only `liquidationCall` simulation.

It never signs or broadcasts a transaction. There is no private-key configuration or transaction submission path.

## Architecture

```text
RPC WebSocket: newHeads
        │
        ├── Aave Pool logs ──► Aave event decoder ──┐
        └── oracle feed logs ─► Aave Oracle read ───┤
                                                    ▼
                                        ProtocolDelta + raw log
                                                    │
                                  PostgreSQL canonical blocks/logs
                                      ├── reserve exposure projection
                                      ├── pinned account snapshots
                                      └── state undo journal
                                                    │
                                         HF candidate ranking
                                           ├── Redis sorted sets
                                           └── Aave reserve reads
                                                    │
                                           profit estimate
                                                    │
                                   liquidationCall eth_call + gas
                                                    │
                                        persisted dry-run decision
```

The adapter maps actual Aave `Supply`, `Withdraw`, `Borrow`, `Repay`, `LiquidationCall`, collateral flag, user eMode, and reserve-risk configuration events into internal `ProtocolDelta` variants. Configured Chainlink `AnswerUpdated` logs can accelerate a refresh; the service also polls the configured reserve assets through Aave's own Oracle on each new block, so feed proxy/aggregator event-address differences do not silently freeze prices. See the official [Aave Pool interface](https://github.com/aave-dao/aave-v3-origin/blob/main/src/contracts/interfaces/IPool.sol) and [Arbitrum deployment address book](https://github.com/aave-dao/aave-address-book/blob/main/src/AaveV3Arbitrum.sol).

Health factor and per-reserve balances are read from the configured Aave Pool/Data Provider at the indexed block. Event-derived balances only identify accounts and reserve pairs; they are not treated as exact live balances because interest accrues between events. The profitability calculation uses Aave reserve configuration, eMode collateral bonus when applicable, oracle prices, configured slippage/flash-loan/execution-risk assumptions, and estimated gas. It is a screening estimate, not a DEX quote or guarantee of realizable profit.

The Redis risk sets are read on every block: the lowest-health-factor accounts are refreshed up to `max_candidate_refresh_per_block` so debt interest can move an account across the liquidation threshold even without a new user event. Event-affected accounts are always refreshed in addition to this cap.

For bounded historical checks, `--end-block BLOCK` indexes through that canonical block, refreshes accounts at the range end, and exits without opening a `newHeads` subscription. This makes short archive/RPC checks reproducible while leaving the continuous observer path unchanged.

## Aave version handling

The deployment configuration records `chain_id`, Pool, protocol revision, and start block. Pin the revision for the selected block range and use a matching RPC/archive provider. v3.7 removed the old `priceOracleSentinel` checks from borrow and liquidation paths; this project does not include sentinel-based gating. Aave's Pool still validates liquidation eligibility during the actual read-only call. See the official [v3.7 sentinel removal note](https://github.com/aave-dao/aave-v3-origin/blob/main/docs/3.7/sentinel-removal.md).

`config/arbitrum.toml` uses Pool, PoolConfigurator, Oracle, and Data Provider addresses from Aave's address book. Its two feed entries are examples, not a complete reserve inventory. Add every reserve whose price can affect a borrower; incomplete asset coverage means oracle-driven refresh is incomplete. Feed addresses are only an event acceleration path and should be checked against the configured Aave Oracle.

## Canonical chain and reorgs

Each block, raw log, normalized delta, and state update is persisted with its block identity. On a head mismatch the service searches backwards for a common ancestor, rolls account/reserve/oracle projections back through the undo journal, rebuilds Redis, and replays the replacement canonical branch. Logs and state changes for each block share a PostgreSQL transaction.

```text
100 ─ 101 ─ 102A ─ 103A
          \
           102B ─ 103B ─ 104B
                        ↑
                    canonical
```

If the ancestor is earlier than the configured start block, indexing stops. The service does not claim to reconstruct users whose required history predates `start_block`; for a complete account set, begin at the Pool's deployment or provide a separately verified bootstrap.

## Run

1. Copy `.env.example` to `.env` and set a WebSocket RPC with historical logs and block-pinned `eth_call`, plus local database/cache credentials.
2. Review `config/arbitrum.toml`: protocol revision, start block, native asset, feed mappings, and risk assumptions.
3. Set `SIMULATION_CALLER` to the address that should be used as `from` for simulation. A caller without the necessary token balance/approval or executor behavior can make a valid candidate's call revert.
4. Start PostgreSQL and Redis with `docker compose up -d postgres redis`. Host ports default to `15432` and `16379` to avoid common local-service conflicts.
5. Start the continuous observer with:

```bash
cargo run --release --bin liquidation-engine -- --config config/arbitrum.toml --observe-only
```

The application runs SQLx migrations on startup. Redis is rebuildable and PostgreSQL is authoritative. The Docker image runs the same observe-only command. The continuous mode requires a WebSocket RPC that responds to `eth_subscribe newHeads`, supports historical logs for catch-up, and provides block-pinned `eth_call` for every indexed block.

To run a bounded Docker sync, use a config whose `start_block` is within the selected range:

```bash
docker compose run --rm --no-deps \
  --volume ./fixtures:/app/fixtures:ro \
  --entrypoint liquidation-engine searcher \
  --config fixtures/aave-arbitrum-window.toml \
  --end-block 511591511 --observe-only
```

## Deterministic replay

The `replay` binary applies normalized protocol deltas from newline-delimited JSON into an empty PostgreSQL database. Header-only rows preserve blocks with no protocol logs; rows can also carry account snapshots. Optional `raw_log` preserves the RPC record. The repository includes a captured five-block Arbitrum window and its expected projection hash:

```bash
cargo run --release --bin replay -- \
  --fixture fixtures/aave-arbitrum-window.ndjson \
  --config fixtures/aave-arbitrum-window.toml \
  --verify-final-state
```

Replay is intentionally strict: it requires an empty canonical-block database and contiguous parent linkage. Each run printed the same final state hash as the live indexer and matched the digest embedded in the fixture. To replay with the Compose profile, first create its dedicated database, then run:

```bash
docker compose exec -T postgres createdb -U searcher replay
docker compose --profile tools run --rm --no-deps replay \
  --fixture fixtures/aave-arbitrum-window.ndjson \
  --config fixtures/aave-arbitrum-window.toml \
  --verify-final-state
```

Use a fresh `replay` database for each import. `--state-hash-only` prints the current PostgreSQL projection digest without importing a fixture.

The Compose validation profile runs formatting, `check`, Clippy, and the full test suite, including the PostgreSQL rollback/replay integration test:

```bash
docker compose --profile tools run --rm --no-deps test
```

The reorg integration test creates a uniquely named schema, validates rollback/replay across competing branches, and drops the schema after success.

## Current limits

- One Arbitrum Aave deployment is configured. The Pool decoder is reusable, but each chain/deployment needs its own revision, pool/oracle/feed configuration and history window.
- Oracle-triggered candidate refresh only covers configured feed sources.
- The reserve exposure projection is for affected-user discovery. Pinned Aave view calls supply balances, collateral flags, risk parameters and health factors.
- Profitability uses configured friction assumptions and no live DEX quote. Flash-loan execution, multi-DEX routing, private bundles, gas auctions and signing are out of scope.
- A passing `eth_call`/gas estimate does not ensure inclusion or profit. It is still read-only and never submits a transaction.
- The captured window contained two `Withdraw` events and one account snapshot. Its account had no debt, so no liquidation candidate or successful liquidation simulation occurred.
- The public shared RPC used for the short fixture served recent block-pinned reads, but did not answer `eth_subscribe newHeads`; historical state roughly 700 blocks behind also returned unavailable. A provider with subscriptions and archive state is required to validate continuous operation and older eventful windows.
- Redis was cleared and rebuilt from the PostgreSQL snapshot; each HF ranking matched the database's zero accounts below its threshold. The reorg test exercised competing database branches; no natural mainnet reorg occurred during the short RPC window.

## Configuration

| Setting | Source | Purpose |
| --- | --- | --- |
| `RPC_WS_URL` | `.env` | `newHeads`, logs, and read-only RPC calls. |
| `DATABASE_URL`, `REDIS_URL` | `.env` | Canonical state and rebuildable health-factor indexes. |
| `SIMULATION_CALLER` | `.env` | `from` address only; no key is read. |
| `chain_id`, `pool`, `pool_configurator`, `oracle`, `protocol_data_provider` | TOML | Aave deployment contracts. |
| `protocol_revision`, `start_block` | TOML | ABI/revision boundary and history start. |
| `oracle_feeds` | TOML | Mapping from emitting feed to underlying reserve. |
| gas, slippage, flash-loan fee, risk, minimum profit | TOML | Screening model assumptions. |
| `max_candidate_refresh_per_block` | TOML | Cap for per-block refreshes from the at-risk Redis ranking. |

## Validation status

Validated with the Compose Rust 1.94.1 profile: `cargo fmt --check`, `cargo check --locked --all-targets --all-features`, `cargo clippy --locked --all-targets --all-features -- -D warnings`, and `cargo test --locked --all-targets --all-features` pass. Five tests passed, including the PostgreSQL reorg integration test. The live indexer processed blocks 511591507–511591511; replay twice produced `0x2a37b03d8e5898f52b31cd71fe614d3b76d722d36a16f2943f60148c48e92a8a`, matching the live PostgreSQL projection.
