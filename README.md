# TRON Block Scanner → Redis Streams

A lightweight TRON block scanner that continuously polls the latest blocks, extracts:

- Native TRX transfers (`TransferContract`)
- TRC20 `transfer(address,uint256)` calls (USDT/USDC by default)

and pushes normalized transfer batches into Redis Streams. The scanner stores the last scanned block height locally so it can resume after restarts.

## Features

- TronWeb 6.x + ESM-only imports
- Redis Streams output (batch per block)
- Local state checkpoint (`lastBlockAll.json`)
- Optional TronGrid API key rotation via environment variables
- Contract allowlist for TRC20 (defaults to USDT + USDC on TRON mainnet)

## Requirements

- Node.js >= 18
- Redis (local or remote)

## Install

```bash
npm install
```

## Run

```bash
npm start
```

The process runs continuously until stopped.

## Output

The script writes to two Redis Stream keys:

- `stream:tron:trx` (TRX transfers)
- `stream:tron:trc20` (TRC20 transfers)

Each entry contains fields:

- `block`: the block height (string)
- `transactions`: a JSON string containing an array of transfer objects

### TRX transfer object

```json
{
  "tokenSymbol": "TRX",
  "tokenDecimals": 6,
  "from": "T...",
  "to": "T...",
  "amount": "1.2345",
  "amountBaseUnits": "1234500",
  "txId": "..."
}
```

### TRC20 transfer object

```json
{
  "tokenSymbol": "USDT",
  "tokenDecimals": 6,
  "contractAddress": "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t",
  "from": "T...",
  "to": "T...",
  "amount": "10",
  "amountBaseUnits": "10000000",
  "txId": "..."
}
```

## Configuration (Environment Variables)

All configuration is done through environment variables (recommended for GitHub).

### Redis

- `REDIS_URL`
  - Default: `redis://localhost:6379/0`
  - Example: `redis://:password@127.0.0.1:6379/0`

- `REDIS_STREAM_PREFIX`
  - Default: `stream:tron`
  - Controls stream keys:
    - `${REDIS_STREAM_PREFIX}:trx`
    - `${REDIS_STREAM_PREFIX}:trc20`

### TRON / TronGrid

- `TRON_FULL_HOST`
  - Default: `https://api.trongrid.io`

- `TRON_PRO_API_KEYS`
  - Optional, comma-separated
  - Used as the `TRON-PRO-API-KEY` header for TronGrid
  - Example:
    - `TRON_PRO_API_KEYS=key1,key2,key3`

### Scanner behavior

- `STATE_FILE_PATH`
  - Optional
  - Default: `./lastBlockAll.json` (next to the script)

- `POLL_INTERVAL_MS`
  - Default: `1000`

- `BLOCK_STEP_DELAY_MS`
  - Default: `200`

### Filters

- `MIN_TRX_AMOUNT`
  - Default: `0.01`
  - Meaning: minimum TRX transfer amount to include

- `MIN_TRC20_AMOUNT`
  - Default: `0.01`
  - Meaning: minimum TRC20 transfer amount to include (assumes 6 decimals by default)

- `TRC20_CONTRACTS`
  - Default: `TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t,TEkxiTehnzSmSe2XqrBj4w32RUN966rdz8`
  - Meaning: TRC20 contract allowlist (comma-separated)
  - Defaults correspond to:
    - USDT: `TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t`
    - USDC: `TEkxiTehnzSmSe2XqrBj4w32RUN966rdz8`

## Notes

- This scanner decodes TRC20 transfers by parsing the `TriggerSmartContract` input data for the `transfer()` method id (`a9059cbb`).
- If you want full token metadata support (dynamic decimals, symbols, etc.), you can extend the script to query token contracts, but the default is intentionally lightweight and fast.

