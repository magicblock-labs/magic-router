# Local Testing Guide

Complete end-to-end test of the Magic Router with automatic validator setup and ephemeral validator registration.

## Prerequisites

- Rust 1.70+
- Solana CLI with `solana-test-validator` command
- `ephemeral-validator` binary (for validator registration testing)
- `nc` (netcat) for port checking
- `jq` for JSON output formatting (optional but recommended)

## Quick Start

Run the complete validator registration test:

```bash
./test/test-registration.sh
```

This single script:
1. ✓ Kills any existing validators for a clean state
2. ✓ Starts a local Solana test validator with WebSocket support
3. ✓ Clones all required programs from devnet (Magic Domain Program, etc.)
4. ✓ Starts an ephemeral validator on port 7799
5. ✓ Registers the ephemeral validator with the Magic Domain Program
6. ✓ Verifies the registration transaction and on-chain account
7. ✓ Starts the Magic Router
8. ✓ Tests route discovery via `getRoutes` endpoint (retries for 10 seconds)
9. ✓ Displays validator configuration
10. ✓ Cleans up and stops all services

## Test Flow

### 1. Solana Test Validator (Port 8899 RPC, 8900 WebSocket)

The script starts a `solana-test-validator` with:
- Local ledger in `./test/test-ledger/`
- Required program clones from devnet
- WebSocket support enabled (`--rpc-pubsub-max-connections 1000`)

### 2. Ephemeral Validator (Port 7799)

The script automatically starts an `ephemeral-validator` instance that:
- Connects to the test validator as its remote cluster
- Listens on `http://127.0.0.1:7799`
- Can be registered in the Magic Domain Program

### 3. Validator Registration

The test registers the ephemeral validator by:
- Running `local-validator-setup` binary
- Creating an ER record in the Magic Domain Program
- Fetching and deserializing the created account to verify

### 4. Router Testing

Once registered, the Magic Router:
- Subscribes to Magic Domain Program account changes via WebSocket
- Discovers the newly registered validator
- Serves the validator info via `getRoutes` endpoint

## Test Results

On success, you'll see:
```
✅ Registration transaction submitted
   Signature: <transaction-signature>

✅ ER Record account verified on-chain
   Account: <PDA>
   Lamports: 1600800

✅ TEST PASSED: Validator successfully registered

✓ Router is running

Attempt 1/10: No validators found yet, retrying...
...
✅ Validator found in getRoutes response!

Validator Configuration:
  Identity: mAGicPQYBMvcYveUZA5F5UNNwyHvfYh5xkLS2Fr1mev
  FQDN: http://127.0.0.1:7799
  Block time: 50ms
  Base fee: 0 lamports
  Country code: USA
  Status: Active
```

## Troubleshooting

### Validator Discovery Fails

The test shows:
```
❌ Validator not found in routes after 10 seconds

Debugging information:
  ✓ Test validator RPC (8899) is up
  ✓ Test validator WebSocket (8900) is up
  ✓ Ephemeral validator RPC (7799) is up
```

If any validator shows "DOWN", restart it:

```bash
# Kill all validators
pkill -f solana-test-validator
pkill -f ephemeral-validator

# Run test again
./test/test-registration.sh
```

### WebSocket Connection Errors

If you see `Connection rejected with status code: 405`:

The router expects WebSocket on port 8900 (RPC port + 1) for the test validator. The router automatically adds 1 to the RPC port for local validators (localhost/127.0.0.1).

### Ephemeral Validator Won't Start

Check if port 7799 is already in use:

```bash
lsof -i :7799
pkill -f ephemeral-validator
```

Then re-run the test.

### Registration Transaction Fails

The test continues even if the transaction fails initially (graceful degradation). It will:
- Log the error
- Fetch the account from the blockchain
- Display the account contents if it exists

This handles the case where the validator was already registered.

## Manual Testing

If you want to run components manually:

### Start Test Validator Only

```bash
solana-test-validator \
  --ledger ./test/test-ledger \
  --reset \
  --rpc-pubsub-max-connections 1000 \
  --clone-upgradeable-program DmnRGfyyftzacFb1XadYhWF6vWqXwtQk5tbr6XgR3BA1 \
  --url https://rpc.magicblock.app/devnet
```

### Start Ephemeral Validator

```bash
ephemeral-validator \
  --accounts-lifecycle ephemeral \
  --remote-cluster development \
  --remote-url http://localhost:8899 \
  --remote-ws-url ws://localhost:8900 \
  --rpc-port 7799
```

### Register Validator

```bash
cargo run -p magic-router-setup --release -- \
  --rpc-url http://localhost:8899 \
  --fqdn http://127.0.0.1:7799
```

### Start Router

```bash
RUST_LOG=info ./target/release/magicblock-rpc-router test/config.local-no-laser.toml
```

### Test getRoutes

```bash
curl -X POST http://127.0.0.1:8080/getRoutes \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "getRoutes"
  }'
```

## Configuration

Edit `test/config.local-no-laser.toml` to adjust:
- `listen-address` - Router's listening address (default: 127.0.0.1:8080)
- `base-chain-urls` - Test validator RPC URL (default: http://127.0.0.1:8899)
- `max-cached-delegations` - Cache size for delegation records
- `max-connections` - Max concurrent connections

## Files

- **test-registration.sh** - Complete validator registration test (recommended)
- **config.local-no-laser.toml** - Router configuration for local testing without Laser Stream

## How It Works

1. **Validator Detection**: Router connects to test validator RPC
2. **Program Subscription**: Router subscribes to Magic Domain Program via WebSocket
3. **Validator Discovery**: When ephemeral validator registers, router receives notification
4. **Route Caching**: Router caches validator info and serves via `getRoutes`

The test validates this entire flow end-to-end.
