# Local Testing Guide

Complete end-to-end test of the Magic Router with automatic validator setup.

## Prerequisites

- Rust 1.70+
- Solana CLI with `solana-test-validator` command
- `nc` (netcat) for port checking
- `jq` for JSON output formatting (optional but recommended)

## Run Complete Test

```bash
./test/run-test.sh
```

This single command:
1. ✓ Checks if validator is running, starts it if needed
2. ✓ Clones all required programs from devnet
3. ✓ Builds the router
4. ✓ Starts the router (works with or without Laser Stream)
5. ✓ Tests the getRoutes endpoint
6. ✓ Verifies validator auto-registration
7. ✓ Displays validator configuration
8. ✓ Cleans up and reports results

## Test Validator Registration

To specifically test validator registration without the full router test:

```bash
./test/test-registration.sh
```

This will:
1. Verify the Solana validator is running
2. Run the `local-validator-setup` binary
3. Confirm the registration transaction was submitted
4. Display the transaction signature

### Laser Stream (Optional for Real-time Updates)

The router works without Laser Stream, but delegation updates will be slower (RPC-only instead of real-time streaming).

To enable real-time delegation updates, get a Helius API key at [Helius](https://www.helius.dev/) and configure it:

```toml
[laser-stream]
endpoint = "https://laserstream-devnet-ewr.helius-rpc.com"
api-key = "your-actual-helius-api-key"
```

Expected output on success (without Laser Stream):
```
✓ Router is working!
Response: {"jsonrpc":"2.0","id":1,"result":[]}
```

Expected output on success (with Laser Stream and registered validator):
```
✅ TEST PASSED: Validator was auto-registered and is discoverable
```

## Manual Testing (Step by Step)

If you prefer to run each step separately:

```bash
# Terminal 1: Start validator
./test/setup.sh

# Terminal 2: Start router (works with or without Laser Stream)
./target/release/magicblock-rpc-router test/config.local.toml

# Terminal 3: Test the endpoint
./test/test-routes.sh
```

The router will work fine without Laser Stream configured. Delegation updates will just be slower (RPC-only).

## Files

- **run-test.sh** - Complete end-to-end test (recommended)
- **setup.sh** - Just setup and build (if you want to run router manually)
- **test-routes.sh** - Just test a running router's getRoutes endpoint
- **test-registration.sh** - Test validator registration specifically
- **config.local.toml** - Router configuration for localhost with Laser Stream config
- **config.local-no-laser.toml** - Router configuration for localhost without Laser Stream

## Detailed Setup Steps

### 1. Automatic Setup (Recommended)

```bash
./test/setup.sh
```

This handles:
- Checking if validator is running
- Starting validator with all clones if needed
- Building the router

### 2. Manual Setup (Alternative)

If you prefer manual control:

```bash
# Terminal 1: Start validator (if not already running)
solana-test-validator \
  --ledger ./test/test-ledger \
  --reset \
  --clone-upgradeable-program DmnRGfyyftzacFb1XadYhWF6vWqXwtQk5tbr6XgR3BA1 \
  --clone mAGicPQYBMvcYveUZA5F5UNNwyHvfYh5xkLS2Fr1mev \
  --clone EpJnX7ueXk7fKojBymqmVuCuwyhDQsYcLVL1XMsBbvDX \
  --clone 7JrkjmZPprHwtuvtuGTXp9hwfGYFAQLnLeFM52kqAgXg \
  --clone noopb9bkMVfRPU8AsbpTUg8AQkHtKwMYZiFUjNRtMmV \
  --clone-upgradeable-program DELeGGvXpWV2fqJUhqcF5ZSYMS4JTLjteaAMARRSaeSh \
  --clone Cuj97ggrhhidhbu39TijNVqE74xvKJ69gDervRUXAxGh \
  --clone 5hBR571xnXppuCPveTrctfTU7tJLSN94nq7kv7FRK5Tc \
  --clone F72HqCR8nwYsVyeVd38pgKkjXmXFzVAM8rjZZsXWbdE \
  --clone vrfkfM4uoisXZQPrFiS2brY4oMkU9EWjyvmvqaFd5AS \
  --clone-upgradeable-program Vrf1RNUjXmQGjmQrQLvJHs9SNkvDJEsRVFPkfSQUwGz \
  --clone-upgradeable-program BTWAqWNBmF2TboMh3fxMJfgR16xGHYD7Kgr2dPwbRPBi \
  --clone-upgradeable-program ACLseoPoyC3cBqoUtkbjZ4aDrkurZW86v19pXz2XQnp1 \
  --url https://api.devnet.solana.com

# Terminal 2: Build and run router
cargo build --release
./target/release/magicblock-rpc-router test/config.local.toml
```

### 3. Run the Router

Once the validator is running:

```bash
./target/release/magicblock-rpc-router test/config.local.toml
```

The router will:
- Detect `http://127.0.0.1:8899` as a local endpoint
- Automatically run `local-validator-setup` to register the validator
- Start accepting connections on `http://127.0.0.1:8080`

Expected output:
```
Local endpoints detected, attempting to auto-register validator...
✓ Local validator registered successfully
Listening for incoming connections on 127.0.0.1:8080
Router is ready and running!
```

## Testing the Router

Once the router is running, test it with:

### Using the Test Script

```bash
./test/test-routes.sh
```

This runs the `getRoutes` test and shows all registered ER nodes.

### Manual Testing

Test specific endpoints:

#### Get Routes

```bash
curl -X POST http://127.0.0.1:8080 \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "getRoutes"
  }'
```

Expected response (should include your auto-registered validator):
```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": [
    {
      "identity": "YOUR_VALIDATOR_PUBKEY",
      "fqdn": "http://localhost:8000",
      "baseFee": 5000,
      "blockTimeMs": 400,
      "countryCode": "USA"
    }
  ]
}
```

#### Get Account Info

```bash
curl -X POST http://127.0.0.1:8080 \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "getAccountInfo",
    "params": ["11111111111111111111111111111111"]
  }'
```

#### Get Delegation Status

```bash
curl -X POST http://127.0.0.1:8080 \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "getDelegationStatus",
    "params": ["11111111111111111111111111111111"]
  }'
```

## How the Registration Works

When the router starts with a local endpoint configured:

1. **Detection** - `src/local_setup.rs` detects localhost endpoint
2. **Binary Execution** - Spawns `local-validator-setup` binary
3. **Registration** - The binary:
   - Loads the validator keypair
   - Derives the PDA for the ER record
   - Creates an `ErRecord` with validator configuration
   - Builds and signs a transaction calling the Magic Domain Program's `Register` instruction
   - Submits the transaction and waits for confirmation
4. **Route Discovery** - Router queries the Magic Domain Program and discovers the registered validator
5. **Routing** - Router can now route requests to the registered validator

The registration happens automatically every time the router starts, updating the record if it already exists.

## Troubleshooting

### Setup Script Issues

#### "Connection refused" or port 8899 not responding

The validator may not have started. Check:
```bash
# Check if validator is running
lsof -i :8899
# or
netstat -an | grep 8899
```

If not running, the setup script will try to start it. If it fails, try manually:
```bash
solana-test-validator \
  --ledger ./test/test-ledger \
  --reset \
  --url https://api.devnet.solana.com
```

#### "netcat not found"

The setup script uses `nc` to check if validator is running. Install it:
```bash
# macOS
brew install netcat

# Ubuntu/Debian
sudo apt-get install netcat

# Or skip the check and manually start validator
solana-test-validator --ledger ./test/test-ledger --reset
```

### Router Issues

#### "Local validator registration failed" but router still running

The router continues even if auto-registration fails (graceful degradation). You can manually register:
```bash
cargo run -p magic-router-setup --release -- \
  --rpc-url http://127.0.0.1:8899 \
  --fqdn http://localhost:8000
```

#### Router can't find programs

The Magic Domain Program and other programs need to be cloned from devnet. The setup script does this automatically with the `--clone` flags. If running validator manually, include those flags.

#### Router won't start

Check the logs for specific errors. Common issues:
- Validator not running on port 8899
- Laser stream config invalid (for local testing, this can be disabled)
- Port 8080 already in use

## Stopping the Router

Press `Ctrl+C` to gracefully shutdown the router.

## Cleaning Up

Stop the Solana validator:
```bash
# The test validator runs in foreground, just Ctrl+C it
# This cleans up the test ledger
```

To reset the local chain completely:
```bash
rm -rf test-ledger/
```

## Configuration

Edit `test/config.local.toml` to adjust:
- `listen-address` - Router's listening address
- `base-chain-urls` - Base chain RPC URLs (should be localhost for testing)
- `max-cached-delegations` - Cache size for delegation records
- `max-connections` - Max concurrent connections
- `laser-stream` - Helius Laser Stream config (optional for local testing)

## How It Works

### Automatic Validator Registration

When the router detects a local endpoint (localhost or http://) in `config.local.toml`:

1. **Startup detection** - `src/local_setup.rs` checks if any `base-chain-urls` are local
2. **Auto-register** - Runs `local-validator-setup` binary to register the validator in the Magic Domain Program
3. **Discover routes** - Router queries the Magic Domain Program and finds the registered validator
4. **Route requests** - Router can now route to the validator

### Program Clones

The `setup.sh` script clones these programs from devnet to ensure all required programs are available:

- **Magic Domain Program** (DmnRGfyyftzacFb1XadYhWF6vWqXwtQk5tbr6XgR3BA1) - ER node registry
- **Delegation Program** (DELeGGvXpWV2fqJUhqcF5ZSYMS4JTLjteaAMARRSaeSh) - Account delegation tracking
- **VRF Program** (Vrf1RNUjXmQGjmQrQLvJHs9SNkvDJEsRVFPkfSQUwGz) - Verifiable randomness
- Various system programs and utilities

### Validator Registration Details

The auto-registered validator has these defaults:
- **Identity**: From hardcoded keypair in `local-validator-setup`
- **FQDN**: `http://localhost:8000`
- **Status**: Active
- **Block Time**: 400ms
- **Base Fee**: 5000 lamports
- **Country Code**: USA

To change these, edit `local-validator-setup/src/main.rs` or pass command line arguments.

## Notes

- The setup script checks if validator is running before starting (won't start duplicates)
- Each router restart auto-registers (updates the record if it already exists)
- Test ledger is in `./test/test-ledger/` (cleaned on validator reset)
- For production testing, use devnet or mainnet configs instead
- Laser stream config is optional for local testing (router continues without it)
