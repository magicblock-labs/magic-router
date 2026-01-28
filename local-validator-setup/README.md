# Magic Router Local Setup

This directory contains a Rust utility to set up and test the Magic Router locally with a Solana validator registered in the Magic Domain Program.

## Prerequisites

- Rust 1.70+
- Solana CLI
- A running local Solana validator (or testnet/devnet)

## Setup

### 1. Build the Setup Utility

```bash
cargo build --release
```

### 2. Register Validator with Magic Domain Program

The validator keypair is pre-configured in the binary. To register it with the Magic Domain Program:

```bash
./target/release/register-validator
```

This will:

- Use the validator keypair provided
- Connect to the local Solana validator (RPC)
- Register it as an ER node in the Magic Domain Program
- Set the FQDN to `http://localhost:7799`
- Set block time to 50ms and base fee to 0 lamports
- Fetch and display the created ER Record account with deserialized data

With custom options:

```bash
./target/release/register-validator \
  --rpc-url http://localhost:8899 \
  --fqdn http://localhost:7799 \
  --block-time-ms 50 \
  --base-fee 0 \
  --country-code USA
```

### What Happens During Registration

1. **Connection Verification** - Checks RPC connection and Solana version
2. **Transaction Building** - Creates a registration transaction with the ER Record
3. **Account Derivation** - Derives the PDA (Program Derived Address) for the ER Record
4. **Transaction Submission** - Sends transaction to the validator
5. **Account Verification** - Fetches the created account from the blockchain
6. **Data Deserialization** - Deserializes and displays the ER Record data:
   - Identity (validator public key)
   - FQDN (endpoint address)
   - Block time
   - Base fee
   - Country code
   - Status (Active/Inactive)

### Error Handling

If the transaction fails (e.g., account already exists):

- The utility logs the error but continues
- It fetches the existing account and displays its content
- Confirms the ER Record is properly configured on-chain

## Environment Variables

- `SOLANA_RPC`: RPC URL for the Solana cluster (default: `http://localhost:8899`)
- `VALIDATOR_FQDN`: FQDN of the validator to register (default: `http://localhost:7799`)

Example:

```bash
SOLANA_RPC=https://api.devnet.solana.com VALIDATOR_FQDN=https://my-validator.com cargo run --release
```

## Command Line Options

```
Options:
  -r, --rpc-url <RPC_URL>
          Solana RPC URL [default: http://localhost:8899] [env: SOLANA_RPC]
  -f, --fqdn <FQDN>
          Validator FQDN [default: http://localhost:7799] [env: VALIDATOR_FQDN]
      --block-time-ms <BLOCK_TIME_MS>
          Block time in milliseconds [default: 50]
      --base-fee <BASE_FEE>
          Base fee in lamports [default: 0]
      --country-code <COUNTRY_CODE>
          Country code (3 chars) [default: USA]
  -h, --help
          Print help
```

## What Gets Registered

The utility registers the following validator record in the Magic Domain Program:

- **Identity**: Derived from the validator keypair
- **FQDN**: The endpoint address (default: `http://localhost:7799`)
- **Status**: Active
- **Block Time**: 50ms (configurable)
- **Base Fee**: 0 lamports (configurable)
- **Country Code**: USA (configurable)
- **Load Average**: 0
- **Features**: 0 (no special features)

## Output Example

```
🚀 Registering validator with Magic Domain Program...
RPC URL: http://localhost:8899
FQDN: http://localhost:7799
Block time: 400ms
Base fee: 5000 lamports

✓ Connected to validator
  Solana version: 2.1.8

Building validator registration transaction...
Latest blockhash: 7xK8Z...

Record PDA: 9vK2M...
Submitting registration transaction...
✓ Transaction submitted successfully
  Signature: 3mSyPKbK...

Fetching ER Record account...
✓ ER Record account found
  Account: 9vK2M...
  Lamports: 5000000
  Owner: DmnRGfyy...

✓ Successfully deserialized ER Record
✓ Validator setup completed
Validator configuration:
  Identity: 3fxZ7M...
  FQDN: http://localhost:7799
  Block time: 50ms
  Base fee: 0 lamports
  Country code: USA
  Status: Active
```

## Integration with Router

Once registered, the validator will be discoverable by the Magic Router:

```bash
# Get routes (should show your registered validator)
curl -X POST http://127.0.0.1:8080 \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "getRoutes"
  }'
```

## Troubleshooting

### Connection Failed

- Ensure the Solana validator is running on the specified RPC URL
- Check that the validator is fully synced
- Verify firewall settings allow connections to the RPC port

### Transaction Submission Failed

- Check that you have the correct RPC URL
- Ensure the Magic Domain Program is deployed on the chain
- Verify the validator account has sufficient lamports (>= 5000000)

### Account Not Found After Registration

- The account may not have been committed yet
- The transaction may have failed silently
- Check the transaction signature on the blockchain explorer
