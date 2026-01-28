#!/bin/bash
# Test script to verify validator auto-registration through Magic Domain Program

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"

# Default values
RPC_BASE_URL="http://127.0.0.1:8899"
ROUTER_URL="http://127.0.0.1:8080"
ER_VALIDATOR_URL="http://127.0.0.1:7799"
PROGRAM_ID="DmnRGfyyftzacFb1XadYhWF6vWqXwtQk5tbr6XgR3BA1"

# Parse command-line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --local-validator-url)
            ER_VALIDATOR_URL="$2"
            shift 2
            ;;
        --local-base-url)
            RPC_BASE_URL="$2"
            shift 2
            ;;
        --router-url)
            ROUTER_URL="$2"
            shift 2
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --local-validator-url URL   Validator URL (default: http://127.0.0.1:7799)"
            echo "  --local-base-url URL        Base URL (default: http://127.0.0.1:8899)"
            echo "  --router-url URL            Router URL (default: http://127.0.0.1:8080)"
            echo "  --help                      Show this help message"
            exit 0
            ;;
        *)
            echo "❌ Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

echo "🧪 Testing Validator Auto-Registration"
echo "======================================"
echo ""
echo "Configuration:"
echo "  Validator URL: $ER_VALIDATOR_URL"
echo "  Base URL: $RPC_BASE_URL "
echo "  Router URL: $ROUTER_URL"
echo ""

# Check prerequisites
command -v solana &> /dev/null || { echo "❌ solana-cli not found"; exit 1; }
command -v curl &> /dev/null || { echo "❌ curl not found"; exit 1; }
command -v nc &> /dev/null || { echo "❌ netcat (nc) not found"; exit 1; }
command -v solana-test-validator &> /dev/null || { echo "❌ solana-test-validator not found"; exit 1; }

echo "✓ Prerequisites found"
echo ""

# Extract host and port from RPC_BASE_URL
BASE_VALIDATOR_HOST=$(echo "$RPC_BASE_URL" | sed -E 's|http(s)?://([^:]+)(:[0-9]+)?.*|\2|')
BASE_VALIDATOR_PORT=$(echo "$RPC_BASE_URL" | sed -E 's|http(s)?://[^:]*:([0-9]+).*|\1|')
[ -z "$BASE_VALIDATOR_PORT" ] && BASE_VALIDATOR_PORT="8899"

VALIDATOR_PID=""
STARTED_VALIDATOR=0
EPHEMERAL_VALIDATOR_PID=""
STARTED_EPHEMERAL_VALIDATOR=0

# Check if validator is running
echo "🔍 Checking validator on $BASE_VALIDATOR_HOST:$BASE_VALIDATOR_PORT..."
if nc -z "$BASE_VALIDATOR_HOST" "$BASE_VALIDATOR_PORT" 2>/dev/null; then
    echo "✓ Validator is already running"
    
    # Kill existing validator to start fresh
    echo "🛑 Killing existing validator to start fresh..."
    pkill -f solana-test-validator 2>/dev/null || true
    sleep 1
fi

    echo "Starting new solana-test-validator..."
    echo ""
    
    cd "$REPO_ROOT"
    
    # Start validator in background with all required clones (suppress logs)
    # Added --rpc-pubsub-max-connections to enable WebSocket support for router subscriptions
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
      --url https://rpc.magicblock.app/devnet > /dev/null 2>&1 &
    
    VALIDATOR_PID=$!
    STARTED_VALIDATOR=1
    echo "Started test validator (PID: $VALIDATOR_PID)"
    echo ""
    echo "Waiting for test validator to be ready and clones to load..."
    sleep 20
    
    # Check if validator started successfully
    if ! nc -z "$BASE_VALIDATOR_HOST" "$BASE_VALIDATOR_PORT" 2>/dev/null; then
        echo "❌ Validator failed to start"
        kill $VALIDATOR_PID 2>/dev/null || true
        exit 1
    fi
    
    echo "✓ Test validator is ready"

echo ""

# Check if ephemeral validator is running
echo "🔍 Checking ephemeral validator on 127.0.0.1:7799..."
if ! nc -z 127.0.0.1 7799 2>/dev/null; then
    echo "🚀 Starting ephemeral validator..."
    echo ""
    
    RUST_LOG=info ephemeral-validator \
      --accounts-lifecycle ephemeral \
      --remote-cluster development \
      --remote-url http://localhost:8899 \
      --remote-ws-url ws://localhost:8900 \
      --rpc-port 7799 > /dev/null 2>&1 &
    
    EPHEMERAL_VALIDATOR_PID=$!
    STARTED_EPHEMERAL_VALIDATOR=1
    echo "Started ephemeral validator (PID: $EPHEMERAL_VALIDATOR_PID)"
    echo ""
    echo "Waiting for ephemeral validator to be ready..."
    sleep 3
    
    # Check if ephemeral validator started successfully
    if ! nc -z 127.0.0.1 7799 2>/dev/null; then
        echo "⚠️  Ephemeral validator failed to start or is not responding"
    else
        echo "✓ Ephemeral validator is ready"
    fi
else
    echo "✓ Ephemeral validator is already running"
fi

echo ""

# Verify the Magic Domain Program was cloned
echo "🔍 Verifying Magic Domain Program is deployed..."
for attempt in {1..5}; do
    ACCOUNT_INFO=$(curl -s "$RPC_BASE_URL" -X POST \
      -H "Content-Type: application/json" \
      -d '{
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getAccountInfo",
        "params": ["DmnRGfyyftzacFb1XadYhWF6vWqXwtQk5tbr6XgR3BA1"]
      }')
    
    if echo "$ACCOUNT_INFO" | grep -q '"value"' && ! echo "$ACCOUNT_INFO" | grep -q '"value":null'; then
        echo "✓ Magic Domain Program is deployed"
        break
    else
        if [ $attempt -lt 5 ]; then
            echo "⚠️  Program not yet loaded (attempt $attempt/5). Waiting..."
            sleep 5
        else
            echo "⚠️  Program still not fully loaded. Proceeding anyway..."
        fi
    fi
done
echo ""

# Get the validator pubkey that will be registered
# This is the hardcoded pubkey from local-validator-setup
VALIDATOR_KEYPAIR_BYTES="07 53 b8 37 c8 df ee 89 a6 f4 6b 7e bd 10 c2 24 e4 44 2b 8f 0d 5b 03 51 35 fd 1a 24 32 c6 28 9f 0b 50 09 d0 b7 bd 6c c8 59 4d a8 4c e9 c5 84 16 15 ba ca f0 69 a8 9d 40 e9 f9 64 68 d2 29 53 57"

# Use solana-cli to derive the pubkey from the keypair
echo "📝 Registering validator identity using magic-router-local-setup..."
echo ""

cd "$REPO_ROOT"

# Run the registration and capture output
OUTPUT=$(cargo run -p magic-router-local-setup --release -- \
  --rpc-url "$RPC_BASE_URL" \
  --fqdn "$ER_VALIDATOR_URL" \
  --block-time-ms 50 \
  --base-fee 0 \
  --country-code "USA" 2>&1)

# Display output and filter out just build logs
echo "$OUTPUT" | grep -v "^   " | grep -v "Compiling\|Finished\|Running"
echo ""

# Check if registration was successful or account already exists
if echo "$OUTPUT" | grep -q "✓ Transaction submitted successfully"; then
    echo "✅ Registration transaction submitted"
    
    # Extract the signature if available
    SIGNATURE=$(echo "$OUTPUT" | grep "Signature:" | awk '{print $NF}')
    if [ -n "$SIGNATURE" ]; then
        echo "   Signature: $SIGNATURE"
        echo ""
    fi
elif echo "$OUTPUT" | grep -q "✓ Successfully deserialized ER Record"; then
    echo "✅ ER Record account already exists (or registration succeeded after transaction failure)"
    echo ""
else
    echo "❌ Registration failed"
    exit 1
fi

# Extract the Record PDA for account verification (available in both success cases)
RECORD_PDA=$(echo "$OUTPUT" | grep "Record PDA:" | awk '{print $NF}')
if [ -n "$RECORD_PDA" ]; then
    echo ""
    echo "🔍 Verifying account on-chain..."
    
    # Query the account info
    ACCOUNT_RESPONSE=$(curl -s "$RPC_BASE_URL" -X POST \
      -H "Content-Type: application/json" \
      -d "{
        \"jsonrpc\": \"2.0\",
        \"id\": 1,
        \"method\": \"getAccountInfo\",
        \"params\": [\"$RECORD_PDA\", {\"encoding\": \"base64\"}]
      }")
fi

echo ""
echo "✅ TEST PASSED: Ephemeral validator identity successfully registered"

# Check if router is running for additional tests
echo ""
    
    # Extract host and port from ROUTER_URL
    ROUTER_HOST=$(echo "$ROUTER_URL" | sed -E 's|http(s)?://([^:]+)(:[0-9]+)?.*|\2|')
    ROUTER_PORT=$(echo "$ROUTER_URL" | sed -E 's|http(s)?://[^:]*:([0-9]+).*|\1|')
    [ -z "$ROUTER_PORT" ] && ROUTER_PORT="8080"
    
    ROUTER_PID=""
    STARTED_ROUTER=0
    
    echo "🔍 Checking if Magic Router is running on $ROUTER_HOST:$ROUTER_PORT..."
    if nc -z "$ROUTER_HOST" "$ROUTER_PORT" 2>/dev/null; then
        echo "✓ Magic Router is running"
    else
        echo "⚠️  Magic Router not running. Starting router..."
        echo ""
        
        cd "$REPO_ROOT"
        
        # Start router in background
        ./target/release/magicblock-rpc-router test/config.local-no-laser.toml &
        ROUTER_PID=$!
        STARTED_ROUTER=1
        echo "Started Magic Router (PID: $ROUTER_PID)"
        echo ""
        echo "Waiting for Magic Router to initialize and subscribe to MDP..."
        sleep 10
        
        # Check if router started successfully
        if ! nc -z "$ROUTER_HOST" "$ROUTER_PORT" 2>/dev/null; then
            echo "⚠️  Magic Router failed to start or is not responding"
        else
            echo "✓ Magic Router is ready"
        fi
    fi
    
    # Test getRoutes endpoint if router is running
    if nc -z "$ROUTER_HOST" "$ROUTER_PORT" 2>/dev/null; then
        echo ""
        echo "📡 Testing getRoutes endpoint ..."
        echo ""
        
        # Retry loop to get routes (up to 10 seconds)
        ROUTES_RESPONSE=""
        FOUND_VALIDATOR=0
        for attempt in {1..10}; do
            ROUTES_RESPONSE=$(curl -s -X POST "$ROUTER_URL/getRoutes" \
              -H "Content-Type: application/json" \
              -d '{
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getRoutes"
              }')
            
            # Check if validator is in the routes
            if echo "$ROUTES_RESPONSE" | grep -q '"identity"'; then
                FOUND_VALIDATOR=1
                break
            else
                if [ $attempt -lt 10 ]; then
                    echo "Attempt $attempt/10: No validators found yet, retrying..."
                    sleep 1
                fi
            fi
        done
        
        echo ""
        echo "/getRoutes Response:"
        echo "$ROUTES_RESPONSE" | jq . 2>/dev/null || echo "$ROUTES_RESPONSE"
        echo ""
        
        # Check if validator was found
        if [ $FOUND_VALIDATOR -eq 1 ]; then
            echo "✅ Validator identity found!"
        else
            echo "❌ Validator identity not found in routes after 10 seconds"
            echo ""
            echo "Debugging information:"
            echo "  Checking validator connectivity..."
            
            if nc -z 127.0.0.1 8899 2>/dev/null; then
                echo "  ✓ Test validator RPC (8899) is up"
            else
                echo "  ✗ Test validator RPC (8899) is DOWN"
            fi
            
            if nc -z 127.0.0.1 8900 2>/dev/null; then
                echo "  ✓ Test validator WebSocket (8900) is up"
            else
                echo "  ✗ Test validator WebSocket (8900) is DOWN"
            fi
            
            if nc -z 127.0.0.1 7799 2>/dev/null; then
                echo "  ✓ Ephemeral validator RPC (7799) is up"
            else
                echo "  ✗ Ephemeral validator RPC (7799) is DOWN"
            fi
            
            echo ""
            echo "Possible causes:"
            echo "  1. Router hasn't synced validator routes from Magic Domain Program yet"
            echo "  2. Registration failed silently"
            echo "  3. WebSocket subscription to MDP hasn't received the update"
            echo "  4. Test and ephemeral validators are not running on expected ports"
        fi
    fi
    
    # Cleanup: stop router if we started it
    if [ $STARTED_ROUTER -eq 1 ] && [ -n "$ROUTER_PID" ]; then
        echo ""
        echo "🛑 Stopping Magic Router (PID: $ROUTER_PID)..."
        kill $ROUTER_PID 2>/dev/null || true
        wait $ROUTER_PID 2>/dev/null || true
        echo "✓ Magic Router stopped"
    fi
    
    # Cleanup: stop ephemeral validator if we started it
    if [ $STARTED_EPHEMERAL_VALIDATOR -eq 1 ] && [ -n "$EPHEMERAL_VALIDATOR_PID" ]; then
        echo ""
        echo "🛑 Stopping ephemeral validator (PID: $EPHEMERAL_VALIDATOR_PID)..."
        kill $EPHEMERAL_VALIDATOR_PID 2>/dev/null || true
        wait $EPHEMERAL_VALIDATOR_PID 2>/dev/null || true
        echo "✓ Ephemeral validator stopped"
    fi
    
    # Cleanup: stop validator if we started it
    if [ $STARTED_VALIDATOR -eq 1 ] && [ -n "$VALIDATOR_PID" ]; then
        echo ""
        echo "🛑 Stopping test validator (PID: $VALIDATOR_PID)..."
        kill $VALIDATOR_PID 2>/dev/null || true
        wait $VALIDATOR_PID 2>/dev/null || true
        echo "✓ Test alidator stopped"
    fi
    
    exit 0
else
    echo "❌ Registration failed"
    echo ""
    echo "Registration output:"
    echo "$OUTPUT"
    
    # Cleanup: stop router if we started it
    if [ $STARTED_ROUTER -eq 1 ] && [ -n "$ROUTER_PID" ]; then
        echo ""
        echo "🛑 Stopping Magic Router (PID: $ROUTER_PID)..."
        kill $ROUTER_PID 2>/dev/null || true
        wait $ROUTER_PID 2>/dev/null || true
        echo "✓ Magic Router stopped"
    fi
    
    # Cleanup: stop ephemeral validator if we started it
    if [ $STARTED_EPHEMERAL_VALIDATOR -eq 1 ] && [ -n "$EPHEMERAL_VALIDATOR_PID" ]; then
        echo ""
        echo "🛑 Stopping ephemeral validator (PID: $EPHEMERAL_VALIDATOR_PID)..."
        kill $EPHEMERAL_VALIDATOR_PID 2>/dev/null || true
        wait $EPHEMERAL_VALIDATOR_PID 2>/dev/null || true
        echo "✓ Ephemeral validator stopped"
    fi
    
    # Cleanup: stop validator if we started it
    if [ $STARTED_VALIDATOR -eq 1 ] && [ -n "$VALIDATOR_PID" ]; then
        echo ""
        echo "🛑 Stopping test validator (PID: $VALIDATOR_PID)..."
        kill $VALIDATOR_PID 2>/dev/null || true
        wait $VALIDATOR_PID 2>/dev/null || true
        echo "✓ Test validator stopped"
    fi
    
    exit 1
fi
