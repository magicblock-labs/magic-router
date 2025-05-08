# MagicBlock RPC Router

The MagicBlock RPC Router is an auxiliary service designed to provide dynamic
request routing capabilities within the MagicBlock Ephemeral Rollups (ER)
infrastructure. Acting as a single endpoint, it efficiently manages a subset of
JSON-RPC requests as outlined in the [Solana
specification](https://solana.com/docs/rpc).

## Overview

The dynamic routing mechanism works by ascertaining the delegation status of
any given Solana account. Each account can be in one of two delegation states:

1. **Undelegated**: The account's most current state resides on the main chain
   (mainnet, testnet, devnet).
2. **Delegated**: This state is accompanied by the identity of the ER to which
   the account is delegated.

Upon encountering a new account, the router retrieves its delegation state and
establishes a WebSocket subscription to monitor this status. Consequently, the
initial request for any new account incurs additional latency due to these
preliminary steps.

By keeping track of each account's delegation status, the router dynamically
routes requests: directing undelegated accounts to the main chain and delegated
accounts to their respective ER.

### ER Identity

An ER identity comprises the public key of the ER validator. To facilitate
routing, the router determines the fully qualified domain name (FQDN) of a
validator given its public key. This is achieved through an on-chain database
of ER nodes, which the router accesses to map identities to FQDNs. Updates to
this database are propagated to the router via a WebSocket subscription.

## Supported HTTP Requests

Given the ephemeral nature of rollups, not all requests from the Solana RPC
specification are supported. The router supports a select subset of these
requests:

1. [getAccountInfo](https://solana.com/docs/rpc/http/getaccountinfo)
2. [getMultipleAccounts](https://solana.com/docs/rpc/http/getmultipleaccounts)
3. [getBalance](https://solana.com/docs/rpc/http/getbalance)
4. [getTokenAccountBalance](https://solana.com/docs/rpc/http/gettokenaccountbalance)

## Supported WebSocket Subscriptions

In addition to HTTP requests, the router can dynamically manage WebSocket
subscriptions. Currently, only
[accountSubscribe](https://solana.com/docs/rpc/websocket/accountsubscribe) is
supported.

Subscriptions are managed by establishing one connection to the main chain and
an additional optional connection to the ER node if the account is delegated.
Updates from these upstream sources are seamlessly relayed to the client.
Should the delegation status change, the router transparently adjusts
subscription sources, ensuring a continuous stream of updates.

### Latency Considerations

The router's additional processing and bookkeeping introduce increased latency
for routed requests. To mitigate this, it is recommended to deploy the router
in proximity to the client, ideally on the same host or within the same data
center.

## Configuration

The router service is configured using a TOML file with the following options:

```toml
# Router Configuration

# Listening IP address and port for incoming connections.
listen-address = "127.0.0.1:8080"

# List of base URLs for chain nodes; employs round-robin load balancing.
base-chain-urls = ["https://api.devnet.solana.com"]

# Maximum delegation cache entries.
max-cached-delegations = 1000

# Maximum simultaneous connections the router can handle.
max-connections = 1024

# Maximum subscriptions allowed per connection.
max-subscriptions-per-connection = 1024

# Frequency of ping requests, performed to upstream nodes, to determine their proximity (latency wise)
proximity-ping-frequency-sec = 30

# WebSocket Configuration
[websocket]
# Ping interval for maintaining WebSocket connections, in seconds.
ping-interval-sec = 30

# Number of connections per upstream WebSocket server.
connections-per-upstream = 5
```

## Deployment

To deploy the router, compile the project in release mode:

```sh
cargo build --release
```

Then, execute the router with the specified configuration file:

```sh
magicblock-rpc-router config.toml
```

The service will initiate and begin accepting connections.

