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
5. [getIdentity](https://solana.com/docs/rpc/http/getidentity), this method is
   used to return an identity of the ER validator which is closest to the
   router, latency wise.
6. [sendTransaction](https://solana.com/docs/rpc/http/sendtransaction) - the
   sent transaction is parsed to extract all the explicitly referenced
   writeable accounts (lookup tables are not supported). If any of those
   accounts is delegated then all of the delegated writeable accounts in the
   transaction should be delegated to the same ER, otherwise the method will
   return an error. The matched route for the transaction signature will linger
   in the router for a while, so, methods like `getSignatureStatuses` or
   `getTransaction` can be routed to the same upstream
7. [getSignatureStatuses](https://solana.com/docs/rpc/http/getsignaturestatuses) - only 
   makes sense for transactions which were recently sent through the router
8. [getTransaction](https://solana.com/docs/rpc/http/gettransaction) - only
   makes sense for transactions which were recently sent through the router
9. [getFirstAvailableBlock](https://solana.com/docs/rpc/http/getfirstavailableblock) - dummy 
   method used primarily for compatibility with solana explorer, the returned
   value should not be used for any decision making
10. [getEpochSchedule](https://solana.com/docs/rpc/http/getEpochSchedule) -
    dummy 
   method used primarily for compatibility with solana explorer, the returned
   value should not be used for any decision making
11. [getEpochInfo](https://solana.com/docs/rpc/http/getEpochInfo) - dummy
    method used primarily for compatibility with solana explorer, the returned
   value should not be used for any decision making
12. [getLatestBlockHash](https://solana.com/docs/rpc/http/getlatestblockhash) -
    a best effort method which returns the blockhash from the closest ER node.
    Note, if the network topology changes, the closest ER along with it, then
    it will cause transactions signed with the given blockhash to fail, as it
    was obtained from the wrong ER

### Custom Methods
There're also a few methods which are unique to the router, and as a result
they go beyond the solana JSON RPC API spec

1. **getRoutes** - a custom method to query all the ER nodes known to the
   router
2. **getBlockhashForAccounts** - another custom method to query the blockhash
   for the provided list of accounts. The list of accounts is usually delegated
   writeable accounts from the transaction that needs to be signed. If the
   accounts are delegated to different ER nodes, the method will return an
   error. If the accounts are not delegated the returned blockhash is from the
   base chain.
3. **getDelegationStatus** - custom method which retrieves the delegation
   status of the account along with the parsed delegation record (if
   applicable)

## API Documentation

### getRoutes

Returns information about all Ephemeral Rollup (ER) nodes known to the router.

**Example Request:**
```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "getRoutes"
}
```

**Example Response:**
```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": [
    {
      "identity": "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
      "fqdn": "er-node-1.magicblock.com",
      "baseFee": 5000,
      "blockTimeMs": 400,
      "countryCode": "US"
    },
    {
      "identity": "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM",
      "fqdn": "er-node-2.magicblock.com",
      "baseFee": 3000,
      "blockTimeMs": 350,
      "countryCode": "EU"
    }
  ]
}
```

**Response Fields:**
- `identity` (string): The public key of the ER validator
- `fqdn` (string): Fully qualified domain name of the ER node
- `baseFee` (number): Base fee in lamports for transactions on this ER
- `blockTimeMs` (number): Block time in milliseconds for this ER
- `countryCode` (string): ISO country code where the ER node is located

### getBlockhashForAccounts

Returns the latest blockhash for a list of accounts. This method is necessary in order to abstract away the splitting in the frontend.

**Example Request:**
```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "getBlockhashForAccounts",
  "params": [
    [
      "7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU",
      "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM"
    ]
  ]
}
```

**Example Response:**
```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "blockhash": "5KKsxSGdJYwBv3rcGZhdJzE3bqJj2vJxP2JxP2JxP2JxP",
    "lastValidBlockHeight": 123456789
  }
}
```

**Response Fields:**
- `blockhash` (string): The latest blockhash as a base58-encoded string
- `lastValidBlockHeight` (number): The block height at which this blockhash expires

**Behavior:**
- If all accounts are **undelegated**: Returns the blockhash from the base chain
- If all accounts are **delegated to the same ER**: Returns the blockhash from that ER
- If accounts are **delegated to different ERs**: Returns an error (conflicting delegations)
- If **no accounts are provided**: Returns the blockhash from the base chain

**Error Cases:**
- `ConflictingDelegations`: When accounts are delegated to different ER nodes
- `UnknownErNode`: When an ER node is not found in the routing table

### getDelegationStatus
Returns the delegation status along with ER record for the given account 

**Example Request:**
```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "getDelegationStatus",
  "params": ["7xKXtg2CW87d97TXJSDpbD5jBkheTqA83TZRuJosgAsU"]
}
```


**Example Response 1:**
```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "isDelegated": false
  }
}
```
**Example Response 2:**
```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "isDelegated": true,
    "delegationRecord": {
      "authority": "11111111111111111111111111111111",
      "owner": "3JnJ727jWEmPVU8qfXwtH63sCNDX7nMgsLbg8qy8aaPX",
      "delegationSlot": 388473478,
      "lamports": 15144960
    }
  }
}
```

**Response Fields:**
- `isDelegated` (bool): flag indicating whether the account is delegated 
- `delegationRecord` (object): the parsed delegation record for the delegated account (not present for non-delegated accounts)
    - `authority` (string): the ER identity (authority), which was used to delegated the account to
    - `owner` (string): the original owner of the delegated account
    - `delegationSlot` (number): base chain slot, at which the account has been delegated 
    - `lamports` (number): base chain balance of the account, recorded at the moment of delegation

**Error Cases:**
- `UnknownErNode`: When an ER node is not found in the routing table

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
# Maximum number of transaction to route mappings to keep in memory 
max-cached-transactions = 16384

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

