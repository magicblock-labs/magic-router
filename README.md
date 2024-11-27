# Magic-Router
This is a utility service to be deployed in front of ephemeral rollup, which then can be used to route client requests between base layer blockhcain and ephemeral rollup 

## Account Delegation Status
The routing logic primarily relies on the account's delegation status. The router maintains a lookup table for each encountered account, categorizing it as "delegated" or "to be delegated". These lookup tables are synchronized with the base layer with minimal latency.

## Routing algorithm
1. Accept incoming JSON-RPC request
2. Check if the request is supported
3. Extract request metadata: - Referenced public keys
4. If the request is `sendTransaction` and contains delegation instructions, subscribe and confirm WebSocket (WS) subscription for each account to be delegated
5. Check the status of each account referenced in the request
6. Based on the delegation status, route the request to the appropriate endpoint (chain or ER)
7. If applicable, verify that the owner of the account in the response corresponds to the expected value, i.e., it should not be equal to the Delegation program, regardless of the response source
8. If the verification in step 8 fails, reroute the request to the correct endpoint. If the account is not found on the ER, route it to the base layer, and if the owner of the account is the Delegation program, resend the request to the ER

## Implementation details of routing algorithm
For redundancy, the router maintains several connection pools (both for HTTP and WebSocket), and load balances between them:
- For HTTP requests, the `reqwest` library (based on `hyper`) is used, which manages its own connection pool and load balances between them
- It is possible to provide several different endpoints for establishing connections, so that multiple connection pools will be maintained for each, and the router will load balance between them using a weighted uniform distribution
- In case of a request failure due to an I/O error, a configurable retry policy can be employed
- For WebSocket connections, similar logic applies, except the pool is maintained at the router level (not by a library)
- If a WebSocket connection fails, it marks all the accounts for which it maintains subscriptions as in a "limbo" state, indicating that their status may be stale. After that, it tries to reestablish the connection and recreate all subscriptions, followed by refetching all those accounts to get their current state
- Account status records marked as potentially stale trigger the router to fetch them from the mainnet and follow steps 7-9 in the higher-level routing for final state resolution

## Latency Considerations
Regular cases when states are fetched from expected sources should have relatively low latency. However, edge cases that can arise from race conditions or indeterminate account statuses due to WebSocket connection failure can introduce additional latency due to multiple requests being performed. This is a compromise that sacrifices latency for consistency.
