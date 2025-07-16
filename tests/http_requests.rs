use std::time::Duration;

use common::{sleep, TestEnv};
use json::{JsonContainerTrait, JsonValueTrait};
use reqwest::header::CONTENT_TYPE;
use solana_hash::Hash;
use solana_keypair::Keypair;
use solana_pubkey::Pubkey;
use solana_rpc_client_api::config::RpcTransactionConfig;
use solana_signature::Signature;
use solana_system_transaction::transfer;
use solana_transaction::versioned::VersionedTransaction;
use solana_transaction_status_client_types::UiTransactionEncoding;
use url::Url;

mod common;

#[tokio::test]
async fn test_get_account_info() {
    let mut env = TestEnv::init().await;

    let owner = Pubkey::new_unique();
    let pubkey = Pubkey::new_unique();
    let er_identity = Pubkey::new_unique();

    // spin up new mock ephemeral
    env.add_route(er_identity).await;
    // add new account to main chain
    env.add_account(pubkey, owner);

    // request account from chain
    let account_from_chain = env
        .router_client
        .get_account(&pubkey)
        .await
        .expect("failed to get account info from chain");
    // wait a bit for router to subscribe to update
    sleep().await;
    // delegate account to the ER, we spun up earlier
    env.delegate_account(pubkey, owner, er_identity).await;
    // change the account on main chain, updating its balance
    env.update_account_balance(pubkey, 42, true).await;

    // fetch account via router, this should route the request to ER
    let account_from_ephem = env
        .router_client
        .get_account(&pubkey)
        .await
        .expect("failed to get account info from ephemeral after delegation");
    assert_eq!(
        account_from_ephem, account_from_chain,
        "account on ER shouldn't have been affected by on chain change"
    );
    env.update_account_balance(pubkey, 42, false).await;
    let account_from_ephem = env
        .router_client
        .get_account(&pubkey)
        .await
        .expect("failed to get account info from ephemeral after delegation");
    env.undelegate_account(pubkey).await;
    let account_from_chain = env
        .router_client
        .get_account(&pubkey)
        .await
        .expect("failed to get account info from ephemeral after delegation");
    assert_eq!(
        account_from_ephem, account_from_chain,
        "account on chain and ephem should be the same after undelegation"
    );
}

#[tokio::test]
async fn test_get_account_balance() {
    let mut env = TestEnv::init().await;

    let owner = Pubkey::new_unique();
    let pubkey = Pubkey::new_unique();
    let er_identity = Pubkey::new_unique();

    // spin up new mock ephemeral
    env.add_route(er_identity).await;
    // add new account to main chain
    env.add_account(pubkey, owner);

    // request account balance from chain
    let balance_from_chain = env
        .router_client
        .get_balance(&pubkey)
        .await
        .expect("failed to get account balance from chain");
    // wait a bit for router to subscribe to update
    sleep().await;
    // delegate account to the ER, we spun up earlier
    env.delegate_account(pubkey, owner, er_identity).await;
    // change the account on main chain, updating its balance
    env.update_account_balance(pubkey, 42, true).await;

    // refetch account balance via router, this should route the request to ER
    let balance_from_ephem = env
        .router_client
        .get_balance(&pubkey)
        .await
        .expect("failed to get account info from ephemeral after delegation");
    assert_eq!(
        balance_from_chain, balance_from_ephem,
        "account balance on ER shouldn't have been affected by on chain change"
    );
    env.update_account_balance(pubkey, 42, false).await;
    let balance_from_ephem = env
        .router_client
        .get_balance(&pubkey)
        .await
        .expect("failed to get account info from ephemeral after delegation");
    env.undelegate_account(pubkey).await;
    let balance_from_chain = env
        .router_client
        .get_balance(&pubkey)
        .await
        .expect("failed to get account info from ephemeral after delegation");
    assert_eq!(
        balance_from_chain, balance_from_ephem,
        "account balance on ER should be the same after undelegation"
    );
}

#[tokio::test]
async fn test_get_token_account_balance() {
    let mut env = TestEnv::init().await;

    let owner = Pubkey::new_unique();
    let pubkey = Pubkey::new_unique();
    let er_identity = Pubkey::new_unique();

    // spin up new mock ephemeral
    env.add_route(er_identity).await;
    // add new account to main chain
    env.add_account(pubkey, owner);

    // request token account balance from chain
    let tokens_from_chain = env
        .router_client
        .get_token_account_balance(&pubkey)
        .await
        .expect("failed to get account balance from chain");
    sleep().await;
    // delegate account to the ER, we spun up earlier
    env.delegate_account(pubkey, owner, er_identity).await;
    // change the account on main chain, updating its token balance
    env.update_token_balance(pubkey, 42, true).await;

    // refetch token account balance via router, this should route the request to ER
    let tokens_from_ephem = env
        .router_client
        .get_token_account_balance(&pubkey)
        .await
        .expect("failed to get account info from ephemeral after delegation");
    assert_eq!(
        tokens_from_chain, tokens_from_ephem,
        "token account balance on ER shouldn't have been affected by on chain change"
    );
    env.update_token_balance(pubkey, 42, false).await;
    let tokens_from_ephem = env
        .router_client
        .get_token_account_balance(&pubkey)
        .await
        .expect("failed to get account info from ephemeral after delegation");
    env.undelegate_account(pubkey).await;
    let tokens_from_chain = env
        .router_client
        .get_token_account_balance(&pubkey)
        .await
        .expect("failed to get account info from ephemeral after delegation");
    assert_eq!(
        tokens_from_chain, tokens_from_ephem,
        "token account balance shouldn't be the same after undelegation"
    );
}

#[tokio::test]
async fn test_get_multiple_accounts() {
    let mut env = TestEnv::init().await;

    let owner = Pubkey::new_unique();
    let pubkey1 = Pubkey::new_unique();
    let pubkey2 = Pubkey::new_unique();
    let er_identity = Pubkey::new_unique();

    // spin up new mock ephemeral
    env.add_route(er_identity).await;
    // add new account to main chain
    env.add_account(pubkey1, owner);
    env.add_account(pubkey2, owner);

    // request all accounts from chain
    let accounts_from_chain = env
        .router_client
        .get_multiple_accounts(&[pubkey1, pubkey2])
        .await
        .expect("failed to get multiple accounts from chain");
    assert!(
        accounts_from_chain.iter().all(Option::is_some),
        "all account should have been added to chain"
    );
    sleep().await;
    // delegate the first account to the ER, we spun up earlier
    env.delegate_account(pubkey1, owner, er_identity).await;
    // change the account on main chain, updating its balance
    env.update_token_balance(pubkey1, 42, true).await;

    // refetch accounts via router, this should fetch a union of results from chain and ER
    let accounts_from_ephem = env
        .router_client
        .get_multiple_accounts(&[pubkey1, pubkey2])
        .await
        .expect("failed to get account info from ephemeral after delegation");
    assert_eq!(
        accounts_from_chain, accounts_from_ephem,
        "both accounts should be in the same state as they were prior to delegation"
    );
    env.update_token_balance(pubkey1, 42, false).await;
    let accounts_from_ephem = env
        .router_client
        .get_multiple_accounts(&[pubkey1, pubkey2])
        .await
        .expect("failed to get account info from ephemeral after delegation");
    env.undelegate_account(pubkey1).await;
    // refetch accounts via router, this should fetch all accounts from chain again
    let accounts_from_chain = env
        .router_client
        .get_multiple_accounts(&[pubkey1, pubkey2])
        .await
        .expect("failed to get account info from ephemeral after delegation");
    assert_eq!(
        accounts_from_chain, accounts_from_ephem,
        "both accounts should have been fetched from chain and both states should be the same after undelegation"
    );
}

#[tokio::test]
async fn test_get_identity() {
    let mut env = TestEnv::init().await;

    let er_identity1 = Pubkey::new_unique();
    let er_identity2 = Pubkey::new_unique();

    // spin up 2 new mock ephemerals
    env.add_route(er_identity1).await;
    env.add_route(er_identity2).await;
    // give the router time to sync up with ephemerals
    tokio::time::sleep(Duration::from_secs(1)).await;

    let identity = env
        .router_client
        .get_identity()
        .await
        .expect("failed to get closest ER identity from router");
    assert!(
        identity.eq(&er_identity2) || identity.eq(&er_identity1),
        "identity should match on of the added routes"
    );
}

#[tokio::test]
async fn test_get_signature_statuses() {
    let env = TestEnv::init().await;

    let statuses = env
        .router_client
        .get_signature_statuses(&[Signature::default(); 2])
        .await
        .expect("failed to get signature statuses from router");
    assert!(statuses
        .value
        .into_iter()
        .all(|s| s.is_some_and(|s| s.status.is_ok())))
}

#[tokio::test]
async fn test_get_transaction() {
    let env = TestEnv::init().await;

    let result = env
        .router_client
        .get_transaction_with_config(
            &Signature::default(),
            RpcTransactionConfig {
                encoding: Some(UiTransactionEncoding::Base64),
                ..Default::default()
            },
        )
        .await;
    assert!(result.is_err());
    let txn = transfer(
        &Keypair::new(),
        &Pubkey::new_unique(),
        1000,
        Hash::default(),
    );
    let sig = txn.signatures[0];
    env.router_client
        .send_transaction(&txn)
        .await
        .expect("failed to send legacy transaction via router");
    let result = env
        .router_client
        .get_transaction_with_config(
            &sig,
            RpcTransactionConfig {
                encoding: Some(UiTransactionEncoding::Base64),
                ..Default::default()
            },
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_send_transaction() {
    let env = TestEnv::init().await;

    let txn = transfer(
        &Keypair::new(),
        &Pubkey::new_unique(),
        1000,
        Hash::default(),
    );
    let sig = txn.signatures[0];
    let result = env
        .router_client
        .send_transaction(&txn)
        .await
        .expect("failed to send legacy transaction via router");
    assert_eq!(
        sig, result,
        "signature mismatch in the sendTransaction result for legacy txn"
    );
    let result = env
        .router_client
        .send_transaction(&VersionedTransaction::from(txn))
        .await
        .expect("failed to send versioned transaction via router for legacy txn");
    assert_eq!(
        sig, result,
        "signature mismatch in the sendTransaction result for versioned txn"
    )
}

#[tokio::test]
async fn test_get_routes() {
    let mut env = TestEnv::init().await;
    let client = reqwest::Client::new();
    let er_identity1 = Pubkey::new_unique();
    let er_identity2 = Pubkey::new_unique();

    // spin up 2 new mock ephemerals
    env.add_route(er_identity1).await;
    env.add_route(er_identity2).await;
    // give the router time to sync up with ephemerals
    tokio::time::sleep(Duration::from_secs(1)).await;

    let response = client
        .post(env.router_client.url().parse::<Url>().unwrap())
        .header(CONTENT_TYPE, "application/json")
        .body(r#"{"jsonrpc":"2.0","id":1,"method":"getRoutes"}"#)
        .send()
        .await
        .expect("failed to send getRoutes request to the router");
    let response = response
        .text()
        .await
        .expect("recieved garbage response for getRoutes");
    let response = json::from_str::<json::Value>(&response);
    let array = response
        .get("result")
        .and_then(|r| r.as_array())
        .expect("getRoutes json contains invalid data");
    for el in array {
        let identity = el
            .get("identity")
            .and_then(|s| s.as_str())
            .expect("getRoutes response contains malformed route info");
        assert!(identity == er_identity1.to_string() || identity == er_identity2.to_string())
    }
}

#[tokio::test]
async fn test_get_blockhash_for_accounts() {
    let mut env = TestEnv::init().await;
    let client = reqwest::Client::new();
    let er_identity = Pubkey::new_unique();

    let owner = Pubkey::new_unique();
    let pubkey1 = Pubkey::new_unique();
    let pubkey2 = Pubkey::new_unique();
    // spin up a new mock ephemerals
    env.add_route(er_identity).await;
    // give the router time to sync up with ephemerals
    tokio::time::sleep(Duration::from_secs(1)).await;
    // add new account to main chain
    env.add_account(pubkey1, owner);
    env.add_account(pubkey2, owner);

    env.delegate_account(pubkey1, owner, er_identity).await;
    env.delegate_account(pubkey2, owner, er_identity).await;

    let response = client
        .post(env.router_client.url().parse::<Url>().unwrap())
        .header(CONTENT_TYPE, "application/json")
        .body(format!(r#"{{"jsonrpc":"2.0","id":1,"method":"getBlockhashForAccounts","params":[["{pubkey1}","{pubkey2}"]]}}"#))
        .send()
        .await
        .expect("failed to send getBlockhashForAccounts request to the router");
    let response = response
        .text()
        .await
        .expect("recieved garbage response for getBlockhashForAccounts");
    let response = json::from_str::<json::Value>(&response);
    response
        .get("result")
        .and_then(|r| r.as_object())
        .and_then(|r| r.get(&"blockhash"))
        .and_then(|h| h.as_str())
        .expect("getBlockhashForAccounts json contains invalid data, no hash was found");
}

#[ignore = "send_and_confirm_transaction uses blockhash related method which are not supported by the router"]
#[tokio::test]
async fn test_send_and_confirm_transaction() {
    let env = TestEnv::init().await;

    let txn = transfer(
        &Keypair::new(),
        &Pubkey::new_unique(),
        1000,
        Hash::default(),
    );
    let sig = txn.signatures[0];
    let result = env
        .router_client
        .send_and_confirm_transaction(&txn)
        .await
        .expect("failed to send legacy transaction via router");
    assert_eq!(
        sig, result,
        "signature mismatch in the sendTransaction result"
    );
}

#[tokio::test]
async fn test_mocked_methods() {
    let env = TestEnv::init().await;
    let result = env.router_client.get_first_available_block().await;
    assert!(result.is_ok(), "getFirstAvailableBlock method failed");
    let result = env.router_client.get_epoch_schedule().await;
    assert!(result.is_ok(), "getEpochSchedule method failed");
    let result = env.router_client.get_epoch_info().await;
    assert!(result.is_ok(), "getEpochInfo method failed");
}

#[tokio::test]
async fn test_get_delegation_status() {
    let mut env = TestEnv::init().await;
    let client = reqwest::Client::new();
    let er_identity = Pubkey::new_unique();

    let owner = Pubkey::new_unique();
    let pubkey = Pubkey::new_unique();
    // spin up a new mock ephemerals
    env.add_route(er_identity).await;
    // give the router time to sync up with ephemerals
    tokio::time::sleep(Duration::from_secs(1)).await;
    // add new account to main chain
    env.add_account(pubkey, owner);

    let response = client
        .post(env.router_client.url().parse::<Url>().unwrap())
        .header(CONTENT_TYPE, "application/json")
        .body(format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"getDelegationStatus","params":["{pubkey}"]}}"#
        ))
        .send()
        .await
        .expect("failed to send getDelegationStatus request to the router");
    let response = response
        .text()
        .await
        .expect("recieved garbage response for getDelegationStatus");

    let response = json::from_str::<json::Value>(&response);
    let is_delegated = response
        .get("result")
        .and_then(|r| r.as_object())
        .and_then(|r| r.get(&"isDelegated"))
        .and_then(|h| h.as_bool())
        .expect("getDelegationStatus json contains invalid data");
    assert!(!is_delegated, "account should not have been delegated");

    env.delegate_account(pubkey, owner, er_identity).await;

    let response = client
        .post(env.router_client.url().parse::<Url>().unwrap())
        .header(CONTENT_TYPE, "application/json")
        .body(format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"getDelegationStatus","params":["{pubkey}"]}}"#
        ))
        .send()
        .await
        .expect("failed to send getDelegationStatus request to the router");
    let response = response
        .text()
        .await
        .expect("recieved garbage response for getDelegationStatus");

    let response = json::from_str::<json::Value>(&response);
    let is_delegated = response
        .get("result")
        .and_then(|r| r.as_object())
        .and_then(|r| r.get(&"isDelegated"))
        .and_then(|h| h.as_bool())
        .expect("getDelegationStatus json contains invalid data");
    assert!(is_delegated, "account should have been delegated");

    let authority = response
        .get("result")
        .and_then(|r| r.as_object())
        .and_then(|r| r.get(&"delegationRecord"))
        .and_then(|h| h.as_object())
        .and_then(|r| r.get(&"authority"))
        .and_then(|a| a.as_str())
        .expect("getDelegationStatus json contains invalid data");
    assert_eq!(
        authority,
        er_identity.to_string(),
        "account should have been delegated to specified ER"
    )
}

#[tokio::test]
async fn test_get_latest_blockhash() {
    let mut env = TestEnv::init().await;
    let er_identity1 = Pubkey::new_unique();
    let er_identity2 = Pubkey::new_unique();

    // spin up 2 new mock ephemerals
    env.add_route(er_identity1).await;
    env.add_route(er_identity2).await;
    // give the router time to sync up with ephemerals
    tokio::time::sleep(Duration::from_secs(1)).await;

    let (_, slot) = env
        .router_client
        .get_latest_blockhash_with_commitment(Default::default())
        .await
        .expect("failed to fetch latest blockhash from the router");
    assert_eq!(slot, 300, "router didn't return mock slot");
}
