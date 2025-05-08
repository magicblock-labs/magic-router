use std::time::Duration;

use common::{sleep, TestEnv};
use solana_pubkey::Pubkey;

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
    env.delegate_account(pubkey, er_identity).await;
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
    env.delegate_account(pubkey, er_identity).await;
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
    env.delegate_account(pubkey, er_identity).await;
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
    env.delegate_account(pubkey1, er_identity).await;
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
    tokio::time::sleep(Duration::from_secs(2)).await;

    // refetch accounts via router, this should fetch a union of results from chain and ER
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
