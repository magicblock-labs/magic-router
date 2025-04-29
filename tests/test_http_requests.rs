use std::time::Duration;

use common::TestEnv;
use solana_pubkey::Pubkey;

mod common;
const SLEEP_MS: u64 = 100;

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
    tokio::time::sleep(Duration::from_millis(SLEEP_MS)).await;
    // delegate account to the ER, we span up earlier
    env.delegate_account(pubkey, er_identity).await;
    // wait a bit for router to sync with updated state
    tokio::time::sleep(Duration::from_millis(SLEEP_MS)).await;
    // chain account on main chain, updating its balance
    env.update_account_balance(pubkey, 42, true).await;

    // fetch account via router, this should route the request to ER
    println!("refetching gai from {er_identity}");
    let account_from_ephem = env
        .router_client
        .get_account(&pubkey)
        .await
        .expect("failed to get account info from ephemeral after delegation");
    assert_eq!(
        account_from_ephem, account_from_chain,
        "account on ER shouldn't have been affected by on chain change"
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
    tokio::time::sleep(Duration::from_millis(SLEEP_MS)).await;
    // delegate account to the ER, we span up earlier
    env.delegate_account(pubkey, er_identity).await;
    // wait a bit for router to sync with updated state
    tokio::time::sleep(Duration::from_millis(SLEEP_MS)).await;
    // chain account on main chain, updating its balance
    env.update_account_balance(pubkey, 42, true).await;

    // refetch account balance via router, this should route the request to ER
    println!("refetching gb from {er_identity}");
    let balance_from_ephem = env
        .router_client
        .get_balance(&pubkey)
        .await
        .expect("failed to get account info from ephemeral after delegation");
    assert_eq!(
        balance_from_chain, balance_from_ephem,
        "account balance on ER shouldn't have been affected by on chain change"
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
    tokio::time::sleep(Duration::from_millis(SLEEP_MS)).await;
    // delegate account to the ER, we span up earlier
    env.delegate_account(pubkey, er_identity).await;
    // wait a bit for router to sync with updated state
    tokio::time::sleep(Duration::from_millis(SLEEP_MS)).await;
    // chain account on main chain, updating its balance
    env.update_token_balance(pubkey, 42, true).await;

    // refetch token account balance via router, this should route the request to ER
    println!("refetching gtab from {er_identity}");
    let tokens_from_ephem = env
        .router_client
        .get_token_account_balance(&pubkey)
        .await
        .expect("failed to get account info from ephemeral after delegation");
    assert_eq!(
        tokens_from_chain, tokens_from_ephem,
        "token account balance on ER shouldn't have been affected by on chain change"
    );
}
