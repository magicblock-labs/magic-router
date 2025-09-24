use futures::StreamExt;
use router::accounts::DELEGATION_PROGRAM_STR;
use solana_hash::Hash;
use solana_keypair::Keypair;
use solana_pubkey::Pubkey;
use solana_system_transaction::transfer;

mod common;

#[tokio::test]
async fn test_account_subscribe() {
    let mut env = common::TestEnv::init().await;

    let owner = Pubkey::new_unique();
    let pubkey = Pubkey::new_unique();
    let er_identity = Pubkey::new_unique();

    // spin up new mock ephemeral
    env.add_route(er_identity).await;
    let pubsub = env.router_pubsub.clone();

    let (mut sub, _) = pubsub
        .account_subscribe(&pubkey, None)
        .await
        .expect("failed to subscribe to account via websocket");
    env.add_account(pubkey, owner);
    env.update_account_balance(pubkey, 42, true).await;
    let response = sub
        .next()
        .await
        .expect("websocket stream shouldn't be closed");
    assert_eq!(
        response.value.lamports, 42,
        "account balance should have been updated on main chain"
    );
    env.delegate_account(pubkey, owner, er_identity).await;
    let response = sub
        .next()
        .await
        .expect("websocket stream shouldn't be closed");
    assert_eq!(
        response.value.owner, DELEGATION_PROGRAM_STR,
        "account should have changed owner on chain"
    );
    env.update_account_balance(pubkey, 43, false).await;
    let response = sub
        .next()
        .await
        .expect("websocket stream shouldn't be closed");
    assert_eq!(
        response.value.owner,
        owner.to_string(),
        "account update should have been received from ER"
    );
    assert_eq!(
        response.value.lamports, 43,
        "account update should include latest balance change"
    );
    env.undelegate_account(pubkey).await;
    let response = sub
        .next()
        .await
        .expect("websocket stream shouldn't be closed");
    assert_eq!(
        response.value.lamports, 43,
        "account update from chain should include latest state from ER"
    );
    env.update_account_balance(pubkey, 44, false).await;
    let response = sub
        .next()
        .await
        .expect("websocket stream shouldn't be closed");
    assert_eq!(
        response.value.lamports, 44,
        "account update from chain should contain latest update"
    );
}

#[tokio::test]
async fn test_signature_subscribe() {
    let mut env = common::TestEnv::init().await;

    let owner = Pubkey::new_unique();
    let pubkey = Pubkey::new_unique();
    let er_identity = Pubkey::new_unique();

    // spin up new mock ephemeral
    env.add_route(er_identity).await;
    let pubsub = env.router_pubsub.clone();

    env.add_account(pubkey, owner);
    env.delegate_account(pubkey, owner, er_identity).await;

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
        .expect("failed to send transaction via router");
    let mut sub = pubsub
        .signature_subscribe(&sig, None)
        .await
        .expect("failed to subscribe to signature")
        .0;
    sub.next().await.expect("websocket stream has been closed");
}
