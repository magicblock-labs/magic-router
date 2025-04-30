use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use borsh::BorshSerialize;
use json::Serialize;
use jsonrpsee::{
    core::{async_trait, RpcResult, SubscriptionResult},
    proc_macros::rpc,
    server::{PingConfig, Server, ServerHandle},
    PendingSubscriptionSink, SubscriptionMessage, SubscriptionSink,
};
use mdp::state::{
    features::FeaturesSet, record::ErRecord, status::ErStatus, version::v0::RecordV0,
};
use router::{
    accounts::{DELEGATION_PROGRAM, DELEGATION_RECORD_DATA_SIZE},
    cache::delegations::delegation_record_pda,
    rpc::{http::RoHttpRpcServer, websocket::WebsocketRpcServer},
    types::SerdePubkey,
};
use scc::HashMap;
use solana_account::{Account, ReadableAccount, WritableAccount};
use solana_account_decoder::{
    encode_ui_account, parse_token::UiTokenAmount, UiAccount, UiAccountEncoding,
};
use solana_pubkey::Pubkey;
use solana_rpc_client_api::{
    config::{RpcAccountInfoConfig, RpcContextConfig, RpcProgramAccountsConfig},
    response::{Response, RpcResponseContext},
};

#[derive(Clone)]
pub struct MockServer {
    pub accounts: Arc<HashMap<Pubkey, Account>>,
    pub account_subscriptions: Arc<HashMap<Pubkey, SubscriptionSink>>,
    pub program_subscriptions: Arc<HashMap<Pubkey, SubscriptionSink>>,
    pub slot: Arc<AtomicU64>,
}

impl MockServer {
    pub async fn start(port: u16) -> (Self, ServerHandle) {
        let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
        let this = Self {
            accounts: Default::default(),
            account_subscriptions: Default::default(),
            program_subscriptions: Default::default(),
            slot: Default::default(),
        };
        let mut module = RoHttpRpcServer::into_rpc(this.clone());
        let _ = module.merge(WebsocketRpcServer::into_rpc(this.clone()));
        let _ = module.merge(TestHttpRpcServer::into_rpc(this.clone()));
        let ping = PingConfig::new().ping_interval(Duration::from_secs(10));

        let handle = Server::builder()
            .enable_ws_ping(ping)
            .build(addr)
            .await
            .unwrap()
            .start(module);
        (this, handle)
    }

    pub fn add_account(&self, pubkey: Pubkey, owner: Pubkey) {
        let _ = self
            .accounts
            .insert(pubkey, Account::new(23242400, 165, &owner));
    }

    pub fn add_existing_account(&self, pubkey: Pubkey, account: Account) {
        let _ = self.accounts.insert(pubkey, account);
    }

    pub async fn update_account_balance(&self, pubkey: &Pubkey, lamports: u64) {
        let Some(mut account) = self.accounts.get(pubkey) else {
            return;
        };
        account.set_lamports(lamports);
        self.notify_account(pubkey, account.get()).await;
    }

    pub async fn update_token_balance(&self, pubkey: &Pubkey, tokens: u64) {
        let Some(mut account) = self.accounts.get(pubkey) else {
            return;
        };
        account.data_as_mut_slice()[64..72].copy_from_slice(&tokens.to_le_bytes());
        self.notify_account(pubkey, account.get()).await;
    }

    pub async fn notify_account(&self, pubkey: &Pubkey, account: &Account) {
        let Some(sink) = self.account_subscriptions.get(pubkey) else {
            return;
        };
        let id = sink.subscription_id();
        let uiaccount = encode_ui_account(
            pubkey,
            account,
            UiAccountEncoding::Base64Zstd,
            None,
            Default::default(),
        );
        let msg =
            SubscriptionMessage::new("accountNotification", id, &self.response(uiaccount)).unwrap();
        if let Err(e) = sink.send(msg).await {
            eprintln!("Failed to send subscription message: {:?}", e);
        }
    }

    pub async fn add_route(&self, identity: Pubkey, port: u16) {
        let record = RecordV0 {
            identity,
            addr: format!("http://127.0.0.1:{port}"),
            block_time_ms: 50,
            country_code: b"AUS".into(),
            base_fee: 5000,
            load_average: 242,
            status: ErStatus::Active,
            features: FeaturesSet::default(),
        };

        let record = ErRecord::V0(record);
        let pda = record.pda().0;
        let mut buffer = Vec::with_capacity(std::mem::size_of::<ErRecord>() + record.addr().len());
        let _ = record.serialize(&mut buffer);
        let mut account = Account::new(24242342, buffer.len(), &mdp::id());
        account.data_as_mut_slice().copy_from_slice(&buffer);
        let owner = account.owner;
        if let Some(sink) = self.program_subscriptions.get(&owner) {
            let id = sink.subscription_id();
            let pubkey = SerdePubkey(pda);
            let account = encode_ui_account(
                &pda,
                &account,
                UiAccountEncoding::Base64Zstd,
                None,
                Default::default(),
            );
            let value = AccountWithPubkey { account, pubkey };
            let msg =
                SubscriptionMessage::new("programNotification", id, &self.response(value)).unwrap();
            if sink.get().send(msg).await.is_err() {
                self.program_subscriptions.remove(&owner);
            }
        }
        let _ = self.accounts.insert(pda, account);
    }

    pub async fn delegate_account(&self, acc: Pubkey, er_node: Pubkey) -> Account {
        let pda = delegation_record_pda(acc);
        let mut data = vec![0; DELEGATION_RECORD_DATA_SIZE];
        data[8..40].copy_from_slice(er_node.as_ref());
        let delegation_record = Account {
            lamports: 1559040,
            owner: DELEGATION_PROGRAM,
            data,
            executable: false,
            rent_epoch: u64::MAX,
        };
        self.notify_account(&pda, &delegation_record).await;
        let _ = self.accounts.insert(pda, delegation_record);

        let mut account = self.accounts.get(&acc).expect("account doesn't exist");
        let original_account = account.clone();
        account.set_owner(DELEGATION_PROGRAM);
        self.notify_account(&acc, account.get()).await;

        original_account
    }

    pub async fn undelegate_account(&self, pubkey: Pubkey, acc: Account) {
        let pda = delegation_record_pda(pubkey);
        self.accounts.remove(&pda);
        self.notify_account(&pda, &Account::default()).await;
        if let Some(mut account) = self.accounts.get(&pubkey) {
            *account = acc;
            self.notify_account(&pubkey, account.get()).await;
        }
    }

    pub fn account(&self, acc: &Pubkey) -> Option<Account> {
        self.accounts.get(acc).map(|a| a.get().clone())
    }

    fn response<T>(&self, value: T) -> Response<T> {
        Response {
            context: RpcResponseContext {
                slot: self.slot.load(Ordering::Relaxed),
                api_version: None,
            },
            value,
        }
    }
}

#[async_trait]
impl RoHttpRpcServer for MockServer {
    async fn account_info(
        &self,
        pubkey: SerdePubkey,
        params: Option<RpcAccountInfoConfig>,
    ) -> RpcResult<Response<Option<UiAccount>>> {
        let pubkey = pubkey.0;
        let config = params.unwrap_or_default();
        let encoding = config.encoding.unwrap_or(UiAccountEncoding::Base64Zstd);
        let data_slice_config = config.data_slice;
        let uiaccount = self
            .accounts
            .get(&pubkey)
            .map(|a| encode_ui_account(&pubkey, a.get(), encoding, None, data_slice_config));
        Ok(self.response(uiaccount))
    }

    async fn multiple_accounts(
        &self,
        pubkeys: Vec<SerdePubkey>,
        params: Option<RpcAccountInfoConfig>,
    ) -> RpcResult<Response<Vec<Option<UiAccount>>>> {
        let mut uiaccounts = Vec::with_capacity(pubkeys.len());
        let config = params.unwrap_or_default();
        let encoding = config.encoding.unwrap_or(UiAccountEncoding::Base64Zstd);
        let data_slice_config = config.data_slice;
        for pubkey in pubkeys {
            let pubkey = pubkey.0;
            let uiaccount = self
                .accounts
                .get(&pubkey)
                .map(|a| encode_ui_account(&pubkey, a.get(), encoding, None, data_slice_config));
            uiaccounts.push(uiaccount);
        }
        Ok(self.response(uiaccounts))
    }

    async fn balance(
        &self,
        pubkey: SerdePubkey,
        _params: Option<RpcContextConfig>,
    ) -> RpcResult<Response<u64>> {
        let lamports = self.accounts.get(&pubkey.0).map(|a| a.get().lamports);
        Ok(self.response(lamports.unwrap_or_default()))
    }

    async fn token_account_balance(
        &self,
        pubkey: SerdePubkey,
        _params: Option<RpcContextConfig>,
    ) -> RpcResult<Response<UiTokenAmount>> {
        let buffer = self
            .accounts
            .get(&pubkey.0)
            .and_then(|a| a.get().data.get(64..72).map(|v| v.to_vec()))
            .unwrap_or_default();
        let mut ui_amount = [0; 8];
        ui_amount.copy_from_slice(&buffer);
        let ui_amount = f64::from_le_bytes(ui_amount);
        let ui_amount = UiTokenAmount {
            ui_amount: Some(ui_amount),
            decimals: 9,
            amount: ui_amount.to_string(),
            ui_amount_string: ui_amount.to_string(),
        };
        Ok(self.response(ui_amount))
    }
}

#[async_trait]
impl WebsocketRpcServer for MockServer {
    async fn account_subscribe(
        &self,
        sink: PendingSubscriptionSink,
        pubkey: SerdePubkey,
        _params: Option<RpcAccountInfoConfig>,
    ) -> SubscriptionResult {
        let sink = sink.accept().await?;
        let _ = self.account_subscriptions.insert(pubkey.0, sink);
        Ok(())
    }
}

#[rpc(server)]
pub trait TestHttpRpc {
    #[method(name = "getProgramAccounts")]
    async fn get_program_accounts(
        &self,
        pubkey: SerdePubkey,
        params: Option<RpcProgramAccountsConfig>,
    ) -> RpcResult<Response<Vec<AccountWithPubkey>>>;

    #[subscription(name = "programSubscribe", unsubscribe = "programUnsubscribe", item = json::Value)]
    async fn program_subscribe(
        &self,
        pubkey: SerdePubkey,
        params: Option<RpcAccountInfoConfig>,
    ) -> SubscriptionResult;
}

#[async_trait]
impl TestHttpRpcServer for MockServer {
    async fn get_program_accounts(
        &self,
        pubkey: SerdePubkey,
        params: Option<RpcProgramAccountsConfig>,
    ) -> RpcResult<Response<Vec<AccountWithPubkey>>> {
        let mut accounts = Vec::new();
        let params = params.unwrap_or_default();
        let filters = params.filters.unwrap_or_default();
        let encoding = params
            .account_config
            .encoding
            .unwrap_or(UiAccountEncoding::Base64Zstd);
        self.accounts.scan(|k, v| {
            if v.owner != pubkey.0 {
                return;
            }
            for f in &filters {
                #[allow(deprecated)]
                if !f.allows(&v.to_account_shared_data()) {
                    return;
                }
            }
            let pubkey = SerdePubkey(*k);
            let account = encode_ui_account(k, v, encoding, None, Default::default());
            accounts.push(AccountWithPubkey { pubkey, account });
        });
        Ok(self.response(accounts))
    }

    async fn program_subscribe(
        &self,
        sink: PendingSubscriptionSink,
        pubkey: SerdePubkey,
        _params: Option<RpcAccountInfoConfig>,
    ) -> SubscriptionResult {
        let sink = sink.accept().await?;
        let _ = self.program_subscriptions.insert(pubkey.0, sink);
        Ok(())
    }
}

#[derive(Serialize, Clone)]
pub struct AccountWithPubkey {
    pubkey: SerdePubkey,
    account: UiAccount,
}
