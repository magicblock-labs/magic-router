use std::{
    net::SocketAddr,
    rc::Rc,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use base64::Engine;
use borsh::{BorshDeserialize, BorshSerialize};
use dlp::state::DelegationRecord;
use json::Serialize;
use jsonrpsee::{
    core::{async_trait, RpcResult, SubscriptionResult},
    proc_macros::rpc,
    server::{PingConfig, Server, ServerHandle},
    types::ErrorObjectOwned,
    PendingSubscriptionSink, SubscriptionMessage, SubscriptionSink,
};
use mdp::state::{
    features::FeaturesSet, record::ErRecord, status::ErStatus, version::v0::RecordV0,
};
use router::{
    cache::delegations::delegation_record_pda,
    error::RouterError,
    rpc::{
        http::{RoHttpRpcServer, RwHttpRpcServer},
        websocket::WebsocketRpcServer,
    },
    types::{DelegationStatus, ParsedDelegationRecord, RouteInfo, RpcIdentity, SerdePubkey},
};
use scc::HashMap;
use solana_account::{Account, ReadableAccount, WritableAccount};
use solana_account_decoder::{
    encode_ui_account, parse_token::UiTokenAmount, UiAccount, UiAccountEncoding,
};
use solana_commitment_config::CommitmentConfig;
use solana_hash::Hash;
use solana_pubkey::Pubkey;
use solana_rpc_client_api::{
    config::{
        RpcAccountInfoConfig, RpcContextConfig, RpcProgramAccountsConfig, RpcSendTransactionConfig,
        RpcTransactionConfig,
    },
    response::{Response, RpcBlockhash, RpcResponseContext},
};
use solana_transaction::{versioned::VersionedTransaction, Transaction};
use solana_transaction_status_client_types::{
    EncodedConfirmedTransactionWithStatusMeta, EncodedTransaction,
    EncodedTransactionWithStatusMeta, TransactionBinaryEncoding, TransactionConfirmationStatus,
    TransactionStatus, UiTransactionEncoding,
};

const TEST_LAMPORTS: u64 = 23242400;

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
        let _ = module.merge(RwHttpRpcServer::into_rpc(this.clone()));
        let ping = PingConfig::new().ping_interval(Duration::from_secs(10));

        let handle = Server::builder()
            .enable_ws_ping(ping)
            .build(addr)
            .await
            .unwrap()
            .start(module);
        (this, handle)
    }

    pub fn add_account(&self, pubkey: Pubkey, owner: Pubkey) -> Account {
        let account = Account::new(TEST_LAMPORTS, 165, &owner);
        let _ = self.accounts.insert(pubkey, account.clone());
        account
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
            tracing::error!("Failed to send subscription message: {:?}", e);
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

    pub async fn delegate_account(
        &self,
        pubkey: Pubkey,
        owner: Pubkey,
        er_node: Pubkey,
    ) -> Account {
        let pda = dlp::pda::delegation_record_pda_from_delegated_account(&pubkey);
        let delegation_record = DelegationRecord {
            authority: er_node,
            owner,
            lamports: TEST_LAMPORTS,
            delegation_slot: 42,
            commit_frequency_ms: 0,
        };
        let mut data = vec![0; DelegationRecord::size_with_discriminator()];
        delegation_record
            .to_bytes_with_discriminator(&mut data)
            .expect("failed to serialize the delegation record");
        data[8..40].copy_from_slice(er_node.as_ref());

        let delegation_record = Account {
            lamports: 1559040,
            owner: dlp::id(),
            data,
            executable: false,
            rent_epoch: u64::MAX,
        };
        self.notify_account(&pda, &delegation_record).await;
        let _ = self.accounts.insert(pda, delegation_record);

        let mut account = self
            .accounts
            .get(&pubkey)
            .expect("delegated account doesn't exist");
        let original_account = account.clone();
        account.set_owner(dlp::id());
        self.notify_account(&pubkey, account.get()).await;

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

    async fn identity(&self) -> RpcResult<RpcIdentity> {
        Ok(RpcIdentity {
            identity: SerdePubkey(Pubkey::default()),
            fqdn: "https://hello.world:4242".to_string(),
        })
    }

    async fn signature_statuses(
        &self,
        signatures: Vec<String>,
    ) -> RpcResult<Response<Vec<Option<TransactionStatus>>>> {
        Ok(Response {
            context: RpcResponseContext::new(0),
            value: vec![
                Some(TransactionStatus {
                    slot: 0,
                    status: Ok(()),
                    confirmations: None,
                    confirmation_status: Some(TransactionConfirmationStatus::Finalized),
                    err: None
                });
                signatures.len()
            ],
        })
    }

    async fn transaction(
        &self,
        _signature: String,
        _params: Option<RpcTransactionConfig>,
    ) -> RpcResult<Option<Rc<EncodedConfirmedTransactionWithStatusMeta>>> {
        let txn = EncodedConfirmedTransactionWithStatusMeta {
            slot: 0,
            transaction: EncodedTransactionWithStatusMeta {
                transaction: EncodedTransaction::Binary(
                    String::new(),
                    TransactionBinaryEncoding::Base64,
                ),
                meta: None,
                version: None,
            },
            block_time: None,
        };
        Ok(Some(Rc::new(txn)))
    }

    async fn is_blockhash_valid(
        &self,
        _hash: String,
        _params: Option<CommitmentConfig>,
    ) -> RpcResult<Response<bool>> {
        return Ok(Response {
            context: RpcResponseContext {
                slot: 0,
                api_version: None,
            },
            value: true,
        });
    }

    async fn routes(&self) -> RpcResult<Vec<RouteInfo>> {
        let mut routes = Vec::new();
        self.accounts
            .scan_async(|_, acc| {
                if acc.owner != mdp::id() {
                    return;
                }
                let Ok(record) = ErRecord::deserialize(&mut acc.data.as_ref()) else {
                    return;
                };
                routes.push(RouteInfo::from(&record))
            })
            .await;
        Ok(routes)
    }

    async fn blockhash_for_accounts(&self, _: Vec<SerdePubkey>) -> RpcResult<RpcBlockhash> {
        let response = RpcBlockhash {
            blockhash: Hash::new_unique().to_string(),
            last_valid_block_height: 0,
        };
        Ok(response)
    }

    async fn delegation_status(&self, pubkey: SerdePubkey) -> RpcResult<DelegationStatus> {
        let pda = dlp::pda::delegation_record_pda_from_delegated_account(&pubkey.0);
        let record = self.account(&pda);
        let record = record
            .and_then(|r| {
                DelegationRecord::try_from_bytes_with_discriminator(&r.data)
                    .ok()
                    .copied()
            })
            .map(|r| ParsedDelegationRecord {
                authority: SerdePubkey(r.authority),
                owner: SerdePubkey(r.owner),
                lamports: r.lamports,
                delegation_slot: r.delegation_slot,
            });
        let status = DelegationStatus {
            is_delegated: record.is_some(),
            delegation_record: record,
        };
        Ok(status)
    }

    async fn latest_blockhash(&self) -> RpcResult<Response<RpcBlockhash>> {
        let value = RpcBlockhash {
            blockhash: Hash::new_unique().to_string(),
            last_valid_block_height: 150,
        };
        Ok(Response {
            context: RpcResponseContext::new(0),
            value,
        })
    }
}

#[async_trait]
impl RwHttpRpcServer for MockServer {
    async fn send_transaction(
        &self,
        txn: String,
        params: Option<RpcSendTransactionConfig>,
    ) -> RpcResult<String> {
        let params = params.unwrap_or_default();
        let encoding = params.encoding.unwrap_or(UiTransactionEncoding::Base58);
        let txn = match encoding {
            UiTransactionEncoding::Base58 | UiTransactionEncoding::Binary => bs58::decode(&txn)
                .into_vec()
                .map_err(RouterError::decode_error)?,
            UiTransactionEncoding::Base64 => base64::prelude::BASE64_STANDARD
                .decode(txn)
                .map_err(RouterError::decode_error)?,
            other => {
                return Err(ErrorObjectOwned::owned(
                    1,
                    format!("{other} transaction encoding is not supported"),
                    None::<()>,
                ))
            }
        };
        let txn = if let Ok(txn) = bincode::deserialize::<VersionedTransaction>(&txn) {
            txn
        } else {
            bincode::deserialize::<Transaction>(&txn)
                .map(VersionedTransaction::from)
                .map_err(RouterError::decode_error)?
        };
        Ok(txn.signatures[0].to_string())
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
