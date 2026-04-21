//! Interface-compatible with <https://crates.io/crates/solana-pubsub-client>.

use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use solana_account_decoder_client_types::UiAccount;
use solana_sdk::{clock::Slot, pubkey::Pubkey, signature::Signature};

use super::{config::*, response::Response as RpcResponse, response::*};

#[derive(Debug, thiserror::Error)]
pub enum PubsubClientError {
	#[error("subscribe failed: {code} {message}")]
	SubscribeFailed {
		code: i32,
		message: String,
		data: Option<Value>,
	},
	#[error(transparent)]
	Io(#[from] std::io::Error),
}

pub type PubsubClientStream<T> = crate::subscription::SubscriptionStream<T>;

type SubscribeResult<T> = Result<PubsubClientStream<T>, PubsubClientError>;

// Each method is `name -> { method_str, params_expr, Stream<Item=T> }`.
macro_rules! subscribe_method {
	($name:ident($($arg:ident: $ty:ty),* $(,)?) -> $ret:ty, $method:literal, $params:expr) => {
		pub async fn $name(&self, $($arg: $ty),*) -> SubscribeResult<$ret> {
			self.subscribe($method, $params).await
		}
	};
}

#[derive(Default)]
pub struct PubsubClient {
	_priv: (),
}

impl PubsubClient {
	pub fn new() -> Self {
		Self::default()
	}

	async fn subscribe<T: DeserializeOwned>(
		&self,
		method: &str,
		params: Value,
	) -> SubscribeResult<T> {
		match crate::subscription::Subscription::new::<Value, Value>(method, params)? {
			Ok(sub) => Ok(sub.into_stream::<T>()),
			Err(crate::RpcError { code, message, data }) => {
				Err(PubsubClientError::SubscribeFailed { code, message, data })
			}
		}
	}

	subscribe_method!(
		account_subscribe(pubkey: &Pubkey, config: Option<RpcAccountInfoConfig>)
			-> RpcResponse<UiAccount>,
		"account",
		json!([pubkey.to_string(), config])
	);
	subscribe_method!(
		block_subscribe(filter: RpcBlockSubscribeFilter, config: Option<RpcBlockSubscribeConfig>)
			-> RpcResponse<RpcBlockUpdate>,
		"block",
		json!([filter, config])
	);
	subscribe_method!(
		logs_subscribe(filter: RpcTransactionLogsFilter, config: RpcTransactionLogsConfig)
			-> RpcResponse<RpcLogsResponse>,
		"logs",
		json!([filter, config])
	);
	subscribe_method!(
		program_subscribe(pubkey: &Pubkey, config: Option<RpcProgramAccountsConfig>)
			-> RpcResponse<RpcKeyedAccount>,
		"program",
		json!([pubkey.to_string(), config])
	);
	subscribe_method!(vote_subscribe() -> RpcVote, "vote", json!([]));
	subscribe_method!(root_subscribe() -> Slot, "root", json!([]));
	subscribe_method!(
		signature_subscribe(signature: &Signature, config: Option<RpcSignatureSubscribeConfig>)
			-> RpcResponse<RpcSignatureResult>,
		"signature",
		json!([signature.to_string(), config])
	);
	subscribe_method!(slot_subscribe() -> SlotInfo, "slot", json!([]));
	subscribe_method!(slot_updates_subscribe() -> SlotUpdate, "slotsUpdates", json!([]));
}
