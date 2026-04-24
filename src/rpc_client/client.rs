//! This RPC Client is based on and should be interface compatible with <https://crates.io/crates/solana-rpc-client>.
//!
//! Every public method on [`RpcClient`] is a thin wrapper over a single Solana JSON-RPC call that
//! is tunneled through the host via `crate::call_rpc` (the `call-rpc` WIT import) rather than
//! opening sockets itself — that's why this crate has no HTTP dependency and why `send` is the
//! sole choke point for error translation.
//!
//! Method naming follows upstream conventions:
//!
//! * `foo()` — convenience wrapper that uses the client's default commitment.
//! * `foo_with_commitment(c)` — same call, caller picks the commitment level.
//! * `foo_with_config(cfg)` — full control, caller provides the whole config struct.
//!
//! Prefer the shortest form that satisfies your requirements; every extra knob is a promise you
//! have to keep.

use std::{
	str::FromStr,
	time::{Duration, Instant},
};

use base64::{Engine, prelude::BASE64_STANDARD};
use log::{debug, info, trace};
use serde_json::{Value, json};
use solana_account_decoder_client_types::{
	UiAccount, UiAccountData, UiAccountEncoding,
	token::{TokenAccountType, UiTokenAccount, UiTokenAmount},
};
use solana_commitment_config::CommitmentConfig;
use solana_sdk::{
	account::Account,
	bs58,
	clock::{DEFAULT_MS_PER_SLOT, Epoch, Slot, UnixTimestamp},
	epoch_info::EpochInfo,
	epoch_schedule::EpochSchedule,
	hash::Hash,
	pubkey::Pubkey,
	signature::Signature,
	transaction::Result as TransactionResult,
};
use solana_transaction_status_client_types::{
	EncodedConfirmedBlock, EncodedConfirmedTransactionWithStatusMeta, TransactionStatus,
	UiConfirmedBlock, UiTransactionEncoding,
};

// Mirrors `solana_vote_interface::state::MAX_LOCKOUT_HISTORY`.
const MAX_LOCKOUT_HISTORY: usize = 31;

use super::*;

// Blocks the executor thread. `async` is kept for upstream API parity; replace
// with a cooperative host sleep when the WIT exposes one.
async fn sleep(dur: Duration) {
	std::thread::sleep(dur);
}

// Generates the `foo() / foo_with_commitment(c)` pair.
//
// Doc comments are forwarded per-method via `$(#[$attr:meta])*` — Rust desugars `///` into
// `#[doc = "..."]`, which satisfies the `:meta` fragment matcher. That lets each generated
// function carry its own rustdoc without giving up the macro's DRY benefit.
macro_rules! trio_commitment {
	(
		$(#[$a_attr:meta])*
		$name:ident,
		$(#[$b_attr:meta])*
		$name_with_c:ident,
		$req:ident -> $ret:ty
	) => {
		$(#[$a_attr])*
		pub async fn $name(&self) -> $ret {
			self.$name_with_c(self.commitment()).await
		}
		$(#[$b_attr])*
		pub async fn $name_with_c(&self, commitment_config: CommitmentConfig) -> $ret {
			self.send(RpcRequest::$req, json!([commitment_config]))
				.await
		}
	};
}

// Nullary send — RPC calls that take no parameters.
macro_rules! nullary {
	(
		$(#[$attr:meta])*
		$name:ident, $req:ident -> $ret:ty
	) => {
		$(#[$attr])*
		pub async fn $name(&self) -> $ret {
			self.send(RpcRequest::$req, Value::Null).await
		}
	};
}

// Generates the `foo(pk) / foo_with_commitment(pk, c).value` pair. The non-`_with_commitment`
// form unwraps the `Response { context, value }` envelope so callers that only care about the
// payload don't have to pattern-match on every call site.
macro_rules! trio_pubkey_value {
	(
		$(#[$a_attr:meta])*
		$name:ident,
		$(#[$b_attr:meta])*
		$name_with_c:ident,
		$req:ident -> $ret:ty
	) => {
		$(#[$a_attr])*
		pub async fn $name(&self, pubkey: &Pubkey) -> ClientResult<$ret> {
			Ok(self.$name_with_c(pubkey, self.commitment()).await?.value)
		}
		$(#[$b_attr])*
		pub async fn $name_with_c(
			&self,
			pubkey: &Pubkey,
			commitment_config: CommitmentConfig,
		) -> RpcResult<$ret> {
			self.send(
				RpcRequest::$req,
				json!([pubkey.to_string(), commitment_config]),
			)
			.await
		}
	};
}

/// Configuration bag for [`RpcClient`].
///
/// The only field most callers need is `commitment_config`; the timeout field is reserved for
/// upstream parity and is not currently consulted by the host tunnel.
#[derive(Default)]
pub struct RpcClientConfig {
	pub commitment_config: CommitmentConfig,
	pub confirm_transaction_initial_timeout: Option<Duration>,
}
impl RpcClientConfig {
	/// Build a config that defaults everything except the commitment level.
	///
	/// Use when you want a one-liner for the common case: pick a commitment, accept all other
	/// defaults. Prefer [`RpcClient::new_with_commitment`] if you don't need the config struct
	/// separately.
	pub fn with_commitment(commitment_config: CommitmentConfig) -> Self {
		RpcClientConfig {
			commitment_config,
			..Self::default()
		}
	}
}

/// Solana JSON-RPC client. All methods are `async`; each one issues exactly one RPC request
/// (with a few documented exceptions that poll in a loop).
///
/// Cheap to construct — holds only the default commitment config. Clone-style reuse is not
/// needed; construct freely.
pub struct RpcClient {
	config: RpcClientConfig,
}
impl Default for RpcClient {
	fn default() -> Self {
		Self::new()
	}
}
impl RpcClient {
	/// Construct a client with all-default settings (commitment = `Finalized`).
	///
	/// Use for read-only procedures where you don't care which commitment level is used and want
	/// the most conservative default.
	pub fn new() -> Self {
		Self::new_with_commitment(CommitmentConfig::default())
	}

	/// Construct a client that uses `commitment_config` as the default for every call that
	/// doesn't take an explicit commitment argument.
	///
	/// Use when most of your calls want the same commitment (e.g. `Confirmed` for latency-sensitive
	/// UI, `Finalized` for settlement-grade reads) and you'd rather not pass it on every method.
	pub fn new_with_commitment(commitment_config: CommitmentConfig) -> Self {
		Self {
			config: RpcClientConfig::with_commitment(commitment_config),
		}
	}

	/// Construct a client from a fully-specified [`RpcClientConfig`].
	///
	/// Use when you've built the config elsewhere (e.g. parsed from a settings file) and want to
	/// hand it over verbatim. For the common case prefer [`new_with_commitment`].
	pub fn new_with_config(config: RpcClientConfig) -> Self {
		Self { config }
	}

	/// The default commitment level this client was constructed with.
	///
	/// Use when building nested configs (e.g. [`RpcAccountInfoConfig`]) and you want to inherit
	/// the client's default instead of hard-coding a commitment level.
	pub fn commitment(&self) -> CommitmentConfig {
		self.config.commitment_config
	}

	/// Submit a signed transaction and poll until a success or error status is observed.
	///
	/// **When**: "send coin", "execute swap", or any user-facing flow where you need a yes/no
	/// answer before continuing. Prefer [`send_transaction`](Self::send_transaction) for
	/// fire-and-forget and confirm later yourself.
	///
	/// **Why**: wraps `sendTransaction` + `getSignatureStatuses` polling + blockhash-validity
	/// checks (for durable-nonce vs recent-blockhash transactions) so callers don't have to
	/// reimplement the expiration-aware retry loop. Polls every 500ms up to ~60s; returns
	/// `ForUser("unable to confirm transaction...")` if the blockhash expires before a status
	/// materializes.
	pub async fn send_and_confirm_transaction(
		&self,
		transaction: &impl SerializableTransaction,
	) -> ClientResult<Signature> {
		const SEND_RETRIES: usize = 1;
		const GET_STATUS_RETRIES: usize = 120; // 60s at 500ms/poll; POLL_BUDGET is the hard backstop

		'sending: for _ in 0..SEND_RETRIES {
			let signature = self.send_transaction(transaction).await?;

			let recent_blockhash = if transaction.uses_durable_nonce() {
				let (recent_blockhash, ..) = self
					.get_latest_blockhash_with_commitment(CommitmentConfig::processed())
					.await?;
				recent_blockhash
			} else {
				*transaction.get_recent_blockhash()
			};

			for status_retry in 0..GET_STATUS_RETRIES {
				match self.get_signature_status(&signature).await? {
					Some(Ok(_)) => return Ok(signature),
					Some(Err(e)) => return Err(e.into()),
					None => {
						if !self
							.is_blockhash_valid(&recent_blockhash, CommitmentConfig::processed())
							.await?
						{
							// Block hash is not found by some reason
							break 'sending;
						} else if cfg!(not(test))
							// Ignore sleep at last step.
							&& status_retry < GET_STATUS_RETRIES
						{
							// Retry twice a second
							sleep(Duration::from_millis(500)).await;
							continue;
						}
					}
				}
			}
		}

		Err(RpcError::ForUser(
			"unable to confirm transaction. \
			 This can happen in situations such as transaction expiration \
			 and insufficient fee-payer funds"
				.to_string(),
		)
		.into())
	}

	/// Submit a signed transaction and return its signature as soon as the RPC node accepts it.
	///
	/// **When**: you plan to track confirmation yourself (e.g. via WebSocket subscriptions or a
	/// dedicated poller), or you're fan-out submitting many transactions and don't want each one
	/// to block the others.
	///
	/// **Why**: fire-and-forget submission with the default preflight commitment. Does NOT wait
	/// for the transaction to be confirmed on-chain — callers are responsible for that. Uses
	/// base64 encoding by default (smaller than base58) and inherits this client's commitment as
	/// the preflight commitment.
	pub async fn send_transaction(
		&self,
		transaction: &impl SerializableTransaction,
	) -> ClientResult<Signature> {
		self.send_transaction_with_config(
			transaction,
			RpcSendTransactionConfig {
				preflight_commitment: Some(self.commitment().commitment),
				..RpcSendTransactionConfig::default()
			},
		)
		.await
	}

	/// Submit a signed transaction with full control over encoding, preflight, and skip flags.
	///
	/// **When**: you need to disable preflight (`skip_preflight: true`) for speed, override the
	/// encoding, or cap preflight to a higher/lower commitment than the client default. Rare
	/// outside of specialized sender/relayer code.
	///
	/// **Why**: also performs a defensive signature cross-check — if the node echoes back a
	/// signature that doesn't match what was sent, we error instead of trusting it, because the
	/// transaction may or may not have landed under the returned signature. Preflight errors
	/// (code `-32002`) with decoded simulation logs are logged at `debug` when the `tx-debug`
	/// feature is on.
	pub async fn send_transaction_with_config(
		&self,
		transaction: &impl SerializableTransaction,
		config: RpcSendTransactionConfig,
	) -> ClientResult<Signature> {
		let encoding = config.encoding.unwrap_or(UiTransactionEncoding::Base64);
		let preflight_commitment = CommitmentConfig {
			commitment: config.preflight_commitment.unwrap_or_default(),
		};
		let config = RpcSendTransactionConfig {
			encoding: Some(encoding),
			preflight_commitment: Some(preflight_commitment.commitment),
			..config
		};
		let serialized_encoded = serialize_and_encode(transaction, encoding)?;
		let signature_base58_str: String = match self
			.send(
				RpcRequest::SendTransaction,
				json!([serialized_encoded, config]),
			)
			.await
		{
			Ok(signature_base58_str) => signature_base58_str,
			Err(err) => {
				if let ClientErrorKind::RpcError(RpcError::RpcResponseError {
					code,
					message,
					data,
				}) = &err.kind
				{
					debug!("{code} {message}");
					if let RpcResponseErrorData::SendTransactionPreflightFailure(r) = data {
						if let Some(ref logs) = r.logs {
							for (i, log) in logs.iter().enumerate() {
								debug!("{:>3}: {log}", i + 1);
							}
							debug!("");
						}
					}
				}
				return Err(err);
			}
		};

		let signature = signature_base58_str
			.parse::<Signature>()
			.map_err(|err| Into::<ClientError>::into(RpcError::ParseError(err.to_string())))?;
		// A mismatching RPC response signature indicates an issue with the RPC node, and
		// should not be passed along to confirmation methods. The transaction may or may
		// not have been submitted to the cluster, so callers should verify the success of
		// the correct transaction signature independently.
		if signature != *transaction.get_signature() {
			Err(RpcError::RpcRequestError(format!(
				"RPC node returned mismatched signature {:?}, expected {:?}",
				signature,
				transaction.get_signature()
			))
			.into())
		} else {
			Ok(*transaction.get_signature())
		}
	}

	/// Check whether a signature has reached (at least) this client's default commitment.
	///
	/// **When**: polling for confirmation of a transaction you already submitted. Returns `true`
	/// only when the signature is both present *and* its status was `Ok` (not failed).
	///
	/// **Why**: convenience wrapper that unwraps the `Response` envelope for the default
	/// commitment — use [`confirm_transaction_with_commitment`](Self::confirm_transaction_with_commitment)
	/// if you need the context slot or a different level.
	pub async fn confirm_transaction(&self, signature: &Signature) -> ClientResult<bool> {
		Ok(self
			.confirm_transaction_with_commitment(signature, self.commitment())
			.await?
			.value)
	}

	/// As [`confirm_transaction`](Self::confirm_transaction) but pick the commitment level and
	/// keep the full `Response { context, value }` envelope.
	///
	/// **When**: you need to know *at which slot* the status was observed (via `context.slot`)
	/// or you're polling at a lower commitment than the client default to reduce latency.
	///
	/// **Why**: returns `value = false` both for "not found" and "found but failed" — callers
	/// that need to distinguish should use [`get_signature_status`](Self::get_signature_status)
	/// instead.
	pub async fn confirm_transaction_with_commitment(
		&self,
		signature: &Signature,
		commitment_config: CommitmentConfig,
	) -> RpcResult<bool> {
		let Response { context, value } = self.get_signature_statuses(&[*signature]).await?;

		Ok(Response {
			context,
			value: value[0]
				.as_ref()
				.filter(|result| result.satisfies_commitment(commitment_config))
				.map(|result| result.status.is_ok())
				.unwrap_or_default(),
		})
	}

	/// Run a transaction through the bank's simulator without broadcasting it.
	///
	/// **When**: you want to know whether a transaction will succeed (and capture its logs /
	/// return data / compute units) before paying fees. Mandatory step for most "preview" flows
	/// and for tuning compute-unit limits.
	///
	/// **Why**: simulation is free, non-binding, and does not change chain state. Replay uses
	/// the client's default commitment — switch to
	/// [`simulate_transaction_with_config`](Self::simulate_transaction_with_config) if you need
	/// to replace recent blockhashes, request inner instructions, or capture specific accounts.
	pub async fn simulate_transaction(
		&self,
		transaction: &impl SerializableTransaction,
	) -> RpcResult<RpcSimulateTransactionResult> {
		self.simulate_transaction_with_config(
			transaction,
			RpcSimulateTransactionConfig {
				commitment: Some(self.commitment()),
				..RpcSimulateTransactionConfig::default()
			},
		)
		.await
	}

	/// As [`simulate_transaction`](Self::simulate_transaction) but with full
	/// [`RpcSimulateTransactionConfig`] control.
	///
	/// **When**: you need `replaceRecentBlockhash`, `sigVerify`, `innerInstructions`, or a
	/// specific `accounts` return set — all of which are only available through the full config.
	///
	/// **Why**: upstream also encodes the transaction (base58 or base64) here; we default to
	/// base64 for payload size. Picking a non-default encoding must be consistent between the
	/// serialized bytes and the `encoding` config field, which this helper handles for you.
	pub async fn simulate_transaction_with_config(
		&self,
		transaction: &impl SerializableTransaction,
		config: RpcSimulateTransactionConfig,
	) -> RpcResult<RpcSimulateTransactionResult> {
		let encoding = config.encoding.unwrap_or(UiTransactionEncoding::Base64);
		let commitment = config.commitment.unwrap_or_default();
		let config = RpcSimulateTransactionConfig {
			encoding: Some(encoding),
			commitment: Some(commitment),
			..config
		};
		let serialized_encoded = serialize_and_encode(transaction, encoding)?;
		self.send(
			RpcRequest::SimulateTransaction,
			json!([serialized_encoded, config]),
		)
		.await
	}

	nullary!(
		/// Highest slot the node has a snapshot for (full + incremental).
		///
		/// **When**: operational tooling that needs to pick the most recent snapshot to bootstrap
		/// a new validator or cold-start a warehouse loader.
		///
		/// **Why**: thin wrapper over `getHighestSnapshotSlot`; not useful for application logic,
		/// only infrastructure. Returns both full and incremental slot numbers.
		get_highest_snapshot_slot, GetHighestSnapshotSlot -> ClientResult<RpcSnapshotSlotInfo>
	);

	/// Fetch the status of a single signature at the client's default commitment.
	///
	/// **When**: polling after [`send_transaction`](Self::send_transaction) to decide whether to
	/// keep waiting, surface success, or surface the on-chain error. `None` means "not seen yet";
	/// `Some(Ok(()))` means success; `Some(Err(_))` means the transaction landed but failed.
	///
	/// **Why**: hides the `Response`+`Vec` envelope for the single-signature common case. Use
	/// [`get_signature_statuses`](Self::get_signature_statuses) to batch up to 256 signatures in
	/// one round-trip.
	pub async fn get_signature_status(
		&self,
		signature: &Signature,
	) -> ClientResult<Option<TransactionResult<()>>> {
		self.get_signature_status_with_commitment(signature, self.commitment())
			.await
	}

	/// Batch-fetch statuses for up to 256 signatures in one call.
	///
	/// **When**: a backend is tracking many in-flight transactions — batching amortizes the
	/// round-trip and dramatically reduces RPC load.
	///
	/// **Why**: `searchTransactionHistory` defaults to `false`, so signatures older than the
	/// status cache (~150 slots) return `None`. Use
	/// [`get_signature_statuses_with_history`](Self::get_signature_statuses_with_history) if you
	/// need to look further back — but be aware it's more expensive on the node.
	pub async fn get_signature_statuses(
		&self,
		signatures: &[Signature],
	) -> RpcResult<Vec<Option<TransactionStatus>>> {
		let signatures: Vec<_> = signatures.iter().map(|s| s.to_string()).collect();
		self.send(RpcRequest::GetSignatureStatuses, json!([signatures]))
			.await
	}

	/// Same as [`get_signature_statuses`](Self::get_signature_statuses) but with
	/// `searchTransactionHistory: true`.
	///
	/// **When**: you're looking up signatures that may be older than the in-memory status cache
	/// (typically ~150 slots, but node-dependent).
	///
	/// **Why**: searching history can be much slower on the node — only opt in when you actually
	/// need older-than-cache lookups. Many public RPCs throttle or disable this.
	pub async fn get_signature_statuses_with_history(
		&self,
		signatures: &[Signature],
	) -> RpcResult<Vec<Option<TransactionStatus>>> {
		let signatures: Vec<_> = signatures.iter().map(|s| s.to_string()).collect();
		self.send(
			RpcRequest::GetSignatureStatuses,
			json!([signatures, {
				"searchTransactionHistory": true
			}]),
		)
		.await
	}

	/// Single-signature status with explicit commitment.
	///
	/// **When**: same as [`get_signature_status`](Self::get_signature_status) but you want
	/// `Processed` (fast, may revert) or `Finalized` (settlement-grade).
	///
	/// **Why**: filters out statuses that don't satisfy the requested commitment — so
	/// `Some(status)` here means *also* "confirmed at least at `commitment_config`".
	pub async fn get_signature_status_with_commitment(
		&self,
		signature: &Signature,
		commitment_config: CommitmentConfig,
	) -> ClientResult<Option<TransactionResult<()>>> {
		let result: Response<Vec<Option<TransactionStatus>>> = self
			.send(
				RpcRequest::GetSignatureStatuses,
				json!([[signature.to_string()]]),
			)
			.await?;
		Ok(result.value[0]
			.clone()
			.filter(|result| result.satisfies_commitment(commitment_config))
			.map(|status_meta| status_meta.status))
	}

	/// Like [`get_signature_status_with_commitment`](Self::get_signature_status_with_commitment)
	/// but with the `searchTransactionHistory` toggle exposed.
	///
	/// **When**: you need BOTH a custom commitment AND history search — typically batch jobs
	/// reconciling historical transactions.
	///
	/// **Why**: combines both knobs in one call. Keep `search_transaction_history: false` unless
	/// you know the signature may be stale.
	pub async fn get_signature_status_with_commitment_and_history(
		&self,
		signature: &Signature,
		commitment_config: CommitmentConfig,
		search_transaction_history: bool,
	) -> ClientResult<Option<TransactionResult<()>>> {
		let result: Response<Vec<Option<TransactionStatus>>> = self
			.send(
				RpcRequest::GetSignatureStatuses,
				json!([[signature.to_string()], {
					"searchTransactionHistory": search_transaction_history
				}]),
			)
			.await?;
		Ok(result.value[0]
			.clone()
			.filter(|result| result.satisfies_commitment(commitment_config))
			.map(|status_meta| status_meta.status))
	}

	trio_commitment!(
		/// Current slot at the client's default commitment.
		///
		/// **When**: you need a rough "now" marker for on-chain time — most recent activity,
		/// lookback windows, scheduling. Cheap enough for tight loops.
		///
		/// **Why**: slot ≠ block — skipped slots have no block. If you need a slot that *has* a
		/// block, chain this with [`get_blocks`](Self::get_blocks).
		get_slot,
		/// Current slot at an explicit commitment. `Processed` is cheapest, `Finalized` is
		/// settlement-grade and ~30s behind `Processed`.
		get_slot_with_commitment,
		GetSlot -> ClientResult<Slot>
	);
	trio_commitment!(
		/// Current block height at the client's default commitment.
		///
		/// **When**: you're building "expires at block N" semantics (e.g. durable
		/// `last_valid_block_height` checks). Block height advances ~2.5/sec and skips no values,
		/// unlike slot.
		get_block_height,
		/// Current block height at an explicit commitment.
		get_block_height_with_commitment,
		GetBlockHeight -> ClientResult<u64>
	);

	/// Upcoming slot leaders starting at `start_slot`, up to `limit` entries.
	///
	/// **When**: submitting transactions with TPU forwarding, or choosing an RPC to send to based
	/// on who will produce the next block.
	///
	/// **Why**: returns validator identities (not vote accounts). Parses each pubkey string and
	/// fails the whole call on any malformed entry — treat that as an RPC-node bug, not user
	/// input error.
	pub async fn get_slot_leaders(
		&self,
		start_slot: Slot,
		limit: u64,
	) -> ClientResult<Vec<Pubkey>> {
		self.send(RpcRequest::GetSlotLeaders, json!([start_slot, limit]))
			.await
			.and_then(|slot_leaders: Vec<String>| {
				slot_leaders
					.iter()
					.map(|slot_leader| {
						Pubkey::from_str(slot_leader).map_err(|err| {
							ClientErrorKind::Custom(format!("pubkey deserialization failed: {err}"))
								.into()
						})
					})
					.collect()
			})
	}

	nullary!(
		/// Block production stats for the current epoch.
		///
		/// **When**: building validator dashboards or epoch-summary views. Returns a map of
		/// leader identity → (blocks scheduled, blocks produced) for the current epoch window.
		///
		/// **Why**: summarises skip rate per validator. Expensive on the node — avoid high-frequency
		/// polling. Use [`get_block_production_with_config`](Self::get_block_production_with_config)
		/// to scope to a specific validator or slot range.
		get_block_production, GetBlockProduction -> RpcResult<RpcBlockProduction>
	);

	/// Block production stats scoped by identity or slot range.
	///
	/// **When**: monitoring a single validator, or a sliding window of recent slots — much cheaper
	/// on the node than the unscoped variant.
	pub async fn get_block_production_with_config(
		&self,
		config: RpcBlockProductionConfig,
	) -> RpcResult<RpcBlockProduction> {
		self.send(RpcRequest::GetBlockProduction, json!([config]))
			.await
	}

	trio_commitment!(
		/// Total native SOL supply (circulating + non-circulating).
		///
		/// **When**: tokenomics displays, staking ratio calculations, or any "% of total supply"
		/// metric. Also returns the list of non-circulating accounts.
		///
		/// **Why**: expensive on the node (scans the stake cache); cache aggressively client-side,
		/// refresh at most once per minute.
		supply,
		/// Supply at an explicit commitment.
		supply_with_commitment,
		GetSupply -> RpcResult<RpcSupply>
	);

	/// Top-N native SOL holders, with filtering by circulating / non-circulating set.
	///
	/// **When**: "rich list" leaderboards, treasury audits. Returns a fixed top-20 by default
	/// (node-enforced); pagination is not supported.
	///
	/// **Why**: node-side filter/commitment must be explicit, so the config form is the only
	/// form — no bare `get_largest_accounts()` is provided upstream either.
	pub async fn get_largest_accounts_with_config(
		&self,
		config: RpcLargestAccountsConfig,
	) -> RpcResult<Vec<RpcAccountBalance>> {
		let commitment = config.commitment.unwrap_or_default();
		let config = RpcLargestAccountsConfig {
			commitment: Some(commitment),
			..config
		};
		self.send(RpcRequest::GetLargestAccounts, json!([config]))
			.await
	}

	/// Current and delinquent vote accounts, at the client's default commitment.
	///
	/// **When**: staking dashboards, validator pickers, delinquency monitors.
	///
	/// **Why**: returns *all* vote accounts — large response, especially on mainnet. Prefer
	/// [`get_vote_accounts_with_config`](Self::get_vote_accounts_with_config) with `vote_pubkey`
	/// if you only care about one validator.
	pub async fn get_vote_accounts(&self) -> ClientResult<RpcVoteAccountStatus> {
		self.get_vote_accounts_with_commitment(self.commitment())
			.await
	}

	/// Vote accounts at an explicit commitment.
	pub async fn get_vote_accounts_with_commitment(
		&self,
		commitment_config: CommitmentConfig,
	) -> ClientResult<RpcVoteAccountStatus> {
		self.get_vote_accounts_with_config(RpcGetVoteAccountsConfig {
			commitment: Some(commitment_config),
			..RpcGetVoteAccountsConfig::default()
		})
		.await
	}

	/// Vote accounts with full control (filter by vote pubkey, delinquency threshold, keep
	/// unstaked validators).
	///
	/// **When**: targeted validator lookups, or when the default delinquency slot distance
	/// doesn't match your definition.
	pub async fn get_vote_accounts_with_config(
		&self,
		config: RpcGetVoteAccountsConfig,
	) -> ClientResult<RpcVoteAccountStatus> {
		self.send(RpcRequest::GetVoteAccounts, json!([config]))
			.await
	}

	/// Block until no single validator holds more than `max_stake_percent` of total active stake.
	///
	/// **When**: test-cluster orchestration where you've just added stake and need to wait for
	/// distribution to normalize before running consensus-sensitive tests. Never call this from
	/// production code.
	///
	/// **Why**: runs forever unless the threshold is hit. Use
	/// [`wait_for_max_stake_below_threshold_with_timeout`](Self::wait_for_max_stake_below_threshold_with_timeout)
	/// in anything that might not converge.
	pub async fn wait_for_max_stake(
		&self,
		commitment: CommitmentConfig,
		max_stake_percent: f32,
	) -> ClientResult<()> {
		self.wait_for_max_stake_below_threshold_with_timeout_helper(
			commitment,
			max_stake_percent,
			None,
		)
		.await
	}

	/// Same as [`wait_for_max_stake`](Self::wait_for_max_stake) but gives up after `timeout`.
	///
	/// **When**: you need the orchestration convenience without risking an indefinite hang.
	pub async fn wait_for_max_stake_below_threshold_with_timeout(
		&self,
		commitment: CommitmentConfig,
		max_stake_percent: f32,
		timeout: Duration,
	) -> ClientResult<()> {
		self.wait_for_max_stake_below_threshold_with_timeout_helper(
			commitment,
			max_stake_percent,
			Some(timeout),
		)
		.await
	}

	/// Shared implementation for the two `wait_for_max_stake_*` entry points.
	///
	/// Polls `getVoteAccounts` every 5 seconds, computes `max_stake / total_stake` across both
	/// `current` and `delinquent` sets, and exits when it drops below `max_stake_percent`.
	async fn wait_for_max_stake_below_threshold_with_timeout_helper(
		&self,
		commitment: CommitmentConfig,
		max_stake_percent: f32,
		timeout: Option<Duration>,
	) -> ClientResult<()> {
		let mut current_percent;
		let start = Instant::now();
		loop {
			let vote_accounts = self.get_vote_accounts_with_commitment(commitment).await?;

			let mut max = 0;
			let total_active_stake = vote_accounts
				.current
				.iter()
				.chain(vote_accounts.delinquent.iter())
				.map(|vote_account| {
					max = std::cmp::max(max, vote_account.activated_stake);
					vote_account.activated_stake
				})
				.sum::<u64>();
			current_percent = 100f32 * max as f32 / total_active_stake as f32;
			if current_percent < max_stake_percent {
				break;
			} else if let Some(timeout) = timeout {
				if start.elapsed() > timeout {
					return Err(ClientErrorKind::Custom(
						"timed out waiting for max stake to drop".to_string(),
					)
					.into());
				}
			}

			info!(
				"Waiting for stake to drop below {max_stake_percent} current: {current_percent:.1}"
			);
			sleep(Duration::from_secs(5)).await;
		}
		Ok(())
	}

	nullary!(
		/// Gossip contact info for every validator the node knows about.
		///
		/// **When**: network-topology views, debugging peer connectivity, or building an RPC
		/// router that prefers geographically-close nodes.
		///
		/// **Why**: entries include IP, gossip/TPU/RPC ports, version, and feature set. No auth,
		/// no rate limit beyond the normal JSON-RPC quota.
		get_cluster_nodes, GetClusterNodes -> ClientResult<Vec<RpcContactInfo>>
	);

	/// Full confirmed block at `slot` with JSON-encoded transactions.
	///
	/// **When**: chain indexers, block explorers, analytics pipelines that want every transaction
	/// and its metadata.
	///
	/// **Why**: JSON encoding is heavy (~MB per block on mainnet) — if you only need a subset,
	/// use [`get_block_with_config`](Self::get_block_with_config) with `transaction_details:
	/// Signatures` or `None`, or switch to `Base64` encoding.
	pub async fn get_block(&self, slot: Slot) -> ClientResult<EncodedConfirmedBlock> {
		self.get_block_with_encoding(slot, UiTransactionEncoding::Json)
			.await
	}

	/// As [`get_block`](Self::get_block) but you pick the encoding.
	///
	/// **When**: `Base64` for indexers that want to re-decode themselves; `JsonParsed` for
	/// pretty displays (resolves account keys to readable form when possible).
	pub async fn get_block_with_encoding(
		&self,
		slot: Slot,
		encoding: UiTransactionEncoding,
	) -> ClientResult<EncodedConfirmedBlock> {
		self.send(RpcRequest::GetBlock, json!([slot, encoding]))
			.await
	}

	/// Block with full [`RpcBlockConfig`] — encoding, `transaction_details`, rewards toggle,
	/// max supported version.
	///
	/// **When**: you want signatures only (`TransactionDetails::Signatures`), or you need v0
	/// transactions (`max_supported_transaction_version: Some(0)`), or you want to skip the
	/// rewards payload for smaller responses.
	///
	/// **Why**: returns [`UiConfirmedBlock`] (lazier than [`EncodedConfirmedBlock`]) because the
	/// caller-provided `transaction_details` means transactions may be absent entirely.
	pub async fn get_block_with_config(
		&self,
		slot: Slot,
		config: RpcBlockConfig,
	) -> ClientResult<UiConfirmedBlock> {
		self.send(RpcRequest::GetBlock, json!([slot, config])).await
	}

	/// Confirmed block slots in `[start_slot, end_slot]` (inclusive). `end_slot: None` means
	/// "up to the latest confirmed slot".
	///
	/// **When**: you want to iterate through recent blocks but need to skip the many slots that
	/// produced no block (due to leader skips).
	///
	/// **Why**: capped at 500,000 slots by the node — for larger ranges, page with
	/// [`get_blocks_with_limit`](Self::get_blocks_with_limit).
	pub async fn get_blocks(
		&self,
		start_slot: Slot,
		end_slot: Option<Slot>,
	) -> ClientResult<Vec<Slot>> {
		self.send(RpcRequest::GetBlocks, json!([start_slot, end_slot]))
			.await
	}

	/// Same as [`get_blocks`](Self::get_blocks) with an explicit commitment. The slightly odd
	/// JSON construction avoids sending `null` for `end_slot`, which some nodes reject.
	pub async fn get_blocks_with_commitment(
		&self,
		start_slot: Slot,
		end_slot: Option<Slot>,
		commitment_config: CommitmentConfig,
	) -> ClientResult<Vec<Slot>> {
		let json = if end_slot.is_some() {
			json!([start_slot, end_slot, commitment_config])
		} else {
			json!([start_slot, commitment_config])
		};
		self.send(RpcRequest::GetBlocks, json).await
	}

	/// At most `limit` confirmed slots starting at `start_slot`.
	///
	/// **When**: forward-pagination through blocks — you don't know the end slot but you know
	/// how many you want. Much cheaper than [`get_blocks`](Self::get_blocks) for bounded pulls.
	///
	/// **Why**: node enforces `limit ≤ 500_000`.
	pub async fn get_blocks_with_limit(
		&self,
		start_slot: Slot,
		limit: usize,
	) -> ClientResult<Vec<Slot>> {
		self.send(RpcRequest::GetBlocksWithLimit, json!([start_slot, limit]))
			.await
	}

	/// Same as [`get_blocks_with_limit`](Self::get_blocks_with_limit) with explicit commitment.
	pub async fn get_blocks_with_limit_and_commitment(
		&self,
		start_slot: Slot,
		limit: usize,
		commitment_config: CommitmentConfig,
	) -> ClientResult<Vec<Slot>> {
		self.send(
			RpcRequest::GetBlocksWithLimit,
			json!([start_slot, limit, commitment_config]),
		)
		.await
	}

	/// Transaction signatures that touched `address`, most-recent first.
	///
	/// **When**: building per-account activity feeds (NFT history, wallet explorer). Returns up
	/// to 1000 signatures per page by default.
	///
	/// **Why**: paginate backwards through history via the `before` cursor in
	/// [`get_signatures_for_address_with_config`](Self::get_signatures_for_address_with_config).
	pub async fn get_signatures_for_address(
		&self,
		address: &Pubkey,
	) -> ClientResult<Vec<RpcConfirmedTransactionStatusWithSignature>> {
		self.get_signatures_for_address_with_config(
			address,
			GetConfirmedSignaturesForAddress2Config::default(),
		)
		.await
	}

	/// As [`get_signatures_for_address`](Self::get_signatures_for_address) with `before` / `until`
	/// cursors, `limit` (≤ 1000), and commitment.
	///
	/// **When**: paginating: pass the last-seen signature as `before` to fetch the next page; pass
	/// a known early signature as `until` to stop at a known boundary.
	pub async fn get_signatures_for_address_with_config(
		&self,
		address: &Pubkey,
		config: GetConfirmedSignaturesForAddress2Config,
	) -> ClientResult<Vec<RpcConfirmedTransactionStatusWithSignature>> {
		let config = RpcSignaturesForAddressConfig {
			before: config.before.map(|signature| signature.to_string()),
			until: config.until.map(|signature| signature.to_string()),
			limit: config.limit,
			commitment: config.commitment,
			min_context_slot: None,
		};

		let result: Vec<RpcConfirmedTransactionStatusWithSignature> = self
			.send(
				RpcRequest::GetSignaturesForAddress,
				json!([address.to_string(), config]),
			)
			.await?;

		Ok(result)
	}

	/// Fetch a confirmed transaction by signature with the given encoding.
	///
	/// **When**: you already have the signature (e.g. from
	/// [`get_signatures_for_address`](Self::get_signatures_for_address)) and want the full
	/// transaction + metadata.
	///
	/// **Why**: returns `None`-like error if the node has pruned the transaction. Not every node
	/// keeps full history — for stale lookups use an archival RPC.
	pub async fn get_transaction(
		&self,
		signature: &Signature,
		encoding: UiTransactionEncoding,
	) -> ClientResult<EncodedConfirmedTransactionWithStatusMeta> {
		self.send(
			RpcRequest::GetTransaction,
			json!([signature.to_string(), encoding]),
		)
		.await
	}

	/// As [`get_transaction`](Self::get_transaction) but with the full config (commitment,
	/// `max_supported_transaction_version`).
	///
	/// **When**: you need to decode v0 transactions — must pass
	/// `max_supported_transaction_version: Some(0)` or the RPC will reject the call.
	pub async fn get_transaction_with_config(
		&self,
		signature: &Signature,
		config: RpcTransactionConfig,
	) -> ClientResult<EncodedConfirmedTransactionWithStatusMeta> {
		self.send(
			RpcRequest::GetTransaction,
			json!([signature.to_string(), config]),
		)
		.await
	}

	/// Estimated production time of `slot` as a Unix timestamp (seconds).
	///
	/// **When**: annotating historical events with wall-clock time — block explorers, analytics.
	///
	/// **Why**: the node returns `null` for slots without an estimated time (too old, too new,
	/// or skipped); we turn that into `RpcError::ForUser("Block Not Found: slot=...")`. Not a
	/// consensus-critical value — validators don't agree on an exact timestamp.
	pub async fn get_block_time(&self, slot: Slot) -> ClientResult<UnixTimestamp> {
		let request = RpcRequest::GetBlockTime;
		let response = self.send(request, json!([slot])).await;

		response
			.map(|result_json: Value| {
				if result_json.is_null() {
					return Err(RpcError::ForUser(format!("Block Not Found: slot={slot}")).into());
				}
				let result = serde_json::from_value(result_json)
					.map_err(|err| ClientError::new_with_request(err.into(), request))?;
				trace!("Response block timestamp {slot:?} {result:?}");
				Ok(result)
			})
			.map_err(|err| err.into_with_request(request))?
	}

	trio_commitment!(
		/// Current epoch number plus progress within the epoch (slot index, slots remaining,
		/// block height, transaction count).
		///
		/// **When**: anything that depends on "how far through the epoch are we" — staking
		/// countdowns, epoch-boundary jobs, UI progress bars.
		get_epoch_info,
		/// Epoch info at an explicit commitment.
		get_epoch_info_with_commitment,
		GetEpochInfo -> ClientResult<EpochInfo>
	);

	/// Leader schedule for the epoch containing `slot` (or the current epoch if `slot: None`).
	///
	/// **When**: you want to know which validators produce which slots in the upcoming epoch —
	/// used by transaction senders that forward to the TPU of the next leader.
	///
	/// **Why**: returns `None` if the requested epoch isn't scheduled yet. The map is keyed by
	/// validator identity → slot indices relative to the epoch start (add the epoch's first slot
	/// to get absolute slots).
	pub async fn get_leader_schedule(
		&self,
		slot: Option<Slot>,
	) -> ClientResult<Option<RpcLeaderSchedule>> {
		self.get_leader_schedule_with_commitment(slot, self.commitment())
			.await
	}

	/// Same as [`get_leader_schedule`](Self::get_leader_schedule) with an explicit commitment.
	pub async fn get_leader_schedule_with_commitment(
		&self,
		slot: Option<Slot>,
		commitment_config: CommitmentConfig,
	) -> ClientResult<Option<RpcLeaderSchedule>> {
		self.get_leader_schedule_with_config(
			slot,
			RpcLeaderScheduleConfig {
				commitment: Some(commitment_config),
				..RpcLeaderScheduleConfig::default()
			},
		)
		.await
	}

	/// Leader schedule with a validator identity filter.
	///
	/// **When**: you only care about one validator's leader slots — massively smaller response.
	pub async fn get_leader_schedule_with_config(
		&self,
		slot: Option<Slot>,
		config: RpcLeaderScheduleConfig,
	) -> ClientResult<Option<RpcLeaderSchedule>> {
		self.send(RpcRequest::GetLeaderSchedule, json!([slot, config]))
			.await
	}

	nullary!(
		/// Cluster's epoch schedule parameters (slots per epoch, warmup, first normal epoch).
		///
		/// **When**: converting between slot and epoch arithmetic without hard-coding cluster
		/// constants. Cache aggressively — this never changes without a cluster restart.
		get_epoch_schedule, GetEpochSchedule -> ClientResult<EpochSchedule>
	);

	/// Recent performance samples (slot range, transactions, TPS, skipped slots).
	///
	/// **When**: monitoring dashboards, "is the cluster healthy?" widgets. Each sample covers
	/// ~60 seconds.
	///
	/// **Why**: `limit` is capped at 720 by the node (12h of samples). `None` uses the node
	/// default (typically 720).
	pub async fn get_recent_performance_samples(
		&self,
		limit: Option<usize>,
	) -> ClientResult<Vec<RpcPerfSample>> {
		self.send(RpcRequest::GetRecentPerformanceSamples, json!([limit]))
			.await
	}

	/// Recent priority-fee percentile data, optionally scoped to a set of writable accounts.
	///
	/// **When**: you're about to send a transaction and want a data-driven compute-unit-price
	/// estimate so it lands without overpaying.
	///
	/// **Why**: passing the *writable* accounts of your upcoming transaction returns fees that
	/// account for write-lock contention on those specific accounts, not the generic chain-wide
	/// percentile. This is the single most valuable Solana RPC for fee estimation.
	pub async fn get_recent_prioritization_fees(
		&self,
		addresses: &[Pubkey],
	) -> ClientResult<Vec<RpcPrioritizationFee>> {
		let addresses: Vec<_> = addresses
			.iter()
			.map(|address| address.to_string())
			.collect();
		self.send(RpcRequest::GetRecentPrioritizationFees, json!([addresses]))
			.await
	}

	/// Identity pubkey of the node we're talking to.
	///
	/// **When**: debugging multi-RPC setups, or sanity-checking that an RPC router ended up
	/// where you expected.
	///
	/// **Why**: parses the returned string into a [`Pubkey`]; any parse failure is surfaced as
	/// `RpcError::ParseError` rather than silently returning junk.
	pub async fn get_identity(&self) -> ClientResult<Pubkey> {
		let rpc_identity: RpcIdentity = self.send(RpcRequest::GetIdentity, Value::Null).await?;

		rpc_identity.identity.parse::<Pubkey>().map_err(|_| {
			ClientError::new_with_request(
				RpcError::ParseError("Pubkey".to_string()).into(),
				RpcRequest::GetIdentity,
			)
		})
	}

	nullary!(
		/// Inflation governor (initial/terminal rates, taper, foundation share).
		///
		/// **When**: static governance/economic displays. These values only change via a cluster
		/// feature-gate activation — cache indefinitely.
		get_inflation_governor, GetInflationGovernor -> ClientResult<RpcInflationGovernor>
	);
	nullary!(
		/// Current inflation breakdown (total, validator, foundation, epoch).
		///
		/// **When**: live APR calculations and staking dashboards. Changes every epoch.
		get_inflation_rate, GetInflationRate -> ClientResult<RpcInflationRate>
	);

	/// Per-address inflation rewards for a given epoch (or the most recent rewarded one).
	///
	/// **When**: staking UI showing "rewards this epoch" for a set of stake accounts.
	///
	/// **Why**: returns `None` for addresses that weren't eligible (not activated, deactivated,
	/// no stake). Expensive call when asking about many accounts — batch, don't loop.
	pub async fn get_inflation_reward(
		&self,
		addresses: &[Pubkey],
		epoch: Option<Epoch>,
	) -> ClientResult<Vec<Option<RpcInflationReward>>> {
		let addresses: Vec<_> = addresses
			.iter()
			.map(|address| address.to_string())
			.collect();
		self.send(
			RpcRequest::GetInflationReward,
			json!([
				addresses,
				RpcEpochConfig {
					epoch,
					commitment: Some(self.commitment()),
					min_context_slot: None,
				}
			]),
		)
		.await
	}

	nullary!(
		/// Node software version (`solana-core` version + feature set hash).
		///
		/// **When**: debugging "why does this node reject v0 transactions?" — older nodes lack
		/// feature-gated RPC behaviors.
		get_version, GetVersion -> ClientResult<RpcVersionInfo>
	);
	nullary!(
		/// Oldest slot the node has ledger for. Anything older has been pruned.
		///
		/// **When**: before attempting a historical [`get_block`](Self::get_block) or
		/// [`get_transaction`](Self::get_transaction) — if `slot < minimum_ledger_slot`, the
		/// request will fail with "block not available" no matter how you phrase it.
		minimum_ledger_slot, MinimumLedgerSlot -> ClientResult<Slot>
	);

	/// Decoded account at `pubkey`, or `AccountNotFound` error if it doesn't exist.
	///
	/// **When**: you want the account and treating "not found" as an error is fine (e.g.
	/// dereferencing a known PDA).
	///
	/// **Why**: strictest variant — use
	/// [`get_account_with_commitment`](Self::get_account_with_commitment) if `None` is a
	/// legitimate outcome. Uses `Base64Zstd` encoding internally for wire efficiency.
	pub async fn get_account(&self, pubkey: &Pubkey) -> ClientResult<Account> {
		self.get_account_with_commitment(pubkey, self.commitment())
			.await?
			.value
			.ok_or_else(|| RpcError::ForUser(format!("AccountNotFound: pubkey={pubkey}")).into())
	}

	/// Decoded account wrapped in `Response<Option<Account>>`.
	///
	/// **When**: you need to distinguish "doesn't exist" from "error fetching", or you want the
	/// `context.slot` alongside the account.
	pub async fn get_account_with_commitment(
		&self,
		pubkey: &Pubkey,
		commitment_config: CommitmentConfig,
	) -> RpcResult<Option<Account>> {
		let config = RpcAccountInfoConfig {
			encoding: Some(UiAccountEncoding::Base64Zstd),
			commitment: Some(commitment_config),
			data_slice: None,
			min_context_slot: None,
		};

		self.get_account_with_config(pubkey, config).await
	}

	/// Raw account fetch with full control (encoding, data slice, min context slot).
	///
	/// **When**: you only need a small window of the account data (`data_slice`) — e.g. reading
	/// a 32-byte field at a known offset from a huge program account. Huge bandwidth savings.
	///
	/// **Why**: requests `Base64Zstd` by default; override if you want `JsonParsed` for typed
	/// decoding. `min_context_slot` lets you enforce freshness — fail rather than return stale
	/// state.
	pub async fn get_account_with_config(
		&self,
		pubkey: &Pubkey,
		config: RpcAccountInfoConfig,
	) -> RpcResult<Option<Account>> {
		let response = self
			.send(
				RpcRequest::GetAccountInfo,
				json!([pubkey.to_string(), config]),
			)
			.await;

		response
			.map(|result_json: Value| {
				if result_json.is_null() {
					return Err(
						RpcError::ForUser(format!("AccountNotFound: pubkey={pubkey}")).into(),
					);
				}
				let Response {
					context,
					value: rpc_account,
				} = serde_json::from_value::<Response<Option<UiAccount>>>(result_json)?;
				trace!("Response account {pubkey:?} {rpc_account:?}");
				let account = rpc_account.and_then(|rpc_account| rpc_account.decode());

				Ok(Response {
					context,
					value: account,
				})
			})
			.map_err(|err| {
				Into::<ClientError>::into(RpcError::ForUser(format!(
					"AccountNotFound: pubkey={pubkey}: {err}"
				)))
			})?
	}

	nullary!(
		/// Highest slot that the node has retransmitted shreds for.
		///
		/// **When**: validator / turbine debugging only. Useless to application code.
		get_max_retransmit_slot, GetMaxRetransmitSlot -> ClientResult<Slot>
	);
	nullary!(
		/// Highest slot the node has inserted shreds for.
		///
		/// **When**: validator debugging — `max_shred_insert_slot` lagging behind the network's
		/// slot is a signal the node is falling behind.
		get_max_shred_insert_slot, GetMaxShredInsertSlot -> ClientResult<Slot>
	);

	/// Fetch up to 100 accounts in one call; missing accounts are `None` at their position.
	///
	/// **When**: you have a known set of pubkeys (e.g. resolving derived PDAs for a user). One
	/// round-trip instead of N.
	///
	/// **Why**: node caps batch size at 100 — chunk larger inputs yourself. Order of results
	/// matches order of `pubkeys`.
	pub async fn get_multiple_accounts(
		&self,
		pubkeys: &[Pubkey],
	) -> ClientResult<Vec<Option<Account>>> {
		Ok(self
			.get_multiple_accounts_with_commitment(pubkeys, self.commitment())
			.await?
			.value)
	}

	/// Batch fetch with explicit commitment.
	pub async fn get_multiple_accounts_with_commitment(
		&self,
		pubkeys: &[Pubkey],
		commitment_config: CommitmentConfig,
	) -> RpcResult<Vec<Option<Account>>> {
		self.get_multiple_accounts_with_config(
			pubkeys,
			RpcAccountInfoConfig {
				encoding: Some(UiAccountEncoding::Base64Zstd),
				commitment: Some(commitment_config),
				data_slice: None,
				min_context_slot: None,
			},
		)
		.await
	}

	/// Batch fetch with full config (data slice, encoding, commitment, min context slot).
	///
	/// **When**: same reasoning as
	/// [`get_account_with_config`](Self::get_account_with_config) but you're batching. `data_slice`
	/// is particularly valuable here — shaving data off 100 accounts compounds.
	pub async fn get_multiple_accounts_with_config(
		&self,
		pubkeys: &[Pubkey],
		config: RpcAccountInfoConfig,
	) -> RpcResult<Vec<Option<Account>>> {
		let config = RpcAccountInfoConfig {
			commitment: config.commitment.or_else(|| Some(self.commitment())),
			..config
		};
		let pubkeys: Vec<_> = pubkeys.iter().map(|pubkey| pubkey.to_string()).collect();
		let response = self
			.send(RpcRequest::GetMultipleAccounts, json!([pubkeys, config]))
			.await?;
		let Response {
			context,
			value: accounts,
		} = serde_json::from_value::<Response<Vec<Option<UiAccount>>>>(response)?;
		let accounts: Vec<Option<Account>> = accounts
			.into_iter()
			.map(|rpc_account| rpc_account.and_then(|a| a.decode()))
			.collect();
		Ok(Response {
			context,
			value: accounts,
		})
	}

	/// Just the `data` bytes of `pubkey`'s account — shorthand for `get_account(pk).await?.data`.
	///
	/// **When**: you're about to deserialize a program-owned account and don't care about
	/// lamports, owner, executable, or rent-epoch fields.
	pub async fn get_account_data(&self, pubkey: &Pubkey) -> ClientResult<Vec<u8>> {
		Ok(self.get_account(pubkey).await?.data)
	}

	/// Lamports needed to rent-exempt an account with `data_len` bytes of data.
	///
	/// **When**: creating a new account — sizing the initial lamport deposit. Any smaller and
	/// the runtime will reject the create-account instruction.
	///
	/// **Why**: depends on current rent parameters; theoretically mutable via feature gate, so
	/// don't hard-code. Cheap enough to call per create.
	pub async fn get_minimum_balance_for_rent_exemption(
		&self,
		data_len: usize,
	) -> ClientResult<u64> {
		let request = RpcRequest::GetMinimumBalanceForRentExemption;
		let minimum_balance_json: Value = self
			.send(request, json!([data_len]))
			.await
			.map_err(|err| err.into_with_request(request))?;

		let minimum_balance: u64 = serde_json::from_value(minimum_balance_json)
			.map_err(|err| ClientError::new_with_request(err.into(), request))?;
		trace!("Response minimum balance {data_len:?} {minimum_balance:?}");
		Ok(minimum_balance)
	}

	trio_pubkey_value!(
		/// Native SOL balance in lamports (1 SOL = 10^9 lamports).
		///
		/// **When**: "do I have enough to pay fees / send X SOL" checks, wallet balance displays.
		/// Fastest account-read call on the network.
		///
		/// **Why**: returns lamports, not SOL. Convert with `lamports as f64 / LAMPORTS_PER_SOL`
		/// only for display — never for arithmetic.
		get_balance,
		/// Balance at an explicit commitment, returned with the full `Response` envelope.
		get_balance_with_commitment,
		GetBalance -> u64
	);

	/// All accounts owned by `pubkey` (a program id), decoded.
	///
	/// **When**: small programs ("give me every account owned by my program"). Default filters
	/// are `None`, so this returns EVERYTHING — including thousands of accounts on popular
	/// programs.
	///
	/// **Why**: most public RPCs heavily restrict or disable this call. Use
	/// [`get_program_accounts_with_config`](Self::get_program_accounts_with_config) with
	/// `memcmp` / `dataSize` filters to narrow the result — even modest filters cut the
	/// response by orders of magnitude.
	pub async fn get_program_accounts(
		&self,
		pubkey: &Pubkey,
	) -> ClientResult<Vec<(Pubkey, Account)>> {
		self.get_program_accounts_with_config(
			pubkey,
			RpcProgramAccountsConfig {
				account_config: RpcAccountInfoConfig {
					encoding: Some(UiAccountEncoding::Base64Zstd),
					..RpcAccountInfoConfig::default()
				},
				..RpcProgramAccountsConfig::default()
			},
		)
		.await
	}

	/// Filtered `getProgramAccounts` — the only sensible version for real-world programs.
	///
	/// **When**: always prefer this over [`get_program_accounts`](Self::get_program_accounts).
	/// Add a `filters` entry with `dataSize` and one or more `memcmp` filters to narrow to the
	/// specific account variant you want.
	///
	/// **Why**: we auto-fill commitment from the client default if the config omits it, so you
	/// only need to set the knobs you actually care about (`filters`, `data_slice`).
	pub async fn get_program_accounts_with_config(
		&self,
		pubkey: &Pubkey,
		mut config: RpcProgramAccountsConfig,
	) -> ClientResult<Vec<(Pubkey, Account)>> {
		let commitment = config
			.account_config
			.commitment
			.unwrap_or_else(|| self.commitment());
		config.account_config.commitment = Some(commitment);

		let accounts = self
			.send::<OptionalContext<Vec<RpcKeyedAccount>>>(
				RpcRequest::GetProgramAccounts,
				json!([pubkey.to_string(), config]),
			)
			.await?
			.parse_value();
		parse_keyed_accounts(accounts, RpcRequest::GetProgramAccounts)
	}

	/// Current minimum stake delegation in lamports.
	///
	/// **When**: stake-UI flows that need to reject "too small to delegate" inputs before
	/// constructing the instruction.
	///
	/// **Why**: this is feature-gated and may change — don't hard-code the 1 SOL historical
	/// value.
	pub async fn get_stake_minimum_delegation(&self) -> ClientResult<u64> {
		self.get_stake_minimum_delegation_with_commitment(self.commitment())
			.await
	}

	/// Minimum stake delegation at an explicit commitment.
	pub async fn get_stake_minimum_delegation_with_commitment(
		&self,
		commitment_config: CommitmentConfig,
	) -> ClientResult<u64> {
		Ok(self
			.send::<Response<u64>>(
				RpcRequest::GetStakeMinimumDelegation,
				json!([commitment_config]),
			)
			.await?
			.value)
	}

	trio_commitment!(
		/// Total transactions seen by the node since genesis.
		///
		/// **When**: TPS calculations (diff two samples / time), explorer headline numbers.
		///
		/// **Why**: counter, not gauge — the delta is what's meaningful, not the raw value.
		get_transaction_count,
		/// Transaction count at an explicit commitment.
		get_transaction_count_with_commitment,
		GetTransactionCount -> ClientResult<u64>
	);

	nullary!(
		/// Lowest slot that still has a confirmed block available.
		///
		/// **When**: chain indexers deciding how far back to start; similar to
		/// [`minimum_ledger_slot`](Self::minimum_ledger_slot) but for *confirmed* blocks rather
		/// than raw ledger data.
		get_first_available_block, GetFirstAvailableBlock -> ClientResult<Slot>
	);

	/// Genesis block hash of the cluster.
	///
	/// **When**: verifying you're talking to the cluster you think you are (mainnet / devnet /
	/// testnet have different genesis hashes). Wallets often check this before signing.
	pub async fn get_genesis_hash(&self) -> ClientResult<Hash> {
		let hash_str: String = self.send(RpcRequest::GetGenesisHash, Value::Null).await?;
		let hash = hash_str.parse().map_err(|_| {
			ClientError::new_with_request(
				RpcError::ParseError("Hash".to_string()).into(),
				RpcRequest::GetGenesisHash,
			)
		})?;
		Ok(hash)
	}

	/// "OK" probe — `Ok(())` means the node considers itself healthy.
	///
	/// **When**: health checks for RPC routers / load balancers before sending real traffic.
	///
	/// **Why**: cheapest RPC call there is. Failure body may contain `numSlotsBehind` data (see
	/// the `tx-debug` feature decoder) but we drop it here; a richer health check would inspect
	/// [`get_version`](Self::get_version) + [`get_slot`](Self::get_slot) lag instead.
	pub async fn get_health(&self) -> ClientResult<()> {
		self.send::<String>(RpcRequest::GetHealth, Value::Null)
			.await
			.map(|_| ())
	}

	/// Decoded SPL-Token account at `pubkey`, or `None` if `pubkey` is not a token account.
	///
	/// **When**: displaying balances for a specific ATA — returns `amount`, `mint`, `owner`,
	/// `state`, delegation info.
	///
	/// **Why**: returns `None` (not error) when the account exists but isn't a token account,
	/// which matters for callers that speculatively try addresses. Errors only on genuine "not
	/// found" / parse failure.
	pub async fn get_token_account(&self, pubkey: &Pubkey) -> ClientResult<Option<UiTokenAccount>> {
		Ok(self
			.get_token_account_with_commitment(pubkey, self.commitment())
			.await?
			.value)
	}

	/// As [`get_token_account`](Self::get_token_account) with an explicit commitment.
	///
	/// **Why**: requests `JsonParsed` encoding — the node does the program-data → typed-fields
	/// conversion server-side, so we don't have to understand token-program layout here.
	pub async fn get_token_account_with_commitment(
		&self,
		pubkey: &Pubkey,
		commitment_config: CommitmentConfig,
	) -> RpcResult<Option<UiTokenAccount>> {
		let config = RpcAccountInfoConfig {
			encoding: Some(UiAccountEncoding::JsonParsed),
			commitment: Some(commitment_config),
			data_slice: None,
			min_context_slot: None,
		};
		let response = self
			.send(
				RpcRequest::GetAccountInfo,
				json!([pubkey.to_string(), config]),
			)
			.await;

		response
			.map(|result_json: Value| {
				if result_json.is_null() {
					return Err(
						RpcError::ForUser(format!("AccountNotFound: pubkey={pubkey}")).into(),
					);
				}
				let Response {
					context,
					value: rpc_account,
				} = serde_json::from_value::<Response<Option<UiAccount>>>(result_json)?;
				trace!("Response account {pubkey:?} {rpc_account:?}");
				let response = {
					if let Some(rpc_account) = rpc_account {
						if let UiAccountData::Json(account_data) = rpc_account.data {
							let token_account_type: TokenAccountType =
								serde_json::from_value(account_data.parsed)?;
							if let TokenAccountType::Account(token_account) = token_account_type {
								return Ok(Response {
									context,
									value: Some(token_account),
								});
							}
						}
					}
					Err(Into::<ClientError>::into(RpcError::ForUser(format!(
						"Account could not be parsed as token account: pubkey={pubkey}"
					))))
				};
				response?
			})
			.map_err(|err| {
				Into::<ClientError>::into(RpcError::ForUser(format!(
					"AccountNotFound: pubkey={pubkey}: {err}"
				)))
			})?
	}

	trio_pubkey_value!(
		/// Decoded `UiTokenAmount` balance for a known SPL-Token account.
		///
		/// **When**: faster than [`get_token_account`](Self::get_token_account) when you only
		/// need the amount / decimals / UI string, not the whole account. Skips token-account
		/// layout decoding round-trip.
		get_token_account_balance,
		/// Token balance at an explicit commitment.
		get_token_account_balance_with_commitment,
		GetTokenAccountBalance -> UiTokenAmount
	);

	/// All SPL-Token accounts where `delegate` has approval, filtered by mint or token program.
	///
	/// **When**: "approvals" UX — showing what tokens this address can spend on behalf of
	/// others. Rare outside of dapps with explicit delegation flows.
	pub async fn get_token_accounts_by_delegate(
		&self,
		delegate: &Pubkey,
		token_account_filter: TokenAccountsFilter,
	) -> ClientResult<Vec<RpcKeyedAccount>> {
		Ok(self
			.get_token_accounts_by_delegate_with_commitment(
				delegate,
				token_account_filter,
				self.commitment(),
			)
			.await?
			.value)
	}

	/// Delegated token accounts at an explicit commitment, keeping the `Response` envelope.
	///
	/// **Why**: historically this method's dispatch had a bug where it fell through to
	/// `GetTokenAccountsByOwner` — fixed in commit `8d30b4e5`. Double-check you're on a recent
	/// `zela-std` if delegation queries are silently returning owned accounts.
	pub async fn get_token_accounts_by_delegate_with_commitment(
		&self,
		delegate: &Pubkey,
		token_account_filter: TokenAccountsFilter,
		commitment_config: CommitmentConfig,
	) -> RpcResult<Vec<RpcKeyedAccount>> {
		let token_account_filter = match token_account_filter {
			TokenAccountsFilter::Mint(mint) => RpcTokenAccountsFilter::Mint(mint.to_string()),
			TokenAccountsFilter::ProgramId(program_id) => {
				RpcTokenAccountsFilter::ProgramId(program_id.to_string())
			}
		};

		let config = RpcAccountInfoConfig {
			encoding: Some(UiAccountEncoding::JsonParsed),
			commitment: Some(commitment_config),
			data_slice: None,
			min_context_slot: None,
		};

		self.send(
			RpcRequest::GetTokenAccountsByDelegate,
			json!([delegate.to_string(), token_account_filter, config]),
		)
		.await
	}

	/// All SPL-Token accounts owned by `owner`, filtered by mint or token program.
	///
	/// **When**: wallet token balance view — pass `TokenAccountsFilter::ProgramId(spl_token::ID)`
	/// to get every token (classic) account, or `::Mint(m)` for a specific token.
	///
	/// **Why**: single most common "portfolio" call. Use `JsonParsed` encoding (we request it by
	/// default) so the response has decoded amounts rather than raw account bytes.
	pub async fn get_token_accounts_by_owner(
		&self,
		owner: &Pubkey,
		token_account_filter: TokenAccountsFilter,
	) -> ClientResult<Vec<RpcKeyedAccount>> {
		Ok(self
			.get_token_accounts_by_owner_with_commitment(
				owner,
				token_account_filter,
				self.commitment(),
			)
			.await?
			.value)
	}

	/// Owned token accounts at an explicit commitment.
	pub async fn get_token_accounts_by_owner_with_commitment(
		&self,
		owner: &Pubkey,
		token_account_filter: TokenAccountsFilter,
		commitment_config: CommitmentConfig,
	) -> RpcResult<Vec<RpcKeyedAccount>> {
		let token_account_filter = match token_account_filter {
			TokenAccountsFilter::Mint(mint) => RpcTokenAccountsFilter::Mint(mint.to_string()),
			TokenAccountsFilter::ProgramId(program_id) => {
				RpcTokenAccountsFilter::ProgramId(program_id.to_string())
			}
		};

		let config = RpcAccountInfoConfig {
			encoding: Some(UiAccountEncoding::JsonParsed),
			commitment: Some(commitment_config),
			data_slice: None,
			min_context_slot: None,
		};

		self.send(
			RpcRequest::GetTokenAccountsByOwner,
			json!([owner.to_string(), token_account_filter, config]),
		)
		.await
	}

	trio_pubkey_value!(
		/// Top 20 holders of an SPL-Token mint.
		///
		/// **When**: "token distribution" widgets on token info pages.
		///
		/// **Why**: size is fixed at 20 by the node — no pagination.
		get_token_largest_accounts,
		/// Top holders at an explicit commitment.
		get_token_largest_accounts_with_commitment,
		GetTokenLargestAccounts -> Vec<RpcTokenAccountBalance>
	);

	trio_pubkey_value!(
		/// Total circulating supply of an SPL-Token mint.
		///
		/// **When**: market cap calculations, per-token "X of Y minted" displays.
		get_token_supply,
		/// Token supply at an explicit commitment.
		get_token_supply_with_commitment,
		GetTokenSupply -> UiTokenAmount
	);

	/// Request an airdrop (devnet/testnet only).
	///
	/// **When**: test setup. Mainnet RPCs universally reject this.
	///
	/// **Why**: heavily rate-limited — faucets cap at ~1–2 SOL per request and a few requests
	/// per hour per IP. For local dev, use `solana-test-validator` airdrops instead.
	pub async fn request_airdrop(&self, pubkey: &Pubkey, lamports: u64) -> ClientResult<Signature> {
		self.request_airdrop_with_config(
			pubkey,
			lamports,
			RpcRequestAirdropConfig {
				commitment: Some(self.commitment()),
				..RpcRequestAirdropConfig::default()
			},
		)
		.await
	}

	/// Airdrop with a caller-provided recent blockhash — useful in tests that need deterministic
	/// blockhash behaviour.
	pub async fn request_airdrop_with_blockhash(
		&self,
		pubkey: &Pubkey,
		lamports: u64,
		recent_blockhash: &Hash,
	) -> ClientResult<Signature> {
		self.request_airdrop_with_config(
			pubkey,
			lamports,
			RpcRequestAirdropConfig {
				commitment: Some(self.commitment()),
				recent_blockhash: Some(recent_blockhash.to_string()),
			},
		)
		.await
	}

	/// Airdrop with full config — commitment, optional specific blockhash.
	///
	/// **Why**: any error is flattened into a generic `"airdrop request failed"` ForUser message
	/// because the common cause is rate limiting, and the specific RPC error rarely helps users.
	pub async fn request_airdrop_with_config(
		&self,
		pubkey: &Pubkey,
		lamports: u64,
		config: RpcRequestAirdropConfig,
	) -> ClientResult<Signature> {
		let commitment = config.commitment.unwrap_or_default();
		let config = RpcRequestAirdropConfig {
			commitment: Some(commitment),
			..config
		};
		self.send(
			RpcRequest::RequestAirdrop,
			json!([pubkey.to_string(), lamports, config]),
		)
		.await
		.and_then(|signature: String| {
			Signature::from_str(&signature).map_err(|err| {
				ClientErrorKind::Custom(format!("signature deserialization failed: {err}")).into()
			})
		})
		.map_err(|_| {
			RpcError::ForUser(
				"airdrop request failed. \
					This can happen when the rate limit is reached."
					.to_string(),
			)
			.into()
		})
	}

	/// Poll `getBalance` every `polling_frequency` until it succeeds or `timeout` is hit.
	///
	/// **When**: shortly after an airdrop, waiting for lamports to actually be visible. Only
	/// used internally by the other poll helpers.
	///
	/// **Why**: `pub(crate)` because callers really should prefer
	/// [`wait_for_balance_with_commitment`](Self::wait_for_balance_with_commitment) which layers
	/// "equals expected amount" semantics on top.
	pub(crate) async fn poll_balance_with_timeout_and_commitment(
		&self,
		pubkey: &Pubkey,
		polling_frequency: &Duration,
		timeout: &Duration,
		commitment_config: CommitmentConfig,
	) -> ClientResult<u64> {
		let now = Instant::now();
		loop {
			match self
				.get_balance_with_commitment(pubkey, commitment_config)
				.await
			{
				Ok(bal) => {
					return Ok(bal.value);
				}
				Err(e) => {
					sleep(*polling_frequency).await;
					if now.elapsed() > *timeout {
						return Err(e);
					}
				}
			};
		}
	}

	/// Convenience wrapper: poll balance every 100ms for up to 1 second.
	///
	/// **When**: tight test loops that just want "get me any successful balance read, soon".
	pub async fn poll_get_balance_with_commitment(
		&self,
		pubkey: &Pubkey,
		commitment_config: CommitmentConfig,
	) -> ClientResult<u64> {
		self.poll_balance_with_timeout_and_commitment(
			pubkey,
			&Duration::from_millis(100),
			&Duration::from_secs(1),
			commitment_config,
		)
		.await
	}

	/// Poll until the balance matches `expected_balance`, or up to 30 attempts.
	///
	/// **When**: test scaffolding after an airdrop or transfer — "wait until this account shows
	/// exactly N lamports".
	///
	/// **Why**: `expected_balance: None` returns as soon as any read succeeds (acts like a
	/// "wait for account to exist"). Otherwise loops until equality or 30 failed reads.
	pub async fn wait_for_balance_with_commitment(
		&self,
		pubkey: &Pubkey,
		expected_balance: Option<u64>,
		commitment_config: CommitmentConfig,
	) -> ClientResult<u64> {
		const LAST: usize = 30;
		let mut run = 0;
		loop {
			let balance_result = self
				.poll_get_balance_with_commitment(pubkey, commitment_config)
				.await;
			if expected_balance.is_none() || (balance_result.is_err() && run == LAST) {
				return balance_result;
			}
			trace!(
				"wait_for_balance_with_commitment [{run}] {balance_result:?} {expected_balance:?}"
			);
			if let (Some(expected_balance), Ok(balance_result)) = (expected_balance, balance_result)
			{
				if expected_balance == balance_result {
					return Ok(balance_result);
				}
			}
			run += 1;
		}
	}

	/// Block up to 15s waiting for `signature` to acquire any status (success OR on-chain error).
	///
	/// **When**: post-submit wait loops where you don't care about on-chain success yet — just
	/// "has the cluster seen this?". Cheaper than
	/// [`send_and_confirm_transaction`](Self::send_and_confirm_transaction) if you're doing your
	/// own confirmation later.
	pub async fn poll_for_signature(&self, signature: &Signature) -> ClientResult<()> {
		self.poll_for_signature_with_commitment(signature, self.commitment())
			.await
	}

	/// Same as [`poll_for_signature`](Self::poll_for_signature) with explicit commitment.
	///
	/// **Why**: polls every 250ms for up to 15s; returns `ForUser` with the elapsed time on
	/// timeout.
	pub async fn poll_for_signature_with_commitment(
		&self,
		signature: &Signature,
		commitment_config: CommitmentConfig,
	) -> ClientResult<()> {
		let now = Instant::now();
		loop {
			if let Ok(Some(_)) = self
				.get_signature_status_with_commitment(signature, commitment_config)
				.await
			{
				break;
			}
			if now.elapsed().as_secs() > 15 {
				return Err(RpcError::ForUser(format!(
					"signature not found after {} seconds",
					now.elapsed().as_secs()
				))
				.into());
			}
			sleep(Duration::from_millis(250)).await;
		}
		Ok(())
	}

	/// Poll until `signature` has at least `min_confirmed_blocks` confirmations, or give up
	/// after 20s of no progress.
	///
	/// **When**: when you want "N blocks past inclusion" rather than a commitment level.
	/// `min_confirmed_blocks` up to `MAX_LOCKOUT_HISTORY` (31) is meaningful — beyond that,
	/// confirmation count plateaus.
	///
	/// **Why**: returns whatever partial count was reached on timeout (if > 0), only erroring if
	/// the signature never appeared at all.
	pub async fn poll_for_signature_confirmation(
		&self,
		signature: &Signature,
		min_confirmed_blocks: usize,
	) -> ClientResult<usize> {
		let mut now = Instant::now();
		let mut confirmed_blocks = 0;
		loop {
			let response = self
				.get_num_blocks_since_signature_confirmation(signature)
				.await;
			match response {
				Ok(count) => {
					if confirmed_blocks != count {
						info!(
							"signature {} confirmed {} out of {} after {} ms",
							signature,
							count,
							min_confirmed_blocks,
							now.elapsed().as_millis()
						);
						now = Instant::now();
						confirmed_blocks = count;
					}
					if count >= min_confirmed_blocks {
						break;
					}
				}
				Err(err) => {
					debug!("check_confirmations request failed: {err:?}");
				}
			};
			if now.elapsed().as_secs() > 20 {
				info!(
					"signature {} confirmed {} out of {} failed after {} ms",
					signature,
					confirmed_blocks,
					min_confirmed_blocks,
					now.elapsed().as_millis()
				);
				if confirmed_blocks > 0 {
					return Ok(confirmed_blocks);
				} else {
					return Err(RpcError::ForUser(format!(
						"signature not found after {} seconds",
						now.elapsed().as_secs()
					))
					.into());
				}
			}
			sleep(Duration::from_millis(250)).await;
		}
		Ok(confirmed_blocks)
	}

	/// Current confirmation count for `signature` (equivalent to the lockout tower depth).
	///
	/// **When**: building custom confirmation-progress UIs. Caps out at
	/// `MAX_LOCKOUT_HISTORY + 1` (32) — any signature older than that reports 32 regardless of
	/// actual depth.
	///
	/// **Why**: errors if signature is missing (`"signature not found"`) — distinct from the
	/// node returning `Some(status { confirmations: None })`, which means "finalized (beyond
	/// lockout history)" and is mapped to 32 here.
	pub async fn get_num_blocks_since_signature_confirmation(
		&self,
		signature: &Signature,
	) -> ClientResult<usize> {
		let result: Response<Vec<Option<TransactionStatus>>> = self
			.send(
				RpcRequest::GetSignatureStatuses,
				json!([[signature.to_string()]]),
			)
			.await?;

		let confirmations = result.value[0]
			.clone()
			.ok_or_else(|| {
				ClientError::new_with_request(
					ClientErrorKind::Custom("signature not found".to_string()),
					RpcRequest::GetSignatureStatuses,
				)
			})?
			.confirmations
			.unwrap_or(MAX_LOCKOUT_HISTORY + 1);
		Ok(confirmations)
	}

	/// Most-recent blockhash usable for a new transaction.
	///
	/// **When**: right before signing a transaction. Blockhashes are valid for ~150 blocks
	/// (~60s) — anything older will be rejected as expired.
	///
	/// **Why**: use [`get_latest_blockhash_with_commitment`](Self::get_latest_blockhash_with_commitment)
	/// if you also need the `last_valid_block_height` (almost always — it's what you'd compare
	/// against [`get_block_height`](Self::get_block_height) to know if you still have time).
	pub async fn get_latest_blockhash(&self) -> ClientResult<Hash> {
		let (blockhash, _) = self
			.get_latest_blockhash_with_commitment(self.commitment())
			.await?;
		Ok(blockhash)
	}

	/// Latest blockhash plus its `last_valid_block_height`.
	///
	/// **When**: building durable "send and retry until valid height" loops — the most robust
	/// way to handle mainnet congestion.
	pub async fn get_latest_blockhash_with_commitment(
		&self,
		commitment: CommitmentConfig,
	) -> ClientResult<(Hash, u64)> {
		let RpcBlockhash {
			blockhash,
			last_valid_block_height,
		} = self
			.send::<Response<RpcBlockhash>>(RpcRequest::GetLatestBlockhash, json!([commitment]))
			.await?
			.value;
		let blockhash = blockhash.parse().map_err(|_| {
			ClientError::new_with_request(
				RpcError::ParseError("Hash".to_string()).into(),
				RpcRequest::GetLatestBlockhash,
			)
		})?;
		Ok((blockhash, last_valid_block_height))
	}

	/// Cheap check whether a blockhash is still in the recent-blockhash window.
	///
	/// **When**: inside send-and-confirm retry loops to detect "blockhash has expired, stop
	/// polling" early instead of waiting for the status poll to time out.
	pub async fn is_blockhash_valid(
		&self,
		blockhash: &Hash,
		commitment: CommitmentConfig,
	) -> ClientResult<bool> {
		Ok(self
			.send::<Response<bool>>(
				RpcRequest::IsBlockhashValid,
				json!([blockhash.to_string(), commitment,]),
			)
			.await?
			.value)
	}

	/// Exact lamport fee the network would charge to process `message`.
	///
	/// **When**: fee-preview UIs ("this will cost X SOL"). The value already reflects any
	/// compute-unit-price instructions inside `message`.
	///
	/// **Why**: `None` in the response is surfaced as `Custom("Invalid blockhash")` — that's
	/// the common cause of a null fee (can't fee-price a transaction whose blockhash has
	/// expired).
	pub async fn get_fee_for_message(
		&self,
		message: &impl SerializableMessage,
	) -> ClientResult<u64> {
		let serialized_encoded = serialize_and_encode(message, UiTransactionEncoding::Base64)?;
		let result = self
			.send::<Response<Option<u64>>>(
				RpcRequest::GetFeeForMessage,
				json!([serialized_encoded, self.commitment()]),
			)
			.await?;
		result
			.value
			.ok_or_else(|| ClientErrorKind::Custom("Invalid blockhash".to_string()).into())
	}

	/// Poll up to 5s until a blockhash *different* from `blockhash` is available.
	///
	/// **When**: you want to retry a transaction but need a fresh blockhash to avoid the
	/// duplicate-transaction ledger cache.
	///
	/// **Why**: polls every ~half-slot (~200ms); errors if the blockhash hasn't rolled in 5s,
	/// which typically means the node is unhealthy.
	pub async fn get_new_latest_blockhash(&self, blockhash: &Hash) -> ClientResult<Hash> {
		let mut num_retries = 0;
		let start = Instant::now();
		while start.elapsed().as_secs() < 5 {
			if let Ok(new_blockhash) = self.get_latest_blockhash().await {
				if new_blockhash != *blockhash {
					return Ok(new_blockhash);
				}
			}
			debug!("Got same blockhash ({blockhash:?}), will retry...");

			// Retry ~twice during a slot
			sleep(Duration::from_millis(DEFAULT_MS_PER_SLOT / 2)).await;
			num_retries += 1;
		}
		Err(RpcError::ForUser(format!(
			"Unable to get new blockhash after {}ms (retried {} times), stuck at {}",
			start.elapsed().as_millis(),
			num_retries,
			blockhash
		))
		.into())
	}

	/// Low-level RPC send — the single exit point for every method above.
	///
	/// **When**: directly only by the other methods on this type. Calling it from outside is
	/// discouraged because the method-specific wrappers add argument encoding, response
	/// decoding, and error framing that you'd otherwise have to duplicate.
	///
	/// **Why**: tunnels through the host-provided `call-rpc` import (no sockets in this crate),
	/// and centralises the JSON-RPC error translation into [`ClientError`] — including the
	/// optional structured `data` payload decoding behind the `tx-debug` feature.
	pub async fn send<T: serde::de::DeserializeOwned>(
		&self,
		request: RpcRequest,
		params: Value,
	) -> ClientResult<T> {
		debug_assert!(params.is_object() || params.is_array() || params.is_null());

		let method = format!("{request}");
		match crate::call_rpc::<Value, T, Value>(&method, params) {
			Ok(Ok(v)) => Ok(v),
			Ok(Err(err)) => {
				let data = decode_response_error_data(err.code.into(), err.data.as_ref());
				Err(ClientError {
					request: Some(request),
					kind: ClientErrorKind::RpcError(RpcError::RpcResponseError {
						code: err.code.into(),
						message: err.message,
						data,
					}),
				})
			}
			Err(err) => Err(ClientError {
				request: Some(request),
				kind: ClientErrorKind::Io(err),
			}),
		}
	}
}

// Decodes the `data` payload of a JSON-RPC error response into the structured
// `RpcResponseErrorData` variants. Gated behind the `tx-debug` feature because
// deserializing `RpcSimulateTransactionResult` pulls ~50-95 KB of type-tree
// code into the binary, which is dead weight for read-only procedures. Without
// the feature, every error gets `Empty` and `Error::get_transaction_error`
// returns `None` — same behavior as before this fix existed, but now it's an
// explicit opt-out instead of a silent bug.

#[cfg(feature = "tx-debug")]
mod tx_debug {
	use super::{RpcResponseErrorData, RpcSimulateTransactionResult, Value, debug};

	// Codes mirror `solana_rpc_client::custom_error`. Duplicated here because
	// that module lives in the HTTP-using `solana-rpc-client` crate which we
	// deliberately don't depend on (we tunnel through the host's `call-rpc`
	// import instead).
	const JSON_RPC_SERVER_ERROR_SEND_TRANSACTION_PREFLIGHT_FAILURE: i64 = -32002;
	const JSON_RPC_SERVER_ERROR_NODE_UNHEALTHY: i64 = -32005;

	#[derive(serde::Deserialize)]
	struct NodeUnhealthyErrorData {
		#[serde(rename = "numSlotsBehind")]
		num_slots_behind: Option<solana_sdk::clock::Slot>,
	}

	/// Decode the free-form `data` object attached to a JSON-RPC error into the structured
	/// [`RpcResponseErrorData`] enum. Only the two error codes worth decoding are handled;
	/// everything else collapses to `Empty`.
	pub(super) fn decode_response_error_data(code: i64, data: Option<&Value>) -> RpcResponseErrorData {
		let Some(data) = data else { return RpcResponseErrorData::Empty };
		match code {
			JSON_RPC_SERVER_ERROR_SEND_TRANSACTION_PREFLIGHT_FAILURE => {
				match serde_json::from_value::<RpcSimulateTransactionResult>(data.clone()) {
					Ok(r) => RpcResponseErrorData::SendTransactionPreflightFailure(r),
					Err(err) => {
						debug!("failed to decode preflight failure data: {err}");
						RpcResponseErrorData::Empty
					}
				}
			}
			JSON_RPC_SERVER_ERROR_NODE_UNHEALTHY => {
				match serde_json::from_value::<NodeUnhealthyErrorData>(data.clone()) {
					Ok(NodeUnhealthyErrorData { num_slots_behind }) => {
						RpcResponseErrorData::NodeUnhealthy { num_slots_behind }
					}
					Err(_) => RpcResponseErrorData::Empty,
				}
			}
			_ => RpcResponseErrorData::Empty,
		}
	}
}

#[cfg(feature = "tx-debug")]
fn decode_response_error_data(code: i64, data: Option<&Value>) -> RpcResponseErrorData {
	tx_debug::decode_response_error_data(code, data)
}

#[cfg(not(feature = "tx-debug"))]
fn decode_response_error_data(_code: i64, _data: Option<&Value>) -> RpcResponseErrorData {
	RpcResponseErrorData::Empty
}

/// Serialize `input` with bincode and then encode the bytes in `encoding`.
///
/// Only `Base58` and `Base64` are accepted — the other UI encodings (`Json`, `JsonParsed`,
/// `Base64Zstd`) don't round-trip through bincode and would silently produce garbage. We reject
/// them explicitly so the caller gets a clear error.
fn serialize_and_encode<T>(input: &T, encoding: UiTransactionEncoding) -> ClientResult<String>
where
	T: serde::ser::Serialize,
{
	let serialized = bincode::serialize(input)
		.map_err(|e| ClientErrorKind::Custom(format!("Serialization failed: {e}")))?;
	let encoded = match encoding {
		UiTransactionEncoding::Base58 => bs58::encode(serialized).into_string(),
		UiTransactionEncoding::Base64 => BASE64_STANDARD.encode(serialized),
		_ => {
			return Err(ClientErrorKind::Custom(format!(
				"unsupported encoding: {encoding}. Supported encodings: base58, base64"
			))
			.into());
		}
	};
	Ok(encoded)
}

/// Parse an `RpcKeyedAccount` list into `(Pubkey, Account)` pairs, erroring on any malformed
/// pubkey or undecodable account blob. Used by
/// [`RpcClient::get_program_accounts_with_config`] — kept out of the macro set because it
/// returns a `Vec` of pairs rather than the usual `Response` envelope.
pub(crate) fn parse_keyed_accounts(
	accounts: Vec<RpcKeyedAccount>,
	request: RpcRequest,
) -> ClientResult<Vec<(Pubkey, Account)>> {
	let mut pubkey_accounts: Vec<(Pubkey, Account)> = Vec::with_capacity(accounts.len());
	for RpcKeyedAccount { pubkey, account } in accounts.into_iter() {
		let pubkey = pubkey.parse().map_err(|_| {
			ClientError::new_with_request(
				RpcError::ParseError("Pubkey".to_string()).into(),
				request,
			)
		})?;
		pubkey_accounts.push((
			pubkey,
			account.decode().ok_or_else(|| {
				ClientError::new_with_request(
					RpcError::ParseError("Account from rpc".to_string()).into(),
					request,
				)
			})?,
		));
	}
	Ok(pubkey_accounts)
}
