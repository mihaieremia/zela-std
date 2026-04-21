//! # zela-std — guest SDK for Zela custom procedures
//!
//! WIT bindings + a typed Solana RPC client that tunnels through the host's
//! `call_rpc` import. Implement [`CustomProcedure`] and export with
//! [`zela_custom_procedure!`].
//!
//! ```ignore
//! use zela_std::*;
//!
//! pub struct Echo;
//! impl CustomProcedure for Echo {
//!     type Params = JsonValue;
//!     type SuccessData = JsonValue;
//!     type ErrorData = ();
//!     async fn run(p: Self::Params) -> Result<Self::SuccessData, RpcError<Self::ErrorData>> {
//!         Ok(p)
//!     }
//! }
//! zela_custom_procedure!(Echo);
//! ```
//!
//! Disable the default `solana` feature to drop the RPC client for a leaner
//! wasm. SDK-emitted errors use the codes in [`error_codes`].

use std::io::{Error as IoError, ErrorKind};
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use serde::{Serialize, de::DeserializeOwned};
pub use serde_json::{self, Value as JsonValue, json};

use crate::constants::POLL_BUDGET;
use crate::sys::rockawayx::zela::types::RpcError as HostRpcError;
use crate::sys::rockawayx::zela::zela_host as host;

pub mod constants;
pub mod subscription;
#[cfg(feature = "solana")]
pub mod rpc_client;
pub mod sys;

// Crate-root surface: what `use zela_std::*` gives a procedure author.
// Internal helpers, `Subscription`, `POLL_BUDGET` stay behind their modules.

pub use constants::error_codes;

#[cfg(feature = "solana")]
pub use rpc_client::{ClientError, ClientResult, PubsubClient, RpcClient};
#[cfg(feature = "solana")]
pub use solana_sdk;
#[cfg(feature = "solana")]
pub use solana_commitment_config::CommitmentConfig;
#[cfg(feature = "solana")]
pub use solana_sdk::{hash::Hash, pubkey::Pubkey, signature::Signature};

// Host imports block, so busy-polling is the only way to make progress.
// Bounded by POLL_BUDGET so a runaway future can't hang the executor.
fn poll_await<F: Future>(f: F) -> Result<F::Output, BusyLoopBudgetExceeded> {
	let mut fut = pin!(f);
	let mut ctx = Context::from_waker(Waker::noop());
	for _ in 0..POLL_BUDGET {
		if let Poll::Ready(r) = Future::poll(fut.as_mut(), &mut ctx) {
			return Ok(r);
		}
	}
	Err(BusyLoopBudgetExceeded)
}

#[derive(Debug)]
struct BusyLoopBudgetExceeded;

/// Error emitted by a procedure or by the SDK. See [`error_codes`] for the
/// `1XXXX` SDK-reserved range.
#[derive(Debug, Clone)]
pub struct RpcError<E = JsonValue> {
	pub code: i32,
	pub message: String,
	pub data: Option<E>,
}

impl<E> std::fmt::Display for RpcError<E> {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "RPC error {}: {}", self.code, self.message)
	}
}

impl<E: std::fmt::Debug> std::error::Error for RpcError<E> {}

/// High-level trait every procedure implements. Pair with
/// [`zela_custom_procedure!`] to export the guest ABI. Procedures without
/// structured errors can use [`SimpleCustomProcedure`] instead.
#[allow(async_fn_in_trait)] // single-threaded wasm, we consume the future
pub trait CustomProcedure {
	type Params: DeserializeOwned;
	type SuccessData: Serialize;
	/// Payload for the `data` field of [`RpcError`]. Use `()` when absent.
	type ErrorData: Serialize;

	/// Called once per incoming RPC. `async` is for ergonomics only — host
	/// imports don't yield, so blocking is the only way to make progress.
	async fn run(params: Self::Params) -> Result<Self::SuccessData, RpcError<Self::ErrorData>>;

	/// Records above this level are dropped before the `log` WIT import.
	const LOG_MAX_LEVEL: log::LevelFilter = log::LevelFilter::Info;

	fn log_fmt(record: &log::Record) -> String {
		format!("{} {}: {}", record.level(), record.target(), record.args())
	}
}

/// [`CustomProcedure`] with `ErrorData = ()` baked in.
#[allow(async_fn_in_trait)]
pub trait SimpleCustomProcedure {
	type Params: DeserializeOwned;
	type SuccessData: Serialize;

	async fn run(params: Self::Params) -> Result<Self::SuccessData, RpcError>;

	const LOG_MAX_LEVEL: log::LevelFilter = log::LevelFilter::Info;

	fn log_fmt(record: &log::Record) -> String {
		format!("{} {}: {}", record.level(), record.target(), record.args())
	}
}

impl<T: SimpleCustomProcedure> CustomProcedure for T {
	type Params = <T as SimpleCustomProcedure>::Params;
	type SuccessData = <T as SimpleCustomProcedure>::SuccessData;
	type ErrorData = JsonValue;

	async fn run(params: Self::Params) -> Result<Self::SuccessData, RpcError<Self::ErrorData>> {
		<T as SimpleCustomProcedure>::run(params).await
	}

	const LOG_MAX_LEVEL: log::LevelFilter = <T as SimpleCustomProcedure>::LOG_MAX_LEVEL;

	fn log_fmt(record: &log::Record) -> String {
		<T as SimpleCustomProcedure>::log_fmt(record)
	}
}

struct CustomProcedureLog {
	fmt: fn(&log::Record) -> String,
}
impl log::Log for CustomProcedureLog {
	fn enabled(&self, _: &log::Metadata) -> bool {
		true
	}
	fn log(&self, record: &log::Record) {
		host::log(&(self.fmt)(record))
	}
	fn flush(&self) {}
}

// `sys::Guest` can't carry the per-impl logger constant, hence this shim.
trait CustomProcedureInternal {
	const LOGGER: CustomProcedureLog;
	fn init();
}
impl<T: CustomProcedure> CustomProcedureInternal for T {
	const LOGGER: CustomProcedureLog = CustomProcedureLog { fmt: T::log_fmt };
	fn init() {
		log::set_logger(&Self::LOGGER).unwrap();
		log::set_max_level(T::LOG_MAX_LEVEL);
	}
}

// Internal helpers

pub(crate) fn invalid_input(
	err: impl Into<Box<dyn std::error::Error + Send + Sync>>,
) -> IoError {
	IoError::new(ErrorKind::InvalidInput, err)
}

pub(crate) fn encode_params<P: Serialize>(params: &P) -> Result<Vec<u8>, IoError> {
	serde_json::to_vec(params).map_err(invalid_input)
}

/// Host-side `RpcError` (bytes in `data`) → typed [`RpcError`].
pub(crate) fn decode_rpc_error<E: DeserializeOwned>(
	err: HostRpcError,
) -> Result<RpcError<E>, IoError> {
	let HostRpcError {
		code,
		message,
		data,
	} = err;
	let data = data
		.map(|v| serde_json::from_slice(&v).map_err(invalid_input))
		.transpose()?;
	Ok(RpcError {
		code,
		message,
		data,
	})
}

fn sys_err(code: i32, message: String) -> HostRpcError {
	HostRpcError {
		code,
		message,
		data: None,
	}
}

// Guest wiring. One `CustomProcedure` per wasm module; `zela_custom_procedure!` picks it.
static INIT_CALLED: std::sync::Once = std::sync::Once::new();

impl<T: CustomProcedure> sys::Guest for T {
	fn run(params: sys::JsonResult) -> Result<sys::JsonResult, sys::RpcError> {
		INIT_CALLED.call_once(<T as CustomProcedureInternal>::init);

		let params: T::Params = serde_json::from_slice(&params).map_err(|err| {
			sys_err(
				error_codes::DESERIALIZE_PARAMS,
				format!("Failed to deserialize params: {err}"),
			)
		})?;

		let result = poll_await(Self::run(params)).map_err(|BusyLoopBudgetExceeded| {
			log::error!("poll_await exceeded POLL_BUDGET ({POLL_BUDGET})");
			sys_err(
				error_codes::BUSY_LOOP_BUDGET,
				format!("procedure exceeded busy-loop budget of {POLL_BUDGET} Pending polls"),
			)
		})?;

		match result {
			Ok(v) => serde_json::to_vec(&v).map_err(|err| {
				sys_err(
					error_codes::SERIALIZE_SUCCESS,
					format!("Failed to serialize success data: {err}"),
				)
			}),
			Err(RpcError {
				code,
				message,
				data,
			}) => {
				let data = data
					.map(|v| serde_json::to_vec(&v))
					.transpose()
					.map_err(|err| {
						sys_err(
							error_codes::SERIALIZE_ERROR_DATA,
							format!("Failed to serialize error data: {err}"),
						)
					})?;
				Err(HostRpcError {
					code,
					message,
					data,
				})
			}
		}
	}
}

/// Raw JSON-RPC call through the host. Two-layer return matches the WIT ABI:
/// outer `Err(IoError)` is transport; inner `Err(RpcError<Re>)` is a
/// server-returned RPC error; `Ok(Ok(_))` is success. For Solana methods use
/// [`rpc_client::RpcClient`], which flattens both layers.
pub fn call_rpc<P: Serialize, Rs: DeserializeOwned, Re: DeserializeOwned>(
	method: &str,
	params: P,
) -> Result<Result<Rs, RpcError<Re>>, IoError> {
	let params = encode_params(&params)?;
	match host::call_rpc(method, &params) {
		Ok(Ok(v)) => Ok(Ok(serde_json::from_slice(&v).map_err(invalid_input)?)),
		Ok(Err(err)) => Ok(Err(decode_rpc_error(err)?)),
		Err(err) => Err(IoError::from_raw_os_error(err as _)),
	}
}

#[cfg(feature = "solana")]
impl<T> From<rpc_client::ClientError> for RpcError<T> {
	fn from(value: rpc_client::ClientError) -> Self {
		RpcError {
			code: error_codes::HOST_TRANSPORT,
			message: value.to_string(),
			data: None,
		}
	}
}

/// Export a [`CustomProcedure`] as the wasm guest's `run` entrypoint. Call
/// exactly once at the crate root of every procedure.
#[macro_export]
macro_rules! zela_custom_procedure {
	( $name: ident ) => {
		$crate::sys::export!($name with_types_in $crate::sys);
	};
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn poll_await_returns_ready_immediately() {
		let fut = std::future::ready(42u32);
		assert_eq!(poll_await(fut).unwrap(), 42);
	}

	#[test]
	fn poll_await_aborts_on_busy_loop() {
		struct AlwaysPending;
		impl Future for AlwaysPending {
			type Output = ();
			fn poll(
				self: std::pin::Pin<&mut Self>,
				_: &mut std::task::Context<'_>,
			) -> std::task::Poll<Self::Output> {
				std::task::Poll::Pending
			}
		}
		assert!(poll_await(AlwaysPending).is_err());
	}

	#[test]
	fn poll_await_tolerates_some_pending() {
		struct PendingThenReady(u32);
		impl Future for PendingThenReady {
			type Output = u32;
			fn poll(
				mut self: std::pin::Pin<&mut Self>,
				_: &mut std::task::Context<'_>,
			) -> std::task::Poll<Self::Output> {
				if self.0 == 0 {
					std::task::Poll::Ready(99)
				} else {
					self.0 -= 1;
					std::task::Poll::Pending
				}
			}
		}
		assert_eq!(poll_await(PendingThenReady(10)).unwrap(), 99);
	}
}
