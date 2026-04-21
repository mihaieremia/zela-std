use solana_rpc_client_types::{request, response};
use solana_sdk::transaction::TransactionError;
use std::io;

/// What kind of failure the [`Error`] wraps.
#[derive(thiserror::Error, Debug)]
pub enum ErrorKind {
	#[error(transparent)]
	Io(#[from] io::Error),
	#[error(transparent)]
	RpcError(#[from] request::RpcError),
	#[error(transparent)]
	SerdeJson(#[from] serde_json::error::Error),
	#[error(transparent)]
	TransactionError(#[from] TransactionError),
	#[error("Custom: {0}")]
	Custom(String),
}

impl ErrorKind {
	pub fn get_transaction_error(&self) -> Option<TransactionError> {
		match self {
			Self::TransactionError(tx_err) => Some(tx_err.clone()),
			Self::RpcError(request::RpcError::RpcResponseError {
				data:
					request::RpcResponseErrorData::SendTransactionPreflightFailure(
						response::RpcSimulateTransactionResult { err: Some(tx_err), .. },
					),
				..
			}) => Some(tx_err.clone().into()),
			_ => None,
		}
	}
}

#[derive(thiserror::Error, Debug)]
#[error("{kind}")]
pub struct Error {
	pub request: Option<request::RpcRequest>,

	#[source]
	pub kind: ErrorKind,
}

impl Error {
	pub fn new_with_request(kind: ErrorKind, request: request::RpcRequest) -> Self {
		Self {
			request: Some(request),
			kind,
		}
	}

	pub fn into_with_request(self, request: request::RpcRequest) -> Self {
		Self {
			request: Some(request),
			..self
		}
	}

	pub fn request(&self) -> Option<&request::RpcRequest> {
		self.request.as_ref()
	}

	pub fn kind(&self) -> &ErrorKind {
		&self.kind
	}

	pub fn get_transaction_error(&self) -> Option<TransactionError> {
		self.kind.get_transaction_error()
	}
}

// `From<X> for Error` for every X that already has `From<X> for ErrorKind`.
macro_rules! from_via_kind {
	($($src:ty),* $(,)?) => { $(
		impl From<$src> for Error {
			fn from(err: $src) -> Self {
				Self { request: None, kind: err.into() }
			}
		}
	)* };
}

from_via_kind!(
	ErrorKind,
	io::Error,
	request::RpcError,
	serde_json::error::Error,
	TransactionError,
);

pub type Result<T> = std::result::Result<T, Error>;
