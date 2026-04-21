// `result_large_err` / `large_enum_variant`: upstream `RpcError` is big; boxing
// breaks pattern matching on `ErrorKind::RpcError(...)` downstream.
#![allow(clippy::collapsible_if, clippy::collapsible_match)]
#![allow(clippy::result_large_err, clippy::large_enum_variant)]
#![allow(mismatched_lifetime_syntaxes)]

mod client;
mod error;
mod pubsub;
mod util;

pub use solana_rpc_client_types::config::*;
pub use solana_rpc_client_types::filter::*;
pub use solana_rpc_client_types::request::{RpcError as ClientRpcError, RpcRequest};
pub use solana_rpc_client_types::response::*;
pub use solana_rpc_client_types::{config, filter, request, response};

use solana_rpc_client_types::request::*;

pub use client::*;
pub use error::{Error as ClientError, ErrorKind as ClientErrorKind, Result as ClientResult};
pub use futures_util::{Stream, StreamExt};
pub use pubsub::{PubsubClient, PubsubClientError};
use util::*;

/// `Result` whose success carries a `Response<T>` (context + value).
pub type RpcResult<T> = ClientResult<Response<T>>;
