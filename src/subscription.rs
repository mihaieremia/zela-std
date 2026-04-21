//! Host subscription + `futures_util::Stream` adapter.

use std::io::Error as IoError;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use serde::{Serialize, de::DeserializeOwned};

use crate::sys::rockawayx::zela::zela_host as host;
use crate::{RpcError, decode_rpc_error, encode_params, invalid_input};

pub struct Subscription(host::RpcSubscription);

impl Subscription {
	pub fn new<P: Serialize, Re: DeserializeOwned>(
		method: &str,
		params: P,
	) -> Result<Result<Self, RpcError<Re>>, IoError> {
		let params = encode_params(&params)?;
		match host::RpcSubscription::subscribe(method, &params) {
			Ok(Ok(v)) => Ok(Ok(Self(v))),
			Ok(Err(err)) => Ok(Err(decode_rpc_error(err)?)),
			Err(err) => Err(IoError::from_raw_os_error(err as _)),
		}
	}

	pub fn recv<N: DeserializeOwned>(&self) -> Result<N, IoError> {
		match self.0.recv() {
			Ok(v) => serde_json::from_slice(&v).map_err(invalid_input),
			Err(err) => Err(IoError::from_raw_os_error(err as _)),
		}
	}

	/// Adapt to a [`futures_util::Stream`]. Yields `Some(Err(_))` on parse or
	/// receive failure, `None` once the host closes the subscription.
	pub fn into_stream<T: DeserializeOwned>(self) -> SubscriptionStream<T> {
		SubscriptionStream {
			subscription: self,
			done: false,
			_marker: PhantomData,
		}
	}
}

pub struct SubscriptionStream<T: DeserializeOwned> {
	subscription: Subscription,
	done: bool,
	_marker: PhantomData<fn() -> T>,
}

impl<T: DeserializeOwned> futures_util::Stream for SubscriptionStream<T> {
	type Item = Result<T, IoError>;

	fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		if self.done {
			return Poll::Ready(None);
		}
		match self.subscription.recv::<T>() {
			Ok(v) => Poll::Ready(Some(Ok(v))),
			Err(err) => {
				// Surface the error once, then end the stream.
				self.done = true;
				Poll::Ready(Some(Err(err)))
			}
		}
	}
}
