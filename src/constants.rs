//! Crate-wide constants.

/// SDK-reserved [`RpcError`](crate::RpcError) codes in the `1XXXX` range.
pub mod error_codes {
	/// Params bytes failed to deserialize into the procedure's `Params` type.
	pub const DESERIALIZE_PARAMS: i32 = 10400;
	/// `SuccessData` failed to serialize back to JSON.
	pub const SERIALIZE_SUCCESS: i32 = 10500;
	/// `ErrorData` failed to serialize back to JSON.
	pub const SERIALIZE_ERROR_DATA: i32 = 10501;
	/// Host transport error (bubbled via `From<ClientError>`).
	pub const HOST_TRANSPORT: i32 = 10502;
	/// Future exceeded [`POLL_BUDGET`] consecutive `Pending` polls.
	pub const BUSY_LOOP_BUDGET: i32 = 10508;
}

/// Max consecutive `Pending` polls before `poll_await` aborts.
pub const POLL_BUDGET: u32 = 1_000_000;
