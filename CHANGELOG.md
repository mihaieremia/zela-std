# Changelog

Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versioning: [SemVer](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-04-21

Breaking release. Solana family upgraded from 2.2 to 3.x. Vendored RPC types
replaced with upstream re-exports. Leaner wasm, faster compile, hardened guest.

### Breaking

- Solana family bumped from 2.2 to 3.x.
- RPC client error surface trimmed (no more signer/transport conversions —
  procedures can't produce those anyway).
- Removed deprecated `RpcClient::set_node_version`.
- `SubscriptionStream` now yields `Result<T, IoError>` instead of `T`.

### Added

- `solana` feature (default on). Disable for a leaner non-Solana wasm.
- `SimpleCustomProcedure` trait for procedures without structured errors.
- `RpcError` now implements `Debug`, `Clone`, `Display`, and `std::error::Error`.
- `error_codes` module for stable SDK-emitted error codes.
- Runaway-future guard: `poll_await` aborts after a bounded number of pending polls.
- `[profile.release]` tuned for `wasm32-wasip2` (copy the block into
  procedure workspaces — Cargo doesn't propagate profiles through deps).
- Package metadata: description, repository, license, `rust-version = "1.85"`.

### Changed

- Crate-root re-exports narrowed to client-building essentials.
- Internal cleanup: `Subscription` extracted to its own module, RPC method
  bodies deduped via macros.

### Fixed

- `get_transaction_error` adapted to upstream's current type shape.

### Metrics

| | Before | After | Δ |
|---|---|---|---|
| Source LOC | 3,892 | 2,153 | −44.7% |
| Release wasm (default) | 186 KB | 118 KB | −36% |
| Release wasm (no-default) | 140 KB | 93 KB | −33% |
| Release compile (default) | ~28 s | ~11 s | −62% |
| Release compile (no-default) | ~10 s | ~2 s | −76% |

## [0.1.0] - Initial release

- `CustomProcedure` trait + `zela_custom_procedure!` macro.
- `call_rpc` and `Subscription` over the WIT host interface.
- Vendored `solana-rpc-client` / `solana-pubsub-client` clone (Solana 2.2).

[0.2.0]: https://github.com/Zela-io/zela-std/releases/tag/v0.2.0
[0.1.0]: https://github.com/Zela-io/zela-std/releases/tag/v0.1.0
