// https://docs.rs/wit-bindgen/latest/wit_bindgen/macro.generate.html#options-to-generate
wit_bindgen::generate!({
	path: "./src/zela.wit",
	// `Borrowing { duplicate_if_necessary: true }` produces separate `JsonParam`/`JsonResult`
	// types per direction, so calling host imports doesn't have to clone byte buffers.
	// Without duplication, `json` (used in both import and export positions) would fall
	// back to owned `Vec<u8>` everywhere, costing one alloc per host call. The original
	// comment here claimed this "breaks with async functions" — that was an older
	// wit-bindgen quirk and is no longer relevant.
	ownership: Borrowing { duplicate_if_necessary: true },
	pub_export_macro: true,
	// NOTE on async: wit-bindgen 0.57 supports `async: true` and the wasm-component-ld
	// linker accepts the async ABI now. We stay sync because (a) `zela.wit` declares no
	// async funcs, (b) flipping `async: true` requires the `async` cargo feature on
	// `wit-bindgen` plus refactoring every host-call site to `.await` (kills `poll_await`
	// in lib.rs, the manual `Stream::poll_next` in subscription.rs, and the
	// `std::thread::sleep` in rpc_client/client.rs), and (c) the host's wasmtime must be
	// configured with `async_support(true)` and re-generate its bindings against the same
	// async option — without coordinated host changes, instantiation fails with an ABI
	// mismatch. The win when it lands: cooperative `recv`, no busy loop, no thread sleeps.
});
