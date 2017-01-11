# Future aware MPSC channel

This crate provides a future aware, bounded, MPSC channel with back pressure
support.

This crate is an exploration of features before they are merged into the
[futures](https://github.com/alexcrichton/futures-rs/) crate.

## Usage

First, add this to your `Cargo.toml`:

```toml
[dependencies]
futures-mpsc = "0.1"
```

Next, add this to your crate:

```rust
extern crate futures_mpsc;
```

# License

`futures-mpsc` is primarily distributed under the terms of both the MIT license
and the Apache License (Version 2.0), with portions covered by various BSD-like
licenses.

See LICENSE-APACHE, and LICENSE-MIT for details.
