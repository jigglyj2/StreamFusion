# Pinned RocksDB bindings

This source subset comes from rust-rocksdb 0.25.0, upstream commit
`e4aac69f55021e619566b6ca5902665e0ea2af4e`:
https://github.com/rust-rocksdb/rust-rocksdb/tree/e4aac69f55021e619566b6ca5902665e0ea2af4e

The two native source submodules match that release exactly:

- RocksDB 11.8.1: `abeebd9630f11bd08c28b7bd43c7bdfc62050654`.
- Snappy: `23b3286820105438c5dbb9bc22f1bb85c5812c8a`.

Initialize them with `git submodule update --init` from the StreamFusion checkout.
Normal CI initializes them during checkout. These dependencies belong only to the
optional native RocksDB state module; the core DataFusion runtime does not depend on them.

The upstream Rust wrapper and sys crate sources, build script, source list, build-version
metadata, support headers, manifests and license are retained. Upstream examples and development test suites
are omitted. The retained local integration test exercises the cache-priority extension.
The manifests disable publishing, omit the sys README reference and omit the upstream
standalone workspace declaration. No upstream execution algorithms are changed.

## Local extension

`cache-priority.patch` records the functional changes against the pinned upstream revision.
Apply this zero-context patch with `git apply --unidiff-zero` only to the pinned revision.
The optional `cache-priority` feature adds a validated Rust setter and a C API setter for
`LRUCacheOptions.high_pri_pool_ratio`. Flink defaults to 0.1; upstream RocksDB defaults to 0.5,
and the unmodified C/Rust bindings do not expose this setting.

The extended C API compiles the pinned upstream `db/c.cc` plus the small setter in one
translation unit, replacing the ordinary `db/c.cc` build entry. It therefore uses the actual
upstream opaque-handle definition and emits each C API symbol exactly once. It does not copy
private struct layouts, inspect Rust wrapper layouts, or interpose symbols at runtime.
The feature requires the bundled native source build; stock prebuilt RocksDB libraries lack
the extra symbol. Existing safe cache/WBM ownership and lifetime handling remain upstream.

Remove this source subset once an upstream release exposes the equivalent setter. When
upgrading, update both native submodule pins, refresh the source subset, rebase the recorded
patch and rerun the option, cache-sharing, memory, checkpoint and Java recovery tests. The
root LICENSE covers the Rust wrapper; native dependency licenses remain in their submodules.
