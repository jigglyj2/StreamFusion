// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

mod binary_row;
mod control;
mod key_group;
mod native_channel;
mod network_stream;
mod partitioner;

pub use binary_row::{encode_binary_row, KeyField};
pub use control::decode_exchange_plan;
pub use key_group::{assign_key_group, binary_row_hash, flink_murmur_hash};
pub use native_channel::{native_batch_channel, NativeBatchReceiver, NativeBatchSender};
pub use network_stream::{IpcExchangeReader, IpcExchangeWriter};
pub use partitioner::{route_batch, RoutedBatch};
