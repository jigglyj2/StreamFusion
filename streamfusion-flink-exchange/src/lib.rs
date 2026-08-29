// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

mod binary_row;
mod key_group;
mod native_channel;
mod partitioner;

pub use binary_row::{encode_binary_row, KeyField};
pub use key_group::{assign_key_group, binary_row_hash, flink_murmur_hash};
pub use native_channel::{native_batch_channel, NativeBatchReceiver, NativeBatchSender};
pub use partitioner::{route_batch, RoutedBatch};
