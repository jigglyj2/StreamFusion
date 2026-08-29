// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

mod batch_frame;
mod binary_row;
mod control;
mod exchange_framer;
mod exchange_writer;
mod key_group;
mod native_channel;
mod network_stream;
mod partitioner;
mod record_metadata;

pub use batch_frame::IpcBatchFrame;
pub use binary_row::{encode_binary_row, KeyField};
pub use control::{decode_exchange_plan, exchange_key_fields};
pub use exchange_framer::{frame_hash_exchange_batch, RoutedFrame};
pub use exchange_writer::HashExchangeWriter;
pub use key_group::{assign_key_group, binary_row_hash, flink_murmur_hash};
pub use native_channel::{native_batch_channel, NativeBatchReceiver, NativeBatchSender};
pub use network_stream::{IpcExchangeReader, IpcExchangeWriter};
pub use partitioner::{route_batch, route_batch_by_key_group, KeyGroupBatch, RoutedBatch};
pub use record_metadata::{
    attach_record_metadata, ROW_KIND_COLUMN, STREAM_RECORD_TIMESTAMP_COLUMN,
};
