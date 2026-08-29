// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::error::{ArrowError, Result};
use arrow::record_batch::RecordBatch;

/// Creates an in-process native exchange channel with one schema handshake.
///
/// Moving a RecordBatch through this channel moves only its lightweight batch metadata and
/// reference-counted arrays. The Arrow buffers and negotiated Schema are retained, not encoded.
pub fn native_batch_channel(
    schema: SchemaRef,
    capacity: usize,
) -> (NativeBatchSender, NativeBatchReceiver) {
    let (sender, receiver) = sync_channel(capacity);
    (
        NativeBatchSender {
            schema: Arc::clone(&schema),
            sender,
        },
        NativeBatchReceiver { schema, receiver },
    )
}

#[derive(Debug)]
pub struct NativeBatchSender {
    schema: SchemaRef,
    sender: SyncSender<RecordBatch>,
}

impl NativeBatchSender {
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn send(&self, batch: RecordBatch) -> Result<()> {
        if batch.schema().as_ref() != self.schema.as_ref() {
            return Err(ArrowError::SchemaError(
                "native exchange batch does not match its negotiated schema".to_string(),
            ));
        }
        self.sender.send(batch).map_err(|_| {
            ArrowError::IoError(
                "native exchange receiver closed".to_string(),
                std::io::Error::new(std::io::ErrorKind::BrokenPipe, "receiver closed"),
            )
        })
    }
}

#[derive(Debug)]
pub struct NativeBatchReceiver {
    schema: SchemaRef,
    receiver: Receiver<RecordBatch>,
}

impl NativeBatchReceiver {
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn receive(&self) -> Result<RecordBatch> {
        self.receiver.recv().map_err(|_| {
            ArrowError::IoError(
                "native exchange sender closed".to_string(),
                std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "sender closed"),
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    #[test]
    fn negotiates_schema_once_and_moves_arrow_buffers_without_copying() {
        let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int32, false)]));
        let values = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let values_pointer = values.values().as_ptr();
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![values]).unwrap();
        let (sender, receiver) = native_batch_channel(Arc::clone(&schema), 1);

        assert!(Arc::ptr_eq(sender.schema(), receiver.schema()));
        assert!(Arc::ptr_eq(receiver.schema(), &schema));
        sender.send(batch).unwrap();
        let received = receiver.receive().unwrap();

        assert_eq!(
            received
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .as_ptr(),
            values_pointer
        );
    }

    #[test]
    fn rejects_schema_drift_before_sending_a_batch() {
        let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int32, false)]));
        let drifted = Arc::new(Schema::new(vec![Field::new(
            "other",
            DataType::Int32,
            false,
        )]));
        let batch =
            RecordBatch::try_new(drifted, vec![Arc::new(Int32Array::from(vec![1]))]).unwrap();
        let (sender, _) = native_batch_channel(schema, 1);

        assert!(sender
            .send(batch)
            .unwrap_err()
            .to_string()
            .contains("negotiated schema"));
    }
}
