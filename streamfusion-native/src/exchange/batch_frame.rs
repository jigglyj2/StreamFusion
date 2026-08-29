// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use std::collections::HashMap;

use arrow::buffer::Buffer;
use arrow::datatypes::SchemaRef;
use arrow::error::{ArrowError, Result};
use arrow::ipc::reader::read_record_batch;
use arrow::ipc::root_as_message;
use arrow::ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteContext, IpcWriteOptions};
use arrow::record_batch::RecordBatch;

/// One schema-free Arrow IPC record-batch frame carried by a Flink network record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IpcBatchFrame {
    pub metadata: Vec<u8>,
    pub body: Vec<u8>,
}

impl IpcBatchFrame {
    pub fn encode(batch: &RecordBatch) -> Result<Self> {
        let mut dictionaries = DictionaryTracker::new(true);
        let mut context = IpcWriteContext::default();
        let (encoded_dictionaries, encoded_batch) = IpcDataGenerator::default().encode(
            batch,
            &mut dictionaries,
            &IpcWriteOptions::default(),
            &mut context,
        )?;
        if !encoded_dictionaries.is_empty() {
            return Err(ArrowError::NotYetImplemented(
                "dictionary-encoded native exchange requires per-channel dictionary state"
                    .to_string(),
            ));
        }
        Ok(Self {
            metadata: encoded_batch.ipc_message,
            body: encoded_batch.arrow_data,
        })
    }

    pub fn decode(&self, schema: SchemaRef) -> Result<RecordBatch> {
        let message = root_as_message(&self.metadata).map_err(|error| {
            ArrowError::ParseError(format!("invalid exchange IPC metadata: {error}"))
        })?;
        let batch = message.header_as_record_batch().ok_or_else(|| {
            ArrowError::ParseError("exchange IPC frame is not a record batch".to_string())
        })?;
        read_record_batch(
            &Buffer::from(self.body.clone()),
            batch,
            schema,
            &HashMap::new(),
            None,
            &message.version(),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int32Array, Int8Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    #[test]
    fn round_trips_without_embedding_the_schema() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
            Field::new("__streamfusion_row_kind", DataType::Int8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 42])) as ArrayRef,
                Arc::new(StringArray::from(vec!["one", "forty-two"])) as ArrayRef,
                Arc::new(Int8Array::from(vec![0, 3])) as ArrayRef,
            ],
        )
        .unwrap();

        let frame = IpcBatchFrame::encode(&batch).unwrap();
        let decoded = frame.decode(schema).unwrap();

        assert_eq!(decoded, batch);
        assert!(!frame.metadata.windows(3).any(|bytes| bytes == b"key"));
        assert!(!frame.metadata.windows(5).any(|bytes| bytes == b"value"));
    }

    #[test]
    fn requires_the_negotiated_schema_to_decode() {
        let batch = RecordBatch::try_from_iter(vec![(
            "key",
            Arc::new(Int32Array::from(vec![1])) as ArrayRef,
        )])
        .unwrap();
        let frame = IpcBatchFrame::encode(&batch).unwrap();
        let wrong_schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, false)]));

        assert!(frame.decode(wrong_schema).is_err());
    }
}
