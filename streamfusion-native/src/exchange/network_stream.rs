// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use std::io::{Read, Write};
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::error::{ArrowError, Result};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

/// Arrow IPC streaming writer used after Flink has selected a network destination.
///
/// The channel schema is written exactly once by `new`; subsequent calls emit dictionary and
/// record-batch messages only. Flink control events are not encoded here: the Flink network stack
/// orders barriers, watermarks, and statuses around flushed data frames.
pub struct IpcExchangeWriter<W: Write> {
    schema: SchemaRef,
    writer: StreamWriter<W>,
}

impl<W: Write> IpcExchangeWriter<W> {
    pub fn new(writer: W, schema: SchemaRef) -> Result<Self> {
        let stream = StreamWriter::try_new(writer, schema.as_ref())?;
        Ok(Self {
            schema,
            writer: stream,
        })
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.schema().as_ref() != self.schema.as_ref() {
            return Err(ArrowError::SchemaError(
                "network exchange batch does not match its channel schema".to_string(),
            ));
        }
        self.writer.write(batch)
    }

    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush()
    }

    pub fn finish(self) -> Result<W> {
        self.writer.into_inner()
    }
}

/// Reads the schema handshake once, then yields the channel's Arrow record batches.
pub struct IpcExchangeReader<R: Read> {
    reader: StreamReader<R>,
}

impl<R: Read> IpcExchangeReader<R> {
    pub fn new(reader: R) -> Result<Self> {
        Ok(Self {
            reader: StreamReader::try_new(reader, None)?,
        })
    }

    pub fn schema(&self) -> SchemaRef {
        self.reader.schema()
    }
}

impl<R: Read> Iterator for IpcExchangeReader<R> {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.reader.next()
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use arrow::array::{ArrayRef, Int32Array, Int8Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    #[test]
    fn negotiates_one_schema_then_streams_multiple_batches() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
        ]));
        let first = batch(&schema, &[1, 2], &["one", "two"]);
        let second = batch(&schema, &[3], &["three"]);
        let mut writer = IpcExchangeWriter::new(Vec::new(), Arc::clone(&schema)).unwrap();
        writer.write(&first).unwrap();
        writer.write(&second).unwrap();
        let encoded = writer.finish().unwrap();

        let mut reader = IpcExchangeReader::new(Cursor::new(encoded)).unwrap();
        assert_eq!(reader.schema().as_ref(), schema.as_ref());
        let decoded = reader.by_ref().collect::<Result<Vec<_>>>().unwrap();

        assert_eq!(decoded, vec![first, second]);
    }

    #[test]
    fn rejects_schema_drift_without_starting_another_handshake() {
        let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int32, false)]));
        let drifted = Arc::new(Schema::new(vec![Field::new(
            "other",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            drifted,
            vec![Arc::new(Int32Array::from(vec![1])) as ArrayRef],
        )
        .unwrap();
        let mut writer = IpcExchangeWriter::new(Vec::new(), schema).unwrap();

        assert!(writer
            .write(&batch)
            .unwrap_err()
            .to_string()
            .contains("channel schema"));
    }

    #[test]
    fn routes_and_streams_destination_batches_with_one_schema() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
            Field::new("__streamfusion_row_kind", DataType::Int8, false),
        ]));
        let input = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![-1, 0, 1, 42])) as ArrayRef,
                Arc::new(StringArray::from(vec!["minus", "zero", "one", "forty-two"])) as ArrayRef,
                Arc::new(Int8Array::from(vec![1, 2, 0, 3])) as ArrayRef,
            ],
        )
        .unwrap();
        let routed =
            crate::exchange::route_batch(input, &[(0, crate::exchange::KeyField::Integer)], 128, 4)
                .unwrap();

        for destination in routed {
            let expected = destination.materialize().unwrap();
            let mut writer = IpcExchangeWriter::new(Vec::new(), Arc::clone(&schema)).unwrap();
            writer.write(&expected).unwrap();
            let bytes = writer.finish().unwrap();
            let decoded = IpcExchangeReader::new(Cursor::new(bytes))
                .unwrap()
                .collect::<Result<Vec<_>>>()
                .unwrap();

            assert_eq!(decoded, vec![expected]);
        }
    }

    fn batch(schema: &SchemaRef, keys: &[i32], values: &[&str]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(Int32Array::from(keys.to_vec())) as ArrayRef,
                Arc::new(StringArray::from(values.to_vec())) as ArrayRef,
            ],
        )
        .unwrap()
    }
}
