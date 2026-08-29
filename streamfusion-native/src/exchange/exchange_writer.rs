// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use std::io::Write;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::error::{ArrowError, Result};
use arrow::record_batch::RecordBatch;

use super::{route_batch, IpcExchangeWriter, KeyField};

/// Persistent Arrow writers for every downstream Flink subtask of a hash exchange.
pub struct HashExchangeWriter<W: Write> {
    key_fields: Vec<(usize, KeyField)>,
    max_parallelism: u32,
    channels: Vec<IpcExchangeWriter<W>>,
}

impl<W: Write> HashExchangeWriter<W> {
    pub fn new(
        outputs: Vec<W>,
        schema: SchemaRef,
        key_fields: Vec<(usize, KeyField)>,
        max_parallelism: u32,
    ) -> Result<Self> {
        if outputs.is_empty() || outputs.len() > max_parallelism as usize {
            return Err(ArrowError::InvalidArgumentError(format!(
                "exchange output count {} must be within 1..={max_parallelism}",
                outputs.len()
            )));
        }
        let channels = outputs
            .into_iter()
            .map(|output| IpcExchangeWriter::new(output, Arc::clone(&schema)))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            key_fields,
            max_parallelism,
            channels,
        })
    }

    /// Routes one input batch and writes each non-empty destination in stable row order.
    pub fn write(&mut self, batch: RecordBatch) -> Result<()> {
        let routed = route_batch(
            batch,
            &self.key_fields,
            self.max_parallelism,
            self.channels.len() as u32,
        )?;
        for destination in routed {
            self.channels[destination.destination() as usize].write(&destination.materialize()?)?;
        }
        Ok(())
    }

    /// Flushes data before Flink emits an ordered watermark, status, or checkpoint barrier.
    pub fn flush_before_control_event(&mut self) -> Result<()> {
        for channel in &mut self.channels {
            channel.flush()?;
        }
        Ok(())
    }

    pub fn finish(self) -> Result<Vec<W>> {
        self.channels
            .into_iter()
            .map(IpcExchangeWriter::finish)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use arrow::array::{ArrayRef, Int32Array, Int8Array};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;
    use crate::exchange::IpcExchangeReader;

    #[test]
    fn keeps_persistent_destination_streams_across_input_batches() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int32, false),
            Field::new("__streamfusion_row_kind", DataType::Int8, false),
        ]));
        let mut writer = HashExchangeWriter::new(
            vec![Vec::new(), Vec::new(), Vec::new(), Vec::new()],
            Arc::clone(&schema),
            vec![(0, KeyField::Integer)],
            128,
        )
        .unwrap();
        writer.write(batch(&schema, &[1, 42], &[0, 3])).unwrap();
        writer.write(batch(&schema, &[0, -1], &[2, 1])).unwrap();
        writer.flush_before_control_event().unwrap();

        let outputs = writer.finish().unwrap();
        let destination_two = IpcExchangeReader::new(Cursor::new(outputs[2].as_slice()))
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        let destination_three = IpcExchangeReader::new(Cursor::new(outputs[3].as_slice()))
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();

        assert_eq!(destination_two.len(), 1);
        assert_eq!(destination_two[0], batch(&schema, &[1, 42], &[0, 3]));
        assert_eq!(destination_three.len(), 1);
        assert_eq!(destination_three[0], batch(&schema, &[0, -1], &[2, 1]));
    }

    fn batch(schema: &SchemaRef, keys: &[i32], row_kinds: &[i8]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(Int32Array::from(keys.to_vec())) as ArrayRef,
                Arc::new(Int8Array::from(row_kinds.to_vec())) as ArrayRef,
            ],
        )
        .unwrap()
    }
}
