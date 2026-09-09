// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use arrow::array::{RecordBatch, RecordBatchReader};
use arrow::compute::concat_batches;
use datafusion::error::Result;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool};

use super::{table::invalid, LookupTable};
use crate::memory_pool::{arrow_lease, buffer_registry, buffer_size, selection};

impl LookupTable {
    /// Drain a producer-budgeted finite source at task open. C Data release callbacks keep
    /// its buffers charged until we drop them. A single batch stays zero-copy; multiple source
    /// chunks are consolidated once for DataFusion's flat build-row addressing, as in its hash
    /// join build. This is cache construction, never an intermediate native batch handoff.
    pub(crate) fn from_source(
        mut source: impl RecordBatchReader,
        keys: Vec<usize>,
        pool: Arc<dyn MemoryPool>,
    ) -> Result<Arc<Self>> {
        let schema = source.schema();
        if keys.is_empty() || keys.iter().any(|&key| key >= schema.fields().len()) {
            return Err(invalid("lookup source requires in-range equality keys"));
        }
        if keys
            .iter()
            .any(|&key| !super::table::supported_key(schema.field(key).data_type()))
        {
            return Err(invalid("unsupported lookup source equality key type"));
        }
        let metadata = MemoryConsumer::new("lookup source batch descriptors").register(&pool);
        let per_batch = selection::add(
            schema
                .fields()
                .iter()
                .try_fold(4096, |bytes, field| selection::add(bytes, field.size()))?,
            selection::multiply(schema.fields().len(), 256)?,
        )?;
        let mut batches = Vec::new();
        let mut rows = 0usize;
        let mut bytes = 0usize;
        loop {
            metadata.try_resize(selection::add(
                4096,
                selection::multiply(batches.len() + 1, per_batch)?,
            )?)?;
            let Some(batch) = source.next() else { break };
            let batch = batch?;
            if batch.schema() != schema {
                return Err(invalid("lookup source changed its snapshot schema"));
            }
            if batch.num_rows() == 0 {
                continue;
            }
            rows = selection::add(rows, batch.num_rows())?;
            if rows >= u32::MAX as usize {
                return Err(invalid("lookup source exceeds native u32 row addressing"));
            }
            bytes = selection::add(bytes, batch.get_array_memory_size())?;
            batches.push(arrow_lease::borrowed_batch(batch, buffer_registry(&pool))?);
        }
        // Close file and C Stream owners before building the table. Array owners survive this.
        drop(source);
        let batch = match batches.len() {
            0 => RecordBatch::new_empty(schema),
            1 => batches.pop().unwrap(),
            _ => {
                let output = MemoryConsumer::new("lookup snapshot consolidation").register(&pool);
                output.try_grow(selection::add(selection::multiply(bytes, 2)?, 64 * 1024)?)?;
                let batch = concat_batches(&schema, &batches)?;
                let size = buffer_size::batch_bytes(&batch)?;
                if size > output.size() {
                    return Err(invalid(
                        "lookup snapshot consolidation exceeded admitted workspace",
                    ));
                }
                // Free input chunks before returning unused construction credit.
                drop(batches);
                output.try_resize(size)?;
                arrow_lease::datafusion_batch_registered(batch, output, buffer_registry(&pool))?
            }
        };
        Self::new(batch, keys, pool)
    }
}
