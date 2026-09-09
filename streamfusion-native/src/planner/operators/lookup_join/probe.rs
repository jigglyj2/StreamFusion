// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, RecordBatch, UInt32Array, UInt64Array};
use arrow::compute::{filter, take};
use arrow::datatypes::SchemaRef;
use datafusion::common::hash_utils::create_hashes;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::joins::join_hash_map::JoinHashMapType;

use super::LookupTable;
use crate::memory_pool::{arrow_lease, buffer_registry, buffer_size, selection};

const CANDIDATES: usize = 1024;

pub(super) struct LookupProbe {
    table: Arc<LookupTable>,
    input: RecordBatch,
    keys: Vec<usize>,
    condition: Arc<dyn PhysicalExpr>,
    key_schema: SchemaRef,
    output_schema: SchemaRef,
    payload_width: usize,
    hashes: Vec<u64>,
    probe_indices: Vec<u32>,
    side_indices: Vec<u64>,
    offset: Option<(usize, Option<u64>)>,
    _memory: MemoryReservation,
}

impl LookupProbe {
    pub(super) fn new(
        table: Arc<LookupTable>,
        input: RecordBatch,
        keys: &[usize],
        condition: Arc<dyn PhysicalExpr>,
        key_schema: SchemaRef,
        output_schema: SchemaRef,
        payload_width: usize,
    ) -> Result<Self> {
        if input.num_rows() >= u32::MAX as usize {
            return Err(DataFusionError::ResourcesExhausted(
                "lookup probe exceeds native u32 row addressing".into(),
            ));
        }
        let memory =
            MemoryConsumer::new("lookup probe hashes and candidate indices").register(&table.pool);
        memory.try_grow(selection::add(
            selection::multiply(input.num_rows(), 8)?,
            CANDIDATES * 16 + 4096,
        )?)?;
        let mut hashes = vec![0; input.num_rows()];
        create_hashes(
            keys.iter().map(|&key| input.column(key)),
            &table.random,
            &mut hashes,
        )?;
        let offset = (input.num_rows() > 0).then_some((0, None));
        Ok(Self {
            table,
            input,
            keys: keys.to_vec(),
            condition,
            key_schema,
            output_schema,
            payload_width,
            hashes,
            probe_indices: Vec::with_capacity(CANDIDATES),
            side_indices: Vec::with_capacity(CANDIDATES),
            offset,
            _memory: memory,
        })
    }

    pub(super) fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        while let Some(start) = self.offset {
            let memory = MemoryConsumer::new("lookup vectorized comparison and Arrow output")
                .register(&self.table.pool);
            let mut limit = CANDIDATES;
            let next = loop {
                let next = self.table.hashes.get_matched_indices_with_limit_offset(
                    &self.hashes,
                    None,
                    limit,
                    start,
                    &mut self.probe_indices,
                    &mut self.side_indices,
                );
                if self.probe_indices.is_empty() {
                    break next;
                }
                let allowance = self.allowance()?;
                match memory.try_resize(allowance) {
                    Ok(()) => break next,
                    Err(DataFusionError::ResourcesExhausted(_)) if limit > 1 => limit /= 2,
                    Err(error) => return Err(error),
                }
            };
            self.offset = next;
            if self.probe_indices.is_empty() {
                continue;
            }
            // Candidate enumeration is DataFusion's bounded FIFO hash-chain traversal. The
            // unchanged physical equality expressions eliminate hash collisions and NULL keys.
            let probe = UInt32Array::from(self.probe_indices.clone());
            let side = UInt64Array::from(self.side_indices.clone());
            let columns = self
                .keys
                .iter()
                .zip(&self.table.keys)
                .flat_map(|(&probe_key, &side_key)| {
                    [
                        take(self.input.column(probe_key), &probe, None),
                        take(self.table.batch.column(side_key), &side, None),
                    ]
                })
                .collect::<std::result::Result<Vec<_>, _>>()?;
            let candidates = RecordBatch::try_new(self.key_schema.clone(), columns)?;
            let mask = self
                .condition
                .evaluate(&candidates)?
                .into_array(candidates.num_rows())?;
            let mask = mask
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    DataFusionError::Internal("lookup equality did not produce Boolean".into())
                })?;
            let probe = filter(&probe, mask)?;
            let side = filter(&side, mask)?;
            if probe.is_empty() {
                continue;
            }
            let mut columns: Vec<ArrayRef> = self.input.columns()[..self.payload_width]
                .iter()
                .map(|column| take(column, probe.as_ref(), None))
                .collect::<std::result::Result<_, _>>()?;
            columns.extend(
                self.table
                    .batch
                    .columns()
                    .iter()
                    .map(|column| take(column, side.as_ref(), None))
                    .collect::<std::result::Result<Vec<_>, _>>()?,
            );
            columns.extend(
                self.input.columns()[self.payload_width..]
                    .iter()
                    .map(|column| take(column, probe.as_ref(), None))
                    .collect::<std::result::Result<Vec<_>, _>>()?,
            );
            let output = RecordBatch::try_new(self.output_schema.clone(), columns)?;
            let bytes = buffer_size::batch_bytes(&output)?;
            if bytes > memory.size() {
                return Err(DataFusionError::Internal(
                    "lookup Arrow output exceeded its admitted workspace".into(),
                ));
            }
            let output_memory = memory.split(bytes);
            return Ok(Some(arrow_lease::datafusion_batch_registered(
                output,
                output_memory,
                buffer_registry(&self.table.pool),
            )?));
        }
        Ok(None)
    }

    fn allowance(&self) -> Result<usize> {
        let mut bytes = selection::add(
            selection::fixed_allowance(self.input.columns())?,
            selection::fixed_allowance(self.table.batch.columns())?,
        )?;
        for (&probe, &side) in self.probe_indices.iter().zip(&self.side_indices) {
            bytes = selection::add(
                bytes,
                selection::row_allowance(self.input.columns(), probe as usize)?,
            )?;
            bytes = selection::add(
                bytes,
                selection::row_allowance(self.table.batch.columns(), side as usize)?,
            )?;
        }
        // Equality-key gathers overlap payload gathers, plus bounded index/mask arrays. A
        // conservative batch allowance also covers skewed/nested values without per-row JNI.
        selection::add(selection::multiply(bytes, 2)?, CANDIDATES * 32)
    }
}
