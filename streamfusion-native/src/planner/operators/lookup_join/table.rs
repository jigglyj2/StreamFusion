// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::DataType;
use datafusion::common::{hash_utils::RandomState, NullEquality};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion::physical_expr::{expressions::Column, PhysicalExpr};
use datafusion::physical_plan::joins::join_hash_map::JoinHashMapU32;
use datafusion::physical_plan::joins::utils::update_hash;

use crate::memory_pool::{arrow_lease, buffer_registry, selection};

/// Immutable lookup state. The native task-open source binding owns file semantics; this
/// object retains only its Arrow snapshot and the unchanged DataFusion join hash table.
pub(crate) struct LookupTable {
    pub(super) batch: RecordBatch,
    pub(super) keys: Vec<usize>,
    pub(super) hashes: JoinHashMapU32,
    pub(super) random: RandomState,
    pub(super) pool: Arc<dyn MemoryPool>,
    _hash_memory: MemoryReservation,
}

impl LookupTable {
    /// The caller has already admitted the source buffers before allocation/import. Reconcile
    /// their existing leases without copying or charging shared buffers a second time.
    pub(crate) fn new(
        batch: RecordBatch,
        keys: Vec<usize>,
        pool: Arc<dyn MemoryPool>,
    ) -> Result<Arc<Self>> {
        if keys.is_empty() || keys.iter().any(|&i| i >= batch.num_columns()) {
            return Err(invalid("lookup snapshot requires in-range equality keys"));
        }
        if batch.num_rows() >= u32::MAX as usize {
            return Err(DataFusionError::ResourcesExhausted(
                "lookup snapshot exceeds native u32 row addressing".into(),
            ));
        }
        if keys
            .iter()
            .any(|&i| !supported_key(batch.schema().field(i).data_type()))
        {
            return Err(invalid(
                "lookup equality requires boolean, signed integer, UTF-8 or binary keys",
            ));
        }
        let batch = arrow_lease::edge_batch(
            batch,
            MemoryConsumer::new("lookup snapshot Arrow ownership").register(&pool),
            buffer_registry(&pool),
        )?;
        let memory = MemoryConsumer::new("lookup DataFusion hash table").register(&pool);
        // HashTable growth/load factor, control bytes, u32 duplicate links, and build hashes.
        // Coarse reservation precedes either allocation; no per-entry allocator callbacks.
        memory.try_grow(selection::add(
            selection::multiply(batch.num_rows(), 64)?,
            64 * 1024,
        )?)?;
        let mut hashes = JoinHashMapU32::with_capacity(batch.num_rows());
        let random = RandomState::default();
        let expressions = keys
            .iter()
            .map(|&i| {
                Arc::new(Column::new(batch.schema().field(i).name(), i)) as Arc<dyn PhysicalExpr>
            })
            .collect::<Vec<_>>();
        let mut scratch = vec![0; batch.num_rows()];
        update_hash(
            &expressions,
            &batch,
            &mut hashes,
            0,
            &random,
            &mut scratch,
            0,
            true,
            NullEquality::NullEqualsNothing,
        )?;
        drop(scratch);
        Ok(Arc::new(Self {
            batch,
            keys,
            hashes,
            random,
            pool,
            _hash_memory: memory,
        }))
    }
}

pub(super) fn supported_key(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Utf8
            | DataType::Binary
    )
}

pub(super) fn invalid(message: &str) -> DataFusionError {
    DataFusionError::Plan(message.into())
}
