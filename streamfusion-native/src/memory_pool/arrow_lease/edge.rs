// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use std::collections::BTreeMap;

/// Move a kernel's already admitted workspace into output owners, charging shared storage
/// once. The caller must admit the kernel workspace before evaluation and retain enough
/// workspace for any raw buffers still held inside the producing stream.
pub(crate) fn host_edge_batch(
    batch: RecordBatch,
    workspace: &mut HostMemoryReservation,
    registry: &Arc<Registry>,
) -> Result<RecordBatch> {
    let (columns, bytes) = annotate(&batch, registry)?;
    let memory = match workspace.split(bytes, "native retained output") {
        Ok(memory) => memory,
        Err(error) => {
            drop(batch);
            return Err(error);
        }
    };
    let schema = batch.schema();
    let rows = batch.num_rows();
    drop(batch);
    let memory = Arc::new(AssertUnwindSafe(Reservation::Host(memory)));
    let columns = columns
        .into_iter()
        .map(|column| make_array(column.retain(&memory, registry)))
        .collect();
    Ok(RecordBatch::try_new_with_options(
        schema,
        columns,
        &arrow::array::RecordBatchOptions::new().with_row_count(Some(rows)),
    )?)
}

/// Charge only storage not already covered by a retained task-local buffer owner.
/// This is ownership reconciliation at the C Stream edge, not kernel admission.
pub(crate) fn edge_batch(
    batch: RecordBatch,
    memory: MemoryReservation,
    registry: Option<Arc<Registry>>,
) -> Result<RecordBatch> {
    let Some(registry) = registry else {
        if let Err(error) = memory.try_grow(super::super::buffer_size::batch_bytes(&batch)?) {
            drop(batch);
            return Err(error);
        }
        return datafusion_batch(batch, memory);
    };
    let admitted = prepare(&batch, &memory, &registry);
    let columns = match admitted {
        Ok(columns) => columns,
        Err(error) => {
            drop(batch);
            return Err(error);
        }
    };
    let schema = batch.schema();
    let rows = batch.num_rows();
    drop(batch);
    let memory = Arc::new(AssertUnwindSafe(Reservation::DataFusion(memory)));
    let columns = columns
        .into_iter()
        .map(|column| make_array(column.retain(&memory, &registry)))
        .collect();
    Ok(RecordBatch::try_new_with_options(
        schema,
        columns,
        &arrow::array::RecordBatchOptions::new().with_row_count(Some(rows)),
    )?)
}

fn prepare(
    batch: &RecordBatch,
    memory: &MemoryReservation,
    registry: &Registry,
) -> Result<Vec<Annotated>> {
    let (columns, bytes) = annotate(batch, registry)?;
    memory.try_grow(bytes)?;
    Ok(columns)
}

fn annotate(batch: &RecordBatch, registry: &Registry) -> Result<(Vec<Annotated>, usize)> {
    // Bounded ownership descriptors do not need separate memory reservations.
    let mut uncovered = BTreeMap::new();
    let columns = batch
        .columns()
        .iter()
        .map(|array| Annotated::new(array.to_data(), registry, &mut uncovered))
        .collect::<Result<Vec<_>>>()?;
    let bytes = uncovered
        .values()
        .try_fold(0usize, |bytes, size| bytes.checked_add(*size))
        .ok_or_else(|| {
            DataFusionError::ResourcesExhausted("native unowned output size overflow".into())
        })?;
    Ok((columns, bytes))
}

struct Annotated {
    data: ArrayData,
    owners: Vec<Option<Arc<BufferOwner>>>,
    null_owner: Option<Arc<BufferOwner>>,
    children: Vec<Annotated>,
}
impl Annotated {
    fn new(
        data: ArrayData,
        registry: &Registry,
        uncovered: &mut BTreeMap<usize, usize>,
    ) -> Result<Self> {
        fn owner(
            buffer: &Buffer,
            registry: &Registry,
            uncovered: &mut BTreeMap<usize, usize>,
        ) -> Result<Option<Arc<BufferOwner>>> {
            let owner = registry.find(buffer);
            if owner.is_none() {
                let size = buffer.capacity().max(buffer.len());
                let prior = uncovered
                    .entry(buffer.data_ptr().as_ptr() as usize)
                    .or_default();
                *prior = (*prior).max(size);
            }
            Ok(owner)
        }
        let owners = data
            .buffers()
            .iter()
            .map(|buffer| owner(buffer, registry, uncovered))
            .collect::<Result<Vec<_>>>()?;
        let null_owner = data
            .nulls()
            .map(|nulls| owner(nulls.inner().inner(), registry, uncovered))
            .transpose()?
            .flatten();
        let children = data
            .child_data()
            .iter()
            .cloned()
            .map(|child| Self::new(child, registry, uncovered))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            data,
            owners,
            null_owner,
            children,
        })
    }

    fn retain(
        self,
        memory: &Arc<AssertUnwindSafe<Reservation>>,
        registry: &Arc<Registry>,
    ) -> ArrayData {
        let buffers = self
            .data
            .buffers()
            .iter()
            .cloned()
            .zip(self.owners)
            .map(|(buffer, owner)| retain_buffer(buffer, memory, Some(registry), owner))
            .collect();
        let nulls = self.data.nulls().map(|nulls| {
            let bits = nulls.inner();
            let buffer = BooleanBuffer::new(
                retain_buffer(
                    bits.inner().clone(),
                    memory,
                    Some(registry),
                    self.null_owner,
                ),
                bits.offset(),
                bits.len(),
            );
            // SAFETY: same bitmap, bit offset, length, and known null count.
            unsafe { NullBuffer::new_unchecked(buffer, nulls.null_count()) }
        });
        let children = self
            .children
            .into_iter()
            .map(|child| child.retain(memory, registry))
            .collect();
        // SAFETY: only immutable buffer owners/descriptors change, never data or offsets.
        unsafe {
            self.data
                .into_builder()
                .buffers(buffers)
                .nulls(nulls)
                .child_data(children)
                .build_unchecked()
        }
    }
}
