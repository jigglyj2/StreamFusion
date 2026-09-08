// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use datafusion::physical_expr::expressions::Literal;
use datafusion::scalar::ScalarValue;

pub(crate) fn evaluate(
    inner: &Arc<dyn PhysicalExpr>,
    batch: &RecordBatch,
    pool: &Arc<dyn MemoryPool>,
) -> Result<ColumnarValue> {
    // Literal::evaluate clones its ScalarValue, including large strings/binary values.
    // The immutable cached literal can feed the identical DF broadcast kernel by reference.
    if let Some(literal) = inner.downcast_ref::<Literal>() {
        return scalar(literal.value(), batch.num_rows(), pool);
    }
    match inner.evaluate(batch)? {
        value @ ColumnarValue::Array(_) => Ok(value),
        ColumnarValue::Scalar(value) => scalar(&value, batch.num_rows(), pool),
    }
}

pub(super) fn scalar(
    value: &ScalarValue,
    rows: usize,
    pool: &Arc<dyn MemoryPool>,
) -> Result<ColumnarValue> {
    let memory = MemoryConsumer::new("native projection scalar broadcast").register(pool);
    memory.try_grow(workspace(value, rows)?)?;
    let result = value.to_array_of_size(rows)?;
    let retained = result.get_array_memory_size();
    if retained > memory.size() {
        return Err(DataFusionError::ResourcesExhausted(
            "projection scalar output exceeded its admitted workspace".into(),
        ));
    }
    memory.try_resize(retained)?;
    Ok(ColumnarValue::Array(
        crate::memory_pool::arrow_lease::datafusion_array_registered(
            result,
            memory,
            crate::memory_pool::buffer_registry(pool),
        )?,
    ))
}

fn workspace(value: &ScalarValue, rows: usize) -> Result<usize> {
    let width = match value.data_type() {
        DataType::Null => Some(0),
        DataType::Boolean => Some(1),
        data_type => data_type.primitive_width(),
    };
    if let Some(width) = width {
        // Primitive broadcasts allocate Arrow values and validity, not one Rust
        // ScalarValue enum per row. Keep coarse builder/array overlap and headroom.
        return width
            .checked_add(1)
            .and_then(|bytes| bytes.checked_mul(rows.max(1)))
            .and_then(|bytes| bytes.checked_mul(4))
            .and_then(|bytes| bytes.checked_add(64 * 1024))
            .ok_or_else(|| {
                DataFusionError::ResourcesExhausted(
                    "projection scalar broadcast size overflow".into(),
                )
            });
    }
    // ScalarValue::size walks nested Arrow storage without constructing ArrayData.
    // FixedSizeBinary(NULL) is the exception: its declared width allocates bytes even
    // though the scalar stores no payload. Include that width explicitly.
    let per_value = match value {
        ScalarValue::FixedSizeBinary(width, _) => usize::try_from(*width)
            .ok()
            .and_then(|width| value.size().checked_add(width)),
        _ => Some(value.size()),
    };
    // Include nested gather/builder overlap, offsets, validity, temporary indices,
    // alignment and descriptors. This is capacity admission, not a measured byte count.
    per_value
        .and_then(|bytes| bytes.checked_add(64))
        .and_then(|bytes| bytes.checked_mul(rows.max(1)))
        .and_then(|bytes| bytes.checked_mul(4))
        .and_then(|bytes| bytes.checked_add(64 * 1024))
        .ok_or_else(|| {
            DataFusionError::ResourcesExhausted("projection scalar broadcast size overflow".into())
        })
}

#[cfg(test)]
mod tests;
