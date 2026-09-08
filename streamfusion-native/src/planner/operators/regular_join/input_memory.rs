// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

/// One coarse allowance for row encoding, equality keys and the batch lookup directory.
/// Input Arrow buffers retain their producer's ownership and reservation. Flat columns
/// may share a much larger IPC allocation; only their logical spans are encoded here.
pub(super) fn workspace(batch: &RecordBatch, visible: usize) -> Result<usize> {
    // The unique-key table is dropped before the manifest and staged-state directories
    // are built. Budget their largest overlapping phase, rather than adding both peaks.
    // 512 bytes per input row covers the directory/vector headers conservatively even
    // when every row has a distinct key; payload bytes have their separate allowance.
    let mut bytes = batch.num_rows().saturating_mul(512).saturating_add(4096);
    for column in &batch.columns()[..visible] {
        let data_type = column.data_type();
        let flat = data_type.is_primitive()
            || matches!(
                data_type,
                DataType::Null
                    | DataType::Boolean
                    | DataType::Utf8
                    | DataType::LargeUtf8
                    | DataType::Binary
                    | DataType::LargeBinary
                    | DataType::FixedSizeBinary(_)
            );
        let payload = if flat {
            column.to_data().get_slice_memory_size()?
        } else {
            // Preserve the existing conservative estimate for nested/dictionary/view
            // encodings, whose expansion is not bounded by flat logical buffer spans.
            column.get_array_memory_size()
        };
        // Row encoding adds validity markers and string block padding. Include per-field
        // headroom for wide schemas of short/nullable values as well as payload overlap.
        bytes = bytes
            .saturating_add(payload.saturating_mul(4))
            .saturating_add(batch.num_rows().saturating_mul(32));
    }
    Ok(bytes)
}
