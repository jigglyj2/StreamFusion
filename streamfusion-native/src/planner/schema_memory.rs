// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Borrowed admission before lowering protobuf types to Arrow schemas and row codecs.
//! Includes nested null-row construction, whose heap size is not its protobuf wire size.

use crate::proto;
use arrow::datatypes::Field;
use arrow::row::SortField;
use datafusion::error::{DataFusionError, Result};

// Schema/type copies, sort fields, codec storage and one-element null arrays/buffers.
// Ancestor depth covers concurrently live recursive encoders. Conservative capacities,
// not allocator byte measurements; no operator-neighbor or fusion rules belong here.
const NODE_BYTES: usize =
    4 * std::mem::size_of::<Field>() + 2 * std::mem::size_of::<SortField>() + 256;

pub(crate) fn planned_schemas(
    input: Option<&proto::Schema>,
    output: Option<&proto::Schema>,
    calls: &[proto::AggregateCall],
) -> Result<usize> {
    let mut bytes = 4096usize;
    for schema in [input, output].into_iter().flatten() {
        for field in &schema.fields {
            add_name(&mut bytes, &field.name)?;
            if let Some(kind) = &field.r#type {
                add(&mut bytes, type_workspace(kind, 1)?)?;
            }
        }
    }
    for call in calls {
        for kind in [&call.input_type, &call.output_type, &call.accumulator_type]
            .into_iter()
            .flatten()
        {
            add(&mut bytes, type_workspace(kind, 1)?)?;
        }
    }
    Ok(bytes)
}

fn type_workspace(kind: &proto::LogicalType, depth: usize) -> Result<usize> {
    let mut bytes = NODE_BYTES.checked_mul(depth).ok_or_else(overflow)?;
    match &kind.r#type {
        Some(proto::logical_type::Type::Array(array)) => {
            if let Some(child) = &array.element_type {
                add(&mut bytes, type_workspace(child, depth + 1)?)?;
            }
        }
        Some(proto::logical_type::Type::Map(map)) => {
            for child in [&map.key_type, &map.value_type].into_iter().flatten() {
                add(&mut bytes, type_workspace(child, depth + 1)?)?;
            }
        }
        Some(proto::logical_type::Type::Row(row)) => {
            for field in &row.fields {
                add_name(&mut bytes, &field.name)?;
                if let Some(child) = &field.r#type {
                    add(&mut bytes, type_workspace(child, depth + 1)?)?;
                }
            }
        }
        _ => {}
    }
    Ok(bytes)
}

fn add_name(total: &mut usize, name: &str) -> Result<()> {
    add(total, name.len().checked_mul(4).ok_or_else(overflow)?)
}
fn add(total: &mut usize, bytes: usize) -> Result<()> {
    *total = total.checked_add(bytes).ok_or_else(overflow)?;
    Ok(())
}
fn overflow() -> DataFusionError {
    DataFusionError::ResourcesExhausted("native planned schema admission overflow".into())
}
