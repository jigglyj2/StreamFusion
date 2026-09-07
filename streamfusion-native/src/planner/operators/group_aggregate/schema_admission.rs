// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Admission before converting compact protobuf types to Arrow schemas and row codecs.
//! In particular, a STRUCT codec constructs child null arrays and an encoded null row.
//! Budget these construction temporaries as well as retained fields/codecs; don't infer
//! their size from protobuf wire length. Traversal borrows the already-admitted plan.

use super::*;

pub(in crate::planner::operators) use crate::planner::schema_memory::planned_schemas;

pub(super) fn planned_workspace(plan: &proto::GroupAggregate) -> Result<usize> {
    planned_schemas(
        plan.input_schema.as_ref(),
        plan.output_schema.as_ref(),
        &plan.aggregate_calls,
    )
}

pub(in crate::planner::operators) fn retained_workspace(
    calls: &[Call],
    converter: Option<&RowConverter>,
    schema: Option<&SchemaRef>,
) -> Result<usize> {
    let mut bytes = 2048usize;
    if let Some(converter) = converter {
        add(&mut bytes, converter.size())?;
    }
    if let Some(schema) = schema {
        for field in schema.fields() {
            add(&mut bytes, field.size())?;
        }
    }
    for call in calls {
        add(&mut bytes, std::mem::size_of::<Call>())?;
        add(&mut bytes, call.output_type.size())?;
        if let Some(kind) = &call.input_type {
            add(&mut bytes, kind.size())?;
        }
    }
    // Arrow's recursive size reporters include payload/vector capacities, but not every
    // Arc header or allocation rounding. Keep slack; shared children may be over-counted.
    bytes.checked_mul(2).ok_or_else(overflow)
}

fn add(total: &mut usize, bytes: usize) -> Result<()> {
    *total = total.checked_add(bytes).ok_or_else(overflow)?;
    Ok(())
}

fn overflow() -> DataFusionError {
    DataFusionError::ResourcesExhausted("group aggregate planned schema admission overflow".into())
}
