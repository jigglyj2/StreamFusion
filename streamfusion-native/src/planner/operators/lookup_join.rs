// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

//! Test-only investigation of cached synchronous DataFusion lookup computation. This candidate
//! preserves inner-lookup values and ordering, but repeated HashJoinExec::execute calls retain
//! fresh metric descriptors. It must not become a production path until that growing workspace
//! is eliminated. The source/resource/control binding and Java-planned protobuf are also pending.

use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion::common::JoinType;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::joins::{HashJoinExecBuilder, PartitionMode};
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};

use super::envelope::{self, Envelope};

mod snapshot;
pub(crate) use snapshot::LookupSnapshotExec;

/// Build once per task and probe once per native invocation. The source must transfer its
/// snapshot buffers to DataFusion instead of retaining a second full snapshot. Its initial
/// Arrow ownership is accounted by the source edge; DataFusion then reserves its build buffers
/// and hash table against the invocation TaskContext's Flink-backed MemoryPool.
///
/// Flink's left side is DataFusion's probe/right side. Inner joins preserve probe order
/// and do not use the final build-side visited bitmap, allowing reuse of the same CollectLeft
/// plan without resetting its cached build future. Do not extend this to build-driven joins.
/// Outer lookup also needs an explicit arrival-order adaptation: DataFusion may emit unmatched
/// probe rows after matched rows when the probe has no declared ordering.
pub(crate) fn create(
    snapshot: Arc<LookupSnapshotExec>,
    probe: Arc<dyn ExecutionPlan>,
    keys: &[(usize, usize)], // (probe payload column, snapshot column)
    left_outer: bool,
) -> Result<Arc<dyn ExecutionPlan>> {
    if left_outer {
        return Err(invalid(
            "outer lookup requires a verified unmatched-row arrival-order adaptation",
        ));
    }
    let probe_schema = probe.schema();
    let side_schema = snapshot.schema();
    let envelope = Envelope::from_schema(&probe_schema)?;
    if envelope::owned_timestamp_index(&probe_schema)?.is_none() {
        return Err(invalid("lookup requires the owned native record envelope"));
    }
    if probe.output_partitioning().partition_count() != 1 {
        return Err(invalid("task-local lookup requires one probe partition"));
    }
    if keys.is_empty() {
        return Err(invalid("lookup requires at least one equality key"));
    }
    let on = keys
        .iter()
        .map(|&(probe_index, side_index)| {
            if probe_index >= envelope.payload_width || side_index >= side_schema.fields().len() {
                return Err(invalid("lookup key is outside its payload schema"));
            }
            let probe_field = probe_schema.field(probe_index);
            let side_field = side_schema.field(side_index);
            if probe_field.data_type() != side_field.data_type()
                || !matches!(
                    probe_field.data_type(),
                    DataType::Boolean
                        | DataType::Int8
                        | DataType::Int16
                        | DataType::Int32
                        | DataType::Int64
                        | DataType::Utf8
                        | DataType::Binary
                )
            {
                return Err(invalid("lookup equality requires matching boolean, signed integer, UTF-8 or binary keys"));
            }
            Ok((
                Arc::new(Column::new(side_field.name(), side_index)) as Arc<dyn PhysicalExpr>,
                Arc::new(Column::new(probe_field.name(), probe_index)) as Arc<dyn PhysicalExpr>,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let side_width = side_schema.fields().len();
    // Gather SQL columns in Flink order and carry probe RowKind, timestamp and ordinal through
    // the very same DataFusion selection. No rows or payload buffers travel through Java here.
    let projection = (side_width..side_width + envelope.payload_width)
        .chain(0..side_width)
        .chain(envelope.indices().map(|index| side_width + index))
        .collect();
    HashJoinExecBuilder::new(snapshot, probe, on, JoinType::Inner)
        .with_partition_mode(PartitionMode::CollectLeft)
        .with_projection(Some(projection))
        .build_exec()
}

fn invalid(message: &str) -> DataFusionError {
    DataFusionError::Plan(message.into())
}

#[cfg(test)]
mod tests;
