// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

//! DataFusion owns bounded matching and outer/semi/anti semantics. Flink's keyed multiset has
//! already applied changelog retractions and equality partitioning. A nullable synthetic key
//! preserves the per-key Flink null-filter decision, including mixed null-safe key comparisons.
use super::*;
use arrow::array::UInt64Array;
use datafusion::common::JoinType;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::{
    context::{SessionConfig, SessionContext},
    runtime_env::RuntimeEnvBuilder,
    TaskContext,
};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::joins::{utils::JoinFilter, HashJoinExecBuilder, PartitionMode};
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use futures::StreamExt;

pub(super) struct JoinRuntime {
    runtime: tokio::runtime::Runtime,
    context: Arc<TaskContext>,
}
impl JoinRuntime {
    pub(super) fn new(owner: &HostMemoryReservation) -> Result<Self> {
        let pool = owner.datafusion_pool(owner.available_capacity()?.unwrap_or(64 << 20));
        let env = RuntimeEnvBuilder::new()
            .with_memory_pool(pool)
            .build_arc()?;
        let context = SessionContext::new_with_config_rt(
            SessionConfig::new().with_batch_size(BOUNDED_EDGE_OUTPUT_MAX_ROWS),
            env,
        )
        .task_ctx();
        Ok(Self {
            runtime: tokio::runtime::Builder::new_current_thread().build()?,
            context,
        })
    }
}
pub(super) struct JoinStream {
    stream: SendableRecordBatchStream,
    runtime: Arc<JoinRuntime>,
    batch: Option<RecordBatch>,
    position: usize,
    _input_memory: HostMemoryReservation,
    _output_memory: HostMemoryReservation,
}
impl JoinStream {
    pub(super) fn new(
        join: &RegularJoinProcessor,
        state: &JoinState,
        left: &[bool],
        right: &[bool],
        runtime: Arc<JoinRuntime>,
    ) -> Result<Self> {
        let mut input_memory = join
            .scratch_reservation
            .sibling("DataFusion bounded join input and row identities");
        let rows = state.left.len().saturating_add(state.right.len());
        let predicate_bytes = if join.residual_condition.is_some() {
            state
                .left
                .iter()
                .chain(&state.right)
                .fold(0usize, |n, row| {
                    n.saturating_add(row.row.len().saturating_mul(8))
                })
                .saturating_add(
                    join.visible_schemas
                        .iter()
                        .map(|s| s.fields().len().saturating_mul(256))
                        .sum::<usize>(),
                )
        } else {
            0
        };
        // A predicate-free join consumes only key/identity vectors. Its payload stays in the
        // existing state owner; reserving another payload copy here rejects valid wide joins.
        input_memory.resize(
            rows.saturating_mul(64)
                .saturating_add(predicate_bytes)
                .saturating_add(4096),
        )?;
        let mut inputs = Vec::<Arc<dyn ExecutionPlan>>::new();
        let mut widths = Vec::new();
        for (side, (rows, matchable)) in [(&state.left, left), (&state.right, right)]
            .into_iter()
            .enumerate()
        {
            let mut fields = Vec::new();
            let mut columns = Vec::new();
            if join.residual_condition.is_some() {
                let parser = join.row_converters[side].parser();
                columns = join.row_converters[side]
                    .convert_rows(rows.iter().map(|r| parser.parse(&r.row)))?;
                fields.extend(
                    join.visible_schemas[side]
                        .fields()
                        .iter()
                        .map(|f| f.as_ref().clone()),
                );
            }
            widths.push(columns.len());
            columns.push(Arc::new(Int32Array::from_iter(
                matchable.iter().map(|&v| v.then_some(0)),
            )) as ArrayRef);
            fields.push(Field::new("match_key", DataType::Int32, true));
            columns.push(Arc::new(UInt64Array::from_iter_values(0..rows.len() as u64)) as ArrayRef);
            fields.push(Field::new("row_id", DataType::UInt64, false));
            let schema = Arc::new(Schema::new(fields));
            let batch = RecordBatch::try_new(schema.clone(), columns)?;
            inputs.push(MemorySourceConfig::try_new_exec(
                &[vec![batch]],
                schema,
                None,
            )?);
        }
        let join_type = match join.join_type {
            proto::RegularJoinType::Inner => JoinType::Inner,
            proto::RegularJoinType::Left => JoinType::Left,
            proto::RegularJoinType::Right => JoinType::Right,
            proto::RegularJoinType::Full => JoinType::Full,
            proto::RegularJoinType::Semi => JoinType::LeftSemi,
            proto::RegularJoinType::Anti => JoinType::LeftAnti,
            _ => {
                return Err(DataFusionError::Plan(
                    "unsupported bounded join type".into(),
                ))
            }
        };
        let filter = join.residual_condition.as_ref().map(|expr| {
            JoinFilter::new(
                expr.clone(),
                JoinFilter::build_column_indices(
                    (0..widths[0]).collect(),
                    (0..widths[1]).collect(),
                ),
                join.condition_schema.clone(),
            )
        });
        let mut projection = vec![widths[0] + 1];
        if !matches!(join_type, JoinType::LeftSemi | JoinType::LeftAnti) {
            projection.push(widths[0] + 2 + widths[1] + 1);
        }
        let plan = HashJoinExecBuilder::new(
            inputs.remove(0),
            inputs.remove(0),
            vec![(
                Arc::new(Column::new("match_key", widths[0])),
                Arc::new(Column::new("match_key", widths[1])),
            )],
            join_type,
        )
        .with_partition_mode(PartitionMode::CollectLeft)
        .with_filter(filter)
        .with_projection(Some(projection))
        .build_exec()?;
        let mut output_memory = join
            .scratch_reservation
            .sibling("DataFusion bounded join output identities");
        output_memory.resize(BOUNDED_EDGE_OUTPUT_MAX_ROWS * 32 + 4096)?;
        let stream = {
            let _entered = runtime.runtime.enter();
            plan.execute(0, runtime.context.clone())?
        };
        Ok(Self {
            stream,
            runtime,
            batch: None,
            position: 0,
            _input_memory: input_memory,
            _output_memory: output_memory,
        })
    }
    pub(super) fn peek(&mut self) -> Result<Option<(Option<usize>, Option<usize>)>> {
        while self
            .batch
            .as_ref()
            .is_none_or(|batch| self.position >= batch.num_rows())
        {
            self.batch = self
                .runtime
                .runtime
                .block_on(self.stream.next())
                .transpose()?;
            self.position = 0;
            if self.batch.is_none() {
                return Ok(None);
            }
        }
        let batch = self.batch.as_ref().unwrap();
        let index = |column: usize| -> Result<Option<usize>> {
            if column >= batch.num_columns() || batch.column(column).is_null(self.position) {
                return Ok(None);
            }
            let values = batch
                .column(column)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| {
                    DataFusionError::Internal("DataFusion join row identity is not u64".into())
                })?;
            Ok(Some(usize::try_from(values.value(self.position)).map_err(
                |_| DataFusionError::Execution("join identity exceeds usize".into()),
            )?))
        };
        Ok(Some((index(0)?, index(1)?)))
    }
    pub(super) fn advance(&mut self) {
        self.position += 1;
    }
}
