// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::collections::VecDeque;
use std::fs::File;
use std::path::Path;
use std::sync::{Arc, Mutex};

use arrow::array::{Array, Int8Array, UInt32Array, UInt64Array};
use arrow::compute::{take, SortOptions};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::ipc::{reader::StreamReader, writer::StreamWriter};
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::{runtime_env::RuntimeEnvBuilder, TaskContext};
use datafusion::physical_expr::{expressions::Column, LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::StreamExt;

use crate::memory_pool::HostMemoryReservation;
use crate::proto;
use crate::state::KeyedState;

const PAGE_ROWS: usize = 1024;

/// One-shot source that releases admitted cached pages as DataFusion consumes them.
struct SortInput {
    schema: SchemaRef,
    source: Mutex<Option<Source>>,
}

impl std::fmt::Debug for SortInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoundedSortStateInput")
            .finish_non_exhaustive()
    }
}

enum Source {
    Memory(VecDeque<RecordBatch>, HostMemoryReservation),
    File(
        StreamReader<File>,
        tempfile::NamedTempFile,
        HostMemoryReservation,
    ),
}

impl Iterator for Source {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Memory(batches, reservation) => batches.pop_front().map(|batch| {
                reservation.resize(
                    reservation
                        .size()
                        .saturating_sub(batch.get_array_memory_size()),
                )?;
                Ok(batch)
            }),
            Self::File(reader, _owner, _workspace) => {
                reader.next().map(|batch| batch.map_err(Into::into))
            }
        }
    }
}

impl PartitionStream for SortInput {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _: Arc<TaskContext>) -> SendableRecordBatchStream {
        let source = self.source.lock().unwrap().take();
        let stream: Box<dyn Iterator<Item = Result<RecordBatch>> + Send> = match source {
            Some(source) => Box::new(source),
            None => Box::new(std::iter::once(Err(DataFusionError::Execution(
                "bounded sort input was already consumed".to_string(),
            )))),
        };
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            futures::stream::iter(stream),
        ))
    }
}

/// DataFusion owns sorting, merge fan-in and spill files; Flink owns its memory budget.
/// Counts remain compressed until the final Arrow output batch is requested.
pub(super) struct SpillableSort {
    stream: SendableRecordBatchStream,
    runtime: tokio::runtime::Runtime,
    plan: Arc<dyn ExecutionPlan>,
    _context: Arc<TaskContext>,
    _control: HostMemoryReservation,
    current: Option<RecordBatch>,
    current_reservation: HostMemoryReservation,
    position: usize,
    emitted: u64,
    skip: u64,
    remaining: Option<u64>,
    input_spills: u64,
    input_spill_bytes: u64,
}

impl SpillableSort {
    pub(super) fn prepare(
        state: &dyn KeyedState,
        groups: std::ops::RangeInclusive<u32>,
        visible_schema: SchemaRef,
        converter: &arrow_row::RowConverter,
        plan: &proto::BoundedSort,
        owner: &HostMemoryReservation,
        directory: &Path,
    ) -> Result<Self> {
        let capacity = owner.available_capacity()?.unwrap_or(64 << 20);
        let page_bytes = (capacity / 64).min(1 << 20).max(1024);
        let cache_limit = capacity / 4;
        let mut workspace = owner.sibling("bounded sort state scan and Arrow conversion");
        workspace.resize(page_bytes.saturating_mul(8))?;
        let mut cache = owner.sibling("bounded sort cached Arrow input");
        let mut control = owner.sibling("bounded sort DataFusion runtime and plan");
        control.resize(32 << 10)?;
        let mut fields = visible_schema.fields().to_vec();
        fields.push(Arc::new(Field::new(
            "__streamfusion_sort_count",
            DataType::UInt64,
            false,
        )));
        let schema = Arc::new(Schema::new(fields));
        let mut batches = VecDeque::new();
        let mut spill: Option<(tempfile::NamedTempFile, StreamWriter<File>)> = None;
        for group in groups {
            state.visit_key_group(group, PAGE_ROWS, page_bytes, &mut |entries| {
                let parser = converter.parser();
                for (key, _) in entries {
                    if key.first() != Some(&super::STATE_KEY_PREFIX) {
                        return Err(DataFusionError::Execution(
                            "bounded sort state contains an unknown namespace".to_string(),
                        ));
                    }
                }
                let mut columns = converter
                    .convert_rows(entries.iter().map(|(key, _)| parser.parse(&key[1..])))?;
                let counts = entries
                    .iter()
                    .map(|(_, value)| super::decode_count(value))
                    .collect::<Result<Vec<_>>>()?;
                columns.push(Arc::new(UInt64Array::from(counts)));
                let batch = RecordBatch::try_new(Arc::clone(&schema), columns)?;
                let bytes = batch.get_array_memory_size();
                if spill.is_none() && cache.size().saturating_add(bytes) <= cache_limit {
                    cache.try_grow(bytes)?;
                    batches.push_back(batch);
                } else {
                    if spill.is_none() {
                        let file = tempfile::Builder::new()
                            .prefix("streamfusion-sort-input-")
                            .tempfile_in(directory)?;
                        let mut writer = StreamWriter::try_new(file.reopen()?, schema.as_ref())?;
                        for retained in batches.drain(..) {
                            writer.write(&retained)?;
                        }
                        cache.resize(0)?;
                        spill = Some((file, writer));
                    }
                    spill.as_mut().unwrap().1.write(&batch)?;
                }
                Ok(())
            })?;
        }
        let (source, input_spills, input_spill_bytes) = if let Some((file, mut writer)) = spill {
            writer.finish()?;
            drop(writer);
            let bytes = file.as_file().metadata()?.len();
            let reader = StreamReader::try_new(file.reopen()?, None)?;
            (Source::File(reader, file, workspace), 1, bytes)
        } else {
            drop(workspace);
            (Source::Memory(batches, cache), 0, 0)
        };
        let pool = owner.datafusion_pool(capacity);
        let runtime_env = RuntimeEnvBuilder::new()
            .with_memory_pool(pool)
            .with_temp_file_path(directory)
            .with_max_spill_merge_fan_in(8)
            .build_arc()?;
        let mut config = SessionConfig::new().with_batch_size(PAGE_ROWS);
        config.options_mut().execution.sort_spill_reservation_bytes = (capacity / 8).min(10 << 20);
        config.options_mut().execution.sort_in_place_threshold_bytes = page_bytes;
        let context = SessionContext::new_with_config_rt(config, runtime_env).task_ctx();
        let source = Arc::new(SortInput {
            schema: Arc::clone(&schema),
            source: Mutex::new(Some(source)),
        });
        let input = Arc::new(StreamingTableExec::try_new(
            schema,
            vec![source],
            None,
            [],
            false,
            None,
        )?);
        let ordering = LexOrdering::new(
            plan.sort_key_indices
                .iter()
                .zip(&plan.sort_ascending)
                .zip(&plan.sort_nulls_last)
                .map(|((&index, &ascending), &nulls_last)| {
                    PhysicalSortExpr::new(
                        Arc::new(Column::new(
                            visible_schema.field(index as usize).name(),
                            index as usize,
                        )),
                        SortOptions {
                            descending: !ascending,
                            // SortSpec specifies final null placement independently of direction.
                            nulls_first: !nulls_last,
                        },
                    )
                }),
        )
        .ok_or_else(|| DataFusionError::Plan("bounded sort requires ordering keys".to_string()))?;
        let sort: Arc<dyn ExecutionPlan> = Arc::new(SortExec::new(ordering, input));
        let runtime = tokio::runtime::Builder::new_current_thread().build()?;
        let stream = {
            let _entered = runtime.enter();
            sort.execute(0, Arc::clone(&context))?
        };
        Ok(Self {
            stream,
            runtime,
            plan: sort,
            _context: context,
            _control: control,
            current: None,
            current_reservation: owner.sibling("bounded sort current output page"),
            position: 0,
            emitted: 0,
            skip: plan.limit_start.unwrap_or(0),
            remaining: plan
                .limit_end
                .zip(plan.limit_start)
                .map(|(end, start)| end - start),
            input_spills,
            input_spill_bytes,
        })
    }

    pub(super) fn spill_statistics(&self) -> (u64, u64) {
        let metrics = self.plan.metrics();
        (
            self.input_spills + metrics.as_ref().and_then(|m| m.spill_count()).unwrap_or(0) as u64,
            self.input_spill_bytes
                + metrics
                    .as_ref()
                    .and_then(|m| m.spilled_bytes())
                    .unwrap_or(0) as u64,
        )
    }

    pub(super) fn next(
        &mut self,
        schema: SchemaRef,
        output_memory: &mut HostMemoryReservation,
    ) -> Result<Option<RecordBatch>> {
        loop {
            if self.remaining == Some(0) {
                return Ok(None);
            }
            if self
                .current
                .as_ref()
                .is_none_or(|batch| self.position == batch.num_rows())
            {
                self.current = None;
                self.current_reservation.resize(0)?;
                let Some(batch) = self.runtime.block_on(self.stream.next()).transpose()? else {
                    return Ok(None);
                };
                self.current_reservation
                    .resize(batch.get_array_memory_size())?;
                self.current = Some(batch);
                self.position = 0;
                self.emitted = 0;
            }
            let batch = self.current.as_ref().unwrap();
            let counts = batch
                .column(batch.num_columns() - 1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            // Bound both output rows and variable-width expansion before constructing Arrow buffers.
            let capacity = output_memory.available_capacity()?.unwrap_or(64 << 20);
            let batch_bytes = batch.get_array_memory_size().max(1);
            let max_rows = super::OUTPUT_BATCH_ROWS.min((capacity / 256).max(1));
            let max_repetitions =
                ((capacity.saturating_sub(max_rows * 128)) / batch_bytes.saturating_mul(2)).max(1);
            output_memory.resize(max_rows * 128)?;
            let mut indices = Vec::with_capacity(max_rows);
            let mut repetitions = 0usize;
            while indices.len() < max_rows
                && self.position < batch.num_rows()
                && self.remaining != Some(0)
            {
                let available = counts.value(self.position) - self.emitted;
                let skip = available.min(self.skip);
                self.skip -= skip;
                self.emitted += skip;
                let take = (available - skip)
                    .min((max_rows - indices.len()) as u64)
                    .min(self.remaining.unwrap_or(u64::MAX))
                    .min(max_repetitions as u64);
                indices.extend(std::iter::repeat_n(self.position as u32, take as usize));
                repetitions = repetitions.max(take as usize);
                self.emitted += take;
                if let Some(remaining) = &mut self.remaining {
                    *remaining -= take;
                }
                if self.emitted == counts.value(self.position) {
                    self.position += 1;
                    self.emitted = 0;
                } else {
                    break;
                }
            }
            if indices.is_empty() {
                output_memory.resize(0)?;
                continue;
            }
            output_memory.resize(
                batch_bytes
                    .saturating_mul(repetitions)
                    .saturating_add(max_rows * 128),
            )?;
            let indices = UInt32Array::from(indices);
            let mut columns = batch.columns()[..batch.num_columns() - 1]
                .iter()
                .map(|column| take(column.as_ref(), &indices, None))
                .collect::<std::result::Result<Vec<_>, _>>()?;
            columns.push(Arc::new(Int8Array::from_value(
                super::INSERT,
                indices.len(),
            )));
            return Ok(Some(RecordBatch::try_new(schema, columns)?));
        }
    }
}
