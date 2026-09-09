// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Retained local-window computation and bounded flush cursor. Java supplies the original
//! Flink operator's memory share and shared-plan control binding.

use super::buffer_layout::BufferLayout;
use super::row_layout::RowLayout;
use super::*;
use crate::planner::operators::group_aggregate::grouped_compute::GroupedOutput;

mod control;
pub(crate) mod execution_plan;
#[cfg(test)]
mod processing_time_tests;
#[cfg(test)]
mod string_tests;
#[cfg(test)]
mod tests;

const OUTPUT_ROWS: usize = 2048;
const COMPUTE_ROWS: usize = 2048;

pub(crate) struct BufferedWindow {
    kernel: LocalWindowAggregateProcessor,
    groups: hashbrown::HashTable<usize>,
    hasher: RandomState,
    order: Vec<SliceKey>,
    key_bytes: usize,
    cursor: Option<InputCursor>,
    flushing: Option<FlushCursor>,
    layout: BufferLayout,
    key_layout: RowLayout,
    input_layout: RowLayout,
    min_slice_end: i64,
    current_progress: i64,
    processing_time: bool,
    next_trigger_progress: i64,
    // Owners precede their credits so cancellation/drop never returns live buffers' budget.
    retained: HostMemoryReservation,
}
struct InputCursor {
    batch: RecordBatch,
    clock: Option<Int64Array>,
    keys: Option<arrow::row::Rows>,
    offset: usize,
}
struct FlushCursor {
    values: GroupedOutput,
    offset: usize,
}

// Vec<u8> and &[u8] have identical Hash implementations. The hash table stores only
// ordinals into first-appearance order; retained encoded keys have exactly one owner.
#[derive(Hash)]
struct BorrowedSliceKey<'a> {
    grouping_row: &'a [u8],
    window_start: i64,
    slice_end: i64,
}
impl BorrowedSliceKey<'_> {
    fn equivalent(&self, key: &SliceKey) -> bool {
        self.grouping_row == key.grouping_row
            && self.window_start == key.window_start
            && self.slice_end == key.slice_end
    }
}

impl BufferedWindow {
    pub(crate) fn validate_capacity(capacity: usize, page: usize) -> Result<()> {
        BufferLayout::new(capacity, page).map(|_| ())
    }

    pub(super) fn new(
        kernel: LocalWindowAggregateProcessor,
        flink_memory_bytes: usize,
        page_bytes: usize,
    ) -> Result<Self> {
        Self::new_inner(kernel, flink_memory_bytes, page_bytes, false)
    }

    fn new_inner(
        kernel: LocalWindowAggregateProcessor,
        flink_memory_bytes: usize,
        page_bytes: usize,
        processing_time: bool,
    ) -> Result<Self> {
        if kernel.plan.input_changelog || kernel.grouped_compute.is_none() {
            return Err(DataFusionError::Plan(
                "buffered local window requires append-only DataFusion grouped accumulators".into(),
            ));
        }
        if kernel.shift_time_zone != chrono_tz::UTC {
            return Err(DataFusionError::Plan(
                "buffered local window timezone control parity is limited to UTC".into(),
            ));
        }
        let time_columns = kernel.plan.attached_window_end_index.map_or_else(
            || vec![kernel.plan.time_attribute_index],
            |end| {
                kernel
                    .plan
                    .attached_window_start_index
                    .into_iter()
                    .chain([end])
                    .collect()
            },
        );
        if kernel.plan.attached_window_end_index.is_some()
            && time_columns
                .iter()
                .any(|&index| kernel.input_schema.field(index as usize).is_nullable())
        {
            return Err(DataFusionError::Plan(
                "buffered local window nullable event-time/window-bound parity is not verified"
                    .into(),
            ));
        }
        for field in kernel.input_schema.fields() {
            if !matches!(
                field.data_type(),
                DataType::Boolean
                    | DataType::Utf8
                    | DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::Float32
                    | DataType::Float64
                    | DataType::Date32
                    | DataType::Time32(_)
                    | DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None)
            ) {
                return Err(DataFusionError::Plan(
                    "buffered local window needs verified Flink row-size geometry for its input type".into(),
                ));
            }
        }
        let retained = kernel
            .reservation
            .sibling("local window retained grouped state");
        let key_layout = RowLayout::new(
            &kernel.input_schema,
            &kernel
                .plan
                .grouping_indices
                .iter()
                .map(|&index| index as usize)
                .collect::<Vec<_>>(),
        )?;
        let input_layout = RowLayout::new(
            &kernel.input_schema,
            &(0..kernel.input_schema.fields().len()).collect::<Vec<_>>(),
        )?;
        Ok(Self {
            kernel,
            groups: hashbrown::HashTable::new(),
            hasher: RandomState::new(),
            order: Vec::new(),
            key_bytes: 0,
            cursor: None,
            flushing: None,
            layout: BufferLayout::new(flink_memory_bytes, page_bytes)?,
            key_layout,
            input_layout,
            min_slice_end: i64::MAX,
            current_progress: 0,
            processing_time,
            next_trigger_progress: 0,
            retained,
        })
    }

    /// The single-stage processing-time window uses Flink's same RecordsWindowBuffer geometry,
    /// but reads explicit per-record clocks and flushes on processing-time progress only.
    pub(crate) fn new_processing_time(
        kernel: LocalWindowAggregateProcessor,
        flink_memory_bytes: usize,
        page_bytes: usize,
    ) -> Result<Self> {
        if kernel.plan.kind != proto::WindowKind::Tumble as i32
            || kernel.plan.attached_window_start_index.is_some()
            || kernel.plan.attached_window_end_index.is_some()
        {
            return Err(DataFusionError::Plan(
                "processing-time buffer requires a direct TUMBLE window".into(),
            ));
        }
        let mut buffer = Self::new_inner(kernel, flink_memory_bytes, page_bytes, true)?;
        // WindowAggProcessorBase starts at MIN; its processing clock is not restored from watermarks.
        buffer.current_progress = i64::MIN;
        buffer.next_trigger_progress = i64::MIN;
        Ok(buffer)
    }

    pub(crate) fn push_processing_time(
        &mut self,
        batch: RecordBatch,
        clock: Int64Array,
    ) -> Result<Option<RecordBatch>> {
        if !self.processing_time || clock.len() != batch.num_rows() || clock.null_count() != 0 {
            return Err(DataFusionError::Plan(
                "processing-time buffer requires one non-null clock per input row".into(),
            ));
        }
        self.push_inner(batch, Some(clock))
    }

    /// The input must be drained before another batch or control event can be accepted.
    pub(super) fn push(&mut self, batch: RecordBatch) -> Result<Option<RecordBatch>> {
        if self.processing_time {
            return Err(DataFusionError::Plan(
                "processing-time buffer requires explicit Flink clock input".into(),
            ));
        }
        self.push_inner(batch, None)
    }

    fn push_inner(
        &mut self,
        batch: RecordBatch,
        clock: Option<Int64Array>,
    ) -> Result<Option<RecordBatch>> {
        if self.has_pending() {
            return Err(DataFusionError::Execution(
                "local window input cursor is still active".into(),
            ));
        }
        self.kernel.validate_batch(&batch)?;
        // Flink accepts a nullable schema but fails a record with a null rowtime.
        // Validate the Arrow bitmap once before any retained grouped state changes.
        if !self.processing_time
            && self.kernel.plan.attached_window_end_index.is_none()
            && batch
                .column(self.kernel.plan.time_attribute_index as usize)
                .null_count()
                != 0
        {
            return Err(DataFusionError::Execution(
                "RowTime field should not be null, please convert it to a non-null long value."
                    .into(),
            ));
        }
        // Also admit replacement hash/group vectors during growth before touching state.
        let allowance = self.input_workspace(&batch, batch.num_rows().min(COMPUTE_ROWS))?;
        self.kernel.reservation.resize(allowance)?;
        let keys = if self.kernel.plan.grouping_indices.is_empty() {
            None
        } else {
            let columns = self
                .kernel
                .plan
                .grouping_indices
                .iter()
                .map(|&index| batch.column(index as usize).clone())
                .collect::<Vec<_>>();
            Some(self.kernel.grouping_converter.convert_columns(&columns)?)
        };
        self.cursor = Some(InputCursor {
            batch,
            clock,
            keys,
            offset: 0,
        });
        self.poll_pending()
    }

    pub(crate) fn has_pending(&self) -> bool {
        self.cursor.is_some() || self.flushing.is_some()
    }

    pub(crate) fn has_buffered_updates(&self) -> bool {
        !self.order.is_empty() || self.has_pending()
    }

    pub(crate) fn poll_pending(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            if self.flushing.is_some() {
                return self.emit_chunk().map(Some);
            }
            let Some(mut cursor) = self.cursor.take() else {
                return Ok(None);
            };
            let result = self.consume_segment(&mut cursor);
            let complete = cursor.offset == cursor.batch.num_rows();
            // Keep the producer's Arrow buffers and workspace alive on failure as well.
            self.cursor = Some(cursor);
            let pressure = result?;
            if pressure {
                self.begin_flush()?;
            } else if complete {
                self.cursor = None;
                self.kernel.reservation.resize(0)?;
                return Ok(None);
            }
        }
    }

    fn consume_segment(&mut self, cursor: &mut InputCursor) -> Result<bool> {
        let start = cursor.offset;
        let rows = (cursor.batch.num_rows() - start).min(COMPUTE_ROWS);
        self.kernel
            .reservation
            .resize(self.input_workspace(&cursor.batch, rows)?)?;
        let end = start + rows;
        let mut indices = Vec::with_capacity(rows);
        let mut pressure = false;
        while cursor.offset < end {
            let row = cursor.offset;
            let bounds = match &cursor.clock {
                Some(clock) => {
                    let start = window_start(
                        clock.value(row),
                        self.kernel.plan.offset_millis,
                        self.kernel.plan.size_millis,
                    );
                    Some((start, start.wrapping_add(self.kernel.plan.size_millis)))
                }
                None => self.kernel.slice_bounds(&cursor.batch, row)?,
            };
            let Some((window_start, slice_end)) = bounds else {
                return Err(DataFusionError::Execution(
                    "buffered local window requires non-null event-time/window bounds".into(),
                ));
            };
            let encoded = cursor.keys.as_ref().map(|keys| keys.row(row));
            let key = BorrowedSliceKey {
                grouping_row: encoded.as_ref().map_or(&[], |key| key.as_ref()),
                window_start,
                slice_end,
            };
            let hash = self.hasher.hash_one(&key);
            let existing = self
                .groups
                .find(hash, |&index| key.equivalent(&self.order[index]))
                .copied();
            if !self.layout.append(
                existing.is_none(),
                self.key_layout.fixed,
                self.key_layout.bytes(&cursor.batch, row)?,
                self.input_layout.fixed,
                self.input_layout.bytes(&cursor.batch, row)?,
            )? {
                if self.groups.is_empty() {
                    return Err(DataFusionError::ResourcesExhausted(
                        "local window input cannot fit the original Flink buffer".into(),
                    ));
                }
                pressure = true;
                break;
            }
            let index = match existing {
                Some(index) => index,
                None => {
                    let index = self.order.len();
                    let key = SliceKey {
                        grouping_row: key.grouping_row.to_vec(),
                        window_start,
                        slice_end,
                    };
                    self.key_bytes = self
                        .key_bytes
                        .checked_add(key.grouping_row.capacity())
                        .ok_or_else(overflow)?;
                    self.order.push(key);
                    self.groups.insert_unique(hash, index, |&index| {
                        self.hasher.hash_one(&self.order[index])
                    });
                    index
                }
            };
            self.min_slice_end = self.min_slice_end.min(slice_end);
            indices.push(index);
            cursor.offset += 1;
        }
        if !indices.is_empty() && !self.groups.is_empty() {
            let batch = cursor.batch.slice(start, indices.len());
            self.kernel
                .grouped_compute
                .as_mut()
                .expect("validated grouped compute")
                .update(
                    &self.kernel.calls,
                    &batch,
                    &indices,
                    None,
                    self.groups.len(),
                )?;
        }
        self.account_retained()?;
        Ok(pressure)
    }

    // Keep the batch's encoded keys while advancing bounded zero-copy compute slices.
    // Chunk completion is internal and must not flush Flink-visible partial records.
    fn input_workspace(&self, batch: &RecordBatch, compute_rows: usize) -> Result<usize> {
        let key_width = self
            .kernel
            .plan
            .grouping_indices
            .len()
            .checked_mul(32)
            .and_then(|bytes| bytes.checked_add(16))
            .ok_or_else(overflow)?;
        let payload = self
            .key_layout
            .encoding_payload(batch)?
            .checked_mul(4)
            .ok_or_else(overflow)?;
        let keys = batch
            .num_rows()
            .checked_mul(key_width)
            .and_then(|bytes| bytes.checked_add(payload))
            .ok_or_else(overflow)?;
        let growth = self.growth_headroom(compute_rows)?;
        self.kernel
            .buffered_batch_admission(compute_rows)?
            .checked_add(keys)
            .and_then(|bytes| bytes.checked_add(growth))
            .ok_or_else(overflow)
    }

    // Existing key bytes are never copied on index growth. Reserve replacement buffers
    // only when this batch can cross their capacity, plus conservative DataFusion growth.
    // The batch allowance covers new entries, keys, selections and row encodings.
    fn growth_headroom(&self, rows: usize) -> Result<usize> {
        let groups = self.groups.len().checked_add(rows).ok_or_else(overflow)?;
        let table = if groups > self.groups.capacity() {
            self.groups
                .allocation_size()
                .checked_mul(2)
                .ok_or_else(overflow)?
        } else {
            0
        };
        let order = if groups > self.order.capacity() {
            self.order
                .capacity()
                .checked_mul(2 * std::mem::size_of::<SliceKey>())
                .ok_or_else(overflow)?
        } else {
            0
        };
        self.kernel
            .grouped_compute
            .as_ref()
            .unwrap()
            .size()
            .checked_mul(2)
            .and_then(|bytes| bytes.checked_add(table))
            .and_then(|bytes| bytes.checked_add(order))
            .ok_or_else(overflow)
    }

    fn retained_bytes(&self) -> Result<usize> {
        self.groups
            .allocation_size()
            .checked_add(self.order.capacity() * std::mem::size_of::<SliceKey>())
            .and_then(|bytes| bytes.checked_add(self.key_bytes))
            .and_then(|bytes| {
                bytes.checked_add(self.kernel.grouped_compute.as_ref().unwrap().size())
            })
            .and_then(|bytes| {
                bytes.checked_add(
                    self.flushing
                        .as_ref()
                        .map_or(0, |flush| flush.values.size()),
                )
            })
            .ok_or_else(overflow)
    }

    fn account_retained(&mut self) -> Result<()> {
        let bytes = self.retained_bytes()?;
        if bytes > self.retained.size() {
            self.retained
                .grow_from(&mut self.kernel.reservation, bytes - self.retained.size())?;
        }
        Ok(())
    }

    fn begin_flush(&mut self) -> Result<()> {
        if self.groups.is_empty() || self.flushing.is_some() {
            return Ok(());
        }
        // EmitTo::All moves DataFusion's compact vectors; admit nullable Arrow descriptors
        // and any implementation-specific evaluation scratch before evaluation.
        let extra = self
            .kernel
            .grouped_compute
            .as_ref()
            .unwrap()
            .size()
            .checked_add(64 * 1024)
            .ok_or_else(overflow)?;
        self.kernel.reservation.try_grow(extra)?;
        let values = self.kernel.grouped_compute.as_mut().unwrap().finish()?;
        self.flushing = Some(FlushCursor { values, offset: 0 });
        self.account_retained()
    }

    fn emit_chunk(&mut self) -> Result<RecordBatch> {
        let flush = self.flushing.as_ref().expect("active flush");
        let per_row = self
            .kernel
            .calls
            .len()
            .checked_mul(256)
            .and_then(|bytes| {
                self.kernel
                    .plan
                    .grouping_indices
                    .len()
                    .checked_mul(256)?
                    .checked_add(bytes)
            })
            .and_then(|bytes| bytes.checked_add(512))
            .ok_or_else(overflow)?;
        let mut output_memory = self
            .retained
            .sibling("local window bounded output workspace");
        // Arrow batch boundaries are internal: preserve Flink's partial/control order
        // while allowing a large flush to drain through a smaller managed-memory share.
        // Admission still fails normally if even one row cannot fit.
        let available = output_memory.available_capacity()?.unwrap_or(usize::MAX);
        let mut end = flush.offset;
        let mut allowance = 64 * 1024usize;
        while end < (flush.offset + OUTPUT_ROWS).min(self.order.len()) {
            let next = self.order[end]
                .grouping_row
                .len()
                .checked_mul(4)
                .and_then(|bytes| bytes.checked_add(per_row))
                .and_then(|bytes| bytes.checked_add(allowance))
                .ok_or_else(overflow)?;
            if end > flush.offset && next > available {
                break;
            }
            allowance = next;
            end += 1;
        }
        output_memory.resize(allowance)?;
        let output = self.kernel.output_partials((flush.offset..end).map(|row| {
            flush
                .values
                .state(&self.kernel.calls, row)
                .map(|state| (self.order[row].clone(), state))
        }))?;
        let lease = output_memory.split(
            output.get_array_memory_size(),
            "local window output buffers",
        )?;
        let output = crate::memory_pool::arrow_lease::host_batch(output, lease)?;
        self.flushing.as_mut().unwrap().offset = end;
        if end == self.order.len() {
            self.flushing = None;
            self.groups = hashbrown::HashTable::new();
            self.order = Vec::new();
            self.key_bytes = 0;
            self.min_slice_end = i64::MAX;
            self.layout.reset();
            self.retained.resize(self.retained_bytes()?)?;
            if self.cursor.is_none() {
                self.kernel.reservation.resize(0)?;
            }
        }
        Ok(output)
    }
}

impl Drop for BufferedWindow {
    fn drop(&mut self) {
        // The compatibility kernel owns the input workspace reservation. It is the first
        // field, so clear every dependent retained/cursor buffer before field destruction
        // can return that credit during cancellation or a failed invocation.
        self.cursor = None;
        self.flushing = None;
        self.groups = hashbrown::HashTable::new();
        self.order = Vec::new();
        self.kernel.grouped_compute = None;
    }
}

fn overflow() -> DataFusionError {
    DataFusionError::ResourcesExhausted("local window retained buffer admission overflow".into())
}
