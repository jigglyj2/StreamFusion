// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Processing-time namespaces share the ordered slice store and DataFusion merger.
//! Raw arrivals register timers; publishing buffered partials only updates state.
use super::*;

impl SharedSlices {
    pub(in crate::planner::operators::window_aggregate) fn new_processing_time(
        mut kernel: WindowAggregateProcessor,
        partial_schema: proto::Schema,
    ) -> Result<Self> {
        let fingerprint = Self::fingerprint(&kernel.plan);
        let groups = kernel.plan.grouping_indices.len() as u32;
        // This is an internal accumulator layout, not a second physical operator or wire
        // plan. Persist the original raw-input plan identity, including its clock column.
        kernel.plan.input_schema = Some(partial_schema);
        kernel.plan.grouping_indices = (0..groups).collect();
        kernel.plan.partial_accumulator_index = Some(groups);
        kernel.plan.partial_window_start_index = Some(groups + 1);
        kernel.plan.partial_slice_end_index = Some(groups + 2);
        let schema = crate::planner::arrow_schema(kernel.plan.input_schema.as_ref().unwrap())?;
        kernel.prepare_schema(schema)?;
        Self::new_inner(kernel, Some(fingerprint))
    }

    pub(in crate::planner::operators::window_aggregate) fn register_processing_timers(
        &mut self,
        batch: &RecordBatch,
        grouping_indices: &[u32],
        clocks: &Int64Array,
    ) -> Result<()> {
        self.require_healthy()?;
        self.started = true;
        let result = self.register_inner(batch, grouping_indices, clocks);
        if result.is_err() {
            self.failed = true;
        } else {
            self.kernel.scratch_reservation.resize(0)?;
        }
        result
    }

    fn register_inner(
        &mut self,
        batch: &RecordBatch,
        grouping_indices: &[u32],
        clocks: &Int64Array,
    ) -> Result<()> {
        let columns = grouping_indices
            .iter()
            .map(|&index| batch.column(index as usize).clone())
            .collect::<Vec<_>>();
        let key_bytes = columns.iter().try_fold(0usize, |bytes, column| {
            Ok::<_, DataFusionError>(
                bytes.saturating_add(column.to_data().get_slice_memory_size()?),
            )
        })?;
        self.admit(
            key_bytes
                .saturating_mul(8)
                .saturating_add(batch.num_rows().saturating_mul(512))
                .saturating_add(64 << 10),
        )?;
        let rows = if columns.is_empty() {
            None
        } else {
            Some(
                self.kernel
                    .grouping_converter
                    .as_ref()
                    .unwrap()
                    .convert_columns(&columns)?,
            )
        };
        let key_fields = grouping_indices
            .iter()
            .map(|&index| {
                Ok((
                    index as usize,
                    KeyField::from_arrow_type(batch.column(index as usize).data_type())?,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        let mut partition = Vec::new();
        let mut registrations = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            // Match the slice store's empty global-group identity as well as keyed input.
            if !key_fields.is_empty() {
                encode_binary_row_into(batch, row, &key_fields, &mut partition)?;
            }
            let group = assign_key_group(&partition, self.kernel.max_parallelism);
            let grouping = rows.as_ref().map(|rows| rows.row(row));
            let key = codec::prefix(grouping.as_ref().map_or(&[], |row| row.as_ref()))?;
            let end = crate::planner::operators::window_table_function::window_start(
                clocks.value(row),
                self.kernel.plan.offset_millis,
                self.kernel.plan.size_millis,
            )
            .wrapping_add(self.kernel.plan.size_millis);
            registrations.push((
                group,
                TimerDomain::ProcessingTime,
                TimerKey {
                    timestamp: end.wrapping_sub(1),
                    key,
                    namespace: end.to_le_bytes().to_vec(),
                },
            ));
        }
        let inserted = self.kernel.timers.register_batch(registrations)?;
        self.kernel.timer_registrations = self
            .kernel
            .timer_registrations
            .saturating_add(inserted.len() as u64);
        Ok(())
    }

    pub(in crate::planner::operators::window_aggregate) fn observe_processing_watermark(
        &mut self,
        watermark: i64,
    ) -> Result<()> {
        self.require_healthy()?;
        self.kernel.current_event_time = self.kernel.current_event_time.max(watermark);
        Ok(())
    }
}
