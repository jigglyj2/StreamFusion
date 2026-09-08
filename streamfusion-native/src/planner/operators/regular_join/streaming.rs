// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::candidate_batch::CandidateBatch;
use super::change_cursor::ChangeCursor;
use super::native_output::NativeJoinOutput;
use super::*;

pub(super) struct StreamingCursor {
    side: usize,
    batch: RecordBatch,
    encoded: Rows,
    staged: Vec<StagedState>,
    indices: Vec<usize>,
    row: usize,
    change: Option<ChangeCursor>,
    matches: Option<CandidateMatches>,
    candidate_batch: CandidateBatch,
    pair_bytes: usize,
    // Keep decoded state, input encodings and lookup metadata admitted between pulls.
    _memory: HostMemoryReservation,
}

impl RegularJoinProcessor {
    pub(crate) fn cancel_streaming_batch(&mut self) {
        if self.streaming_cursor.take().is_some() {
            self.streaming_failed = true;
        }
    }

    #[cfg(test)]
    pub(crate) fn begin_streaming_batch(&mut self, side: usize, batch: RecordBatch) -> Result<()> {
        self.require_idle_stream()?;
        self.begin_streaming_batch_impl(side, batch)
    }

    pub(super) fn begin_region_input(&mut self, side: usize, batch: RecordBatch) -> Result<()> {
        if !self.streaming_region_active || self.streaming_failed || self.streaming_cursor.is_some()
        {
            return Err(DataFusionError::Execution(
                "regular join region input is not idle".into(),
            ));
        }
        self.begin_streaming_batch_impl(side, batch)
    }

    fn begin_streaming_batch_impl(&mut self, side: usize, batch: RecordBatch) -> Result<()> {
        if side > 1 || self.plan.bounded_final_output {
            return Err(DataFusionError::Execution(
                "invalid streaming regular join input".into(),
            ));
        }
        self.prepare_schema(side, batch.schema())?;
        let visible = self.visible_schemas[side].fields().len();
        let mut memory = self
            .scratch_reservation
            .sibling("regular join in-flight batch state");
        memory.resize(
            batch
                .get_array_memory_size()
                .saturating_mul(4)
                .saturating_add(batch.num_rows().saturating_mul(1024))
                .saturating_add(4096),
        )?;
        let encoded = self.row_converters[side].convert_columns(&batch.columns()[..visible])?;
        let mut unique = HashMap::<StateKey, usize, RandomState>::with_capacity_and_hasher(
            batch.num_rows(),
            RandomState::new(),
        );
        let mut indices = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let key = self.group_key(side, &batch, row)?;
            let key = StateKey {
                key_group: assign_key_group(&key, self.max_parallelism),
                key,
            };
            let index = unique.len();
            indices.push(*unique.entry(key).or_insert(index));
        }
        let mut keys = vec![None; unique.len()];
        for (key, index) in unique {
            keys[index] = Some(key);
        }
        let keys = keys
            .into_iter()
            .map(|key| key.expect("populated join key"))
            .collect::<Vec<_>>();
        let (staged, reads) = paged_state::load(self.state.as_ref(), keys, &mut memory)?;
        self.state_read_batches = self.state_read_batches.saturating_add(reads);
        self.streaming_cursor = Some(StreamingCursor {
            side,
            batch,
            encoded,
            staged,
            indices,
            row: 0,
            change: None,
            matches: None,
            candidate_batch: CandidateBatch::default(),
            pair_bytes: 0,
            _memory: memory,
        });
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn next_streaming_batch(&mut self) -> Result<Option<RecordBatch>> {
        let result = self
            .next_native_output()?
            .map(NativeJoinOutput::into_arrow)
            .transpose();
        if result.is_err() {
            self.streaming_failed = true;
        }
        result
    }

    pub(super) fn next_native_output(&mut self) -> Result<Option<NativeJoinOutput>> {
        if self.streaming_failed {
            return Err(DataFusionError::Execution(
                "regular join stream failed; task recovery is required".into(),
            ));
        }
        let Some(mut cursor) = self.streaming_cursor.take() else {
            return Ok(None);
        };
        match self.drain_streaming_batch(&mut cursor) {
            Ok((output, done)) => {
                if !done {
                    self.streaming_cursor = Some(cursor);
                }
                Ok(output)
            }
            Err(error) => {
                // Do not permit a checkpoint or another batch after partial changelog delivery.
                self.streaming_failed = true;
                Err(error)
            }
        }
    }

    pub(super) fn require_idle_stream(&self) -> Result<()> {
        if self.streaming_failed
            || self.streaming_cursor.is_some()
            || self.streaming_region_active
            || self.streaming_invocation_active
        {
            return Err(DataFusionError::Execution(
                "regular join has an undrained or failed output stream".into(),
            ));
        }
        Ok(())
    }

    fn drain_streaming_batch(
        &mut self,
        cursor: &mut StreamingCursor,
    ) -> Result<(Option<NativeJoinOutput>, bool)> {
        loop {
            let mut output = Vec::new();
            let mut output_memory = self
                .scratch_reservation
                .sibling("regular join bounded Arrow output");
            let mut admitted = 4096usize;
            while cursor.row < cursor.batch.num_rows() {
                let state_index = cursor.indices[cursor.row];
                if cursor.change.is_none() {
                    if self.residual_condition.is_some() && cursor.candidate_batch.is_empty() {
                        // The previous transition is complete, so release its exhausted cache
                        // before admitting the next predicate chunk.
                        cursor.candidate_batch = CandidateBatch::default();
                        cursor.candidate_batch = self.condition_matches_batch(
                            cursor.side,
                            &cursor.batch,
                            &cursor.encoded,
                            &cursor.staged,
                            &cursor.indices,
                            cursor.row,
                        )?;
                    }
                    let state = &cursor.staged[state_index].value;
                    let candidates = if cursor.side == 0 {
                        &state.right
                    } else {
                        &state.left
                    };
                    let input = cursor.encoded.row(cursor.row).data();
                    let kind = cursor
                        .batch
                        .column(self.input_kind_indices[cursor.side].expect("prepared schema"))
                        .as_any()
                        .downcast_ref::<Int8Array>()
                        .ok_or_else(|| {
                            DataFusionError::Execution("regular join RowKinds are not Int8".into())
                        })?
                        .value(cursor.row);
                    let accumulate = match kind {
                        INSERT | UPDATE_AFTER => true,
                        UPDATE_BEFORE | DELETE => false,
                        _ => {
                            return Err(DataFusionError::Execution(format!(
                                "unknown Flink RowKind byte {kind}"
                            )))
                        }
                    };
                    cursor.matches = Some(match cursor.candidate_batch.pop() {
                        Some(matches) => matches,
                        None => self.condition_matches_row(
                            cursor.side,
                            &cursor.batch,
                            cursor.row,
                            input,
                            candidates,
                        )?,
                    });
                    cursor.pair_bytes = candidates
                        .iter()
                        .map(|row| row.row.len())
                        .max()
                        .unwrap_or(0)
                        .saturating_mul(2)
                        .saturating_add(input.len())
                        .saturating_add(512);
                    cursor.change = Some(ChangeCursor::new(
                        self.join_type,
                        cursor.side,
                        kind,
                        accumulate,
                        Arc::from(input),
                        i32::try_from(cursor.row).map_err(|_| {
                            DataFusionError::Execution(
                                "regular join input exceeds Int32 ordinals".into(),
                            )
                        })?,
                    ));
                }
                // A byte target complements the row cap for wide rows. One candidate transition
                // may produce a padding retract and a pair, so progress always permits two rows.
                let limit =
                    ((2 << 20) / cursor.pair_bytes.max(1)).clamp(2, BOUNDED_EDGE_OUTPUT_MAX_ROWS);
                if output.len().saturating_add(2) > limit {
                    break;
                }
                let maximum_rows = cursor
                    .matches
                    .as_ref()
                    .expect("initialized mask")
                    .count()
                    .saturating_mul(2)
                    .saturating_add(1)
                    .min(limit - output.len());
                admit_output_capacity(
                    &mut output_memory,
                    admitted.saturating_add(
                        maximum_rows
                            .saturating_mul(cursor.pair_bytes)
                            .saturating_mul(4),
                    ),
                )?;
                let before = output.len();
                let done = cursor.change.as_mut().expect("initialized change").drain(
                    &mut cursor.staged[state_index].value,
                    cursor.matches.as_ref().expect("initialized mask"),
                    &mut output,
                    limit,
                )?;
                admitted = admitted.saturating_add(
                    output[before..]
                        .iter()
                        .fold(0usize, |bytes, row| {
                            bytes
                                .saturating_add(row.left.as_ref().map_or(0, |row| row.len()))
                                .saturating_add(row.right.as_ref().map_or(0, |row| row.len()))
                                .saturating_add(512)
                        })
                        .saturating_mul(4),
                );
                cursor.staged[state_index].touched = true;
                if !done {
                    break;
                }
                cursor.change = None;
                cursor.matches = None;
                cursor.row += 1;
            }
            let done = cursor.row == cursor.batch.num_rows();
            if done {
                let mutations = paged_state::batch_mutations(&cursor.staged, &mut cursor._memory)?;
                // The write owns its encoded keys and values. Final output rows own payload
                // Arcs under output_memory, so decoded state and input encodings can now go.
                // Do not overlap an entire drained batch's workspace with backend growth.
                let mutation_bytes = mutations.iter().fold(
                    mutations
                        .capacity()
                        .saturating_mul(std::mem::size_of::<StateMutation>())
                        .saturating_add(4096),
                    |bytes, mutation| {
                        bytes
                            .saturating_add(mutation.key.key.capacity())
                            .saturating_add(mutation.value.as_ref().map_or(0, Vec::capacity))
                            .saturating_add(64)
                    },
                );
                cursor.staged = Vec::new();
                cursor.indices = Vec::new();
                cursor.encoded = self.row_converters[cursor.side].empty_rows(0, 0);
                cursor.batch = RecordBatch::new_empty(cursor.batch.schema());
                cursor.candidate_batch = CandidateBatch::default();
                cursor._memory.resize(mutation_bytes)?;
                if !mutations.is_empty() {
                    self.state.write_batch(mutations)?;
                    self.state_write_batches = self.state_write_batches.saturating_add(1);
                }
            }
            if output.is_empty() {
                return Ok((None, done));
            }
            let mut batch = self.output_batch(output)?;
            if let Some(calcs) = &self.fused_output_calcs {
                self.fused_calc_batches = self
                    .fused_calc_batches
                    .saturating_add(calcs.stage_count() as u64);
                batch = calcs.execute(batch)?;
            }
            if batch.num_rows() == 0 {
                if done {
                    return Ok((None, true));
                }
                continue;
            }
            let bytes = batch.get_array_memory_size();
            output_memory.resize(bytes)?;
            return Ok((
                Some(NativeJoinOutput {
                    batch,
                    _memory: output_memory,
                }),
                done,
            ));
        }
    }
}

// Keep the buffer's high-water allowance until this output chunk is emitted. Growing in
// coarse powers of two avoids a JVM reservation on every input row without weakening admission.
fn admit_output_capacity(memory: &mut HostMemoryReservation, required: usize) -> Result<()> {
    if required <= memory.size() {
        return Ok(());
    }
    let capacity = required
        .checked_next_power_of_two()
        .unwrap_or(required)
        .max(64 << 10);
    match memory.resize(capacity) {
        Err(DataFusionError::ResourcesExhausted(_)) if capacity != required => {
            memory.resize(required)
        }
        result => result,
    }
}
