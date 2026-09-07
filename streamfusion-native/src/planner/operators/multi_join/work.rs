// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
const OUTPUT_ROWS: usize = 4096;
const OUTPUT_BYTES: usize = 1 << 20;
const RESERVATION_CHUNK: usize = 64 << 10;

pub(super) struct Work {
    input: usize,
    batch: RecordBatch,
    encoded: Rows,
    groups: Vec<usize>,
    states: Vec<StagedState>,
    _loaded: HostMemoryReservation,
    row: usize,
    active: Option<Active>,
    selection: Option<cursor::Selection>,
}
struct Active {
    stored: StoredRow,
    cursor: cursor::Cursor,
    remove: Option<usize>,
}
pub(super) struct Output {
    pub(super) batch: RecordBatch,
    pub(super) memory: HostMemoryReservation,
}

impl MultiJoinProcessor {
    pub(crate) fn start_arrow_stream(&mut self, input: usize, batch: RecordBatch) -> Result<()> {
        if self.pending.is_some() || self.failed {
            return Err(DataFusionError::Execution(
                "multi-join stream is active or failed".into(),
            ));
        }
        if input >= self.plan.inputs.len() {
            return Err(DataFusionError::Execution(
                "multi-join input index out of range".into(),
            ));
        }
        self.prepare_schema(input, batch.schema())?;
        let visible = self.visible_schemas[input].fields().len();
        let base = crate::memory_pool::buffer_size::arrays_bytes(&batch.columns()[..visible])?
            .saturating_mul(2)
            .saturating_add(batch.num_rows().saturating_mul(256));
        self.scratch_reservation.resize(base)?;
        let result = (|| {
            let encoded =
                self.row_converters[input].convert_columns(&batch.columns()[..visible])?;
            let mut unique =
                HashMap::<StateKey, usize, RandomState>::with_hasher(RandomState::new());
            let mut groups = Vec::with_capacity(batch.num_rows());
            for row in 0..batch.num_rows() {
                let key = self.group_key(input, &batch, row)?;
                let key = StateKey {
                    key_group: assign_key_group(&key, self.max_parallelism),
                    key,
                };
                let next = unique.len();
                groups.push(*unique.entry(key).or_insert(next));
            }
            let mut ordered = (0..unique.len()).map(|_| None).collect::<Vec<_>>();
            for (key, index) in unique {
                ordered[index] = Some(key);
            }
            let keys = ordered.into_iter().map(Option::unwrap).collect();
            let (states, loaded) = pages::load(
                self.state.as_ref(),
                keys,
                self.plan.inputs.len(),
                &self.scratch_reservation,
            )?;
            // Diagnostic counts logical state-access batches, each consisting of directory
            // and page multi-gets performed before any input-row computation.
            if !groups.is_empty() {
                self.state_read_batches = self.state_read_batches.saturating_add(1);
            }
            Ok(Work {
                input,
                batch,
                encoded,
                groups,
                states,
                _loaded: loaded,
                row: 0,
                active: None,
                selection: None,
            })
        })();
        match result {
            Ok(work) => {
                self.pending = Some(work);
                Ok(())
            }
            Err(error) => {
                self.scratch_reservation.resize(0)?;
                Err(error)
            }
        }
    }

    pub(super) fn next_output(&mut self) -> Result<Option<Output>> {
        if self.failed {
            return Err(DataFusionError::Execution(
                "multi-join stream failed; task recovery required".into(),
            ));
        }
        let Some(mut work) = self.pending.take() else {
            return Ok(None);
        };
        match work.next(self) {
            Ok(Some(output)) => {
                self.pending = Some(work);
                Ok(Some(output))
            }
            Ok(None) => {
                drop(work);
                self.scratch_reservation.resize(0)?;
                Ok(None)
            }
            Err(error) => {
                drop(work);
                self.scratch_reservation.resize(0)?;
                self.failed = true;
                Err(error)
            }
        }
    }

    /// Native pull interface. The production planner remains gated until its fused execution,
    /// metrics and checkpoint integration are complete; the legacy edge accepts only one chunk.
    #[allow(dead_code)]
    pub(crate) fn next_arrow_batch(&mut self) -> Result<Option<RecordBatch>> {
        self.next_output()?
            .map(|output| crate::memory_pool::arrow_lease::host_batch(output.batch, output.memory))
            .transpose()
    }

    pub(super) fn abort_unpublished(&mut self) -> Result<()> {
        self.pending = None;
        self.scratch_reservation.resize(0)
    }
}

impl Work {
    fn next(&mut self, processor: &mut MultiJoinProcessor) -> Result<Option<Output>> {
        let mut rows = Vec::new();
        let mut encoded_bytes = 0usize;
        let mut memory = processor
            .scratch_reservation
            .sibling("multi-join bounded output buffers");
        loop {
            if self.row == self.batch.num_rows() {
                if !rows.is_empty() {
                    break;
                }
                let (changes, _memory) =
                    pages::mutations(&mut self.states, &processor.scratch_reservation)?;
                if !changes.is_empty() {
                    processor.state.write_batch(changes)?;
                    processor.state_write_batches = processor.state_write_batches.saturating_add(1);
                }
                return Ok(None);
            }
            let group = self.groups[self.row];
            if self.active.is_none() {
                let kind = self
                    .batch
                    .column(processor.input_kind_indices[self.input].unwrap())
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .ok_or_else(|| {
                        DataFusionError::Execution("multi-join RowKind is not Int8".into())
                    })?
                    .value(self.row);
                if !matches!(kind, INSERT | UPDATE_AFTER | UPDATE_BEFORE | DELETE) {
                    return Err(DataFusionError::Execution(
                        "unknown multi-join RowKind".into(),
                    ));
                }
                let mut stored = processor.stored_row(
                    self.input,
                    &self.batch,
                    self.row,
                    self.encoded.row(self.row).data().to_vec(),
                )?;
                let remove = if matches!(kind, UPDATE_BEFORE | DELETE) {
                    let position = self.states[group].value.inputs[self.input]
                        .iter()
                        .position(|row| row.row == stored.row);
                    let Some(position) = position else {
                        self.row += 1;
                        continue;
                    };
                    stored = self.states[group].value.inputs[self.input][position].clone();
                    Some(position)
                } else {
                    None
                };
                let ordinal = i32::try_from(self.row).map_err(|_| {
                    DataFusionError::Execution("multi-join ordinal overflow".into())
                })?;
                self.active = Some(Active {
                    stored,
                    cursor: cursor::Cursor::new(self.input, kind, ordinal),
                    remove,
                });
            }
            let active = self.active.as_mut().unwrap();
            let selection = self.selection.take().or_else(|| {
                active
                    .cursor
                    .next(&processor.plan, &self.states[group].value, &active.stored)
            });
            let Some(selection) = selection else {
                let mut active = self.active.take().unwrap();
                let entry = &mut self.states[group];
                if let Some(position) = active.remove {
                    entry.value.inputs[self.input].remove(position);
                } else {
                    active.stored.slot = entry.directory.append(self.input)?;
                }
                entry
                    .dirty
                    .insert((self.input, active.stored.slot / pages::PAGE_ROWS));
                entry.touched = true;
                if active.remove.is_none() {
                    entry.value.inputs[self.input].push(active.stored);
                }
                self.row += 1;
                continue;
            };
            let selected = selection
                .rows
                .iter()
                .enumerate()
                .map(|(input, position)| match position {
                    Some(cursor::ACTIVE) => &active.stored.row,
                    Some(position) => &self.states[group].value.inputs[input][*position].row,
                    None => &processor.null_rows[input],
                })
                .collect::<Vec<_>>();
            let bytes = selected
                .iter()
                .try_fold(0usize, |sum, row| sum.checked_add(row.len()))
                .ok_or_else(truncated)?;
            if !rows.is_empty()
                && (rows.len() == OUTPUT_ROWS || encoded_bytes.saturating_add(bytes) > OUTPUT_BYTES)
            {
                self.selection = Some(selection);
                break;
            }
            encoded_bytes = encoded_bytes.checked_add(bytes).ok_or_else(truncated)?;
            let allowance = encoded_bytes
                .checked_mul(4)
                .and_then(|bytes| {
                    bytes.checked_add(
                        (rows.len() + 1)
                            .checked_mul(processor.output_schema.fields().len())?
                            .checked_mul(64)?,
                    )
                })
                .and_then(|bytes| bytes.checked_add(RESERVATION_CHUNK - 1))
                .map(|bytes| bytes / RESERVATION_CHUNK * RESERVATION_CHUNK)
                .ok_or_else(truncated)?;
            memory.resize(allowance)?;
            rows.push(OutputRow {
                inputs: selected.into_iter().cloned().collect(),
                kind: selection.kind,
                input_ordinal: selection.ordinal,
            });
        }
        let batch = processor.output_batch(rows)?;
        let retained = crate::memory_pool::buffer_size::batch_bytes(&batch)?;
        if retained > memory.size() {
            return Err(DataFusionError::ResourcesExhausted(
                "multi-join output exceeded admitted buffer workspace".into(),
            ));
        }
        memory.resize(retained)?;
        Ok(Some(Output { batch, memory }))
    }
}
