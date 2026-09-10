// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

//! Flink rowtime changelog and batched state adaptation around DataFusion winner selection.
use super::*;

impl DeduplicateProcessor {
    pub(super) fn select_rowtime(&mut self, batch: &RecordBatch) -> Result<Selection> {
        self.prepare_schema(batch.schema(), batch.num_columns())?;
        let sidecar_rows = self.stored_rows(batch)?;
        let encoded_rows = if self.plan.generate_update_before && sidecar_rows.is_none() {
            Some(self.encode_visible_rows(batch)?)
        } else {
            None
        };
        let state_keys = self.state_keys(batch)?;
        let state_key_refs = state_keys
            .iter()
            .map(|(key_group, key)| StateKeyRef {
                key_group: *key_group,
                key,
            })
            .collect::<Vec<_>>();
        // RocksDB lowers this call to one multi_get. In-memory state returns borrowed values.
        let existing = self
            .state
            .get_batch(&state_key_refs, &self.scratch_reservation)?;
        let _loaded_state_workspace =
            crate::state::reserve_decoded_values(&existing, &self.scratch_reservation)?;
        let selection = datafusion_rowtime::winners(
            batch.column(self.plan.order_index as usize),
            &state_key_refs,
            &existing,
            self.plan.keep_last,
            &self.scratch_reservation,
        )?;
        let mut staged = HashMap::<StateKeyRef<'_>, usize, RandomState>::with_capacity_and_hasher(
            batch.num_rows(),
            RandomState::new(),
        );
        let mut staged_values = Vec::<Option<(usize, Vec<u8>)>>::with_capacity(batch.num_rows());
        let mut content_ordinals = Vec::with_capacity(batch.num_rows());
        let mut envelope_ordinals = Vec::with_capacity(batch.num_rows());
        let mut row_kinds = Vec::with_capacity(batch.num_rows());
        let mut output_stored_rows = self
            .plan
            .generate_update_before
            .then(|| Vec::with_capacity(batch.num_rows() * 2));

        for row in 0..batch.num_rows() {
            if !selection.winners[row] {
                continue;
            }
            let order_millis = selection.orders[row];
            let key = state_key_refs[row];
            let previous_value = staged
                .get(&key)
                .and_then(|&index| {
                    staged_values[index]
                        .as_ref()
                        .map(|(_, value)| value.as_slice())
                })
                .or_else(|| existing[row].as_deref());
            let previous_order = previous_value.map(decode_order).transpose()?;
            let previous_stored = if self.plan.generate_update_before {
                previous_value
                    .map(decode_stored_row)
                    .transpose()?
                    .map(<[u8]>::to_vec)
            } else {
                None
            };
            if let Some(previous_staging) = staged.insert(key, staged_values.len()) {
                staged_values[previous_staging] = None;
            }
            let encoded_row = encoded_rows.as_ref().map(|rows| rows.row(row));
            let current_stored = sidecar_rows
                .map(|rows| rows.value(row))
                .or_else(|| encoded_row.as_ref().map(|row| row.as_ref()));
            staged_values.push(Some((row, encode_value(order_millis, current_stored))));
            let row_ordinal = i32::try_from(row).map_err(|_| {
                DataFusionError::Execution("deduplicate batch exceeds Int32 indexing".to_string())
            })?;
            if self.plan.generate_update_before {
                if let Some(previous) = previous_stored {
                    content_ordinals.push(-1);
                    envelope_ordinals.push(row_ordinal);
                    row_kinds.push(UPDATE_BEFORE);
                    output_stored_rows.as_mut().unwrap().push(Some(previous));
                }
            }
            content_ordinals.push(row_ordinal);
            envelope_ordinals.push(row_ordinal);
            row_kinds.push(
                if previous_order.is_none()
                    && (self.plan.generate_insert || self.plan.generate_update_before)
                {
                    INSERT
                } else {
                    UPDATE_AFTER
                },
            );
            if let Some(output) = output_stored_rows.as_mut() {
                output.push(None);
            }
        }

        drop(existing);
        drop(staged);
        drop(state_key_refs);
        let mut final_values = vec![None; batch.num_rows()];
        for (input_row, value) in staged_values.into_iter().flatten() {
            final_values[input_row] = Some(value);
        }
        let mutations = state_keys
            .into_iter()
            .zip(final_values)
            .filter_map(|((key_group, key), value)| {
                value.map(|value| StateMutation {
                    key: StateKey { key_group, key },
                    value: Some(value),
                })
            })
            .collect();
        // RocksDB lowers this call to one atomic WriteBatch.
        self.state.write_batch(mutations)?;
        Ok(Selection {
            content_ordinals,
            envelope_ordinals,
            row_kinds,
            stored_rows: output_stored_rows,
            input_rows: encoded_rows,
            _historical_memory: Some(_loaded_state_workspace),
        })
    }
}
