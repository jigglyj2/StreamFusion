// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

impl TopNProcessor {
    pub(crate) fn finish_bounded(&mut self) -> Result<RecordBatch> {
        if !self.plan.bounded_final_output {
            return Err(DataFusionError::Plan(
                "terminal Top-N output requires a bounded-final plan".to_string(),
            ));
        }
        if self.bounded_drained {
            return output_batch(&self.plan, &self.output_schema, &[], Vec::new());
        }
        if self.bounded_output.is_none() {
            self.prepare_bounded_output()?;
            if self.bounded_output.is_none() {
                self.bounded_drained = true;
                self.scratch_reservation.resize(0)?;
                return output_batch(&self.plan, &self.output_schema, &[], Vec::new());
            }
        }

        const OUTPUT_BATCH_ROWS: usize = 16_384;
        let pending = self
            .bounded_output
            .as_mut()
            .expect("bounded output prepared");
        let end = pending
            .position
            .saturating_add(OUTPUT_BATCH_ROWS)
            .min(pending.indices.len());
        let selection = UInt32Array::from(pending.indices[pending.position..end].to_vec());
        let mut columns = pending
            .rows
            .columns()
            .iter()
            .map(|column| take(column.as_ref(), &selection, None))
            .collect::<arrow::error::Result<Vec<_>>>()?;
        if self.plan.output_rank_number {
            columns.push(Arc::new(Int64Array::from(
                pending.ranks[pending.position..end].to_vec(),
            )) as ArrayRef);
        }
        columns.push(Arc::new(Int8Array::from(
            pending.kinds[pending.position..end].to_vec(),
        )) as ArrayRef);
        let mut fields = self
            .output_schema
            .fields()
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        fields.push(Arc::new(Field::new(
            OUTPUT_KIND_COLUMN,
            DataType::Int8,
            false,
        )));
        let output = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)?;
        pending.position = end;
        let retained = pending.retained_bytes();
        let output_bytes = output.get_array_memory_size();
        let finished = end == pending.indices.len();
        self.scratch_reservation
            .resize(retained.saturating_add(output_bytes))?;
        self.scratch_reservation.transfer_to_arrow(output_bytes)?;
        if finished {
            self.bounded_output = None;
            self.bounded_drained = true;
            self.scratch_reservation.resize(0)?;
        }
        Ok(output)
    }

    fn prepare_bounded_output(&mut self) -> Result<()> {
        let converter = self.row_converter.as_ref().ok_or_else(|| {
            DataFusionError::Execution("bounded Top-N received no input schema".to_string())
        })?;
        let mut encoded_rows = Vec::<Vec<u8>>::new();
        let mut row_kinds = Vec::<i8>::new();
        let mut groups = Vec::<(usize, usize)>::new();
        let mut metadata_credit = self.scratch_reservation.sibling("bounded Top-N metadata");
        for key_group in self.first_key_group..=self.last_key_group {
            for (key, value) in crate::planner::operators::ordered_partition::metadata(
                self.state.as_ref(),
                key_group,
                STATE_KEY_PREFIX,
                &mut metadata_credit,
            )? {
                if key.first().copied() != Some(STATE_KEY_PREFIX) {
                    continue;
                }
                let mut index_credit = self
                    .scratch_reservation
                    .sibling("bounded Top-N ordered partition");
                let entries =
                    if value.starts_with(crate::planner::operators::sortable_state::FORMAT) {
                        Some(crate::planner::operators::ordered_partition::load(
                            self.state.as_ref(),
                            &StateKey {
                                key_group,
                                key: key.clone(),
                            },
                            &mut index_credit,
                        )?)
                    } else {
                        None
                    };
                let mut decoded = decode_state_rows(
                    value
                        .strip_prefix(crate::planner::operators::sortable_state::FORMAT)
                        .unwrap_or(&value),
                )?;
                if let Some(entries) = &entries {
                    if entries.values().any(Vec::is_empty) {
                        return Err(DataFusionError::Execution(
                            "truncated Top-N indexed candidate".into(),
                        ));
                    }
                    decoded.rows = entries.values().map(|v| &v[1..]).collect();
                    decoded.row_kinds = Some(entries.values().map(|v| v[0] as i8).collect());
                }
                if decoded.rows.is_empty() {
                    continue;
                }
                let kinds = decoded.row_kinds.ok_or_else(|| {
                    DataFusionError::Execution(
                        "bounded Top-N state is missing physical RowKinds".to_string(),
                    )
                })?;
                self.scratch_reservation.try_grow(
                    decoded
                        .rows
                        .iter()
                        .map(|row| row.len().saturating_mul(3).saturating_add(256))
                        .sum(),
                )?;
                let offset = encoded_rows.len();
                encoded_rows.extend(decoded.rows.into_iter().map(<[u8]>::to_vec));
                row_kinds.extend(kinds);
                groups.push((offset, encoded_rows.len() - offset));
            }
        }
        if encoded_rows.is_empty() {
            return Ok(());
        }
        let parser = converter.parser();
        let columns = converter.convert_rows(encoded_rows.iter().map(|row| parser.parse(row)))?;
        let rows = RecordBatch::try_new(Arc::clone(&self.input_schema), columns)?;
        let mut group_order = (0..groups.len()).collect::<Vec<_>>();
        let partition_ascending = vec![true; self.plan.partition_key_indices.len()];
        let partition_nulls_last = vec![false; self.plan.partition_key_indices.len()];
        let mut compare_error = None;
        group_order.sort_by(|left, right| {
            self.comparator_calls = self.comparator_calls.saturating_add(1);
            let (left, _) = groups[*left];
            let (right, _) = groups[*right];
            match compare_rows(
                &rows,
                left,
                &rows,
                right,
                &self.plan.partition_key_indices,
                &partition_ascending,
                &partition_nulls_last,
            ) {
                Ok(order) => order,
                Err(error) => {
                    compare_error = Some(error);
                    Ordering::Equal
                }
            }
        });
        if let Some(error) = compare_error {
            return Err(error);
        }
        let mut indices = Vec::new();
        let mut ranks = Vec::new();
        let mut kinds = Vec::new();
        let ordering = self
            .plan
            .sort_key_indices
            .iter()
            .map(|&i| rows.column(i as usize).clone())
            .collect::<Vec<_>>();
        let needs_peer_adapter = ordering.iter().any(|array| {
            crate::planner::operators::bounded_rank::requires_flink_peers(array.data_type())
        });
        for group in group_order {
            let (offset, count) = groups[group];
            let mut evaluator = Rank::basic().partition_evaluator(Default::default())?;
            let mut rank = 1u64;
            for index in 0..count {
                let row = offset + index;
                if index > 0 {
                    self.comparator_calls = self.comparator_calls.saturating_add(1);
                    if needs_peer_adapter
                        && compare_rows(
                            &rows,
                            row - 1,
                            &rows,
                            row,
                            &self.plan.sort_key_indices,
                            &self.plan.sort_ascending,
                            &self.plan.sort_nulls_last,
                        )? != Ordering::Equal
                    {
                        rank = index as u64 + 1;
                    }
                }
                if !needs_peer_adapter {
                    let ScalarValue::UInt64(Some(value)) =
                        evaluator.evaluate(&ordering, &(row..row + 1))?
                    else {
                        return Err(DataFusionError::Internal(
                            "DataFusion RANK returned a non-u64 value".into(),
                        ));
                    };
                    rank = value;
                }
                if rank >= self.plan.rank_start && rank <= self.plan.rank_end.expect("validated") {
                    indices.push(u32::try_from(row).map_err(|_| {
                        DataFusionError::Execution(
                            "bounded Top-N terminal output exceeds u32 rows".to_string(),
                        )
                    })?);
                    ranks.push(rank as i64);
                    kinds.push(row_kinds[row]);
                }
            }
        }
        let pending = BoundedOutput {
            rows,
            indices,
            ranks,
            kinds,
            position: 0,
        };
        self.scratch_reservation.resize(pending.retained_bytes())?;
        self.bounded_output = Some(pending);
        Ok(())
    }
}
