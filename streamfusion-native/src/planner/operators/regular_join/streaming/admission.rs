// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

impl RegularJoinProcessor {
    pub(super) fn begin_streaming_batch_impl(
        &mut self,
        side: usize,
        batch: RecordBatch,
        input_offset: usize,
    ) -> Result<()> {
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
        memory.resize(input_memory::workspace(&batch, visible)?)?;
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
        let kinds = batch
            .column(self.input_kind_indices[side].expect("prepared schema"))
            .as_any()
            .downcast_ref::<Int8Array>()
            .ok_or_else(|| {
                DataFusionError::Execution("regular join RowKinds are not Int8".into())
            })?;
        let accumulating = kinds.null_count() == 0
            && kinds
                .values()
                .iter()
                .all(|kind| matches!(*kind, INSERT | UPDATE_AFTER));
        let input_bytes = memory.size();
        let resources = self
            .spill_resources
            .clone()
            .filter(|_| self.join_type == proto::RegularJoinType::Inner);
        // Leave capacity for a bounded predicate/output page while trying the resident fast path.
        // This is temporary workspace admission within Flink's budget, not a private state limit.
        let mut headroom = memory.sibling("regular join next output workspace");
        if resources.is_some() {
            let available = headroom.available_capacity()?.unwrap_or(16 << 20);
            headroom.resize((available / 2).min(8 << 20))?;
        }
        let retry_keys = resources.as_ref().map(|_| keys.clone());
        let mut reads = 0;
        let resident = paged_state::load_counted(
            self.state.as_ref(),
            keys,
            &mut memory,
            accumulating.then_some(side),
            &mut reads,
        );
        drop(headroom);
        self.state_read_batches = self.state_read_batches.saturating_add(reads);
        let (staged, prepared) = match resident {
            Ok(staged) => (staged, None),
            Err(DataFusionError::ResourcesExhausted(_)) if resources.is_some() => {
                // No input transition or write has occurred; the rejected resident attempt has
                // dropped its rows. Retry with bounded preparation and preserve actual read counts.
                memory.resize(input_bytes)?;
                let manager = resources.unwrap().manager()?;
                let mut reads = 0;
                let result = prepared_history::load(
                    self.state.as_ref(),
                    retry_keys.unwrap(),
                    side,
                    &encoded,
                    &indices,
                    kinds,
                    &manager,
                    &memory,
                    &mut reads,
                );
                self.state_read_batches = self.state_read_batches.saturating_add(reads);
                let (staged, prepared) = result?;
                (staged, Some(prepared))
            }
            Err(error) => return Err(error),
        };
        self.streaming_cursor = Some(StreamingCursor {
            side,
            batch,
            encoded,
            staged,
            indices,
            row: 0,
            input_offset,
            change: None,
            matches: None,
            candidate_batch: CandidateBatch::default(),
            pair_bytes: 0,
            prepared,
            history_row: None,
            reader: None,
            page: None,
            _memory: memory,
        });
        Ok(())
    }
}
