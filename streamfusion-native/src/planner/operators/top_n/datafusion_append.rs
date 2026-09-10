// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! DataFusion orders constant-range append-only candidates; Flink's adapter emits each transition.
use super::*;
use arrow::array::UInt64Array;
use datafusion::physical_expr::{expressions::Column, LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::sorts::sort::sort_batch;

pub(super) fn compatible(plan: &proto::TopN, sources: &CandidateSources) -> bool {
    plan.strategy == proto::TopNStrategy::AppendFast as i32
        && plan.rank_end.is_some_and(|end| end > 1)
        && plan.variable_rank_end_index.is_none()
        && rank_type(plan) == proto::TopNRankType::RowNumber
        && !plan.bounded_final_output
        && !plan.physical_input_semantics
        && !plan.sort_key_indices.is_empty()
        && sources.orders.is_some()
}

pub(super) struct Selection {
    priorities: Vec<Vec<u64>>,
    candidates: Vec<CandidateRef>,
    schema: SchemaRef,
    ordering: LexOrdering,
    // Includes retained ordinals/references and the largest per-arrival fixed-width sort.
    _memory: HostMemoryReservation,
}

impl Selection {
    pub(super) fn new(
        sources: &CandidateSources,
        groups: &[GroupWork],
        row_groups: &[usize],
        owner: &HostMemoryReservation,
    ) -> Result<Self> {
        let orders = sources.orders.as_ref().expect("compatible Arrow sort keys");
        let count = sources.iter().map(|batch| batch.num_rows()).sum::<usize>();
        let key_bytes = orders
            .iter()
            .map(|rows| rows.iter().map(|row| row.data().len()).sum::<usize>())
            .sum::<usize>();
        let mut memory = owner.sibling("DataFusion append Top-N priorities and selection");
        memory.resize(
            key_bytes
                .saturating_mul(3)
                .saturating_add(count.saturating_mul(384))
                .saturating_add(64 << 10),
        )?;
        let mut tokens = groups
            .iter()
            .flat_map(|group| group.candidates.iter().copied())
            .collect::<Vec<_>>();
        let mut sequences = groups
            .iter()
            .map(|group| group.next_sequence)
            .collect::<Vec<_>>();
        for (row, &group) in row_groups.iter().enumerate() {
            tokens.push(CandidateRef {
                source: 0,
                row,
                sequence: sequences[group],
                kind: INSERT,
            });
            sequences[group] = next_sequence(sequences[group])?;
        }
        // Persisted sequence resolves equal Flink sort keys, including restored candidates.
        // Token only disambiguates equal sequences from different partitions in this batch sort.
        let input = RecordBatch::try_from_iter(vec![
            (
                "key",
                Arc::new(BinaryArray::from_iter_values(
                    tokens.iter().map(|c| orders[c.source].row(c.row)),
                )) as ArrayRef,
            ),
            (
                "sequence",
                Arc::new(UInt64Array::from_iter_values(
                    tokens.iter().map(|c| c.sequence),
                )) as ArrayRef,
            ),
            (
                "token",
                Arc::new(UInt64Array::from_iter_values(0..tokens.len() as u64)) as ArrayRef,
            ),
        ])?;
        let sorted = sort_batch(&input, &ordering(&["key", "sequence", "token"]), None)?;
        let sorted_tokens = sorted
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("UInt64 token");
        let mut priorities = sources
            .iter()
            .map(|batch| vec![0; batch.num_rows()])
            .collect::<Vec<_>>();
        let mut candidates = Vec::with_capacity(tokens.len());
        for (priority, &token) in sorted_tokens.values().iter().enumerate() {
            let candidate = tokens[token as usize];
            priorities[candidate.source][candidate.row] = priority as u64;
            candidates.push(candidate);
        }
        Ok(Self {
            priorities,
            candidates,
            schema: Arc::new(Schema::new(vec![Field::new(
                "priority",
                DataType::UInt64,
                false,
            )])),
            ordering: ordering(&["priority"]),
            _memory: memory,
        })
    }

    pub(super) fn insert(
        &self,
        candidates: &mut Vec<CandidateRef>,
        candidate: CandidateRef,
        limit: usize,
    ) -> Result<()> {
        candidates.push(candidate);
        self.retain(candidates, limit)
    }

    pub(super) fn retain(&self, candidates: &mut Vec<CandidateRef>, limit: usize) -> Result<()> {
        // Only priorities enter the per-arrival kernel. Payloads and wide sort keys stay in
        // their original Arrow batches; retaining a wide winner never multiplies its bytes.
        let input = RecordBatch::try_new(
            self.schema.clone(),
            vec![Arc::new(UInt64Array::from_iter_values(
                candidates.iter().map(|c| self.priorities[c.source][c.row]),
            ))],
        )?;
        let selected = sort_batch(&input, &self.ordering, Some(limit))?;
        let priorities = selected
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("UInt64 priority");
        candidates.clear();
        candidates.extend(
            priorities
                .values()
                .iter()
                .map(|&priority| self.candidates[priority as usize]),
        );
        Ok(())
    }
}

fn ordering(names: &[&str]) -> LexOrdering {
    LexOrdering::new(names.iter().enumerate().map(|(index, &name)| {
        PhysicalSortExpr::new(
            Arc::new(Column::new(name, index)),
            arrow::compute::SortOptions::default(),
        )
    }))
    .expect("nonempty ordering")
}

#[cfg(test)]
mod tests;
