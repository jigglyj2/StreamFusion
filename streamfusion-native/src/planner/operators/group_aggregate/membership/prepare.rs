// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

impl MembershipLayout {
    pub(super) fn collect_members(
        &self,
        calls: &[Call],
        batch: &RecordBatch,
        keys: &[StateKey],
        row_groups: &[usize],
        staged: &[Option<AccumulatorState>],
        external: &[bool],
        prefixes: &[Vec<u8>],
    ) -> Result<HashMap<StateKey, Member, RandomState>> {
        let mut members = HashMap::with_hasher(RandomState::new());
        for (column_index, column) in self.columns.iter().enumerate() {
            let encoded = column
                .converter
                .convert_columns(&[batch.column(column.input).clone()])?;
            for row in 0..batch.num_rows() {
                let mut selected = false;
                for &call in &column.calls {
                    selected |= aggregate_filter(&calls[call], batch, row)?;
                }
                if !selected {
                    continue;
                }
                let Some(value) = aggregate_value(batch.column(column.input).as_ref(), row)? else {
                    continue;
                };
                let group = row_groups[row];
                let key = member_key(
                    &prefixes[group],
                    column.identity,
                    encoded.row(row).as_ref(),
                    keys[group].key_group,
                );
                members.entry(key).or_insert_with(|| Member {
                    group,
                    column: column_index,
                    value,
                    original: vec![0; column.calls.len()],
                });
            }
            // Existing inline states remain readable. Migrate the union of each argument's
            // memberships, keeping independent signed counts for its filters.
            for (group, value) in staged
                .iter()
                .enumerate()
                .filter(|(group, _)| !external[*group])
            {
                let Some(value) = value else {
                    continue;
                };
                for &call in &column.calls {
                    let Accumulator::DistinctCount { values, .. } = &value.accumulators[call]
                    else {
                        unreachable!()
                    };
                    let values = values.keys().cloned().map(Some).collect::<Vec<_>>();
                    let arrays = aggregate_array(&values, &column.data_type)?;
                    let encoded = column.converter.convert_columns(&[arrays])?;
                    for (row, value) in values.into_iter().enumerate() {
                        let key = member_key(
                            &prefixes[group],
                            column.identity,
                            encoded.row(row).as_ref(),
                            keys[group].key_group,
                        );
                        members.entry(key).or_insert_with(|| Member {
                            group,
                            column: column_index,
                            value: value.unwrap(),
                            original: vec![0; column.calls.len()],
                        });
                    }
                }
            }
        }
        Ok(members)
    }
}
