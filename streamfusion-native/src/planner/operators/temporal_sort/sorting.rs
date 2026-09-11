// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use arrow::array::{ArrayRef, BinaryArray, UInt64Array};
use datafusion::physical_expr::{expressions::Column, LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::sorts::sort::sort_batch;

/// Flink sorts each fired timestamp group stably. Keep timer grouping and arrival order explicit,
/// and delegate the comparison/selection to DataFusion over Arrow-encoded secondary keys.
/// Payloads remain in state-row buffers until the selected output is decoded.
pub(super) fn ordered_rows(groups: &[Vec<BufferedRow>]) -> Result<Vec<&BufferedRow>> {
    let rows = groups.iter().flatten().collect::<Vec<_>>();
    let batch = RecordBatch::try_from_iter(vec![
        (
            "group",
            Arc::new(UInt64Array::from_iter_values(
                groups
                    .iter()
                    .enumerate()
                    .flat_map(|(group, rows)| std::iter::repeat_n(group as u64, rows.len())),
            )) as ArrayRef,
        ),
        (
            "key",
            Arc::new(BinaryArray::from_iter_values(
                rows.iter().map(|row| row.sort_key.as_slice()),
            )) as ArrayRef,
        ),
        (
            "arrival",
            Arc::new(UInt64Array::from_iter_values(0..rows.len() as u64)) as ArrayRef,
        ),
    ])?;
    let ordering = LexOrdering::new(["group", "key", "arrival"].into_iter().enumerate().map(
        |(index, name)| {
            PhysicalSortExpr::new(
                Arc::new(Column::new(name, index)),
                SortOptions {
                    descending: false,
                    nulls_first: true,
                },
            )
        },
    ))
    .expect("temporal sort has three ordering columns");
    let sorted = sort_batch(&batch, &ordering, None)?;
    let arrivals = sorted
        .column(2)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("arrival type");
    Ok(arrivals
        .values()
        .iter()
        .map(|&index| rows[index as usize])
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn generated_groups_preserve_equal_key_arrival_order() {
        for seed in [3usize, 17, 91] {
            let groups = (0..7)
                .map(|group| {
                    (0..128)
                        .map(|row| BufferedRow {
                            kind: (row % 4) as i8,
                            sort_key: vec![((row * seed + group) % 11) as u8],
                            row: (row as u64).to_le_bytes().to_vec(),
                        })
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();
            let mut expected = groups.clone();
            for group in &mut expected {
                group.sort_by(|a, b| a.sort_key.cmp(&b.sort_key));
            }
            let actual = ordered_rows(&groups).unwrap();
            assert_eq!(actual, expected.iter().flatten().collect::<Vec<_>>());
        }
    }
}
