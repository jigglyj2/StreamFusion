// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

//! Ordering-only encoding. Flink BinaryRow partition identity and hashing remain unchanged.
use arrow::array::ArrayRef;
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Schema};
use arrow_row::{RowConverter, Rows, SortField};
use datafusion::error::{DataFusionError, Result};

pub(super) const PAGE_ROWS: usize = 256;
pub(super) const PAGE_BYTES: usize = 1024 * 1024;
// State format 1 pins Arrow 59 row encoding and the configured logical schema/order.
pub(super) const FORMAT: &[u8; 6] = b"SFOS\x01\x3b";

pub(super) struct SortKeys {
    indices: Vec<usize>,
    converter: RowConverter,
}
impl SortKeys {
    pub(super) fn new(
        schema: &Schema,
        indices: &[u32],
        ascending: &[bool],
        nulls_last: &[bool],
    ) -> Result<Option<Self>> {
        if indices.is_empty() {
            return Ok(None);
        }
        if indices.len() != ascending.len() || indices.len() != nulls_last.len() {
            return Err(DataFusionError::Plan(
                "invalid sortable state ordering".into(),
            ));
        }
        let mut fields = Vec::with_capacity(indices.len());
        for ((&i, &asc), &nulls_last) in indices.iter().zip(ascending).zip(nulls_last) {
            let field = schema.fields().get(i as usize).ok_or_else(|| {
                DataFusionError::Plan("sortable state column outside schema".into())
            })?;
            // Nested null ordering and floating peers require Flink-specific comparison.
            if !matches!(
                field.data_type(),
                DataType::Null
                    | DataType::Boolean
                    | DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64
                    | DataType::Utf8
                    | DataType::LargeUtf8
                    | DataType::Binary
                    | DataType::LargeBinary
                    | DataType::FixedSizeBinary(_)
                    | DataType::Decimal128(_, _)
                    | DataType::Date32
                    | DataType::Date64
                    | DataType::Time32(_)
                    | DataType::Time64(_)
                    | DataType::Timestamp(_, _)
                    | DataType::Duration(_)
            ) {
                return Ok(None);
            }
            fields.push(SortField::new_with_options(
                field.data_type().clone(),
                SortOptions {
                    descending: !asc,
                    nulls_first: !nulls_last,
                },
            ));
        }
        Ok(Some(Self {
            indices: indices.iter().map(|&i| i as usize).collect(),
            converter: RowConverter::new(fields)?,
        }))
    }
    pub(super) fn encode(&self, columns: &[ArrayRef]) -> Result<Rows> {
        Ok(self.converter.convert_columns(
            &self
                .indices
                .iter()
                .map(|&i| columns[i].clone())
                .collect::<Vec<_>>(),
        )?)
    }
}

/// Length framing prevents one partition's keys from overlapping another's prefix.
pub(super) fn prefix(namespace: u8, identity: &[u8]) -> Result<Vec<u8>> {
    let len = u32::try_from(identity.len())
        .map_err(|_| DataFusionError::Execution("state partition key exceeds UInt32".into()))?;
    let mut out = Vec::with_capacity(identity.len() + 11);
    out.push(namespace);
    out.extend_from_slice(FORMAT);
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(identity);
    Ok(out)
}
pub(super) fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last != 255 {
            end.push(last + 1);
            return Some(end);
        }
    }
    None
}
pub(super) fn row_key(prefix: &[u8], order: &[u8], sequence: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(prefix.len() + order.len() + 8);
    key.extend_from_slice(prefix);
    key.extend_from_slice(order);
    key.extend_from_slice(&sequence.to_be_bytes());
    key
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::operators::top_n::compare::compare_rows;
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    #[test]
    fn generated_keys_match_flink_comparison_and_preserve_partition_boundaries() {
        let numbers = (0..101)
            .map(|i| (i % 7 != 0).then_some(((i * 37) % 43) as i64 - 21))
            .collect::<Vec<_>>();
        let strings = (0..101)
            .map(|i| match i % 5 {
                0 => None,
                1 => Some(""),
                2 => Some("a\0b"),
                3 => Some("é"),
                _ => Some("aa"),
            })
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_from_iter(vec![
            ("n", Arc::new(Int64Array::from(numbers)) as ArrayRef),
            ("s", Arc::new(StringArray::from(strings)) as ArrayRef),
        ])
        .unwrap();
        for asc in [false, true] {
            for nulls_last in [false, true] {
                let keys = SortKeys::new(
                    &batch.schema(),
                    &[0, 1],
                    &[asc, !asc],
                    &[nulls_last, !nulls_last],
                )
                .unwrap()
                .unwrap()
                .encode(batch.columns())
                .unwrap();
                for i in 0..101 {
                    for j in 0..101 {
                        assert_eq!(
                            keys.row(i).cmp(&keys.row(j)),
                            compare_rows(
                                &batch,
                                i,
                                &batch,
                                j,
                                &[0, 1],
                                &[asc, !asc],
                                &[nulls_last, !nulls_last]
                            )
                            .unwrap()
                        );
                    }
                }
            }
        }
        for identity in [b"a".as_slice(), b"a\0", b"a\xff"] {
            let p = prefix(240, identity).unwrap();
            let end = prefix_end(&p).unwrap();
            let k = row_key(&p, b"\xff\0", u64::MAX);
            assert!(k >= p && k < end);
            for other in [b"a".as_slice(), b"a\0", b"a\xff"] {
                if other != identity {
                    assert!(!prefix(240, other).unwrap().starts_with(&p));
                }
            }
        }
    }

    #[test]
    fn persisted_arrow_59_integer_key_fixture() {
        let batch = RecordBatch::try_from_iter(vec![(
            "n",
            Arc::new(Int64Array::from(vec![Some(-1), Some(0), None])) as ArrayRef,
        )])
        .unwrap();
        let keys = SortKeys::new(&batch.schema(), &[0], &[true], &[false])
            .unwrap()
            .unwrap()
            .encode(batch.columns())
            .unwrap();
        assert_eq!(
            keys.row(0).data(),
            &[1, 127, 255, 255, 255, 255, 255, 255, 255]
        );
        assert_eq!(keys.row(1).data(), &[1, 128, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(keys.row(2).data(), &[0, 0, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(FORMAT, b"SFOS\x01\x3b");
    }

    #[test]
    fn float_and_empty_orderings_keep_the_flink_path() {
        let batch = RecordBatch::try_from_iter(vec![(
            "f",
            Arc::new(Float64Array::from(vec![f64::NAN, -0.0, 0.0])) as ArrayRef,
        )])
        .unwrap();
        assert!(SortKeys::new(&batch.schema(), &[0], &[true], &[false])
            .unwrap()
            .is_none());
        assert!(SortKeys::new(&batch.schema(), &[], &[], &[])
            .unwrap()
            .is_none());
    }
}
