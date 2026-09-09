// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.
use super::*;
use arrow::array::{Int32Array, Int64Array, TimestampMillisecondArray};
use arrow::datatypes::Field;
use arrow::record_batch::RecordBatch;
use datafusion::physical_expr::expressions::Column;

#[test]
fn timestamp_clock_fields_preserve_signed_remainders_full_range_nulls_and_slices() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "ts",
        DataType::Timestamp(TimeUnit::Millisecond, None),
        true,
    )]));
    let values = TimestampMillisecondArray::from(vec![
        Some(42), // excluded by slicing
        Some(i64::MIN),
        Some(-86_400_001),
        Some(-3_661_234),
        Some(-1),
        Some(0),
        Some(3_661_234),
        Some(86_400_001),
        Some(i64::MAX),
        None,
    ]);
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(values.slice(1, 9))]).unwrap();
    // Expected fields follow Flink ExtractCallGen's signed clock contract.
    for (field, expected) in [
        ("hour", vec![-7, 0, -1, 0, 0, 1, 0, 7]),
        ("minute", vec![-12, 0, -1, 0, 0, 1, 0, 12]),
        ("second", vec![-55, 0, -1, 0, 0, 1, 0, 55]),
        (
            "flink_millisecond",
            vec![-808, -1, -234, -1, 0, 234, 1, 807],
        ),
    ] {
        for bigint in [false, true] {
            let expr = create(Arc::new(Column::new("ts", 0)), field, bigint, &schema).unwrap();
            let output = expr.evaluate(&batch).unwrap().into_array(9).unwrap();
            let actual: Vec<Option<i64>> = if bigint {
                output
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .iter()
                    .collect()
            } else {
                output
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .iter()
                    .map(|v| v.map(i64::from))
                    .collect()
            };
            let mut expected: Vec<_> = expected.iter().copied().map(Some).collect();
            expected.push(None);
            assert_eq!(actual, expected, "{field}, bigint={bigint}");
            assert_eq!(
                expr.evaluate(&RecordBatch::new_empty(schema.clone()))
                    .unwrap()
                    .into_array(0)
                    .unwrap()
                    .len(),
                0
            );
        }
    }
    let scalar = create(
        Arc::new(Literal::new(ScalarValue::TimestampMillisecond(
            Some(-1),
            None,
        ))),
        "flink_millisecond",
        true,
        &schema,
    )
    .unwrap();
    assert!(matches!(
        scalar.evaluate(&batch).unwrap(),
        datafusion::logical_expr::ColumnarValue::Scalar(ScalarValue::Int64(Some(-1)))
    ));
}

#[test]
fn timestamps_reject_unproven_calendar_zone_and_precision_contracts() {
    for data_type in [
        DataType::Timestamp(TimeUnit::Millisecond, None),
        DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
        DataType::Timestamp(TimeUnit::Microsecond, None),
        DataType::Timestamp(TimeUnit::Nanosecond, None),
    ] {
        let schema = Schema::new(vec![Field::new("ts", data_type.clone(), true)]);
        assert!(create(Arc::new(Column::new("ts", 0)), "year", true, &schema).is_err());
        if data_type != DataType::Timestamp(TimeUnit::Millisecond, None) {
            assert!(create(Arc::new(Column::new("ts", 0)), "hour", true, &schema).is_err());
        }
    }
}

#[test]
fn timestamp_integer_buffers_use_existing_managed_expression_allowance() {
    use crate::memory_pool::{tests_support::TestBroker, FlinkMemoryPool};
    use datafusion::execution::memory_pool::MemoryPool;
    let schema = Arc::new(Schema::new(vec![Field::new(
        "ts",
        DataType::Timestamp(TimeUnit::Millisecond, None),
        true,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(TimestampMillisecondArray::from(vec![
            Some(
                -3_661_234
            );
            4096
        ]))],
    )
    .unwrap();
    for limit in [128 << 10, 16 << 20] {
        let broker = Arc::new(TestBroker::new(limit));
        let pool: Arc<dyn MemoryPool> = Arc::new(FlinkMemoryPool::new(broker.clone(), limit));
        let expression = super::super::managed_scalar::install(
            create(Arc::new(Column::new("ts", 0)), "minute", true, &schema).unwrap(),
            Some(&pool),
            &schema,
        )
        .unwrap();
        let result = expression.evaluate(&batch);
        if limit == 128 << 10 {
            assert!(matches!(
                result,
                Err(DataFusionError::ResourcesExhausted(_))
            ));
        } else {
            let output = result.unwrap().into_array(4096).unwrap();
            assert_eq!(
                output
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0),
                -1
            );
            let slice = output.slice(1, 17);
            drop(output);
            assert!(broker.reserved() > 0);
            drop(slice);
        }
        assert_eq!(broker.reserved(), 0);
    }
}
