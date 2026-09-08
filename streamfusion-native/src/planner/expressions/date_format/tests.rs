// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.
use super::*;
use arrow::array::{Array, StringArray, TimestampMillisecondArray};
use arrow::datatypes::Field;
use arrow::record_batch::RecordBatch;
use datafusion::physical_expr::expressions::Column;
#[test]
fn full_signed_millisecond_range_matches_java_year_of_era_and_sign_rules() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "ts",
        DataType::Timestamp(TimeUnit::Millisecond, None),
        true,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(TimestampMillisecondArray::from(vec![
            None,
            Some(i64::MIN),
            Some(-62167219200000),
            Some(-62198755200000),
            Some(253402300800000),
            Some(i64::MAX),
            Some(-1),
            Some(0),
        ]))],
    )
    .unwrap();
    let expr = create(
        Arc::new(Column::new("ts", 0)),
        "yyyy-MM-dd HH:mm:ss.SSS",
        &schema,
    )
    .unwrap();
    let output = expr
        .evaluate(&batch)
        .unwrap()
        .into_array(batch.num_rows())
        .unwrap();
    let strings = output.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(
        strings.iter().collect::<Vec<_>>(),
        vec![
            None,
            Some("+292275056-05-16 16:47:04.192"),
            Some("0001-01-01 00:00:00.000"),
            Some("0002-01-01 00:00:00.000"),
            Some("+10000-01-01 00:00:00.000"),
            Some("+292278994-08-17 07:12:55.807"),
            Some("1969-12-31 23:59:59.999"),
            Some("1970-01-01 00:00:00.000")
        ]
    );
}

#[test]
fn numeric_patterns_preserve_quotes_literals_nulls_empty_batches_and_scalar_inputs() {
    use datafusion::logical_expr::ColumnarValue;
    use datafusion::physical_expr::expressions::Literal;
    let schema = Arc::new(Schema::new(vec![Field::new(
        "ts",
        DataType::Timestamp(TimeUnit::Millisecond, None),
        true,
    )]));
    let array = Arc::new(TimestampMillisecondArray::from(vec![
        Some(1),
        Some(0),
        None,
        Some(-1),
    ]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(array.slice(1, 3))]).unwrap();
    for (pattern, expected) in [
        ("HH:mm", vec![Some("00:00"), None, Some("23:59")]),
        (
            "'年'yyyy' % it''s 'MM/dd",
            vec![
                Some("年1970 % it's 01/01"),
                None,
                Some("年1969 % it's 12/31"),
            ],
        ),
        ("", vec![Some(""), None, Some("")]),
    ] {
        let expr = create(Arc::new(Column::new("ts", 0)), pattern, &schema).unwrap();
        let output = expr.evaluate(&batch).unwrap().into_array(3).unwrap();
        assert_eq!(
            output
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            expected
        );
        assert_eq!(
            expr.evaluate(&RecordBatch::new_empty(schema.clone()))
                .unwrap()
                .into_array(0)
                .unwrap()
                .len(),
            0
        );
    }
    let scalar = create(
        Arc::new(Literal::new(ScalarValue::TimestampMillisecond(
            Some(0),
            None,
        ))),
        "yyyy",
        &schema,
    )
    .unwrap();
    assert!(matches!(scalar.evaluate(&batch).unwrap(),
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(value))) if value == "1970"));
    for pattern in [
        "yy",
        "YYYY",
        "MMMM",
        "hh:mm a",
        "yyyy['x']",
        "yyyy-MM-ddZ",
        "SSSS",
        "'unclosed",
        "#",
    ] {
        assert!(
            create(Arc::new(Column::new("ts", 0)), pattern, &schema).is_err(),
            "{pattern}"
        );
    }
}

#[test]
fn coarse_format_admission_covers_large_output_and_retains_credit_for_slices() {
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
                i64::MIN
            );
            4096
        ]))],
    )
    .unwrap();
    let pattern = format!("'{}' yyyy/MM/dd HH:mm:ss.SSS", "界".repeat(100));
    for limit in [1 << 20, 64 << 20] {
        let broker = Arc::new(PeakBroker {
            inner: TestBroker::new(limit),
            peak: std::sync::atomic::AtomicUsize::new(0),
        });
        let pool: Arc<dyn MemoryPool> = Arc::new(FlinkMemoryPool::new(broker.clone(), limit));
        let expr = super::super::managed_scalar::install(
            create(Arc::new(Column::new("ts", 0)), &pattern, &schema).unwrap(),
            Some(&pool),
            &schema,
        )
        .unwrap();
        let (result, observed) = crate::allocation_test_support::measure(|| expr.evaluate(&batch));
        if limit == 1 << 20 {
            assert!(matches!(
                result,
                Err(DataFusionError::ResourcesExhausted(_))
            ));
            assert!(
                observed.peak < 64 << 10,
                "denial precedes large arrays: {observed:?}"
            );
            assert_eq!(broker.inner.reserved(), 0);
        } else {
            let result = result.unwrap().into_array(4096).unwrap();
            assert!(
                observed.peak <= broker.peak.load(std::sync::atomic::Ordering::Relaxed),
                "{observed:?}, reserved {}",
                broker.inner.reserved()
            );
            assert_eq!(
                result
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(0),
                format!("{} +292275056/05/16 16:47:04.192", "界".repeat(100))
            );
            let slice = result.slice(1, 128);
            drop(result);
            assert!(broker.inner.reserved() > 0);
            drop(slice);
            assert_eq!(broker.inner.reserved(), 0);
        }
    }
}

#[derive(Debug)]
struct PeakBroker {
    inner: crate::memory_pool::tests_support::TestBroker,
    peak: std::sync::atomic::AtomicUsize,
}
impl crate::memory_pool::MemoryReservationBroker for PeakBroker {
    fn try_reserve(&self, bytes: usize) -> Result<bool> {
        let accepted = self.inner.try_reserve(bytes)?;
        if accepted {
            self.peak
                .fetch_max(self.inner.reserved(), std::sync::atomic::Ordering::Relaxed);
        }
        Ok(accepted)
    }
    fn release(&self, bytes: usize) -> Result<()> {
        self.inner.release(bytes)
    }
    fn available(&self) -> Result<Option<usize>> {
        self.inner.available()
    }
}
