// Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0.
use super::*;
use crate::allocation_test_support::measure;
use crate::memory_pool::{tests_support::TestBroker, FlinkMemoryPool};
use arrow::array::{Array, StringArray};
use datafusion::execution::memory_pool::MemoryPool;

fn input(values: Vec<Option<&str>>) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, true)])),
        vec![Arc::new(StringArray::from(values))],
    )
    .unwrap()
}
fn expression(
    pattern: &str,
    schema: &Schema,
    broker: Arc<TestBroker>,
    budget: usize,
) -> Arc<dyn PhysicalExpr> {
    let pool = Arc::new(FlinkMemoryPool::new(broker, budget)) as Arc<dyn MemoryPool>;
    let raw = create(Arc::new(Column::new("v", 0)), pattern, schema).unwrap();
    super::super::managed_scalar::install(raw, Some(&pool), schema).unwrap()
}
#[test]
fn regexp_extract_preserves_absent_and_empty_captures_unicode_and_first_match() {
    let batch = input(vec![
        None,
        Some(""),
        Some("a"),
        Some("b"),
        Some("channel_id="),
        Some("x&channel_id=😀漢é\u{301}\n\r\u{85}\u{2028}\u{2029}&channel_id=second"),
        Some("xchannel_id=wrong"),
    ]);
    let expected = [
        None,
        None,
        None,
        None,
        Some(""),
        Some("😀漢é\u{301}\n\r\u{85}\u{2028}\u{2029}"),
        None,
    ];
    let broker = Arc::new(TestBroker::new(64 << 20));
    let expr = expression(
        "(?:&|^)channel_id=([^&]*)",
        batch.schema().as_ref(),
        broker.clone(),
        64 << 20,
    );
    let result = expr
        .evaluate(&batch)
        .unwrap()
        .into_array(batch.num_rows())
        .unwrap();
    let strings = result.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(strings.iter().collect::<Vec<_>>(), expected);
    drop(result);
    assert_eq!(broker.reserved(), 0);
    let expr = expression(
        "(?:a)|(b)",
        batch.schema().as_ref(),
        broker.clone(),
        64 << 20,
    );
    let result = expr
        .evaluate(&batch)
        .unwrap()
        .into_array(batch.num_rows())
        .unwrap();
    let strings = result.as_any().downcast_ref::<StringArray>().unwrap();
    assert!(strings.is_null(2)); // A matched alternative with no participating capture.
    assert_eq!(strings.value(3), "b");
}
#[test]
fn regexp_extract_validates_projected_wire_contract() {
    for pattern in [
        "",
        "abc",
        "(a)(b)",
        "(a)*",
        "(a$)",
        "(.)",
        "(?i:a)(b)",
        "(?<name>a)",
        "(a*?)",
        "(a{2})",
        "([a&&b])",
        "(\\d)",
        "(😀)",
    ] {
        assert!(pattern::validate(pattern).is_err(), "{pattern}");
    }
    for pattern in [
        "()",
        "((?:a)(?:b))",
        "(?:a)|(b)",
        "(?:&|^)channel_id=([^&]*)",
        "([a-zA-Z0-9_]*)",
        "((?:a)|b)(?:c*)",
    ] {
        pattern::validate(pattern).unwrap();
    }
    assert!(pattern::validate(&format!("({})", "a".repeat(768))).is_err());
}
#[test]
fn regexp_extract_denies_large_workspace_before_kernel_allocation() {
    let large = "v".repeat(256 << 10);
    let batch = input(vec![Some(&large); 256]);
    let broker = Arc::new(TestBroker::new(32 << 20));
    let expr = expression("([^&]*)", batch.schema().as_ref(), broker.clone(), 32 << 20);
    let (result, observed) = measure(|| expr.evaluate(&batch));
    assert!(matches!(
        result,
        Err(DataFusionError::ResourcesExhausted(_))
    ));
    assert!(
        observed.peak < 64 << 10,
        "denied before output allocation: {observed:?}"
    );
    assert_eq!(broker.reserved(), 0);
}
#[test]
fn regexp_extract_owns_wide_output_and_slices_until_final_release() {
    let value = "😀漢é".repeat(8192);
    let batch = input(vec![Some(&value); 128]);
    let broker = Arc::new(TestBroker::new(128 << 20));
    let expr = expression(
        "([^&]*)",
        batch.schema().as_ref(),
        broker.clone(),
        128 << 20,
    );
    let (result, observed) = measure(|| {
        expr.evaluate(&batch)
            .unwrap()
            .into_array(batch.num_rows())
            .unwrap()
    });
    assert!(observed.peak < 128 << 20, "{observed:?}");
    assert!(broker.reserved() >= result.get_array_memory_size());
    let slice = result.slice(1, 2);
    assert_eq!(
        slice.to_data().buffers()[1].as_ptr(),
        result.to_data().buffers()[1].as_ptr()
    );
    drop(result);
    drop(expr);
    assert!(broker.reserved() > 0);
    assert_eq!(
        slice
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        value
    );
    drop(slice);
    assert_eq!(broker.reserved(), 0);
}
#[test]
fn regexp_extract_scalar_broadcast_and_empty_batch_keep_admission() {
    let schema = Schema::empty();
    let value = "漢😀".repeat(4096);
    let literal = Arc::new(Literal::new(ScalarValue::Utf8(Some(value.clone()))));
    let raw = create(literal, "([^&]*)", &schema).unwrap();
    let broker = Arc::new(TestBroker::new(64 << 20));
    let pool = Arc::new(FlinkMemoryPool::new(broker.clone(), 64 << 20)) as Arc<dyn MemoryPool>;
    let expr = super::super::managed_scalar::install(raw, Some(&pool), &schema).unwrap();
    for rows in [0, 64] {
        let batch = RecordBatch::try_new_with_options(
            Arc::new(schema.clone()),
            vec![],
            &arrow::array::RecordBatchOptions::new().with_row_count(Some(rows)),
        )
        .unwrap();
        let result = expr.evaluate(&batch).unwrap().into_array(rows).unwrap();
        assert_eq!(result.len(), rows);
        if rows > 0 {
            assert_eq!(
                result
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(63),
                value
            );
        }
        drop(result);
        assert_eq!(broker.reserved(), 0);
    }
}
#[test]
fn regexp_extract_scoped_case_preserves_input_selection() {
    use datafusion::physical_expr::expressions::CaseExpr;
    let batch = input(vec![Some("a"), None, Some("😀"), Some("b")]);
    let extracted = create(
        Arc::new(Column::new("v", 0)),
        "([^&]*)",
        batch.schema().as_ref(),
    )
    .unwrap();
    let test =
        datafusion::physical_expr::expressions::is_not_null(Arc::new(Column::new("v", 0))).unwrap();
    let case = Arc::new(
        CaseExpr::try_new(
            None,
            vec![(test, extracted)],
            Some(Arc::new(Literal::new(ScalarValue::Utf8(Some(
                "null".into(),
            ))))),
        )
        .unwrap(),
    ) as Arc<dyn PhysicalExpr>;
    let broker = Arc::new(TestBroker::new(64 << 20));
    let pool = Arc::new(FlinkMemoryPool::new(broker.clone(), 64 << 20)) as Arc<dyn MemoryPool>;
    let expr =
        super::super::managed_scalar::install(case, Some(&pool), batch.schema().as_ref()).unwrap();
    let result = expr.evaluate(&batch).unwrap().into_array(4).unwrap();
    assert_eq!(
        result
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some("a"), Some("null"), Some("😀"), Some("b")]
    );
    drop(result);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn regexp_extract_bounded_pattern_compilation_has_fixed_headroom() {
    for source in [
        "a".repeat(256),
        "[^&]".repeat(64),
        "[a-z]".repeat(51),
        "a?".repeat(128),
        "[ -~]".repeat(51),
        "a|".repeat(127),
    ] {
        let pattern = format!("({source})");
        let batch = input(vec![Some("😀漢é\nabcabcabc"), Some("aaaaaaaaa"), None]);
        let expr = create(
            Arc::new(Column::new("v", 0)),
            &pattern,
            batch.schema().as_ref(),
        )
        .unwrap();
        let (_, observed) = measure(|| expr.evaluate(&batch).unwrap());
        eprintln!("pattern {} bytes; peak {}", source.len(), observed.peak);
        assert!(
            observed.peak < 4 << 20,
            "bounded grammar compilation exceeds headroom: {observed:?}"
        );
    }
}
