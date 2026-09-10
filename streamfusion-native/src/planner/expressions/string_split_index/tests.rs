// Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0.
use super::*;
use crate::allocation_test_support::measure;
use crate::memory_pool::{tests_support::TestBroker, FlinkMemoryPool};
use arrow::array::{Array, Int32Array, StringArray};
use datafusion::execution::memory_pool::MemoryPool;

fn input(values: Vec<Option<&str>>, indices: Vec<Option<i32>>) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("v", DataType::Utf8, true),
            Field::new("i", DataType::Int32, true),
        ])),
        vec![
            Arc::new(StringArray::from(values)),
            Arc::new(Int32Array::from(indices)),
        ],
    )
    .unwrap()
}
fn expression(
    delimiter: &str,
    schema: &Schema,
    broker: Arc<TestBroker>,
    budget: usize,
) -> Arc<dyn PhysicalExpr> {
    let pool = Arc::new(FlinkMemoryPool::new(broker, budget)) as Arc<dyn MemoryPool>;
    let raw = create(
        Arc::new(Column::new("v", 0)),
        delimiter,
        Arc::new(Column::new("i", 1)),
        schema,
    )
    .unwrap();
    super::super::managed_scalar::install(raw, Some(&pool), schema).unwrap()
}
#[test]
fn split_index_preserves_empty_tokens_nulls_unicode_and_integer_extremes() {
    let batch = input(
        vec![
            Some("a::b::c"),
            Some("::a::::b::"),
            Some("a::"),
            Some("a::"),
            Some(""),
            None,
            Some("😀::漢é\u{301}"),
            Some("abc"),
            Some("abc"),
            Some("abc"),
            Some("abc"),
        ],
        vec![
            Some(2),
            Some(2),
            Some(1),
            Some(2),
            Some(0),
            Some(0),
            Some(1),
            Some(-1),
            Some(i32::MIN),
            Some(i32::MAX),
            None,
        ],
    );
    let broker = Arc::new(TestBroker::new(64 << 20));
    let expr = expression("::", &batch.schema(), broker.clone(), 64 << 20);
    let result = expr
        .evaluate(&batch)
        .unwrap()
        .into_array(batch.num_rows())
        .unwrap();
    assert_eq!(
        result
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![
            Some("c"),
            Some(""),
            Some(""),
            None,
            None,
            None,
            Some("漢é\u{301}"),
            None,
            None,
            None,
            None
        ]
    );
    drop(result);
    assert_eq!(broker.reserved(), 0);
}
#[test]
fn split_index_denies_growing_workspace_before_splitting() {
    let value = "a/".repeat(128 << 10);
    let batch = input(vec![Some(&value); 128], vec![Some(0); 128]);
    let broker = Arc::new(TestBroker::new(8 << 20));
    let expr = expression("/", &batch.schema(), broker.clone(), 8 << 20);
    let (result, observed) = measure(|| expr.evaluate(&batch));
    assert!(matches!(
        result,
        Err(DataFusionError::ResourcesExhausted(_))
    ));
    assert!(observed.peak < 64 << 10, "{observed:?}");
    assert_eq!(broker.reserved(), 0);
}
#[test]
fn split_index_admits_dense_tokens_and_retains_wide_output_slices() {
    for value in ["/".repeat(8192), "😀漢é".repeat(1024)] {
        let batch = input(vec![Some(&value); 64], vec![Some(0); 64]);
        let broker = Arc::new(TestBroker::new(64 << 20));
        let expr = expression("/", &batch.schema(), broker.clone(), 64 << 20);
        let (result, observed) = measure(|| {
            expr.evaluate(&batch)
                .unwrap()
                .into_array(batch.num_rows())
                .unwrap()
        });
        let workspace = value.len() * 64 * 24 + 64 * 256 + (64 << 10);
        assert!(
            observed.peak < workspace,
            "{observed:?}, workspace {workspace}"
        );
        assert!(broker.reserved() >= result.get_array_memory_size());
        let slice = result.slice(1, 2);
        drop(result);
        drop(expr);
        assert!(broker.reserved() > 0);
        assert_eq!(
            slice
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            if value.starts_with('/') { "" } else { &value }
        );
        drop(slice);
        assert_eq!(broker.reserved(), 0);
    }
}
#[test]
fn split_index_scalar_broadcast_empty_batch_and_multibyte_delimiters() {
    let schema = Schema::empty();
    let raw = create(
        Arc::new(Literal::new(ScalarValue::Utf8(Some("a界😀界".into())))),
        "界",
        Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
        &schema,
    )
    .unwrap();
    let broker = Arc::new(TestBroker::new(8 << 20));
    let pool = Arc::new(FlinkMemoryPool::new(broker.clone(), 8 << 20)) as Arc<dyn MemoryPool>;
    let expr = super::super::managed_scalar::install(raw, Some(&pool), &schema).unwrap();
    for rows in [0, 1, 4096] {
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
                    .value(rows - 1),
                "😀"
            );
        }
        drop(result);
        assert_eq!(broker.reserved(), 0);
    }
}
#[test]
fn split_index_validates_delimiter_and_wire_index_type() {
    let schema = Schema::empty();
    let value = Arc::new(Literal::new(ScalarValue::Utf8(Some("a/b".into()))));
    assert!(create(
        value.clone(),
        "",
        Arc::new(Literal::new(ScalarValue::Int32(Some(0)))),
        &schema
    )
    .is_err());
    assert!(create(
        value,
        "/",
        Arc::new(Literal::new(ScalarValue::Int64(Some(i64::MAX)))),
        &schema
    )
    .is_err());
}
#[test]
fn split_index_scoped_case_preserves_selected_rows() {
    use datafusion::physical_expr::expressions::CaseExpr;
    let batch = input(
        vec![Some("a/b"), None, Some("😀/漢"), Some("")],
        vec![Some(1); 4],
    );
    let split = create(
        Arc::new(Column::new("v", 0)),
        "/",
        Arc::new(Column::new("i", 1)),
        &batch.schema(),
    )
    .unwrap();
    let test =
        datafusion::physical_expr::expressions::is_not_null(Arc::new(Column::new("v", 0))).unwrap();
    let case = Arc::new(
        CaseExpr::try_new(
            None,
            vec![(test, split)],
            Some(Arc::new(Literal::new(ScalarValue::Utf8(Some(
                "null".into(),
            ))))),
        )
        .unwrap(),
    ) as Arc<dyn PhysicalExpr>;
    let broker = Arc::new(TestBroker::new(8 << 20));
    let pool = Arc::new(FlinkMemoryPool::new(broker.clone(), 8 << 20)) as Arc<dyn MemoryPool>;
    let expr = super::super::managed_scalar::install(case, Some(&pool), &batch.schema()).unwrap();
    let result = expr.evaluate(&batch).unwrap().into_array(4).unwrap();
    assert_eq!(
        result
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some("b"), Some("null"), Some("漢"), None]
    );
    drop(result);
    assert_eq!(broker.reserved(), 0);
}
