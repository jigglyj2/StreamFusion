// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::sync::{Arc, Mutex};

#[test]
fn stateful_edge_timestamp_is_not_a_sql_field_and_removal_preserves_array_owners() {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int64, true),
            Field::new(super::ROW_KIND, DataType::Int8, false),
            Field::new(
                crate::exchange::STREAM_RECORD_TIMESTAMP_COLUMN,
                DataType::Int64,
                true,
            ),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![None, Some(42)])),
            Arc::new(Int8Array::from(vec![0, 3])),
            Arc::new(Int64Array::from(vec![None, Some(100)])),
        ],
    )
    .unwrap()
    .slice(1, 1);
    let stripped = super::without_edge_timestamp(batch.clone()).unwrap();
    let owned = super::own_edge_timestamp(batch.clone()).unwrap();
    assert!(Arc::ptr_eq(owned.column(0), batch.column(0)));
    assert!(Arc::ptr_eq(owned.column(1), batch.column(2)));
    assert!(Arc::ptr_eq(owned.column(2), batch.column(1)));
    assert_eq!(owned.schema().field(1).name(), super::OWNED_TIMESTAMP_V1);
    assert!(super::own_edge_timestamp(stripped.clone()).is_err());
    assert_eq!(stripped.num_columns(), 2);
    for (actual, expected) in stripped.columns().iter().zip(batch.columns()) {
        assert!(Arc::ptr_eq(actual, expected));
    }
    let unchanged = super::without_edge_timestamp(stripped.clone()).unwrap();
    assert!(Arc::ptr_eq(unchanged.column(0), stripped.column(0)));
    let malformed = batch.project(&[0, 2]).unwrap();
    assert!(super::without_edge_timestamp(malformed).is_err());
}

use arrow::array::{Array, Int32Array, Int64Array, Int8Array, ListArray, RecordBatch};
use arrow::datatypes::{DataType, Field, Int32Type, Schema};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::physical_plan::{collect, ExecutionPlan};
use datafusion::prelude::SessionContext;
use prost::Message;

use super::{Envelope, INPUT_ROW, ROW_KIND};
use crate::planner::operators::{
    array_unnest, arrow_handoff_tests::Observe, calc, expand, replicate_rows,
};
use crate::proto;

#[tokio::test]
async fn owned_timestamp_envelope_survives_adjacent_calc_without_referencing_an_arrival() {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int64, true),
            Field::new(super::OWNED_TIMESTAMP_V1, DataType::Int64, true),
            Field::new(ROW_KIND, DataType::Int8, false),
            Field::new(INPUT_ROW, DataType::Int32, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![None, Some(1), Some(2), Some(3)])),
            Arc::new(Int64Array::from(vec![
                Some(i64::MIN),
                None,
                Some(0),
                Some(i64::MAX),
            ])),
            Arc::new(Int8Array::from(vec![0, 1, 2, 3])),
            Arc::new(Int32Array::from(vec![-1; 4])),
        ],
    )
    .unwrap()
    .slice(1, 3);
    for depth in [1, 4, 8] {
        let mut plan: Arc<dyn ExecutionPlan> =
            MemorySourceConfig::try_new_exec(&[vec![batch.clone()]], batch.schema(), None).unwrap();
        for _ in 0..depth {
            plan = calc::create(&projection(&[0]), plan).unwrap();
        }
        let result = collect(plan, SessionContext::new().task_ctx())
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(
            Envelope::from_schema(result[0].schema().as_ref())
                .unwrap()
                .payload_width,
            1
        );
        for (actual, expected) in result[0].columns().iter().zip(batch.columns()) {
            assert!(Arc::ptr_eq(actual, expected));
        }
    }
    for (name, datatype) in [
        ("__streamfusion_owned_timestamp_v99", DataType::Int64),
        (super::OWNED_TIMESTAMP_V1, DataType::Int32),
    ] {
        let schema = Schema::new(vec![
            Field::new(name, datatype, true),
            Field::new(ROW_KIND, DataType::Int8, false),
            Field::new(INPUT_ROW, DataType::Int32, false),
        ]);
        assert!(Envelope::from_schema(&schema).is_err());
    }
}

fn reference(index: u32) -> proto::Expression {
    proto::Expression {
        expression: Some(proto::expression::Expression::InputReference(
            proto::InputReference {
                index,
                r#type: None,
            },
        )),
    }
}

fn projection(indices: &[u32]) -> proto::Calc {
    proto::Calc {
        input: None,
        projections: indices.iter().map(|index| reference(*index)).collect(),
        condition: None,
        preserve_input_envelope: true,
    }
}

#[tokio::test]
async fn mixed_native_stages_preserve_every_row_kind_and_sliced_ordinals() {
    for kind_name in [ROW_KIND, "__streamfusion_input_row_kind"] {
        let items = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>([
            Some(vec![Some(99)]),
            Some(vec![Some(10), Some(11)]),
            Some(vec![]),
            Some(vec![None]),
            Some(vec![Some(-7), Some(8)]),
        ]));
        let schema = Arc::new(Schema::new(vec![
            Field::new("count", DataType::Int64, false),
            Field::new("items", items.data_type().clone(), true),
            Field::new(kind_name, DataType::Int8, false),
            Field::new(INPUT_ROW, DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![99, 1, 2, 1, 1])),
                items,
                Arc::new(Int8Array::from(vec![0, 0, 1, 2, 3])),
                Arc::new(Int32Array::from(vec![99, 9, 4, 17, 8])),
            ],
        )
        .unwrap()
        .slice(1, 4);
        let source =
            MemorySourceConfig::try_new_exec(&[vec![batch.clone()]], schema, None).unwrap();
        let first = calc::create(&projection(&[0, 1]), source).unwrap();
        let before = Arc::new(Mutex::new(Vec::new()));
        let first: Arc<dyn ExecutionPlan> = Arc::new(Observe {
            input: first,
            columns: before.clone(),
        });
        let unnested = array_unnest::create(
            &proto::ArrayUnnest {
                input: None,
                array_index: 1,
                with_ordinality: false,
                preserve_empty: true,
                collection: proto::UnnestCollection::Array as i32,
                collection_expression: None,
            },
            first,
        )
        .unwrap();
        let expanded = expand::create(
            &proto::Expand {
                input: None,
                projections: vec![
                    proto::ExpandProjection {
                        expressions: vec![reference(0), reference(2)]
                    };
                    2
                ],
            },
            unnested,
        )
        .unwrap();
        let repeated = replicate_rows::create(
            &proto::ReplicateRows {
                input: None,
                repetition: Some(reference(0)),
                values: vec![reference(1)],
            },
            expanded,
        )
        .unwrap();
        let after = Arc::new(Mutex::new(Vec::new()));
        let repeated: Arc<dyn ExecutionPlan> = Arc::new(Observe {
            input: repeated,
            columns: after.clone(),
        });
        let output = collect(
            calc::create(&projection(&[2]), repeated).unwrap(),
            SessionContext::new().task_ctx(),
        )
        .await
        .unwrap();
        assert_eq!(output.len(), 1);
        let result = &output[0];
        assert_eq!(result.num_columns(), 3);
        assert_eq!(
            result
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![
                Some(10),
                Some(10),
                Some(11),
                Some(11),
                None,
                None,
                None,
                None,
                None,
                None,
                Some(-7),
                Some(-7),
                Some(8),
                Some(8)
            ]
        );
        assert_eq!(
            result
                .column(1)
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .values(),
            &[0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 3, 3, 3, 3]
        );
        assert_eq!(
            result
                .column(2)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
            &[9, 9, 9, 9, 4, 4, 4, 4, 17, 17, 8, 8, 8, 8]
        );
        for (original, projected) in batch.columns().iter().zip(before.lock().unwrap().iter()) {
            assert!(Arc::ptr_eq(original, projected));
        }
        for (index, column) in result.columns().iter().enumerate() {
            assert!(Arc::ptr_eq(column, &after.lock().unwrap()[index + 2]));
        }
    }
}

#[test]
fn rejects_malformed_envelopes_and_sql_access_to_hidden_columns() {
    for fields in [
        vec![],
        vec![Field::new(INPUT_ROW, DataType::Int64, false)],
        vec![Field::new(INPUT_ROW, DataType::Int32, true)],
        vec![
            Field::new(ROW_KIND, DataType::Int32, false),
            Field::new(INPUT_ROW, DataType::Int32, false),
        ],
        vec![
            Field::new(ROW_KIND, DataType::Int8, true),
            Field::new(INPUT_ROW, DataType::Int32, false),
        ],
    ] {
        assert!(Envelope::from_schema(&Schema::new(fields)).is_err());
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::Int32, false),
        Field::new(ROW_KIND, DataType::Int8, false),
        Field::new(INPUT_ROW, DataType::Int32, false),
    ]));
    let source = MemorySourceConfig::try_new_exec(&[vec![]], schema, None).unwrap();
    assert!(calc::create(&projection(&[1]), source.clone())
        .unwrap_err()
        .to_string()
        .contains("SQL projection"));
    let mut predicate = projection(&[0]);
    predicate.condition = Some(reference(2));
    assert!(calc::create(&predicate, source)
        .unwrap_err()
        .to_string()
        .contains("SQL predicate"));
}

#[test]
fn version_two_envelope_contract_survives_plan_decoding() {
    let mut plan = proto::NativePlan {
        protocol_version: 2,
        root: Some(proto::Operator {
            plan_node_id: 0,
            metric_name: String::new(),
            clear_record_timestamps: false,
            metric_uid: None,
            operator: Some(proto::operator::Operator::Calc(Box::new(projection(&[0])))),
        }),
    };
    let decoded = crate::decode_plan(&plan.encode_to_vec()).unwrap();
    let Some(proto::operator::Operator::Calc(calc)) = decoded.root.unwrap().operator else {
        panic!("missing Calc")
    };
    assert!(calc.preserve_input_envelope);
    plan.protocol_version = 1;
    assert!(crate::decode_plan(&plan.encode_to_vec())
        .unwrap_err()
        .to_string()
        .contains("requires plan protocol version 2"));
}
