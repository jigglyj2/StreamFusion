// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use arrow::array::{Array, Int32Array, Int64Array, Int8Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext;
use prost::Message;

fn batch() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("rowtime_payload", DataType::Int64, true),
            Field::new(OWNED_TIMESTAMP_V1, DataType::Int64, true),
            Field::new(ROW_KIND, DataType::Int8, false),
            Field::new(INPUT_ROW, DataType::Int32, false),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![
                None,
                Some(i64::MIN),
                Some(0),
                Some(i64::MAX),
            ])),
            Arc::new(Int64Array::from(vec![None, Some(-9), Some(0), Some(99)])),
            Arc::new(Int8Array::from(vec![0, 1, 2, 3])),
            Arc::new(Int32Array::from(vec![-1; 4])),
        ],
    )
    .unwrap()
}

#[tokio::test]
async fn generic_stage_policy_preserves_payload_owners_and_all_changelog_kinds() {
    for owned in [false, true] {
        let input = if owned {
            batch()
        } else {
            batch().project(&[0, 2, 3]).unwrap()
        }
        .slice(1, 3);
        let source =
            MemorySourceConfig::try_new_exec(&[vec![input.clone()]], input.schema(), None).unwrap();
        // Deliberately attach the policy to an Input, not Calc/Expand: lowering is generic.
        let plan = crate::proto::NativePlan {
            protocol_version: crate::RECORD_POLICY_PLAN_PROTOCOL_VERSION,
            root: Some(crate::proto::Operator {
                clear_record_timestamps: true,
                operator: Some(crate::proto::operator::Operator::Input(Default::default())),
                ..Default::default()
            }),
        };
        let output = collect(
            crate::planner::create_plan(&plan.encode_to_vec(), source).unwrap(),
            SessionContext::new().task_ctx(),
        )
        .await
        .unwrap();
        assert!(Arc::ptr_eq(output[0].column(0), input.column(0)));
        assert!(Arc::ptr_eq(
            output[0].column(2),
            input.column(input.num_columns() - 2)
        ));
        assert_eq!(output[0].column(1).null_count(), 3);
        assert_eq!(
            output[0]
                .column(3)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
            &[-1; 3]
        );
        for version in [1, 2] {
            let mut invalid = plan.clone();
            invalid.protocol_version = version;
            assert!(crate::decode_plan(&invalid.encode_to_vec())
                .unwrap_err()
                .to_string()
                .contains("timestamp policy requires"));
        }
    }
}

#[tokio::test]
async fn metadata_materialization_is_admitted_and_released() {
    use crate::memory_pool::{tests_support::TestBroker, FlinkMemoryPool};
    for limit in [0, 1 << 20] {
        let input = batch();
        let broker = Arc::new(TestBroker::new(limit));
        let pool: Arc<dyn MemoryPool> = Arc::new(FlinkMemoryPool::new(broker.clone(), limit));
        let source =
            MemorySourceConfig::try_new_exec(&[vec![input.clone()]], input.schema(), None).unwrap();
        let plan = clear_timestamps(source, Some(&pool)).unwrap();
        let output = collect(plan, SessionContext::new().task_ctx()).await;
        if limit == 0 {
            assert!(output.is_err());
        } else {
            assert!(output.is_ok());
            assert!(broker.reserved() > 0);
        }
        drop(output);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn missing_row_kind_fails_without_inventing_insert_semantics() {
    let input = batch().project(&[0, 3]).unwrap();
    let source =
        MemorySourceConfig::try_new_exec(&[vec![input.clone()]], input.schema(), None).unwrap();
    assert!(clear_timestamps(source, None)
        .unwrap_err()
        .to_string()
        .contains("explicit native RowKind"));
}
