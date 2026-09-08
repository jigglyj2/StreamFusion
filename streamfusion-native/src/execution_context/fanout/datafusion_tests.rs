// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::tests::{batch, fixture};
use super::*;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion::physical_plan::{collect, ExecutionPlan};
use datafusion::scalar::ScalarValue;
use std::sync::atomic::Ordering;

struct Port {
    schema: SchemaRef,
    stream: Mutex<Option<SendableRecordBatchStream>>,
}
impl std::fmt::Debug for Port {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("TestSharedPort")
    }
}
impl PartitionStream for Port {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
    fn execute(&self, _: Arc<TaskContext>) -> SendableRecordBatchStream {
        self.stream
            .lock()
            .unwrap()
            .take()
            .expect("test input executed twice")
    }
}
fn input(stream: SendableRecordBatchStream) -> Arc<dyn ExecutionPlan> {
    let schema = stream.schema();
    Arc::new(
        StreamingTableExec::try_new(
            schema.clone(),
            vec![Arc::new(Port {
                schema,
                stream: Mutex::new(Some(stream)),
            })],
            None,
            [],
            false,
            None,
        )
        .unwrap(),
    )
}

#[tokio::test]
async fn different_datafusion_branches_consume_one_source_cooperatively() {
    let (mut readers, polls, completion, _) = fixture(64, 2, false);
    let left = input(readers.remove(0));
    let right = input(readers.remove(0));
    let left: Arc<dyn ExecutionPlan> = Arc::new(
        ProjectionExec::try_new(
            vec![(
                Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("n", 0)),
                    Operator::Plus,
                    Arc::new(Literal::new(ScalarValue::Int32(Some(7)))),
                )) as _,
                "plus_seven".into(),
            )],
            left,
        )
        .unwrap(),
    );
    let right: Arc<dyn ExecutionPlan> = Arc::new(
        ProjectionExec::try_new(
            vec![(Arc::new(Column::new("s", 1)) as _, "text".into())],
            right,
        )
        .unwrap(),
    );
    let task = Arc::new(TaskContext::default());
    let (numbers, text) =
        futures::try_join!(collect(left, task.clone()), collect(right, task)).unwrap();
    assert_eq!(polls.load(Ordering::Relaxed), 65);
    assert_eq!(completion.load(Ordering::Relaxed), 1);
    assert_eq!(numbers.len(), 64);
    assert_eq!(text.len(), 64);
    for index in 0..64 {
        let values = numbers[index]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();
        assert_eq!(
            values.iter().collect::<Vec<_>>(),
            [Some(index as i32 + 7), None]
        );
        assert_eq!(
            text[index].column(0).as_ref(),
            batch(index as i32).column(1).as_ref()
        );
    }
}
