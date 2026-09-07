// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::sync::{Arc, Mutex};

use arrow::array::{Array, ArrayRef, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    collect, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
};
use datafusion::prelude::SessionContext;
use futures::StreamExt;

use crate::planner::operators::{
    calc, expand, identified::IdentifiedExec, reusable_input::ReusableInputExec,
};
use crate::proto;

#[tokio::test]
async fn calc_filter_passes_whole_batches_without_coalescing_or_single_row_splitting() {
    use datafusion::datasource::memory::MemorySourceConfig;
    let schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::Utf8, true),
        Field::new("__streamfusion_input_row", DataType::Int32, false),
    ]));
    let batches = [1, 7, 4096]
        .into_iter()
        .map(|rows| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from_iter(
                        (0..rows).map(|row| (row % 3 != 0).then(|| format!("é-{row}"))),
                    )),
                    Arc::new(Int32Array::from_iter_values(0..rows as i32)),
                ],
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let child = MemorySourceConfig::try_new_exec(&[batches.clone()], schema, None).unwrap();
    let definition = proto::Calc {
        preserve_input_envelope: false,
        input: None,
        projections: vec![reference(0), reference(1)],
        condition: Some(proto::Expression {
            expression: Some(proto::expression::Expression::BooleanLiteral(
                proto::BooleanLiteral { value: true },
            )),
        }),
    };
    let plan = calc::create(&definition, child).unwrap();
    let mut stream = plan.execute(0, SessionContext::new().task_ctx()).unwrap();
    for expected in batches {
        let actual = stream.next().await.unwrap().unwrap();
        assert_eq!(actual.num_rows(), expected.num_rows());
        for (actual, expected) in actual.columns().iter().zip(expected.columns()) {
            let actual = actual.to_data();
            let expected = expected.to_data();
            assert_eq!(actual, expected);
            for (actual, expected) in actual.buffers().iter().zip(expected.buffers()) {
                assert_eq!(actual.as_ptr(), expected.as_ptr());
            }
        }
    }
    assert!(stream.next().await.is_none());
}

/// An observer at a native plan edge. It retains only shared array references and forwards the
/// original RecordBatch, so tests can assert buffer identity without any production instrumentation.
#[derive(Debug)]
pub(crate) struct Observe {
    pub(crate) input: Arc<dyn ExecutionPlan>,
    pub(crate) columns: Arc<Mutex<Vec<ArrayRef>>>,
}

impl DisplayAs for Observe {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "ObserveArrowHandoff")
    }
}

impl ExecutionPlan for Observe {
    fn name(&self) -> &str {
        "ObserveArrowHandoff"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        assert_eq!(children.len(), 1);
        Ok(Arc::new(Self {
            input: children[0].clone(),
            columns: self.columns.clone(),
        }))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let columns = self.columns.clone();
        let stream = self.input.execute(partition, context)?.map(move |result| {
            if let Ok(batch) = &result {
                *columns.lock().unwrap() = batch.columns().to_vec();
            }
            result
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

#[tokio::test]
async fn calc_expand_calc_handoff_shares_buffers_and_counts_stages_independently() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::Utf8, true),
        Field::new("__streamfusion_input_row", DataType::Int32, false),
    ]));
    let payload: ArrayRef = Arc::new(StringArray::from(vec![
        Some("é"),
        None,
        Some("wide payload"),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![payload.clone(), Arc::new(Int32Array::from(vec![0, 1, 2]))],
    )
    .unwrap();
    let input = Arc::new(ReusableInputExec::new(schema));
    input.replace_batch(batch).unwrap();
    let calc = proto::Calc {
        preserve_input_envelope: false,
        input: None,
        projections: vec![reference(0), reference(1)],
        condition: None,
    };
    let before = Arc::new(Mutex::new(Vec::new()));
    let after = Arc::new(Mutex::new(Vec::new()));
    let first = IdentifiedExec::wrap(
        3,
        calc::create(&calc, IdentifiedExec::wrap(4, input.clone())).unwrap(),
    );
    let first: Arc<dyn ExecutionPlan> = Arc::new(Observe {
        input: first,
        columns: before.clone(),
    });
    let expanded = IdentifiedExec::wrap(
        2,
        expand::create(
            &proto::Expand {
                input: None,
                projections: vec![
                    proto::ExpandProjection {
                        expressions: vec![reference(0)]
                    };
                    2
                ],
            },
            first,
        )
        .unwrap(),
    );
    let expanded: Arc<dyn ExecutionPlan> = Arc::new(Observe {
        input: expanded,
        columns: after.clone(),
    });
    let plan = IdentifiedExec::wrap(1, calc::create(&calc, expanded).unwrap());
    let output = collect(plan.clone(), SessionContext::new().task_ctx())
        .await
        .unwrap();
    assert_eq!(output.len(), 1);
    assert_eq!(output[0].num_rows(), 6);
    assert!(Arc::ptr_eq(&payload, &before.lock().unwrap()[0]));
    for (source, output) in after.lock().unwrap().iter().zip(output[0].columns()) {
        assert!(Arc::ptr_eq(source, output));
        assert_eq!(
            source.to_data().buffers()[0].as_ptr(),
            output.to_data().buffers()[0].as_ptr()
        );
    }
    assert_eq!(
        crate::plan_metrics::snapshot(&plan),
        vec![1, 6, 6, 2, 3, 6, 3, 3, 3, 4, 0, 3]
    );
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
