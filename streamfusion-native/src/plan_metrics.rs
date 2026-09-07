// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use crate::planner::operators::identified::IdentifiedExec;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

/// One snapshot of the entire native metric tree. Counters describe logical rows
/// at physical-stage boundaries, independent of the number of DataFusion kernels
/// used to implement a stage (such as FilterExec followed by ProjectionExec).
pub(crate) fn snapshot(plan: &Arc<dyn ExecutionPlan>) -> Vec<i64> {
    fn visit(plan: &Arc<dyn ExecutionPlan>, result: &mut Vec<i64>) {
        if let Some(stage) = plan.downcast_ref::<IdentifiedExec>() {
            result.extend([
                stage.plan_node_id() as i64,
                stage.input_rows().min(i64::MAX as u64) as i64,
                stage.output_rows().min(i64::MAX as u64) as i64,
            ]);
        }
        for child in plan.children() {
            visit(child, result);
        }
    }
    let mut result = Vec::new();
    visit(plan, &mut result);
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::operators::reusable_input::ReusableInputExec;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
    use datafusion::physical_plan::collect;
    use datafusion::physical_plan::filter::FilterExecBuilder;
    use datafusion::physical_plan::projection::ProjectionExec;
    use datafusion::prelude::SessionContext;
    use datafusion::scalar::ScalarValue;

    fn filtered_stage(
        id: u64,
        input: Arc<dyn ExecutionPlan>,
        minimum: i32,
    ) -> Arc<dyn ExecutionPlan> {
        let condition = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("id", 0)),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Int32(Some(minimum)))),
        ));
        let filter = Arc::new(FilterExecBuilder::new(condition, input).build().unwrap());
        let projection = Arc::new(
            ProjectionExec::try_new(
                vec![(Arc::new(Column::new("id", 0)) as _, "id".to_owned())],
                filter,
            )
            .unwrap(),
        );
        IdentifiedExec::wrap(id, projection)
    }

    #[tokio::test]
    async fn counts_each_physical_stage_once_across_reused_batches() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let input = Arc::new(ReusableInputExec::new(Arc::clone(&schema)));
        let leaf = IdentifiedExec::wrap(3, input.clone());
        let plan = filtered_stage(1, filtered_stage(2, leaf, 1), 3);
        let context = SessionContext::new();
        assert_eq!(snapshot(&plan), vec![1, 0, 0, 2, 0, 0, 3, 0, 0]);
        for fork in 1..=2 {
            input
                .replace_batch(
                    RecordBatch::try_new(
                        Arc::clone(&schema),
                        vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]))],
                    )
                    .unwrap(),
                )
                .unwrap();
            let output = collect(Arc::clone(&plan), context.task_ctx())
                .await
                .unwrap();
            assert_eq!(output.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
            assert_eq!(
                snapshot(&plan),
                vec![1, 4 * fork, 2 * fork, 2, 5 * fork, 4 * fork, 3, 0, 5 * fork]
            );
        }
    }
}
