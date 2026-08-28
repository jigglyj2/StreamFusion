// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{NullHandling, UnnestOptions};
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::execution_plan::ExecutionPlan;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::unnest::{ListUnnest, UnnestExec};

use crate::proto;

const VALUE_COLUMN: &str = "__streamfusion_unnest_value";
const INPUT_ROW_COLUMN: &str = "__streamfusion_input_row";

pub(crate) fn create(
    unnest: &proto::ArrayUnnest,
    child: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let child_schema = child.schema();
    let visible_field_count = child_schema.fields().len().checked_sub(1).ok_or_else(|| {
        DataFusionError::Plan("array unnest input has no input-row ordinal".to_string())
    })?;
    if child_schema.field(visible_field_count).name() != INPUT_ROW_COLUMN {
        return Err(DataFusionError::Plan(
            "array unnest input-row ordinal must be the final column".to_string(),
        ));
    }
    let array_index = unnest.array_index as usize;
    let array_field = child_schema.fields().get(array_index).filter(|_| array_index < visible_field_count).ok_or_else(|| {
        DataFusionError::Plan(format!(
            "array unnest index {array_index} is outside the {visible_field_count}-column visible input schema"
        ))
    })?;
    let element_field = match array_field.data_type() {
        DataType::List(element) => Arc::new(Field::new(
            VALUE_COLUMN,
            element.data_type().clone(),
            element.is_nullable(),
        )),
        other => {
            return Err(DataFusionError::Plan(format!(
                "array unnest requires List input, got {other}"
            )));
        }
    };

    let mut projection: Vec<(Arc<dyn PhysicalExpr>, String)> = child_schema
        .fields()
        .iter()
        .take(visible_field_count)
        .enumerate()
        .map(|(index, field)| {
            (
                Arc::new(Column::new(field.name(), index)) as Arc<dyn PhysicalExpr>,
                field.name().clone(),
            )
        })
        .collect::<Vec<_>>();
    projection.push((
        Arc::new(Column::new(array_field.name(), array_index)),
        VALUE_COLUMN.to_string(),
    ));
    projection.push((
        Arc::new(Column::new(INPUT_ROW_COLUMN, visible_field_count)),
        INPUT_ROW_COLUMN.to_string(),
    ));
    let projected = Arc::new(ProjectionExec::try_new(projection, child)?);

    let mut output_fields = child_schema
        .fields()
        .iter()
        .take(visible_field_count)
        .cloned()
        .collect::<Vec<_>>();
    output_fields.push(element_field);
    output_fields.push(Arc::clone(&child_schema.fields()[visible_field_count]));
    let output_schema = Arc::new(Schema::new(output_fields));
    Ok(Arc::new(UnnestExec::new(
        projected,
        vec![ListUnnest {
            index_in_input_schema: visible_field_count,
            depth: 1,
        }],
        vec![],
        output_schema,
        UnnestOptions::new().with_null_handling(NullHandling::Drop),
    )?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int32Array, ListArray, RecordBatch};
    use arrow::datatypes::Int32Type;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    async fn drops_null_and_empty_arrays_and_repeats_input_ordinals() {
        let arrays = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>([
            Some(vec![Some(1), Some(2)]),
            Some(vec![]),
            None,
            Some(vec![None, Some(4)]),
        ]));
        let ids = Arc::new(Int32Array::from(vec![10, 20, 30, 40]));
        let ordinals = Arc::new(Int32Array::from(vec![0, 1, 2, 3]));
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("items", arrays.data_type().clone(), true),
            Field::new(INPUT_ROW_COLUMN, DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(schema.clone(), vec![ids, arrays, ordinals]).unwrap();
        let source = MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap();
        let plan = create(
            &proto::ArrayUnnest {
                input: None,
                array_index: 1,
            },
            source,
        )
        .unwrap();

        let output = collect(plan, SessionContext::new().task_ctx())
            .await
            .unwrap();
        let batch = &output[0];
        assert_eq!(batch.num_rows(), 4);
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(10), Some(10), Some(40), Some(40)]
        );
        assert_eq!(
            batch
                .column(3)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
            &[0, 0, 3, 3]
        );
    }
}
