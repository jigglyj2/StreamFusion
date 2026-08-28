// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{DFSchema, NullHandling, UnnestOptions};
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::execution_props::ExecutionProps;
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_expr::expressions::{is_not_null, Column};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::execution_plan::ExecutionPlan;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::unnest::{ListUnnest, UnnestExec};

use crate::proto;

mod map_entries;
mod multiset_entries;
mod ordinality;

const VALUE_COLUMN: &str = "__streamfusion_unnest_value";
const ORDINALITY_COLUMN: &str = "__streamfusion_unnest_ordinality";
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
    let source: Arc<dyn PhysicalExpr> = Arc::new(Column::new(array_field.name(), array_index));
    let collection = proto::UnnestCollection::try_from(unnest.collection).map_err(|_| {
        DataFusionError::Plan(format!(
            "unknown UNNEST collection kind {}",
            unnest.collection
        ))
    })?;
    let is_map = collection == proto::UnnestCollection::Map;
    let is_multiset = collection == proto::UnnestCollection::Multiset;
    let (collection, element_field) = match (collection, array_field.data_type()) {
        (proto::UnnestCollection::Array, DataType::List(element)) => (
            source,
            Arc::new(Field::new(
                VALUE_COLUMN,
                element.data_type().clone(),
                element.is_nullable() || unnest.preserve_empty,
            )),
        ),
        (proto::UnnestCollection::Map, DataType::Map(entries, _)) => (
            map_entries::create(source),
            Arc::new(Field::new(
                VALUE_COLUMN,
                entries.data_type().clone(),
                unnest.preserve_empty,
            )),
        ),
        (proto::UnnestCollection::Multiset, DataType::Map(entries, _)) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(DataFusionError::Plan(
                    "multiset Arrow map entries must be a struct".to_string(),
                ));
            };
            let element = fields.first().ok_or_else(|| {
                DataFusionError::Plan("multiset Arrow map has no element field".to_string())
            })?;
            (
                multiset_entries::create(source),
                Arc::new(Field::new(
                    VALUE_COLUMN,
                    element.data_type().clone(),
                    unnest.preserve_empty,
                )),
            )
        }
        (proto::UnnestCollection::Array, other) => {
            return Err(DataFusionError::Plan(format!(
                "array unnest requires List input, got {other}"
            )));
        }
        (proto::UnnestCollection::Map, other) => {
            return Err(DataFusionError::Plan(format!(
                "map unnest requires Map input, got {other}"
            )));
        }
        (proto::UnnestCollection::Multiset, other) => {
            return Err(DataFusionError::Plan(format!(
                "multiset unnest requires Map input, got {other}"
            )));
        }
    };
    let flatten_array_row =
        !is_map && !is_multiset && matches!(element_field.data_type(), DataType::Struct(_));
    let needs_ordinality = unnest.with_ordinality || (unnest.preserve_empty && flatten_array_row);

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
    projection.push((Arc::clone(&collection), VALUE_COLUMN.to_string()));
    if needs_ordinality {
        projection.push((
            ordinality::create(collection),
            ORDINALITY_COLUMN.to_string(),
        ));
    }
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
    if needs_ordinality {
        output_fields.push(Arc::new(Field::new(
            ORDINALITY_COLUMN,
            DataType::Int32,
            unnest.preserve_empty,
        )));
    }
    output_fields.push(Arc::clone(&child_schema.fields()[visible_field_count]));
    let output_schema = Arc::new(Schema::new(output_fields));
    let mut list_columns = vec![ListUnnest {
        index_in_input_schema: visible_field_count,
        depth: 1,
    }];
    if needs_ordinality {
        list_columns.push(ListUnnest {
            index_in_input_schema: visible_field_count + 1,
            depth: 1,
        });
    }
    let null_handling = if unnest.preserve_empty {
        NullHandling::PreserveAndExpandEmpty
    } else {
        NullHandling::Drop
    };
    let unnested = Arc::new(UnnestExec::new(
        projected,
        list_columns,
        vec![],
        output_schema,
        UnnestOptions::new().with_null_handling(null_handling),
    )?);
    flatten_struct_element(
        unnested,
        visible_field_count,
        unnest.with_ordinality,
        needs_ordinality,
        unnest.preserve_empty,
        !is_map && !is_multiset,
    )
}

fn flatten_struct_element(
    unnested: Arc<dyn ExecutionPlan>,
    visible_field_count: usize,
    with_ordinality: bool,
    has_ordinality: bool,
    preserve_empty: bool,
    skip_null_struct: bool,
) -> Result<Arc<dyn ExecutionPlan>> {
    let schema = unnested.schema();
    let DataType::Struct(fields) = schema.field(visible_field_count).data_type() else {
        return Ok(unnested);
    };
    let fields = fields.clone();
    let unnested: Arc<dyn ExecutionPlan> = if skip_null_struct {
        let predicate = if preserve_empty {
            let df_schema = DFSchema::try_from(Arc::clone(&schema))?;
            let logical = datafusion::logical_expr::col(VALUE_COLUMN)
                .is_not_null()
                .or(datafusion::logical_expr::col(ORDINALITY_COLUMN).is_null());
            create_physical_expr(
                &logical,
                &df_schema,
                &ExecutionProps::default(),
                &PhysicalPlanningContext::default(),
            )?
        } else {
            is_not_null(Arc::new(Column::new(VALUE_COLUMN, visible_field_count)))?
        };
        Arc::new(FilterExec::try_new(predicate, unnested)?)
    } else {
        unnested
    };
    let schema = unnested.schema();
    let mut projection = schema
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
    let df_schema = DFSchema::try_from(Arc::clone(&schema))?;
    for field in &fields {
        let logical = datafusion_functions::expr_fn::get_field(
            datafusion::logical_expr::col(VALUE_COLUMN),
            field.name().as_str(),
        );
        projection.push((
            create_physical_expr(
                &logical,
                &df_schema,
                &ExecutionProps::default(),
                &PhysicalPlanningContext::default(),
            )?,
            field.name().clone(),
        ));
    }
    let ordinal_index = visible_field_count + 1;
    if with_ordinality {
        projection.push((
            Arc::new(Column::new(ORDINALITY_COLUMN, ordinal_index)),
            ORDINALITY_COLUMN.to_string(),
        ));
    }
    let input_row_index = ordinal_index + usize::from(has_ordinality);
    projection.push((
        Arc::new(Column::new(INPUT_ROW_COLUMN, input_row_index)),
        INPUT_ROW_COLUMN.to_string(),
    ));
    Ok(Arc::new(ProjectionExec::try_new(projection, unnested)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int32Array, ListArray, MapArray, RecordBatch, StringArray};
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
                with_ordinality: false,
                preserve_empty: false,
                collection: proto::UnnestCollection::Array as i32,
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

    #[tokio::test]
    async fn emits_one_based_ordinality_for_each_array() {
        let arrays = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>([
            Some(vec![Some(7), Some(8)]),
            Some(vec![Some(9)]),
        ]));
        let ids = Arc::new(Int32Array::from(vec![10, 20]));
        let ordinals = Arc::new(Int32Array::from(vec![0, 1]));
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
                with_ordinality: true,
                preserve_empty: false,
                collection: proto::UnnestCollection::Array as i32,
            },
            source,
        )
        .unwrap();

        let output = collect(plan, SessionContext::new().task_ctx())
            .await
            .unwrap();
        let batch = &output[0];
        assert_eq!(
            batch
                .column(3)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
            &[1, 2, 1]
        );
        assert_eq!(
            batch
                .column(4)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
            &[0, 0, 1]
        );
    }

    #[tokio::test]
    async fn preserves_null_and_empty_arrays_as_null_extended_rows() {
        let arrays = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>([
            Some(vec![Some(7)]),
            Some(vec![]),
            None,
        ]));
        let ids = Arc::new(Int32Array::from(vec![10, 20, 30]));
        let ordinals = Arc::new(Int32Array::from(vec![0, 1, 2]));
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
                with_ordinality: false,
                preserve_empty: true,
                collection: proto::UnnestCollection::Array as i32,
            },
            source,
        )
        .unwrap();

        let output = collect(plan, SessionContext::new().task_ctx())
            .await
            .unwrap();
        let batch = &output[0];
        let values = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(values.iter().collect::<Vec<_>>(), vec![Some(7), None, None]);
        assert_eq!(
            batch
                .column(3)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
            &[0, 1, 2]
        );
    }

    #[tokio::test]
    async fn emits_map_entries_in_stored_order_with_ordinality() {
        let maps = Arc::new(MapArray::from_vec_of_maps::<StringArray, Int32Array, _, _>(
            vec![
                Some(vec![("b", Some(2)), ("a", None)]),
                Some(vec![]),
                None,
                Some(vec![("z", Some(9))]),
            ],
            true,
        ));
        let ids = Arc::new(Int32Array::from(vec![10, 20, 30, 40]));
        let ordinals = Arc::new(Int32Array::from(vec![0, 1, 2, 3]));
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("items", maps.data_type().clone(), true),
            Field::new(INPUT_ROW_COLUMN, DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(schema.clone(), vec![ids, maps, ordinals]).unwrap();
        let source = MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap();
        let plan = create(
            &proto::ArrayUnnest {
                input: None,
                array_index: 1,
                with_ordinality: true,
                preserve_empty: false,
                collection: proto::UnnestCollection::Map as i32,
            },
            source,
        )
        .unwrap();

        let output = collect(plan, SessionContext::new().task_ctx())
            .await
            .unwrap();
        let batch = &output[0];
        assert_eq!(
            batch
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some("b"), Some("a"), Some("z")]
        );
        assert_eq!(
            batch
                .column(3)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(2), None, Some(9)]
        );
        assert_eq!(
            batch
                .column(4)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
            &[1, 2, 1]
        );
    }

    #[tokio::test]
    async fn repeats_multiset_elements_by_count_with_global_ordinality() {
        let multisets = Arc::new(MapArray::from_vec_of_maps::<StringArray, Int32Array, _, _>(
            vec![
                Some(vec![("b", Some(2)), ("a", Some(1)), ("none", Some(0))]),
                Some(vec![("z", Some(2))]),
            ],
            true,
        ));
        let ids = Arc::new(Int32Array::from(vec![10, 20]));
        let ordinals = Arc::new(Int32Array::from(vec![0, 1]));
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("items", multisets.data_type().clone(), true),
            Field::new(INPUT_ROW_COLUMN, DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(schema.clone(), vec![ids, multisets, ordinals]).unwrap();
        let source = MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap();
        let plan = create(
            &proto::ArrayUnnest {
                input: None,
                array_index: 1,
                with_ordinality: true,
                preserve_empty: false,
                collection: proto::UnnestCollection::Multiset as i32,
            },
            source,
        )
        .unwrap();

        let output = collect(plan, SessionContext::new().task_ctx())
            .await
            .unwrap();
        let batch = &output[0];
        assert_eq!(
            batch
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some("b"), Some("b"), Some("a"), Some("z"), Some("z")]
        );
        assert_eq!(
            batch
                .column(3)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values(),
            &[1, 2, 3, 1, 2]
        );
    }
}
