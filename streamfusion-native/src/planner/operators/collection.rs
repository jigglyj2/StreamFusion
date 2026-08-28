// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

use std::sync::Arc;

use arrow::datatypes::Schema;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_expr::PhysicalExpr;

use super::calc::create_expression;
use crate::{planner::expressions, proto};

pub(super) fn create(
    expression: &proto::expression::Expression,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    use proto::expression::Expression::*;
    match expression {
        StructField(field) => {
            let operand = required(&field.operand, "struct field operand is empty")?;
            expressions::struct_field::create(
                create_expression(operand, schema)?,
                &field.field_name,
                schema,
            )
        }
        ArrayElement(element) => expressions::array_element::create(
            create_expression(
                required(&element.array, "array element is missing its array")?,
                schema,
            )?,
            element.index,
            schema,
        ),
        MapElement(element) => expressions::map_element::create(
            create_expression(
                required(&element.map, "map element is missing its map")?,
                schema,
            )?,
            create_expression(
                required(&element.key, "map element is missing its key")?,
                schema,
            )?,
            schema,
        ),
        Cardinality(cardinality) => expressions::cardinality::create(
            create_expression(
                required(
                    &cardinality.collection,
                    "cardinality is missing its collection",
                )?,
                schema,
            )?,
            schema,
        ),
        ArrayContains(contains) => binary(
            &contains.array,
            &contains.needle,
            "array contains is missing its array",
            "array contains is missing its needle",
            schema,
            expressions::array_contains::create,
        ),
        ArrayReverse(reverse) => expressions::array_reverse::create(
            create_expression(
                required(&reverse.array, "array reverse is missing its array")?,
                schema,
            )?,
            schema,
        ),
        ArrayAppend(append) => binary(
            &append.array,
            &append.element,
            "array append is missing its array",
            "array append is missing its element",
            schema,
            expressions::array_append::create,
        ),
        ArrayPrepend(prepend) => binary(
            &prepend.array,
            &prepend.element,
            "array prepend is missing its array",
            "array prepend is missing its element",
            schema,
            expressions::array_prepend::create,
        ),
        ArrayConcat(concat) => expressions::array_concat::create(
            concat
                .arrays
                .iter()
                .map(|array| create_expression(array, schema))
                .collect::<Result<Vec<_>>>()?,
            schema,
        ),
        ArrayPosition(position) => binary(
            &position.array,
            &position.needle,
            "array position is missing its array",
            "array position is missing its needle",
            schema,
            expressions::array_position::create,
        ),
        ArrayRemove(remove) => binary(
            &remove.array,
            &remove.needle,
            "array remove is missing its array",
            "array remove is missing its needle",
            schema,
            expressions::array_remove::create,
        ),
        ArrayMinimum(minimum) => expressions::array_minimum::create(
            create_expression(
                required(&minimum.array, "array minimum is missing its array")?,
                schema,
            )?,
            schema,
        ),
        ArrayMaximum(maximum) => expressions::array_maximum::create(
            create_expression(
                required(&maximum.array, "array maximum is missing its array")?,
                schema,
            )?,
            schema,
        ),
        ArrayJoin(join) => {
            let mut arguments = vec![
                create_expression(
                    required(&join.array, "array join is missing its array")?,
                    schema,
                )?,
                create_expression(
                    required(&join.delimiter, "array join is missing its delimiter")?,
                    schema,
                )?,
            ];
            if let Some(replacement) = &join.null_replacement {
                arguments.push(create_expression(replacement, schema)?);
            }
            expressions::array_join::create(arguments, schema)
        }
        Split(split) => binary(
            &split.value,
            &split.delimiter,
            "split is missing its value",
            "split is missing its delimiter",
            schema,
            expressions::split::create,
        ),
        ArraySort(sort) => expressions::array_sort::create(
            create_expression(
                required(&sort.array, "array sort is missing its array")?,
                schema,
            )?,
            sort.ascending,
            sort.null_first,
            schema,
        ),
        ArraySlice(slice) => expressions::array_slice::create(
            create_expression(
                required(&slice.array, "array slice is missing its array")?,
                schema,
            )?,
            slice.start,
            slice.end,
            schema,
        ),
        RowConstructor(constructor) => {
            if constructor.field_names.len() != constructor.fields.len() {
                return Err(DataFusionError::Plan(
                    "row constructor field names and values have different lengths".into(),
                ));
            }
            expressions::row_constructor::create(
                constructor
                    .field_names
                    .iter()
                    .cloned()
                    .zip(
                        constructor
                            .fields
                            .iter()
                            .map(|field| create_expression(field, schema)),
                    )
                    .map(|(name, field)| field.map(|field| (name, field)))
                    .collect::<Result<Vec<_>>>()?,
                schema,
            )
        }
        MapConstructor(constructor) => {
            if constructor.keys.len() != constructor.values.len() {
                return Err(DataFusionError::Plan(
                    "map constructor keys and values have different lengths".into(),
                ));
            }
            expressions::map_constructor::create(
                constructor
                    .keys
                    .iter()
                    .map(|key| create_expression(key, schema))
                    .collect::<Result<Vec<_>>>()?,
                constructor
                    .values
                    .iter()
                    .map(|value| create_expression(value, schema))
                    .collect::<Result<Vec<_>>>()?,
                schema,
            )
        }
        ArrayDistinct(distinct) => expressions::array_distinct::create(
            create_expression(
                required(&distinct.array, "array distinct is missing its array")?,
                schema,
            )?,
            schema,
        ),
        ArrayUnion(union) => binary_set(
            &union.left,
            &union.right,
            "union",
            schema,
            expressions::array_union::create,
        ),
        ArrayIntersect(intersect) => binary_set(
            &intersect.left,
            &intersect.right,
            "intersect",
            schema,
            expressions::array_intersect::create,
        ),
        ArrayConstructor(constructor) => expressions::array_constructor::create(
            constructor
                .elements
                .iter()
                .map(|element| create_expression(element, schema))
                .collect::<Result<Vec<_>>>()?,
            schema,
        ),
        ArrayExcept(except) => binary_set(
            &except.left,
            &except.right,
            "except",
            schema,
            expressions::array_except::create,
        ),
        _ => Err(DataFusionError::Plan(format!(
            "unsupported native expression variant: {expression:?}"
        ))),
    }
}

fn required<'a>(
    value: &'a Option<Box<proto::Expression>>,
    message: &str,
) -> Result<&'a proto::Expression> {
    value
        .as_deref()
        .ok_or_else(|| DataFusionError::Plan(message.to_string()))
}

fn binary(
    left: &Option<Box<proto::Expression>>,
    right: &Option<Box<proto::Expression>>,
    left_error: &str,
    right_error: &str,
    schema: &Schema,
    create: fn(
        Arc<dyn PhysicalExpr>,
        Arc<dyn PhysicalExpr>,
        &Schema,
    ) -> Result<Arc<dyn PhysicalExpr>>,
) -> Result<Arc<dyn PhysicalExpr>> {
    create(
        create_expression(required(left, left_error)?, schema)?,
        create_expression(required(right, right_error)?, schema)?,
        schema,
    )
}

fn binary_set(
    left: &Option<Box<proto::Expression>>,
    right: &Option<Box<proto::Expression>>,
    name: &str,
    schema: &Schema,
    create: fn(
        Arc<dyn PhysicalExpr>,
        Arc<dyn PhysicalExpr>,
        &Schema,
    ) -> Result<Arc<dyn PhysicalExpr>>,
) -> Result<Arc<dyn PhysicalExpr>> {
    binary(
        left,
        right,
        &format!("array {name} is missing its left array"),
        &format!("array {name} is missing its right array"),
        schema,
        create,
    )
}
