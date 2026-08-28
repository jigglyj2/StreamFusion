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
