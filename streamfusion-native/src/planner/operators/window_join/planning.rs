// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Native compute contract, independent of the legacy candidate-output handle. Equality
//! partitioning belongs to Flink-compatible state; DataFusion evaluates null filtering and the
//! residual predicate. Merely lowering this contract does not admit the shared runtime.

use super::*;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{
    BinaryExpr, CaseExpr, Column, IsNotNullExpr, Literal,
};
use datafusion::physical_expr::utils::collect_columns;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::joins::utils::JoinFilter;
use std::collections::BTreeMap;

pub(crate) fn is_native_contract(plan: &proto::WindowJoin) -> bool {
    plan.left_input.is_some()
        || plan.right_input.is_some()
        || plan.join_type != proto::RegularJoinType::Unspecified as i32
        || !plan.filter_nulls.is_empty()
        || plan.residual_condition.is_some()
}

/// Only serialize/execute semantics with a concrete DataFusion ordering proof. Other join
/// types remain explicit protocol values, but cannot silently reuse the inner-window proof.
pub(super) fn filter(plan: &proto::WindowJoin) -> Result<Option<JoinFilter>> {
    if plan.join_type != proto::RegularJoinType::Inner as i32 {
        return Err(invalid(
            "native window join currently requires INNER semantics",
        ));
    }
    if plan.left_key_indices.len() != plan.right_key_indices.len()
        || plan.left_key_indices.len() != plan.filter_nulls.len()
    {
        return Err(invalid("window join key and null-filter counts differ"));
    }
    if !(plan.shift_time_zone.is_empty() || plan.shift_time_zone == "UTC") {
        return Err(invalid(
            "native window join currently requires UTC event time",
        ));
    }
    let left = arrow_schema(
        plan.left_schema
            .as_ref()
            .ok_or_else(|| invalid("missing window join left schema"))?,
    )?;
    let right = arrow_schema(
        plan.right_schema
            .as_ref()
            .ok_or_else(|| invalid("missing window join right schema"))?,
    )?;
    for (schema, index) in [
        (&left, plan.left_window_end_index),
        (&right, plan.right_window_end_index),
    ] {
        let field = schema
            .fields()
            .get(index as usize)
            .ok_or_else(|| invalid("window join window-end index outside input"))?;
        if !matches!(
            field.data_type(),
            DataType::Int64 | DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None)
        ) {
            return Err(invalid(
                "window join requires millisecond window-end columns",
            ));
        }
    }
    for (&l, &r) in plan.left_key_indices.iter().zip(&plan.right_key_indices) {
        let a = left
            .fields()
            .get(l as usize)
            .ok_or_else(|| invalid("window join left key outside input"))?;
        let b = right
            .fields()
            .get(r as usize)
            .ok_or_else(|| invalid("window join right key outside input"))?;
        if a.data_type() != b.data_type()
            || !matches!(
                a.data_type(),
                DataType::Boolean
                    | DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::Utf8
                    | DataType::Binary
                    | DataType::Decimal128(_, _)
            )
        {
            return Err(invalid(
                "window join requires matching Flink-compatible scalar equality keys",
            ));
        }
    }
    let fields = left
        .fields()
        .iter()
        .chain(right.fields())
        .enumerate()
        .map(|(i, field)| {
            Field::new(
                format!("window_join_field_{i}"),
                field.data_type().clone(),
                field.is_nullable(),
            )
        })
        .collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(fields));
    let predicate = plan
        .residual_condition
        .as_ref()
        .map(|condition| super::super::calc::create_expression(condition, &schema))
        .transpose()?;
    if let Some(expr) = &predicate {
        if expr.data_type(&schema)? != DataType::Boolean {
            return Err(invalid(
                "window join residual predicate must return BOOLEAN",
            ));
        }
    }
    let mut matchable: Option<Arc<dyn PhysicalExpr>> = None;
    for ((&left_key, &right_key), &filter_nulls) in plan
        .left_key_indices
        .iter()
        .zip(&plan.right_key_indices)
        .zip(&plan.filter_nulls)
    {
        if !filter_nulls {
            continue;
        }
        for index in [left_key as usize, left.fields().len() + right_key as usize] {
            let not_null: Arc<dyn PhysicalExpr> = Arc::new(IsNotNullExpr::new(Arc::new(
                Column::new(schema.field(index).name(), index),
            )));
            matchable = Some(match matchable {
                Some(expr) => Arc::new(BinaryExpr::new(not_null, Operator::And, expr)),
                None => not_null,
            });
        }
    }
    // Flink's null-key wrapper skips the residual entirely. An eager AND could evaluate
    // division/casts on rejected rows and raise an exception Flink would never produce.
    let predicate = match (matchable, predicate) {
        (Some(matchable), Some(residual)) => Some(Arc::new(CaseExpr::try_new(
            None,
            vec![(matchable, residual)],
            Some(Arc::new(Literal::new(
                datafusion::common::ScalarValue::Boolean(Some(false)),
            ))),
        )?) as Arc<dyn PhysicalExpr>),
        (Some(matchable), None) => Some(matchable),
        (None, residual) => residual,
    };
    let Some(predicate) = predicate else {
        return Ok(None);
    };
    // JoinFilter materializes its declared columns for candidate pairs. Include only columns
    // actually read by the predicate, so a wide payload does not multiply filter workspace.
    let mut positions = collect_columns(&predicate)
        .into_iter()
        .map(|c| (c.index(), 0usize))
        .collect::<BTreeMap<_, _>>();
    for (ordinal, (_, position)) in positions.iter_mut().enumerate() {
        *position = ordinal;
    }
    let compact = predicate
        .transform_up(|expr| {
            let Some(column) = expr.downcast_ref::<Column>() else {
                return Ok(Transformed::no(expr));
            };
            let index = positions
                .get(&column.index())
                .copied()
                .ok_or_else(|| invalid("window join predicate column mapping is incomplete"))?;
            Ok(Transformed::yes(
                Arc::new(Column::new(column.name(), index)) as Arc<dyn PhysicalExpr>,
            ))
        })?
        .data;
    let filter_schema = Arc::new(Schema::new(
        positions
            .keys()
            .map(|&i| schema.field(i).clone())
            .collect::<Vec<_>>(),
    ));
    let left_indices = positions
        .keys()
        .copied()
        .filter(|&i| i < left.fields().len())
        .collect();
    let right_indices = positions
        .keys()
        .copied()
        .filter(|&i| i >= left.fields().len())
        .map(|i| i - left.fields().len())
        .collect();
    Ok(Some(JoinFilter::new(
        compact,
        JoinFilter::build_column_indices(left_indices, right_indices),
        filter_schema,
    )))
}

fn invalid(message: &str) -> DataFusionError {
    DataFusionError::Plan(message.into())
}

#[cfg(test)]
pub(crate) mod tests;
