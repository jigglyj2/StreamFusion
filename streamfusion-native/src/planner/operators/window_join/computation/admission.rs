// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Bound large candidate arrays. This is not a ledger for ephemeral expression objects.
//! Expanding string/collection functions need their own kernel admission before use here.

use super::*;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{
    BinaryExpr, CaseExpr, Column, IsNotNullExpr, IsNullExpr, Literal, NotExpr,
};

pub(super) fn scalar_payload(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Null
            | DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
            | DataType::Binary
            | DataType::FixedSizeBinary(_)
            | DataType::Decimal128(_, _)
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Timestamp(_, _)
    )
}

/// Per-candidate capacity for primitive expression arrays and broadcast literal values.
/// Input payload is admitted separately using the maximum encoded row widths, so a wide
/// window is not multiplied by its total left-row count or by every expression descriptor.
pub(super) fn predicate_row_bytes(
    filter: Option<&JoinFilter>,
    input_row_bytes: usize,
) -> Result<usize> {
    let mut bytes = 0usize;
    if let Some(filter) = filter {
        filter.expression().apply(|expr| {
            if let Some(literal) = expr.downcast_ref::<Literal>() {
                if !scalar_payload(&literal.value().data_type()) {
                    return Err(unsupported());
                }
                bytes = bytes.saturating_add(literal.value().size());
            } else if expr.downcast_ref::<Column>().is_some() {
                // Input aliases are covered by the candidate payload bound.
            } else if let Some(binary) = expr.downcast_ref::<BinaryExpr>() {
                if !matches!(
                    binary.op(),
                    Operator::Eq
                        | Operator::NotEq
                        | Operator::Lt
                        | Operator::LtEq
                        | Operator::Gt
                        | Operator::GtEq
                        | Operator::And
                        | Operator::Or
                        | Operator::Plus
                        | Operator::Minus
                        | Operator::Multiply
                        | Operator::Divide
                        | Operator::Modulo
                ) {
                    return Err(unsupported());
                }
                bytes = bytes.saturating_add(32);
                if matches!(binary.op(), Operator::And | Operator::Or) {
                    // Short-circuit evaluation can select a new input batch for the RHS.
                    bytes = bytes.saturating_add(input_row_bytes);
                }
            } else if expr.downcast_ref::<CaseExpr>().is_some()
                || expr.downcast_ref::<IsNullExpr>().is_some()
                || expr.downcast_ref::<IsNotNullExpr>().is_some()
                || expr.downcast_ref::<NotExpr>().is_some()
            {
                bytes = bytes.saturating_add(32);
                if expr.downcast_ref::<CaseExpr>().is_some() {
                    bytes = bytes.saturating_add(input_row_bytes);
                }
            } else {
                return Err(unsupported());
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
    }
    Ok(bytes)
}

fn unsupported() -> DataFusionError {
    DataFusionError::Plan("native window join predicate needs bounded candidate workspace; expanding or unsupported kernels are not admitted".into())
}
