// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{
    BinaryExpr, CastExpr, IsNotNullExpr, IsNullExpr, NegativeExpr, NotExpr,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) enum Policy {
    Fixed,
    ShortCircuit,
    Forward,
}

fn fixed(kind: &DataType) -> bool {
    matches!(
        kind,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Timestamp(_, _)
            | DataType::Duration(_)
            | DataType::Interval(_)
    )
}
impl Policy {
    pub(super) fn for_expression(
        expression: &dyn PhysicalExpr,
        schema: &Schema,
    ) -> Result<Option<Self>> {
        // These kernels only inspect validity; they never copy or materialize the
        // possibly variable-width operand. Its child has its own admission policy.
        if expression.downcast_ref::<IsNullExpr>().is_some()
            || expression.downcast_ref::<IsNotNullExpr>().is_some()
        {
            return Ok(Some(Self::Fixed));
        }
        let binary = expression.downcast_ref::<BinaryExpr>();
        if binary.is_none()
            && expression.downcast_ref::<CastExpr>().is_none()
            && expression.downcast_ref::<NotExpr>().is_none()
            && expression.downcast_ref::<NegativeExpr>().is_none()
            && expression
                .downcast_ref::<super::super::wrapping_cast::SignedIntegerWrappingCastExpr>()
                .is_none()
            && expression
                .downcast_ref::<super::super::decimal::FlinkDecimalExpr>()
                .is_none()
        {
            return Ok(None);
        }
        if !fixed(&expression.data_type(schema)?) {
            return Ok(None);
        }
        for child in expression.children() {
            if !fixed(&child.data_type(schema)?) {
                return Ok(None);
            }
        }
        if let Some(cast) = expression.downcast_ref::<CastExpr>() {
            if cast.expr().data_type(schema)? == *cast.cast_type() {
                return Ok(Some(Self::Forward));
            }
        }
        if let Some(binary) = binary {
            if matches!(binary.op(), Operator::And | Operator::Or) {
                return Ok(Some(Self::ShortCircuit));
            }
            if !matches!(
                binary.op(),
                Operator::Plus
                    | Operator::Minus
                    | Operator::Multiply
                    | Operator::Divide
                    | Operator::Modulo
                    | Operator::Eq
                    | Operator::NotEq
                    | Operator::Lt
                    | Operator::LtEq
                    | Operator::Gt
                    | Operator::GtEq
                    | Operator::IsDistinctFrom
                    | Operator::IsNotDistinctFrom
            ) {
                return Ok(None);
            }
        }
        Ok(Some(Self::Fixed))
    }

    pub(super) fn workspace(self, batch: &RecordBatch, selection: bool) -> Result<usize> {
        // Two scalar-broadcast operands, fixed-width output (up to Decimal256),
        // validity, alignment and a working array. Decimal BigInt slow-path temporaries
        // are bounded by declared precision and reused per row, covered by control credit.
        let bytes = batch
            .num_rows()
            .checked_mul(256)
            .and_then(|n| n.checked_add(64 * 1024));
        let bytes = if selection || self != Self::Fixed {
            bytes.and_then(|bytes| {
                batch.columns().iter().try_fold(bytes, |bytes, array| {
                    bytes.checked_add(array.get_array_memory_size().checked_mul(4)?)
                })
            })
        } else {
            // Numeric kernels can reuse validity storage from a tiny slice of a
            // much larger input. Row count alone does not bound that retained credit.
            bytes.and_then(|bytes| {
                batch.columns().iter().try_fold(bytes, |bytes, array| {
                    bytes.checked_add(super::validity::retained_bytes(array.as_ref())?)
                })
            })
        };
        bytes.ok_or_else(|| {
            DataFusionError::ResourcesExhausted("physical expression workspace overflow".into())
        })
    }
}
