// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use std::sync::Arc;

use arrow::datatypes::{DataType, Schema};
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_expr::expressions::CastExpr;
use datafusion::physical_expr::PhysicalExpr;

use crate::proto;

pub(crate) fn create(
    cast: &proto::Cast,
    operand: Arc<dyn PhysicalExpr>,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    let (approved_source, approved_target) = approved_types(cast.kind())?;
    let actual_source = operand.data_type(schema)?;
    let declared_target = declared_target(cast)?;
    if actual_source != approved_source || declared_target != approved_target {
        return Err(DataFusionError::Plan(format!(
            "cast kind {:?} requires {approved_source} to {approved_target}, got {actual_source} to {declared_target}",
            cast.kind()
        )));
    }
    Ok(Arc::new(CastExpr::new(operand, approved_target, None)))
}

fn approved_types(kind: proto::CastKind) -> Result<(DataType, DataType)> {
    match kind {
        proto::CastKind::TinyintToSmallint => Ok((DataType::Int8, DataType::Int16)),
        proto::CastKind::TinyintToInteger => Ok((DataType::Int8, DataType::Int32)),
        proto::CastKind::TinyintToBigint => Ok((DataType::Int8, DataType::Int64)),
        proto::CastKind::SmallintToInteger => Ok((DataType::Int16, DataType::Int32)),
        proto::CastKind::SmallintToBigint => Ok((DataType::Int16, DataType::Int64)),
        proto::CastKind::IntegerToBigint => Ok((DataType::Int32, DataType::Int64)),
        proto::CastKind::TinyintToFloat => Ok((DataType::Int8, DataType::Float32)),
        proto::CastKind::TinyintToDouble => Ok((DataType::Int8, DataType::Float64)),
        proto::CastKind::SmallintToFloat => Ok((DataType::Int16, DataType::Float32)),
        proto::CastKind::SmallintToDouble => Ok((DataType::Int16, DataType::Float64)),
        proto::CastKind::IntegerToDouble => Ok((DataType::Int32, DataType::Float64)),
        proto::CastKind::FloatToDouble => Ok((DataType::Float32, DataType::Float64)),
        proto::CastKind::Unspecified => Err(DataFusionError::Plan(
            "cast kind is unspecified or unknown".to_string(),
        )),
    }
}

fn declared_target(cast: &proto::Cast) -> Result<DataType> {
    let target = cast
        .target_type
        .as_ref()
        .ok_or_else(|| DataFusionError::Plan("cast target type is empty".to_string()))?;
    match target.r#type {
        Some(proto::logical_type::Type::Smallint(_)) => Ok(DataType::Int16),
        Some(proto::logical_type::Type::Integer(_)) => Ok(DataType::Int32),
        Some(proto::logical_type::Type::Bigint(_)) => Ok(DataType::Int64),
        Some(proto::logical_type::Type::Float(_)) => Ok(DataType::Float32),
        Some(proto::logical_type::Type::Double(_)) => Ok(DataType::Float64),
        _ => Err(DataFusionError::Plan(
            "cast target is not an approved numeric type".to_string(),
        )),
    }
}
