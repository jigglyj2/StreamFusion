// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::ExecutionPlan;

use crate::{decode_plan, proto};

mod expressions;
pub(crate) mod operators;

pub(crate) fn arrow_schema(schema: &proto::Schema) -> Result<arrow::datatypes::SchemaRef> {
    let fields = schema
        .fields
        .iter()
        .map(|field| {
            let logical_type = field.r#type.as_ref().ok_or_else(|| {
                DataFusionError::Plan(format!("field {} type is missing", field.name))
            })?;
            Ok(arrow::datatypes::Field::new(
                &field.name,
                expressions::null_literal::data_type(logical_type)?,
                logical_type.nullable,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(arrow::datatypes::Schema::new(fields)))
}

pub fn create_plan(bytes: &[u8], input: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    create_plan_with_inputs(bytes, vec![input])
}

pub fn create_plan_with_inputs(
    bytes: &[u8],
    inputs: Vec<Arc<dyn ExecutionPlan>>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let plan = decode_plan(bytes)?;
    create_plan_from_decoded(&plan, inputs)
}

pub(crate) fn create_plan_from_decoded(
    plan: &proto::NativePlan,
    inputs: Vec<Arc<dyn ExecutionPlan>>,
) -> Result<Arc<dyn ExecutionPlan>> {
    create_operator(
        plan.root
            .as_ref()
            .ok_or_else(|| DataFusionError::Plan("StreamFusion plan has no root".to_string()))?,
        &inputs,
    )
}

fn create_operator(
    operator: &proto::Operator,
    external_inputs: &[Arc<dyn ExecutionPlan>],
) -> Result<Arc<dyn ExecutionPlan>> {
    match operator.operator.as_ref() {
        Some(proto::operator::Operator::Input(input)) => {
            operators::input::create(input, external_inputs)
        }
        Some(proto::operator::Operator::Calc(calc)) => {
            let child = create_operator(
                calc.input
                    .as_ref()
                    .ok_or_else(|| DataFusionError::Plan("calc has no input".to_string()))?,
                external_inputs,
            )?;
            operators::calc::create(calc, child)
        }
        Some(proto::operator::Operator::ArrayUnnest(unnest)) => {
            let child = create_operator(
                unnest.input.as_ref().ok_or_else(|| {
                    DataFusionError::Plan("array unnest has no input".to_string())
                })?,
                external_inputs,
            )?;
            operators::array_unnest::create(unnest, child)
        }
        Some(proto::operator::Operator::Union(union)) => {
            let children = union
                .inputs
                .iter()
                .map(|input| create_operator(input, external_inputs))
                .collect::<Result<Vec<_>>>()?;
            operators::union::create(union, children)
        }
        Some(proto::operator::Operator::Expand(expand)) => {
            let child = create_operator(
                expand
                    .input
                    .as_ref()
                    .ok_or_else(|| DataFusionError::Plan("expand has no input".to_string()))?,
                external_inputs,
            )?;
            operators::expand::create(expand, child)
        }
        Some(proto::operator::Operator::Values(values)) => operators::values::create(values),
        Some(proto::operator::Operator::WindowTableFunction(window)) => {
            let child = create_operator(
                window.input.as_ref().ok_or_else(|| {
                    DataFusionError::Plan("window table function has no input".to_string())
                })?,
                external_inputs,
            )?;
            operators::window_table_function::create(window, child)
        }
        Some(proto::operator::Operator::Deduplicate(_)) => Err(DataFusionError::Plan(
            "Deduplicate requires a persistent stateful execution handle".to_string(),
        )),
        Some(proto::operator::Operator::GroupAggregate(_)) => Err(DataFusionError::Plan(
            "GroupAggregate requires a persistent stateful execution handle".to_string(),
        )),
        Some(proto::operator::Operator::WindowAggregate(_)) => Err(DataFusionError::Plan(
            "WindowAggregate requires a persistent stateful execution handle".to_string(),
        )),
        Some(proto::operator::Operator::WindowDeduplicate(_)) => Err(DataFusionError::Plan(
            "WindowDeduplicate requires a persistent stateful execution handle".to_string(),
        )),
        Some(proto::operator::Operator::WindowRank(_)) => Err(DataFusionError::Plan(
            "WindowRank requires a persistent stateful execution handle".to_string(),
        )),
        Some(proto::operator::Operator::TopN(_)) => Err(DataFusionError::Plan(
            "TopN requires a persistent stateful execution handle".to_string(),
        )),
        Some(proto::operator::Operator::WindowJoin(_)) => Err(DataFusionError::Plan(
            "WindowJoin requires a persistent stateful execution handle".to_string(),
        )),
        Some(proto::operator::Operator::RegularJoin(_)) => Err(DataFusionError::Plan(
            "RegularJoin requires a persistent stateful execution handle".to_string(),
        )),
        Some(proto::operator::Operator::ChangelogNormalize(_)) => Err(DataFusionError::Plan(
            "ChangelogNormalize requires a persistent stateful execution handle".to_string(),
        )),
        None => Err(DataFusionError::Plan(
            "StreamFusion operator is empty".to_string(),
        )),
    }
}
