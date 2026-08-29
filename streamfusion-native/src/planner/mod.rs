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
mod operators;

pub fn create_plan(bytes: &[u8], input: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    create_plan_with_inputs(bytes, vec![input])
}

pub fn create_plan_with_inputs(
    bytes: &[u8],
    inputs: Vec<Arc<dyn ExecutionPlan>>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let plan = decode_plan(bytes)?;
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
        None => Err(DataFusionError::Plan(
            "StreamFusion operator is empty".to_string(),
        )),
    }
}
