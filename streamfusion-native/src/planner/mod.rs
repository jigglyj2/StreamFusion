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
    let plan = decode_plan(bytes)?;
    create_operator(
        plan.root
            .as_ref()
            .ok_or_else(|| DataFusionError::Plan("StreamFusion plan has no root".to_string()))?,
        input,
    )
}

fn create_operator(
    operator: &proto::Operator,
    external_input: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    match operator.operator.as_ref() {
        Some(proto::operator::Operator::Input(input)) => {
            operators::input::create(input, external_input)
        }
        Some(proto::operator::Operator::Calc(calc)) => {
            let child = create_operator(
                calc.input
                    .as_ref()
                    .ok_or_else(|| DataFusionError::Plan("calc has no input".to_string()))?,
                external_input,
            )?;
            operators::calc::create(calc, child)
        }
        Some(proto::operator::Operator::ArrayUnnest(unnest)) => {
            let child = create_operator(
                unnest.input.as_ref().ok_or_else(|| {
                    DataFusionError::Plan("array unnest has no input".to_string())
                })?,
                external_input,
            )?;
            operators::array_unnest::create(unnest, child)
        }
        None => Err(DataFusionError::Plan(
            "StreamFusion operator is empty".to_string(),
        )),
    }
}
