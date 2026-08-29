// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::ExecutionPlan;

use crate::proto;

pub(crate) fn create(
    _union: &proto::Union,
    inputs: Vec<Arc<dyn ExecutionPlan>>,
) -> Result<Arc<dyn ExecutionPlan>> {
    if inputs.len() < 2 {
        return Err(DataFusionError::Plan(
            "StreamFusion UNION ALL requires at least two inputs".to_string(),
        ));
    }
    UnionExec::try_new(inputs)
}
