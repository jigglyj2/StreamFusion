// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

use std::sync::Arc;

use arrow::datatypes::Schema;
use datafusion::common::config::ConfigOptions;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};

use crate::proto;

pub(crate) fn create(
    value: Arc<dyn PhysicalExpr>,
    characters: Option<Arc<dyn PhysicalExpr>>,
    direction: proto::StringTrimDirection,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    let function = match direction {
        proto::StringTrimDirection::Both => datafusion_functions::string::btrim(),
        proto::StringTrimDirection::Leading => datafusion_functions::string::ltrim(),
        proto::StringTrimDirection::Trailing => datafusion_functions::string::rtrim(),
        proto::StringTrimDirection::Unspecified => {
            return Err(DataFusionError::Plan(
                "string trim direction is unspecified".to_string(),
            ));
        }
    };
    let mut arguments = vec![value];
    if let Some(characters) = characters {
        arguments.push(characters);
    }
    Ok(Arc::new(ScalarFunctionExpr::try_new(
        function,
        arguments,
        schema,
        Arc::new(ConfigOptions::new()),
    )?))
}
