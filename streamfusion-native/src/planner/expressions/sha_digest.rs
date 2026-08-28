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
use datafusion::physical_expr::expressions::Literal;
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion::scalar::ScalarValue;
use datafusion_functions::crypto::{sha224, sha256, sha384, sha512};
use datafusion_functions::encoding::encode;

use crate::proto;

pub(crate) fn create(
    operand: Arc<dyn PhysicalExpr>,
    algorithm: proto::ShaAlgorithm,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    let digest_function = match algorithm {
        proto::ShaAlgorithm::ShaAlgorithm224 => sha224(),
        proto::ShaAlgorithm::ShaAlgorithm256 => sha256(),
        proto::ShaAlgorithm::ShaAlgorithm384 => sha384(),
        proto::ShaAlgorithm::ShaAlgorithm512 => sha512(),
        proto::ShaAlgorithm::Unspecified => {
            return Err(DataFusionError::Plan(
                "SHA digest algorithm is unspecified".to_string(),
            ));
        }
    };
    let digest = Arc::new(ScalarFunctionExpr::try_new(
        digest_function,
        vec![operand],
        schema,
        Arc::new(ConfigOptions::new()),
    )?) as Arc<dyn PhysicalExpr>;
    Ok(Arc::new(ScalarFunctionExpr::try_new(
        encode(),
        vec![
            digest,
            Arc::new(Literal::new(ScalarValue::Utf8(Some("hex".to_string())))),
        ],
        schema,
        Arc::new(ConfigOptions::new()),
    )?))
}
