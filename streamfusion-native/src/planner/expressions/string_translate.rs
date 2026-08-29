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
use datafusion::error::Result;
use datafusion::physical_expr::expressions::{CaseExpr, IsNullExpr, Literal};
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion::scalar::ScalarValue;

pub(crate) fn create(
    value: Arc<dyn PhysicalExpr>,
    source_characters: Arc<dyn PhysicalExpr>,
    target_characters: Arc<dyn PhysicalExpr>,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    // Flink interprets a null target alphabet as empty, deleting every matched
    // source character. DataFusion is null-strict, so encode Flink's rule here.
    let target_or_empty = super::coalesce::create(vec![
        target_characters,
        Arc::new(Literal::new(ScalarValue::Utf8(Some(String::new())))),
    ])?;
    let source_is_null =
        Arc::new(IsNullExpr::new(Arc::clone(&source_characters))) as Arc<dyn PhysicalExpr>;
    let translated = Arc::new(ScalarFunctionExpr::try_new(
        datafusion_functions::unicode::translate(),
        vec![Arc::clone(&value), source_characters, target_or_empty],
        schema,
        Arc::new(ConfigOptions::new()),
    )?) as Arc<dyn PhysicalExpr>;
    // Flink also returns the original value when the source alphabet is null.
    Ok(Arc::new(CaseExpr::try_new(
        None,
        vec![(source_is_null, value)],
        Some(translated),
    )?))
}
