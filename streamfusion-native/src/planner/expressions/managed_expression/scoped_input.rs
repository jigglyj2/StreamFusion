// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! DataFusion's conditional kernels gather their input batch. Give them only their referenced
//! columns/ROW fields so an unused sibling's payload is neither gathered nor reserved as scratch.
//! The original expression remains the optimizer-visible tree; this cached tree uses local slots.

use super::*;
use datafusion::common::tree_node::{TransformedResult, TreeNode};
use datafusion::physical_expr::expressions::Column;

#[derive(Debug)]
pub(super) struct ScopedInput {
    inputs: Vec<Arc<dyn PhysicalExpr>>,
    pub(super) expression: Arc<dyn PhysicalExpr>,
}

impl ScopedInput {
    pub(super) fn new(expression: Arc<dyn PhysicalExpr>) -> Result<Self> {
        let mut inputs: Vec<Arc<dyn PhysicalExpr>> = Vec::new();
        let expression = expression
            .transform_down(|expression| {
                if !super::super::struct_field::is_input_access(&expression) {
                    return Ok(Transformed::no(expression));
                }
                let index = match inputs.iter().position(|input| input.eq(&expression)) {
                    Some(index) => index,
                    None => {
                        inputs.push(expression);
                        inputs.len() - 1
                    }
                };
                Ok(Transformed::yes(
                    Arc::new(Column::new(&format!("scoped_input_{index}"), index))
                        as Arc<dyn PhysicalExpr>,
                ))
            })
            .data()?;
        Ok(Self { inputs, expression })
    }

    pub(super) fn workspace(&self, rows: usize) -> Result<usize> {
        // Field access borrows values and may combine parent/child validity. Keep one
        // resulting bitmap per selector plus two transient bitmaps while descending a
        // nested ROW. A byte per row leaves headroom over the one-bit validity layout;
        // the fixed allowance covers bitmap alignment and bounded view descriptors.
        self.inputs.len().checked_add(2)
            .and_then(|bitmaps| rows.checked_mul(bitmaps))
            .and_then(|bytes| bytes.checked_add(64 << 10))
            .ok_or_else(|| {
                DataFusionError::ResourcesExhausted(
                    "conditional input view workspace overflow".into(),
                )
            })
    }

    pub(super) fn project(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let mut columns = Vec::with_capacity(self.inputs.len());
        let mut fields = Vec::with_capacity(self.inputs.len());
        for (index, expression) in self.inputs.iter().enumerate() {
            let ColumnarValue::Array(array) = expression.evaluate(batch)? else {
                return Err(DataFusionError::Internal(
                    "conditional input access returned a scalar".into(),
                ));
            };
            fields.push(arrow::datatypes::Field::new(
                format!("scoped_input_{index}"),
                array.data_type().clone(),
                true,
            ));
            columns.push(array);
        }
        Ok(RecordBatch::try_new_with_options(
            Arc::new(Schema::new_with_metadata(
                fields,
                batch.schema().metadata().clone(),
            )),
            columns,
            &arrow::array::RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
        )?)
    }
}
