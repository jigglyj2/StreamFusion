// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::ops::Range;
use std::sync::Arc;

use arrow::datatypes::{DataType, Schema};
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::PhysicalExpr;

pub(crate) const INPUT_ROW: &str = "__streamfusion_input_row";
pub(crate) const ROW_KIND: &str = "__streamfusion_row_kind";
/// Independently versioned owned-record envelope: nullable epoch-millisecond timestamp,
/// then RowKind and ordinal. Owned outputs use ordinal -1 and never refer to an arrival.
pub(crate) const OWNED_TIMESTAMP_V1: &str = "__streamfusion_owned_timestamp_v1";

mod selection;
pub(crate) use selection::{
    owned_timestamp_index, select_output, selected_output_fields, validate_owned_input,
};

/// Protocol 3 inputs carry their record metadata natively, so branches that clear
/// timestamps and branches that preserve them still have the same Arrow schema.
/// Reorder only array references; the Java producer's payload and timestamp buffers
/// remain shared. The caller appends detached ordinals under its import reservation.
pub(crate) fn own_edge_timestamp(
    batch: arrow::record_batch::RecordBatch,
) -> Result<arrow::record_batch::RecordBatch> {
    let schema = batch.schema();
    let last =
        schema.fields().len().checked_sub(1).ok_or_else(|| {
            DataFusionError::Plan("protocol 3 input requires record metadata".into())
        })?;
    if last == 0
        || schema.field(last).name() != crate::exchange::STREAM_RECORD_TIMESTAMP_COLUMN
        || schema.field(last).data_type() != &DataType::Int64
        || schema.field(last - 1).name() != ROW_KIND
        || schema.field(last - 1).data_type() != &DataType::Int8
        || schema.field(last - 1).is_nullable()
    {
        return Err(DataFusionError::Plan(
            "protocol 3 input requires explicit RowKind and timestamp metadata".into(),
        ));
    }
    let mut fields = schema.fields()[..last - 1].to_vec();
    fields.push(Arc::new(arrow::datatypes::Field::new(
        OWNED_TIMESTAMP_V1,
        DataType::Int64,
        true,
    )));
    fields.push(schema.fields()[last - 1].clone());
    let mut columns = batch.columns()[..last - 1].to_vec();
    columns.push(batch.column(last).clone());
    columns.push(batch.column(last - 1).clone());
    Ok(arrow::record_batch::RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}

/// The shared stateful Java edge borrows exchange metadata. Record timestamps stay with the
/// edge's input owner and are selected by global ordinal on output; they are not SQL columns.
/// Remove that one metadata vector before adding native ordinals, retaining every payload and
/// RowKind array verbatim. The imported C Data schema remains cached at the boundary unchanged.
pub(crate) fn without_edge_timestamp(
    batch: arrow::record_batch::RecordBatch,
) -> Result<arrow::record_batch::RecordBatch> {
    let schema = batch.schema();
    let Some(last) = schema.fields().len().checked_sub(1) else {
        return Ok(batch);
    };
    if schema.field(last).name() != crate::exchange::STREAM_RECORD_TIMESTAMP_COLUMN {
        return Ok(batch);
    }
    if last == 0
        || schema.field(last).data_type() != &DataType::Int64
        || schema.field(last - 1).name() != ROW_KIND
        || schema.field(last - 1).data_type() != &DataType::Int8
        || schema.field(last - 1).is_nullable()
    {
        return Err(DataFusionError::Plan(
            "invalid stateful native input edge envelope".into(),
        ));
    }
    Ok(batch.project(&(0..last).collect::<Vec<_>>())?)
}

/// Native envelope columns are not SQL payload. Stateless stages carry them through using
/// the same Arrow selection as the payload, irrespective of their upstream operator family.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Envelope {
    pub(crate) payload_width: usize,
    width: usize,
}

impl Envelope {
    pub(crate) fn from_schema(schema: &Schema) -> Result<Self> {
        let ordinal = schema
            .fields()
            .len()
            .checked_sub(1)
            .ok_or_else(|| DataFusionError::Plan("native input has no envelope".into()))?;
        let field = schema.field(ordinal);
        if field.name() != INPUT_ROW || field.data_type() != &DataType::Int32 || field.is_nullable()
        {
            return Err(DataFusionError::Plan(
                "native input must end with a non-null Int32 input-row ordinal".into(),
            ));
        }
        let has_kind = ordinal > 0
            && matches!(
                schema.field(ordinal - 1).name().as_str(),
                ROW_KIND | "__streamfusion_input_row_kind"
            );
        if has_kind {
            let field = schema.field(ordinal - 1);
            if field.data_type() != &DataType::Int8 || field.is_nullable() {
                return Err(DataFusionError::Plan(
                    "native RowKind must be a non-null Int8 column".into(),
                ));
            }
        }
        let owned =
            has_kind && ordinal > 1 && schema.field(ordinal - 2).name() == OWNED_TIMESTAMP_V1;
        if schema
            .fields()
            .iter()
            .filter(|field| field.name().starts_with("__streamfusion_owned_timestamp_"))
            .count()
            != usize::from(owned)
        {
            return Err(DataFusionError::Plan(
                "unsupported or misplaced owned native envelope version".into(),
            ));
        }
        if owned && schema.field(ordinal - 2).data_type() != &DataType::Int64 {
            return Err(DataFusionError::Plan(
                "owned native timestamp v1 must be Int64".into(),
            ));
        }
        let width = 1 + usize::from(has_kind) + usize::from(owned);
        Ok(Self {
            payload_width: schema.fields().len() - width,
            width,
        })
    }

    pub(crate) fn indices(self) -> Range<usize> {
        self.payload_width..self.payload_width + self.width
    }

    pub(crate) fn expressions(self, schema: &Schema) -> Vec<(Arc<dyn PhysicalExpr>, String)> {
        self.indices()
            .map(|index| {
                let name = schema.field(index).name();
                (
                    Arc::new(Column::new(name, index)) as Arc<dyn PhysicalExpr>,
                    name.clone(),
                )
            })
            .collect()
    }
}

#[cfg(test)]
mod tests;
