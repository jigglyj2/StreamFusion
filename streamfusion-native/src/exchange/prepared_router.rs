// Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0.

use super::{
    decode_exchange_plan, exchange_key_fields,
    managed_routing::{route_record_batch, AccountedFrames},
    KeyField,
};
use crate::memory_pool::{HostMemoryReservation, MemoryReservationBroker};
use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use datafusion::error::{DataFusionError, Result};
use std::sync::Arc;

/// One prepared router serves both source-edge C Data and native-plan output batches.
pub(crate) struct PreparedRouter {
    plan: crate::proto::NativeExchangePlan,
    keys: Vec<(usize, KeyField)>,
    schema: SchemaRef,
    broker: Arc<dyn MemoryReservationBroker>,
    _memory: HostMemoryReservation,
}
impl PreparedRouter {
    pub(crate) fn new(
        bytes: &[u8],
        broker: Arc<dyn MemoryReservationBroker>,
        memory: HostMemoryReservation,
    ) -> Result<Self> {
        let plan = decode_exchange_plan(bytes)?;
        let keys = exchange_key_fields(&plan)?;
        let schema =
            crate::planner::arrow_schema(plan.schema.as_ref().expect("validated exchange schema"))?;
        Ok(Self {
            plan,
            keys,
            schema,
            broker,
            _memory: memory,
        })
    }
    pub(crate) fn route(&self, batch: RecordBatch) -> Result<AccountedFrames> {
        route_record_batch(&self.plan, &self.keys, batch, self.broker.clone())
    }
    pub(crate) fn validate_native_output(&self) -> Result<()> {
        if self
            .keys
            .iter()
            .any(|(_, key)| *key == KeyField::PreencodedBinaryRow)
        {
            return Err(DataFusionError::Plan(
                "native output routing requires an independently verified native key encoder"
                    .into(),
            ));
        }
        Ok(())
    }
    pub(crate) fn route_owned(&self, batch: RecordBatch) -> Result<AccountedFrames> {
        self.validate_native_output()?;
        // Native envelopes end with timestamp, RowKind, detached ordinal. Exchange carries
        // payload, RowKind, timestamp. Reorder descriptors and drop the ordinal; no transpose.
        let fields = self.schema.fields().len().checked_sub(2).ok_or_else(|| {
            DataFusionError::Plan("exchange output schema omits record metadata".into())
        })?;
        let input = batch.schema();
        if batch.num_columns() != fields + 3
            || input.field(fields).name() != crate::planner::operators::envelope::OWNED_TIMESTAMP_V1
            || input.field(fields + 1).name() != super::ROW_KIND_COLUMN
            || input.field(fields + 2).name() != "__streamfusion_input_row"
        {
            return Err(DataFusionError::Execution(
                "native exchange output requires its owned record envelope".into(),
            ));
        }
        let mut columns = batch.columns()[..fields].to_vec();
        columns.push(batch.column(fields + 1).clone());
        columns.push(batch.column(fields).clone());
        let transport = RecordBatch::try_new(self.schema.clone(), columns)?;
        self.route(transport)
    }
}
