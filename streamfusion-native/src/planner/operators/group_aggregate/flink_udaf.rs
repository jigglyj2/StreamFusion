// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

//! Semantic adapter for operations whose Flink numeric/state behavior differs from DataFusion's
//! built-ins. DataFusion still owns window framing and execution; this UDAF owns only the
//! accumulator contract (integer AVG, decimal overflow, DISTINCT multiplicities and float order).

use super::*;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator as NativeAccumulator, AggregateUDFImpl, Signature, Volatility,
};
use datafusion::scalar::ScalarValue;

#[derive(Debug, PartialEq, Eq, Hash)]
pub(in crate::planner::operators) struct FlinkAggregate {
    call: Call,
    signature: Signature,
}
impl FlinkAggregate {
    pub(in crate::planner::operators) fn new(call: &Call) -> Self {
        let mut call = call.clone();
        call.input_index = call.input_index.map(|_| 0);
        call.filter_index = None; // OVER receives already selected contributions.
        let signature = Signature::exact(
            vec![call.input_type.clone().unwrap_or(DataType::Int8)],
            Volatility::Immutable,
        );
        Self { call, signature }
    }
}
impl AggregateUDFImpl for FlinkAggregate {
    fn name(&self) -> &str {
        "flink_semantic_aggregate"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(self.call.output_type.clone())
    }
    fn accumulator(&self, _: AccumulatorArgs) -> Result<Box<dyn NativeAccumulator>> {
        Ok(Box::new(FlinkAccumulator {
            call: self.call.clone(),
            kernels: datafusion_compute::Kernels::new(std::slice::from_ref(&self.call))?,
            state: AccumulatorState::new(std::slice::from_ref(&self.call)),
        }))
    }
    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<Arc<Field>>> {
        Ok(vec![Arc::new(Field::new(
            format!("{}_flink_state", args.name),
            DataType::Binary,
            false,
        ))])
    }
}
#[derive(Debug)]
struct FlinkAccumulator {
    call: Call,
    kernels: datafusion_compute::Kernels,
    state: AccumulatorState,
}
impl NativeAccumulator for FlinkAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let batch = RecordBatch::try_from_iter(vec![("value", values[0].clone())])?;
        // Integer AVG can reuse DataFusion SUM/COUNT even though its final type is Flink-specific.
        self.state.apply_append_batch(
            std::slice::from_ref(&self.call),
            &self.kernels,
            &batch,
            &(0..batch.num_rows()).collect::<Vec<_>>(),
        )
    }
    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let batch = RecordBatch::try_from_iter(vec![("value", values[0].clone())])?;
        for row in 0..batch.num_rows() {
            self.state
                .apply(std::slice::from_ref(&self.call), &batch, row, false)?;
        }
        Ok(())
    }
    fn supports_retract_batch(&self) -> bool {
        true
    }
    fn evaluate(&mut self) -> Result<ScalarValue> {
        let values = self.state.values(std::slice::from_ref(&self.call));
        let array = aggregate_array(&values, &self.call.output_type)?;
        ScalarValue::try_from_array(&array, 0)
    }
    fn size(&self) -> usize {
        size_of::<Self>() + self.state.estimated_dynamic_bytes()
    }
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Binary(Some(encode_state(&self.state)))])
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let states = states[0]
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                DataFusionError::Internal("Flink accumulator state must be binary".into())
            })?;
        for bytes in states.iter().flatten() {
            self.state.merge(
                std::slice::from_ref(&self.call),
                &decode_state(bytes, std::slice::from_ref(&self.call))?,
            )?;
        }
        Ok(())
    }
}
