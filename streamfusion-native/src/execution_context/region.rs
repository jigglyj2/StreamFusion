// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::{invocation::InvocationGuard, Definition, NativeExecutionContext, PreparedPlan};
use crate::planner::{persistent::control::ControlEvent, region::RegionOutput};
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

/// Own the same context completion across stream construction, unwinding, and all outputs.
/// A constructor may fail before or after taking its callback; completion still happens once.
struct Completion {
    context: Arc<NativeExecutionContext>,
    finished: AtomicBool,
}
impl Completion {
    fn finish(&self, success: bool) {
        if !self.finished.swap(true, Ordering::AcqRel) {
            self.context.finish_invocation(success);
        }
    }
}
impl Drop for Completion {
    fn drop(&mut self) {
        self.finish(false);
    }
}

impl NativeExecutionContext {
    pub(crate) fn region_input_count(&self) -> Result<usize> {
        match &self.plan {
            Definition::Region(plan) => Ok(plan.message.input_count as usize),
            Definition::Tree(_) => Err(DataFusionError::Plan(
                "native tree requires the single-output execution API".into(),
            )),
        }
    }

    pub(crate) fn start_region(
        self: &Arc<Self>,
        batches: Vec<RecordBatch>,
    ) -> Result<RegionOutput> {
        self.start_region_invocation(batches, None)
    }
    pub(crate) fn start_region_control(
        self: &Arc<Self>,
        batches: Vec<RecordBatch>,
        events: &[(u64, ControlEvent)],
    ) -> Result<RegionOutput> {
        self.validate_control_inputs(&batches, events)?;
        self.start_region_invocation(batches, Some(events))
    }
    fn start_region_invocation(
        self: &Arc<Self>,
        batches: Vec<RecordBatch>,
        events: Option<&[(u64, ControlEvent)]>,
    ) -> Result<RegionOutput> {
        if !matches!(self.plan, Definition::Region(_)) {
            return Err(DataFusionError::Plan(
                "native tree requires the single-output execution API".into(),
            ));
        }
        let mut invocation = InvocationGuard::begin(self)?;
        if let Some(events) = events {
            self.controls.install(events, &self.task_context())?;
        }
        let PreparedPlan::Region(region) = self.prepare_execution(batches)? else {
            unreachable!("region API was checked")
        };
        self.set_input_streaming(false)?;
        let completion = Arc::new(Completion {
            context: self.clone(),
            finished: AtomicBool::new(false),
        });
        let callback = completion.clone();
        invocation.transfer_to_stream();
        self.stream_creations.fetch_add(1, Ordering::Relaxed);
        region.start(
            self.task_context(),
            Box::new(move |success| callback.finish(success)),
        )
    }
}
