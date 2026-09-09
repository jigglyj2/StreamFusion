// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::NativeExecutionContext;
use crate::planner::persistent::control::ControlEvent;
use datafusion::error::{DataFusionError, Result};

impl NativeExecutionContext {
    /// One read per idle edge, not one JNI callback per native timer or row.
    pub(crate) fn processing_time_deadlines(&self) -> Result<Vec<i64>> {
        self.require_idle()?;
        // Two scalar descriptors per already-admitted plan binding fit its bounded control
        // headroom. Do not add per-poll host reservations for these ephemeral descriptors.
        let mut deadlines = Vec::with_capacity(self.persistent.len().saturating_mul(2));
        for (id, factory) in &self.persistent {
            if !factory.supports_control(ControlEvent::ProcessingTime(0)) {
                continue;
            }
            if let Some(deadline) = factory.next_processing_time_timer()? {
                deadlines.push(i64::try_from(*id).map_err(|_| {
                    DataFusionError::Plan(
                        "processing-time stage ID exceeds Java's positive long range".into(),
                    )
                })?);
                deadlines.push(deadline);
            }
        }
        Ok(deadlines)
    }
}
