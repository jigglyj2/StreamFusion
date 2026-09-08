// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::planner::persistent::control::ControlEvent;

impl BufferedWindow {
    /// Invoke once per event, then drain `poll_pending` before propagating the control.
    pub(super) fn control(&mut self, event: ControlEvent) -> Result<Option<RecordBatch>> {
        if self.has_pending() {
            return Err(DataFusionError::Execution(
                "local window must drain its input before a control event".into(),
            ));
        }
        let flush = match event {
            ControlEvent::BeforeCheckpoint(_) => true,
            // Flink's LocalSlicingWindowAggOperator is not BoundedOneInput. A terminal
            // MAX watermark performs the normal event-time flush; EOF alone does not.
            ControlEvent::EndInput => false,
            ControlEvent::Watermark(watermark) => {
                if watermark <= self.current_watermark {
                    return Ok(None);
                }
                self.current_watermark = watermark;
                if watermark < self.next_trigger_watermark {
                    return Ok(None);
                }
                let interval = match proto::WindowKind::try_from(self.kernel.plan.kind) {
                    Ok(proto::WindowKind::Tumble) => self.kernel.plan.size_millis,
                    _ => self.kernel.plan.slide_or_step_millis,
                };
                self.next_trigger_watermark = if watermark == i64::MAX {
                    watermark
                } else {
                    let trigger = window_start(watermark, self.kernel.plan.offset_millis, interval)
                        .wrapping_add(interval)
                        .wrapping_sub(1);
                    if trigger > watermark {
                        trigger
                    } else {
                        trigger.wrapping_add(interval)
                    }
                };
                self.min_slice_end != i64::MAX && watermark >= self.min_slice_end.wrapping_sub(1)
            }
        };
        if flush {
            self.begin_flush()?;
        }
        self.poll_pending()
    }
}
