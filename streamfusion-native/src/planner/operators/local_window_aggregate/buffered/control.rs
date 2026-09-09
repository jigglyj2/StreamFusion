// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::planner::persistent::control::ControlEvent;

impl BufferedWindow {
    /// Invoke once per event, then drain `poll_pending` before propagating the control.
    pub(crate) fn control(&mut self, event: ControlEvent) -> Result<Option<RecordBatch>> {
        if self.has_pending() {
            return Err(DataFusionError::Execution(
                "local window must drain its input before a control event".into(),
            ));
        }
        let flush = match event {
            ControlEvent::ProcessingTime(timestamp) if self.processing_time => {
                self.advance_buffer_clock(timestamp)
            }
            ControlEvent::Watermark(_) if self.processing_time => false,
            ControlEvent::ProcessingTime(_) => {
                return Err(DataFusionError::Plan(
                    "event-time local window cannot consume a processing-time timer".into(),
                ))
            }
            ControlEvent::BeforeCheckpoint(_) => true,
            // Flink's LocalSlicingWindowAggOperator is not BoundedOneInput. A terminal
            // MAX watermark performs the normal event-time flush; EOF alone does not.
            ControlEvent::EndInput => false,
            ControlEvent::Watermark(watermark) => self.advance_buffer_clock(watermark),
        };
        if flush {
            self.begin_flush()?;
        }
        self.poll_pending()
    }
    fn advance_buffer_clock(&mut self, progress: i64) -> bool {
        if progress <= self.current_progress {
            return false;
        }
        self.current_progress = progress;
        if progress < self.next_trigger_progress {
            return false;
        }
        let interval = match proto::WindowKind::try_from(self.kernel.plan.kind) {
            Ok(proto::WindowKind::Tumble) => self.kernel.plan.size_millis,
            _ => self.kernel.plan.slide_or_step_millis,
        };
        self.next_trigger_progress = if progress == i64::MAX {
            progress
        } else {
            let trigger = window_start(progress, self.kernel.plan.offset_millis, interval)
                .wrapping_add(interval)
                .wrapping_sub(1);
            if trigger > progress {
                trigger
            } else {
                trigger.wrapping_add(interval)
            }
        };
        self.min_slice_end != i64::MAX && progress >= self.min_slice_end.wrapping_sub(1)
    }
}
