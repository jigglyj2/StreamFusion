// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

//! Invocation-local Flink control events. A control invocation drains children, then each
//! addressed stage, through the ordinary native tree. Invocation EOF alone is never a flush.

use datafusion::error::{DataFusionError, Result};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::execution::TaskContext;
use std::collections::BTreeMap;
use std::sync::Mutex;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ControlEvent {
    Watermark(i64),
    BeforeCheckpoint(u64),
    EndInput,
}

#[derive(Default)]
pub(crate) struct ControlEvents {
    current: Mutex<Option<ControlRequest>>,
}
struct ControlRequest {
    events: BTreeMap<u64, ControlEvent>,
    _memory: MemoryReservation,
}
impl ControlEvents {
    /// Caller admits wire/decode workspace before parsing. Install validates duplicate IDs
    /// and owns the retained request; this decoder never mutates an execution context.
    pub(crate) fn decode(bytes: &[u8]) -> Result<Vec<(u64, ControlEvent)>> {
        use crate::proto::native_stage_control::Event;
        use prost::Message;
        let request = crate::proto::NativeControlInvocation::decode(bytes).map_err(|error| {
            DataFusionError::Plan(format!("invalid native control protobuf: {error}"))
        })?;
        if request.protocol_version != 1 {
            return Err(DataFusionError::Plan(format!(
                "unsupported native control version {}",
                request.protocol_version
            )));
        }
        request
            .stages
            .into_iter()
            .map(|stage| {
                let event = match stage.event {
                    Some(Event::WatermarkMillis(value)) => ControlEvent::Watermark(value),
                    Some(Event::BeforeCheckpoint(value)) => ControlEvent::BeforeCheckpoint(value),
                    Some(Event::EndInput(_)) => ControlEvent::EndInput,
                    None => {
                        return Err(DataFusionError::Plan(
                            "missing or unknown native control event".into(),
                        ))
                    }
                };
                Ok((stage.plan_node_id, event))
            })
            .collect()
    }

    pub(crate) fn install(&self, events: &[(u64, ControlEvent)], task: &TaskContext) -> Result<()> {
        let memory =
            MemoryConsumer::new("native control invocation events").register(task.memory_pool());
        memory.try_grow(
            events
                .len()
                .checked_mul(256)
                .and_then(|n| n.checked_add(4096))
                .ok_or_else(|| {
                    DataFusionError::ResourcesExhausted(
                        "native control event admission overflow".into(),
                    )
                })?,
        )?;
        let mut unique = BTreeMap::new();
        for &(id, event) in events {
            if id == 0 || unique.insert(id, event).is_some() {
                return Err(DataFusionError::Plan(
                    "native control events require unique nonzero stage IDs".into(),
                ));
            }
        }
        let mut current = self.current.lock().map_err(|_| poisoned())?;
        if current.is_some() {
            return Err(DataFusionError::Execution(
                "native control invocation is already installed".into(),
            ));
        }
        *current = Some(ControlRequest {
            events: unique,
            _memory: memory,
        });
        Ok(())
    }

    pub(crate) fn clear(&self) -> Result<()> {
        *self.current.lock().map_err(|_| poisoned())? = None;
        Ok(())
    }

    pub(crate) fn for_stage(task: &TaskContext, id: u64) -> Result<Option<ControlEvent>> {
        let Some(events) = task.session_config().get_extension::<Self>() else {
            return Ok(None);
        };
        let current = events.current.lock().map_err(|_| poisoned())?;
        Ok(current
            .as_ref()
            .and_then(|request| request.events.get(&id).copied()))
    }
}
fn poisoned() -> DataFusionError {
    DataFusionError::Execution("native control invocation lock poisoned".into())
}
