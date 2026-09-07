// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::NativeExecutionContext;
use crate::proto;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::memory_pool::MemoryReservation;
use prost::Message;

impl NativeExecutionContext {
    pub(crate) fn gauge_schema(&self) -> Result<(Vec<u8>, MemoryReservation)> {
        self.require_idle()?;
        let memory = self.reservation("native gauge schema protobuf");
        let mut bytes = 4096usize;
        for (_, factory) in &self.persistent {
            for gauge in factory.gauge_definitions()? {
                bytes = bytes
                    .saturating_add(512)
                    .saturating_add(gauge.name.len().saturating_mul(4));
                for group in gauge.groups {
                    bytes = bytes
                        .saturating_add(128)
                        .saturating_add(group.len().saturating_mul(4));
                }
            }
        }
        memory.try_grow(bytes)?;
        let mut gauges = Vec::new();
        for (id, factory) in &self.persistent {
            for gauge in factory.gauge_definitions()? {
                gauges.push(proto::NativeGaugeDescriptor {
                    plan_node_id: *id,
                    groups: gauge
                        .groups
                        .iter()
                        .map(|group| (*group).to_owned())
                        .collect(),
                    name: gauge.name.to_owned(),
                    value_kind: gauge.kind as i32,
                });
            }
        }
        Ok((
            proto::NativeGaugeSchema {
                protocol_version: 1,
                gauges,
            }
            .encode_to_vec(),
            memory,
        ))
    }

    pub(crate) fn gauge_snapshot(&self) -> Result<(Vec<i64>, MemoryReservation)> {
        let count = self
            .persistent
            .iter()
            .try_fold(0usize, |count, (_, factory)| {
                count
                    .checked_add(factory.gauge_definitions()?.len())
                    .ok_or_else(|| {
                        DataFusionError::ResourcesExhausted("native gauge count overflow".into())
                    })
            })?;
        let memory = self.reservation("native gauge values");
        memory.try_grow(
            count
                .saturating_mul(std::mem::size_of::<i64>())
                .saturating_add(256),
        )?;
        let mut values = vec![0; count];
        let mut offset = 0;
        for (_, factory) in &self.persistent {
            let end = offset + factory.gauge_definitions()?.len();
            factory.write_gauge_values(values.get_mut(offset..end).ok_or_else(|| {
                DataFusionError::Execution("native gauge schema changed".into())
            })?)?;
            offset = end;
        }
        Ok((values, memory))
    }
}
