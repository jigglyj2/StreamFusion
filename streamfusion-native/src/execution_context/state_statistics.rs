// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::NativeExecutionContext;
use crate::{proto, state::rocks_plugin::RocksPluginStatistics};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::memory_pool::MemoryReservation;
use prost::Message;
use streamfusion_state_abi::{validate_rocksdb_tickers, ROCKSDB_TICKER_NAMES};

pub(super) struct StateStatistics {
    pub(super) id: u64,
    pub(super) codes: Vec<u32>,
    pub(super) reader: RocksPluginStatistics,
}

pub(super) fn validate(rocks: &proto::NativeRocksDbState, version: u32) -> Result<()> {
    if !rocks.statistics_tickers.is_empty() && version < 11 {
        return Err(DataFusionError::Plan(
            "RocksDB statistics require state-binding protocol 11".into(),
        ));
    }
    validate_rocksdb_tickers(&rocks.statistics_tickers).map_err(DataFusionError::Plan)
}

impl NativeExecutionContext {
    fn state_statistics(&self) -> &[StateStatistics] {
        self.state_resources
            .as_ref()
            .map_or(&[], |resources| &resources.statistics)
    }

    pub(crate) fn state_statistics_schema(&self) -> Result<(Vec<u8>, MemoryReservation)> {
        let entries = self.state_statistics();
        let count: usize = entries.iter().map(|entry| entry.codes.len()).sum();
        let memory = self.reservation("RocksDB statistics schema");
        memory.try_grow(count.saturating_mul(512).saturating_add(256))?;
        let gauges = entries
            .iter()
            .flat_map(|entry| {
                entry
                    .codes
                    .iter()
                    .map(|&code| proto::NativeGaugeDescriptor {
                        plan_node_id: entry.id,
                        name: ROCKSDB_TICKER_NAMES[code as usize].into(),
                        value_kind: proto::NativeGaugeValueKind::Int64 as i32,
                        metric_kind: proto::NativeMetricKind::Gauge as i32,
                        ..Default::default()
                    })
            })
            .collect();
        Ok((
            proto::NativeGaugeSchema {
                protocol_version: 1,
                gauges,
            }
            .encode_to_vec(),
            memory,
        ))
    }

    /// Only reads independent atomic statistics. Do not take InvocationGuard or an operator
    /// processor lock: Flink's view updater must observe background work between input batches.
    pub(crate) fn state_statistics_snapshot(&self) -> Result<(Vec<i64>, MemoryReservation)> {
        let entries = self.state_statistics();
        let count: usize = entries.iter().map(|entry| entry.codes.len()).sum();
        let memory = self.reservation("RocksDB statistics snapshot");
        memory.try_grow(count.saturating_mul(8).saturating_add(256))?;
        let mut values = vec![0; count];
        let mut offset = 0;
        for entry in entries {
            let end = offset + entry.codes.len();
            entry
                .reader
                .sample(&entry.codes, &mut values[offset..end])?;
            offset = end;
        }
        Ok((values, memory))
    }
}
