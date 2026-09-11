// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::memory_pool::HostMemoryReservation;
use datafusion::error::DataFusionError;
use std::mem::size_of;

#[derive(Clone, Copy)]
pub(crate) enum CheckpointSource<'a> {
    Canonical(&'a [u8]),
    Physical(&'a RocksPluginKeyedState),
}

impl CheckpointSource<'_> {
    pub(crate) fn visit(
        &self,
        group: u32,
        owner: &HostMemoryReservation,
        visitor: &mut dyn FnMut(&[u8], &[u8]) -> Result<()>,
    ) -> Result<()> {
        match self {
            Self::Canonical(bytes) => {
                for (key, value) in streamfusion_state_abi::key_group_snapshot_entries(group, bytes)
                    .map_err(|error| DataFusionError::Execution(error.to_string()))?
                {
                    visitor(key, value)?;
                }
                Ok(())
            }
            Self::Physical(source) => {
                source.visit_key_group_admitted(group, 1024, 256 << 10, owner, &mut |page| {
                    for &(key, value) in page {
                        visitor(key, value)?;
                    }
                    Ok(())
                })
            }
        }
    }

    pub(crate) fn validate_unique_keys(
        &self,
        group: u32,
        owner: &HostMemoryReservation,
    ) -> Result<()> {
        if let Self::Canonical(bytes) = self {
            let entries = streamfusion_state_abi::key_group_snapshot_entries(group, bytes)
                .map_err(|error| DataFusionError::Execution(error.to_string()))?;
            let mut memory = owner.sibling("checkpoint borrowed key validation");
            memory.resize(entries.len().saturating_mul(size_of::<&[u8]>() * 2))?;
            let mut keys = entries.map(|(key, _)| key).collect::<Vec<_>>();
            keys.sort_unstable();
            if keys.windows(2).any(|pair| pair[0] == pair[1]) {
                return Err(DataFusionError::Execution(
                    "duplicate checkpoint key".into(),
                ));
            }
        }
        Ok(())
    }

    /// Read one control record while its source owner retains the buffer.
    pub(crate) fn with_value(
        &self,
        group: u32,
        key: &[u8],
        owner: &HostMemoryReservation,
        visitor: &mut dyn FnMut(Option<&[u8]>) -> Result<()>,
    ) -> Result<()> {
        match self {
            Self::Canonical(bytes) => {
                let mut entries = streamfusion_state_abi::key_group_snapshot_entries(group, bytes)
                    .map_err(|error| DataFusionError::Execution(error.to_string()))?;
                visitor(
                    entries
                        .find(|(candidate, _)| *candidate == key)
                        .map(|(_, value)| value),
                )
            }
            Self::Physical(source) => {
                let values = source.get_batch(
                    &[StateKeyRef {
                        key_group: group,
                        key,
                    }],
                    owner,
                )?;
                visitor(values[0].as_ref().map(|value| value.as_ref()))
            }
        }
    }
}
