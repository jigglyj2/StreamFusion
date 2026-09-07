// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use datafusion::error::{DataFusionError, Result};

use super::StateValue;
use crate::memory_pool::HostMemoryReservation;

/// Keeps batch-loaded state and its decoded working representation admitted for
/// the complete computation, independently of output/scratch reservation resizing.
/// The codecs retain byte payloads plus row/map headers and a serialized mutation.
/// Budget from historical state size, never just the current input batch size.
pub(crate) fn reserve_decoded_values(
    values: &[Option<StateValue<'_>>],
    owner: &HostMemoryReservation,
) -> Result<HostMemoryReservation> {
    let bytes = values.iter().flatten().try_fold(0usize, |total, value| {
        total.checked_add(value.len()).ok_or_else(|| {
            DataFusionError::ResourcesExhausted("loaded native state size overflow".to_string())
        })
    })?;
    let mut reservation = owner.sibling("native decoded state batch workspace");
    reservation.try_grow(
        bytes
            .saturating_mul(8)
            .saturating_add(values.len().saturating_mul(64)),
    )?;
    Ok(reservation)
}
