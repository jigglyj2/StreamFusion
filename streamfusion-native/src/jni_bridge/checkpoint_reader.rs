// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use crate::state::RocksPluginKeyedState;
use datafusion::error::{DataFusionError, Result};
use std::path::Path;

/// One initialization-only reader shared by standalone operator JNI bridges. The caller owns
/// its admitted temporary-cache allowance and must discard the target after a partial import.
/// The closure retains operator-specific state validation, timer rebuilds, and output resets.
pub(super) fn import(
    plugin: &Path,
    checkpoint: &Path,
    first: u32,
    last: u32,
    memory_limit: usize,
    mut restore: impl FnMut(u32, &RocksPluginKeyedState) -> Result<()>,
) -> Result<()> {
    if first > last {
        return Err(DataFusionError::Execution(
            "invalid checkpoint key-group range".into(),
        ));
    }
    let source =
        RocksPluginKeyedState::open_checkpoint(plugin, checkpoint, first, last, memory_limit)?;
    for group in first..=last {
        restore(group, &source)?;
    }
    Ok(())
}
