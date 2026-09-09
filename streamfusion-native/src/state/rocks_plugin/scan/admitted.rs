// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

impl RocksPluginKeyedState {
    /// A synchronous state transfer: every Arrow page remains admitted through the visitor.
    /// Source state must stay immutable across calls. A large single value is admitted on its
    /// own, rather than turning the normal page target into a persisted-state compatibility limit.
    pub(crate) fn visit_key_group_admitted(
        &self,
        group: u32,
        max_rows: usize,
        target_bytes: usize,
        owner: &HostMemoryReservation,
        visitor: &mut dyn FnMut(&[(&[u8], &[u8])]) -> Result<()>,
    ) -> Result<()> {
        let rows = u32::try_from(max_rows).map_err(|_| {
            DataFusionError::Execution("state scan row limit exceeds UInt32".into())
        })?;
        let schema = scan_schema();
        let mut after: Option<Vec<u8>> = None;
        let mut cursor_memory = owner.sibling("RocksDB restore scan cursor");
        loop {
            let mut page_memory = owner.sibling("RocksDB restore scan page");
            page_memory.resize(
                4096usize
                    .saturating_add(after.as_ref().map_or(0, |key| key.len().saturating_mul(3))),
            )?;
            let input = scan_input(
                &schema,
                group,
                after.as_deref(),
                rows,
                target_bytes,
                &[],
                None,
            )?;
            let page =
                self.invoke_admitted(self.api.scan_key_group_admitted, input, &mut page_memory)?;
            let complete = scan_complete(&page)?;
            if page.num_rows() == 0 {
                return if complete {
                    Ok(())
                } else {
                    Err(DataFusionError::Execution(
                        "admitted state scan returned an empty incomplete page".into(),
                    ))
                };
            }
            let keys = column::<BinaryViewArray>(&page, 0, "key")?;
            let values = column::<BinaryViewArray>(&page, 1, "value")?;
            if keys.null_count() != 0 || values.null_count() != 0 {
                return Err(DataFusionError::Execution(
                    "admitted state scan returned null state".into(),
                ));
            }
            let entries = (0..page.num_rows())
                .map(|row| (keys.value(row), values.value(row)))
                .collect::<Vec<_>>();
            visitor(&entries)?;
            if complete {
                return Ok(());
            }
            let next = keys.value(keys.len() - 1);
            if after.as_ref().is_some_and(|after| next <= after.as_slice()) {
                return Err(DataFusionError::Execution(
                    "admitted state scan did not advance its cursor".into(),
                ));
            }
            // Retain one coarse cursor allowance for this transfer. Ordinary short keys
            // must not produce tiny admission/release callbacks on every page.
            let required = after
                .as_ref()
                .map_or(0, Vec::capacity)
                .saturating_add(next.len());
            if required > cursor_memory.size() {
                let admitted = required
                    .max(4096)
                    .checked_next_power_of_two()
                    .ok_or_else(|| {
                        DataFusionError::ResourcesExhausted("restore cursor size overflow".into())
                    })?;
                cursor_memory.resize(admitted)?;
            }
            after = Some(next.to_vec());
        }
    }
}
