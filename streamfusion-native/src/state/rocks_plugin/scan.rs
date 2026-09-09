// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

impl RocksPluginKeyedState {
    pub(super) fn scan_range(
        &self,
        key_group: u32,
        start: &[u8],
        end: Option<&[u8]>,
        max_rows: usize,
        max_bytes: usize,
        visitor: &mut dyn FnMut(&[(&[u8], &[u8])]) -> Result<bool>,
    ) -> Result<()> {
        let rows = u32::try_from(max_rows).map_err(|_| {
            DataFusionError::Execution("state scan row limit exceeds UInt32".to_string())
        })?;
        let schema = Arc::new(Schema::new(vec![
            Field::new("key_group", DataType::UInt32, false),
            Field::new("after", DataType::Binary, true),
            Field::new("max_rows", DataType::UInt32, false),
            Field::new("max_bytes", DataType::UInt64, false),
            Field::new("start", DataType::Binary, false),
            Field::new("end", DataType::Binary, true),
        ]));
        let mut after: Option<Vec<u8>> = None;
        loop {
            let input = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(UInt32Array::from(vec![key_group])),
                    Arc::new(BinaryArray::from(vec![after.as_deref()])),
                    Arc::new(UInt32Array::from(vec![rows])),
                    Arc::new(UInt64Array::from(vec![max_bytes as u64])),
                    Arc::new(BinaryArray::from(vec![start])),
                    Arc::new(BinaryArray::from(vec![end])),
                ],
            )?;
            let output = self.invoke(self.api.scan_key_group, input)?;
            let complete = scan_complete(&output)?;
            if output.num_rows() == 0 {
                return Ok(());
            }
            let keys = column::<BinaryViewArray>(&output, 0, "key")?;
            let values = column::<BinaryViewArray>(&output, 1, "value")?;
            let entries = (0..output.num_rows())
                .map(|row| (keys.value(row), values.value(row)))
                .collect::<Vec<_>>();
            if !visitor(&entries)? || complete {
                return Ok(());
            }
            after = Some(keys.value(keys.len() - 1).to_vec());
        }
    }
}

fn scan_complete(batch: &RecordBatch) -> Result<bool> {
    match batch
        .schema()
        .metadata()
        .get(streamfusion_state_abi::STATE_SCAN_COMPLETE_METADATA)
        .map(String::as_str)
    {
        None | Some("false") => Ok(false),
        Some("true") => Ok(true),
        Some(value) => Err(DataFusionError::Execution(format!(
            "unsupported state scan completion marker: {value}"
        ))),
    }
}

#[cfg(test)]
mod tests;
