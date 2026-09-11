// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use arrow::array::{Array, BinaryViewBuilder};
use std::cmp::Ordering;

impl CanonicalFile {
    fn compare(&self, entry: Entry, key: &[u8], scratch: &mut [u8]) -> Result<Ordering> {
        let common = entry.key_len.min(key.len() as u64) as usize;
        let mut offset = 0;
        while offset < common {
            let count = (common - offset).min(scratch.len());
            read_at(&self.data, entry.key + offset as u64, &mut scratch[..count])?;
            let order = scratch[..count].cmp(&key[offset..offset + count]);
            if order != Ordering::Equal {
                return Ok(order);
            }
            offset += count;
        }
        Ok(entry.key_len.cmp(&(key.len() as u64)))
    }

    fn lower_bound(&self, key: &[u8], scratch: &mut [u8]) -> Result<u64> {
        let (mut low, mut high) = (0, self.count);
        while low < high {
            let mid = low + (high - low) / 2;
            if self.compare(self.entry(mid)?, key, scratch)? == Ordering::Less {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        Ok(low)
    }

    fn range(
        &self,
        group: u32,
        start: &[u8],
        end: Option<&[u8]>,
        max_rows: usize,
        max_bytes: usize,
        visitor: &mut dyn FnMut(&[(&[u8], &[u8])]) -> Result<bool>,
    ) -> Result<()> {
        if group != self.group {
            return Err(invalid("checkpoint key-group assignment mismatch"));
        }
        if max_rows == 0 || max_bytes == 0 {
            return Err(invalid("checkpoint page limits must be positive"));
        }
        let mut memory = self.memory.sibling("canonical checkpoint range page");
        let rows = max_rows.min(1024);
        memory.resize(CHUNK + rows * 128)?;
        let mut scratch = vec![0; CHUNK];
        let mut offset = self.lower_bound(start, &mut scratch)?;
        let limit = match end {
            Some(end) => self.lower_bound(end, &mut scratch)?,
            None => self.count,
        };
        while offset < limit {
            let mut entries = Vec::with_capacity(rows);
            let mut bytes = 0usize;
            while offset + (entries.len() as u64) < limit && entries.len() < rows {
                let entry = self.entry(offset + entries.len() as u64)?;
                let size = usize::try_from(entry.key_len + entry.value_len)
                    .map_err(|_| invalid("checkpoint entry exceeds host range"))?;
                if !entries.is_empty() && size > max_bytes.saturating_sub(bytes) {
                    break;
                }
                bytes = bytes
                    .checked_add(size)
                    .ok_or_else(|| invalid("checkpoint page size overflow"))?;
                entries.push(entry);
            }
            memory.resize(CHUNK.saturating_add(rows * 128).saturating_add(bytes))?;
            let mut values = Vec::with_capacity(entries.len());
            for entry in &entries {
                let mut key = vec![0; entry.key_len as usize];
                let mut value = vec![0; entry.value_len as usize];
                read_at(&self.data, entry.key, &mut key)?;
                read_at(&self.data, entry.value, &mut value)?;
                values.push((key, value));
            }
            let refs = values
                .iter()
                .map(|(key, value)| (key.as_slice(), value.as_slice()))
                .collect::<Vec<_>>();
            if !visitor(&refs)? {
                return Ok(());
            }
            offset += entries.len() as u64;
            drop(refs);
            drop((values, entries));
            memory.resize(CHUNK + rows * 128)?;
        }
        Ok(())
    }
}

impl KeyedState for CanonicalFile {
    fn get_batch<'a>(
        &'a self,
        keys: &[StateKeyRef<'_>],
        owner: &HostMemoryReservation,
    ) -> Result<StateReadBatch<'a>> {
        let slots = StateReadBatch::admit(keys.len(), owner)?;
        let mut search = owner.sibling("canonical checkpoint batch lookup");
        search.resize(CHUNK.saturating_add(keys.len().saturating_mul(64)))?;
        let mut scratch = vec![0; CHUNK];
        let mut entries = Vec::with_capacity(keys.len());
        let mut bytes = 0usize;
        for key in keys {
            let found = if key.key_group != self.group {
                None
            } else {
                let index = self.lower_bound(key.key, &mut scratch)?;
                if index == self.count {
                    None
                } else {
                    let entry = self.entry(index)?;
                    (self.compare(entry, key.key, &mut scratch)? == Ordering::Equal)
                        .then_some(entry)
                }
            };
            bytes = bytes
                .checked_add(found.map_or(0, |entry| entry.value_len as usize))
                .ok_or_else(|| invalid("checkpoint read size overflow"))?;
            entries.push(found);
        }
        let mut memory = owner.sibling("canonical checkpoint Arrow state values");
        memory.resize(
            bytes
                .saturating_mul(2)
                .saturating_add(CHUNK)
                .saturating_add(keys.len().saturating_mul(128)),
        )?;
        let mut builder = BinaryViewBuilder::with_capacity(keys.len());
        for entry in entries {
            if let Some(entry) = entry {
                let mut value = vec![0; entry.value_len as usize];
                read_at(&self.data, entry.value, &mut value)?;
                builder.append_value(&value);
            } else {
                builder.append_null();
            }
        }
        let array = builder.finish();
        memory.resize(array.get_array_memory_size())?;
        let present = (0..array.len())
            .map(|index| !array.is_null(index))
            .collect::<Vec<_>>();
        let values = Arc::new(super::super::value::AccountedStateValues::new(
            array, memory, None,
        ));
        Ok(StateReadBatch::new(
            present
                .into_iter()
                .enumerate()
                .map(|(index, present)| {
                    present.then(|| StateValue::ArrowView {
                        values: values.clone(),
                        index,
                    })
                })
                .collect(),
            slots,
        ))
    }

    fn visit_key_group(
        &self,
        group: u32,
        rows: usize,
        bytes: usize,
        visitor: &mut dyn FnMut(&[(&[u8], &[u8])]) -> Result<()>,
    ) -> Result<()> {
        self.range(group, &[], None, rows, bytes, &mut |page| {
            visitor(page)?;
            Ok(true)
        })
    }
    fn visit_prefix(
        &self,
        group: u32,
        prefix: &[u8],
        rows: usize,
        bytes: usize,
        visitor: &mut dyn FnMut(&[(&[u8], &[u8])]) -> Result<()>,
    ) -> Result<()> {
        self.range(
            group,
            prefix,
            super::super::prefix_end(prefix).as_deref(),
            rows,
            bytes,
            &mut |page| {
                visitor(page)?;
                Ok(true)
            },
        )
    }
    fn visit_prefix_admitted(
        &self,
        group: u32,
        prefix: &[u8],
        rows: usize,
        bytes: usize,
        _owner: &HostMemoryReservation,
        visitor: &mut dyn FnMut(&[(&[u8], &[u8])]) -> Result<()>,
    ) -> Result<()> {
        self.visit_prefix(group, prefix, rows, bytes, visitor)
    }
    fn visit_range(
        &self,
        group: u32,
        start: &[u8],
        end: Option<&[u8]>,
        rows: usize,
        bytes: usize,
        visitor: &mut dyn FnMut(&[(&[u8], &[u8])]) -> Result<bool>,
    ) -> Result<()> {
        self.range(group, start, end, rows, bytes, visitor)
    }
    fn write_batch(&mut self, _: Vec<StateMutation>) -> Result<()> {
        Err(invalid("checkpoint source is read-only"))
    }
    fn snapshot_key_group(&self, _: u32, _: &HostMemoryReservation) -> Result<SnapshotBytes> {
        Err(invalid("checkpoint source requires bounded visitation"))
    }
    fn restore_key_group(&mut self, _: u32, _: &[u8], _: &HostMemoryReservation) -> Result<()> {
        Err(invalid("checkpoint source is read-only"))
    }
}
