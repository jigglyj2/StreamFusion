// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::paged_codec::*;
use super::spilled_rows::{History, Range, Writer};
use super::*;
use datafusion::execution::disk_manager::DiskManager;
use std::collections::VecDeque;

struct Legacy {
    layout: Layout,
    pages: [EntryIds; 2],
}

#[derive(Default)]
struct Group {
    ranges: [Option<Range>; 2],
    legacy: Option<Legacy>,
}

pub(super) struct Prepared {
    history: History,
    groups: Vec<Group>,
    pub(super) retracted_history: Vec<bool>,
    _memory: HostMemoryReservation,
}

mod loading;
pub(super) use loading::load;

impl Prepared {
    pub(super) fn transfer_directory_memory(
        &mut self,
        owner: &mut HostMemoryReservation,
    ) -> Result<()> {
        let bytes = self._memory.size();
        owner.grow_from(&mut self._memory, bytes)
    }

    pub(super) fn reader(
        &self,
        index: usize,
        side: usize,
        owner: &HostMemoryReservation,
    ) -> Result<Option<super::spilled_rows::Reader>> {
        self.groups[index].ranges[side]
            .map(|range| self.history.reader(range, owner))
            .transpose()
    }

    /// Rewrite legacy state only after the whole batch's changelog has drained. Any partial write
    /// failure is handled by the caller's failed-invocation guard, requiring Flink recovery.
    pub(super) fn migrate(
        &self,
        state: &mut dyn KeyedState,
        staged: &[StagedState],
        owner: &HostMemoryReservation,
        writes: &mut u64,
    ) -> Result<()> {
        if self.groups.iter().all(|group| group.legacy.is_none()) {
            return Ok(());
        }
        let mut memory = owner.sibling("regular join legacy spill migration writes");
        let mut writer = crate::state::StateBatchWriter::new(state, &mut memory, 0, writes)?;
        for (index, group) in self.groups.iter().enumerate() {
            let Some(legacy) = &group.legacy else {
                continue;
            };
            let entry = &staged[index];
            if legacy.layout == Layout::Compact {
                writer.push(entry.key.key.len() + 16, 0, || {
                    Ok(StateMutation {
                        key: manifest_key(&entry.key),
                        value: None,
                    })
                })?;
            } else {
                for input in 0..2 {
                    for id in legacy.pages[input].iter() {
                        writer.push(entry.key.key.len() + 32, 0, || {
                            Ok(StateMutation {
                                key: entry_key(&entry.key, input, id, legacy.layout),
                                value: None,
                            })
                        })?;
                    }
                }
            }
            for input in 0..2 {
                if let Some(mut reader) = self.reader(index, input, owner)? {
                    while let Some(page) = reader.next()? {
                        for row in &page.rows {
                            if !entry.unloaded.as_ref().unwrap().ids[input].contains(row.id) {
                                continue;
                            }
                            writer.push(entry.key.key.len() + row.row.len() + 64, 0, || {
                                Ok(StateMutation {
                                    key: row_key(&entry.key, input, row.id),
                                    value: Some(encode_row(row.id, row.associations, &row.row)?),
                                })
                            })?;
                        }
                    }
                }
            }
        }
        writer.finish()
    }
}
