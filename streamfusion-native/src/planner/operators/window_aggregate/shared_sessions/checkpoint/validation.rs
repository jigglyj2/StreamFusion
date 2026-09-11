// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

/// Ordered session intervals are disjoint within each framed partition. Validation retains only
/// the preceding interval, rather than a second in-memory index of the complete RocksDB group.
pub(super) struct CurrentState<'a> {
    calls: &'a [Call],
    end_codec: &'a mut RowConverter,
    workspace: HostMemoryReservation,
    marker: &'a [u8],
    watermark: i64,
    partition: Vec<u8>,
    previous_end: Option<i64>,
}

impl<'a> CurrentState<'a> {
    pub(super) fn new(
        calls: &'a [Call],
        end_codec: &'a mut RowConverter,
        workspace: HostMemoryReservation,
        marker: &'a [u8],
        watermark: i64,
    ) -> Self {
        Self {
            calls,
            end_codec,
            workspace,
            marker,
            watermark,
            partition: Vec::new(),
            previous_end: None,
        }
    }

    pub(super) fn entry(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if key == MARKER_KEY {
            if value != self.marker {
                return Err(DataFusionError::Execution(
                    "shared session state plan/version marker differs".into(),
                ));
            }
            return Ok(());
        }
        if key == TIMER_STATE_KEY {
            return Ok(());
        }
        if key.len() < 20 || key[key.len() - 9] != 1 {
            return Err(DataFusionError::Execution(
                "invalid ordered session end key".into(),
            ));
        }
        let prefix = &key[..key.len() - 9];
        codec::grouping(prefix)?;
        let bound = key
            .len()
            .saturating_mul(4)
            .saturating_add(value.len().saturating_mul(8))
            .saturating_add(65536);
        if bound > self.workspace.size() {
            self.workspace.resize(bound)?;
        }
        let (start, end, _) = codec::decode(value, self.calls)?;
        let row = self
            .end_codec
            .convert_columns(&[Arc::new(Int64Array::from(vec![end])) as ArrayRef])?;
        if &key[key.len() - 9..] != row.row(0).as_ref() || end - 1 <= self.watermark {
            return Err(DataFusionError::Execution(
                "session key or restored watermark differs from live interval".into(),
            ));
        }
        if prefix == self.partition {
            if self.previous_end.is_some_and(|previous| start <= previous) {
                return Err(DataFusionError::Execution(
                    "overlapping or invalid persisted sessions".into(),
                ));
            }
        } else {
            if prefix < self.partition.as_slice() {
                return Err(DataFusionError::Execution(
                    "session checkpoint entries are not ordered".into(),
                ));
            }
            self.partition.clear();
            self.partition.extend_from_slice(prefix);
        }
        self.previous_end = Some(end);
        Ok(())
    }
}
