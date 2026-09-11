// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

impl StreamingCursor {
    #[cfg(test)]
    pub(in super::super) fn uses_spilled_history(&self) -> bool {
        self.prepared.is_some()
    }

    pub(super) fn load_history_page(&mut self) -> Result<()> {
        let index = self.indices[self.row];
        if self.history_row != Some(self.row) {
            self.reader =
                self.prepared
                    .as_ref()
                    .unwrap()
                    .reader(index, 1 - self.side, &self._memory)?;
            self.history_row = Some(self.row);
        }
        if self.page.is_none() {
            if let Some(reader) = &mut self.reader {
                if let Some(mut page) = reader.next()? {
                    let rows = if self.side == 0 {
                        &mut self.staged[index].value.right
                    } else {
                        &mut self.staged[index].value.left
                    };
                    debug_assert!(rows.is_empty());
                    *rows = std::mem::take(&mut page.rows);
                    self.page = Some(page);
                }
            }
        }
        Ok(())
    }

    pub(super) fn clear_history_page(&mut self) {
        if self.page.is_some() {
            let index = self.indices[self.row];
            let rows = if self.side == 0 {
                &mut self.staged[index].value.right
            } else {
                &mut self.staged[index].value.left
            };
            *rows = Vec::new();
            self.page = None;
        }
    }
}
