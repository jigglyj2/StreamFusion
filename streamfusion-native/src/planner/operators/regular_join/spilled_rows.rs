// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Prepared historical state, independent of the state backend. A batch reads RocksDB before
//! computation and can then revisit each key's Arrow IPC stream without another state lookup.
//! Flink owns the assigned directories; DataFusion owns the temporary file and disk accounting.

use super::StoredRow;
use crate::memory_pool::HostMemoryReservation;
use arrow::array::{Array, BinaryArray, BinaryBuilder, Int32Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::ipc::{reader::StreamReader, writer::StreamWriter};
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::disk_manager::DiskManager;
use datafusion::execution::spill_file::{SpillFile, SpillWriter};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Take};
use std::sync::Arc;

/// Only group descriptors remain resident; their size follows distinct keys in the input batch.
#[derive(Clone, Copy)]
pub(super) struct Range {
    start: u64,
    end: u64,
    rows: usize,
    workspace: usize,
}

pub(super) struct Writer {
    file: Arc<dyn SpillFile>,
    schema: SchemaRef,
    active: Option<StreamWriter<Box<dyn SpillWriter>>>,
    range: Range,
    previous_id: Option<u64>,
    failed: bool,
    memory: HostMemoryReservation,
}

impl Writer {
    pub(super) fn new(manager: &Arc<DiskManager>, owner: &HostMemoryReservation) -> Result<Self> {
        let mut memory = owner.sibling("regular join prepared spill encoding");
        memory.resize(64 << 10)?;
        Ok(Self {
            file: manager.create_tmp_file("regular join prepared history")?,
            schema: schema(),
            active: None,
            range: Range {
                start: 0,
                end: 0,
                rows: 0,
                workspace: 64 << 10,
            },
            previous_id: None,
            failed: false,
            memory,
        })
    }

    /// Preserve workspace for encoding the next decoded backend page. The loader can halve its
    /// read chunk while this credit is held, then writes reuse the credit without another pool.
    pub(super) fn prepare_read(&mut self) -> Result<()> {
        let available = self.memory.available_capacity()?.unwrap_or(2 << 20);
        self.memory
            .resize((available / 2).min(1 << 20).max(64 << 10))
    }

    pub(super) fn start_group(&mut self) -> Result<()> {
        if self.failed {
            return Err(invalid("spill writer failed; discard prepared history"));
        }
        if self.active.is_some() {
            return Err(invalid("previous history group has not finished"));
        }
        // Preserve the loader's encoding headroom across group changes.
        let start = self
            .file
            .size()
            .ok_or_else(|| invalid("local spill has no size"))?;
        self.active =
            Some(
                match self.file.open_writer().and_then(|writer| {
                    StreamWriter::try_new(writer, &self.schema).map_err(Into::into)
                }) {
                    Ok(writer) => writer,
                    Err(error) => {
                        self.failed = true;
                        return Err(error);
                    }
                },
            );
        self.range = Range {
            start,
            end: start,
            rows: 0,
            workspace: 64 << 10,
        };
        self.previous_id = None;
        Ok(())
    }

    /// The caller owns/admitted the input rows. Admit conversion and IPC workspace before building
    /// any arrays. Large pages can be retried at a smaller boundary before any bytes are written.
    pub(super) fn write(&mut self, rows: &[StoredRow]) -> Result<()> {
        if self.failed {
            return Err(invalid("spill writer failed; discard prepared history"));
        }
        if self.active.is_none() {
            return Err(invalid("history group has not started"));
        }
        if rows.is_empty() {
            return Ok(());
        }
        let mut previous = self.previous_id;
        let mut payload_bytes = 0usize;
        for row in rows {
            if previous.is_some_and(|id| id >= row.id) {
                return Err(invalid("history row identities are not increasing"));
            }
            previous = Some(row.id);
            payload_bytes = payload_bytes
                .checked_add(row.row.len())
                .ok_or_else(|| invalid("history payload size overflow"))?;
        }
        if payload_bytes > i32::MAX as usize {
            return Err(DataFusionError::ResourcesExhausted(
                "join spill page exceeds Arrow Binary offsets".into(),
            ));
        }
        let workspace = payload_bytes
            .checked_add(
                rows.len()
                    .checked_mul(128)
                    .ok_or_else(|| invalid("history row count overflow"))?,
            )
            .and_then(|bytes| bytes.checked_mul(8))
            .and_then(|bytes| bytes.checked_add(64 << 10))
            .ok_or_else(|| invalid("history workspace overflow"))?;
        self.memory.resize(workspace)?;
        let total_rows = self
            .range
            .rows
            .checked_add(rows.len())
            .ok_or_else(|| invalid("history row count overflow"))?;
        let mut payload = BinaryBuilder::with_capacity(rows.len(), payload_bytes);
        for row in rows {
            payload.append_value(&row.row);
        }
        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(UInt64Array::from_iter_values(rows.iter().map(|row| row.id))),
                Arc::new(Int32Array::from_iter_values(
                    rows.iter().map(|row| row.associations),
                )),
                Arc::new(payload.finish()),
            ],
        )?;
        if let Err(error) = self.active.as_mut().unwrap().write(&batch) {
            self.failed = true;
            return Err(error.into());
        }
        self.range.rows = total_rows;
        self.range.workspace = self.range.workspace.max(workspace);
        self.previous_id = previous;
        drop(batch);
        // Only the stream descriptor survives a write. Do not strand conversion credit while
        // the next backend page is being admitted.
        self.memory.resize(64 << 10)?;
        Ok(())
    }

    pub(super) fn finish_group(&mut self) -> Result<Range> {
        if self.failed {
            return Err(invalid("spill writer failed; discard prepared history"));
        }
        let mut writer = self
            .active
            .take()
            .ok_or_else(|| invalid("history group has not started"))?;
        let result = (|| -> Result<()> {
            writer.finish()?;
            writer.get_mut().finish()
        })();
        if let Err(error) = result {
            self.failed = true;
            return Err(error);
        }
        drop(writer);
        self.range.end = self
            .file
            .size()
            .ok_or_else(|| invalid("local spill has no size"))?;
        // Keep encoding headroom until the next read/write boundary.
        Ok(self.range)
    }

    pub(super) fn finish(self) -> Result<History> {
        if self.failed {
            return Err(invalid("spill writer failed; discard prepared history"));
        }
        if self.active.is_some() {
            return Err(invalid("last history group has not finished"));
        }
        Ok(History { file: self.file })
    }
}

pub(super) struct History {
    file: Arc<dyn SpillFile>,
}

impl History {
    pub(super) fn reader(&self, range: Range, owner: &HostMemoryReservation) -> Result<Reader> {
        let mut memory = owner.sibling("regular join prepared spill read page");
        memory.resize(64 << 10)?;
        let path = self
            .file
            .path()
            .ok_or_else(|| invalid("Flink spill file is not local"))?;
        let mut file = File::open(path)?;
        if range.start >= range.end || range.end > file.metadata()?.len() {
            return Err(invalid("truncated history spill range"));
        }
        file.seek(SeekFrom::Start(range.start))?;
        let reader = StreamReader::try_new(file.take(range.end - range.start), None)?;
        if reader.schema() != schema() {
            return Err(invalid("unexpected history spill schema"));
        }
        Ok(Reader {
            reader,
            remaining: range.rows,
            workspace: range.workspace,
            previous_id: None,
            _file: self.file.clone(),
            _memory: memory,
        })
    }
}

pub(super) struct Reader {
    reader: StreamReader<Take<File>>,
    remaining: usize,
    workspace: usize,
    previous_id: Option<u64>,
    _file: Arc<dyn SpillFile>,
    _memory: HostMemoryReservation,
}

impl Reader {
    pub(super) fn has_more(&self) -> bool {
        self.remaining != 0
    }

    /// These opaque Arrow-row state bytes are decoded to query columns by the existing join
    /// adapters. Keep this bounded page alive until its candidate transition has completed.
    pub(super) fn next(&mut self) -> Result<Option<Page>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        let mut memory = self._memory.sibling("regular join decoded history page");
        memory.resize(self.workspace)?;
        let Some(batch) = self.reader.next().transpose()? else {
            if self.remaining != 0 {
                return Err(invalid("history spill ended before all rows were read"));
            }
            return Ok(None);
        };
        if batch.num_rows() > self.remaining
            || batch.columns().iter().any(|array| array.null_count() != 0)
        {
            return Err(invalid("invalid history spill row count or null values"));
        }
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let associations = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let payload = batch
            .column(2)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        let mut rows = Vec::with_capacity(batch.num_rows());
        for index in 0..batch.num_rows() {
            let id = ids.value(index);
            if self.previous_id.is_some_and(|previous| previous >= id) {
                return Err(invalid("history spill row identities are not increasing"));
            }
            self.previous_id = Some(id);
            rows.push(StoredRow {
                id,
                associations: associations.value(index),
                row: Arc::from(payload.value(index)),
            });
        }
        self.remaining -= rows.len();
        Ok(Some(Page {
            rows,
            _memory: memory,
        }))
    }
}

/// Credit follows a decoded page even if its reader, history file, or operator is closed.
pub(super) struct Page {
    pub(super) rows: Vec<StoredRow>,
    pub(super) _memory: HostMemoryReservation,
}

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new("associations", DataType::Int32, false),
        Field::new("payload", DataType::Binary, false),
    ]))
}

fn invalid(message: &str) -> DataFusionError {
    DataFusionError::Execution(format!("regular join prepared history: {message}"))
}

#[cfg(test)]
mod tests;
