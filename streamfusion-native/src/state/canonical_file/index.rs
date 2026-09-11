// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use arrow::array::{Array, LargeBinaryArray, LargeBinaryBuilder, UInt64Array};
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::execution::disk_manager::DiskManager;
use datafusion::execution::{runtime_env::RuntimeEnvBuilder, TaskContext};
use datafusion::physical_expr::{expressions::Column, LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::{PartitionStream, StreamingTableExec};
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::StreamExt;

const ROWS: usize = 1024;

pub(super) fn build(
    group: u32,
    file: &Arc<dyn SpillFile>,
    length: u64,
    manager: &Arc<DiskManager>,
    owner: &HostMemoryReservation,
) -> Result<(Arc<dyn SpillFile>, u64, usize)> {
    let mut reader = File::open(path(file)?)?;
    let mut header = [0; 16];
    reader.read_exact(&mut header)?;
    let count = u32::from_le_bytes(header[12..].try_into().unwrap());
    let expected = streamfusion_state_abi::key_group_snapshot_header(
        group,
        count as usize,
        usize::try_from(length).map_err(|_| invalid("checkpoint exceeds host file range"))?,
    )
    .map_err(|error| invalid(&error.to_string()))?;
    if header != expected {
        return Err(invalid("wrong canonical magic, version or key group"));
    }
    let capacity = owner.available_capacity()?.unwrap_or(64 << 20);
    let page_bytes = (capacity / 128).clamp(1024, CHUNK);
    let mut control = owner.sibling("canonical checkpoint sorting control");
    control.resize(64 << 10)?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::LargeBinary, false),
        Field::new("key_offset", DataType::UInt64, false),
        Field::new("key_length", DataType::UInt64, false),
        Field::new("value_offset", DataType::UInt64, false),
        Field::new("value_length", DataType::UInt64, false),
    ]));
    let mut memory = owner.sibling("canonical checkpoint index input");
    memory.resize(CHUNK)?;
    let mut source = Source {
        file: BufReader::with_capacity(CHUNK, reader),
        length,
        offset: 16,
        remaining: count,
        page_bytes,
        rows: ROWS,
        schema: schema.clone(),
        memory,
        finished: false,
    };
    // DataFusion merge output uses the configured row count. A fixed 1024-row batch can
    // exceed the whole free budget for wide keys even when every input page was bounded.
    // Inspect only length framing first; never load values or retain a key directory here.
    let width = source.validate_framing()?.saturating_add(64);
    let rows = (page_bytes / width).clamp(1, ROWS);
    source.rows = rows;
    let input = Arc::new(Input {
        schema: schema.clone(),
        source: Mutex::new(Some(source)),
    });
    let input = Arc::new(StreamingTableExec::try_new(
        schema,
        vec![input],
        None,
        [],
        false,
        None,
    )?);
    let ordering = LexOrdering::new([PhysicalSortExpr::new(
        Arc::new(Column::new("key", 0)),
        SortOptions::default(),
    )])
    .unwrap();
    let sort = SortExec::new(ordering, input);
    let mut config = SessionConfig::new().with_batch_size(rows);
    config.options_mut().execution.sort_spill_reservation_bytes = (capacity / 8).min(1 << 20);
    config.options_mut().execution.sort_in_place_threshold_bytes = page_bytes;
    let mut environment = RuntimeEnvBuilder::new().with_memory_pool(owner.datafusion_pool()?);
    environment.disk_manager = Some(manager.clone());
    let context = SessionContext::new_with_config_rt(config, environment.build_arc()?).task_ctx();
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let mut stream = {
        let _entered = runtime.enter();
        sort.execute(0, context)?
    };
    let index = manager.create_tmp_file("canonical checkpoint ordered offsets")?;
    let mut writer = std::io::BufWriter::with_capacity(CHUNK, index.open_writer()?);
    let mut output_memory = owner.sibling("canonical checkpoint sorted output page");
    let mut previous_memory = owner.sibling("canonical checkpoint duplicate key check");
    let mut previous: Option<Vec<u8>> = None;
    let mut emitted = 0u64;
    while let Some(batch) = runtime.block_on(stream.next()).transpose()? {
        output_memory.resize(batch.get_array_memory_size())?;
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();
        let columns = [1, 2, 3, 4].map(|index| {
            batch
                .column(index)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
        });
        for row in 0..batch.num_rows() {
            let key = keys.value(row);
            let before = if row == 0 {
                previous.as_deref()
            } else {
                Some(keys.value(row - 1))
            };
            if before.is_some_and(|before| before >= key) {
                return Err(invalid("duplicate or unordered canonical checkpoint key"));
            }
            writer.write_all(
                &Entry {
                    key: columns[0].value(row),
                    key_len: columns[1].value(row),
                    value: columns[2].value(row),
                    value_len: columns[3].value(row),
                }
                .bytes(),
            )?;
            emitted += 1;
        }
        if !keys.is_empty() {
            drop(previous.take());
            previous_memory.resize(keys.value(keys.len() - 1).len())?;
            previous = Some(keys.value(keys.len() - 1).to_vec());
        }
        drop(batch);
        output_memory.resize(0)?;
    }
    if emitted != u64::from(count) {
        return Err(invalid("canonical checkpoint entry count changed"));
    }
    writer.flush()?;
    writer.get_mut().finish()?;
    drop(writer);
    let spills = sort
        .metrics()
        .and_then(|metrics| metrics.spill_count())
        .unwrap_or(0);
    Ok((index, emitted, spills))
}

struct Input {
    schema: SchemaRef,
    source: Mutex<Option<Source>>,
}
impl std::fmt::Debug for Input {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CanonicalCheckpointKeys")
            .finish_non_exhaustive()
    }
}
impl PartitionStream for Input {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }
    fn execute(&self, _: Arc<TaskContext>) -> SendableRecordBatchStream {
        let source = self.source.lock().unwrap().take();
        let iter: Box<dyn Iterator<Item = Result<RecordBatch>> + Send> = match source {
            Some(source) => Box::new(source),
            None => Box::new(std::iter::once(Err(invalid(
                "checkpoint key source consumed twice",
            )))),
        };
        Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            futures::stream::iter(iter),
        ))
    }
}

struct Source {
    file: BufReader<File>,
    length: u64,
    offset: u64,
    remaining: u32,
    page_bytes: usize,
    rows: usize,
    schema: SchemaRef,
    memory: HostMemoryReservation,
    finished: bool,
}
impl Source {
    fn validate_framing(&mut self) -> Result<usize> {
        let mut width = 0u64;
        for _ in 0..self.remaining {
            let key = self.read_len()?;
            self.ensure(key)?;
            width = width.max(key);
            self.file.seek_relative(key as i64)?;
            self.offset += key;
            let value = self.read_len()?;
            self.ensure(value)?;
            self.file.seek_relative(value as i64)?;
            self.offset += value;
        }
        if self.offset != self.length {
            return Err(invalid("trailing canonical checkpoint bytes"));
        }
        self.file.seek(SeekFrom::Start(16))?;
        self.offset = 16;
        usize::try_from(width).map_err(|_| invalid("canonical key exceeds host range"))
    }
    fn read_len(&mut self) -> Result<u64> {
        self.ensure(4)?;
        let mut bytes = [0; 4];
        self.file.read_exact(&mut bytes)?;
        self.offset += 4;
        Ok(u64::from(u32::from_le_bytes(bytes)))
    }
    fn ensure(&self, bytes: u64) -> Result<()> {
        if bytes > self.length.saturating_sub(self.offset) {
            return Err(invalid("truncated canonical entry"));
        }
        Ok(())
    }
    fn batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.remaining == 0 {
            if self.offset != self.length {
                return Err(invalid("trailing canonical checkpoint bytes"));
            }
            return Ok(None);
        }
        let count = (self.remaining as usize).min(self.rows);
        let mut admitted = CHUNK + count * 128 + self.page_bytes * 4;
        self.memory.resize(admitted)?;
        let mut keys = LargeBinaryBuilder::with_capacity(count, self.page_bytes);
        let mut entries = Vec::with_capacity(count);
        let mut bytes = 0usize;
        while entries.len() < count && (entries.is_empty() || bytes < self.page_bytes) {
            let key_len = self.read_len()?;
            self.ensure(key_len)?;
            let key_offset = self.offset;
            let key_len_usize = usize::try_from(key_len)
                .map_err(|_| invalid("canonical key length exceeds host range"))?;
            admitted = admitted.max(
                (bytes + key_len_usize)
                    .saturating_mul(4)
                    .saturating_add(CHUNK + count * 128),
            );
            self.memory.resize(admitted)?;
            let mut key = vec![0; key_len_usize];
            self.file.read_exact(&mut key)?;
            self.offset += key_len;
            keys.append_value(&key);
            bytes += key.len();
            let value_len = self.read_len()?;
            self.ensure(value_len)?;
            entries.push(Entry {
                key: key_offset,
                key_len,
                value: self.offset,
                value_len,
            });
            self.offset += value_len;
            self.file.seek_relative(value_len as i64)?;
            self.remaining -= 1;
        }
        let mut columns: Vec<Arc<dyn Array>> = vec![Arc::new(keys.finish())];
        for column in 0..4 {
            columns.push(Arc::new(UInt64Array::from_iter_values(entries.iter().map(
                |entry| [entry.key, entry.key_len, entry.value, entry.value_len][column],
            ))));
        }
        Ok(Some(RecordBatch::try_new(self.schema.clone(), columns)?))
    }
}
impl Iterator for Source {
    type Item = Result<RecordBatch>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        let next = self.batch().transpose();
        self.finished = !matches!(next, Some(Ok(_)));
        next
    }
}
