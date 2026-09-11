// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Bounded, read-only canonical checkpoint staging. DataFusion sorts keys and file offsets;
//! opaque payloads are copied from Flink once and never enter the sort's Arrow batches.

use super::*;
use crate::memory_pool::HostMemoryReservation;
use datafusion::error::DataFusionError;
use datafusion::execution::spill_file::SpillFile;
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::sync::{Arc, Mutex};

mod index;
mod reading;

const CHUNK: usize = 64 << 10;
const INDEX_BYTES: u64 = 32;

pub(crate) struct CanonicalFile {
    group: u32,
    count: u64,
    data: Mutex<Positioned>,
    index: Mutex<Positioned>,
    _data_owner: Arc<dyn SpillFile>,
    _index_owner: Arc<dyn SpillFile>,
    memory: HostMemoryReservation,
    #[cfg(test)]
    sort_spills: usize,
}

#[derive(Clone, Copy, Debug)]
struct Entry {
    key: u64,
    key_len: u64,
    value: u64,
    value_len: u64,
}

impl Entry {
    fn bytes(self) -> [u8; INDEX_BYTES as usize] {
        let mut bytes = [0; INDEX_BYTES as usize];
        for (out, value) in
            bytes
                .chunks_exact_mut(8)
                .zip([self.key, self.key_len, self.value, self.value_len])
        {
            out.copy_from_slice(&value.to_le_bytes());
        }
        bytes
    }
    fn decode(bytes: &[u8; INDEX_BYTES as usize]) -> Self {
        let mut fields = bytes
            .chunks_exact(8)
            .map(|part| u64::from_le_bytes(part.try_into().unwrap()));
        Self {
            key: fields.next().unwrap(),
            key_len: fields.next().unwrap(),
            value: fields.next().unwrap(),
            value_len: fields.next().unwrap(),
        }
    }
}

impl CanonicalFile {
    /// Complete framing and uniqueness validation before publishing a source to any state owner.
    /// EOF, duplicate keys, sort admission or I/O failure releases all temporary files.
    pub(crate) fn read(
        group: u32,
        length: u64,
        input: &mut dyn Read,
        resources: &crate::spill::Resources,
        owner: &HostMemoryReservation,
    ) -> Result<Self> {
        if length < 16 {
            return Err(invalid("truncated canonical header"));
        }
        let manager = resources.manager()?;
        let mut transport = owner.sibling("canonical checkpoint file transport");
        transport.resize(CHUNK)?;
        let file = manager.create_tmp_file("canonical checkpoint input")?;
        let mut writer = file.open_writer()?;
        let mut buffer = vec![0; CHUNK];
        let mut remaining = length;
        while remaining != 0 {
            let count = remaining.min(CHUNK as u64) as usize;
            input.read_exact(&mut buffer[..count])?;
            writer.write_all(&buffer[..count])?;
            remaining -= count as u64;
        }
        writer.finish()?;
        drop((writer, buffer, transport));
        let (index, count, _spills) = index::build(group, &file, length, &manager, owner)?;
        let data = File::open(path(&file)?)?;
        let index_file = File::open(path(&index)?)?;
        let mut memory = owner.sibling("canonical checkpoint file buffers");
        memory.resize(CHUNK * 2)?;
        Ok(Self {
            group,
            count,
            data: Mutex::new(Positioned::new(data)),
            index: Mutex::new(Positioned::new(index_file)),
            _data_owner: file,
            _index_owner: index,
            memory,
            #[cfg(test)]
            sort_spills: _spills,
        })
    }

    fn entry(&self, index: u64) -> Result<Entry> {
        if index >= self.count {
            return Err(invalid("checkpoint index outside its directory"));
        }
        let mut bytes = [0; INDEX_BYTES as usize];
        read_at(
            &self.index,
            index
                .checked_mul(INDEX_BYTES)
                .ok_or_else(|| invalid("checkpoint index overflow"))?,
            &mut bytes,
        )?;
        Ok(Entry::decode(&bytes))
    }
}

fn path(file: &Arc<dyn SpillFile>) -> Result<&Path> {
    file.path()
        .ok_or_else(|| invalid("Flink checkpoint spill file is not local"))
}

struct Positioned {
    reader: BufReader<File>,
    position: u64,
}
impl Positioned {
    fn new(file: File) -> Self {
        Self {
            reader: BufReader::with_capacity(CHUNK, file),
            position: 0,
        }
    }
}
fn read_at(file: &Mutex<Positioned>, offset: u64, bytes: &mut [u8]) -> Result<()> {
    let mut file = file
        .lock()
        .map_err(|_| invalid("checkpoint file lock poisoned"))?;
    let relative = i128::from(offset) - i128::from(file.position);
    if let Ok(relative) = i64::try_from(relative) {
        file.reader.seek_relative(relative)?;
    } else {
        file.reader.seek(SeekFrom::Start(offset))?;
    }
    file.position = offset;
    // A failed read can advance its cursor; force an absolute resynchronization before reuse.
    if let Err(error) = file.reader.read_exact(bytes) {
        file.position = file.reader.stream_position()?;
        return Err(error.into());
    }
    file.position += bytes.len() as u64;
    Ok(())
}

fn invalid(message: &str) -> DataFusionError {
    DataFusionError::Execution(format!("canonical checkpoint file: {message}"))
}

#[cfg(test)]
mod tests;
