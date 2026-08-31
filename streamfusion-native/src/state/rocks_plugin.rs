// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::borrow::Cow;
use std::ffi::{c_void, CStr};
use std::mem::ManuallyDrop;
use std::path::Path;
use std::ptr;
use std::sync::Arc;

use arrow::array::{Array, BinaryArray, BinaryBuilder, RecordBatch, StructArray, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use datafusion::error::{DataFusionError, Result};
use libloading::Library;
use streamfusion_state_abi::{
    ArrowOperation, InitializeStateBackend, StateBackendApiV1, STATE_BACKEND_ABI_VERSION,
    STATE_BACKEND_OK,
};

use super::{KeyedState, StateKeyRef, StateMutation};

/// Dynamically loaded RocksDB component accessed exclusively through the versioned C/Arrow ABI.
pub(crate) struct RocksPluginKeyedState {
    // Kept alive until after `handle` is closed and every API pointer is dead.
    library: Library,
    api: &'static StateBackendApiV1,
    handle: *mut c_void,
}

unsafe impl Send for RocksPluginKeyedState {}

impl RocksPluginKeyedState {
    pub(crate) fn open(
        library_path: &Path,
        database_path: &Path,
        first_key_group: u32,
        last_key_group: u32,
        memory_limit: usize,
    ) -> Result<Self> {
        let library = unsafe { Library::new(library_path) }
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let initialize = unsafe {
            library
                .get::<InitializeStateBackend>(b"streamfusion_state_backend_init\0")
                .map_err(|error| DataFusionError::External(Box::new(error)))?
        };
        let mut api = ptr::null();
        let status = unsafe { initialize(STATE_BACKEND_ABI_VERSION, &mut api) };
        if status != STATE_BACKEND_OK || api.is_null() {
            return Err(DataFusionError::Execution(format!(
                "RocksDB state plugin rejected ABI version {STATE_BACKEND_ABI_VERSION}"
            )));
        }
        let api = unsafe { &*api };
        if api.abi_version != STATE_BACKEND_ABI_VERSION {
            return Err(DataFusionError::Execution(format!(
                "RocksDB state plugin returned ABI version {}",
                api.abi_version
            )));
        }
        let database_path = database_path.to_str().ok_or_else(|| {
            DataFusionError::Execution("RocksDB state path is not UTF-8".to_string())
        })?;
        let mut handle = ptr::null_mut();
        let status = unsafe {
            (api.open)(
                database_path.as_ptr(),
                database_path.len(),
                first_key_group,
                last_key_group,
                memory_limit,
                &mut handle,
            )
        };
        check(api, status)?;
        if handle.is_null() {
            return Err(DataFusionError::Execution(
                "RocksDB state plugin returned a null handle".to_string(),
            ));
        }
        Ok(Self {
            library,
            api,
            handle,
        })
    }

    fn invoke(&self, operation: ArrowOperation, input: RecordBatch) -> Result<RecordBatch> {
        let input_data = StructArray::from(input).to_data();
        let input_array = ManuallyDrop::new(FFI_ArrowArray::new(&input_data));
        let input_schema = ManuallyDrop::new(FFI_ArrowSchema::try_from(input_data.data_type())?);
        let mut output_array = FFI_ArrowArray::empty();
        let mut output_schema = FFI_ArrowSchema::empty();
        let status = unsafe {
            operation(
                self.handle,
                (&*input_array as *const FFI_ArrowArray).cast_mut(),
                (&*input_schema as *const FFI_ArrowSchema).cast_mut(),
                &mut output_array,
                &mut output_schema,
            )
        };
        check(self.api, status)?;
        let data = unsafe { from_ffi(output_array, &output_schema) }?;
        Ok(RecordBatch::from(StructArray::from(data)))
    }
}

impl KeyedState for RocksPluginKeyedState {
    fn get_batch<'a>(&'a self, keys: &[StateKeyRef<'_>]) -> Result<Vec<Option<Cow<'a, [u8]>>>> {
        let input = keys_batch(keys, None)?;
        let output = self.invoke(self.api.get_batch, input)?;
        let values = column::<BinaryArray>(&output, 0, "value")?;
        Ok((0..values.len())
            .map(|row| (!values.is_null(row)).then(|| Cow::Owned(values.value(row).to_vec())))
            .collect())
    }

    fn write_batch(&mut self, mutations: Vec<StateMutation>) -> Result<()> {
        let key_refs = mutations
            .iter()
            .map(|mutation| StateKeyRef {
                key_group: mutation.key.key_group,
                key: &mutation.key.key,
            })
            .collect::<Vec<_>>();
        let input = keys_batch(
            &key_refs,
            Some(
                mutations
                    .iter()
                    .map(|mutation| mutation.value.as_deref())
                    .collect(),
            ),
        )?;
        self.invoke(self.api.write_batch, input)?;
        Ok(())
    }

    fn snapshot_key_group(&self, key_group: u32) -> Result<Vec<u8>> {
        let input = key_group_batch(key_group, None)?;
        let output = self.invoke(self.api.snapshot_key_group, input)?;
        let state = column::<BinaryArray>(&output, 0, "state")?;
        if state.len() != 1 || state.is_null(0) {
            return Err(DataFusionError::Execution(
                "RocksDB snapshot returned invalid Arrow state".to_string(),
            ));
        }
        Ok(state.value(0).to_vec())
    }

    fn restore_key_group(&mut self, key_group: u32, bytes: &[u8]) -> Result<()> {
        let input = key_group_batch(key_group, Some(bytes))?;
        self.invoke(self.api.restore_key_group, input)?;
        Ok(())
    }

    fn checkpoint(&self, directory: &Path) -> Result<()> {
        let path = directory.to_str().ok_or_else(|| {
            DataFusionError::Execution("RocksDB checkpoint path is not UTF-8".to_string())
        })?;
        let input = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "path",
                DataType::Binary,
                false,
            )])),
            vec![Arc::new(BinaryArray::from_vec(vec![path.as_bytes()]))],
        )?;
        self.invoke(self.api.checkpoint, input)?;
        Ok(())
    }
}

impl Drop for RocksPluginKeyedState {
    fn drop(&mut self) {
        unsafe { (self.api.close)(self.handle) };
        self.handle = ptr::null_mut();
        let _keep_library_alive = &self.library;
    }
}

fn keys_batch(keys: &[StateKeyRef<'_>], values: Option<Vec<Option<&[u8]>>>) -> Result<RecordBatch> {
    let key_groups = UInt32Array::from_iter_values(keys.iter().map(|key| key.key_group));
    let mut key_builder = BinaryBuilder::new();
    for key in keys {
        key_builder.append_value(key.key);
    }
    let mut fields = vec![
        Field::new("key_group", DataType::UInt32, false),
        Field::new("key", DataType::Binary, false),
    ];
    let mut columns: Vec<Arc<dyn Array>> =
        vec![Arc::new(key_groups), Arc::new(key_builder.finish())];
    if let Some(values) = values {
        let mut builder = BinaryBuilder::new();
        for value in values {
            match value {
                Some(value) => builder.append_value(value),
                None => builder.append_null(),
            }
        }
        fields.push(Field::new("value", DataType::Binary, true));
        columns.push(Arc::new(builder.finish()));
    }
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}

fn key_group_batch(key_group: u32, state: Option<&[u8]>) -> Result<RecordBatch> {
    let mut fields = vec![Field::new("key_group", DataType::UInt32, false)];
    let mut columns: Vec<Arc<dyn Array>> = vec![Arc::new(UInt32Array::from(vec![key_group]))];
    if let Some(state) = state {
        fields.push(Field::new("state", DataType::Binary, false));
        columns.push(Arc::new(BinaryArray::from_vec(vec![state])));
    }
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
    )?)
}

fn column<'a, T: Array + 'static>(
    batch: &'a RecordBatch,
    index: usize,
    description: &str,
) -> Result<&'a T> {
    batch
        .columns()
        .get(index)
        .and_then(|array| array.as_any().downcast_ref())
        .ok_or_else(|| {
            DataFusionError::Execution(format!(
                "RocksDB state {description} column has the wrong Arrow type"
            ))
        })
}

fn check(api: &StateBackendApiV1, status: i32) -> Result<()> {
    if status == STATE_BACKEND_OK {
        return Ok(());
    }
    let error = unsafe { (api.last_error)() };
    let message = if error.is_null() {
        "RocksDB state plugin failed without an error".to_string()
    } else {
        unsafe { CStr::from_ptr(error) }
            .to_string_lossy()
            .into_owned()
    };
    Err(DataFusionError::Execution(message))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory_pool::{tests_support::TestBroker, HostMemoryReservation};
    use crate::state::{MemoryKeyedState, StateKey};

    #[test]
    fn exchanges_canonical_snapshots_with_memory_through_the_dynamic_arrow_abi() {
        let Ok(plugin_path) = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN") else {
            // The Maven reactor sets this after building the optional component. Plain core-only
            // Cargo tests remain independent of RocksDB and its C++ toolchain.
            return;
        };
        let source_directory = tempfile::tempdir().unwrap();
        let mut rocks = RocksPluginKeyedState::open(
            Path::new(&plugin_path),
            source_directory.path(),
            2,
            2,
            16 << 20,
        )
        .unwrap();
        rocks
            .write_batch(vec![mutation(2, b"b", b"two"), mutation(2, b"a", b"one")])
            .unwrap();
        let rocks_snapshot = rocks.snapshot_key_group(2).unwrap();

        let broker = Arc::new(TestBroker::new(1 << 20));
        let mut memory = MemoryKeyedState::new(
            2,
            2,
            HostMemoryReservation::new(broker, "canonical snapshot test"),
        )
        .unwrap();
        memory.restore_key_group(2, &rocks_snapshot).unwrap();
        assert_eq!(
            memory.snapshot_key_group(2).unwrap(),
            rocks_snapshot,
            "canonical bytes must not depend on the producing backend"
        );

        let target_directory = tempfile::tempdir().unwrap();
        let mut restored_rocks = RocksPluginKeyedState::open(
            Path::new(&plugin_path),
            target_directory.path(),
            2,
            2,
            16 << 20,
        )
        .unwrap();
        restored_rocks
            .restore_key_group(2, &memory.snapshot_key_group(2).unwrap())
            .unwrap();
        let values = restored_rocks
            .get_batch(&[
                StateKeyRef {
                    key_group: 2,
                    key: b"a",
                },
                StateKeyRef {
                    key_group: 2,
                    key: b"b",
                },
            ])
            .unwrap();
        assert_eq!(values[0].as_deref(), Some(b"one".as_slice()));
        assert_eq!(values[1].as_deref(), Some(b"two".as_slice()));
    }

    fn mutation(key_group: u32, key: &[u8], value: &[u8]) -> StateMutation {
        StateMutation {
            key: StateKey {
                key_group,
                key: key.to_vec(),
            },
            value: Some(value.to_vec()),
        }
    }
}
