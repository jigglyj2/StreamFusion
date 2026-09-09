// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use crate::state::StateValue;
use std::ffi::{c_void, CStr};
use std::mem::ManuallyDrop;
use std::path::Path;
use std::ptr;
use std::sync::Arc;

use arrow::array::{
    Array, BinaryArray, BinaryBuilder, BinaryViewArray, RecordBatch, StructArray, UInt32Array,
    UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use datafusion::error::{DataFusionError, Result};
use libloading::Library;
use streamfusion_state_abi::{
    ArrowOperation, InitializeStateBackend, StateBackendApiV1, StateBackendOpenOptions,
    StateMemoryAdmission, STATE_BACKEND_ABI_VERSION, STATE_BACKEND_OK,
};

use crate::memory_pool::HostMemoryReservation;

use super::{KeyedState, StateKeyRef, StateMutation};

mod scan;

/// Dynamically loaded RocksDB component accessed exclusively through the versioned C/Arrow ABI.
pub(crate) struct RocksPluginKeyedState {
    // Kept alive until after `handle` is closed and every API pointer is dead.
    library: Arc<Library>,
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
        Self::open_scoped(
            library_path,
            database_path,
            first_key_group,
            last_key_group,
            memory_limit,
            [0, 0],
            None,
        )
    }

    pub(crate) fn open_for_owner(
        library_path: &Path,
        database_path: &Path,
        first_key_group: u32,
        last_key_group: u32,
        memory_limit: usize,
        owner: &HostMemoryReservation,
    ) -> Result<Self> {
        Self::open_scoped(
            library_path,
            database_path,
            first_key_group,
            last_key_group,
            memory_limit,
            owner.rocks_scope()?,
            None,
        )
    }

    pub(crate) fn open_configured(
        resources: &crate::proto::NativeRocksDbState,
        first_key_group: u32,
        last_key_group: u32,
        owner: Option<&HostMemoryReservation>,
    ) -> Result<Self> {
        Self::open_scoped(
            Path::new(&resources.plugin_path),
            Path::new(&resources.database_path),
            first_key_group,
            last_key_group,
            resources.memory_limit as usize,
            owner
                .map(HostMemoryReservation::rocks_scope)
                .transpose()?
                .unwrap_or([0, 0]),
            resources.log_directory.as_deref(),
        )
    }

    fn open_scoped(
        library_path: &Path,
        database_path: &Path,
        first_key_group: u32,
        last_key_group: u32,
        memory_limit: usize,
        scope: [u64; 2],
        log_directory: Option<&str>,
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
        let log_directory = log_directory.unwrap_or_default();
        let options = StateBackendOpenOptions {
            struct_size: std::mem::size_of::<StateBackendOpenOptions>(),
            path: database_path.as_ptr(),
            path_len: database_path.len(),
            first_key_group,
            last_key_group,
            memory_limit,
            memory_scope_high: scope[0],
            memory_scope_low: scope[1],
            log_directory: log_directory.as_ptr(),
            log_directory_len: log_directory.len(),
        };
        let mut handle = ptr::null_mut();
        let status = unsafe { (api.open)(&options, &mut handle) };
        check(api, status)?;
        if handle.is_null() {
            return Err(DataFusionError::Execution(
                "RocksDB state plugin returned a null handle".to_string(),
            ));
        }
        Ok(Self {
            library: Arc::new(library),
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
        let mut batch = RecordBatch::from(StructArray::from(data));
        *batch.schema_metadata_mut() = output_schema.metadata()?;
        Ok(batch)
    }

    fn invoke_admitted(
        &self,
        operation: streamfusion_state_abi::AdmittedArrowOperation,
        input: RecordBatch,
        reservation: &mut HostMemoryReservation,
    ) -> Result<RecordBatch> {
        let input_data = StructArray::from(input).to_data();
        let mut input_array = FFI_ArrowArray::new(&input_data);
        let mut input_schema = FFI_ArrowSchema::try_from(input_data.data_type())?;
        let mut output_array = FFI_ArrowArray::empty();
        let mut output_schema = FFI_ArrowSchema::empty();
        let mut callback = ReadAdmission {
            reservation,
            error: None,
        };
        let admission = StateMemoryAdmission {
            context: (&mut callback as *mut ReadAdmission<'_>).cast(),
            try_grow: admit_read,
        };
        let status = unsafe {
            operation(
                self.handle,
                &mut input_array,
                &mut input_schema,
                &mut output_array,
                &mut output_schema,
                &admission,
            )
        };
        if let Some(error) = callback.error {
            return Err(error);
        }
        check(self.api, status)?;
        let data = unsafe { from_ffi(output_array, &output_schema) }?;
        let mut batch = RecordBatch::from(StructArray::from(data));
        *batch.schema_metadata_mut() = output_schema.metadata()?;
        Ok(batch)
    }
}

impl KeyedState for RocksPluginKeyedState {
    fn get_batch<'a>(
        &'a self,
        keys: &[StateKeyRef<'_>],
        owner: &HostMemoryReservation,
    ) -> Result<super::StateReadBatch<'a>> {
        if keys.is_empty() {
            return Ok(super::StateReadBatch::empty(owner));
        }
        let slots = super::StateReadBatch::admit(keys.len(), owner)?;
        // A hot key can occur thousands of times in one operator batch. Request
        // its stored payload once, then reuse its Arrow owner in logical order.
        // Singleton reads avoid the hash table and its temporary reservation.
        let coalesced = (keys.len() > 1)
            .then(|| super::read_keys::CoalescedReadKeys::new(keys, owner))
            .transpose()?;
        let unique_keys = coalesced.as_ref().map_or(keys, |read| &read.unique);
        let mut reservation = owner.sibling("RocksDB batch read buffers");
        // Admit key copies, result descriptors, BinaryView headers, and ABI control
        // objects before construction. The plugin admits payload bytes after pinning
        // the batch and before allocating owned values.
        let overhead = unique_keys.iter().fold(4096usize, |bytes, key| {
            bytes
                .saturating_add(key.key.len().saturating_mul(3))
                .saturating_add(512)
        });
        reservation.try_grow(overhead)?;
        let input = keys_batch(unique_keys, None)?;
        let output = self.invoke_admitted(self.api.get_batch, input, &mut reservation)?;
        let values = column::<BinaryViewArray>(&output, 0, "value")?;
        if values.len() != unique_keys.len() {
            return Err(DataFusionError::Execution(
                "RocksDB read returned a different number of values than requested keys".into(),
            ));
        }
        let values_owner = Arc::new(super::value::AccountedStateValues::new(
            values.clone(),
            reservation,
            Some(Arc::clone(&self.library)),
        ));
        let values = (0..keys.len())
            .map(|position| {
                let row = coalesced
                    .as_ref()
                    .map_or(position, |read| read.positions[position]);
                (!values.is_null(row)).then(|| StateValue::ArrowView {
                    values: Arc::clone(&values_owner),
                    index: row,
                })
            })
            .collect();
        Ok(super::StateReadBatch::new(values, slots))
    }

    fn write_batch(&mut self, mutations: Vec<StateMutation>) -> Result<()> {
        let input = mutations_batch(mutations)?;
        self.invoke(self.api.write_batch, input)?;
        Ok(())
    }

    fn visit_key_group(
        &self,
        key_group: u32,
        max_rows: usize,
        max_bytes: usize,
        visitor: &mut dyn FnMut(&[(&[u8], &[u8])]) -> Result<()>,
    ) -> Result<()> {
        self.visit_range(key_group, &[], None, max_rows, max_bytes, &mut |page| {
            visitor(page)?;
            Ok(true)
        })
    }

    fn visit_prefix(
        &self,
        key_group: u32,
        prefix: &[u8],
        max_rows: usize,
        max_bytes: usize,
        visitor: &mut dyn FnMut(&[(&[u8], &[u8])]) -> Result<()>,
    ) -> Result<()> {
        let end = super::prefix_end(prefix);
        self.visit_range(
            key_group,
            prefix,
            end.as_deref(),
            max_rows,
            max_bytes,
            &mut |page| {
                visitor(page)?;
                Ok(true)
            },
        )
    }

    fn visit_range(
        &self,
        key_group: u32,
        start: &[u8],
        end: Option<&[u8]>,
        max_rows: usize,
        max_bytes: usize,
        visitor: &mut dyn FnMut(&[(&[u8], &[u8])]) -> Result<bool>,
    ) -> Result<()> {
        self.scan_range(key_group, start, end, max_rows, max_bytes, visitor)
    }

    fn snapshot_key_group(
        &self,
        key_group: u32,
        owner: &HostMemoryReservation,
    ) -> Result<super::SnapshotBytes> {
        let mut reservation = owner.sibling("canonical RocksDB snapshot");
        reservation.resize(4096)?;
        let input = key_group_batch(key_group, None)?;
        let output = self.invoke_admitted(self.api.snapshot_key_group, input, &mut reservation)?;
        let state = column::<BinaryViewArray>(&output, 0, "state")?;
        if state.len() != 1 || state.is_null(0) {
            return Err(DataFusionError::Execution(
                "RocksDB snapshot returned invalid Arrow state".to_string(),
            ));
        }
        Ok(super::SnapshotBytes::arrow(
            state.clone(),
            reservation,
            Arc::clone(&self.library),
        ))
    }

    fn restore_key_group(
        &mut self,
        key_group: u32,
        bytes: &[u8],
        owner: &HostMemoryReservation,
    ) -> Result<()> {
        let count = streamfusion_state_abi::validate_key_group_snapshot(key_group, bytes)
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        let mut reservation = owner.sibling("canonical RocksDB restore workspace");
        reservation.resize(
            bytes
                .len()
                .saturating_mul(4)
                .saturating_add(count.saturating_mul(128))
                .saturating_add(4096),
        )?;
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

struct ReadAdmission<'a> {
    reservation: &'a mut HostMemoryReservation,
    error: Option<DataFusionError>,
}

unsafe extern "C" fn admit_read(context: *mut c_void, bytes: usize) -> i32 {
    // The ABI borrows this callback only for the synchronous get_batch call.
    let context = unsafe { &mut *context.cast::<ReadAdmission<'_>>() };
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        context.reservation.try_grow(bytes)
    })) {
        Ok(Ok(())) => STATE_BACKEND_OK,
        Ok(Err(error)) => {
            context.error = Some(error);
            -1
        }
        Err(_) => {
            context.error = Some(DataFusionError::Execution(
                "host state memory admission panicked".into(),
            ));
            -1
        }
    }
}

fn mutations_batch(mutations: Vec<StateMutation>) -> Result<RecordBatch> {
    let groups =
        UInt32Array::from_iter_values(mutations.iter().map(|mutation| mutation.key.key_group));
    let (keys, values): (Vec<_>, Vec<_>) = mutations
        .into_iter()
        .map(|mutation| (Some(mutation.key.key), mutation.value))
        .unzip();
    let keys =
        streamfusion_state_abi::owned_binary_views(keys).map_err(DataFusionError::Execution)?;
    let values =
        streamfusion_state_abi::owned_binary_views(values).map_err(DataFusionError::Execution)?;
    Ok(RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("key_group", DataType::UInt32, false),
            Field::new("key", DataType::BinaryView, false),
            Field::new("value", DataType::BinaryView, true),
        ])),
        vec![Arc::new(groups), Arc::new(keys), Arc::new(values)],
    )?)
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
    fn repeated_hot_keys_read_one_payload_and_preserve_key_groups_order_and_missing_values() {
        let Ok(plugin) = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN") else {
            return;
        };
        let directory = tempfile::tempdir().unwrap();
        let mut state =
            RocksPluginKeyedState::open(Path::new(&plugin), directory.path(), 0, 1, 1 << 20)
                .unwrap();
        let first = vec![17; 128 << 10];
        let second = vec![42; 128 << 10];
        state
            .write_batch(vec![
                mutation(0, b"hot", &first),
                mutation(1, b"hot", &second),
            ])
            .unwrap();
        let keys = (0..256)
            .map(|row| StateKeyRef {
                key_group: (row % 2) as u32,
                key: if row % 3 == 0 { b"missing" } else { b"hot" },
            })
            .collect::<Vec<_>>();
        let broker = Arc::new(TestBroker::new(1 << 20));
        let owner = HostMemoryReservation::new(broker.clone(), "hot key batch");
        let values = state.get_batch(&keys, &owner).unwrap();
        assert_eq!(values.len(), keys.len());
        for (key, value) in keys.iter().zip(values.iter()) {
            assert_eq!(
                value.as_deref(),
                if key.key == b"missing" {
                    None
                } else if key.key_group == 0 {
                    Some(first.as_slice())
                } else {
                    Some(second.as_slice())
                }
            );
        }
        // Repeated logical requests retain the same producer-owned bytes, not copied payloads.
        assert_eq!(
            values[1].as_ref().unwrap().as_ptr(),
            values[5].as_ref().unwrap().as_ptr()
        );
        assert_eq!(
            values[2].as_ref().unwrap().as_ptr(),
            values[4].as_ref().unwrap().as_ptr()
        );
        let retained = values[1].clone();
        drop(values);
        assert!(broker.reserved() >= first.len() + second.len());
        drop(retained);
        assert_eq!(broker.reserved(), 0);
    }

    #[test]
    fn batch_reads_admit_payload_and_keep_charge_through_the_last_value_clone() {
        let Ok(plugin) = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN") else {
            return;
        };
        let directory = tempfile::tempdir().unwrap();
        let mut state =
            RocksPluginKeyedState::open(Path::new(&plugin), directory.path(), 0, 0, 1 << 20)
                .unwrap();
        let payload = vec![42; 200_000];
        state
            .write_batch(vec![mutation(0, b"large", &payload)])
            .unwrap();
        let keys = [StateKeyRef {
            key_group: 0,
            key: b"large",
        }];

        let small = Arc::new(TestBroker::new(64 << 10));
        let owner = HostMemoryReservation::new(small.clone(), "denied read");
        let error = state.get_batch(&keys, &owner).unwrap_err();
        assert!(
            error.to_string().contains("Flink denied 200000 bytes"),
            "{error}"
        );
        assert_eq!(small.reserved(), 0);

        let broker = Arc::new(TestBroker::new(1 << 20));
        let owner = HostMemoryReservation::new(broker.clone(), "read test");
        let values = state.get_batch(&keys, &owner).unwrap();
        assert_eq!(values[0].as_deref(), Some(payload.as_slice()));
        assert!(broker.reserved() >= payload.len());
        let used = broker.reserved();
        let retained = values[0].clone().unwrap();
        let pointer = retained.as_ptr();
        drop(values);
        assert_eq!(
            broker.reserved(),
            used - std::mem::size_of::<Option<StateValue<'_>>>()
        );
        assert_eq!(retained.as_ptr(), pointer);
        drop(retained);
        assert_eq!(broker.reserved(), 0);
    }

    #[test]
    fn detached_value_owner_retains_plugin_until_arrow_release() {
        let Ok(plugin) = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN") else {
            return;
        };
        let directory = tempfile::tempdir().unwrap();
        let mut state =
            RocksPluginKeyedState::open(Path::new(&plugin), directory.path(), 0, 0, 1 << 20)
                .unwrap();
        state
            .write_batch(vec![mutation(0, b"key", &[7; 64])])
            .unwrap();
        let library = Arc::downgrade(&state.library);
        let broker = Arc::new(TestBroker::new(1 << 20));
        let owner = HostMemoryReservation::new(broker.clone(), "detached state value");
        let values = {
            let mut batch = state
                .get_batch(
                    &[StateKeyRef {
                        key_group: 0,
                        key: b"key",
                    }],
                    &owner,
                )
                .unwrap();
            let StateValue::ArrowView { values, .. } = batch.pop().unwrap().unwrap() else {
                panic!("expected Arrow owner");
            };
            values
        };
        drop(state);
        assert!(library.upgrade().is_some());
        let value = StateValue::ArrowView { values, index: 0 };
        assert_eq!(value.as_ref(), &[7; 64]);
        assert!(broker.reserved() >= 64);
        drop(value);
        assert!(library.upgrade().is_none());
        assert_eq!(broker.reserved(), 0);
    }

    #[test]
    fn canonical_snapshot_is_admitted_and_owns_its_charge_until_release() {
        let Ok(plugin) = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN") else {
            return;
        };
        let directory = tempfile::tempdir().unwrap();
        let mut state =
            RocksPluginKeyedState::open(Path::new(&plugin), directory.path(), 0, 0, 1 << 20)
                .unwrap();
        state
            .write_batch(vec![mutation(0, b"key", &vec![7; 100_000])])
            .unwrap();
        let denied = Arc::new(TestBroker::new(16 << 10));
        let owner = HostMemoryReservation::new(denied.clone(), "snapshot denial");
        assert!(state.snapshot_key_group(0, &owner).is_err());
        assert_eq!(denied.reserved(), 0);
        let broker = Arc::new(TestBroker::new(1 << 20));
        let owner = HostMemoryReservation::new(broker.clone(), "snapshot");
        let snapshot = state.snapshot_key_group(0, &owner).unwrap();
        assert_eq!(snapshot.len(), 100_027);
        assert!(broker.reserved() >= snapshot.len());
        let used = broker.reserved();
        drop(state);
        assert_eq!(broker.reserved(), used);
        assert_eq!(
            super::super::snapshot::decode(0, &snapshot).unwrap()[0].1,
            vec![7; 100_000]
        );
        drop(snapshot);
        assert_eq!(broker.reserved(), 0);
    }

    #[test]
    fn mutations_transfer_owned_payloads_into_arrow_without_repacking() {
        let key = vec![1; 128];
        let value = vec![2; 64 * 1024];
        let key_pointer = key.as_ptr();
        let value_pointer = value.as_ptr();
        let batch = mutations_batch(vec![StateMutation {
            key: StateKey { key_group: 7, key },
            value: Some(value),
        }])
        .unwrap();
        let keys = column::<BinaryViewArray>(&batch, 1, "key").unwrap();
        let values = column::<BinaryViewArray>(&batch, 2, "value").unwrap();
        assert_eq!(keys.value(0).as_ptr(), key_pointer);
        assert_eq!(values.value(0).as_ptr(), value_pointer);
    }

    #[test]
    fn bounded_scans_visit_each_owned_entry_once_on_both_backends() {
        let Ok(plugin) = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN") else {
            return;
        };
        let directory = tempfile::tempdir().unwrap();
        let memory = MemoryKeyedState::new(
            3,
            4,
            HostMemoryReservation::new(Arc::new(TestBroker::new(1 << 20)), "scan test"),
        )
        .unwrap();
        let rocks =
            RocksPluginKeyedState::open(Path::new(&plugin), directory.path(), 3, 4, 1 << 20)
                .unwrap();
        for mut state in [Box::new(memory) as Box<dyn KeyedState>, Box::new(rocks)] {
            state
                .write_batch(
                    (0..93u32)
                        .map(|index| StateMutation {
                            key: StateKey {
                                key_group: 3,
                                key: index.to_be_bytes().to_vec(),
                            },
                            value: Some(vec![index as u8; 40]),
                        })
                        .chain(std::iter::once(StateMutation {
                            key: StateKey {
                                key_group: 4,
                                key: b"other-group".to_vec(),
                            },
                            value: Some(vec![9]),
                        }))
                        .collect(),
                )
                .unwrap();
            let mut seen = std::collections::BTreeMap::new();
            let mut pages = 0;
            state
                .visit_key_group(3, 7, 700, &mut |entries| {
                    pages += 1;
                    assert!(entries.len() <= 7);
                    assert!(
                        entries
                            .iter()
                            .map(|(key, value)| key.len() + value.len() + 96)
                            .sum::<usize>()
                            <= 700
                    );
                    for (key, value) in entries {
                        assert!(seen.insert(key.to_vec(), value.to_vec()).is_none());
                    }
                    Ok(())
                })
                .unwrap();
            assert!(pages > 10);
            assert_eq!(seen.len(), 93);
            for index in 0..93u32 {
                assert_eq!(seen[&index.to_be_bytes().to_vec()], vec![index as u8; 40]);
            }
            let mut called = false;
            assert!(state
                .visit_key_group(3, 1, 4, &mut |_| {
                    called = true;
                    Ok(())
                })
                .is_err());
            assert!(!called);
        }
    }

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
        let snapshot_owner =
            HostMemoryReservation::new(Arc::new(TestBroker::new(1 << 20)), "snapshot test");
        let rocks_snapshot = rocks.snapshot_key_group(2, &snapshot_owner).unwrap();

        let broker = Arc::new(TestBroker::new(1 << 20));
        let mut memory = MemoryKeyedState::new(
            2,
            2,
            HostMemoryReservation::new(broker, "canonical snapshot test"),
        )
        .unwrap();
        memory
            .restore_key_group(2, &rocks_snapshot, &snapshot_owner)
            .unwrap();
        assert_eq!(
            memory.snapshot_key_group(2, &snapshot_owner).unwrap(),
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
            .restore_key_group(
                2,
                &memory.snapshot_key_group(2, &snapshot_owner).unwrap(),
                &snapshot_owner,
            )
            .unwrap();
        let values = restored_rocks
            .get_batch(
                &[
                    StateKeyRef {
                        key_group: 2,
                        key: b"a",
                    },
                    StateKeyRef {
                        key_group: 2,
                        key: b"b",
                    },
                ],
                &HostMemoryReservation::new(Arc::new(TestBroker::new(1 << 20)), "read test"),
            )
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
