// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::*;

#[test]
fn rejects_old_abi_and_invalid_open_configuration_without_returning_a_handle() {
    unsafe {
        let mut api = ptr::null();
        assert_ne!(
            streamfusion_state_backend_init(STATE_BACKEND_ABI_VERSION - 1, &mut api),
            STATE_BACKEND_OK
        );
        assert!(api.is_null());
        assert_eq!(
            streamfusion_state_backend_init(STATE_BACKEND_ABI_VERSION, &mut api),
            STATE_BACKEND_OK
        );
        let mut handle = ptr::null_mut();
        assert_ne!(((*api).open)(ptr::null(), &mut handle), STATE_BACKEND_OK);
        assert!(handle.is_null());
        let mut options = StateBackendOpenOptions {
            struct_size: 0,
            path: b"/unused".as_ptr(),
            path_len: 7,
            first_key_group: 0,
            last_key_group: 0,
            memory_limit: 64 << 20,
            memory_scope_high: 0,
            memory_scope_low: 0,
            log_directory: ptr::null(),
            log_directory_len: 1,
        };
        assert_ne!(((*api).open)(&options, &mut handle), STATE_BACKEND_OK);
        assert!(handle.is_null());
        assert!(std::ffi::CStr::from_ptr(last_error())
            .to_str()
            .unwrap()
            .contains("options size"));
        options.struct_size = std::mem::size_of::<StateBackendOpenOptions>();
        assert_ne!(((*api).open)(&options, &mut handle), STATE_BACKEND_OK);
        assert!(handle.is_null());
        assert!(std::ffi::CStr::from_ptr(last_error())
            .to_str()
            .unwrap()
            .contains("log directory has a null"));
    }
}

#[test]
fn checkpoint_open_never_creates_a_database_and_preserves_existing_state() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("checkpoint");
    let path_text = path.to_str().unwrap();
    let options = StateBackendOpenOptions {
        struct_size: std::mem::size_of::<StateBackendOpenOptions>(),
        path: path_text.as_ptr(),
        path_len: path_text.len(),
        first_key_group: 3,
        last_key_group: 3,
        memory_limit: 1 << 20,
        memory_scope_high: 0,
        memory_scope_low: 0,
        log_directory: ptr::null(),
        log_directory_len: 0,
    };
    unsafe {
        let mut handle = ptr::null_mut();
        assert_ne!(
            (API.open_checkpoint)(&options, &mut handle),
            STATE_BACKEND_OK
        );
        assert!(handle.is_null());
        assert!(!path.join("CURRENT").exists());
    }
    // A directory alone must never count as a restored empty checkpoint.
    std::fs::create_dir_all(&path).unwrap();
    unsafe {
        let mut handle = ptr::null_mut();
        assert_ne!(
            (API.open_checkpoint)(&options, &mut handle),
            STATE_BACKEND_OK
        );
        assert!(handle.is_null());
        assert!(!path.join("CURRENT").exists());
    }
    std::fs::remove_dir_all(&path).unwrap();
    let source =
        RocksStateBackend::open_with_memory_limit(&directory.path().join("source"), 3, 3, 1 << 20)
            .unwrap();
    let key = crate::StateKey {
        key_group: 3,
        key: b"retained-key".to_vec(),
    };
    source.checkpoint(&path).unwrap();
    unsafe {
        let mut handle = ptr::null_mut();
        assert_eq!(
            (API.open_checkpoint)(&options, &mut handle),
            STATE_BACKEND_OK
        );
        assert_eq!(
            backend(handle)
                .unwrap()
                .get_batch(std::slice::from_ref(&key))
                .unwrap(),
            vec![None]
        );
        (API.close)(handle);
    }
    std::fs::remove_dir_all(&path).unwrap();
    source
        .write_batch(vec![crate::StateMutation {
            key: key.clone(),
            value: Some(b"retained-value".to_vec()),
        }])
        .unwrap();
    source.checkpoint(&path).unwrap();
    let durable_files = std::fs::read_dir(&path)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|file| {
            let name = file.file_name().unwrap().to_string_lossy();
            name == "CURRENT" || name.starts_with("MANIFEST-") || name.ends_with(".sst")
        })
        .map(|file| {
            let bytes = std::fs::read(&file).unwrap();
            (file, bytes)
        })
        .collect::<Vec<_>>();
    unsafe {
        let mut handle = ptr::null_mut();
        assert_eq!(
            (API.open_checkpoint)(&options, &mut handle),
            STATE_BACKEND_OK
        );
        assert!(!handle.is_null());
        let reader = backend(handle).unwrap();
        assert_eq!(
            reader.get_batch(std::slice::from_ref(&key)).unwrap(),
            vec![Some(b"retained-value".to_vec())]
        );
        assert!(reader
            .write_batch(vec![crate::StateMutation { key, value: None }])
            .is_err());
        (API.close)(handle);
    }
    for (file, bytes) in durable_files {
        assert_eq!(std::fs::read(file).unwrap(), bytes);
    }
    std::fs::write(path.join("CURRENT"), b"MANIFEST-999999\n").unwrap();
    unsafe {
        let mut handle = ptr::null_mut();
        assert_ne!(
            (API.open_checkpoint)(&options, &mut handle),
            STATE_BACKEND_OK
        );
        assert!(handle.is_null());
        assert_eq!(
            std::fs::read(path.join("CURRENT")).unwrap(),
            b"MANIFEST-999999\n"
        );
        assert!(!path.join("MANIFEST-999999").exists());
    }
}
