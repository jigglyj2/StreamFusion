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
