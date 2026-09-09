// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

#[test]
fn optional_completion_metadata_preserves_legacy_pagination_and_rejects_invalid_values() {
    for (value, expected) in [(None, false), (Some("false"), false), (Some("true"), true)] {
        let schema = Schema::empty().with_metadata(
            value
                .into_iter()
                .map(|value| {
                    (
                        streamfusion_state_abi::STATE_SCAN_COMPLETE_METADATA.to_string(),
                        value.to_string(),
                    )
                })
                .collect(),
        );
        let batch = RecordBatch::new_empty(Arc::new(schema));
        assert_eq!(scan_complete(&batch).unwrap(), expected);
    }
    let schema = Schema::empty().with_metadata(std::collections::HashMap::from([(
        streamfusion_state_abi::STATE_SCAN_COMPLETE_METADATA.to_string(),
        "unknown".to_string(),
    )]));
    assert!(scan_complete(&RecordBatch::new_empty(Arc::new(schema))).is_err());
}

#[test]
fn component_completion_crosses_arrow_c_data_without_changing_page_contents() {
    let plugin = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").unwrap();
    let directory = tempfile::tempdir().unwrap();
    let mut state =
        RocksPluginKeyedState::open(Path::new(&plugin), directory.path(), 0, 0, 1 << 20).unwrap();
    state
        .write_batch(
            (0..3u8)
                .map(|i| StateMutation {
                    key: crate::state::StateKey {
                        key_group: 0,
                        key: vec![i],
                    },
                    value: Some(vec![i]),
                })
                .collect(),
        )
        .unwrap();
    for (after, rows, complete) in [(None, 2, false), (Some(&[1u8][..]), 1, true)] {
        let input = RecordBatch::try_from_iter(vec![
            (
                "key_group",
                Arc::new(UInt32Array::from(vec![0])) as Arc<dyn Array>,
            ),
            (
                "after",
                Arc::new(BinaryArray::from(vec![after])) as Arc<dyn Array>,
            ),
            (
                "max_rows",
                Arc::new(UInt32Array::from(vec![2])) as Arc<dyn Array>,
            ),
            (
                "max_bytes",
                Arc::new(UInt64Array::from(vec![4096])) as Arc<dyn Array>,
            ),
            (
                "start",
                Arc::new(BinaryArray::from_vec(vec![&[][..]])) as Arc<dyn Array>,
            ),
            (
                "end",
                Arc::new(BinaryArray::from(vec![None::<&[u8]>])) as Arc<dyn Array>,
            ),
        ])
        .unwrap();
        let output = state.invoke(state.api.scan_key_group, input).unwrap();
        assert_eq!(output.num_rows(), rows);
        assert_eq!(scan_complete(&output).unwrap(), complete);
        let keys = column::<BinaryViewArray>(&output, 0, "key").unwrap();
        for row in 0..rows {
            assert_eq!(
                keys.value(row),
                &[row as u8 + if after.is_none() { 0 } else { 2 }]
            );
        }
    }
    let mut seen = Vec::new();
    state
        .visit_range(0, &[], None, 2, 4096, &mut |entries| {
            seen.extend(entries.iter().map(|(key, _)| key[0]));
            Ok(true)
        })
        .unwrap();
    assert_eq!(seen, vec![0, 1, 2]);
}
