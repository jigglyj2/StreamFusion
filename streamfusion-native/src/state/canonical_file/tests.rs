// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::memory_pool::tests_support::TestBroker;
use std::io::Cursor;

#[test]
fn wide_payloads_and_unordered_keys_restore_through_bounded_index_and_arrow_reads() {
    let directory = tempfile::tempdir().unwrap();
    let resources = crate::spill::Resources::new(vec![directory.path().to_path_buf()]).unwrap();
    let broker = Arc::new(TestBroker::new(4 << 20));
    let owner = HostMemoryReservation::new(broker.clone(), "canonical file test");
    let entries = (0u32..3201)
        .rev()
        .map(|i| (i.to_be_bytes().to_vec(), vec![(i % 251) as u8; 4096]))
        .collect::<Vec<_>>();
    let bytes = streamfusion_state_abi::encode_key_group_snapshot(
        7,
        entries
            .iter()
            .map(|(key, value)| (key.as_slice(), value.as_slice())),
    )
    .unwrap();
    assert!(bytes.len() > 12 << 20);
    let mut input = Cursor::new(&bytes);
    let source =
        CanonicalFile::read(7, bytes.len() as u64, &mut input, &resources, &owner).unwrap();
    assert_eq!(input.position(), bytes.len() as u64);
    assert!(broker.reserved() < 256 << 10);
    let keys = [
        0u32.to_be_bytes(),
        1234u32.to_be_bytes(),
        9999u32.to_be_bytes(),
        1234u32.to_be_bytes(),
    ];
    let values = source
        .get_batch(
            &keys
                .iter()
                .map(|key| StateKeyRef { key_group: 7, key })
                .collect::<Vec<_>>(),
            &owner,
        )
        .unwrap();
    assert_eq!(values[0].as_ref().unwrap().as_ref(), vec![0; 4096]);
    assert_eq!(
        values[1].as_ref().unwrap().as_ref(),
        vec![(1234 % 251) as u8; 4096]
    );
    assert!(values[2].is_none());
    assert_eq!(
        values[1].as_ref().unwrap().as_ref(),
        values[3].as_ref().unwrap().as_ref()
    );
    let held: StateValue<'static> = match values[1].clone().unwrap() {
        StateValue::ArrowView { values, index } => StateValue::ArrowView { values, index },
        _ => panic!("file values must own their Arrow buffers"),
    };
    drop(values);
    let mut next = 0u32;
    source
        .visit_key_group(7, 64, 32 << 10, &mut |page| {
            assert!(page.len() <= 8);
            for &(key, value) in page {
                assert_eq!(key, next.to_be_bytes());
                assert_eq!(value, vec![(next % 251) as u8; 4096]);
                next += 1;
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(next, 3201);
    let mut seen = 0;
    source
        .visit_range(
            7,
            &11u32.to_be_bytes(),
            Some(&19u32.to_be_bytes()),
            3,
            100 << 10,
            &mut |page| {
                seen += page.len();
                Ok(false)
            },
        )
        .unwrap();
    assert_eq!(seen, 3);
    drop((source, resources));
    assert!(
        broker.reserved() > 0,
        "retained Arrow value keeps its credit"
    );
    assert_eq!(held.as_ref(), vec![(1234 % 251) as u8; 4096]);
    drop((held, owner));
    assert_eq!(broker.reserved(), 0);
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
}

#[test]
fn duplicate_truncated_and_wrong_headers_release_files_before_publication() {
    let directory = tempfile::tempdir().unwrap();
    let resources = crate::spill::Resources::new(vec![directory.path().to_path_buf()]).unwrap();
    let broker = Arc::new(TestBroker::new(4 << 20));
    let owner = HostMemoryReservation::new(broker.clone(), "canonical rejection");
    let valid = streamfusion_state_abi::encode_key_group_snapshot(
        7,
        [
            (b"a".as_slice(), b"one".as_slice()),
            (b"b".as_slice(), b"two".as_slice()),
        ]
        .into_iter(),
    )
    .unwrap();
    let duplicate = streamfusion_state_abi::encode_key_group_snapshot(
        7,
        [
            (b"a".as_slice(), b"one".as_slice()),
            (b"a".as_slice(), b"two".as_slice()),
        ]
        .into_iter(),
    )
    .unwrap();
    let mut variants = vec![duplicate];
    for index in [0, 4, 8, 12] {
        let mut bytes = valid.clone();
        bytes[index] ^= 255;
        variants.push(bytes);
    }
    let mut trailing = valid.clone();
    trailing.push(0);
    variants.push(trailing);
    for end in 0..valid.len() {
        variants.push(valid[..end].to_vec());
    }
    for bytes in variants {
        assert!(CanonicalFile::read(
            7,
            bytes.len() as u64,
            &mut Cursor::new(&bytes),
            &resources,
            &owner
        )
        .is_err());
        assert_eq!(broker.reserved(), 0);
        assert_eq!(resources.manager().unwrap().used_disk_space(), 0);
    }
    assert!(CanonicalFile::read(
        7,
        valid.len() as u64 + 1,
        &mut Cursor::new(valid),
        &resources,
        &owner
    )
    .is_err());
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn high_cardinality_directory_uses_datafusion_spilling_instead_of_a_resident_key_index() {
    let directory = tempfile::tempdir().unwrap();
    let resources = crate::spill::Resources::new(vec![directory.path().to_path_buf()]).unwrap();
    let broker = Arc::new(TestBroker::new(4 << 20));
    let owner = HostMemoryReservation::new(broker.clone(), "bounded directory sort");
    let entries = (0u32..120_003)
        .rev()
        .map(|i| (i.to_be_bytes().to_vec(), i.to_le_bytes().to_vec()))
        .collect::<Vec<_>>();
    let bytes = streamfusion_state_abi::encode_key_group_snapshot(
        7,
        entries
            .iter()
            .map(|(key, value)| (key.as_slice(), value.as_slice())),
    )
    .unwrap();
    let source = CanonicalFile::read(
        7,
        bytes.len() as u64,
        &mut Cursor::new(bytes),
        &resources,
        &owner,
    )
    .unwrap();
    assert!(
        source.sort_spills > 0,
        "DataFusion must own external sorting"
    );
    assert!(broker.reserved() < 256 << 10);
    let mut next = 0u32;
    source
        .visit_key_group(7, 511, 8192, &mut |page| {
            for &(key, value) in page {
                assert_eq!(key, next.to_be_bytes());
                assert_eq!(value, next.to_le_bytes());
                next += 1;
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(next, 120_003);
    drop((source, resources, owner));
    assert_eq!(broker.reserved(), 0);
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
}

#[test]
fn wide_keys_bound_datafusion_merge_batches_by_bytes() {
    let directory = tempfile::tempdir().unwrap();
    let resources = crate::spill::Resources::new(vec![directory.path().to_path_buf()]).unwrap();
    let broker = Arc::new(TestBroker::new(4 << 20));
    let owner = HostMemoryReservation::new(broker.clone(), "wide key merge");
    let entries = (0u32..3201)
        .rev()
        .map(|id| {
            let mut key = vec![b'x'; 4096];
            key[..4].copy_from_slice(&id.to_be_bytes());
            (key, id.to_le_bytes())
        })
        .collect::<Vec<_>>();
    let bytes = streamfusion_state_abi::encode_key_group_snapshot(
        7,
        entries
            .iter()
            .map(|(key, value)| (key.as_slice(), value.as_slice())),
    )
    .unwrap();
    assert!(bytes.len() > 12 << 20);
    let source = CanonicalFile::read(
        7,
        bytes.len() as u64,
        &mut Cursor::new(bytes),
        &resources,
        &owner,
    )
    .unwrap();
    assert!(source.sort_spills > 0);
    let mut next = 0u32;
    source
        .visit_key_group(7, 1024, 256 << 10, &mut |page| {
            assert!(page.len() <= 64);
            for &(key, value) in page {
                assert_eq!(&key[..4], next.to_be_bytes());
                assert_eq!(value, next.to_le_bytes());
                next += 1;
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(next, 3201);
    drop((source, resources, owner));
    assert_eq!(broker.reserved(), 0);
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
}
