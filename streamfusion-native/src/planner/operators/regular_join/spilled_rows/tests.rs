// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::memory_pool::tests_support::TestBroker;

fn rows(first: u64, count: usize) -> Vec<StoredRow> {
    (first..first + count as u64)
        .map(|id| StoredRow {
            id,
            row: Arc::from(vec![(id % 251) as u8; 1024]),
            associations: (id as i32).wrapping_neg(),
        })
        .collect()
}

#[test]
fn large_history_is_replayed_in_bounded_pages_and_stable_identity_order() {
    let directory = tempfile::tempdir().unwrap();
    let manager = Arc::new(
        crate::spill::disk_manager(vec![directory.path().to_path_buf()])
            .unwrap()
            .build()
            .unwrap(),
    );
    let broker = Arc::new(TestBroker::new(2 << 20));
    let owner = HostMemoryReservation::new(broker.clone(), "prepared history test");
    let mut writer = Writer::new(&manager, &owner).unwrap();
    let mut ranges = Vec::new();
    for group in 0..2 {
        writer.start_group().unwrap();
        for start in (0..10_240).step_by(128) {
            writer.write(&rows(group * 20_000 + start, 128)).unwrap();
        }
        ranges.push(writer.finish_group().unwrap());
    }
    let history = writer.finish().unwrap();
    assert!(manager.used_disk_space() > 20 << 20);
    assert_eq!(broker.reserved(), 0);
    for group in [1, 0, 1] {
        let mut reader = history.reader(ranges[group], &owner).unwrap();
        let mut expected = group as u64 * 20_000;
        while let Some(page) = reader.next().unwrap() {
            assert!(page.rows.len() <= 128);
            for row in &page.rows {
                assert_eq!(row.id, expected);
                assert_eq!(row.associations, (expected as i32).wrapping_neg());
                assert_eq!(&*row.row, vec![(expected % 251) as u8; 1024]);
                expected += 1;
            }
        }
        assert_eq!(expected, group as u64 * 20_000 + 10_240);
    }
    drop(history);
    assert_eq!(manager.used_disk_space(), 0);
    assert_eq!(broker.reserved(), 0);
    drop(manager);
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
}

#[test]
fn denied_encoding_can_retry_a_smaller_page_before_writing() {
    let directory = tempfile::tempdir().unwrap();
    let manager = Arc::new(
        crate::spill::disk_manager(vec![directory.path().to_path_buf()])
            .unwrap()
            .build()
            .unwrap(),
    );
    let broker = Arc::new(TestBroker::new(2 << 20));
    let owner = HostMemoryReservation::new(broker.clone(), "denied history encoding");
    let mut writer = Writer::new(&manager, &owner).unwrap();
    writer.start_group().unwrap();
    let schema_bytes = manager.used_disk_space();
    assert!(matches!(
        writer.write(&rows(0, 1024)),
        Err(DataFusionError::ResourcesExhausted(_))
    ));
    assert_eq!(manager.used_disk_space(), schema_bytes);
    for start in (0..1024).step_by(128) {
        writer.write(&rows(start, 128)).unwrap();
    }
    let range = writer.finish_group().unwrap();
    let history = writer.finish().unwrap();
    let mut reader = history.reader(range, &owner).unwrap();
    let mut count = 0;
    while let Some(page) = reader.next().unwrap() {
        count += page.rows.len();
    }
    assert_eq!(count, 1024);
    drop(reader);
    drop(history);
    assert_eq!(broker.reserved(), 0);
    assert_eq!(manager.used_disk_space(), 0);
}

#[test]
fn reader_and_decoded_page_keep_their_own_resource_lifetimes() {
    let directory = tempfile::tempdir().unwrap();
    let manager = Arc::new(
        crate::spill::disk_manager(vec![directory.path().to_path_buf()])
            .unwrap()
            .build()
            .unwrap(),
    );
    let broker = Arc::new(TestBroker::new(2 << 20));
    let owner = HostMemoryReservation::new(broker.clone(), "retained history");
    let mut writer = Writer::new(&manager, &owner).unwrap();
    writer.start_group().unwrap();
    writer.write(&rows(7, 128)).unwrap();
    let range = writer.finish_group().unwrap();
    let history = writer.finish().unwrap();
    let mut reader = history.reader(range, &owner).unwrap();
    drop(history);
    assert!(manager.used_disk_space() > 0);
    let page = reader.next().unwrap().unwrap();
    drop(reader);
    assert_eq!(manager.used_disk_space(), 0);
    assert!(broker.reserved() > 0);
    assert_eq!(page.rows[0].id, 7);
    assert_eq!(&*page.rows[0].row, vec![7; 1024]);
    drop(page);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn partial_writes_cancel_cleanly_and_truncated_files_are_rejected() {
    let directory = tempfile::tempdir().unwrap();
    let manager = Arc::new(
        crate::spill::disk_manager(vec![directory.path().to_path_buf()])
            .unwrap()
            .build()
            .unwrap(),
    );
    let broker = Arc::new(TestBroker::new(2 << 20));
    let owner = HostMemoryReservation::new(broker.clone(), "failed history");
    {
        let mut writer = Writer::new(&manager, &owner).unwrap();
        writer.start_group().unwrap();
        writer.write(&rows(7, 128)).unwrap();
        assert!(manager.used_disk_space() > 0);
    }
    assert_eq!(manager.used_disk_space(), 0);
    assert_eq!(broker.reserved(), 0);
    let mut writer = Writer::new(&manager, &owner).unwrap();
    writer.start_group().unwrap();
    writer.write(&rows(7, 128)).unwrap();
    let range = writer.finish_group().unwrap();
    let history = writer.finish().unwrap();
    std::fs::OpenOptions::new()
        .write(true)
        .open(history.file.path().unwrap())
        .unwrap()
        .set_len(range.end - 1)
        .unwrap();
    assert!(history.reader(range, &owner).is_err());
    drop(history);
    assert_eq!(manager.used_disk_space(), 0);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn retained_pages_prevent_unaccounted_read_ahead_and_denial_does_not_advance() {
    let directory = tempfile::tempdir().unwrap();
    let manager = Arc::new(
        crate::spill::disk_manager(vec![directory.path().to_path_buf()])
            .unwrap()
            .build()
            .unwrap(),
    );
    let broker = Arc::new(TestBroker::new(2 << 20));
    let owner = HostMemoryReservation::new(broker.clone(), "retained history pages");
    let mut writer = Writer::new(&manager, &owner).unwrap();
    writer.start_group().unwrap();
    writer.write(&rows(0, 128)).unwrap();
    writer.write(&rows(128, 128)).unwrap();
    let range = writer.finish_group().unwrap();
    let history = writer.finish().unwrap();
    let mut reader = history.reader(range, &owner).unwrap();
    let first = reader.next().unwrap().unwrap();
    assert!(matches!(
        reader.next(),
        Err(DataFusionError::ResourcesExhausted(_))
    ));
    assert_eq!(first.rows[0].id, 0);
    drop(first);
    let second = reader.next().unwrap().unwrap();
    assert_eq!(second.rows[0].id, 128);
    assert_eq!(second.rows.last().unwrap().id, 255);
    assert!(reader.next().unwrap().is_none());
    drop(second);
    drop(reader);
    drop(history);
    assert_eq!(broker.reserved(), 0);
}

#[test]
fn write_errors_poison_the_spool_instead_of_publishing_a_partial_group() {
    let directory = tempfile::tempdir().unwrap();
    let manager = Arc::new(
        crate::spill::disk_manager(vec![directory.path().to_path_buf()])
            .unwrap()
            .build()
            .unwrap(),
    );
    let broker = Arc::new(TestBroker::new(2 << 20));
    let owner = HostMemoryReservation::new(broker.clone(), "failed spill I/O");
    let mut writer = Writer::new(&manager, &owner).unwrap();
    writer.start_group().unwrap();
    manager
        .set_max_temp_directory_size(manager.used_disk_space() + 1024)
        .unwrap();
    assert!(writer.write(&rows(0, 128)).is_err());
    assert!(writer.finish_group().is_err());
    assert!(writer.start_group().is_err());
    assert!(writer.write(&rows(0, 1)).is_err());
    assert!(writer.finish().is_err());
    assert_eq!(manager.used_disk_space(), 0);
    assert_eq!(broker.reserved(), 0);
}
