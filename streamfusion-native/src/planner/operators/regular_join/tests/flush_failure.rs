// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::state::{SnapshotBytes, StateReadBatch};

struct FailSecondWrite {
    inner: Box<dyn KeyedState>,
    writes: usize,
}

impl KeyedState for FailSecondWrite {
    fn get_batch<'a>(
        &'a self,
        keys: &[StateKeyRef<'_>],
        owner: &HostMemoryReservation,
    ) -> Result<StateReadBatch<'a>> {
        self.inner.get_batch(keys, owner)
    }

    fn write_batch(&mut self, mutations: Vec<StateMutation>) -> Result<()> {
        self.writes += 1;
        if self.writes == 2 {
            return Err(DataFusionError::Execution(
                "injected second write failure".into(),
            ));
        }
        self.inner.write_batch(mutations)
    }

    fn visit_key_group(
        &self,
        group: u32,
        rows: usize,
        bytes: usize,
        visitor: &mut dyn FnMut(&[(&[u8], &[u8])]) -> Result<()>,
    ) -> Result<()> {
        self.inner.visit_key_group(group, rows, bytes, visitor)
    }

    fn snapshot_key_group(
        &self,
        group: u32,
        owner: &HostMemoryReservation,
    ) -> Result<SnapshotBytes> {
        self.inner.snapshot_key_group(group, owner)
    }

    fn restore_key_group(
        &mut self,
        group: u32,
        bytes: &[u8],
        owner: &HostMemoryReservation,
    ) -> Result<()> {
        self.inner.restore_key_group(group, bytes, owner)
    }
}

#[test]
fn partial_dirty_flush_requires_recovery_and_replay_preserves_the_full_changelog() {
    for rocks in [false, true] {
        let plugin = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").ok();
        if rocks && plugin.is_none() {
            continue;
        }
        let directory = tempfile::tempdir().unwrap();
        let broker = Arc::new(TestBroker::new(128 << 20));
        let owner = HostMemoryReservation::new(broker.clone(), "join flush recovery");
        let serialized = plan(proto::RegularJoinType::Full);
        let mut actual = if rocks {
            RegularJoinProcessor::new_rocksdb(
                &serialized,
                128,
                0,
                127,
                std::path::Path::new(&std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").unwrap()),
                directory.path(),
                1 << 20,
                owner.sibling("RocksDB join"),
            )
            .unwrap()
        } else {
            RegularJoinProcessor::new(&serialized, 128, 0, 127, owner.sibling("memory join"))
                .unwrap()
        };
        let count = 5003;
        actual
            .begin_streaming_batch(
                0,
                batch(&vec![1; count], &vec!["left"; count], &vec![INSERT; count]),
            )
            .unwrap();
        while actual.next_streaming_batch().unwrap().is_some() {}
        let snapshots = (0..128)
            .map(|group| actual.snapshot_key_group(group).unwrap())
            .collect::<Vec<_>>();
        let mut reference =
            RegularJoinProcessor::new(&serialized, 128, 0, 127, owner.sibling("reference"))
                .unwrap();
        let mut recovered =
            RegularJoinProcessor::new(&serialized, 128, 0, 127, owner.sibling("recovered"))
                .unwrap();
        for (group, snapshot) in snapshots.iter().enumerate() {
            reference.restore_key_group(group as u32, snapshot).unwrap();
            recovered.restore_key_group(group as u32, snapshot).unwrap();
        }
        let empty = Box::new(MemoryKeyedState::new(0, 127, owner.sibling("replacement")).unwrap());
        let original = std::mem::replace(&mut actual.state, empty);
        actual.state = Box::new(FailSecondWrite {
            inner: original,
            writes: 0,
        });
        let input = batch(&[1], &["right"], &[INSERT]);
        let expected = reference.process_arrow(1, input.clone()).unwrap();
        let writes = actual.statistics()[1];
        actual.begin_streaming_batch(1, input.clone()).unwrap();
        let mut delivered = 0;
        loop {
            match actual.next_streaming_batch() {
                Ok(Some(output)) => delivered += output.num_rows(),
                Ok(None) => panic!("the second backend write must fail"),
                Err(error) => {
                    assert!(error.to_string().contains("injected second write failure"));
                    break;
                }
            }
        }
        assert!(
            delivered > 0,
            "exercise failure after partial changelog delivery"
        );
        let group = assign_key_group(&actual.group_key(1, &input, 0).unwrap(), 128);
        assert_ne!(
            actual.state.snapshot_key_group(group, &owner).unwrap(),
            snapshots[group as usize]
        );
        assert_eq!(actual.statistics()[1], writes + 1);
        assert!(actual.snapshot_key_group(group).is_err());
        assert!(actual.begin_streaming_batch(1, input.clone()).is_err());
        assert!(actual.next_streaming_batch().is_err());
        recovered.begin_streaming_batch(1, input).unwrap();
        let mut output = Vec::new();
        while let Some(batch) = recovered.next_streaming_batch().unwrap() {
            output.push(batch);
        }
        assert_eq!(
            arrow::compute::concat_batches(&expected.schema(), &output).unwrap(),
            expected
        );
        for group in 0..128 {
            assert_eq!(
                recovered.snapshot_key_group(group).unwrap(),
                reference.snapshot_key_group(group).unwrap()
            );
        }
        drop((actual, reference, recovered, snapshots, owner));
        assert_eq!(broker.reserved(), 0);
    }
}
