// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

#[test]
fn cancelling_after_two_output_pages_cannot_checkpoint_and_restores_all_rows() {
    for rocks in backends() {
        let (source, broker, _source_dir) = context(rocks, None, 0, 127);
        let left = (0..1201)
            .map(|i| format!("left-{}", i / 2).into_bytes())
            .collect::<Vec<_>>();
        for page in left.chunks(256) {
            run(
                &source,
                input(
                    &vec![9; page.len()],
                    &vec![100; page.len()],
                    &page.iter().map(Vec::as_slice).collect::<Vec<_>>(),
                    &vec![INSERT; page.len()],
                ),
                empty(),
                None,
            )
            .unwrap();
        }
        run(
            &source,
            empty(),
            input(&[9; 2], &[100; 2], &[b"a", b"b"], &[INSERT; 2]),
            None,
        )
        .unwrap();
        let snapshots = (0..128)
            .map(|group| source.snapshot_state(3, group).unwrap())
            .collect::<Vec<_>>();
        let mut stream = source
            .start_control(vec![empty(), empty()], &[(3, ControlEvent::Watermark(99))])
            .unwrap();
        let first = source.runtime().block_on(stream.next()).unwrap().unwrap();
        let second = source.runtime().block_on(stream.next()).unwrap().unwrap();
        assert_eq!(first.num_rows(), 512);
        assert_eq!(second.num_rows(), 512);
        drop(stream);
        assert!(source.snapshot_state(3, 0).is_err());
        assert!(run(&source, empty(), empty(), None).is_err());
        drop(source);
        assert!(broker.reserved() > 0);
        assert_eq!(
            pairs(&[second]).last().unwrap(),
            &(left[511].clone(), b"b".to_vec())
        );
        drop(first);
        let (restored, _, _restore_dir) = context(!rocks && backends().len() == 2, None, 0, 127);
        for (group, bytes) in snapshots.iter().enumerate() {
            restored.restore_state(3, group as u32, bytes).unwrap();
        }
        drop(snapshots);
        assert_eq!(broker.reserved(), 0);
        let output = run(
            &restored,
            empty(),
            empty(),
            Some(ControlEvent::Watermark(99)),
        )
        .unwrap();
        assert_eq!(
            pairs(&output),
            left.iter()
                .flat_map(|value| [
                    (value.clone(), b"a".to_vec()),
                    (value.clone(), b"b".to_vec())
                ])
                .collect::<Vec<_>>()
        );
        assert!(run(
            &restored,
            empty(),
            empty(),
            Some(ControlEvent::Watermark(100))
        )
        .unwrap()
        .is_empty());
    }
}
