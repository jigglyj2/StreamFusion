// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

#[test]
fn flat_encoding_allowance_covers_wide_nullable_schemas_and_nonzero_offsets() {
    use arrow::array::{BooleanArray, Decimal128Array, TimestampMillisecondArray};
    let rows = 257;
    let columns = (0..80).map(|index| {
        let column: ArrayRef = match index % 5 {
            0 => Arc::new(BooleanArray::from_iter(
                (0..rows).map(|i| (i % 3 != 0).then_some(i % 2 == 0)),
            )),
            1 => Arc::new(StringArray::from_iter((0..rows).map(|i| match i % 3 {
                0 => None,
                1 => Some(""),
                _ => Some("é"),
            }))),
            2 => Arc::new(
                Decimal128Array::from_iter(
                    (0..rows).map(|i| (i % 3 != 0).then_some(i as i128 - 100)),
                )
                .with_precision_and_scale(38, 7)
                .unwrap(),
            ),
            3 => Arc::new(TimestampMillisecondArray::from_iter(
                (0..rows).map(|i| (i % 3 != 0).then_some(i as i64 - 100)),
            )),
            _ => Arc::new(Int8Array::from_iter(
                (0..rows).map(|i| (i % 3 != 0).then_some(i as i8)),
            )),
        };
        (format!("v{index}"), column)
    });
    let input = RecordBatch::try_from_iter(columns).unwrap();
    for input in [input.clone(), input.slice(7, 129)] {
        let converter = row_converter(&input.schema()).unwrap();
        let allowance = super::super::input_memory::workspace(&input, input.num_columns()).unwrap();
        let (encoded, observed) = crate::allocation_test_support::measure(|| {
            converter.convert_columns(input.columns()).unwrap()
        });
        assert!(
            observed.peak <= allowance,
            "{} > {allowance}",
            observed.peak
        );
        assert_eq!(encoded.num_rows(), input.num_rows());
        assert_eq!(
            converter.convert_rows(encoded.iter()).unwrap(),
            input.columns()
        );
    }
}

#[test]
fn shared_ipc_input_and_slices_admit_only_the_join_encoding_workspace() {
    let keys = (0..2048).collect::<Vec<i64>>();
    let payload = "é".repeat(256);
    let input = batch(
        &keys,
        &vec![payload.as_str(); keys.len()],
        &vec![INSERT; keys.len()],
    );
    let frame = crate::exchange::IpcBatchFrame::encode(&input).unwrap();
    let metadata_len = frame.metadata.len();
    let mut bytes = frame.metadata;
    bytes.extend_from_slice(&frame.body);
    let decoded =
        crate::exchange::IpcBatchFrame::decode_contiguous(bytes, metadata_len, input.schema())
            .unwrap();
    assert_eq!(decoded, input);
    for input in [decoded.clone(), decoded.slice(7, 1024)] {
        let broker = Arc::new(TestBroker::new(16 << 20));
        let mut join = RegularJoinProcessor::new(
            &plan(proto::RegularJoinType::Inner),
            128,
            0,
            127,
            HostMemoryReservation::new(broker.clone(), "IPC join workspace"),
        )
        .unwrap();
        let before = broker.reserved();
        let ((), observed) = crate::allocation_test_support::measure(|| {
            join.begin_streaming_batch(0, input.clone()).unwrap();
        });
        assert!(
            observed.peak <= broker.reserved() - before,
            "observed {} allocated bytes, admitted {}",
            observed.peak,
            broker.reserved() - before
        );
        while let Some(output) = join.next_streaming_batch().unwrap() {
            assert_eq!(output.num_rows(), 0);
        }
        let keys = input
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let mut count = 0;
        for keys in keys.values().chunks(128) {
            join.begin_streaming_batch(
                1,
                batch(keys, &vec!["right"; keys.len()], &vec![INSERT; keys.len()]),
            )
            .unwrap();
            while let Some(output) = join.next_streaming_batch().unwrap() {
                count += output.num_rows();
                assert!(output
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .all(|value| value == Some(payload.as_str())));
            }
        }
        assert_eq!(count, input.num_rows());
        drop(join);
        assert_eq!(broker.reserved(), 0);
    }
}
