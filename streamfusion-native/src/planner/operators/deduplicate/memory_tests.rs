// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::*;
use crate::memory_pool::tests_support::TestBroker;
use arrow::array::{Int64Array, StringArray};
use prost::Message;

const LIMIT: usize = 32 << 20;
const WIDE: usize = 128 << 10;

#[derive(Clone, Copy, Debug)]
enum Mode {
    Rowtime,
    ProcessingTime,
    Changelog,
}

fn plan(mode: Mode) -> Vec<u8> {
    proto::NativePlan {
        protocol_version: crate::PLAN_PROTOCOL_VERSION,
        root: Some(proto::Operator {
            plan_node_id: 1,
            metric_name: String::new(),
            clear_record_timestamps: false,
            metric_uid: None,
            operator: Some(proto::operator::Operator::Deduplicate(Box::new(
                proto::Deduplicate {
                    input: None,
                    key_indices: vec![0],
                    order_index: 2,
                    keep_last: true,
                    generate_insert: true,
                    generate_update_before: true,
                    input_changelog: matches!(mode, Mode::Changelog),
                    processing_time: matches!(mode, Mode::ProcessingTime),
                },
            ))),
        }),
    }
    .encode_to_vec()
}

fn batch(mode: Mode, wide: bool) -> RecordBatch {
    let value = if wide {
        "x".repeat(WIDE)
    } else {
        "narrow".into()
    };
    let mut fields = vec![
        Field::new("key", DataType::Int64, false),
        Field::new("value", DataType::Utf8, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
    ];
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![1])),
        Arc::new(StringArray::from(vec![value.as_str()])),
        Arc::new(TimestampMillisecondArray::from(vec![if wide {
            100
        } else {
            200
        }])),
    ];
    if matches!(mode, Mode::Changelog) {
        fields.push(Field::new(
            "__streamfusion_stored_row",
            DataType::Binary,
            false,
        ));
        fields.push(Field::new(
            "__streamfusion_input_row_kind",
            DataType::Int8,
            false,
        ));
        columns.push(Arc::new(BinaryArray::from(vec![value.as_bytes()])));
        columns.push(Arc::new(Int8Array::from(vec![if wide {
            INSERT
        } else {
            DELETE
        }])));
    }
    fields.push(Field::new(
        "__streamfusion_input_row",
        DataType::Int32,
        false,
    ));
    columns.push(Arc::new(Int32Array::from(vec![37])));
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

fn each_backend(mut test: impl FnMut(Mode, DeduplicateProcessor, Arc<TestBroker>)) {
    let plugin = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").ok();
    for mode in [Mode::Rowtime, Mode::ProcessingTime, Mode::Changelog] {
        for rocks in [false, true] {
            if rocks && plugin.is_none() {
                continue;
            }
            let broker = Arc::new(TestBroker::new(LIMIT));
            let memory = HostMemoryReservation::new(broker.clone(), "historical row test");
            let directory = tempfile::tempdir().unwrap();
            let mut processor = if rocks {
                DeduplicateProcessor::new_rocksdb(
                    &plan(mode),
                    128,
                    0,
                    127,
                    std::path::Path::new(plugin.as_ref().unwrap()),
                    directory.path(),
                    4 << 20,
                    memory,
                )
            } else {
                DeduplicateProcessor::new(&plan(mode), 128, 0, 127, memory)
            }
            .unwrap();
            let input = batch(mode, true);
            if matches!(mode, Mode::Changelog) {
                drop(processor.process_selection(input).unwrap());
            } else {
                drop(processor.process_native(input).unwrap());
            }
            test(mode, processor, broker.clone());
            assert_eq!(broker.reserved(), 0, "mode={mode:?}, rocks={rocks}");
        }
    }
}

#[test]
fn historical_selection_keeps_its_admission_after_state_is_replaced_or_deleted() {
    each_backend(|mode, mut processor, broker| {
        let input = batch(mode, false);
        let selection = match mode {
            Mode::Rowtime => processor.select_rowtime(&input),
            Mode::ProcessingTime => processor.select_processing_time(&input),
            Mode::Changelog => processor.select_changelog(&input),
        }
        .unwrap();
        assert!(
            selection.stored_rows.as_ref().unwrap()[0]
                .as_ref()
                .unwrap()
                .len()
                >= WIDE
        );
        let retained = selection._historical_memory.as_ref().unwrap().size();
        assert!(retained >= WIDE * 8);
        assert_eq!(processor.scratch_reservation.size(), 0);
        drop(processor);
        assert_eq!(broker.reserved(), retained);
        drop(selection);
    });
}

#[test]
fn materialized_history_has_output_admission_before_the_outer_boundary_takes_ownership() {
    each_backend(|mode, mut processor, broker| {
        let input = batch(mode, false);
        assert!(input.get_array_memory_size() < WIDE / 4);
        let output = if matches!(mode, Mode::Changelog) {
            processor.process_selection_accounted(&input)
        } else {
            processor.process_arrow_accounted(&input)
        }
        .unwrap();
        assert!(output.get_array_memory_size() >= WIDE);
        if matches!(mode, Mode::Changelog) {
            assert_eq!(output.num_rows(), 1);
            assert_eq!(
                output
                    .column(2)
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .unwrap()
                    .value(0),
                vec![b'x'; WIDE]
            );
            assert_eq!(
                output
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .unwrap()
                    .values()
                    .as_ref(),
                &[DELETE]
            );
        } else {
            assert_eq!(output.num_rows(), 2);
            let values = output
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(values.value(0), "x".repeat(WIDE));
            assert_eq!(values.value(1), "narrow");
            assert_eq!(
                output
                    .column(3)
                    .as_any()
                    .downcast_ref::<Int8Array>()
                    .unwrap()
                    .values()
                    .as_ref(),
                &[UPDATE_BEFORE, UPDATE_AFTER]
            );
            assert_eq!(
                output
                    .column(4)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .as_ref(),
                &[37, 37]
            );
        }
        assert!(processor.scratch_reservation.size() >= output.get_array_memory_size());
        // Match the native output edge: admission follows a retained array even after the
        // selection workspace, processor and original output RecordBatch are all gone.
        let memory = processor
            .scratch_reservation
            .split(output.get_array_memory_size(), "retained historical output")
            .unwrap();
        let output = crate::memory_pool::arrow_lease::host_batch(output, memory).unwrap();
        let column = if matches!(mode, Mode::Changelog) {
            2
        } else {
            1
        };
        let retained = output.column(column).slice(0, 1);
        drop(output);
        drop(processor);
        assert!(broker.reserved() >= WIDE);
        drop(retained);
    });
}

#[test]
fn denied_historical_workspace_releases_batch_scratch_without_changing_state() {
    each_backend(|mode, mut processor, broker| {
        let before = (0..128)
            .map(|group| processor.snapshot_key_group(group).unwrap())
            .collect::<Vec<_>>();
        let mut pressure = HostMemoryReservation::new(broker.clone(), "competing operator");
        // Enough for the incoming tiny batch, not for materializing the much wider history.
        pressure
            .resize(LIMIT - broker.reserved() - WIDE * 2)
            .unwrap();
        let result = if matches!(mode, Mode::Changelog) {
            processor.process_selection(batch(mode, false))
        } else {
            processor
                .process_native(batch(mode, false))
                .map(|output| output.batch)
        };
        assert!(matches!(
            result,
            Err(DataFusionError::ResourcesExhausted(_))
        ));
        assert_eq!(processor.scratch_reservation.size(), 0);
        drop(pressure);
        for (group, before) in before.iter().enumerate() {
            assert_eq!(before, &processor.snapshot_key_group(group as u32).unwrap());
        }
        drop(before);
        drop(processor);
    });
}
