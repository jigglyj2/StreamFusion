// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! Global partials use the same unary tree, controls, metrics and canonical state binding.
use super::*;

fn global_plan(trigger: u64) -> proto::NativePlan {
    let mut plan = plan(true);
    let proto::operator::Operator::Calc(root) =
        plan.root.as_mut().unwrap().operator.as_mut().unwrap()
    else {
        unreachable!()
    };
    let node = root.input.as_mut().unwrap();
    let proto::operator::Operator::GroupAggregate(raw) = node.operator.take().unwrap() else {
        unreachable!()
    };
    let mut calls = raw.aggregate_calls;
    // Raw SQL indices must never index the receiving key + opaque accumulator batch.
    calls[1].input_index = Some(27);
    node.operator = Some(proto::operator::Operator::GlobalGroupAggregate(Box::new(
        proto::GlobalGroupAggregate {
            input: raw.input,
            grouping_indices: raw.grouping_indices,
            aggregate_calls: calls,
            generate_update_before: true,
            mini_batch_size: trigger,
            input_schema: Some(proto::Schema {
                fields: vec![
                    proto::Field {
                        name: "key".into(),
                        r#type: Some(bigint(false)),
                    },
                    proto::Field {
                        name: "partial".into(),
                        r#type: Some(proto::LogicalType {
                            nullable: false,
                            r#type: Some(proto::logical_type::Type::Binary(proto::EmptyType {})),
                        }),
                    },
                ],
            }),
            output_schema: Some(proto::Schema {
                fields: vec![
                    proto::Field {
                        name: "key".into(),
                        r#type: Some(bigint(false)),
                    },
                    proto::Field {
                        name: "count".into(),
                        r#type: Some(bigint(false)),
                    },
                    proto::Field {
                        name: "sum".into(),
                        r#type: Some(bigint(true)),
                    },
                ],
            }),
            bounded_final_output: false,
        },
    )));
    plan
}

fn global_node(plan: &proto::NativePlan) -> &proto::Operator {
    let proto::operator::Operator::Calc(root) =
        plan.root.as_ref().unwrap().operator.as_ref().unwrap()
    else {
        unreachable!()
    };
    root.input.as_ref().unwrap()
}

fn input(rows: &[(i64, i64, bool)]) -> RecordBatch {
    let plan = global_plan(3);
    let proto::operator::Operator::GlobalGroupAggregate(global) =
        global_node(&plan).operator.as_ref().unwrap()
    else {
        unreachable!()
    };
    let calls = global
        .aggregate_calls
        .iter()
        .map(lower_call)
        .collect::<Result<Vec<_>>>()
        .unwrap();
    let bytes = rows
        .iter()
        .map(|(_, value, insert)| {
            let mut state = AccumulatorState::new(&calls);
            state
                .apply_values(
                    &calls,
                    &[None, Some(AggregateValue::Int(*value as i128))],
                    *insert,
                )
                .unwrap();
            encode_state(&state)
        })
        .collect::<Vec<_>>();
    let mut fields = crate::planner::arrow_schema(global.input_schema.as_ref().unwrap())
        .unwrap()
        .fields()
        .to_vec();
    fields.extend([
        Arc::new(Field::new("__streamfusion_row_kind", DataType::Int8, false)),
        Arc::new(Field::new(INPUT_ROW, DataType::Int32, false)),
    ]);
    RecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
            Arc::new(BinaryArray::from_iter_values(
                bytes.iter().map(Vec::as_slice),
            )),
            Arc::new(Int8Array::from(vec![INSERT; rows.len()])),
            Arc::new(Int32Array::from_iter_values(0..rows.len() as i32)),
        ],
    )
    .unwrap()
}

fn values(batches: &[RecordBatch]) -> Vec<(i64, i64, Option<i64>, i8)> {
    batches
        .iter()
        .flat_map(|batch| {
            let key = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let count = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let sum = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let kinds = batch
                .column(4)
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap();
            (0..batch.num_rows()).map(move |row| {
                (
                    key.value(row),
                    count.value(row),
                    (!sum.is_null(row)).then(|| sum.value(row)),
                    kinds.value(row),
                )
            })
        })
        .collect()
}

#[test]
fn partial_global_tree_drains_and_restores_cross_backend_with_metrics_and_retractions() {
    let plugin = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").ok();
    for rocks_first in [false, true] {
        if rocks_first && plugin.is_none() {
            continue;
        }
        let directory = tempfile::tempdir().unwrap();
        let broker = Arc::new(Broker {
            inner: TestBroker::new(128 << 20),
            transfers: AtomicUsize::new(0),
        });
        let memory = HostMemoryReservation::new(broker.clone(), "global tree");
        let mut snapshots: Vec<Vec<u8>> = Vec::new();
        for phase in 0..2 {
            let path = directory.path().join(format!("phase-{phase}"));
            let rocks = (rocks_first != (phase != 0))
                .then(|| plugin.as_ref().map(|p| (p.as_str(), path.as_path())))
                .flatten();
            let context = context(&global_plan(100_000), &binding(rocks), &memory);
            if phase > 0 {
                for (group, bytes) in snapshots.iter().enumerate() {
                    context.restore_state(3, group as u32, bytes).unwrap();
                }
            }
            let rows = (0..5000)
                .map(|key| (key, key * 7, phase == 0))
                .collect::<Vec<_>>();
            let batch = input(&rows);
            assert_eq!(run(&context, batch.clone()).num_rows(), 0);
            assert!(context
                .snapshot_state(3, 0)
                .unwrap_err()
                .to_string()
                .contains("bundle control drain"));
            let (gauges, credit) = context.gauge_snapshot().unwrap();
            assert_eq!(gauges, vec![5000, 1.0f64.to_bits() as i64]);
            drop((gauges, credit));
            let mut output = Vec::new();
            for event in [
                ControlEvent::BeforeCheckpoint(7),
                ControlEvent::Watermark(100),
                ControlEvent::EndInput,
            ] {
                let drained = control::drain(&context, batch.slice(0, 0), &[(3, event)]);
                if event != ControlEvent::BeforeCheckpoint(7) {
                    assert!(drained.iter().all(|batch| batch.num_rows() == 0));
                }
                output.extend(drained);
            }
            assert!(output.iter().all(|batch| batch.num_rows() <= 4096));
            let mut actual = values(&output);
            actual.sort();
            assert_eq!(
                actual,
                (0..5000)
                    .map(|key| (
                        key,
                        1,
                        Some(key * 7),
                        if phase == 0 { INSERT } else { DELETE }
                    ))
                    .collect::<Vec<_>>()
            );
            for batch in &output {
                assert_eq!(batch.schema().field(3).name(), OWNED_TIMESTAMP_V1);
                assert_eq!(batch.column(3).null_count(), batch.num_rows());
                assert!(batch
                    .column(5)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .all(|row| *row == -1));
            }
            assert_eq!(
                context.metric_snapshot().unwrap(),
                vec![4, 5000, 5000, 3, 5000, 5000, 2, 5000, 5000, 1, 0, 5000]
            );
            let (gauges, credit) = context.gauge_snapshot().unwrap();
            assert_eq!(gauges, vec![0, 0]);
            drop((gauges, credit));
            if phase == 0 {
                snapshots = (0..16)
                    .map(|group| context.snapshot_state(3, group).unwrap().to_vec())
                    .collect();
            }
            let retained = output
                .iter()
                .find(|batch| batch.num_rows() > 0)
                .unwrap()
                .column(1)
                .slice(0, 1);
            drop(output);
            drop(context);
            assert!(broker.inner.reserved() > 0);
            drop(retained);
            assert_eq!(broker.inner.reserved(), 0);
            assert_eq!(broker.transfers.load(Ordering::Relaxed), 0);
        }
    }
}

#[test]
fn partial_batches_preserve_changelog_across_chunkings_with_one_backend_read_and_write() {
    let plugin = std::env::var("STREAMFUSION_TEST_ROCKSDB_PLUGIN").ok();
    let rows = (0..180)
        .map(|row| ((row % 13) as i64, row as i64 * 7, true))
        .collect::<Vec<_>>();
    let rows = rows
        .iter()
        .copied()
        .chain(rows.iter().map(|(key, value, _)| (*key, *value, false)))
        .collect::<Vec<_>>();
    for trigger in [1, 3, 17] {
        let mut expected = None;
        for rocks in [false, true] {
            if rocks && plugin.is_none() {
                continue;
            }
            for chunk in [1, 7, rows.len()] {
                let directory = tempfile::tempdir().unwrap();
                let broker = Arc::new(TestBroker::new(128 << 20));
                let memory = HostMemoryReservation::new(broker.clone(), "global chunking");
                let bytes = proto::NativePlan {
                    protocol_version: 2,
                    root: Some(global_node(&global_plan(trigger)).clone()),
                }
                .encode_to_vec();
                let mut processor = if rocks {
                    GroupAggregateProcessor::new_rocksdb(
                        &bytes,
                        16,
                        0,
                        15,
                        std::path::Path::new(plugin.as_ref().unwrap()),
                        directory.path(),
                        1 << 20,
                        memory,
                    )
                    .unwrap()
                } else {
                    GroupAggregateProcessor::new(&bytes, 16, 0, 15, memory).unwrap()
                };
                let input = input(&rows);
                processor.prepare_output_schema(input.schema()).unwrap();
                let mut output = Vec::new();
                for offset in (0..rows.len()).step_by(chunk) {
                    let before = processor.statistics();
                    output.push(
                        processor
                            .process_batch(input.slice(offset, chunk.min(rows.len() - offset)))
                            .unwrap(),
                    );
                    let after = processor.statistics();
                    assert!(
                        after[0] - before[0] <= 1 && after[1] - before[1] <= 1,
                        "backend I/O belongs to incoming Arrow batch"
                    );
                }
                while let Some(batch) = processor.poll_control(ControlEvent::EndInput).unwrap() {
                    output.push(batch);
                }
                let actual = values(&output);
                if let Some(expected) = &expected {
                    assert_eq!(&actual, expected);
                } else {
                    expected = Some(actual);
                }
                drop(output);
                drop(processor);
                assert_eq!(broker.reserved(), 0);
            }
        }
    }
}

#[test]
fn invalid_global_lifecycle_is_rejected_before_opening_state() {
    for mode in 0..3 {
        let mut plan = global_plan(if mode == 0 { 0 } else { 3 });
        let proto::operator::Operator::Calc(root) =
            plan.root.as_mut().unwrap().operator.as_mut().unwrap()
        else {
            unreachable!()
        };
        let proto::operator::Operator::GlobalGroupAggregate(global) =
            root.input.as_mut().unwrap().operator.as_mut().unwrap()
        else {
            unreachable!()
        };
        if mode == 1 {
            global.input_schema = None;
        }
        if mode == 2 {
            global.bounded_final_output = true;
        }
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("must-not-open");
        let broker = Arc::new(TestBroker::new(32 << 20));
        let memory = HostMemoryReservation::new(broker.clone(), "invalid global plan");
        let mut context =
            NativeExecutionContext::new(&plan.encode_to_vec(), memory.datafusion_pool(32 << 20))
                .unwrap();
        let error = context
            .install_state(
                &binding(Some(("/missing-plugin.so", &path))).encode_to_vec(),
                memory,
            )
            .unwrap_err();
        assert!(error.to_string().contains(match mode {
            0 => "must be positive",
            1 => "requires input and output schemas",
            _ => "bounded-final control",
        }));
        assert!(!path.exists());
        drop(context);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn corrupt_and_unadmitted_partials_do_not_mutate_pending_or_access_state() {
    for denied in [false, true] {
        let broker = Arc::new(TestBroker::new(8 << 20));
        let bytes = proto::NativePlan {
            protocol_version: 2,
            root: Some(global_node(&global_plan(3)).clone()),
        }
        .encode_to_vec();
        let memory = HostMemoryReservation::new(broker.clone(), "partial validation");
        let mut blocker = memory.sibling("test occupied task memory");
        let mut processor = GroupAggregateProcessor::new(&bytes, 16, 0, 15, memory).unwrap();
        let valid = input(&[(7, 10, true)]);
        processor.prepare_output_schema(valid.schema()).unwrap();
        assert_eq!(
            processor.process_batch(valid.clone()).unwrap().num_rows(),
            0
        );
        let before = processor.statistics();
        let pending = processor.pending_elements;
        let mut incoming = input(&[(7, 20, true), (7, 30, true)]);
        if denied {
            blocker
                .resize(broker.available().unwrap().unwrap() - 2048)
                .unwrap();
        } else {
            let mut columns = incoming.columns().to_vec();
            let original = columns[1].as_any().downcast_ref::<BinaryArray>().unwrap();
            columns[1] = Arc::new(BinaryArray::from_vec(vec![original.value(0), b"invalid"]));
            incoming = RecordBatch::try_new(incoming.schema(), columns).unwrap();
        }
        let error = processor.process_batch(incoming).unwrap_err();
        assert!(
            error.to_string().contains(if denied {
                "global aggregate decoded partials"
            } else {
                "invalid magic"
            }),
            "{error}"
        );
        assert_eq!(processor.statistics(), before);
        assert_eq!(processor.pending_elements, pending);
        blocker.resize(0).unwrap();
        let output = processor
            .poll_control(ControlEvent::EndInput)
            .unwrap()
            .unwrap();
        assert_eq!(values(&[output]), vec![(7, 1, Some(10), INSERT)]);
        assert!(processor
            .poll_control(ControlEvent::EndInput)
            .unwrap()
            .is_none());
        drop(processor);
        assert_eq!(broker.reserved(), 0);
    }
}
