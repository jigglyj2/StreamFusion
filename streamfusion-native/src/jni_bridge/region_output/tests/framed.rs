// Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0.
use super::*;
use crate::exchange::{output_bindings::OutputBindings, prepared_router::PreparedRouter};
use crate::memory_pool::{HostMemoryReservation, MemoryReservationBroker};

fn router(broker: &Arc<TestBroker>, preserve: bool) -> Arc<PreparedRouter> {
    let types = [
        proto::logical_type::Type::Integer(proto::EmptyType {}),
        proto::logical_type::Type::Tinyint(proto::EmptyType {}),
        proto::logical_type::Type::Bigint(proto::EmptyType {}),
    ];
    let names = [
        "n",
        "__streamfusion_row_kind",
        "__streamfusion_stream_record_timestamp",
    ];
    let plan = proto::NativeExchangePlan {
        protocol_version: 1,
        schema: Some(proto::Schema {
            fields: types
                .into_iter()
                .zip(names)
                .enumerate()
                .map(|(i, (kind, name))| proto::Field {
                    name: name.into(),
                    r#type: Some(proto::LogicalType {
                        nullable: i != 1,
                        r#type: Some(kind),
                    }),
                })
                .collect(),
        }),
        distribution: proto::ExchangeDistribution::Hash.into(),
        transport: proto::ExchangeTransport::ArrowIpcStream.into(),
        key_indices: vec![0],
        max_parallelism: 128,
        parallelism: 4,
        preserve_key_groups: preserve,
        metadata_columns: Some(proto::ExchangeMetadataColumns {
            row_kind_index: 1,
            stream_record_timestamp_index: Some(2),
            routing_key_index: None,
        }),
        transport_routing_key: false,
    };
    let mut memory = HostMemoryReservation::new(broker.clone(), "test output router");
    memory.try_grow(65536).unwrap();
    Arc::new(PreparedRouter::new(&plan.encode_to_vec(), broker.clone(), memory).unwrap())
}

#[test]
fn native_frames_preserve_envelopes_without_exporting_c_data_and_can_share_an_arrow_exit() {
    for arrow in [false, true] {
        let (context, broker) = tree_context();
        context
            .bind_exchange_outputs(
                OutputBindings::new(
                    &[arrow],
                    vec![(0, router(&broker, false)), (0, router(&broker, true))],
                )
                .unwrap(),
            )
            .unwrap();
        let handle = open(&context, true);
        let mut frame_rows = [0, 0];
        let mut arrow_rows = 0;
        loop {
            let mut array = FFI_ArrowArray::empty();
            let mut schema = FFI_ArrowSchema::empty();
            let event = with_output(handle, |output| unsafe {
                output.next_event(&mut array, &mut schema)
            })
            .unwrap();
            match event {
                Event::End => break,
                Event::Arrow(port, rows) => {
                    assert!(arrow);
                    assert_eq!(port, 0);
                    arrow_rows += rows;
                }
                Event::Frames(id, rows, frames) => {
                    assert!(array.is_released() && schema.release.is_none());
                    assert_eq!(rows, 2);
                    for frame in &frames.frames {
                        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
                            arrow::datatypes::Field::new(
                                "n",
                                arrow::datatypes::DataType::Int32,
                                true,
                            ),
                            arrow::datatypes::Field::new(
                                "__streamfusion_row_kind",
                                arrow::datatypes::DataType::Int8,
                                false,
                            ),
                            arrow::datatypes::Field::new(
                                "__streamfusion_stream_record_timestamp",
                                arrow::datatypes::DataType::Int64,
                                true,
                            ),
                        ]));
                        let batch = frame.frame().decode(schema).unwrap();
                        let values = batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .unwrap();
                        let kinds = batch
                            .column(1)
                            .as_any()
                            .downcast_ref::<Int8Array>()
                            .unwrap();
                        let times = batch
                            .column(2)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap();
                        for row in 0..batch.num_rows() {
                            if values.is_null(row) {
                                assert_eq!(kinds.value(row), 2);
                                assert!(times.is_null(row));
                            } else {
                                assert!([11, 29].contains(&values.value(row)));
                                assert_eq!(kinds.value(row), 1);
                                assert_eq!(times.value(row), 123);
                            }
                        }
                        frame_rows[id] += batch.num_rows();
                    }
                }
            }
        }
        assert_eq!(frame_rows, [4, 4]);
        assert_eq!(arrow_rows, if arrow { 4 } else { 0 });
        close(handle).unwrap();
        context.require_idle().unwrap();
        drop(context);
        assert_eq!(broker.reserved(), 0);
    }
}

#[test]
fn denied_frame_workspace_drops_pending_outputs_and_keeps_c_descriptors_empty() {
    let (context, broker) = tree_context();
    context
        .bind_exchange_outputs(
            OutputBindings::new(&[false], vec![(0, router(&broker, false))]).unwrap(),
        )
        .unwrap();
    let handle = open(&context, true);
    let competing = (16 << 20) - broker.reserved() - 4096;
    assert!(broker.try_reserve(competing).unwrap());
    let mut array = FFI_ArrowArray::empty();
    let mut schema = FFI_ArrowSchema::empty();
    let result = with_output(handle, |output| unsafe {
        output.next_event(&mut array, &mut schema)
    });
    assert!(result.is_err());
    assert!(array.is_released() && schema.release.is_none());
    assert!(with_output(handle, |output| unsafe {
        output.next_event(&mut array, &mut schema)
    })
    .is_err());
    close(handle).unwrap();
    broker.release(competing).unwrap();
    drop(context);
    assert_eq!(broker.reserved(), 0);
}
