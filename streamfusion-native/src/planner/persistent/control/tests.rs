// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::proto;
use prost::Message;

#[test]
fn processing_time_is_versioned_and_distinct_from_event_time() {
    use proto::native_stage_control::Event;
    for value in [i64::MIN, -1, 0, 9999, i64::MAX] {
        let mut request = proto::NativeControlInvocation {
            protocol_version: 2,
            stages: vec![
                proto::NativeStageControl {
                    plan_node_id: 3,
                    event: Some(Event::ProcessingTimeMillis(value)),
                },
                proto::NativeStageControl {
                    plan_node_id: 5,
                    event: Some(Event::WatermarkMillis(42)),
                },
            ],
        };
        assert_eq!(
            ControlEvents::decode(&request.encode_to_vec()).unwrap(),
            vec![
                (3, ControlEvent::ProcessingTime(value)),
                (5, ControlEvent::Watermark(42))
            ]
        );
        request.protocol_version = 1;
        assert!(ControlEvents::decode(&request.encode_to_vec())
            .unwrap_err()
            .to_string()
            .contains("requires protocol 2"));
        request.stages.remove(0);
        assert_eq!(
            ControlEvents::decode(&request.encode_to_vec()).unwrap(),
            vec![(5, ControlEvent::Watermark(42))]
        );
        for version in [0, 3, u32::MAX] {
            request.protocol_version = version;
            assert!(ControlEvents::decode(&request.encode_to_vec()).is_err());
        }
    }
}
