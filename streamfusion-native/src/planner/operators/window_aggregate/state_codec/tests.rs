// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

#[test]
fn session_index_counts_are_bounded_by_the_frame_before_allocating() {
    let encoded = encode_session_index(&[(10, 20)]);
    assert_eq!(decode_session_index(&encoded).unwrap(), [(10, 20)]);
    for count in [2, u32::MAX] {
        let mut corrupt = encoded.clone();
        corrupt[5..9].copy_from_slice(&count.to_le_bytes());
        assert!(decode_session_index(&corrupt)
            .unwrap_err()
            .to_string()
            .contains("session interval count exceeds its encoded byte length"));
    }
    let mut truncated = encoded.clone();
    truncated.pop();
    assert!(decode_session_index(&truncated)
        .unwrap_err()
        .to_string()
        .contains("session interval count exceeds its encoded byte length"));
    assert!(decode_session_index(&encode_session_index(&[]))
        .unwrap()
        .is_empty());
}

#[test]
fn session_event_counts_are_bounded_by_remaining_payload_before_allocating() {
    let state = AccumulatorState {
        row_count: 1,
        accumulators: vec![],
    };
    let events = [SessionEvent {
        timestamp: 7,
        values: vec![],
    }];
    let encoded = encode_session_state(b"group", &state, &events);
    let (group, decoded, decoded_events) = decode_session_state(&encoded, &[]).unwrap();
    assert_eq!(group, b"group");
    assert_eq!(decoded.row_count, 1);
    assert_eq!(decoded_events.len(), 1);
    assert_eq!(decoded_events[0].timestamp, 7);
    for count in [2, u32::MAX] {
        let mut corrupt = encoded.clone();
        corrupt[13..17].copy_from_slice(&count.to_le_bytes());
        assert!(decode_session_state(&corrupt, &[])
            .unwrap_err()
            .to_string()
            .contains("session event count exceeds its encoded byte length"));
    }
    let mut truncated = encoded.clone();
    truncated.pop();
    assert!(decode_session_state(&truncated, &[])
        .unwrap_err()
        .to_string()
        .contains("session event count exceeds its encoded byte length"));
    assert!(
        decode_session_state(&encode_session_state(b"group", &state, &[]), &[])
            .unwrap()
            .2
            .is_empty()
    );
}
