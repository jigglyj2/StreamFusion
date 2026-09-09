// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

#[test]
fn arrow_member_keys_preserve_scalar_comparison_and_frame_partition_identity() {
    let fixtures = vec![
        (
            DataType::Int64,
            vec![
                AggregateValue::Int(i64::MIN as i128),
                AggregateValue::Int(0),
                AggregateValue::Int(i64::MAX as i128),
            ],
        ),
        (
            DataType::Utf8,
            ["", "a", "a\0", "\0", "é", "😀"]
                .map(|value| AggregateValue::Bytes(value.as_bytes().to_vec()))
                .to_vec(),
        ),
        (
            DataType::Boolean,
            vec![
                AggregateValue::Boolean(false),
                AggregateValue::Boolean(true),
            ],
        ),
        (
            DataType::Decimal128(38, 4),
            vec![
                AggregateValue::Int(-10i128.pow(30)),
                AggregateValue::Int(0),
                AggregateValue::Int(10i128.pow(30)),
            ],
        ),
        (
            DataType::Date32,
            vec![
                AggregateValue::Int(-100),
                AggregateValue::Int(0),
                AggregateValue::Int(100),
            ],
        ),
    ];
    for (data_type, values) in fixtures {
        let call = Call {
            function: proto::AggregateFunction::Count,
            input_index: Some(1),
            filter_index: None,
            distinct: true,
            input_type: Some(data_type.clone()),
            output_type: DataType::Int64,
            retractable: true,
        };
        assert!(MembershipLayout::eligible(std::slice::from_ref(&call)));
        let layout = MembershipLayout::new(&[call], false).unwrap();
        let input = aggregate_array(
            &values.iter().cloned().map(Some).collect::<Vec<_>>(),
            &data_type,
        )
        .unwrap();
        let rows = layout.columns[0]
            .converter
            .convert_columns(&[input])
            .unwrap();
        let partition = StateKey {
            key_group: 37,
            key: vec![0, 7],
        };
        let prefix = prefix(&partition).unwrap();
        let mut encoded = values
            .iter()
            .enumerate()
            .map(|(index, value)| {
                let key = member_key(&prefix, 1, rows.row(index).as_ref(), partition.key_group);
                assert_eq!(key.key_group, 37);
                (key.key, value.clone())
            })
            .collect::<Vec<_>>();
        encoded.sort_by(|a, b| a.0.cmp(&b.0));
        let mut expected = values;
        expected.sort();
        assert_eq!(
            encoded
                .into_iter()
                .map(|(_, value)| value)
                .collect::<Vec<_>>(),
            expected
        );
    }
    let a = prefix(&StateKey {
        key_group: 0,
        key: vec![0, 1],
    })
    .unwrap();
    let b = prefix(&StateKey {
        key_group: 0,
        key: vec![0, 1, 0],
    })
    .unwrap();
    assert!(!a.starts_with(&b) && !b.starts_with(&a));
    let floating = Call {
        function: proto::AggregateFunction::Count,
        input_index: Some(1),
        filter_index: None,
        distinct: true,
        input_type: Some(DataType::Float64),
        output_type: DataType::Int64,
        retractable: true,
    };
    assert!(!MembershipLayout::eligible(&[floating])); // preserve Flink NaN/zero peers via existing inline adapter
}
