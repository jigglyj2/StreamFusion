// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;
use arrow::array::{BooleanArray, Int64Array};
use prost::Message;

pub(crate) fn comparison_plan() -> proto::WindowJoin {
    let integer = proto::LogicalType {
        nullable: true,
        r#type: Some(proto::logical_type::Type::Bigint(proto::EmptyType {})),
    };
    let schema = proto::Schema {
        fields: vec![
            proto::Field {
                name: "value".into(),
                r#type: Some(integer.clone()),
            },
            proto::Field {
                name: "ordinal".into(),
                r#type: Some(proto::LogicalType {
                    nullable: false,
                    ..integer.clone()
                }),
            },
        ],
    };
    let reference = |index| proto::Expression {
        expression: Some(proto::expression::Expression::InputReference(
            proto::InputReference {
                index,
                r#type: Some(integer.clone()),
            },
        )),
    };
    let input = |index| {
        Box::new(proto::Operator {
            operator: Some(proto::operator::Operator::Input(proto::Input {
                input_index: index,
                ..Default::default()
            })),
            ..Default::default()
        })
    };
    proto::WindowJoin {
        left_input: Some(input(0)),
        right_input: Some(input(1)),
        left_schema: Some(schema.clone()),
        right_schema: Some(schema),
        left_window_end_index: 1,
        right_window_end_index: 1,
        join_type: proto::RegularJoinType::Inner as i32,
        shift_time_zone: "UTC".into(),
        residual_condition: Some(proto::Expression {
            expression: Some(proto::expression::Expression::Comparison(Box::new(
                proto::Comparison {
                    left: Some(Box::new(reference(0))),
                    right: Some(Box::new(reference(2))),
                    operator: proto::ComparisonOperator::GreaterThanOrEqual as i32,
                },
            ))),
        }),
        ..Default::default()
    }
}

#[test]
fn compact_predicate_reads_only_referenced_columns_and_preserves_null_semantics() {
    let plan = comparison_plan();
    let filter = filter(&plan).unwrap().unwrap();
    assert_eq!(filter.schema().fields().len(), 2);
    assert_eq!(
        filter
            .column_indices()
            .iter()
            .map(|c| c.index)
            .collect::<Vec<_>>(),
        [0, 0]
    );
    let left = [Some(-1), None, Some(7), Some(7), Some(9)];
    let right = [Some(0), Some(2), None, Some(7), Some(8)];
    let batch = RecordBatch::try_new(
        filter.schema().clone(),
        vec![
            Arc::new(Int64Array::from(left.to_vec())),
            Arc::new(Int64Array::from(right.to_vec())),
        ],
    )
    .unwrap();
    let result = filter
        .expression()
        .evaluate(&batch)
        .unwrap()
        .into_array(5)
        .unwrap();
    assert_eq!(
        result
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        left.into_iter()
            .zip(right)
            .map(|(a, b)| a.zip(b).map(|(a, b)| a >= b))
            .collect::<Vec<_>>()
    );
}

#[test]
fn key_null_filter_is_explicit_and_null_safe_keys_do_not_add_a_filter() {
    let mut plan = comparison_plan();
    plan.residual_condition = None;
    plan.left_key_indices = vec![0];
    plan.right_key_indices = vec![0];
    plan.filter_nulls = vec![false];
    assert!(filter(&plan).unwrap().is_none());
    plan.filter_nulls = vec![true];
    let filter = filter(&plan).unwrap().unwrap();
    let batch = RecordBatch::try_new(
        filter.schema().clone(),
        vec![
            Arc::new(Int64Array::from(vec![None, None, Some(7), Some(7)])),
            Arc::new(Int64Array::from(vec![None, Some(7), None, Some(7)])),
        ],
    )
    .unwrap();
    let result = filter
        .expression()
        .evaluate(&batch)
        .unwrap()
        .into_array(4)
        .unwrap();
    assert_eq!(
        result
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(false), Some(false), Some(false), Some(true)]
    );
}

#[test]
fn native_child_identity_and_record_policy_require_version_three() {
    let mut plan = proto::NativePlan {
        protocol_version: 3,
        root: Some(proto::Operator {
            plan_node_id: 41,
            clear_record_timestamps: true,
            operator: Some(proto::operator::Operator::WindowJoin(Box::new(
                comparison_plan(),
            ))),
            ..Default::default()
        }),
    };
    let decoded = crate::decode_plan(&plan.encode_to_vec()).unwrap();
    let node = decoded.root.as_ref().unwrap();
    let children = crate::planner::persistent::children(node).unwrap();
    assert_eq!(node.plan_node_id, 41);
    assert_eq!(
        children.iter().map(|c| c.plan_node_id).collect::<Vec<_>>(),
        [1, 2]
    );
    let bare = crate::execution_context::operator_spec::without_children(node);
    assert!(crate::planner::persistent::children(&bare)
        .unwrap_err()
        .to_string()
        .contains("explicit physical child"));
    plan.protocol_version = 2;
    plan.root.as_mut().unwrap().clear_record_timestamps = false;
    assert!(crate::decode_plan(&plan.encode_to_vec())
        .unwrap_err()
        .to_string()
        .contains("window join requires plan protocol version 3"));
}

#[test]
fn unsupported_native_semantics_are_rejected_instead_of_using_legacy_defaults() {
    let mut plan = comparison_plan();
    plan.join_type = proto::RegularJoinType::Left as i32;
    assert!(filter(&plan).unwrap_err().to_string().contains("INNER"));
    plan.join_type = 999;
    assert!(filter(&plan).is_err());
    plan = comparison_plan();
    plan.shift_time_zone = "Europe/Berlin".into();
    assert!(filter(&plan).unwrap_err().to_string().contains("UTC"));
    plan = comparison_plan();
    plan.filter_nulls.push(true);
    assert!(filter(&plan)
        .unwrap_err()
        .to_string()
        .contains("counts differ"));
    plan = comparison_plan();
    plan.left_window_end_index = 7;
    assert!(filter(&plan)
        .unwrap_err()
        .to_string()
        .contains("window-end index"));
    plan = comparison_plan();
    plan.left_key_indices = vec![8];
    plan.right_key_indices = vec![0];
    plan.filter_nulls = vec![true];
    assert!(filter(&plan)
        .unwrap_err()
        .to_string()
        .contains("left key outside"));
}

#[test]
fn null_key_rows_do_not_evaluate_a_throwing_residual() {
    let mut plan = comparison_plan();
    plan.left_key_indices = vec![0];
    plan.right_key_indices = vec![0];
    plan.filter_nulls = vec![true];
    let integer = plan.left_schema.as_ref().unwrap().fields[0].r#type.clone();
    let reference = |index| proto::Expression {
        expression: Some(proto::expression::Expression::InputReference(
            proto::InputReference {
                index,
                r#type: integer.clone(),
            },
        )),
    };
    let division = proto::Expression {
        expression: Some(proto::expression::Expression::Arithmetic(Box::new(
            proto::Arithmetic {
                left: Some(Box::new(reference(1))),
                right: Some(Box::new(reference(3))),
                operator: proto::ArithmeticOperator::Divide as i32,
                result_type: None,
            },
        ))),
    };
    plan.residual_condition = Some(proto::Expression {
        expression: Some(proto::expression::Expression::Comparison(Box::new(
            proto::Comparison {
                left: Some(Box::new(division)),
                right: Some(Box::new(reference(0))),
                operator: proto::ComparisonOperator::GreaterThan as i32,
            },
        ))),
    });
    let filter = filter(&plan).unwrap().unwrap();
    let columns = vec![
        Arc::new(Int64Array::from(vec![None, Some(7)])) as arrow::array::ArrayRef,
        Arc::new(Int64Array::from(vec![10, 10])),
        Arc::new(Int64Array::from(vec![7, 7])),
        Arc::new(Int64Array::from(vec![0, 1])),
    ];
    let batch = RecordBatch::try_new(filter.schema().clone(), columns).unwrap();
    let result = filter
        .expression()
        .evaluate(&batch)
        .unwrap()
        .into_array(2)
        .unwrap();
    assert_eq!(
        result
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        [Some(false), Some(true)]
    );
    // The same zero divisor on a matching key must retain Flink's exception, not suppress it.
    let mut columns = batch.columns().to_vec();
    columns[0] = Arc::new(Int64Array::from(vec![7, 7]));
    let matching = RecordBatch::try_new(filter.schema().clone(), columns).unwrap();
    assert!(filter.expression().evaluate(&matching).is_err());
}
