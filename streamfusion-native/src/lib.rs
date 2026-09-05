// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use datafusion::error::{DataFusionError, Result};
use prost::Message;
use std::collections::HashSet;

pub mod exchange;
mod execution_context;
mod jni_bridge;
mod memory_pool;
pub mod planner;
mod state;

pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/streamfusion.plan.v1.rs"));
}

pub const PLAN_PROTOCOL_VERSION: u32 = 1;

pub fn decode_plan(bytes: &[u8]) -> Result<proto::NativePlan> {
    let mut plan = proto::NativePlan::decode(bytes)
        .map_err(|error| DataFusionError::Plan(format!("invalid StreamFusion plan: {error}")))?;
    if plan.protocol_version != PLAN_PROTOCOL_VERSION {
        return Err(DataFusionError::Plan(format!(
            "unsupported StreamFusion plan protocol version {}, expected {}",
            plan.protocol_version, PLAN_PROTOCOL_VERSION
        )));
    }
    if let Some(root) = plan.root.as_mut() {
        let mut next_id = 1;
        let mut assigned = HashSet::new();
        assign_plan_node_ids(root, &mut next_id, &mut assigned)?;
    }
    Ok(plan)
}

fn assign_plan_node_ids(
    operator: &mut proto::Operator,
    next_id: &mut u64,
    assigned: &mut HashSet<u64>,
) -> Result<()> {
    if operator.plan_node_id == 0 {
        while assigned.contains(next_id) {
            *next_id = next_id.checked_add(1).ok_or_else(|| {
                DataFusionError::Plan("native plan-node identity overflowed u64".to_string())
            })?;
        }
        operator.plan_node_id = *next_id;
    }
    if !assigned.insert(operator.plan_node_id) {
        return Err(DataFusionError::Plan(format!(
            "duplicate native plan-node identity {}",
            operator.plan_node_id
        )));
    }
    *next_id = (*next_id)
        .max(operator.plan_node_id)
        .checked_add(1)
        .ok_or_else(|| {
            DataFusionError::Plan("native plan-node identity overflowed u64".to_string())
        })?;

    use proto::operator::Operator::*;
    match operator.operator.as_mut() {
        Some(BoundedSort(node)) => assign_child(&mut node.input, next_id, assigned),
        Some(TemporalSort(node)) => assign_child(&mut node.input, next_id, assigned),
        Some(OverAggregate(node)) => assign_child(&mut node.input, next_id, assigned),
        Some(TopN(node)) => assign_child(&mut node.input, next_id, assigned),
        Some(Deduplicate(node)) => assign_child(&mut node.input, next_id, assigned),
        Some(ChangelogNormalize(node)) => assign_child(&mut node.input, next_id, assigned),
        Some(GroupAggregate(node)) => assign_child(&mut node.input, next_id, assigned),
        Some(LocalGroupAggregate(node)) => assign_child(&mut node.input, next_id, assigned),
        Some(GlobalGroupAggregate(node)) => assign_child(&mut node.input, next_id, assigned),
        Some(IncrementalGroupAggregate(node)) => assign_child(&mut node.input, next_id, assigned),
        Some(WindowAggregate(node)) => assign_child(&mut node.input, next_id, assigned),
        Some(WindowDeduplicate(node)) => assign_child(&mut node.input, next_id, assigned),
        Some(WindowRank(node)) => assign_child(&mut node.input, next_id, assigned),
        Some(WindowTableFunction(node)) => assign_child(&mut node.input, next_id, assigned),
        Some(Expand(node)) => assign_child(&mut node.input, next_id, assigned),
        Some(Calc(node)) => assign_child(&mut node.input, next_id, assigned),
        Some(ArrayUnnest(node)) => assign_child(&mut node.input, next_id, assigned),
        Some(ReplicateRows(node)) => assign_child(&mut node.input, next_id, assigned),
        Some(Union(node)) => {
            for input in &mut node.inputs {
                assign_plan_node_ids(input, next_id, assigned)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn assign_child(
    child: &mut Option<Box<proto::Operator>>,
    next_id: &mut u64,
    assigned: &mut HashSet<u64>,
) -> Result<()> {
    match child.as_deref_mut() {
        Some(child) => assign_plan_node_ids(child, next_id, assigned),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::planner::{create_plan, create_plan_with_inputs};
    use arrow::array::{Array, Decimal128Array, Int32Array, Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;

    fn identity_plan() -> proto::NativePlan {
        let integer_type = proto::LogicalType {
            nullable: false,
            r#type: Some(proto::logical_type::Type::Integer(proto::EmptyType {})),
        };
        proto::NativePlan {
            protocol_version: PLAN_PROTOCOL_VERSION,
            root: Some(proto::Operator {
                plan_node_id: 0,
                operator: Some(proto::operator::Operator::Calc(Box::new(proto::Calc {
                    input: Some(Box::new(proto::Operator {
                        plan_node_id: 0,
                        operator: Some(proto::operator::Operator::Input(proto::Input {
                            schema: Some(proto::Schema {
                                fields: vec![proto::Field {
                                    name: "id".to_string(),
                                    r#type: Some(integer_type.clone()),
                                }],
                            }),
                            input_index: 0,
                        })),
                    })),
                    projections: vec![proto::Expression {
                        expression: Some(proto::expression::Expression::InputReference(
                            proto::InputReference {
                                index: 0,
                                r#type: Some(integer_type),
                            },
                        )),
                    }],
                    condition: None,
                }))),
            }),
        }
    }

    fn chained_identity_plan() -> proto::NativePlan {
        let mut plan = identity_plan();
        let inner = plan.root.take().unwrap();
        let integer_type = proto::LogicalType {
            nullable: false,
            r#type: Some(proto::logical_type::Type::Integer(proto::EmptyType {})),
        };
        plan.root = Some(proto::Operator {
            plan_node_id: 0,
            operator: Some(proto::operator::Operator::Calc(Box::new(proto::Calc {
                input: Some(Box::new(inner)),
                projections: vec![proto::Expression {
                    expression: Some(proto::expression::Expression::InputReference(
                        proto::InputReference {
                            index: 0,
                            r#type: Some(integer_type),
                        },
                    )),
                }],
                condition: None,
            }))),
        });
        plan
    }

    fn union_plan(input_count: u32) -> proto::NativePlan {
        proto::NativePlan {
            protocol_version: PLAN_PROTOCOL_VERSION,
            root: Some(proto::Operator {
                plan_node_id: 0,
                operator: Some(proto::operator::Operator::Union(proto::Union {
                    inputs: (0..input_count)
                        .map(|input_index| proto::Operator {
                            plan_node_id: 0,
                            operator: Some(proto::operator::Operator::Input(proto::Input {
                                schema: None,
                                input_index,
                            })),
                        })
                        .collect(),
                })),
            }),
        }
    }

    #[test]
    fn assigns_stable_ids_to_legacy_plan_nodes() {
        let decoded = decode_plan(&chained_identity_plan().encode_to_vec()).unwrap();
        let root = decoded.root.unwrap();
        let root_id = root.plan_node_id;
        let proto::operator::Operator::Calc(outer) = root.operator.unwrap() else {
            panic!("expected outer calc");
        };
        let inner = outer.input.unwrap();
        let inner_id = inner.plan_node_id;
        let proto::operator::Operator::Calc(inner_calc) = inner.operator.unwrap() else {
            panic!("expected inner calc");
        };

        assert_eq!(root_id, 1);
        assert_eq!(inner_id, 2);
        assert_eq!(inner_calc.input.unwrap().plan_node_id, 3);
    }

    #[test]
    fn rejects_duplicate_explicit_plan_node_ids() {
        let mut plan = chained_identity_plan();
        let root = plan.root.as_mut().unwrap();
        root.plan_node_id = 7;
        let proto::operator::Operator::Calc(outer) = root.operator.as_mut().unwrap() else {
            panic!("expected outer calc");
        };
        outer.input.as_mut().unwrap().plan_node_id = 7;

        let error = decode_plan(&plan.encode_to_vec()).unwrap_err();

        assert!(error
            .to_string()
            .contains("duplicate native plan-node identity 7"));
    }

    fn q1_decimal_plan() -> proto::NativePlan {
        let bigint_type = proto::LogicalType {
            nullable: false,
            r#type: Some(proto::logical_type::Type::Bigint(proto::EmptyType {})),
        };
        let result_type = proto::LogicalType {
            nullable: false,
            r#type: Some(proto::logical_type::Type::Decimal(proto::DecimalType {
                precision: 23,
                scale: 3,
            })),
        };
        let reference = proto::Expression {
            expression: Some(proto::expression::Expression::InputReference(
                proto::InputReference {
                    index: 0,
                    r#type: Some(bigint_type.clone()),
                },
            )),
        };
        let factor = proto::Expression {
            expression: Some(proto::expression::Expression::DecimalLiteral(
                proto::DecimalLiteral {
                    unscaled_value: "908".to_string(),
                    precision: 4,
                    scale: 3,
                },
            )),
        };
        proto::NativePlan {
            protocol_version: PLAN_PROTOCOL_VERSION,
            root: Some(proto::Operator {
                plan_node_id: 0,
                operator: Some(proto::operator::Operator::Calc(Box::new(proto::Calc {
                    input: Some(Box::new(proto::Operator {
                        plan_node_id: 0,
                        operator: Some(proto::operator::Operator::Input(proto::Input {
                            schema: Some(proto::Schema {
                                fields: vec![proto::Field {
                                    name: "price".to_string(),
                                    r#type: Some(bigint_type),
                                }],
                            }),
                            input_index: 0,
                        })),
                    })),
                    projections: vec![proto::Expression {
                        expression: Some(proto::expression::Expression::Arithmetic(Box::new(
                            proto::Arithmetic {
                                left: Some(Box::new(factor)),
                                right: Some(Box::new(reference)),
                                operator: proto::ArithmeticOperator::Multiply.into(),
                                result_type: Some(result_type),
                            },
                        ))),
                    }],
                    condition: None,
                }))),
            }),
        }
    }

    #[tokio::test]
    async fn identity_calc_runs_as_datafusion_projection_without_copying_values() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let values = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let input_pointer = values.values().as_ptr();
        let batch = RecordBatch::try_new(schema.clone(), vec![values]).unwrap();
        let source = MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap();
        let plan = create_plan(&identity_plan().encode_to_vec(), source).unwrap();

        let output = collect(plan, SessionContext::new().task_ctx())
            .await
            .unwrap();
        let output_values = output[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        assert_eq!(output_values.values(), &[1, 2, 3]);
        assert_eq!(output_values.values().as_ptr(), input_pointer);
    }

    #[tokio::test]
    async fn adjacent_identity_calcs_share_the_input_arrow_buffer() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let values = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let input_pointer = values.values().as_ptr();
        let batch = RecordBatch::try_new(schema.clone(), vec![values]).unwrap();
        let source = MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap();
        let plan = create_plan(&chained_identity_plan().encode_to_vec(), source).unwrap();

        let output = collect(plan, SessionContext::new().task_ctx())
            .await
            .unwrap();
        let output_values = output[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        assert_eq!(output_values.values(), &[1, 2, 3]);
        assert_eq!(output_values.values().as_ptr(), input_pointer);
    }

    #[tokio::test]
    async fn q1_decimal_multiplication_uses_flinks_bigint_precision() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "price",
            DataType::Int64,
            false,
        )]));
        let prices = Int64Array::from(vec![-100, 0, 100, i64::MAX]);
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(prices)]).unwrap();
        let source = MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap();
        let plan = create_plan(&q1_decimal_plan().encode_to_vec(), source).unwrap();

        let output = collect(plan, SessionContext::new().task_ctx())
            .await
            .unwrap();
        let values = output[0]
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();

        assert_eq!(values.data_type(), &DataType::Decimal128(23, 3));
        assert_eq!(
            values.values(),
            &[-90_800, 0, 90_800, i128::from(i64::MAX) * 908]
        );
    }

    #[tokio::test]
    async fn union_all_preserves_each_inputs_arrow_buffer() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let left_values = Arc::new(Int32Array::from(vec![1, 2]));
        let right_values = Arc::new(Int32Array::from(vec![3, 4]));
        let left_pointer = left_values.values().as_ptr();
        let right_pointer = right_values.values().as_ptr();
        let left_batch = RecordBatch::try_new(schema.clone(), vec![left_values]).unwrap();
        let right_batch = RecordBatch::try_new(schema.clone(), vec![right_values]).unwrap();
        let left =
            MemorySourceConfig::try_new_exec(&[vec![left_batch]], schema.clone(), None).unwrap();
        let right = MemorySourceConfig::try_new_exec(&[vec![right_batch]], schema, None).unwrap();

        let plan =
            create_plan_with_inputs(&union_plan(2).encode_to_vec(), vec![left, right]).unwrap();
        let output = collect(plan, SessionContext::new().task_ctx())
            .await
            .unwrap();

        assert_eq!(output.len(), 2);
        let output_left = output[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let output_right = output[1]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(output_left.values(), &[1, 2]);
        assert_eq!(output_right.values(), &[3, 4]);
        assert_eq!(output_left.values().as_ptr(), left_pointer);
        assert_eq!(output_right.values().as_ptr(), right_pointer);
    }

    #[test]
    fn rejects_union_input_outside_external_inputs() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let source = MemorySourceConfig::try_new_exec(&[vec![]], schema, None).unwrap();

        let error =
            create_plan_with_inputs(&union_plan(2).encode_to_vec(), vec![source]).unwrap_err();

        assert!(error
            .to_string()
            .contains("input index 1 is outside 1 external inputs"));
    }

    #[test]
    fn rejects_unknown_protocol_version() {
        let mut plan = identity_plan();
        plan.protocol_version = PLAN_PROTOCOL_VERSION + 1;

        let error = decode_plan(&plan.encode_to_vec()).unwrap_err();

        assert!(error
            .to_string()
            .contains("unsupported StreamFusion plan protocol version"));
    }
}
