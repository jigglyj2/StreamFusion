// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use datafusion::error::{DataFusionError, Result};
use prost::Message;

mod jni_bridge;
pub mod planner;

pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/streamfusion.plan.v1.rs"));
}

pub const PLAN_PROTOCOL_VERSION: u32 = 1;

pub fn decode_plan(bytes: &[u8]) -> Result<proto::NativePlan> {
    let plan = proto::NativePlan::decode(bytes)
        .map_err(|error| DataFusionError::Plan(format!("invalid StreamFusion plan: {error}")))?;
    if plan.protocol_version != PLAN_PROTOCOL_VERSION {
        return Err(DataFusionError::Plan(format!(
            "unsupported StreamFusion plan protocol version {}, expected {}",
            plan.protocol_version, PLAN_PROTOCOL_VERSION
        )));
    }
    Ok(plan)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::planner::{create_plan, create_plan_with_inputs};
    use arrow::array::{Array, Int32Array, RecordBatch};
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
                operator: Some(proto::operator::Operator::Calc(Box::new(proto::Calc {
                    input: Some(Box::new(proto::Operator {
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
                operator: Some(proto::operator::Operator::Union(proto::Union {
                    inputs: (0..input_count)
                        .map(|input_index| proto::Operator {
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
