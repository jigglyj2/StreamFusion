// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::ExecutionPlan;

use crate::{decode_plan, proto};

mod expressions;
pub(crate) mod operators;
pub(crate) mod persistent;
pub(crate) mod region;
pub(crate) mod schema_memory;

pub(crate) fn arrow_schema(schema: &proto::Schema) -> Result<arrow::datatypes::SchemaRef> {
    let fields = schema
        .fields
        .iter()
        .map(|field| {
            let logical_type = field.r#type.as_ref().ok_or_else(|| {
                DataFusionError::Plan(format!("field {} type is missing", field.name))
            })?;
            Ok(arrow::datatypes::Field::new(
                &field.name,
                expressions::null_literal::data_type(logical_type)?,
                logical_type.nullable,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(arrow::datatypes::Schema::new(fields)))
}

pub fn create_plan(bytes: &[u8], input: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    create_plan_with_inputs(bytes, vec![input])
}

pub fn create_plan_with_inputs(
    bytes: &[u8],
    inputs: Vec<Arc<dyn ExecutionPlan>>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let plan = decode_plan(bytes)?;
    create_plan_from_decoded(&plan, inputs)
}

pub(crate) fn create_plan_from_decoded(
    plan: &proto::NativePlan,
    inputs: Vec<Arc<dyn ExecutionPlan>>,
) -> Result<Arc<dyn ExecutionPlan>> {
    create_plan_with_persistent_nodes(plan, inputs, &[])
}

/// Persistent processors are created by the Flink lifecycle owner, then bound to their stable
/// protobuf identities during physical lowering. Their native children remain part of the tree.
pub(crate) fn create_plan_with_persistent_nodes(
    plan: &proto::NativePlan,
    inputs: Vec<Arc<dyn ExecutionPlan>>,
    persistent: &[persistent::PersistentBinding],
) -> Result<Arc<dyn ExecutionPlan>> {
    create_plan_with_memory(plan, inputs, persistent, None)
}

pub(crate) fn create_plan_with_memory(
    plan: &proto::NativePlan,
    inputs: Vec<Arc<dyn ExecutionPlan>>,
    persistent: &[persistent::PersistentBinding],
    memory: Option<Arc<dyn datafusion::execution::memory_pool::MemoryPool>>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let resources = LoweringResources { persistent, memory };
    for (index, (id, _)) in persistent.iter().enumerate() {
        if *id == 0 || persistent[..index].iter().any(|(other, _)| other == id) {
            return Err(DataFusionError::Plan(
                "persistent native bindings need unique nonzero plan-node identities".into(),
            ));
        }
    }
    let physical = create_operator(
        plan.root
            .as_ref()
            .ok_or_else(|| DataFusionError::Plan("StreamFusion plan has no root".to_string()))?,
        &inputs,
        &resources,
    )?;
    if !persistent.is_empty() {
        fn visit(plan: &Arc<dyn ExecutionPlan>, ids: &mut Vec<u64>) -> Result<()> {
            if let Some(stage) = plan.downcast_ref::<operators::identified::IdentifiedExec>() {
                let id = stage.plan_node_id();
                if id == 0 || ids.contains(&id) {
                    return Err(DataFusionError::Plan(
                        "persistent native trees require unique nonzero stage identities".into(),
                    ));
                }
                ids.push(id);
            }
            for child in plan.children() {
                visit(child, ids)?;
            }
            Ok(())
        }
        let mut bindings = Vec::new();
        visit(&physical, &mut bindings)?;
        if persistent.iter().any(|(id, _)| !bindings.contains(id)) {
            return Err(DataFusionError::Plan(
                "unused or mismatched persistent native binding".into(),
            ));
        }
    }
    Ok(physical)
}

struct LoweringResources<'a> {
    persistent: &'a [persistent::PersistentBinding],
    memory: Option<Arc<dyn datafusion::execution::memory_pool::MemoryPool>>,
}

fn create_operator(
    operator: &proto::Operator,
    external_inputs: &[Arc<dyn ExecutionPlan>],
    resources: &LoweringResources<'_>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let persistent = resources.persistent;
    if let Some((_, factory)) = persistent
        .iter()
        .find(|(id, _)| *id == operator.plan_node_id)
    {
        let children = persistent::children(operator)?
            .into_iter()
            .map(|child| create_operator(child, external_inputs, resources))
            .collect::<Result<Vec<_>>>()?;
        if !factory.supports_owned_envelope()
            && children.iter().any(|child| {
                child
                    .schema()
                    .fields()
                    .iter()
                    .any(|field| field.name() == operators::envelope::OWNED_TIMESTAMP_V1)
            })
        {
            return Err(DataFusionError::Plan(format!(
                "native stage {} has no migrated owned-record envelope binding",
                operator.plan_node_id
            )));
        }
        let plan = factory.build(operator, children)?;
        if plan
            .schema()
            .fields()
            .iter()
            .any(|field| field.name().starts_with("__streamfusion_processing_time_"))
        {
            return Err(DataFusionError::Plan(format!(
                "native clock consumer {} must consume clock metadata before output",
                operator.plan_node_id
            )));
        }
        return identify_stage(operator, plan, resources);
    }
    let plan = match operator.operator.as_ref() {
        Some(proto::operator::Operator::Input(input)) => {
            operators::input::create(input, external_inputs)
        }
        Some(proto::operator::Operator::Calc(calc)) => {
            let child = create_operator(
                calc.input
                    .as_ref()
                    .ok_or_else(|| DataFusionError::Plan("calc has no input".to_string()))?,
                external_inputs,
                resources,
            )?;
            operators::calc::create_with_memory(calc, child, resources.memory.clone())
        }
        Some(proto::operator::Operator::ArrayUnnest(unnest)) => {
            let child = create_operator(
                unnest.input.as_ref().ok_or_else(|| {
                    DataFusionError::Plan("array unnest has no input".to_string())
                })?,
                external_inputs,
                resources,
            )?;
            operators::array_unnest::create(unnest, child)
        }
        Some(proto::operator::Operator::ReplicateRows(replicate)) => {
            let child = create_operator(
                replicate.input.as_ref().ok_or_else(|| {
                    DataFusionError::Plan("replicate rows has no input".to_string())
                })?,
                external_inputs,
                resources,
            )?;
            operators::replicate_rows::create(replicate, child)
        }
        Some(proto::operator::Operator::Union(union)) => {
            let children = union
                .inputs
                .iter()
                .map(|input| create_operator(input, external_inputs, resources))
                .collect::<Result<Vec<_>>>()?;
            operators::union::create(union, children)
        }
        Some(proto::operator::Operator::Expand(expand)) => {
            let child = create_operator(
                expand
                    .input
                    .as_ref()
                    .ok_or_else(|| DataFusionError::Plan("expand has no input".to_string()))?,
                external_inputs,
                resources,
            )?;
            operators::expand::create_with_memory(expand, child, resources.memory.clone())
        }
        Some(proto::operator::Operator::Values(values)) => operators::values::create(values),
        Some(proto::operator::Operator::WindowTableFunction(window)) => {
            let child = create_operator(
                window.input.as_ref().ok_or_else(|| {
                    DataFusionError::Plan("window table function has no input".to_string())
                })?,
                external_inputs,
                resources,
            )?;
            operators::window_table_function::create(window, child)
        }
        Some(proto::operator::Operator::Deduplicate(_)) => Err(DataFusionError::Plan(
            "Deduplicate requires a persistent stateful execution handle".into(),
        )),
        Some(proto::operator::Operator::GroupAggregate(_)) => Err(DataFusionError::Plan(
            "GroupAggregate requires a persistent stateful execution handle".to_string(),
        )),
        Some(proto::operator::Operator::LocalGroupAggregate(_)) => Err(DataFusionError::Plan(
            "LocalGroupAggregate requires a persistent bundle execution handle".to_string(),
        )),
        Some(proto::operator::Operator::GlobalGroupAggregate(_)) => Err(DataFusionError::Plan(
            "GlobalGroupAggregate requires a persistent stateful execution handle".to_string(),
        )),
        Some(proto::operator::Operator::IncrementalGroupAggregate(_)) => {
            Err(DataFusionError::Plan(
                "IncrementalGroupAggregate requires a persistent bundle execution handle"
                    .to_string(),
            ))
        }
        Some(proto::operator::Operator::WindowAggregate(_)) => Err(DataFusionError::Plan(
            "WindowAggregate requires a persistent stateful execution handle".to_string(),
        )),
        Some(proto::operator::Operator::LocalWindowAggregate(_)) => Err(DataFusionError::Plan(
            "LocalWindowAggregate requires a persistent bundle execution handle".to_string(),
        )),
        Some(proto::operator::Operator::WindowDeduplicate(_)) => Err(DataFusionError::Plan(
            "WindowDeduplicate requires a persistent stateful execution handle".to_string(),
        )),
        Some(proto::operator::Operator::WindowRank(_)) => Err(DataFusionError::Plan(
            "WindowRank requires a persistent stateful execution handle".to_string(),
        )),
        Some(proto::operator::Operator::TopN(_)) => Err(DataFusionError::Plan(
            "TopN requires a persistent stateful execution handle".to_string(),
        )),
        Some(proto::operator::Operator::WindowJoin(_)) => Err(DataFusionError::Plan(
            "WindowJoin requires a persistent stateful execution handle".to_string(),
        )),
        Some(proto::operator::Operator::LookupJoin(_)) => Err(DataFusionError::Plan(
            "LookupJoin requires a task-open Arrow snapshot binding".into(),
        )),
        Some(proto::operator::Operator::RegularJoin(_)) => Err(DataFusionError::Plan(
            "RegularJoin requires a persistent stateful execution handle".into(),
        )),
        Some(proto::operator::Operator::IntervalJoin(_)) => Err(DataFusionError::Plan(
            "IntervalJoin requires a persistent stateful execution handle".to_string(),
        )),
        Some(proto::operator::Operator::TemporalJoin(_)) => Err(DataFusionError::Plan(
            "TemporalJoin requires a persistent stateful execution handle".to_string(),
        )),
        Some(proto::operator::Operator::MultiJoin(_)) => Err(DataFusionError::Plan(
            "MultiJoin requires a persistent stateful execution handle".to_string(),
        )),
        Some(proto::operator::Operator::OverAggregate(_)) => Err(DataFusionError::Plan(
            "OverAggregate requires a persistent stateful execution handle".to_string(),
        )),
        Some(proto::operator::Operator::ChangelogNormalize(_)) => Err(DataFusionError::Plan(
            "ChangelogNormalize requires a persistent stateful execution handle".to_string(),
        )),
        Some(proto::operator::Operator::TemporalSort(_)) => Err(DataFusionError::Plan(
            "TemporalSort requires a persistent stateful execution handle".to_string(),
        )),
        Some(proto::operator::Operator::MatchRecognize(_)) => Err(DataFusionError::Plan(
            "MatchRecognize requires a persistent stateful execution handle".to_string(),
        )),
        Some(proto::operator::Operator::BoundedSort(_)) => Err(DataFusionError::Plan(
            "BoundedSort requires a persistent stateful execution handle".to_string(),
        )),
        Some(proto::operator::Operator::BoundedRank(_)) => Err(DataFusionError::Plan(
            "BoundedRank requires a persistent execution handle".to_string(),
        )),
        None => Err(DataFusionError::Plan(
            "StreamFusion operator is empty".to_string(),
        )),
    }?;
    identify_stage(operator, plan, resources)
}

fn identify_stage(
    operator: &proto::Operator,
    plan: Arc<dyn ExecutionPlan>,
    resources: &LoweringResources<'_>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let plan = if operator.clear_record_timestamps {
        operators::record_policy::clear_timestamps(plan, resources.memory.as_ref())?
    } else {
        plan
    };
    Ok(operators::identified::IdentifiedExec::wrap(
        operator.plan_node_id,
        operators::local_partitions::single_partition(plan),
    ))
}
