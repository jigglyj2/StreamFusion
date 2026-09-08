// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use super::*;

pub(super) fn validate_plan(plan: &proto::TopN, max_parallelism: u32) -> Result<()> {
    if max_parallelism == 0
        || max_parallelism > 32_768
        || plan.rank_start == 0
        || (plan.rank_end.is_some() == plan.variable_rank_end_index.is_some())
        || plan.input_schema.is_none()
        || plan.output_schema.is_none()
        || !matches!(
            proto::TopNStrategy::try_from(plan.strategy),
            Ok(proto::TopNStrategy::AppendFast)
                | Ok(proto::TopNStrategy::UpdateFast)
                | Ok(proto::TopNStrategy::Retract)
        )
        || plan.sort_key_indices.len() != plan.sort_ascending.len()
        || plan.sort_key_indices.len() != plan.sort_nulls_last.len()
    {
        return Err(DataFusionError::Plan(
            "top-n requires valid schemas, strategy, range, ordering, and max parallelism"
                .to_string(),
        ));
    }
    if let Some(end) = plan.rank_end {
        if end < plan.rank_start || end > i64::MAX as u64 {
            return Err(DataFusionError::Plan(
                "top-n rank end is outside its valid one-based range".to_string(),
            ));
        }
    }
    let rank_type = proto::TopNRankType::try_from(plan.rank_type)
        .map_err(|_| DataFusionError::Plan("top-n rank type is unknown".to_string()))?;
    if rank_type == proto::TopNRankType::Rank
        && (!plan.bounded_final_output
            || !plan.physical_input_semantics
            || plan.strategy != proto::TopNStrategy::AppendFast as i32
            || plan.rank_end.is_none()
            || plan.state_ttl_millis != 0)
    {
        return Err(DataFusionError::Plan(
            "SQL RANK selection requires bounded physical append-state with a constant range"
                .to_string(),
        ));
    }
    if plan.bounded_final_output && rank_type != proto::TopNRankType::Rank {
        return Err(DataFusionError::Plan(
            "bounded Top-N final output currently requires SQL RANK semantics".to_string(),
        ));
    }
    if plan.sort_key_indices.is_empty()
        && (!plan.partition_key_indices.is_empty()
            || plan.output_rank_number
            || plan.variable_rank_end_index.is_some())
    {
        return Err(DataFusionError::Plan(
            "unordered top-n is valid only for a global constant LIMIT/OFFSET".to_string(),
        ));
    }
    Ok(())
}

pub(super) fn validate_output_schema(
    plan: &proto::TopN,
    input: &SchemaRef,
    output: &SchemaRef,
) -> Result<()> {
    let expected = input.fields().len() + usize::from(plan.output_rank_number);
    if output.fields().len() != expected
        || output.fields()[..input.fields().len()] != input.fields()[..]
        || (plan.output_rank_number && output.field(expected - 1).data_type() != &DataType::Int64)
    {
        return Err(DataFusionError::Plan(
            "top-n output schema must be the input plus an optional BIGINT rank".to_string(),
        ));
    }
    for &index in plan
        .partition_key_indices
        .iter()
        .chain(&plan.sort_key_indices)
        .chain(&plan.primary_key_indices)
    {
        if index as usize >= input.fields().len() {
            return Err(DataFusionError::Plan(format!(
                "top-n field index {index} is outside the input"
            )));
        }
    }
    Ok(())
}

pub(super) fn metadata_index(schema: &SchemaRef, name: &str) -> Option<usize> {
    schema
        .fields()
        .iter()
        .position(|field| field.name() == name)
}

pub(super) fn fields_compatible(expected: &Field, actual: &Field, compare_name: bool) -> bool {
    (!compare_name || expected.name() == actual.name())
        && expected.is_nullable() == actual.is_nullable()
        && data_types_compatible(expected.data_type(), actual.data_type())
}

pub(super) fn data_types_compatible(expected: &DataType, actual: &DataType) -> bool {
    match (expected, actual) {
        (DataType::List(left), DataType::List(right))
        | (DataType::LargeList(left), DataType::LargeList(right)) => {
            fields_compatible(left, right, false)
        }
        (DataType::FixedSizeList(left, left_size), DataType::FixedSizeList(right, right_size)) => {
            left_size == right_size && fields_compatible(left, right, false)
        }
        (DataType::Struct(left), DataType::Struct(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .zip(right.iter())
                    .all(|(left, right)| fields_compatible(left, right, true))
        }
        (DataType::Map(left, left_sorted), DataType::Map(right, right_sorted)) => {
            left_sorted == right_sorted && fields_compatible(left, right, false)
        }
        _ => expected == actual,
    }
}
