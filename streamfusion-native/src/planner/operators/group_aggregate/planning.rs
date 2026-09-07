// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::*;

pub(in crate::planner::operators) fn validate_plan(
    _plan: &proto::GroupAggregate,
    max_parallelism: u32,
) -> Result<()> {
    if max_parallelism == 0 {
        return Err(DataFusionError::Plan(
            "group aggregate max parallelism must be positive".to_string(),
        ));
    }
    validate_mini_batch(
        _plan.mini_batch_size,
        _plan.input_schema.as_ref(),
        _plan.output_schema.as_ref(),
    )
}

pub(super) fn validate_mini_batch(
    size: u64,
    input: Option<&proto::Schema>,
    output: Option<&proto::Schema>,
) -> Result<()> {
    if size > usize::MAX as u64 {
        return Err(DataFusionError::Plan(
            "group aggregate mini-batch size exceeds usize".to_string(),
        ));
    }
    if size > 0 && (input.is_none() || output.is_none()) {
        return Err(DataFusionError::Plan(
            "mini-batch group aggregate requires input and output schemas".to_string(),
        ));
    }
    Ok(())
}

pub(in crate::planner::operators) fn planned_group_output(
    plan: &proto::GroupAggregate,
) -> Result<(Option<RowConverter>, Option<SchemaRef>)> {
    let (Some(input), Some(output)) = (&plan.input_schema, &plan.output_schema) else {
        return Ok((None, None));
    };
    let input = crate::planner::arrow_schema(input)?;
    let output = crate::planner::arrow_schema(output)?;
    if output.fields().len() != plan.grouping_indices.len() + plan.aggregate_calls.len() {
        return Err(DataFusionError::Plan(
            "group aggregate output schema does not match keys and calls".to_string(),
        ));
    }
    let sort_fields = plan
        .grouping_indices
        .iter()
        .map(|&index| {
            input
                .fields()
                .get(index as usize)
                .map(|field| SortField::new(field.data_type().clone()))
                .ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "group aggregate grouping index {index} is outside the planned input"
                    ))
                })
        })
        .collect::<Result<Vec<_>>>()?;
    if !RowConverter::supports_fields(&sort_fields) {
        return Err(DataFusionError::Plan(
            "group aggregate key type is not supported by Arrow row encoding".to_string(),
        ));
    }
    let converter = RowConverter::new(sort_fields)?;
    let mut fields = output.fields().iter().cloned().collect::<Vec<_>>();
    fields.push(Arc::new(Field::new(
        "__streamfusion_row_kind",
        DataType::Int8,
        false,
    )));
    Ok((Some(converter), Some(Arc::new(Schema::new(fields)))))
}

pub(in crate::planner::operators) fn lower_call(call: &proto::AggregateCall) -> Result<Call> {
    let function = proto::AggregateFunction::try_from(call.function).map_err(|_| {
        DataFusionError::Plan(format!("unknown aggregate function {}", call.function))
    })?;
    if function == proto::AggregateFunction::Unspecified {
        return Err(DataFusionError::Plan(
            "aggregate function is unspecified".to_string(),
        ));
    }
    let input_index = call.input_index.map(|index| index as usize);
    let filter_index = call.filter_index.map(|index| index as usize);
    if function == proto::AggregateFunction::CountStar && input_index.is_some() {
        return Err(DataFusionError::Plan(
            "COUNT(*) must not name an input field".to_string(),
        ));
    }
    if function == proto::AggregateFunction::CountStar && call.distinct {
        return Err(DataFusionError::Plan(
            "COUNT(DISTINCT *) is not a valid aggregate call".to_string(),
        ));
    }
    if function != proto::AggregateFunction::CountStar && input_index.is_none() {
        return Err(DataFusionError::Plan(
            "aggregate call requires an input field".to_string(),
        ));
    }
    let input_type = call
        .input_type
        .as_ref()
        .map(null_literal::data_type)
        .transpose()?;
    let output_type =
        null_literal::data_type(call.output_type.as_ref().ok_or_else(|| {
            DataFusionError::Plan("aggregate output type is missing".to_string())
        })?)?;
    let accumulator_type = match call.accumulator_type.as_ref() {
        Some(data_type) => null_literal::data_type(data_type)?,
        None => output_type.clone(),
    };
    match function {
        proto::AggregateFunction::CountStar | proto::AggregateFunction::Count => {
            if output_type != DataType::Int64 {
                return Err(DataFusionError::Plan(format!(
                    "COUNT output must be BIGINT, got {output_type}"
                )));
            }
        }
        proto::AggregateFunction::Sum | proto::AggregateFunction::Sum0 => {
            ensure_sum_type(&output_type)?;
            ensure_sum_type(input_type.as_ref().expect("SUM input was validated"))?;
        }
        proto::AggregateFunction::Avg => {
            let input_type = input_type.as_ref().expect("AVG input was validated");
            ensure_average_types(input_type, &accumulator_type, &output_type)?;
        }
        proto::AggregateFunction::Min | proto::AggregateFunction::Max => {
            ensure_extremum_type(&output_type)?;
            let input_type = input_type.as_ref().expect("extremum input was validated");
            ensure_extremum_type(input_type)?;
            if input_type != &output_type {
                return Err(DataFusionError::Plan(format!(
                    "MIN/MAX input {input_type} does not match output {output_type}"
                )));
            }
        }
        proto::AggregateFunction::Unspecified => unreachable!("validated aggregate function"),
    }
    Ok(Call {
        function,
        input_index,
        filter_index,
        distinct: call.distinct,
        input_type,
        output_type,
        retractable: call.retractable,
    })
}

pub(in crate::planner::operators) fn ensure_average_types(
    input: &DataType,
    accumulator: &DataType,
    output: &DataType,
) -> Result<()> {
    let valid = match input {
        DataType::Int8 => accumulator == &DataType::Int64 && output == &DataType::Int8,
        DataType::Int16 => accumulator == &DataType::Int64 && output == &DataType::Int16,
        DataType::Int32 => accumulator == &DataType::Int64 && output == &DataType::Int32,
        DataType::Int64 => accumulator == &DataType::Int64 && output == &DataType::Int64,
        DataType::Float32 => accumulator == &DataType::Float64 && output == &DataType::Float32,
        DataType::Float64 => accumulator == &DataType::Float64 && output == &DataType::Float64,
        DataType::Decimal128(_, input_scale) => {
            matches!(accumulator, DataType::Decimal128(38, sum_scale) if sum_scale == input_scale)
                && matches!(output, DataType::Decimal128(38, output_scale) if output_scale >= input_scale)
        }
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(DataFusionError::Plan(format!(
            "group aggregate AVG requires a Flink-compatible input/buffer/output triple, got {input}/{accumulator}/{output}"
        )))
    }
}

pub(in crate::planner::operators) fn ensure_sum_type(data_type: &DataType) -> Result<()> {
    if matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
    ) {
        Ok(())
    } else {
        Err(DataFusionError::Plan(format!(
            "group aggregate SUM type {data_type} is not supported"
        )))
    }
}

pub(in crate::planner::operators) fn ensure_extremum_type(data_type: &DataType) -> Result<()> {
    if matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Boolean
            | DataType::Utf8
            | DataType::Date32
            | DataType::Time32(TimeUnit::Second | TimeUnit::Millisecond)
            | DataType::Time64(TimeUnit::Microsecond | TimeUnit::Nanosecond)
            | DataType::Timestamp(
                TimeUnit::Second
                    | TimeUnit::Millisecond
                    | TimeUnit::Microsecond
                    | TimeUnit::Nanosecond,
                _
            )
    ) {
        Ok(())
    } else {
        Err(DataFusionError::Plan(format!(
            "group aggregate MIN/MAX type {data_type} is not supported"
        )))
    }
}
