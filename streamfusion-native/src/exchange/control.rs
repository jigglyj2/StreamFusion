// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use datafusion::error::{DataFusionError, Result};
use prost::Message;

use crate::exchange::KeyField;
use crate::proto;

pub fn decode_exchange_plan(bytes: &[u8]) -> Result<proto::NativeExchangePlan> {
    let plan = proto::NativeExchangePlan::decode(bytes).map_err(|error| {
        DataFusionError::Plan(format!("invalid StreamFusion exchange plan: {error}"))
    })?;
    validate(&plan)?;
    Ok(plan)
}

pub fn exchange_key_fields(plan: &proto::NativeExchangePlan) -> Result<Vec<(usize, KeyField)>> {
    let schema = plan
        .schema
        .as_ref()
        .ok_or_else(|| DataFusionError::Plan("exchange schema is required".to_string()))?;
    plan.key_indices
        .iter()
        .map(|index| {
            let logical_type = schema.fields[*index as usize]
                .r#type
                .as_ref()
                .and_then(|logical_type| logical_type.r#type.as_ref())
                .ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "exchange key {index} does not have a logical type"
                    ))
                })?;
            let key_field = match logical_type {
                proto::logical_type::Type::Boolean(_) => KeyField::Boolean,
                proto::logical_type::Type::Tinyint(_) => KeyField::TinyInt,
                proto::logical_type::Type::Smallint(_) => KeyField::SmallInt,
                proto::logical_type::Type::Integer(_) => KeyField::Integer,
                proto::logical_type::Type::Bigint(_) => KeyField::BigInt,
                proto::logical_type::Type::Float(_) => KeyField::Float,
                proto::logical_type::Type::Double(_) => KeyField::Double,
                proto::logical_type::Type::FixedChar(_) | proto::logical_type::Type::Varchar(_) => {
                    KeyField::String
                }
                proto::logical_type::Type::FixedBinary(_) | proto::logical_type::Type::Binary(_) => {
                    KeyField::Binary
                }
                proto::logical_type::Type::Date(_) => KeyField::Date,
                proto::logical_type::Type::Time(_) => KeyField::Time,
                proto::logical_type::Type::Timestamp(precision)
                | proto::logical_type::Type::TimestampLtz(precision) => KeyField::Timestamp {
                    precision: precision.precision as u8,
                },
                proto::logical_type::Type::Decimal(decimal) => KeyField::Decimal {
                    precision: decimal.precision as u8,
                },
                unsupported => {
                    return Err(DataFusionError::Plan(format!(
                        "exchange key {index} type {unsupported:?} has no exact Flink BinaryRow encoding"
                    )))
                }
            };
            Ok((*index as usize, key_field))
        })
        .collect()
}

fn validate(plan: &proto::NativeExchangePlan) -> Result<()> {
    if plan.protocol_version != crate::PLAN_PROTOCOL_VERSION {
        return Err(DataFusionError::Plan(format!(
            "unsupported StreamFusion exchange protocol version {}, expected {}",
            plan.protocol_version,
            crate::PLAN_PROTOCOL_VERSION
        )));
    }
    let distribution = proto::ExchangeDistribution::try_from(plan.distribution).map_err(|_| {
        DataFusionError::Plan(format!(
            "unknown exchange distribution {}",
            plan.distribution
        ))
    })?;
    let transport = proto::ExchangeTransport::try_from(plan.transport).map_err(|_| {
        DataFusionError::Plan(format!("unknown exchange transport {}", plan.transport))
    })?;
    if transport == proto::ExchangeTransport::Unspecified {
        return Err(DataFusionError::Plan(
            "exchange transport is required".to_string(),
        ));
    }
    let schema = plan
        .schema
        .as_ref()
        .ok_or_else(|| DataFusionError::Plan("exchange schema is required".to_string()))?;
    let metadata = plan.metadata_columns.as_ref().ok_or_else(|| {
        DataFusionError::Plan("exchange row-kind metadata column is required".to_string())
    })?;
    if metadata.row_kind_index as usize >= schema.fields.len() {
        return Err(DataFusionError::Plan(format!(
            "exchange row-kind index {} is outside the {}-field schema",
            metadata.row_kind_index,
            schema.fields.len()
        )));
    }
    let row_kind_logical_type = schema.fields[metadata.row_kind_index as usize]
        .r#type
        .as_ref();
    if row_kind_logical_type.is_none_or(|field| {
        field.nullable || !matches!(field.r#type, Some(proto::logical_type::Type::Tinyint(_)))
    }) {
        return Err(DataFusionError::Plan(
            "exchange row-kind metadata must be a non-null TINYINT".to_string(),
        ));
    }
    if let Some(timestamp_index) = metadata.stream_record_timestamp_index {
        if timestamp_index as usize >= schema.fields.len()
            || timestamp_index == metadata.row_kind_index
        {
            return Err(DataFusionError::Plan(format!(
                "exchange stream-record timestamp index {timestamp_index} is invalid"
            )));
        }
        let timestamp_logical_type = schema.fields[timestamp_index as usize].r#type.as_ref();
        if timestamp_logical_type.is_none_or(|field| {
            !field.nullable || !matches!(field.r#type, Some(proto::logical_type::Type::Bigint(_)))
        }) {
            return Err(DataFusionError::Plan(
                "exchange stream-record timestamp metadata must be a nullable BIGINT".to_string(),
            ));
        }
    }
    match distribution {
        proto::ExchangeDistribution::Hash => {
            if plan.key_indices.is_empty() {
                return Err(DataFusionError::Plan(
                    "hash exchange requires at least one key".to_string(),
                ));
            }
            if plan.max_parallelism == 0 || plan.max_parallelism > 32_768 {
                return Err(DataFusionError::Plan(format!(
                    "hash exchange max parallelism {} is outside Flink's range",
                    plan.max_parallelism
                )));
            }
            if plan.parallelism == 0 || plan.parallelism > plan.max_parallelism {
                return Err(DataFusionError::Plan(format!(
                    "hash exchange parallelism {} is outside 1..={}",
                    plan.parallelism, plan.max_parallelism
                )));
            }
            for index in &plan.key_indices {
                if *index as usize >= schema.fields.len() {
                    return Err(DataFusionError::Plan(format!(
                        "exchange key index {index} is outside the {}-field schema",
                        schema.fields.len()
                    )));
                }
                if *index == metadata.row_kind_index
                    || metadata.stream_record_timestamp_index == Some(*index)
                {
                    return Err(DataFusionError::Plan(format!(
                        "exchange key index {index} refers to a metadata column"
                    )));
                }
            }
        }
        proto::ExchangeDistribution::Singleton => {
            if !plan.key_indices.is_empty() {
                return Err(DataFusionError::Plan(
                    "singleton exchange must not define hash keys".to_string(),
                ));
            }
            if plan.max_parallelism != 1 || plan.parallelism != 1 {
                return Err(DataFusionError::Plan(
                    "singleton exchange parallelism and max parallelism must both be one"
                        .to_string(),
                ));
            }
        }
        proto::ExchangeDistribution::Unspecified => {
            return Err(DataFusionError::Plan(
                "exchange distribution is required".to_string(),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan() -> proto::NativeExchangePlan {
        proto::NativeExchangePlan {
            protocol_version: crate::PLAN_PROTOCOL_VERSION,
            schema: Some(proto::Schema {
                fields: vec![
                    proto::Field {
                        name: "key".to_string(),
                        r#type: Some(proto::LogicalType {
                            nullable: false,
                            r#type: Some(proto::logical_type::Type::Integer(proto::EmptyType {})),
                        }),
                    },
                    proto::Field {
                        name: "__streamfusion_row_kind".to_string(),
                        r#type: Some(proto::LogicalType {
                            nullable: false,
                            r#type: Some(proto::logical_type::Type::Tinyint(proto::EmptyType {})),
                        }),
                    },
                ],
            }),
            distribution: proto::ExchangeDistribution::Hash.into(),
            key_indices: vec![0],
            max_parallelism: 128,
            parallelism: 4,
            preserve_key_groups: true,
            transport: proto::ExchangeTransport::ArrowIpcStream.into(),
            metadata_columns: Some(proto::ExchangeMetadataColumns {
                row_kind_index: 1,
                stream_record_timestamp_index: None,
            }),
        }
    }

    #[test]
    fn accepts_complete_flink_hash_contract() {
        let encoded = plan().encode_to_vec();
        assert_eq!(decode_exchange_plan(&encoded).unwrap(), plan());
    }

    #[test]
    fn rejects_keys_outside_the_handshake_schema() {
        let mut invalid = plan();
        invalid.key_indices = vec![2];
        assert!(validate(&invalid)
            .unwrap_err()
            .to_string()
            .contains("outside"));
    }

    #[test]
    fn rejects_hash_key_that_refers_to_row_kind() {
        let mut invalid = plan();
        invalid.key_indices = vec![1];
        assert!(validate(&invalid)
            .unwrap_err()
            .to_string()
            .contains("metadata column"));
    }

    #[test]
    fn rejects_nullable_row_kind() {
        let mut invalid = plan();
        invalid.schema.as_mut().unwrap().fields[1]
            .r#type
            .as_mut()
            .unwrap()
            .nullable = true;
        assert!(validate(&invalid)
            .unwrap_err()
            .to_string()
            .contains("non-null TINYINT"));
    }

    #[test]
    fn accepts_nullable_stream_record_timestamp() {
        let mut timestamped = plan();
        timestamped
            .schema
            .as_mut()
            .unwrap()
            .fields
            .push(proto::Field {
                name: "__streamfusion_stream_record_timestamp".to_string(),
                r#type: Some(proto::LogicalType {
                    nullable: true,
                    r#type: Some(proto::logical_type::Type::Bigint(proto::EmptyType {})),
                }),
            });
        timestamped
            .metadata_columns
            .as_mut()
            .unwrap()
            .stream_record_timestamp_index = Some(2);
        assert!(validate(&timestamped).is_ok());
    }

    #[test]
    fn rejects_non_nullable_stream_record_timestamp() {
        let mut timestamped = plan();
        timestamped
            .schema
            .as_mut()
            .unwrap()
            .fields
            .push(proto::Field {
                name: "__streamfusion_stream_record_timestamp".to_string(),
                r#type: Some(proto::LogicalType {
                    nullable: false,
                    r#type: Some(proto::logical_type::Type::Bigint(proto::EmptyType {})),
                }),
            });
        timestamped
            .metadata_columns
            .as_mut()
            .unwrap()
            .stream_record_timestamp_index = Some(2);
        assert!(validate(&timestamped)
            .unwrap_err()
            .to_string()
            .contains("nullable BIGINT"));
    }

    #[test]
    fn lowers_validated_key_types_for_the_native_partitioner() {
        assert_eq!(
            exchange_key_fields(&plan()).unwrap(),
            vec![(0, KeyField::Integer)]
        );
    }
}
