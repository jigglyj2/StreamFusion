// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use datafusion::error::{DataFusionError, Result};
use prost::Message;

use crate::proto;

pub fn decode_exchange_plan(bytes: &[u8]) -> Result<proto::NativeExchangePlan> {
    let plan = proto::NativeExchangePlan::decode(bytes).map_err(|error| {
        DataFusionError::Plan(format!("invalid StreamFusion exchange plan: {error}"))
    })?;
    validate(&plan)?;
    Ok(plan)
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
            for index in &plan.key_indices {
                if *index as usize >= schema.fields.len() {
                    return Err(DataFusionError::Plan(format!(
                        "exchange key index {index} is outside the {}-field schema",
                        schema.fields.len()
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
                fields: vec![proto::Field {
                    name: "key".to_string(),
                    r#type: Some(proto::LogicalType {
                        nullable: false,
                        r#type: Some(proto::logical_type::Type::Integer(proto::EmptyType {})),
                    }),
                }],
            }),
            distribution: proto::ExchangeDistribution::Hash.into(),
            key_indices: vec![0],
            max_parallelism: 128,
            transport: proto::ExchangeTransport::ArrowIpcStream.into(),
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
        invalid.key_indices = vec![1];
        assert!(validate(&invalid)
            .unwrap_err()
            .to_string()
            .contains("outside"));
    }
}
