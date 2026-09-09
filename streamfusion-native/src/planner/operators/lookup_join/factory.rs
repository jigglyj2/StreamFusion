// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::error::Result;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::physical_plan::ExecutionPlan;

use super::{table::invalid, LookupJoinExec, LookupTable};
use crate::execution_context::operator_spec;
use crate::planner::{
    arrow_schema, operators::envelope::Envelope, persistent::control::ControlEvent,
    persistent::PersistentOperatorFactory,
};
use crate::proto;

/// One immutable cache per original Flink lookup node. The caller loads its source at task
/// open, before probing; recovery constructs a new cache, like Flink's CsvLookupFunction.
/// It is a task resource, not keyed state and not a separately checkpointed database.
pub(crate) struct LookupJoinFactory {
    table: Arc<LookupTable>,
    node: proto::Operator,
    input_schema: SchemaRef,
    output_schema: SchemaRef,
    keys: Vec<usize>,
    _configuration: MemoryReservation,
}

impl LookupJoinFactory {
    pub(crate) fn new(node: &proto::Operator, table: Arc<LookupTable>) -> Result<Self> {
        let Some(proto::operator::Operator::LookupJoin(spec)) = &node.operator else {
            return Err(invalid("lookup binding requires its own physical node"));
        };
        if node.plan_node_id == 0 || spec.kind != proto::LookupJoinKind::Inner as i32 {
            return Err(invalid(
                "lookup binding requires a nonzero identity and inner equality mode",
            ));
        }
        let configuration =
            MemoryConsumer::new("lookup physical configuration").register(&table.pool);
        let configuration_bytes = operator_spec::admission(node)?;
        let schemas = crate::planner::schema_memory::planned_schemas(
            spec.input_schema.as_ref(),
            spec.output_schema.as_ref(),
            &[],
        )?
        .checked_add(crate::planner::schema_memory::planned_schemas(
            spec.side_schema.as_ref(),
            None,
            &[],
        )?)
        .and_then(|n| n.checked_add(configuration_bytes))
        .ok_or_else(|| invalid("lookup configuration admission overflow"))?;
        configuration.try_grow(schemas)?;
        let schema = |schema: &Option<proto::Schema>| {
            arrow_schema(
                schema
                    .as_ref()
                    .ok_or_else(|| invalid("lookup requires explicit payload schemas"))?,
            )
        };
        let input_schema = schema(&spec.input_schema)?;
        let side_schema = schema(&spec.side_schema)?;
        let output_schema = schema(&spec.output_schema)?;
        if side_schema != table.batch.schema()
            || spec
                .side_keys
                .iter()
                .map(|&i| i as usize)
                .ne(table.keys.iter().copied())
        {
            return Err(invalid(
                "lookup snapshot schema or keys differ from the physical plan",
            ));
        }
        let keys = spec
            .probe_keys
            .iter()
            .map(|&i| i as usize)
            .collect::<Vec<_>>();
        if keys.len() != table.keys.len()
            || keys.iter().any(|&i| i >= input_schema.fields().len())
            || keys.iter().zip(&table.keys).any(|(&probe, &side)| {
                input_schema.field(probe).data_type() != side_schema.field(side).data_type()
            })
        {
            return Err(invalid(
                "lookup probe keys must match the snapshot types and key count",
            ));
        }
        if input_schema
            .fields()
            .iter()
            .chain(side_schema.fields())
            .map(|f| (f.data_type(), f.is_nullable()))
            .ne(output_schema
                .fields()
                .iter()
                .map(|f| (f.data_type(), f.is_nullable())))
        {
            return Err(invalid(
                "lookup output schema must concatenate probe and snapshot payloads",
            ));
        }
        Ok(Self {
            table,
            node: operator_spec::without_children(node),
            input_schema,
            output_schema,
            keys,
            _configuration: configuration,
        })
    }
}

impl PersistentOperatorFactory for LookupJoinFactory {
    fn supports_owned_envelope(&self) -> bool {
        true
    }

    fn supports_control(&self, event: ControlEvent) -> bool {
        // All probe outputs drain in the ordinary child stream. The immutable cache has no
        // pending bundle, watermark timers or checkpointed state to flush on these controls.
        matches!(
            event,
            ControlEvent::Watermark(_) | ControlEvent::BeforeCheckpoint(_) | ControlEvent::EndInput
        )
    }

    fn build(
        &self,
        node: &proto::Operator,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 || operator_spec::without_children(node) != self.node {
            return Err(invalid(
                "lookup binding differs from its physical node or probe arity",
            ));
        }
        let input = children.remove(0);
        let schema = input.schema();
        let envelope = Envelope::from_schema(&schema)?;
        if schema.fields()[..envelope.payload_width]
            .iter()
            .map(|f| (f.data_type(), f.is_nullable()))
            .ne(self
                .input_schema
                .fields()
                .iter()
                .map(|f| (f.data_type(), f.is_nullable())))
        {
            return Err(invalid(
                "lookup child payload differs from its planned schema",
            ));
        }
        let plan = LookupJoinExec::new(self.table.clone(), input, self.keys.clone())?;
        debug_assert!(plan.schema().fields()[..self.output_schema.fields().len()]
            .iter()
            .map(|f| (f.data_type(), f.is_nullable()))
            .eq(self
                .output_schema
                .fields()
                .iter()
                .map(|f| (f.data_type(), f.is_nullable()))));
        Ok(plan)
    }
}
