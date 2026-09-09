// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::fmt;
use std::sync::Arc;

use arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::Result;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::{
    expressions::{BinaryExpr, Column},
    EquivalenceProperties, PhysicalExpr,
};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties, SendableRecordBatchStream,
};
use futures::StreamExt;

use super::{probe::LookupProbe, table::invalid, LookupTable};
use crate::planner::operators::envelope::{self, Envelope};

/// One fixed physical stage. Its streams own only a bounded probe cursor; no per-invocation
/// DataFusion execution plans or retained metric descriptors are registered.
pub(crate) struct LookupJoinExec {
    pub(super) table: Arc<LookupTable>,
    input: Arc<dyn ExecutionPlan>,
    pub(super) keys: Vec<usize>,
    pub(super) condition: Arc<dyn PhysicalExpr>,
    pub(super) key_schema: SchemaRef,
    pub(super) probe_payload_width: usize,
    properties: Arc<PlanProperties>,
}

impl LookupJoinExec {
    pub(crate) fn new(
        table: Arc<LookupTable>,
        input: Arc<dyn ExecutionPlan>,
        keys: Vec<usize>,
    ) -> Result<Arc<Self>> {
        let schema = input.schema();
        let envelope = Envelope::from_schema(&schema)?;
        if envelope::owned_timestamp_index(&schema)?.is_none() {
            return Err(invalid("lookup requires the owned native record envelope"));
        }
        if input.output_partitioning().partition_count() != 1 {
            return Err(invalid("lookup requires one task-local probe partition"));
        }
        if keys.len() != table.keys.len() || keys.iter().any(|&i| i >= envelope.payload_width) {
            return Err(invalid(
                "lookup probe keys must match the snapshot keys and stay inside the SQL payload",
            ));
        }
        let mut condition: Option<Arc<dyn PhysicalExpr>> = None;
        let mut key_fields = Vec::new();
        for (index, (&probe, &side)) in keys.iter().zip(&table.keys).enumerate() {
            let data_type = schema.field(probe).data_type();
            if data_type != table.batch.schema().field(side).data_type() {
                return Err(invalid(
                    "lookup key types must match without implicit casts",
                ));
            }
            key_fields.push(Field::new(
                format!("probe_key_{index}"),
                data_type.clone(),
                true,
            ));
            key_fields.push(Field::new(
                format!("side_key_{index}"),
                data_type.clone(),
                true,
            ));
            let eq = Arc::new(BinaryExpr::new(
                Arc::new(Column::new(
                    format!("probe_key_{index}").as_str(),
                    2 * index,
                )),
                Operator::Eq,
                Arc::new(Column::new(
                    format!("side_key_{index}").as_str(),
                    2 * index + 1,
                )),
            )) as Arc<dyn PhysicalExpr>;
            condition = Some(match condition {
                None => eq,
                Some(prior) => Arc::new(BinaryExpr::new(prior, Operator::And, eq)),
            });
        }
        let mut fields = schema.fields()[..envelope.payload_width].to_vec();
        fields.extend(table.batch.schema().fields().iter().cloned());
        fields.extend(envelope.indices().map(|i| schema.fields()[i].clone()));
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::new(Schema::new(fields))),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Arc::new(Self {
            table,
            input,
            keys,
            condition: condition.unwrap(),
            key_schema: Arc::new(Schema::new(key_fields)),
            probe_payload_width: envelope.payload_width,
            properties,
        }))
    }
}

impl fmt::Debug for LookupJoinExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamFusionLookupJoinExec")
            .field("keys", &self.keys)
            .finish_non_exhaustive()
    }
}
impl DisplayAs for LookupJoinExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "StreamFusionLookupJoinExec: DataFusion cached hash lookup"
        )
    }
}
impl ExecutionPlan for LookupJoinExec {
    fn name(&self) -> &str {
        "StreamFusionLookupJoinExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }
    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        f(&self.condition)
    }
    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(invalid("lookup requires one probe child"));
        }
        Ok(Self::new(
            self.table.clone(),
            children.remove(0),
            self.keys.clone(),
        )?)
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(invalid("lookup requires one task-local probe partition"));
        }
        if !Arc::ptr_eq(context.memory_pool(), &self.table.pool) {
            return Err(invalid(
                "lookup snapshot and probe must use the same Flink memory pool",
            ));
        }
        let table = self.table.clone();
        let keys = self.keys.clone();
        let condition = self.condition.clone();
        let key_schema = self.key_schema.clone();
        let output_schema = self.schema();
        let stream_schema = output_schema.clone();
        let payload = self.probe_payload_width;
        let stream = self
            .input
            .execute(partition, context)?
            .flat_map(move |batch| {
                match batch.and_then(|batch| {
                    LookupProbe::new(
                        table.clone(),
                        batch,
                        &keys,
                        condition.clone(),
                        key_schema.clone(),
                        output_schema.clone(),
                        payload,
                    )
                }) {
                    Ok(probe) => futures::stream::unfold(Some(probe), |probe| async move {
                        let mut probe = probe?;
                        match probe.next_batch() {
                            Ok(Some(batch)) => Some((Ok(batch), Some(probe))),
                            Ok(None) => None,
                            Err(error) => Some((Err(error), None)),
                        }
                    })
                    .boxed(),
                    Err(error) => futures::stream::once(async move { Err(error) }).boxed(),
                }
            });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            stream_schema,
            stream,
        )))
    }
}
