// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::sync::{Arc, Mutex};

use arrow::array::RecordBatchReader;
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use futures::StreamExt;
use prost::Message;

use super::execution_plan::RegularJoinFactory;
use super::{region_input, RegularJoinProcessor};
use crate::execution_context::{stream::NativePlanStream, NativeExecutionContext};
use crate::memory_pool::HostMemoryReservation;
use crate::planner::persistent::{children, find_unique};
use crate::{decode_plan, proto};

/// Flink owns the handle's checkpoint lifecycle; the streaming data plane is one native tree.
pub(crate) struct RegularJoinHandle {
    pub(crate) processor: Arc<Mutex<RegularJoinProcessor>>,
    pub(crate) region: Option<Arc<RegularJoinRegion>>,
    _control: Arc<HostMemoryReservation>,
}

impl RegularJoinHandle {
    pub(crate) fn new(
        bytes: &[u8],
        mut control: HostMemoryReservation,
        create: impl FnOnce(&[u8]) -> Result<RegularJoinProcessor>,
    ) -> Result<Self> {
        control.resize(bytes.len().saturating_mul(16).saturating_add(512 * 1024))?;
        let plan = decode_plan(bytes)?;
        let join_node = find_unique(&plan, |node| {
            matches!(
                node.operator,
                Some(proto::operator::Operator::RegularJoin(_))
            )
        })?;
        let Some(proto::operator::Operator::RegularJoin(join)) = &join_node.operator else {
            return Err(DataFusionError::Plan(
                "regular join region requires a join with an optional Calc tail".into(),
            ));
        };
        let bounded = join.bounded_final_output;
        let processor_plan = if bounded {
            bytes.to_vec()
        } else {
            proto::NativePlan {
                root: Some(join_node.clone()),
                ..plan.clone()
            }
            .encode_to_vec()
        };
        let processor = Arc::new(Mutex::new(create(&processor_plan)?));
        let control = Arc::new(control);
        let region = if bounded {
            None
        } else {
            Some(Arc::new(RegularJoinRegion::new(
                plan,
                processor.clone(),
                control.clone(),
            )?))
        };
        Ok(Self {
            processor,
            region,
            _control: control,
        })
    }
}

pub(crate) struct RegularJoinRegion {
    processor: Arc<Mutex<RegularJoinProcessor>>,
    schemas: [SchemaRef; 2],
    keys: [Vec<u32>; 2],
    context: Arc<NativeExecutionContext>,
    calc_ids: Vec<u64>,
    control: Arc<HostMemoryReservation>,
}

impl RegularJoinRegion {
    fn new(
        plan: proto::NativePlan,
        processor: Arc<Mutex<RegularJoinProcessor>>,
        control: Arc<HostMemoryReservation>,
    ) -> Result<Self> {
        let (schemas, keys) = {
            let join = processor.lock().map_err(|_| poisoned())?;
            (
                join.visible_schemas.each_ref().map(region_input::schema),
                [
                    join.plan.left_key_indices.clone(),
                    join.plan.right_key_indices.clone(),
                ],
            )
        };
        let join_node = find_unique(&plan, |node| {
            matches!(
                node.operator,
                Some(proto::operator::Operator::RegularJoin(_))
            )
        })?;
        let mut calc_ids = Vec::new();
        fn collect_calc_ids(node: &proto::Operator, ids: &mut Vec<u64>) -> Result<()> {
            if matches!(node.operator, Some(proto::operator::Operator::Calc(_))) {
                ids.push(node.plan_node_id);
            }
            for child in children(node)? {
                collect_calc_ids(child, ids)?;
            }
            Ok(())
        }
        collect_calc_ids(plan.root.as_ref().expect("validated root"), &mut calc_ids)?;
        let Some(proto::operator::Operator::RegularJoin(join)) = &join_node.operator else {
            unreachable!()
        };
        // The current Java join edge supplies the join's two planned schemas. Upstream native
        // subtrees require their own boundary schemas in the driver contract before admission.
        for (side, child) in [join.left_input.as_ref(), join.right_input.as_ref()]
            .into_iter()
            .enumerate()
        {
            if !matches!(child.and_then(|child| child.operator.as_ref()), Some(proto::operator::Operator::Input(input)) if input.input_index == side as u32)
            {
                return Err(DataFusionError::Plan("regular join region currently requires explicit Input(0) and Input(1) boundaries".into()));
            }
        }
        let memory_pool =
            control.datafusion_pool(control.available_capacity()?.unwrap_or(usize::MAX));
        let mut context = NativeExecutionContext::new(&plan.encode_to_vec(), memory_pool)?;
        context.bind_persistent(vec![(
            join_node.plan_node_id,
            Arc::new(RegularJoinFactory(processor.clone())),
        )])?;
        context.execute_plan(
            schemas
                .iter()
                .map(|schema| RecordBatch::new_empty(schema.clone()))
                .collect(),
            |_| Ok(()),
        )?;
        Ok(Self {
            processor,
            schemas,
            keys,
            context: Arc::new(context),
            calc_ids,
            control,
        })
    }

    pub(crate) fn metrics(&self) -> Vec<i64> {
        self.context
            .metric_snapshot()
            .expect("native metric-tree lock poisoned")
    }

    pub(crate) fn calc_batches(&self) -> u64 {
        self.context
            .input_batch_count(&self.calc_ids)
            .expect("native metric-tree lock poisoned")
    }

    pub(crate) fn start(self: &Arc<Self>, side: usize, batch: RecordBatch) -> Result<RegionStream> {
        if side > 1 {
            return Err(DataFusionError::Execution(
                "invalid regular join input side".into(),
            ));
        }
        self.processor
            .lock()
            .map_err(|_| poisoned())?
            .require_idle_stream()?;
        let mut input_memory = self
            .control
            .sibling("regular join region input routing metadata");
        let batch = region_input::normalize(
            batch,
            self.schemas[side].clone(),
            &self.keys[side],
            &mut input_memory,
        )?;
        let mut inputs = self
            .schemas
            .iter()
            .map(|schema| RecordBatch::new_empty(schema.clone()))
            .collect::<Vec<_>>();
        inputs[side] = batch;
        let stream = self.context.start(inputs)?;
        self.processor
            .lock()
            .map_err(|_| poisoned())?
            .streaming_invocation_active = true;
        Ok(RegionStream {
            stream,
            region: self.clone(),
            complete: false,
            terminal: false,
            _input_memory: input_memory,
        })
    }
}

pub(crate) struct RegionStream {
    stream: NativePlanStream,
    region: Arc<RegularJoinRegion>,
    complete: bool,
    terminal: bool,
    _input_memory: HostMemoryReservation,
}

impl Iterator for RegionStream {
    type Item = std::result::Result<RecordBatch, ArrowError>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.terminal {
            return None;
        }
        let result = self.region.context.runtime().block_on(self.stream.next());
        match result {
            None => {
                self.terminal = true;
                if let Ok(mut join) = self.region.processor.lock() {
                    join.streaming_invocation_active = false;
                    self.complete = true;
                }
                None
            }
            Some(result) => {
                let result = result.and_then(|batch| {
                    // Only the outermost C Stream edge transfers memory to Arrow Java. No native
                    // child relinquishes its lease merely to hand a RecordBatch to its parent.
                    let bytes = batch.get_array_memory_size();
                    let mut edge = self
                        .region
                        .control
                        .sibling("regular join region Arrow export");
                    edge.resize(bytes)?;
                    edge.transfer_to_arrow(bytes)?;
                    Ok(batch)
                });
                if result.is_err() {
                    self.terminal = true;
                }
                Some(result.map_err(|error| ArrowError::ExternalError(Box::new(error))))
            }
        }
    }
}

impl RecordBatchReader for RegionStream {
    fn schema(&self) -> SchemaRef {
        datafusion::physical_plan::RecordBatchStream::schema(&self.stream)
    }
}

impl Drop for RegionStream {
    fn drop(&mut self) {
        if !self.complete {
            if let Ok(mut join) = self.region.processor.lock() {
                join.cancel_streaming_batch();
                join.streaming_failed = true;
                join.streaming_invocation_active = false;
            }
        }
    }
}

fn poisoned() -> DataFusionError {
    DataFusionError::Execution("regular join region state lock is poisoned".into())
}
