// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

//! One-definition native DAG plans. The caller supplies Flink's distinct external channels;
//! only internal stage references can share execution. All edges use the owned Arrow envelope.

mod plan;
pub(crate) use plan::{RegionInput, RegionPlan};
