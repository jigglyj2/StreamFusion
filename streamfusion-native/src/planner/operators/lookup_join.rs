// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

//! Flink's immutable task-open lookup cache, using DataFusion's hash build/probe kernels and
//! physical equality expressions. HashJoinExec cannot be repeatedly executed with bounded metric
//! storage in DataFusion 55, and retaining its stream coalesces results across Flink controls.
//! Only that lifetime/stream adaptation is custom; matching and gathering remain vectorized.

mod exec;
mod factory;
pub(crate) use factory::LookupJoinFactory;
mod probe;
mod table;
pub(crate) use exec::LookupJoinExec;
pub(crate) use table::LookupTable;

#[cfg(test)]
mod experiment;
#[cfg(test)]
mod kernel_tests;
#[cfg(test)]
mod memory_tests;
#[cfg(test)]
mod test_support;

#[cfg(test)]
mod binding_tests;
