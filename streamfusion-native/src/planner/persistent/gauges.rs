// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

pub(crate) struct GaugeDefinition {
    pub(crate) groups: &'static [&'static str],
    pub(crate) name: &'static str,
    pub(crate) metric_kind: crate::proto::NativeMetricKind,
    pub(crate) meter_name: &'static str,
    pub(crate) kind: crate::proto::NativeGaugeValueKind,
}
