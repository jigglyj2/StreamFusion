// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

fn main() {
    let proto = "../streamfusion-proto/src/main/proto/streamfusion_plan.proto";
    println!("cargo:rerun-if-changed={proto}");
    std::env::set_var(
        "PROTOC",
        protoc_bin_vendored::protoc_bin_path().expect("vendored protoc must be available"),
    );
    prost_build::compile_protos(&[proto], &["../streamfusion-proto/src/main/proto"])
        .expect("StreamFusion plan protobuf must compile");
}
