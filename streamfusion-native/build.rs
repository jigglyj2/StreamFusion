// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

fn main() {
    let proto = "../streamfusion-proto/src/main/proto/streamfusion_plan.proto";
    let runtime = "../streamfusion-proto/src/main/proto/streamfusion_runtime.proto";
    println!("cargo:rerun-if-changed={proto}");
    println!("cargo:rerun-if-changed={runtime}");
    println!("cargo:rerun-if-changed=build/operator_specs.rs");
    std::env::set_var(
        "PROTOC",
        protoc_bin_vendored::protoc_bin_path().expect("vendored protoc must be available"),
    );
    let mut config = prost_build::Config::new();
    let descriptors = config
        .load_fds(&[proto, runtime], &["../streamfusion-proto/src/main/proto"])
        .expect("StreamFusion protobuf descriptors must compile");
    // Admission metadata is generated from the same schema as the Comet-style plan
    // contract, rather than maintained as operator/expression-specific sizing recipes.
    let file = descriptors
        .file
        .iter()
        .find(|file| file.package() == "streamfusion.plan.v1")
        .expect("plan protobuf package");
    let mut generated = String::from("const LAYOUTS: &[Layout] = &[\n");
    for message in &file.message_type {
        assert!(
            message.nested_type.is_empty(),
            "nested protobuf declarations need Rust path mapping"
        );
        generated.push_str(&format!(
            "Layout {{ inline: std::mem::size_of::<crate::proto::{}>(), fields: &[",
            message.name()
        ));
        for field in &message.field {
            let kind = match field.r#type.unwrap_or_default() {
                11 => {
                    let target = field.type_name().rsplit('.').next().unwrap();
                    let index = file
                        .message_type
                        .iter()
                        .position(|message| message.name() == target)
                        .expect("plan message target belongs to plan package");
                    format!("Kind::Message({index})")
                }
                9 | 12 => "Kind::Bytes".into(),
                _ => "Kind::Scalar".into(),
            };
            generated.push_str(&format!("({}, {kind}),", field.number()));
        }
        generated.push_str("] },\n");
    }
    generated.push_str("];\n");
    let root = file
        .message_type
        .iter()
        .position(|message| message.name() == "NativePlan")
        .unwrap();
    generated.push_str(&format!("const ROOT: usize = {root};\n"));
    let region_root = file
        .message_type
        .iter()
        .position(|message| message.name() == "NativeRegionPlan")
        .unwrap();
    generated.push_str(&format!("const REGION_ROOT: usize = {region_root};\n"));
    let output = std::path::PathBuf::from(std::env::var_os("OUT_DIR").unwrap());
    std::fs::write(output.join("plan_memory_layout.rs"), generated)
        .expect("write plan admission metadata");
    include!("build/operator_specs.rs");
    config
        .compile_fds(descriptors)
        .expect("StreamFusion plan protobuf must compile");
}
