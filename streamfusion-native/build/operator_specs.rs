// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0.

// Included inside build.rs with the same descriptor set as prost. Generate borrowed sizing
// and child-free configuration copies; adding a physical family needs no fusion-pair code.
{
    fn camel(value: &str) -> String {
        value.split('_').map(|part| {
            let mut chars = part.chars();
            chars.next().map(|first| first.to_uppercase().collect::<String>() + chars.as_str()).unwrap_or_default()
        }).collect()
    }
    fn snake(value: &str) -> String {
        let chars: Vec<_> = value.chars().collect();
        let mut result = String::new();
        for (i, &c) in chars.iter().enumerate() {
            if c.is_uppercase() && i > 0 && (chars[i-1].is_lowercase() || chars[i-1].is_numeric()
                    || chars.get(i+1).is_some_and(|c| c.is_lowercase())) { result.push('_'); }
            result.extend(c.to_lowercase());
        }
        result
    }
    let mut generated = String::new();
    for message in &file.message_type {
        generated.push_str(&format!("impl ConfigurationMemory for crate::proto::{} {{\nfn configuration_memory(&self, shallow: bool) -> Result<usize> {{\nlet mut bytes = std::mem::size_of::<Self>().checked_mul(4).and_then(|n| n.checked_add(128)).ok_or_else(overflow)?;\n", message.name()));
        for field in &message.field {
            let actual_oneof = field.oneof_index.filter(|_| !field.proto3_optional.unwrap_or(false));
            let expr = if let Some(index) = actual_oneof {
                let group = message.oneof_decl[index as usize].name();
                generated.push_str(&format!("if let Some(crate::proto::{}::{}::{}(value)) = &self.r#{} {{\n",
                    snake(message.name()), camel(group), camel(field.name()), group));
                "value".to_owned()
            } else {
                format!("&self.r#{}", field.name())
            };
            let target = field.type_name().rsplit('.').next().unwrap_or("");
            if field.r#type == Some(11) {
                if target == "Operator" { generated.push_str("if !shallow {\n"); }
                if actual_oneof.is_some() {
                    generated.push_str("add(&mut bytes, value.configuration_memory(shallow)?)?;\n");
                } else {
                    generated.push_str(&format!("for value in ({expr}).iter() {{ add(&mut bytes, value.configuration_memory(shallow)?)?; }}\n"));
                }
                if target == "Operator" { generated.push_str("}\n"); }
            } else if matches!(field.r#type, Some(9 | 12)) {
                if actual_oneof.is_some() {
                    generated.push_str("add_payload(&mut bytes, value.len())?;\n");
                } else if field.label == Some(3) || field.proto3_optional.unwrap_or(false) {
                    generated.push_str(&format!("for value in ({expr}).iter() {{ add_payload(&mut bytes, value.len())?; }}\n"));
                } else {
                    generated.push_str(&format!("add_payload(&mut bytes, self.r#{}.len())?;\n", field.name()));
                }
            } else if field.label == Some(3) {
                generated.push_str(&format!("add(&mut bytes, self.r#{}.len().checked_mul(32).ok_or_else(overflow)?)?;\n", field.name()));
            }
            if actual_oneof.is_some() { generated.push_str("}\n"); }
        }
        generated.push_str("Ok(bytes)\n}\n}\n");
    }
    generated.push_str("pub(super) fn without_children(node: &crate::proto::Operator) -> crate::proto::Operator {\ncrate::proto::Operator {\n");
    let operator = file.message_type.iter().find(|m| m.name() == "Operator").unwrap();
    for field in operator.field.iter().filter(|f| f.oneof_index.is_none() || f.proto3_optional.unwrap_or(false)) {
        generated.push_str(&format!("r#{}: node.r#{}.clone(),\n", field.name(), field.name()));
    }
    generated.push_str("operator: node.operator.as_ref().map(|kind| match kind {\n");
    for field in operator.field.iter().filter(|f| f.oneof_index.is_some() && !f.proto3_optional.unwrap_or(false)) {
        let variant = camel(field.name());
        let target = field.type_name().rsplit('.').next().unwrap();
        let config = file.message_type.iter().find(|m| m.name() == target).unwrap();
        generated.push_str(&format!("crate::proto::operator::Operator::{variant}(value) => crate::proto::operator::Operator::{variant}(crate::proto::{target} {{\n"));
        let mut copied = std::collections::HashSet::new();
        for property in &config.field {
            let name = property.oneof_index.filter(|_| !property.proto3_optional.unwrap_or(false))
                .map(|index| config.oneof_decl[index as usize].name()).unwrap_or_else(|| property.name());
            if !copied.insert(name) { continue; }
            let value = if property.type_name() == ".streamfusion.plan.v1.Operator" {
                "Default::default()".to_owned()
            } else { format!("value.r#{name}.clone()") };
            generated.push_str(&format!("r#{name}: {value},\n"));
        }
        generated.push_str("}.into()),\n");
    }
    generated.push_str("}),\n}\n}\n");
    std::fs::write(output.join("operator_specs.rs"), generated).expect("write shallow operator configurations");
}
