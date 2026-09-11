// Copyright 2026 StreamFusion Authors. Licensed under the Apache License, Version 2.0.
use super::{
    common::throw,
    region_output::{self, Event},
};
use jni::{
    errors::ThrowRuntimeExAndDefault,
    objects::{JClass, JIntArray, JLongArray},
    sys::{jbyteArray, jlong},
    EnvUnowned,
};

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeExchangeOutputs_bindNative<'a>(
    mut env: EnvUnowned<'a>,
    _: JClass<'a>,
    context: jlong,
    ports: JIntArray<'a>,
    routers: JLongArray<'a>,
    arrow: JIntArray<'a>,
) {
    env.with_env(|env| -> jni::errors::Result<_> {
        let count = ports.len(env)?;
        if count != routers.len(env)? {
            return Err(throw(env, "exchange output binding arity mismatch"));
        }
        let mut port_values = vec![0; count];
        let mut handles = vec![0; count];
        let mut arrows = vec![0; arrow.len(env)?];
        ports.get_region(env, 0, &mut port_values)?;
        routers.get_region(env, 0, &mut handles)?;
        arrow.get_region(env, 0, &mut arrows)?;
        if arrows.iter().any(|&v| v != 0 && v != 1) {
            return Err(throw(env, "invalid Arrow output flag"));
        }
        let bound: datafusion::error::Result<_> = (|| {
            let routers = port_values
                .into_iter()
                .zip(handles)
                .map(|(port, handle)| {
                    let port = usize::try_from(port)
                        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
                    Ok((port, super::exchange_router::get(handle)?))
                })
                .collect::<datafusion::error::Result<Vec<_>>>()?;
            let arrows = arrows.into_iter().map(|v| v == 1).collect::<Vec<_>>();
            crate::execution_context::get(context)?.bind_exchange_outputs(
                crate::exchange::output_bindings::OutputBindings::new(&arrows, routers)?,
            )
        })();
        bound.map_err(|e| throw(env, e))
    })
    .resolve::<ThrowRuntimeExAndDefault>();
}

#[unsafe(no_mangle)]
pub extern "system" fn Java_tech_streamfusion_nativebridge_NativeRegionStream_nextOutputBatch<
    'a,
>(
    mut env: EnvUnowned<'a>,
    _: JClass<'a>,
    handle: jlong,
    array: jlong,
    schema: jlong,
) -> jbyteArray {
    env.with_env(|env| -> jni::errors::Result<_> {
        let event = region_output::with_output(handle, |output| unsafe {
            output.next_event(array as *mut _, schema as *mut _)
        })
        .map_err(|e| throw(env, e))?;
        let (kind, port, rows) = match &event {
            Event::End => return Ok(std::ptr::null_mut()),
            Event::Arrow(port, rows) => (1u32, *port, *rows),
            Event::Frames(id, rows, _) => (2u32, *id, *rows),
        };
        let port = i32::try_from(port).map_err(|e| throw(env, e))?;
        let rows = i32::try_from(rows).map_err(|e| throw(env, e))?;
        let mut header = [0u8; 16];
        header[..4].copy_from_slice(&1u32.to_le_bytes());
        header[4..8].copy_from_slice(&kind.to_le_bytes());
        header[8..12].copy_from_slice(&port.to_le_bytes());
        header[12..].copy_from_slice(&rows.to_le_bytes());
        match event {
            Event::Frames(_, _, frames) => {
                super::exchange::export_frames_prefixed(env, &frames.frames, &header)
            }
            _ => env
                .byte_array_from_slice(&header)
                .map(|value| value.into_raw()),
        }
    })
    .resolve::<ThrowRuntimeExAndDefault>()
}
