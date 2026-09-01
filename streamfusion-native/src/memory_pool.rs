// Copyright 2026 StreamFusion Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

use std::fmt::{Debug, Display, Formatter};
use std::sync::{Arc, Mutex};

use datafusion::error::{DataFusionError, Result};
use datafusion::execution::memory_pool::{MemoryLimit, MemoryPool, MemoryReservation};
use jni::objects::{Global, JObject};
use jni::{jni_sig, jni_str, JValue, JavaVM};

pub(crate) trait MemoryReservationBroker: Debug + Send + Sync {
    fn try_reserve(&self, bytes: usize) -> Result<bool>;

    fn release(&self, bytes: usize) -> Result<()>;

    fn transfer_to_arrow(&self, bytes: usize) -> Result<()> {
        self.release(bytes)
    }
}

/// RAII accounting for native allocations that are not owned by a DataFusion operator.
///
/// The physical allocation still comes from Rust's allocator, as it does in Comet. This
/// reservation is the admission-control token that makes the allocation visible to Flink.
pub(crate) struct HostMemoryReservation {
    broker: Arc<dyn MemoryReservationBroker>,
    consumer: String,
    size: usize,
}

impl HostMemoryReservation {
    pub(crate) fn new(
        broker: Arc<dyn MemoryReservationBroker>,
        consumer: impl Into<String>,
    ) -> Self {
        Self {
            broker,
            consumer: consumer.into(),
            size: 0,
        }
    }

    pub(crate) fn try_grow(&mut self, additional: usize) -> Result<()> {
        if additional == 0 {
            return Ok(());
        }
        let requested = self.size.checked_add(additional).ok_or_else(|| {
            DataFusionError::ResourcesExhausted(format!(
                "{} memory reservation overflowed usize",
                self.consumer
            ))
        })?;
        if !self.broker.try_reserve(additional)? {
            return Err(DataFusionError::ResourcesExhausted(format!(
                "Flink denied {additional} bytes for {}; {} bytes are already reserved by this consumer",
                self.consumer, self.size
            )));
        }
        self.size = requested;
        Ok(())
    }

    pub(crate) fn sibling(&self, consumer: impl Into<String>) -> Self {
        Self::new(Arc::clone(&self.broker), consumer)
    }

    pub(crate) fn resize(&mut self, size: usize) -> Result<()> {
        if size > self.size {
            self.try_grow(size - self.size)
        } else {
            let released = self.size - size;
            self.broker.release(released)?;
            self.size = size;
            Ok(())
        }
    }

    pub(crate) fn transfer_to_arrow(&mut self, bytes: usize) -> Result<()> {
        if bytes > self.size {
            return Err(DataFusionError::Internal(format!(
                "attempted to transfer {bytes} bytes from {} with only {} bytes reserved",
                self.consumer, self.size
            )));
        }
        self.broker.transfer_to_arrow(bytes)?;
        self.size -= bytes;
        Ok(())
    }

    #[cfg(test)]
    fn size(&self) -> usize {
        self.size
    }
}

impl Drop for HostMemoryReservation {
    fn drop(&mut self) {
        if self.size != 0 {
            self.broker
                .release(self.size)
                .expect("Flink host-memory cleanup failed");
        }
    }
}

#[derive(Debug)]
pub(crate) struct JvmMemoryReservationBroker {
    java_vm: JavaVM,
    memory_manager: Global<JObject<'static>>,
}

impl JvmMemoryReservationBroker {
    pub(crate) fn new(java_vm: JavaVM, memory_manager: Global<JObject<'static>>) -> Self {
        Self {
            java_vm,
            memory_manager,
        }
    }
}

impl MemoryReservationBroker for JvmMemoryReservationBroker {
    fn try_reserve(&self, bytes: usize) -> Result<bool> {
        let bytes = i64::try_from(bytes).map_err(|_| {
            DataFusionError::ResourcesExhausted(format!(
                "native allocation of {bytes} bytes exceeds the JVM reservation range"
            ))
        })?;
        self.java_vm
            .attach_current_thread(|env| -> jni::errors::Result<bool> {
                env.call_method(
                    &self.memory_manager,
                    jni_str!("tryReserve"),
                    jni_sig!("(J)Z"),
                    &[JValue::Long(bytes)],
                )?
                .z()
            })
            .map_err(|error| DataFusionError::External(Box::new(error)))
    }

    fn release(&self, bytes: usize) -> Result<()> {
        if bytes == 0 {
            return Ok(());
        }
        let bytes = i64::try_from(bytes).map_err(|_| {
            DataFusionError::Internal(format!(
                "native release of {bytes} bytes exceeds the JVM reservation range"
            ))
        })?;
        self.java_vm
            .attach_current_thread(|env| -> jni::errors::Result<()> {
                env.call_method(
                    &self.memory_manager,
                    jni_str!("release"),
                    jni_sig!("(J)V"),
                    &[JValue::Long(bytes)],
                )?;
                Ok(())
            })
            .map_err(|error| DataFusionError::External(Box::new(error)))
    }

    fn transfer_to_arrow(&self, bytes: usize) -> Result<()> {
        if bytes == 0 {
            return Ok(());
        }
        let bytes = i64::try_from(bytes).map_err(|_| {
            DataFusionError::Internal(format!(
                "native Arrow transfer of {bytes} bytes exceeds the JVM reservation range"
            ))
        })?;
        self.java_vm
            .attach_current_thread(|env| -> jni::errors::Result<()> {
                env.call_method(
                    &self.memory_manager,
                    jni_str!("transferToArrow"),
                    jni_sig!("(J)V"),
                    &[JValue::Long(bytes)],
                )?;
                Ok(())
            })
            .map_err(|error| DataFusionError::External(Box::new(error)))
    }
}

pub(crate) struct FlinkMemoryPool {
    broker: Arc<dyn MemoryReservationBroker>,
    limit: usize,
    reserved: Mutex<usize>,
}

impl FlinkMemoryPool {
    pub(crate) fn new(broker: Arc<dyn MemoryReservationBroker>, limit: usize) -> Self {
        Self {
            broker,
            limit,
            reserved: Mutex::new(0),
        }
    }
}

impl Debug for FlinkMemoryPool {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FlinkMemoryPool")
            .field("limit", &self.limit)
            .field("reserved", &self.reserved())
            .finish()
    }
}

impl Display for FlinkMemoryPool {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "FlinkMemoryPool(reserved: {}, limit: {})",
            self.reserved(),
            self.limit
        )
    }
}

impl MemoryPool for FlinkMemoryPool {
    fn name(&self) -> &str {
        "flink-managed"
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.try_grow(reservation, additional)
            .expect("Flink managed-memory reservation failed during infallible growth");
    }

    fn shrink(&self, _reservation: &MemoryReservation, shrink: usize) {
        if shrink == 0 {
            return;
        }
        let mut reserved = self
            .reserved
            .lock()
            .expect("Flink memory-pool lock poisoned");
        assert!(
            shrink <= *reserved,
            "attempted to release {shrink} bytes with only {reserved} bytes reserved"
        );
        self.broker
            .release(shrink)
            .expect("Flink managed-memory release failed");
        *reserved -= shrink;
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        if additional == 0 {
            return Ok(());
        }
        let mut reserved = self.reserved.lock().map_err(|_| {
            DataFusionError::Internal("Flink memory-pool lock poisoned".to_string())
        })?;
        let requested = reserved.checked_add(additional).ok_or_else(|| {
            DataFusionError::ResourcesExhausted(
                "native memory reservation overflowed usize".to_string(),
            )
        })?;
        if requested > self.limit {
            return Err(DataFusionError::ResourcesExhausted(format!(
                "{} requested {additional} bytes with {reserved} already reserved; Flink assigned {} bytes",
                reservation.consumer().name(),
                self.limit
            )));
        }
        if !self.broker.try_reserve(additional)? {
            return Err(DataFusionError::ResourcesExhausted(format!(
                "Flink denied {additional} bytes for {}; {reserved} of {} bytes are reserved",
                reservation.consumer().name(),
                self.limit
            )));
        }
        *reserved = requested;
        Ok(())
    }

    fn reserved(&self) -> usize {
        *self
            .reserved
            .lock()
            .expect("Flink memory-pool lock poisoned")
    }

    fn memory_limit(&self) -> MemoryLimit {
        MemoryLimit::Finite(self.limit)
    }
}

impl Drop for FlinkMemoryPool {
    fn drop(&mut self) {
        let reserved = *self
            .reserved
            .get_mut()
            .expect("Flink memory-pool lock poisoned during drop");
        if reserved != 0 {
            self.broker
                .release(reserved)
                .expect("Flink managed-memory cleanup failed");
        }
    }
}

#[cfg(test)]
pub(crate) mod tests_support {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool};

    use super::*;

    #[derive(Debug)]
    pub(crate) struct TestBroker {
        reserved: AtomicUsize,
        limit: usize,
    }

    impl TestBroker {
        pub(crate) fn new(limit: usize) -> Self {
            Self {
                reserved: AtomicUsize::new(0),
                limit,
            }
        }

        pub(crate) fn reserved(&self) -> usize {
            self.reserved.load(Ordering::Relaxed)
        }
    }

    impl MemoryReservationBroker for TestBroker {
        fn try_reserve(&self, bytes: usize) -> Result<bool> {
            self.reserved
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |reserved| {
                    (reserved + bytes <= self.limit).then_some(reserved + bytes)
                })
                .map(|_| true)
                .or_else(|_| Ok(false))
        }

        fn release(&self, bytes: usize) -> Result<()> {
            self.reserved.fetch_sub(bytes, Ordering::Relaxed);
            Ok(())
        }
    }
    #[test]
    fn accounts_datafusion_reservations_in_host_broker() {
        let broker = Arc::new(TestBroker {
            reserved: AtomicUsize::new(0),
            limit: 128,
        });
        let pool: Arc<dyn MemoryPool> = Arc::new(FlinkMemoryPool::new(broker.clone(), 128));
        let reservation = MemoryConsumer::new("test").register(&pool);

        reservation.try_grow(96).unwrap();
        assert_eq!(pool.reserved(), 96);
        assert_eq!(broker.reserved.load(Ordering::Relaxed), 96);
        assert!(reservation.try_grow(33).is_err());

        reservation.shrink(32);
        assert_eq!(pool.reserved(), 64);
        assert_eq!(broker.reserved.load(Ordering::Relaxed), 64);
        drop(reservation);
        assert_eq!(broker.reserved.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn accounts_custom_rust_memory_with_raii() {
        let broker = Arc::new(TestBroker {
            reserved: AtomicUsize::new(0),
            limit: 128,
        });
        {
            let mut reservation = HostMemoryReservation::new(broker.clone(), "custom buffer");
            reservation.try_grow(96).unwrap();
            assert_eq!(reservation.size(), 96);
            assert_eq!(broker.reserved.load(Ordering::Relaxed), 96);
            assert!(reservation.try_grow(33).is_err());
            reservation.resize(64).unwrap();
            assert_eq!(broker.reserved.load(Ordering::Relaxed), 64);
        }
        assert_eq!(broker.reserved.load(Ordering::Relaxed), 0);
    }
}
