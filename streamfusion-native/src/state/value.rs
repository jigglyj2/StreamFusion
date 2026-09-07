// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use std::ops::Deref;
use std::sync::Arc;

use arrow::array::BinaryViewArray;

/// A state value either borrows the in-memory backend or retains a slice of a
/// producer-owned Arrow buffer. Decoding a RocksDB batch does not copy its payload.
#[derive(Clone)]
pub(crate) enum StateValue<'a> {
    Borrowed(&'a [u8]),
    ArrowView {
        values: Arc<AccountedStateValues>,
        index: usize,
    },
}

/// The producer buffers and admission token share one lifetime, including clones.
/// Keep the fields private so callers cannot detach an Arrow owner from its charge.
pub(crate) struct AccountedStateValues {
    values: BinaryViewArray,
    _reservation: crate::memory_pool::HostMemoryReservation,
    // Drop Arrow buffers before unloading their producer's release callbacks.
    _producer: Option<Arc<libloading::Library>>,
}

impl AccountedStateValues {
    pub(crate) fn new(
        values: BinaryViewArray,
        reservation: crate::memory_pool::HostMemoryReservation,
        producer: Option<Arc<libloading::Library>>,
    ) -> Self {
        Self {
            values,
            _reservation: reservation,
            _producer: producer,
        }
    }
}

impl StateValue<'_> {
    pub(crate) fn into_owned(self) -> Vec<u8> {
        self.as_ref().to_vec()
    }
}

impl AsRef<[u8]> for StateValue<'_> {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Borrowed(bytes) => bytes,
            Self::ArrowView { values, index } => values.values.value(*index),
        }
    }
}

impl std::fmt::Debug for StateValue<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_tuple("StateValue")
            .field(&self.as_ref())
            .finish()
    }
}

impl Deref for StateValue<'_> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_share_producer_buffers_and_survive_the_array_owner() {
        let array = BinaryViewArray::from(vec![
            Some(b"first".as_slice()),
            None,
            Some(b"a long state value outside the inline view".as_slice()),
        ]);
        let pointer = array.value(2).as_ptr();
        let broker = Arc::new(crate::memory_pool::tests_support::TestBroker::new(1 << 20));
        let mut reservation =
            crate::memory_pool::HostMemoryReservation::new(broker.clone(), "view test");
        reservation.resize(1024).unwrap();
        let array = Arc::new(AccountedStateValues::new(array, reservation, None));
        let value = StateValue::ArrowView {
            values: Arc::clone(&array),
            index: 2,
        };
        drop(array);
        assert_eq!(
            value.as_ref(),
            b"a long state value outside the inline view"
        );
        assert_eq!(value.as_ptr(), pointer);
        assert_eq!(broker.reserved(), 1024);
        let clone = value.clone();
        drop(value);
        assert_eq!(broker.reserved(), 1024);
        drop(clone);
        assert_eq!(broker.reserved(), 0);
    }
}
