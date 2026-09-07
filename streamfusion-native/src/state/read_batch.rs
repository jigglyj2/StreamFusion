// Copyright 2026 StreamFusion Authors
// Licensed under the Apache License, Version 2.0

use super::StateValue;
use crate::memory_pool::HostMemoryReservation;
use datafusion::error::Result;
use std::ops::Deref;

/// Owns the result slots, including missing keys, independently of payload owners.
pub(crate) struct StateReadBatch<'a> {
    values: Vec<Option<StateValue<'a>>>,
    reservation: HostMemoryReservation,
}

impl<'a> StateReadBatch<'a> {
    pub(crate) fn empty(owner: &HostMemoryReservation) -> Self {
        Self::new(Vec::new(), owner.sibling("empty state read"))
    }
    pub(crate) fn admit(
        len: usize,
        owner: &HostMemoryReservation,
    ) -> Result<HostMemoryReservation> {
        let mut reservation = owner.sibling("native state read result slots");
        reservation.resize(len.saturating_mul(std::mem::size_of::<Option<StateValue<'a>>>()))?;
        Ok(reservation)
    }

    pub(crate) fn new(
        values: Vec<Option<StateValue<'a>>>,
        reservation: HostMemoryReservation,
    ) -> Self {
        Self {
            values,
            reservation,
        }
    }

    pub(crate) fn pop(&mut self) -> Option<Option<StateValue<'a>>> {
        self.values.pop()
    }
}

impl<'a> Deref for StateReadBatch<'a> {
    type Target = [Option<StateValue<'a>>];
    fn deref(&self) -> &Self::Target {
        &self.values
    }
}
impl std::fmt::Debug for StateReadBatch<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.values.fmt(f)
    }
}

pub(crate) struct StateReadIter<'a> {
    values: std::vec::IntoIter<Option<StateValue<'a>>>,
    _reservation: HostMemoryReservation,
}
impl<'a> Iterator for StateReadIter<'a> {
    type Item = Option<StateValue<'a>>;
    fn next(&mut self) -> Option<Self::Item> {
        self.values.next()
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.values.size_hint()
    }
}
impl ExactSizeIterator for StateReadIter<'_> {}
impl<'a> IntoIterator for StateReadBatch<'a> {
    type Item = Option<StateValue<'a>>;
    type IntoIter = StateReadIter<'a>;
    fn into_iter(self) -> Self::IntoIter {
        StateReadIter {
            values: self.values.into_iter(),
            _reservation: self.reservation,
        }
    }
}
impl<'a, 'b> IntoIterator for &'b StateReadBatch<'a> {
    type Item = &'b Option<StateValue<'a>>;
    type IntoIter = std::slice::Iter<'b, Option<StateValue<'a>>>;
    fn into_iter(self) -> Self::IntoIter {
        self.values.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory_pool::tests_support::TestBroker;
    use std::sync::Arc;

    #[test]
    fn missing_value_slots_remain_charged_while_an_iterator_owns_them() {
        let broker = Arc::new(TestBroker::new(4096));
        let owner = HostMemoryReservation::new(broker.clone(), "state read test");
        let reservation = StateReadBatch::admit(10, &owner).unwrap();
        let batch = StateReadBatch::new(vec![None; 10], reservation);
        let size = 10 * std::mem::size_of::<Option<StateValue<'_>>>();
        assert_eq!(broker.reserved(), size);
        let mut iterator = batch.into_iter();
        assert!(iterator.next().unwrap().is_none());
        assert_eq!(broker.reserved(), size);
        drop(iterator);
        assert_eq!(broker.reserved(), 0);
        assert!(StateReadBatch::admit(4096, &owner).is_err());
        assert_eq!(broker.reserved(), 0);
    }
}
