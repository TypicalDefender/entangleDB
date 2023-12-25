use super::{Engine, Status};
use crate::error::Result;

/// An in-memory key/value storage engine using the Rust standard library B-tree
/// implementation. Data is not persisted.
/// This engine is primarily used for testing and scenarios where persistence is not required.
/// It provides a simple and fast key-value store with all data held in memory.
pub struct Memory {
    data: std::collections::BTreeMap<Vec<u8>, Vec<u8>>,
}

impl Memory {
    /// Creates a new Memory key-value storage engine.
    /// Initializes an empty BTreeMap to hold the key-value data in memory.
    pub fn new() -> Self {
        Self { data: std::collections::BTreeMap::new() }
    }
}

impl std::fmt::Display for Memory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Display the name of the engine when printed.
        write!(f, "memory")
    }
}

impl Engine for Memory {
    type ScanIterator<'a> = ScanIterator<'a>;

    fn flush(&mut self) -> Result<()> {
        // Flushing is a no-op for the in-memory engine, as there is no disk I/O.
        Ok(())
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        // Remove the key from the BTreeMap, effectively deleting it from the in-memory store.
        self.data.remove(key);
        Ok(())
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Retrieve the value associated with the key from the BTreeMap, if it exists.
        Ok(self.data.get(key).cloned())
    }

    fn scan<R: std::ops::RangeBounds<Vec<u8>>>(&mut self, range: R) -> Self::ScanIterator<'_> {
        // Create an iterator that scans over the range of keys specified, using the BTreeMap's range function.
        ScanIterator { inner: self.data.range(range) }
    }

    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        // Insert the key-value pair into the BTreeMap, updating the value if the key already exists.
        self.data.insert(key.to_vec(), value);
        Ok(())
    }

    fn status(&mut self) -> Result<Status> {
        // Generate a status report containing the engine name, number of keys, and size of all keys and values.
        Ok(Status {
            name: self.to_string(),
            keys: self.data.len() as u64,
            size: self.data.iter().fold(0, |size, (k, v)| size + k.len() as u64 + v.len() as u64),
            total_disk_size: 0,
            live_disk_size: 0,
            garbage_disk_size: 0,
        })
    }
}

pub struct ScanIterator<'a> {
    inner: std::collections::btree_map::Range<'a, Vec<u8>, Vec<u8>>,
}

impl<'a> ScanIterator<'a> {
    fn map(item: (&Vec<u8>, &Vec<u8>)) -> <Self as Iterator>::Item {
        // Map the key and value references to owned clones for the iterator to yield.
        let (key, value) = item;
        Ok((key.clone(), value.clone()))
    }
}

impl<'a> Iterator for ScanIterator<'a> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        // Advance the iterator and return the next key-value pair, cloning the data from the BTreeMap.
        self.inner.next().map(Self::map)
    }
}

impl<'a> DoubleEndedIterator for ScanIterator<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        // Advance the iterator in reverse and return the previous key-value pair, cloning the data from the BTreeMap.
        self.inner.next_back().map(Self::map)
    }
}

#[cfg(test)]
mod tests {
    // This module contains tests for the Memory storage engine.
    use super::*;

    // Run the generic engine tests using an instance of the Memory engine.
    super::super::tests::test_engine!(Memory::new());
}
