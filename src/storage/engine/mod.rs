mod bitcask;
mod memory;

#[cfg(test)]
pub use super::debug::Engine as Debug;
pub use bitcask::BitCask;
pub use memory::Memory;

use crate::error::Result;

use serde::{Deserialize, Serialize};

/// A key/value storage engine, where both keys and values are arbitrary byte
/// strings between 0 B and 2 GB, stored in lexicographical key order. Writes
/// are only guaranteed durable after calling flush().
/// 
/// This trait defines the core operations that storage engines must implement,
/// including CRUD operations and scans over key ranges. It is designed to be
/// flexible enough to support different storage engine implementations with
/// varying characteristics.
///
/// Only supports single-threaded use since all methods (including reads) take a
/// mutable reference -- serialized access can't be avoided anyway, since both
/// Raft execution and file access is serial.
pub trait Engine: std::fmt::Display + Send + Sync {
    /// The iterator returned by scan(). Traits can't return "impl Trait", and
    /// we don't want to use trait objects, so the type must be specified.
    /// 
    /// The ScanIterator type allows for iterating over key-value pairs in a
    /// specified range. It is a double-ended iterator, meaning it can be
    /// iterated from both the front and the back. This is useful for queries
    /// that need to traverse keys in a specific order.
    type ScanIterator<'a>: DoubleEndedIterator<Item = Result<(Vec<u8>, Vec<u8>)>> + 'a
    where
        Self: 'a;

    /// Deletes a key, or does nothing if it does not exist.
    /// 
    /// This method is used to remove a key and its associated value from the
    /// storage engine. If the key does not exist, the operation will have no
    /// effect.
    fn delete(&mut self, key: &[u8]) -> Result<()>;

    /// Flushes any buffered data to the underlying storage medium.
    /// 
    /// This method ensures that all buffered writes are persisted to the
    /// storage medium. It is important to call this method before shutting
    /// down the system to prevent data loss.
    fn flush(&mut self) -> Result<()>;

    /// Gets a value for a key, if it exists.
    /// 
    /// This method retrieves the value associated with a given key, if it
    /// exists. If the key is not found, it returns None.
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Iterates over an ordered range of key/value pairs.
    /// 
    /// This method returns an iterator that provides access to key-value pairs
    /// within a specified range. The range is defined using Rust's standard
    /// RangeBounds trait, allowing for flexible range specification.
    fn scan<R: std::ops::RangeBounds<Vec<u8>>>(&mut self, range: R) -> Self::ScanIterator<'_>;

    /// Sets a value for a key, replacing the existing value if any.
    /// 
    /// This method is used to set the value for a given key. If the key
    /// already exists, its value is updated with the new value provided.
    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()>;

    /// Returns engine status.
    /// 
    /// This method provides a snapshot of the current status of the storage
    /// engine, including the number of keys, the size of the data, and
    /// information about disk usage. This can be used for monitoring and
    /// diagnostics.
    fn status(&mut self) -> Result<Status>;

    /// Iterates over all key/value pairs starting with prefix.
    /// 
    /// This method is a convenience function for scanning keys that share a
    /// common prefix. It is particularly useful for queries that target a
    /// specific subset of keys.
    fn scan_prefix(&mut self, prefix: &[u8]) -> Self::ScanIterator<'_> {
        let start = std::ops::Bound::Included(prefix.to_vec());
        let end = match prefix.iter().rposition(|b| *b != 0xff) {
            Some(i) => std::ops::Bound::Excluded(
                prefix.iter().take(i).copied().chain(std::iter::once(prefix[i] + 1)).collect(),
            ),
            None => std::ops::Bound::Unbounded,
        };
        self.scan((start, end))
    }
}

/// Engine status.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Status {
    /// The name of the storage engine.
    /// 
    /// This field identifies the storage engine being used. It can be helpful
    /// when working with multiple storage engines or for display in monitoring
    /// tools.
    pub name: String,
    /// The number of live keys in the engine.
    /// 
    /// This field indicates the total number of keys that are currently stored
    /// in the engine. It does not include keys that have been marked for
    /// deletion.
    pub keys: u64,
    /// The logical size of live key/value pairs.
    /// 
    /// This field represents the total size, in bytes, of all keys and values
    /// stored in the engine. It is the sum of the sizes of all live key-value
    /// pairs.
    pub size: u64,
    /// The on-disk size of all data, live and garbage.
    /// 
    /// This field represents the total size, in bytes, of the data on disk,
    /// including both live data and any data that has been marked for deletion
    /// but not yet compacted or cleaned up.
    pub total_disk_size: u64,
    /// The on-disk size of live data.
    /// 
    /// This field indicates the size, in bytes, of the data on disk that is
    /// currently in use and accessible by the engine. It excludes any space
    /// taken up by data that has been marked for deletion.
    pub live_disk_size: u64,
    /// The on-disk size of garbage data.
    /// 
    /// This field indicates the size, in bytes, of data on disk that is no
    /// longer in use, such as values that have been overwritten or keys that
    /// have been deleted. This data may be reclaimed during a compaction
    /// process.
    pub garbage_disk_size: u64,
}

#[cfg(test)]
mod tests {
    /// Generates common tests for any Engine implementation.
    macro_rules! test_engine {
        ($setup:expr) => {
            #[track_caller]
            /// Asserts that a scan yields the expected items.
            fn assert_scan<I>(iter: I, expect: Vec<(&[u8], Vec<u8>)>) -> Result<()>
            where
                I: Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>,
            {
                assert_eq!(
                    iter.collect::<Result<Vec<_>>>()?,
                    expect.into_iter().map(|(k, v)| (k.to_vec(), v)).collect::<Vec<_>>()
                );
                Ok(())
            }

            /// Tests Engine point operations, i.e. set, get, and delete.
            #[test]
            fn point_ops() -> Result<()> {
                let mut s = $setup;

                // Getting a missing key should return None.
                assert_eq!(s.get(b"a")?, None);

                // Setting and getting a key should return its value.
                s.set(b"a", vec![1])?;
                assert_eq!(s.get(b"a")?, Some(vec![1]));

                // Setting a different key should not affect the first.
                s.set(b"b", vec![2])?;
                assert_eq!(s.get(b"b")?, Some(vec![2]));
                assert_eq!(s.get(b"a")?, Some(vec![1]));

                // Getting a different missing key should return None. The
                // comparison is case-insensitive for strings.
                assert_eq!(s.get(b"c")?, None);
                assert_eq!(s.get(b"A")?, None);

                // Setting an existing key should replace its value.
                s.set(b"a", vec![0])?;
                assert_eq!(s.get(b"a")?, Some(vec![0]));

                // Deleting a key should remove it, but not affect others.
                s.delete(b"a")?;
                assert_eq!(s.get(b"a")?, None);
                assert_eq!(s.get(b"b")?, Some(vec![2]));

                // Deletes are idempotent.
                s.delete(b"a")?;
                assert_eq!(s.get(b"a")?, None);

                Ok(())
            }

            #[test]
            /// Tests Engine point operations on empty keys and values. These
            /// are as valid as any other key/value.
            fn point_ops_empty() -> Result<()> {
                let mut s = $setup;
                assert_eq!(s.get(b"")?, None);
                s.set(b"", vec![])?;
                assert_eq!(s.get(b"")?, Some(vec![]));
                s.delete(b"")?;
                assert_eq!(s.get(b"")?, None);
                Ok(())
            }

            #[test]
            /// Tests Engine point operations on keys and values of increasing
            /// sizes, up to 16 MB.
            fn point_ops_sizes() -> Result<()> {
                let mut s = $setup;

                // Generate keys/values for increasing powers of two.
                for size in (1..=24).map(|i| 1 << i) {
                    let bytes = "x".repeat(size);
                    let key = bytes.as_bytes();
                    let value = bytes.clone().into_bytes();

                    assert_eq!(s.get(key)?, None);
                    s.set(key, value.clone())?;
                    assert_eq!(s.get(key)?, Some(value));
                    s.delete(key)?;
                    assert_eq!(s.get(key)?, None);
                }

                Ok(())
            }

            #[test]
            /// Tests various Engine scans.
            fn scan() -> Result<()> {
                let mut s = $setup;
                s.set(b"a", vec![1])?;
                s.set(b"b", vec![2])?;
                s.set(b"ba", vec![2, 1])?;
                s.set(b"bb", vec![2, 2])?;
                s.set(b"c", vec![3])?;
                s.set(b"C", vec![3])?;

                // Forward/reverse scans.
                assert_scan(
                    s.scan(b"b".to_vec()..b"bz".to_vec()),
                    vec![(b"b", vec![2]), (b"ba", vec![2, 1]), (b"bb", vec![2, 2])],
                )?;
                assert_scan(
                    s.scan(b"b".to_vec()..b"bz".to_vec()).rev(),
                    vec![(b"bb", vec![2, 2]), (b"ba", vec![2, 1]), (b"b", vec![2])],
                )?;

                // Inclusive/exclusive ranges.
                assert_scan(
                    s.scan(b"b".to_vec()..b"bb".to_vec()),
                    vec![(b"b", vec![2]), (b"ba", vec![2, 1])],
                )?;
                assert_scan(
                    s.scan(b"b".to_vec()..=b"bb".to_vec()),
                    vec![(b"b", vec![2]), (b"ba", vec![2, 1]), (b"bb", vec![2, 2])],
                )?;

                // Open ranges.
                assert_scan(s.scan(b"bb".to_vec()..), vec![(b"bb", vec![2, 2]), (b"c", vec![3])])?;
                assert_scan(
                    s.scan(..=b"b".to_vec()),
                    vec![(b"C", vec![3]), (b"a", vec![1]), (b"b", vec![2])],
                )?;

                // Full range.
                assert_scan(
                    s.scan(..),
                    vec![
                        (b"C", vec![3]),
                        (b"a", vec![1]),
                        (b"b", vec![2]),
                        (b"ba", vec![2, 1]),
                        (b"bb", vec![2, 2]),
                        (b"c", vec![3]),
                    ],
                )?;
                Ok(())
            }

            #[test]
            /// Tests prefix scans.
            fn scan_prefix() -> Result<()> {
                let mut s = $setup;
                s.set(b"a", vec![1])?;
                s.set(b"b", vec![2])?;
                s.set(b"ba", vec![2, 1])?;
                s.set(b"bb", vec![2, 2])?;
                s.set(b"b\xff", vec![2, 0xff])?;
                s.set(b"b\xff\x00", vec![2, 0xff, 0x00])?;
                s.set(b"b\xffb", vec![2, 0xff, 2])?;
                s.set(b"b\xff\xff", vec![2, 0xff, 0xff])?;
                s.set(b"c", vec![3])?;
                s.set(b"\xff", vec![0xff])?;
                s.set(b"\xff\xff", vec![0xff, 0xff])?;
                s.set(b"\xff\xff\xff", vec![0xff, 0xff, 0xff])?;
                s.set(b"\xff\xff\xff\xff", vec![0xff, 0xff, 0xff, 0xff])?;

                assert_scan(
                    s.scan_prefix(b""),
                    vec![
                        (b"a", vec![1]),
                        (b"b", vec![2]),
                        (b"ba", vec![2, 1]),
                        (b"bb", vec![2, 2]),
                        (b"b\xff", vec![2, 0xff]),
                        (b"b\xff\x00", vec![2, 0xff, 0x00]),
                        (b"b\xffb", vec![2, 0xff, 2]),
                        (b"b\xff\xff", vec![2, 0xff, 0xff]),
                        (b"c", vec![3]),
                        (b"\xff", vec![0xff]),
                        (b"\xff\xff", vec![0xff, 0xff]),
                        (b"\xff\xff\xff", vec![0xff, 0xff, 0xff]),
                        (b"\xff\xff\xff\xff", vec![0xff, 0xff, 0xff, 0xff]),
                    ],
                )?;

                assert_scan(
                    s.scan_prefix(b"b"),
                    vec![
                        (b"b", vec![2]),
                        (b"ba", vec![2, 1]),
                        (b"bb", vec![2, 2]),
                        (b"b\xff", vec![2, 0xff]),
                        (b"b\xff\x00", vec![2, 0xff, 0x00]),
                        (b"b\xffb", vec![2, 0xff, 2]),
                        (b"b\xff\xff", vec![2, 0xff, 0xff]),
                    ],
                )?;

                assert_scan(s.scan_prefix(b"bb"), vec![(b"bb", vec![2, 2])])?;

                assert_scan(s.scan_prefix(b"bq"), vec![])?;

                assert_scan(
                    s.scan_prefix(b"b\xff"),
                    vec![
                        (b"b\xff", vec![2, 0xff]),
                        (b"b\xff\x00", vec![2, 0xff, 0x00]),
                        (b"b\xffb", vec![2, 0xff, 2]),
                        (b"b\xff\xff", vec![2, 0xff, 0xff]),
                    ],
                )?;

                assert_scan(
                    s.scan_prefix(b"b\xff\x00"),
                    vec![(b"b\xff\x00", vec![2, 0xff, 0x00])],
                )?;

                assert_scan(
                    s.scan_prefix(b"b\xff\xff"),
                    vec![(b"b\xff\xff", vec![2, 0xff, 0xff])],
                )?;

                assert_scan(
                    s.scan_prefix(b"\xff"),
                    vec![
                        (b"\xff", vec![0xff]),
                        (b"\xff\xff", vec![0xff, 0xff]),
                        (b"\xff\xff\xff", vec![0xff, 0xff, 0xff]),
                        (b"\xff\xff\xff\xff", vec![0xff, 0xff, 0xff, 0xff]),
                    ],
                )?;

                assert_scan(
                    s.scan_prefix(b"\xff\xff"),
                    vec![
                        (b"\xff\xff", vec![0xff, 0xff]),
                        (b"\xff\xff\xff", vec![0xff, 0xff, 0xff]),
                        (b"\xff\xff\xff\xff", vec![0xff, 0xff, 0xff, 0xff]),
                    ],
                )?;

                assert_scan(
                    s.scan_prefix(b"\xff\xff\xff"),
                    vec![
                        (b"\xff\xff\xff", vec![0xff, 0xff, 0xff]),
                        (b"\xff\xff\xff\xff", vec![0xff, 0xff, 0xff, 0xff]),
                    ],
                )?;

                assert_scan(
                    s.scan_prefix(b"\xff\xff\xff\xff"),
                    vec![(b"\xff\xff\xff\xff", vec![0xff, 0xff, 0xff, 0xff])],
                )?;

                assert_scan(s.scan_prefix(b"\xff\xff\xff\xff\xff"), vec![])?;

                Ok(())
            }

            #[test]
            /// Runs random operations both on a Engine and a known-good
            /// BTreeMap, comparing the results of each operation as well as the
            /// final state.
            fn random_ops() -> Result<()> {
                const NUM_OPS: u64 = 1000;

                use rand::{seq::SliceRandom, Rng, RngCore};
                let seed: u64 = rand::thread_rng().gen();
                let mut rng: rand::rngs::StdRng = rand::SeedableRng::seed_from_u64(seed);
                println!("seed = {}", seed);

                #[derive(Debug)]
                enum Op {
                    Set,
                    Delete,
                    Get,
                    Scan,
                }

                impl rand::distributions::Distribution<Op> for rand::distributions::Standard {
                    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> Op {
                        match rng.gen_range(0..=3) {
                            0 => Op::Set,
                            1 => Op::Delete,
                            2 => Op::Get,
                            3 => Op::Scan,
                            _ => panic!("unexpected value"),
                        }
                    }
                }

                let mut s = $setup;
                let mut keys: Vec<Vec<u8>> = Vec::new();
                let mut m = std::collections::BTreeMap::new();

                // Pick an already-used key with 80% probability, or generate a
                // new key.
                let mut random_key = |mut rng: &mut rand::rngs::StdRng| -> Vec<u8> {
                    if rng.gen::<f64>() < 0.8 && !keys.is_empty() {
                        keys.choose(&mut rng).unwrap().clone()
                    } else {
                        let mut key = vec![0; rng.gen_range(0..=16)];
                        rng.fill_bytes(&mut key);
                        keys.push(key.clone());
                        key
                    }
                };

                let random_value = |rng: &mut rand::rngs::StdRng| -> Vec<u8> {
                    let mut value = vec![0; rng.gen_range(0..=16)];
                    rng.fill_bytes(&mut value);
                    value
                };

                // Run random operations.
                for _ in 0..NUM_OPS {
                    match rng.gen::<Op>() {
                        Op::Set => {
                            let key = random_key(&mut rng);
                            let value = random_value(&mut rng);
                            println!("set {:?} = {:?}", key, value);
                            s.set(&key, value.clone())?;
                            m.insert(key, value);
                        }
                        Op::Delete => {
                            let key = random_key(&mut rng);
                            println!("delete {:?}", key);
                            s.delete(&key)?;
                            m.remove(&key);
                        }
                        Op::Get => {
                            let key = random_key(&mut rng);
                            let value = s.get(&key)?;
                            let expect = m.get(&key).cloned();
                            println!("get {:?} => {:?}", key, value);
                            assert_eq!(value, expect);
                        }
                        Op::Scan => {
                            let mut from = random_key(&mut rng);
                            let mut to = random_key(&mut rng);
                            if (to < from) {
                                (from, to) = (to, from)
                            }
                            println!("scan {:?} .. {:?}", from, to);
                            let result =
                                s.scan(from.clone()..to.clone()).collect::<Result<Vec<_>>>()?;
                            let expect = m
                                .range(from..to)
                                .map(|(k, v)| (k.clone(), v.clone()))
                                .collect::<Vec<_>>();
                            assert_eq!(result, expect);
                        }
                    }
                }

                // Compare the final states.
                println!("comparing final state");

                let state = s.scan(..).collect::<Result<Vec<_>>>()?;
                let expect = m
                    .range::<Vec<u8>, _>(..)
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect::<Vec<_>>();
                assert_eq!(state, expect);

                Ok(())
            }

            #[test]
            /// Tests implementation-independent aspects of Status.
            fn status() -> Result<()> {
                let mut s = $setup;
                s.set(b"foo", vec![1, 2, 3])?;
                s.set(b"bar", vec![1])?;
                s.delete(b"bar")?;
                s.set(b"baz", vec![1])?;
                s.set(b"baz", vec![2])?;
                s.set(b"baz", vec![3])?;
                s.delete(b"qux")?;

                let status = s.status()?;
                assert!(status.name.len() > 0);
                assert_eq!(status.keys, 2);
                assert_eq!(status.size, 10);

                Ok(())
            }
        };
    }

    pub(super) use test_engine; // export for use in submodules
}
