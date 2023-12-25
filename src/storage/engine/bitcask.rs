use super::{Engine, Status};
use crate::error::Result;

use fs4::FileExt;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

pub struct BitCask {
    /// The active append-only log file.
    log: Log,
    /// Maps keys to a value position and length in the log file.
    keydir: KeyDir,
}

/// Maps keys to a value position and length in the log file.
type KeyDir = std::collections::BTreeMap<Vec<u8>, (u64, u32)>;

impl BitCask {
    /// Compacts the current log file by writing out a new log file containing
    /// only live keys and replacing the current file with it. This operation is
    /// used to reclaim space from deleted entries (tombstones).
    /// Opens or creates a BitCask database in the given file.
    /// Opens or creates a BitCask database in the given directory.
    /// It initializes the log file and builds the key directory from it.
    pub fn new(path: PathBuf) -> Result<Self> {
        let mut log = Log::new(path)?;
        let keydir = log.build_keydir()?;
        Ok(Self { log, keydir })
    }

    /// Opens a BitCask database, and automatically compacts it if the amount
    /// of garbage exceeds the given ratio when opened.
    /// Opens a BitCask database, and automatically compacts it if the amount
    /// of garbage (i.e., space taken by deleted entries) exceeds the given ratio.
    /// This helps to reclaim disk space and improve read performance.
    pub fn new_compact(path: PathBuf, garbage_ratio_threshold: f64) -> Result<Self> {
        let mut s = Self::new(path)?;

        let status = s.status()?;
        let garbage_ratio = status.garbage_disk_size as f64 / status.total_disk_size as f64;
        if status.garbage_disk_size > 0 && garbage_ratio >= garbage_ratio_threshold {
            log::info!(
                "Compacting {} to remove {:.3}MB garbage ({:.0}% of {:.3}MB)",
                s.log.path.display(),
                status.garbage_disk_size / 1024 / 1024,
                garbage_ratio * 100.0,
                status.total_disk_size / 1024 / 1024
            );
            s.compact()?;
            log::info!(
                "Compacted {} to size {:.3}MB",
                s.log.path.display(),
                (status.total_disk_size - status.garbage_disk_size) / 1024 / 1024
            );
        }

        Ok(s)
    }
}

impl std::fmt::Display for BitCask {
    /// Provides a simple string representation of the BitCask engine.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "bitcask")
    }
}

impl Engine for BitCask {
    /// The iterator type used by the `scan` method to iterate over key-value pairs.
    type ScanIterator<'a> = ScanIterator<'a>;

    /// Deletes a key from the BitCask store. It appends a tombstone entry to the log file
    /// and removes the key from the key directory.
    fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.log.write_entry(key, None)?;
        self.keydir.remove(key);
        Ok(())
    }

    /// Flushes the log file to disk to ensure all written data is persisted.
    fn flush(&mut self) -> Result<()> {
        Ok(self.log.file.sync_all()?)
    }

    /// Retrieves a value for a given key from the BitCask store.
    /// It looks up the key in the key directory to find the position and length of the value
    /// in the log file and then reads the value from the log file.
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some((value_pos, value_len)) = self.keydir.get(key) {
            Ok(Some(self.log.read_value(*value_pos, *value_len)?))
        } else {
            Ok(None)
        }
    }

    /// Returns an iterator over the key-value pairs in the BitCask store that fall within
    /// the specified range. The iterator will yield keys in sorted order as per the BTreeMap.
    fn scan<R: std::ops::RangeBounds<Vec<u8>>>(&mut self, range: R) -> Self::ScanIterator<'_> {
        ScanIterator { inner: self.keydir.range(range), log: &mut self.log }
    }

    /// Sets a key to a value in the BitCask store. It appends a new entry to the log file
    /// and updates the key directory with the position and length of the new value.
    fn set(&mut self, key: &[u8], value: Vec<u8>) -> Result<()> {
        let (pos, len) = self.log.write_entry(key, Some(&*value))?;
        let value_len = value.len() as u32;
        self.keydir.insert(key.to_vec(), (pos + len as u64 - value_len as u64, value_len));
        Ok(())
    }

    /// Retrieves the status of the BitCask store, including the number of keys, the size of
    /// live data, and the amount of garbage data. This is useful for monitoring and
    /// administrative purposes.
    fn status(&mut self) -> Result<Status> {
        let keys = self.keydir.len() as u64;
        let size = self
            .keydir
            .iter()
            .fold(0, |size, (key, (_, value_len))| size + key.len() as u64 + *value_len as u64);
        let total_disk_size = self.log.file.metadata()?.len();
        let live_disk_size = size + 8 * keys; // account for length prefixes
        let garbage_disk_size = total_disk_size - live_disk_size;
        Ok(Status {
            name: self.to_string(),
            keys,
            size,
            total_disk_size,
            live_disk_size,
            garbage_disk_size,
        })
    }
}

pub struct ScanIterator<'a> {
    /// An iterator over a range of keys in the key directory.
    /// It holds a reference to the log file to read values for the keys as they are iterated.
    inner: std::collections::btree_map::Range<'a, Vec<u8>, (u64, u32)>,
    log: &'a mut Log,
}

impl<'a> ScanIterator<'a> {
    /// Maps a key and its associated value position and length to the actual key-value pair.
    /// It reads the value from the log file based on the position and length.
    fn map(&mut self, item: (&Vec<u8>, &(u64, u32))) -> <Self as Iterator>::Item {
        let (key, (value_pos, value_len)) = item;
        Ok((key.clone(), self.log.read_value(*value_pos, *value_len)?))
    }
}

impl<'a> Iterator for ScanIterator<'a> {
    /// Advances the iterator and returns the next key-value pair.
    /// It uses the `map` method to convert the key and value position from the key directory
    /// into an actual key-value pair by reading the value from the log file.
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|item| self.map(item))
    }
}

impl<'a> DoubleEndedIterator for ScanIterator<'a> {
    /// Advances the iterator in reverse and returns the previous key-value pair.
    /// It uses the `map` method similar to `next` but iterates from the end towards the beginning.
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back().map(|item| self.map(item))
    }
}

impl BitCask {
    /// Compacts the current log file by writing out a new log file containing
    /// only live keys and replacing the current file with it. This operation is
    /// used to reclaim space from deleted entries (tombstones).
    /// Compacts the current log file by writing out a new log file containing
    /// only live keys and replacing the current file with it.
    pub fn compact(&mut self) -> Result<()> {
        let mut tmp_path = self.log.path.clone();
        tmp_path.set_extension("new");
        let (mut new_log, new_keydir) = self.write_log(tmp_path)?;

        std::fs::rename(&new_log.path, &self.log.path)?;
        new_log.path = self.log.path.clone();

        self.log = new_log;
        self.keydir = new_keydir;
        Ok(())
    }

    /// Writes out a new log file with the live entries of the current log file
    /// and returns it along with its keydir. Entries are written in key order.
    /// Writes out a new log file with the live entries of the current log file
    /// and returns it along with its key directory. Entries are written in key order.
    /// This is used as part of the compaction process.
    fn write_log(&mut self, path: PathBuf) -> Result<(Log, KeyDir)> {
        let mut new_keydir = KeyDir::new();
        let mut new_log = Log::new(path)?;
        new_log.file.set_len(0)?; // truncate file if it exists
        for (key, (value_pos, value_len)) in self.keydir.iter() {
            let value = self.log.read_value(*value_pos, *value_len)?;
            let (pos, len) = new_log.write_entry(key, Some(&value))?;
            new_keydir.insert(key.clone(), (pos + len as u64 - *value_len as u64, *value_len));
        }
        Ok((new_log, new_keydir))
    }
}

/// Attempt to flush the file when the database is closed.
impl Drop for BitCask {
    /// Ensures that the log file is flushed to disk when the BitCask instance is dropped.
    /// This is important to prevent data loss in case the program terminates unexpectedly.
    fn drop(&mut self) {
        if let Err(error) = self.flush() {
            log::error!("failed to flush file: {}", error)
        }
    }
}

struct Log {
    /// Path to the log file.
    path: PathBuf,
    /// The opened file containing the log.
    file: std::fs::File,
}

impl Log {
    /// Opens a log file, or creates one if it does not exist. Takes out an
    /// exclusive lock on the file until it is closed, or errors if the lock is
    /// already held. This ensures that only one BitCask instance can write to the
    /// log file at a time.
    /// Opens a log file, or creates one if it does not exist. Takes out an
    /// exclusive lock on the file until it is closed, or errors if the lock is
    /// already held.
    fn new(path: PathBuf) -> Result<Self> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?
        }
        let file = std::fs::OpenOptions::new().read(true).write(true).create(true).open(&path)?;
        file.try_lock_exclusive()?;
        Ok(Self { path, file })
    }

    /// Builds a keydir by scanning the log file. If an incomplete entry is
    /// encountered, it is assumed to be caused by an incomplete write operation
    /// and the remainder of the file is truncated.
    /// Builds a key directory by scanning the log file. If an incomplete entry is
    /// encountered, it is assumed to be caused by an incomplete write operation
    /// and the remainder of the file is truncated. This is part of the recovery
    /// process when opening a BitCask database.
    fn build_keydir(&mut self) -> Result<KeyDir> {
        let mut len_buf = [0u8; 4];
        let mut keydir = KeyDir::new();
        let file_len = self.file.metadata()?.len();
        let mut r = BufReader::new(&mut self.file);
        let mut pos = r.seek(SeekFrom::Start(0))?;

        while pos < file_len {
            // Read the next entry from the file, returning the key, value
            // position, and value length or None for tombstones.
            let result = || -> std::result::Result<(Vec<u8>, u64, Option<u32>), std::io::Error> {
                r.read_exact(&mut len_buf)?;
                let key_len = u32::from_be_bytes(len_buf);
                r.read_exact(&mut len_buf)?;
                let value_len_or_tombstone = match i32::from_be_bytes(len_buf) {
                    l if l >= 0 => Some(l as u32),
                    _ => None, // -1 for tombstones
                };
                let value_pos = pos + 4 + 4 + key_len as u64;

                let mut key = vec![0; key_len as usize];
                r.read_exact(&mut key)?;

                if let Some(value_len) = value_len_or_tombstone {
                    if value_pos + value_len as u64 > file_len {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            "value extends beyond end of file",
                        ));
                    }
                    r.seek_relative(value_len as i64)?; // avoids discarding buffer
                }

                Ok((key, value_pos, value_len_or_tombstone))
            }();

            match result {
                // Populate the keydir with the entry, or remove it on tombstones.
                Ok((key, value_pos, Some(value_len))) => {
                    keydir.insert(key, (value_pos, value_len));
                    pos = value_pos + value_len as u64;
                }
                Ok((key, value_pos, None)) => {
                    keydir.remove(&key);
                    pos = value_pos;
                }
                // If an incomplete entry was found at the end of the file, assume an
                // incomplete write and truncate the file.
                Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
                    log::error!("Found incomplete entry at offset {}, truncating file", pos);
                    self.file.set_len(pos)?;
                    break;
                }
                Err(err) => return Err(err.into()),
            }
        }

        Ok(keydir)
    }

    /// Reads a value from the log file.
    /// Reads a value from the log file. It seeks to the position of the value and
    /// reads the number of bytes specified by the value length. This is used by the
    /// `BitCask` struct to retrieve values for keys.
    fn read_value(&mut self, value_pos: u64, value_len: u32) -> Result<Vec<u8>> {
        let mut value = vec![0; value_len as usize];
        self.file.seek(SeekFrom::Start(value_pos))?;
        self.file.read_exact(&mut value)?;
        Ok(value)
    }

    /// Appends a key/value entry to the log file, using a None value for
    /// tombstones. It returns the position and length of the entry.
    /// Appends a key/value entry to the log file, using a None value for
    /// tombstones. It returns the position and length of the entry. This is used by
    /// the `BitCask` struct to write new entries and delete existing ones.
    fn write_entry(&mut self, key: &[u8], value: Option<&[u8]>) -> Result<(u64, u32)> {
        let key_len = key.len() as u32;
        let value_len = value.map_or(0, |v| v.len() as u32);
        let value_len_or_tombstone = value.map_or(-1, |v| v.len() as i32);
        let len = 4 + 4 + key_len + value_len;

        let pos = self.file.seek(SeekFrom::End(0))?;
        let mut w = BufWriter::with_capacity(len as usize, &mut self.file);
        w.write_all(&key_len.to_be_bytes())?;
        w.write_all(&value_len_or_tombstone.to_be_bytes())?;
        w.write_all(key)?;
        if let Some(value) = value {
            w.write_all(value)?;
        }
        w.flush()?;

        Ok((pos, len))
    }

    #[cfg(test)]
    /// Prints the entire log file to the given writer in human-readable form.
    fn print<W: Write>(&mut self, w: &mut W) -> Result<()> {
        let mut len_buf = [0u8; 4];
        let file_len = self.file.metadata()?.len();
        let mut r = BufReader::new(&mut self.file);
        let mut pos = r.seek(SeekFrom::Start(0))?;
        let mut idx = 0;

        while pos < file_len {
            writeln!(w, "entry = {}, offset {}", idx, pos)?;

            r.read_exact(&mut len_buf)?;
            let key_len = u32::from_be_bytes(len_buf);
            writeln!(w, "klen  = {} {:x?}", key_len, len_buf)?;

            r.read_exact(&mut len_buf)?;
            let value_len_or_tombstone = i32::from_be_bytes(len_buf); // NB: -1 for tombstones
            let value_len = value_len_or_tombstone.max(0) as u32;
            writeln!(w, "vlen  = {} {:x?}", value_len_or_tombstone, len_buf)?;

            let mut key = vec![0; key_len as usize];
            r.read_exact(&mut key)?;
            write!(w, "key   = ")?;
            if let Ok(str) = std::str::from_utf8(&key) {
                write!(w, r#""{}" "#, str)?;
            }
            writeln!(w, "{:x?}", key)?;

            let mut value = vec![0; value_len as usize];
            r.read_exact(&mut value)?;
            write!(w, "value = ")?;
            if value_len_or_tombstone < 0 {
                write!(w, "tombstone ")?;
            } else if let Ok(str) = std::str::from_utf8(&value) {
                if str.chars().all(|c| !c.is_control()) {
                    write!(w, r#""{}" "#, str)?;
                }
            }
            write!(w, "{:x?}\n\n", value)?;

            pos += 4 + 4 + key_len as u64 + value_len as u64;
            idx += 1;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    /// This module contains tests for the BitCask storage engine. It includes tests for
    /// log file writing and reading, log compaction, file locking, recovery from incomplete
    /// writes, and the status method.
    use super::*;

    const GOLDEN_DIR: &str = "src/storage/golden/bitcask";

    super::super::tests::test_engine!({
        let path = tempdir::TempDir::new("entangledb")?.path().join("entangledb");
        BitCask::new(path)?
    });

    /// Creates a new BitCask engine for testing.
    fn setup() -> Result<BitCask> {
        BitCask::new(tempdir::TempDir::new("entangledb")?.path().join("entangledb"))
    }

    fn setup_log(s: &mut BitCask) -> Result<()> {
        s.set(b"b", vec![0x01])?;
        s.set(b"b", vec![0x02])?;

        s.set(b"e", vec![0x05])?;
        s.delete(b"e")?;

        s.set(b"c", vec![0x00])?;
        s.delete(b"c")?;
        s.set(b"c", vec![0x03])?;

        s.set(b"", vec![])?;

        s.set(b"a", vec![0x01])?;

        s.delete(b"f")?;

        s.delete(b"d")?;
        s.set(b"d", vec![0x04])?;

        // Make sure the scan yields the expected results.
        assert_eq!(
            vec![
                (b"".to_vec(), vec![]),
                (b"a".to_vec(), vec![0x01]),
                (b"b".to_vec(), vec![0x02]),
                (b"c".to_vec(), vec![0x03]),
                (b"d".to_vec(), vec![0x04]),
            ],
            s.scan(..).collect::<Result<Vec<_>>>()?,
        );

        Ok(())
    }

    #[test]
    /// Tests that logs are written correctly using a golden file.
    fn log() -> Result<()> {
        let mut s = setup()?;
        setup_log(&mut s)?;

        let mut mint = goldenfile::Mint::new(GOLDEN_DIR);
        s.log.print(&mut mint.new_goldenfile("log")?)?;
        Ok(())
    }

    #[test]
    /// Tests that writing and then reading a file yields the same results.
    fn reopen() -> Result<()> {
        // NB: Don't use setup(), because the tempdir will be removed when
        // the path falls out of scope.
        let path = tempdir::TempDir::new("entangledb")?.path().join("entangledb");
        let mut s = BitCask::new(path.clone())?;
        setup_log(&mut s)?;

        let expect = s.scan(..).collect::<Result<Vec<_>>>()?;
        drop(s);
        let mut s = BitCask::new(path)?;
        assert_eq!(expect, s.scan(..).collect::<Result<Vec<_>>>()?,);

        Ok(())
    }

    #[test]
    /// Tests log compaction, by writing golden files of the before/after state,
    /// and checking that the database contains the same results, even after
    /// reopening the file.
    fn compact() -> Result<()> {
        // NB: Don't use setup(), because the tempdir will be removed when
        // the path falls out of scope.
        let path = tempdir::TempDir::new("entangledb")?.path().join("entangledb");
        let mut s = BitCask::new(path.clone())?;
        setup_log(&mut s)?;

        // Dump the initial log file.
        let mut mint = goldenfile::Mint::new(GOLDEN_DIR);
        s.log.print(&mut mint.new_goldenfile("compact-before")?)?;
        let expect = s.scan(..).collect::<Result<Vec<_>>>()?;

        // Compact the log file and assert the new log file contents.
        s.compact()?;
        assert_eq!(path, s.log.path);
        assert_eq!(expect, s.scan(..).collect::<Result<Vec<_>>>()?,);
        s.log.print(&mut mint.new_goldenfile("compact-after")?)?;

        // Reopen the log file and assert that the contents are the same.
        drop(s);
        let mut s = BitCask::new(path)?;
        assert_eq!(expect, s.scan(..).collect::<Result<Vec<_>>>()?,);

        Ok(())
    }

    #[test]
    /// Tests that new_compact() will automatically compact the file when appropriate.
    fn new_compact() -> Result<()> {
        // Create an initial log file with a few entries.
        let dir = tempdir::TempDir::new("entangledb")?;
        let path = dir.path().join("orig");
        let compactpath = dir.path().join("compact");

        let mut s = BitCask::new_compact(path.clone(), 0.2)?;
        setup_log(&mut s)?;
        let status = s.status()?;
        let garbage_ratio = status.garbage_disk_size as f64 / status.total_disk_size as f64;
        drop(s);

        // Test a few threshold value and assert whether it should trigger compaction.
        let cases = vec![
            (-1.0, true),
            (0.0, true),
            (garbage_ratio - 0.001, true),
            (garbage_ratio, true),
            (garbage_ratio + 0.001, false),
            (1.0, false),
            (2.0, false),
        ];
        for (threshold, expect_compact) in cases.into_iter() {
            std::fs::copy(&path, &compactpath)?;
            let mut s = BitCask::new_compact(compactpath.clone(), threshold)?;
            let new_status = s.status()?;
            assert_eq!(new_status.live_disk_size, status.live_disk_size);
            if expect_compact {
                assert_eq!(new_status.total_disk_size, status.live_disk_size);
                assert_eq!(new_status.garbage_disk_size, 0);
            } else {
                assert_eq!(new_status, status);
            }
        }

        Ok(())
    }

    #[test]
    /// Tests that exclusive locks are taken out on log files, released when the
    /// database is closed, and that an error is returned if a lock is already
    /// held.
    fn log_lock() -> Result<()> {
        let path = tempdir::TempDir::new("entangledb")?.path().join("entangledb");
        let s = BitCask::new(path.clone())?;

        assert!(BitCask::new(path.clone()).is_err());
        drop(s);
        assert!(BitCask::new(path.clone()).is_ok());

        Ok(())
    }

    #[test]
    /// Tests that an incomplete write at the end of the log file can be
    /// recovered by discarding the last entry.
    fn recovery() -> Result<()> {
        // Create an initial log file with a few entries.
        let dir = tempdir::TempDir::new("entangledb")?;
        let path = dir.path().join("complete");
        let truncpath = dir.path().join("truncated");

        let mut log = Log::new(path.clone())?;
        let mut ends = vec![];

        let (pos, len) = log.write_entry("deleted".as_bytes(), Some(&[1, 2, 3]))?;
        ends.push(pos + len as u64);

        let (pos, len) = log.write_entry("deleted".as_bytes(), None)?;
        ends.push(pos + len as u64);

        let (pos, len) = log.write_entry(&[], Some(&[]))?;
        ends.push(pos + len as u64);

        let (pos, len) = log.write_entry("key".as_bytes(), Some(&[1, 2, 3, 4, 5]))?;
        ends.push(pos + len as u64);

        drop(log);

        // Copy the file, and truncate it at each byte, then try to open it
        // and assert that we always retain a prefix of entries.
        let size = std::fs::metadata(&path)?.len();
        for pos in 0..=size {
            std::fs::copy(&path, &truncpath)?;
            let f = std::fs::OpenOptions::new().write(true).open(&truncpath)?;
            f.set_len(pos)?;
            drop(f);

            let mut expect = vec![];
            if pos >= ends[0] {
                expect.push((b"deleted".to_vec(), vec![1, 2, 3]))
            }
            if pos >= ends[1] {
                expect.pop(); // "deleted" key removed
            }
            if pos >= ends[2] {
                expect.push((b"".to_vec(), vec![]))
            }
            if pos >= ends[3] {
                expect.push((b"key".to_vec(), vec![1, 2, 3, 4, 5]))
            }

            let mut s = BitCask::new(truncpath.clone())?;
            assert_eq!(expect, s.scan(..).collect::<Result<Vec<_>>>()?);
        }

        Ok(())
    }

    #[test]
    /// Tests status(), both for a log file with known garbage, and
    /// after compacting it when the live size must equal the file size.
    fn status_full() -> Result<()> {
        let mut s = setup()?;
        setup_log(&mut s)?;

        // Before compaction.
        assert_eq!(
            s.status()?,
            Status {
                name: "bitcask".to_string(),
                keys: 5,
                size: 8,
                total_disk_size: 114,
                live_disk_size: 48,
                garbage_disk_size: 66
            }
        );

        // After compaction.
        s.compact()?;
        assert_eq!(
            s.status()?,
            Status {
                name: "bitcask".to_string(),
                keys: 5,
                size: 8,
                total_disk_size: 48,
                live_disk_size: 48,
                garbage_disk_size: 0,
            }
        );

        Ok(())
    }
}
