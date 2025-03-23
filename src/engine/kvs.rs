// #![deny(missing_docs)]

//! A simple in-memory key-value store library.
//!
//! You can store, query, and remove key value pair.
//!
//! # Examples
//!
//! ```
//! use kvs::KvStore;
//! let mut kvs = KvStore::new();
//! let k1 = String::from("jack");
//! let v1 = String::from("2024");
//! kvs.set(k1, v1);
//! assert_eq!(kvs.get(String::from("jack")), Some(String::from("2024")));
//! kvs.remove(String::from("jack"));
//! assert_eq!(kvs.get(String::from("jack")), None);
//! ```

use crate::error::KvsError;
use crate::error::Result;
use log::trace;
use serde::{Deserialize, Serialize};
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Seek, SeekFrom};
use std::path::PathBuf;
use std::{collections::HashMap, env, fs::File, io::Write};
use super::KvsEngine;

/// The maximum size of a kvs log.
/// Compact if exceeds.
const THRESHOLD: usize = 1024; // 1KB

/// A key value store
pub struct KvStore {
    // key to pointer of log
    store: HashMap<String, usize>,
    dir: PathBuf,
    log: File,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Op {
    Set { key: String, value: String },
    Rm { key: String },
}

impl KvsEngine for KvStore {
    /// Map `key` to `value` in the kv store
    ///
    /// # Examples
    ///
    /// ```
    /// use kvs::KvStore;
    /// let mut kvs = KvStore::new();
    /// kvs.set("jack".to_string(), "2024".to_string()).unwrap();
    /// ```
    fn set(&mut self, key: String, value: String) -> Result<()> {
        trace!("in kvs: set");
        let cur_op = Op::Set {
            key: key.clone(),
            value: value.clone(),
        };
        let serial = serde_json::to_string(&cur_op)?;
        let offset = self.log.seek(SeekFrom::End(0))?; // The length of the file
        self.log.write_all(serial.as_bytes())?;
        self.log.write_all(b"\n")?;
        self.store
            .entry(key)
            .and_modify(|v| *v = offset as usize)
            .or_insert(offset as usize);
        self.compact()
    }

    /// If `key` is in the kv store, return the `Some(value)`
    /// Otherwise, return `None`
    ///
    /// # Examples
    ///
    /// ```
    /// use kvs::KvStore;
    /// let mut kvs = KvStore::new();
    /// let k1 = String::from("jack");
    /// let k2 = String::from("jone");
    /// let v1 = String::from("2024");
    /// kvs.set(k1, v1);
    /// assert_eq!(kvs.get(String::from("jack")).unwrap(), Some(String::from("2024")));
    /// assert_eq!(kvs.get(k2).unwrap(), None);
    /// ```
    fn get(&mut self, key: String) -> Result<Option<String>> {
        if self.store.contains_key(&key) {
            let offset = *self.store.get(&key).unwrap();
            self.log.seek(SeekFrom::Start(offset as u64))?;

            let mut reader = BufReader::new(&self.log);
            let mut res = String::new();

            // only `BufReader` has read_line
            reader.read_line(&mut res)?;
            let op: Op = serde_json::from_str(&res)?;
            match op {
                Op::Set { key: _, value } => Ok(Some(value)),
                _ => Err(KvsError::KeyNotFound),
            }
        } else {
            Ok(None)
        }
    }

    /// If `key` is in the kv store, remove it
    /// Otherwise, do nothing
    ///
    /// # Examples
    ///
    /// ```
    /// use kvs::KvStore;
    /// let mut kvs = KvStore::new();
    /// let k1 = String::from("jack");
    /// let v1 = String::from("2024");
    /// kvs.set(k1, v1);
    /// assert_eq!(kvs.get(String::from("jack")), Some(String::from("2024")));
    /// kvs.rm(String::from("jack"));
    /// assert_eq!(kvs.get(String::from("jack")), None);
    /// ```
    fn remove(&mut self, key: String) -> Result<()> {
        if !self.store.contains_key(&key) {
            return Err(KvsError::KeyNotFound);
        }
        self.store.remove(&key).unwrap();
        let cur_op = Op::Rm { key };
        let serial = serde_json::to_string(&cur_op)?;
        self.log.write_all(serial.as_bytes())?;
        self.log.write_all(b"\n")?;
        self.compact()
    }

}

impl KvStore {
    /// Create a new KvStore
    /// The default path of `kvs.log` is the working directory
    /// `kvs.log` will be created if it does not exist
    ///
    /// # Examples
    ///
    /// ```
    /// use kvs::KvStore;
    /// let kvs = KvStore::new().unwrap();    
    /// ```
    pub fn new() -> Result<Self> {
        let cwd = env::current_dir()?;
        Self::open(cwd)
    }

    /// Create a new KvStorage
    /// Load `kvs.log` from the `path` directory
    /// `kvs.log` will be created if it does not exist
    ///
    /// # Examples
    ///
    /// ```
    /// use kvs::KvStore;
    /// let kvs = KvStore::path(env::current_dir().unwrap()).unwrap();
    /// ```
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let store = HashMap::new();
        let dir: PathBuf = path.into();
        let file = dir.join("kvs.log");
        let log = OpenOptions::new()
            .append(true)
            .create(true)
            .read(true)
            .open(file)?;
        let mut kvs = Self { store, dir, log };

        kvs.load()?;

        Ok(kvs)
    }

    /// Load the `kvs.log` into memory
    /// Update the `store` hashmap correspondingly
    fn load(&mut self) -> Result<()> {
        self.log.seek(std::io::SeekFrom::Start(0))?;
        let reader = BufReader::new(&self.log);

        // The starting pos (byte counts) of current line
        let mut offset = 0_usize;

        for line in reader.lines() {
            if line.is_err() {
                return Err(KvsError::LogLoadError);
            }
            let line = line.unwrap();
            let op: Op = serde_json::from_str(&line)?;
            match op {
                Op::Set { key, value: _ } => {
                    self.store.insert(key, offset);
                }
                Op::Rm { key } => {
                    self.store.remove(&key).unwrap();
                }
            }
            offset += line.len() + 1;
        }

        Ok(())
    }

    /// Compact the `kvs.log`
    /// Make sure every valid key just exist once
    fn compact(&mut self) -> Result<()> {
        let offset = self.log.seek(SeekFrom::End(0))?;
        if offset < THRESHOLD as u64 {
            return Ok(());
        }

        // In `self.store`, all live datas are there
        let mut new_file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(self.dir.join("tmp.log"))?;

        let mut writer = BufWriter::new(&new_file);

        self.log.seek(SeekFrom::Start(0))?;
        let mut reader = BufReader::new(&self.log);
        let mut cur = String::new();
        let mut cnt = 0_usize;

        for (_, offset) in self.store.iter_mut() {
            reader.seek(SeekFrom::Start(*offset as u64))?;
            reader.read_line(&mut cur)?;
            writer.write_all(cur.as_bytes())?;
            *offset = cnt;
            cnt += cur.len();
            cur.clear();
        }

        writer.flush()?;
        drop(writer);

        // modify the log and rename the new file
        self.log = new_file;
        // mem::swap(&mut self.log, &mut new_file);
        // drop(new_file);
        fs::remove_file(self.dir.join("kvs.log"))?;
        fs::rename(self.dir.join("tmp.log"), self.dir.join("kvs.log"))?;
        Ok(())
    }
}
