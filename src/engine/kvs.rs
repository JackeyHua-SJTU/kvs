//! A simple in-memory key-value store library using BitCask algorithm.
//!
//! You can store, query, and remove key value pair.
//!

/// BitCask Config
///
/// All log is in `log/` sub dir
///
/// Active log will be written into a `active.log`. Append only. Flush if exceed the threshold.
/// After that, it will be renamed into `<version>.log`, and will be read-only
///
/// When the size of old log reaches `compact threshold`, all old logs will be merged and
/// produce a new old log. 
/// 
/// In this implementation, compact happens after flush. So when compact happens, there will be
/// no active data. We can merge all log files into one.
///
/// We need to assign each old log a version, so that we can find it
///
use super::KvsEngine;
use crate::error::KvsError;
use crate::error::Result;
use log::trace;
use serde::{Deserialize, Serialize};
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Seek, SeekFrom};
use std::path::PathBuf;
use std::{
    collections::{BTreeMap, HashMap},
    env,
    fs::File,
    io::Write,
};

/// The maximum size of sum of size of old logs
/// Compact happens in init stage, i.e. offline compaction
const THRESHOLD: usize = 1024 * 1024; // 1GB
const ACTIVE_THRESHOLD: usize = 1024; // 32KB

/// A key value store
pub struct KvStore {
    // base directory
    dir: PathBuf,
    // version to reader of `<version>.log`
    ver_to_file: HashMap<usize, BufReader<File>>,
    // writer to `active.log`
    writer: BufWriter<File>,
    current_ver: usize,
    current_len: usize,
    old_log_len: usize,
    entry_to_index: BTreeMap<String, InMemIndex>,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Op {
    Set { key: String, value: String },
    Rm { key: String },
}

struct InMemIndex {
    version: usize,
    start_pos: usize,
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
            value,
        };
        let mut serial = serde_json::to_string(&cur_op)?;
        serial.push('\n');
        self.current_len += serial.len();
        let pos = self.writer.seek(SeekFrom::End(0))? as usize;
        self.writer.write_all(serial.as_bytes())?;
        self.writer.flush()?;
        self.entry_to_index
            .entry(key)
            .and_modify(|cur| {
                cur.version = self.current_ver;
                cur.start_pos = pos;
            })
            .or_insert(InMemIndex {
                version: self.current_ver,
                start_pos: pos,
            });
        self.to_flush()
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
        if self.entry_to_index.contains_key(&key) {
            let metainfo = self.entry_to_index.get(&key).unwrap();
            let reader = self.ver_to_file.get_mut(&metainfo.version).unwrap();
            reader.seek(SeekFrom::Start(metainfo.start_pos as u64))?;
            let mut result = String::new();
            reader.read_line(&mut result)?;
            let op: Op = serde_json::from_str(&result)?;
            match op {
                Op::Rm { key: _ } => Err(KvsError::UnexpectedType),
                Op::Set { key: _, value } => Ok(Some(value)),
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
        if !self.entry_to_index.contains_key(&key) {
            return Err(KvsError::KeyNotFound);
        }
        self.entry_to_index.remove(&key).unwrap();
        let cur_op = Op::Rm { key };
        let mut serial = serde_json::to_string(&cur_op)?;
        serial.push('\n');
        self.current_len += serial.len();
        self.writer.write_all(serial.as_bytes())?;
        self.writer.flush()?;
        self.to_flush()
    }
}

impl Drop for KvStore {
    fn drop(&mut self) {
        self.flush(true).unwrap();
    }
}

impl KvStore {
    /// Create a new KvStore using default directory
    ///
    /// # Examples
    ///
    /// ```
    /// use kvs::engine::kvs::KvStore;
    /// let kvs = KvStore::new().unwrap();    
    /// ```
    pub fn new() -> Result<Self> {
        let cwd = env::current_dir()?;
        Self::open(cwd)
    }

    /// Create a new KvStorage with given directory
    ///
    /// # Examples
    ///
    /// ```
    /// use kvs::engine::kvs::KvStore;
    /// use std::env;
    /// let kvs = KvStore::open(env::current_dir().unwrap()).unwrap();
    /// ```
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let mut ver_to_file: HashMap<usize, BufReader<File>> = HashMap::new();
        let dir: PathBuf = path.into();
        let log_subdir = dir.join("log");

        if !log_subdir.exists() {
            trace!("Create a directory {:?}", log_subdir);
            fs::create_dir(&log_subdir)?;
        }

        let mut max_old_version = 0;

        let mut total_len = 0_u64;
        let mut version_list = Vec::new();

        for file in fs::read_dir(&log_subdir)? {
            let file = file?;
            trace!("Read a file {:?}", file.file_name());
            let open_file = OpenOptions::new().read(true).open(file.path())?;
            total_len += open_file.metadata().unwrap().len();
            let path = file.path();
            assert!(path.is_file());
            let name = path
                .file_name()
                .expect("The name is invalid")
                .to_str()
                .unwrap();
            let cur_ver: usize = name
                .split(".")
                .take(1)
                .next()
                .expect("Fail to get the version part of an old log file")
                .parse()?;
            trace!("current file has version {}", cur_ver);
            max_old_version = max_old_version.max(cur_ver);
            version_list.push(cur_ver);
            ver_to_file.insert(cur_ver, BufReader::new(open_file));
        }

        version_list.sort_unstable();

        let mut entry_to_index: BTreeMap<String, InMemIndex> = BTreeMap::new();

        for v in version_list.iter() {
            let reader = BufReader::new(ver_to_file.get(v).unwrap().get_ref().try_clone()?);
            let mut offset = 0_usize;

            for line in reader.lines() {
                match line {
                    Ok(s) => {
                        let op: Op = serde_json::from_str(&s)?;
                        match op {
                            Op::Set { key, value: _ } => {
                                entry_to_index
                                    .entry(key)
                                    .and_modify(|cur| {
                                        cur.version = *v;
                                        cur.start_pos = offset;
                                    })
                                    .or_insert(InMemIndex {
                                        version: *v,
                                        start_pos: offset,
                                    });
                            }
                            Op::Rm { key } => {
                                entry_to_index
                                    .remove(&key)
                                    .expect("remove an invalid key from a map");
                            }
                        }
                        offset += s.len() + 1;
                    }
                    Err(e) => {
                        return Err(e.into());
                    }
                }
            }
        }

        max_old_version += 1;

        let cur_file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(log_subdir.join(format!("{}.log", max_old_version)))?;
        trace!("Create a new active log");
        let reader = BufReader::new(cur_file.try_clone()?);
        let writer = BufWriter::new(cur_file);
        ver_to_file.insert(max_old_version, reader);

        Ok(Self {
            dir,
            ver_to_file,
            writer,
            current_ver: max_old_version,
            current_len: 0,
            old_log_len: total_len as usize,
            entry_to_index,
        })
    }

    /// Flush a full active log into disk
    /// Rename it, and open a new active log
    fn flush(&mut self, drop: bool) -> Result<()> {
        self.writer.flush()?;
        // let log_subdir = self.dir.join("log");
        // fs::rename(log_subdir.join(ACTIVE_LOG), log_subdir.join(format!("{}.log", self.current_ver)))?;
        self.old_log_len += self.writer.get_mut().metadata().unwrap().len() as usize;
        self.current_len = 0;
        if self.old_log_len >= THRESHOLD {
            self.compact()?;
        }

        if !drop {
            self.current_ver += 1;
            let cur_file = OpenOptions::new()
                .create(true)
                .append(true)
                .read(true)
                .open(self.dir.join(format!("log/{}.log", self.current_ver)))?;

            let reader = BufReader::new(cur_file.try_clone()?);
            let writer = BufWriter::new(cur_file);
            self.ver_to_file.insert(self.current_ver, reader);
            self.writer = writer;
        }
        Ok(())
    }

    /// Wrapper on whether to flush the active log or not
    fn to_flush(&mut self) -> Result<()> {
        if self.current_len > ACTIVE_THRESHOLD {
            self.flush(false)
        } else {
            Ok(())
        }
    }

    /// compact all old log into one
    fn compact(&mut self) -> Result<()> {
        trace!("compact begins");
        self.current_ver += 1;
        let new_log = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(self.dir.join(format! {"log/{}.log", self.current_ver}))?;
        let reader = BufReader::new(new_log.try_clone()?);
        let mut writer = BufWriter::new(new_log);
        let mut dict: HashMap<String, String> = HashMap::new();
        let mut list: Vec<_> = self.ver_to_file.drain().collect();
        list.sort_by(|a, b| a.0.cmp(&b.0));

        for (ver, mut cur_reader) in list.into_iter() {
            trace!("current log version is {}", ver);
            cur_reader.seek(SeekFrom::Start(0))?;
            for line in cur_reader.lines() {
                match line {
                    Ok(s) => {
                        let op: Op = serde_json::from_str(&s)?;
                        match op {
                            Op::Set { key, value } => {
                                trace!("set {} to {}", key, value);
                                dict.insert(key, value);
                            }
                            Op::Rm { key } => {
                                trace!("remove {}", key);
                                dict.remove(&key).unwrap();
                            }
                        }
                    }
                    Err(e) => return Err(e.into()),
                }
            }

            fs::remove_file(self.dir.join(format!("log/{}.log", ver)))?;
        }

        let mut offset = 0_usize;
        self.entry_to_index.clear();
        for (k, v) in dict.into_iter() {
            self.entry_to_index.insert(
                k.clone(),
                InMemIndex {
                    version: self.current_ver,
                    start_pos: offset,
                },
            );
            let op = Op::Set { key: k, value: v };
            let info = serde_json::to_string(&op)?;
            writer.write_all(info.as_bytes())?;
            writer.write_all(b"\n")?;
            offset += info.len() + 1;
        }
        writer.flush()?;

        self.old_log_len = offset;

        self.ver_to_file.insert(self.current_ver, reader);
        Ok(())
    }
}
