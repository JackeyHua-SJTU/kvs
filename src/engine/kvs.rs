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
use std::cell::RefCell;
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::RwLock;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::{
    collections::{BTreeMap, HashMap},
    env,
    fs::File,
    io::Write,
    sync::{Arc, Mutex},
};

/// The maximum size of sum of size of old logs
/// Compact happens in init stage, i.e. offline compaction
const THRESHOLD: usize = 40 * 1024; // 1GB
const ACTIVE_THRESHOLD: usize = 1024; // 32KB

/// Rust thread spawn requires FnOnce(), therefore if we distribute each TCP connection
/// to a corresponding thread, we need to clone a KvStore object. Some data should
/// be shared, while others can be self-owned.

/// dir - Since it is read only, just put it inside a Arc is enough
/// writer - Write operation should be exclusive. Arc<Mutex>, enforcing one instance
/// entry_to_index - Must ensure one instance, Arc<Mutex>
///                     Read operation can be parallel.
///                     Write should be exclusive. ==> Arc<RwLock> should be better
///                  A step further. We can encapsulate the value inside a RwLock.
///                     We only acquire the write lock of the hashmap when we need
///                     to `compact`. We then acquire the R/W lock for value holding
///                     the read lock of the hashmap.
/// ver_to_file - only used in `get` and `compact`. One key observation is that the map
///                 may not be synced. Each kvstore can have its own map. Better read
///                 perf. We can use a version atomic to periodically remove outdated entry. (lazy clean)

/// A smart design pattern. Separate reader from writer. Reader can be parallelized. Writers
/// share the same instance.
///
/// Set/Remove - First get the writer lock, then the entry_to_index lock

/// A key value store
#[derive(Clone)]
pub struct KvStore {
    // base directory
    dir: Arc<PathBuf>,
    // there is only one writer in essence
    kv_writer: Arc<Mutex<KvStoreWriter>>,
    // every kv store has its own reader
    kv_reader: KvStoreReader,
    // used in get
    entry_to_index: Arc<RwLock<BTreeMap<String, RwLock<InMemIndex>>>>,
}

pub struct KvStoreReader {
    dir: Arc<PathBuf>,
    min_version: Arc<AtomicU32>,
    ver_to_file: RefCell<HashMap<usize, BufReader<File>>>,
}

impl Clone for KvStoreReader {
    fn clone(&self) -> Self {
        Self {
            dir: Arc::clone(&self.dir),
            min_version: Arc::clone(&self.min_version),
            ver_to_file: RefCell::new(HashMap::new()),
        }
    }
}

impl KvStoreReader {
    /// KvStore Reader will be created after the writer
    pub fn new(
        dir: Arc<PathBuf>,
        min_version: Arc<AtomicU32>,
        ver_to_file: HashMap<usize, BufReader<File>>,
    ) -> Result<Self> {
        Ok(Self {
            dir,
            min_version,
            ver_to_file: RefCell::new(ver_to_file),
        })
    }

    pub fn get(&self, index: InMemIndex) -> Result<String> {
        self.clean()?;
        let flag = self.ver_to_file.borrow().contains_key(&index.version);
        let mut ans = String::new();

        let mut reader = self.ver_to_file.borrow_mut();

        if flag {
            let reader = reader.get_mut(&index.version).unwrap();
            reader.seek(SeekFrom::Start(index.start_pos as u64))?;
            reader.read_line(&mut ans)?;
        } else {
            let mut cur_reader = self.load(index.version)?;
            cur_reader.seek(SeekFrom::Start(index.start_pos as u64))?;
            cur_reader.read_line(&mut ans)?;
            reader.insert(index.version, cur_reader);
        }
        let op = serde_json::from_str(&ans)?;
        match op {
            Op::Rm { key: _ } => Err(KvsError::UnexpectedType),
            Op::Set { key: _, value } => Ok(value),
        }
    }

    /// load log/`id`.log into self.ver_to_file
    fn load(&self, id: usize) -> Result<BufReader<File>> {
        let path = self.dir.join(format!("log/{}.log", id));
        let file = OpenOptions::new().read(true).open(path)?;
        let reader = BufReader::new(file);

        Ok(reader)
    }

    fn clean(&self) -> Result<()> {
        let mut mp = self.ver_to_file.borrow_mut();
        let mut vc = Vec::new();

        let version = self.min_version.load(Ordering::SeqCst) as usize;

        for &k in mp.keys() {
            if k < version {
                vc.push(k);
            }
        }

        for k in vc {
            mp.remove(&k);
        }

        Ok(())
    }
}

pub struct KvStoreWriter {
    min_version: Arc<AtomicU32>,
    entry_to_index: Arc<RwLock<BTreeMap<String, RwLock<InMemIndex>>>>,
    current_ver: usize,
    current_len: usize,
    old_log_len: usize,
    dir: Arc<PathBuf>,
    writer: BufWriter<File>,
}

impl KvStoreWriter {
    fn traverse_dir(dir: &PathBuf) -> Result<(HashMap<usize, BufReader<File>>, Vec<usize>, u64)> {
        let mut ver_to_file = HashMap::new();
        let mut version_list = Vec::new();
        let mut total_len = 0;
        for file in fs::read_dir(&dir)? {
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
            version_list.push(cur_ver);
            ver_to_file.insert(cur_ver, BufReader::new(open_file));
        }
        version_list.sort_unstable();
        Ok((ver_to_file, version_list, total_len))
    }

    pub fn new(
        path: impl Into<PathBuf>,
        ver_to_file: &mut HashMap<usize, BufReader<File>>,
    ) -> Result<Self> {
        let path: PathBuf = path.into();
        let log_subdir = path.join("log");

        if !log_subdir.exists() {
            trace!("Create a directory {:?}", log_subdir);
            fs::create_dir(&log_subdir)?;
        }

        let mut max_old_version = 0;

        let (mut v_to_f, version_list, total_len) = Self::traverse_dir(&log_subdir)?;

        if !version_list.is_empty() {
            max_old_version = *version_list.last().unwrap();
        }

        let mut entry_to_index: BTreeMap<String, RwLock<InMemIndex>> = BTreeMap::new();

        for v in version_list.iter() {
            let reader = BufReader::new(v_to_f.get(v).unwrap().get_ref().try_clone()?);
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
                                        let cur = cur.get_mut().expect(
                                            "Fail to get the RwLock instance in entry to index",
                                        );
                                        cur.version = *v;
                                        cur.start_pos = offset;
                                    })
                                    .or_insert(RwLock::new(InMemIndex {
                                        version: *v,
                                        start_pos: offset,
                                    }));
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
        v_to_f.insert(max_old_version, reader);

        *ver_to_file = v_to_f;

        Ok(Self {
            min_version: Arc::new(AtomicU32::new(0)),
            entry_to_index: Arc::new(RwLock::new(entry_to_index)),
            current_ver: max_old_version,
            current_len: 0,
            old_log_len: total_len as usize,
            dir: Arc::new(path),
            writer,
        })
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let op: Op = Op::Set {
            key: key.clone(),
            value,
        };
        let mut serial = serde_json::to_string(&op)?;
        serial.push('\n');
        self.current_len += serial.len();
        let pos = self.writer.seek(SeekFrom::End(0))? as usize;
        self.writer.write_all(serial.as_bytes())?;
        self.writer.flush()?;
        {
            let mut mp = self
                .entry_to_index
                .write()
                .expect("Fail to fetch the read lock");
            let version = self.current_ver;

            mp.entry(key)
                .and_modify(|lock| {
                    let mut v = lock.write().expect("Fail to get the exclusive key in set");
                    *v = InMemIndex {
                        version,
                        start_pos: pos,
                    };
                })
                .or_insert(RwLock::new(InMemIndex {
                    version,
                    start_pos: pos,
                }));
        }

        self.to_flush()
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        {
            let reader = self.entry_to_index.read().unwrap();
            if !reader.contains_key(&key) {
                return Err(KvsError::KeyNotFound);
            }
        }
        {
            let mut writer = self.entry_to_index.write().unwrap();
            writer.remove(&key);
        }

        let cur_op = Op::Rm { key };
        let mut serial = serde_json::to_string(&cur_op)?;
        serial.push('\n');
        self.current_len += serial.len();
        self.writer.write_all(serial.as_bytes())?;
        self.writer.flush()?;

        self.to_flush()
    }

    /// Wrapper on whether to flush the active log or not
    fn to_flush(&mut self) -> Result<()> {
        if self.current_len >= ACTIVE_THRESHOLD {
            trace!("current active log length is {}", self.current_len);
            self.flush()
        } else {
            Ok(())
        }
    }

    /// Flush a full active log into disk
    /// Rename it, and open a new active log
    fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        self.old_log_len += self.current_len;
        self.current_len = 0;
        if self.old_log_len >= THRESHOLD {
            self.compact()?;
        }

        self.current_ver += 1;
        trace!("Flush old log, and create {}.log", self.current_ver);
        let cur_file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(self.dir.join(format!("log/{}.log", self.current_ver)))?;
        self.writer = BufWriter::new(cur_file);
        Ok(())
    }

    /// Compact all old logs into one
    fn compact(&mut self) -> Result<()> {
        trace!("Begin compacting");
        let mut entry_to_index = self.entry_to_index.write().unwrap();
        let base_dir = self.dir.join("log");

        let (mut list, order, ..) = Self::traverse_dir(&base_dir)?;

        self.current_ver += 1;
        let new_log = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(base_dir.join(format!("{}.log", self.current_ver)))?;
        trace!(
            "All compacted entries will be written into {}.log",
            self.current_ver
        );
        let mut writer = BufWriter::new(new_log);
        let mut dict: HashMap<String, String> = HashMap::new();

        for ver in order {
            trace!("current log version is {}", ver);
            let mut cur_reader = list.remove(&ver).unwrap();
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

            fs::remove_file(base_dir.join(format!("{}.log", ver)))?;
        }

        let mut offset = 0_usize;
        entry_to_index.clear();
        for (k, v) in dict.into_iter() {
            entry_to_index.insert(
                k.clone(),
                RwLock::new(InMemIndex {
                    version: self.current_ver,
                    start_pos: offset,
                }),
            );
            let op = Op::Set { key: k, value: v };
            let info = serde_json::to_string(&op)?;
            writer.write_all(info.as_bytes())?;
            writer.write_all(b"\n")?;
            offset += info.len() + 1;
        }
        writer.flush()?;
        self.min_version
            .store(self.current_ver as u32, Ordering::SeqCst);
        self.old_log_len = 0;

        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Op {
    Set { key: String, value: String },
    Rm { key: String },
}

#[derive(Clone)]
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
    fn set(&self, key: String, value: String) -> Result<()> {
        trace!("in kvs: set");
        self.kv_writer.lock().unwrap().set(key, value)
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
    fn get(&self, key: String) -> Result<Option<String>> {
        let reader = self
            .entry_to_index
            .read()
            .expect("Fail to get read lock of entry to index");
        if reader.contains_key(&key) {
            let s = self
                .kv_reader
                .get(reader.get(&key).unwrap().read().unwrap().clone())?;
            Ok(Some(s))
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
    fn remove(&self, key: String) -> Result<()> {
        trace!("in kvs remove");
        self.kv_writer.lock().unwrap().remove(key)
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
        let kv_writer = KvStoreWriter::new(path, &mut ver_to_file)?;
        let kv_reader = KvStoreReader::new(
            Arc::clone(&kv_writer.dir),
            Arc::clone(&kv_writer.min_version),
            ver_to_file,
        )?;

        Ok(Self {
            dir: Arc::clone(&kv_writer.dir),
            entry_to_index: Arc::clone(&kv_writer.entry_to_index),
            kv_writer: Arc::new(Mutex::new(kv_writer)),
            kv_reader,
        })
    }
}
