use std::env;

use super::KvsEngine;
use crate::error::{KvsError, Result};
use log::debug;
use sled::Db;

pub struct SledKvsEngine {
    db: Db,
}

impl KvsEngine for SledKvsEngine {
    fn get(&mut self, key: String) -> Result<Option<String>> {
        let ans = self.db.get(key)?;
        match ans {
            None => {
                debug!("key does not exist");
                Ok(None)
            }
            Some(arr) => {
                let s = String::from_utf8(arr.to_vec())?;
                debug!("key exists, value is {}", s);
                Ok(Some(s))
            }
        }
    }

    fn remove(&mut self, key: String) -> Result<()> {
        let q = self.db.remove(key)?;
        if q.is_none() {
            return Err(KvsError::KeyNotFound);
        }
        self.db.flush()?;
        Ok(())
    }

    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.db.insert(key, value)?;
        self.db.flush()?;
        Ok(())
    }
}

impl SledKvsEngine {
    pub fn new() -> Result<Self> {
        let cwd = env::current_dir()?;
        let cwd = cwd.join("sled-db");
        let db = sled::open(cwd)?;
        Ok(Self { db })
    }

    pub fn open(path: Db) -> Self {
        Self { db: path }
    }
}
