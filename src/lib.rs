#![deny(missing_docs)]

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

use std::collections::HashMap;

/// A key value store
pub struct KvStore {

    store: HashMap<String, String>,
}

impl KvStore {
    /// Create a new KvStore
    /// 
    /// # Examples
    /// 
    /// ```
    /// use kvs::KvStore;
    /// let kvs = KvStore::new();    
    /// ```
    pub fn new() -> Self {
        Self {
            store: HashMap::new(),
        }
    }

    /// Map `key` to `value` in the kv store
    /// 
    /// # Examples
    /// 
    /// ```
    /// use kvs::KvStore;
    /// let mut kvs = KvStore::new();
    /// kvs.set("jack".to_string(), "2024".to_string());
    /// ```
    pub fn set(&mut self, key: String, value: String) {
        self.store.insert(key, value);
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
    /// assert_eq!(kvs.get(String::from("jack")), Some(String::from("2024")));
    /// assert_eq!(kvs.get(k2), None);
    /// ```
    pub fn get(&self, key: String) -> Option<String> {
        let q = self.store.get(&key);
        match q {
            Some(s) => Some(s.clone()),
            None => None,
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
    /// kvs.remove(String::from("jack"));
    /// assert_eq!(kvs.get(String::from("jack")), None);
    /// ```
    pub fn remove(&mut self, key: String) {
        self.store.remove(&key).unwrap();
    }
}