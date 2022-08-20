use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use rocksdb::{DB, WriteOptions};
use tokio::sync::broadcast;

#[derive(Clone)]
pub struct RocksDB {
    db: Arc<DB>,
}

impl RocksDB {
    pub(crate) fn new(path: &str) -> RocksDB {
        RocksDB {db: Arc::new(DB::open_default(path).unwrap()) }
    }

    pub(crate) fn get(&self, key: &str) -> Option<Bytes> {
        match self.db.get(key) {
            Ok(Some(v)) => {
                Some(v.into())
            },
            Ok(None) => {
                None
            },
            Err(e) => {
                println!("got error while get key `{}`, err: {}", key, e);
                None
            }
        }
    }

    /// Set the value associated with a key along with an optional expiration Duration
    pub(crate) fn set(&self, key: String, value: Bytes, expire: Option<Duration>) {
        self.db.put(key, value).unwrap();
    }

    pub(crate) fn subscribe(&self, key: String) -> broadcast::Receiver<Bytes> {
        panic!("impl")
    }

    /// Publish a mesage to the channel. Returns the number of subscribers listening on the channel
    pub(crate) fn publish(&self, key: &str, value: Bytes) -> usize {
        panic!("impl")
    }
}
