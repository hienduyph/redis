use tokio::sync::{broadcast, Notify};
use tokio::time::{self, Duration, Instant};

use bytes::Bytes;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

/// Server state shared across all connections
///
#[derive(Debug, Clone)]
pub(crate) struct Db {
    shared: Arc<Shared>,
}

#[derive(Debug)]
struct Shared {
    state: Mutex<State>,
    background_task: Notify,
}

#[derive(Debug)]
struct State {
    /// key - value data
    entries: HashMap<String, Entry>,

    /// The pub/sub key-space. Redis use a **separate** key space for key-value and pub/sub.
    /// `mini-redis` handles this by using a separate `HashMap`
    pub_sub: HashMap<String, broadcast::Sender<Bytes>>,

    /// Tracks key ttls
    ///
    /// A BTreemaps is used to maintain expiratyions sorted by when they expire. This allow the
    /// background task to iterate to this map to find the value expiring ntext.
    ///
    /// This  highly unlikely, it possible for more than one expiration to be created for the same
    /// instant. Because of this, the `Instant` is insufficient for the key. A unique exxpiration
    /// identifier (`u64`) is used to break these ties.
    expirations: BTreeMap<(Instant, u64), String>,

    // Identifier to use for the next expiration. Each expiration is associated with a unique
    // identifier
    next_id: u64,

    shutdown: bool,
}

#[derive(Debug)]
struct Entry {
    // Uniquely identifier this entry
    id: u64,

    data: Bytes,

    expires_at: Option<Instant>,
}

impl Db {
    pub(crate) fn new() -> Db {
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                entries: HashMap::new(),
                pub_sub: HashMap::new(),
                expirations: BTreeMap::new(),
                next_id: 0,
                shutdown: false,
            }),
            background_task: Notify::new(),
        });

        tokio::spawn(purge_expired_tasks(shared.clone()));
        Db { shared }
    }

    pub(crate) fn get(&self, key: &str) -> Option<Bytes> {
        let state = self.shared.state.lock().unwrap();
        state.entries.get(key).map(|entry| entry.data.clone())
    }

    /// Set the value associated with a key along with an optional expiration Duration
    pub(crate) fn set(&self, key: String, value: Bytes, expire: Option<Duration>) {
        let mut state = self.shared.state.lock().unwrap();

        let id = state.next_id;
        state.next_id += 1;

        // if this `set` becomes the key that expires **next**, thie background task needs to be
        // notified so it can update its sate
        //
        // whther or not the task needs to be notifie is computed during the `set` routine.
        let mut notify = false;

        let expires_at = expire.map(|duration| {
            let when = Instant::now() + duration;
            // Only notify the worker task if the newly inserted expiration is the **next** key to
            // evict. In this case, the worker needs to be woken up to update its state
            notify = state.next_expiration().map(|e| e > when).unwrap_or(true);

            // track the expiration
            state.expirations.insert((when, id), key.clone());
            when
        });
        // insert then entry nito the `HashMap`
        let prev = state.entries.insert(
            key,
            Entry {
                id,
                data: value,
                expires_at,
            },
        );

        // if there was a value previously associated with the key **and** it had an expiration
        // time. The associated entry in the `expirations` map must also be removed. This avoud
        // leak data.
        if let Some(prev) = prev {
            if let Some(when) = prev.expires_at {
                // clear the expiration
                state.expirations.remove(&(when, prev.id));
            }
        }
        // relase the mutex before notifying the background task. This helps reduce contention by
        // aboud the background task waking up only to be unable to acquire the mutex due to this
        // functions still holding it.
        drop(state);

        if notify {
            self.shared.background_task.notify_one();
        }
    }

    pub(crate) fn subscribe(&self, key: String) -> broadcast::Receiver<Bytes> {
        use std::collections::hash_map::Entry;
        let mut state = self.shared.state.lock().unwrap();

        match state.pub_sub.entry(key) {
            Entry::Occupied(e) => e.get().subscribe(),
            Entry::Vacant(e) => {
                // No broadcast channel exist yet, so create one.
                //
                // The channel is crated with a capacity of `1024` messages. A mesage is stored in
                // the channel until *all* subscribers have seen it. This means that a slow
                // subscriber could result in messages being held indefinitely.
                //
                // When the channel's capacity fills up, publishing will result in old messages
                // being dropped. This prevent slow consumers from blocking enrire system.
                let (tx, rx) = broadcast::channel(1024);
                e.insert(tx);
                rx
            }
        }
    }

    /// Publish a mesage to the channel. Returns the number of subscribers listening on the channel
    pub(crate) fn publish(&self, key: &str, value: Bytes) -> usize {
        let state = self.shared.state.lock().unwrap();
        state
            .pub_sub
            .get(key)
            .map(|tx| tx.send(value).unwrap_or(0))
            .unwrap_or(0)
    }
}

impl Drop for Db {
    /// If this is the last active `Db` instance, the background task must be notified to shutdown
    ///
    /// First determine if this the last `Db` instance. This is done by checking `strong_count`.
    /// The count will be 2. On for this `Db` instance and one for the handle held by the
    /// background task
    fn drop(&mut self) {
        if Arc::strong_count(&self.shared) == 2 {
            // this background task must be signaled to shutdown
            let mut state = self.shared.state.lock().unwrap();
            state.shutdown = true;

            // Drop the lock before signalling the background task. This helps reduce lock
            // contention by ensuring the background task doesn't ake up only to be unable to
            // acquire the mutex.
            drop(state);
            self.shared.background_task.notify_one();
        }
    }
}

impl Shared {
    fn purge_expired_keys(&self) -> Option<Instant> {
        let mut state = self.state.lock().unwrap();

        if state.shutdown {
            return None;
        }

        let state = &mut *state;
        let now = Instant::now();

        while let Some((&(when, id), key)) = state.expirations.iter().next() {
            if when > now {
                return Some(when);
            }
            state.entries.remove(key);
            state.expirations.remove(&(when, id));
        }
        None
    }

    fn is_shutdown(&self) -> bool {
        self.state.lock().unwrap().shutdown
    }
}

impl State {
    fn next_expiration(&self) -> Option<Instant> {
        self.expirations.keys().next().map(|e| e.0)
    }
}

/// Routine excuted by the background task
async fn purge_expired_tasks(shared: Arc<Shared>) {
    while !shared.is_shutdown() {
        if let Some(when) = shared.purge_expired_keys() {
            // Wait until the next keys expires or until the background task is notified. If the
            // task is notified, then it must reload its state as new keys has been set to expire
            // early. This is done by looping
            tokio::select! {
                _ = time::sleep_until(when) => {}
                _ = shared.background_task.notified() => {}
            }
        } else {
            shared.background_task.notified().await;
        }
    }
}
